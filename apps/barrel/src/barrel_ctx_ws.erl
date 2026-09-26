%%%-------------------------------------------------------------------
%%% @doc Working sets (B13, action plan 3.5): a local record of the
%%% contexts an agent works with, with budgets (6.1). One document per
%%% working set in the local database `_barrel_worksets'.
%%%
%%% Attach and detach are logical: they open, pin, and download
%%% nothing. {@link members/1} resolves each member (mode, local
%%% database, generation, coverage, open options) for an executor;
%%% {@link open_member/1} is the one call that opens a local copy.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ctx_ws).

-export([create/1,
         get/1,
         list/0,
         delete/1,
         attach/2,
         detach/2,
         detach/3,
         members/1,
         open_member/1,
         import_snapshot/3,
         materialize/2,
         budget_defaults/0]).

%% Internal: shared with barrel_ctx_slice.
-export([update/2, remaining_bytes/1]).

-export_type([ws/0, member/0, budget/0]).

-define(DB, <<"_barrel_worksets">>).
-define(MAX_RETRIES, 16).

-type mode() :: local | remote | snapshot | retrieved_set.
-type member() :: #{context := binary(), mode := mode(),
                    local_db => binary(),
                    location => #{endpoint := binary(), db := binary()},
                    credential_ref => binary(),
                    generation => pos_integer(),
                    predicate => [term()],
                    derived => map(),
                    bytes => non_neg_integer()}.
-type budget() :: #{bytes := non_neg_integer(),
                    contexts := pos_integer(),
                    open_dbs := non_neg_integer(),
                    remote_parallel := pos_integer(),
                    transfer_bytes := non_neg_integer(),
                    deadline_ms := pos_integer()}.
-type ws() :: #{id := binary(), owner := term(), created_at := binary(),
                budget := budget(), members := [member()],
                usage := #{bytes := non_neg_integer()}}.

%%====================================================================
%% Lifecycle
%%====================================================================

%% @doc 6.1 defaults; `open_dbs' follows barrel_dbs `dbs_max_open'.
-spec budget_defaults() -> budget().
budget_defaults() ->
    #{bytes => 1073741824, contexts => 8,
      open_dbs => application:get_env(barrel, dbs_max_open, 0),
      remote_parallel => 4, transfer_bytes => 268435456,
      deadline_ms => 5000}.

%% @doc Create a working set. Options: `id', `owner', `budget'
%% (overrides merged over the defaults).
-spec create(map()) -> {ok, binary()} | {error, term()}.
create(Opts) ->
    Id = maps:get(id, Opts, barrel_ctx_manifest:new_id(<<"ws_">>)),
    Budget = maps:merge(budget_defaults(), maps:get(budget, Opts, #{})),
    Ws = #{id => Id, owner => maps:get(owner, Opts, null),
           created_at => now_rfc3339(), budget => Budget, members => [],
           usage => #{bytes => 0}},
    ok = ensure_store(),
    case barrel_docdb:put_doc(?DB, to_doc(Ws)) of
        {ok, _} -> {ok, Id};
        {error, conflict} -> {error, already_exists};
        {error, _} = Err -> Err
    end.

-spec get(binary()) -> {ok, ws()} | {error, not_found}.
get(Id) ->
    ok = ensure_store(),
    case barrel_docdb:get_doc(?DB, Id) of
        {ok, Doc} -> {ok, from_doc(Doc)};
        {error, _} -> {error, not_found}
    end.

-spec list() -> [binary()].
list() ->
    ok = ensure_store(),
    {ok, Ids} = barrel_docdb:fold_docs(?DB,
        fun(#{<<"id">> := Id}, Acc) -> {ok, [Id | Acc]} end, []),
    lists:sort(Ids).

%% @doc Delete a working set and the slices it owns (snapshots are
%% imports shared by name and stay).
-spec delete(binary()) -> ok | {error, term()}.
delete(Id) ->
    case ?MODULE:get(Id) of
        {ok, #{members := Members}} ->
            [ok = barrel_ctx_slice:remove(Db)
             || #{mode := retrieved_set, local_db := Db} <- Members],
            {ok, Doc} = barrel_docdb:get_doc(?DB, Id),
            case barrel_docdb:delete_doc(?DB, Id,
                                         #{rev => maps:get(<<"_rev">>, Doc)}) of
                {ok, _} -> ok;
                {error, _} = Err -> Err
            end;
        {error, _} = Err ->
            Err
    end.

%%====================================================================
%% Members
%%====================================================================

%% @doc Add a member. Logical: nothing is opened. One member per
%% context; the `contexts' budget bounds the member count.
-spec attach(binary(), member()) -> {ok, ws()} | {error, term()}.
attach(Id, Member) ->
    case validate_member(Member) of
        ok ->
            update(Id, fun(Ws) -> add_member(Ws, Member) end);
        {error, _} = Err ->
            Err
    end.

add_member(#{members := Ms, budget := #{contexts := Max}} = Ws,
           #{context := Ctx} = M) ->
    Bytes = maps:get(bytes, M, 0),
    case {lists:any(fun(#{context := C}) -> C =:= Ctx end, Ms),
          length(Ms) >= Max, Bytes > remaining_bytes(Ws)} of
        {true, _, _} -> {error, {already_attached, Ctx}};
        {_, true, _} -> {error, {over_budget, contexts, Max}};
        {_, _, true} -> {error, {over_budget, bytes,
                                 #{needed => Bytes,
                                   available => remaining_bytes(Ws)}}};
        {false, false, false} ->
            {ok, add_usage(Ws#{members := Ms ++ [M]}, Bytes)}
    end.

validate_member(#{context := C, mode := local, local_db := Db})
        when is_binary(C), is_binary(Db) -> ok;
validate_member(#{context := C, mode := remote,
                  location := #{endpoint := E, db := Db}})
        when is_binary(C), is_binary(E), is_binary(Db) -> ok;
validate_member(#{context := C, mode := snapshot, local_db := Db,
                  generation := G})
        when is_binary(C), is_binary(Db), is_integer(G) -> ok;
validate_member(#{context := C, mode := retrieved_set, local_db := Db,
                  derived := D})
        when is_binary(C), is_binary(Db), is_map(D) -> ok;
validate_member(Other) ->
    {error, {invalid_member, Other}}.

%% @doc Remove the member for `Ctx'. Its slice is removed too (a slice
%% belongs to one working set); `#{keep_slice => true}' keeps it.
-spec detach(binary(), binary()) -> {ok, ws()} | {error, term()}.
detach(Id, Ctx) ->
    detach(Id, Ctx, #{}).

-spec detach(binary(), binary(), map()) -> {ok, ws()} | {error, term()}.
detach(Id, Ctx, Opts) ->
    case update(Id, fun(Ws) -> remove_member(Ws, Ctx) end) of
        {ok, Ws, #{mode := retrieved_set, local_db := Db}} ->
            _ = case maps:get(keep_slice, Opts, false) of
                true -> ok;
                false -> barrel_ctx_slice:remove(Db)
            end,
            {ok, Ws};
        {ok, Ws, _Member} ->
            {ok, Ws};
        {error, _} = Err ->
            Err
    end.

remove_member(#{members := Ms} = Ws, Ctx) ->
    case lists:partition(fun(#{context := C}) -> C =:= Ctx end, Ms) of
        {[M], Rest} ->
            {ok, add_usage(Ws#{members := Rest}, -maps:get(bytes, M, 0)), M};
        {[], _} ->
            {error, {not_attached, Ctx}}
    end.

%% @doc Members resolved for an executor, without opening anything:
%% local database and open options for local copies, whether the copy
%% is present, the version kind and the coverage it gives.
-spec members(binary()) -> {ok, [map()]} | {error, not_found}.
members(Id) ->
    case ?MODULE:get(Id) of
        {ok, #{members := Ms}} -> {ok, [resolve(M) || M <- Ms]};
        {error, _} = Err -> Err
    end.

resolve(#{mode := local, local_db := Db} = M) ->
    M#{coverage => live, version => #{kind => live},
       available => true, open_opts => #{}, db_name => Db};
resolve(#{mode := remote} = M) ->
    M#{coverage => live, version => #{kind => live}, available => false};
resolve(#{mode := snapshot, local_db := Db, generation := G} = M) ->
    Base = M#{coverage => complete_generation,
              version => #{kind => generation, generation => G}},
    case barrel_ctx_export:open_opts(Db) of
        {ok, Opts} -> Base#{available => true, open_opts => Opts};
        {error, _} -> Base#{available => false}
    end;
resolve(#{mode := retrieved_set, local_db := Db, derived := D} = M) ->
    Base = M#{coverage => retrieved_set,
              version => #{kind => retrieved_set,
                           observed => maps:get(<<"observed">>, D, null)}},
    case barrel_ctx_slice:open_opts(Db) of
        {ok, Opts} -> Base#{available => true, open_opts => Opts};
        {error, _} -> Base#{available => false}
    end.

%% @doc Open a resolved member's local copy through barrel_dbs.
-spec open_member(map()) -> {ok, barrel:db()} | {error, term()}.
open_member(#{mode := remote}) ->
    {error, remote_member};
open_member(#{available := false, local_db := Db}) ->
    {error, {local_copy_missing, Db}};
open_member(#{local_db := Db, open_opts := Opts}) ->
    barrel_dbs:ensure(Db, Opts).

%% @doc Import a published generation into this node (unopened) and
%% attach it as a snapshot member. The manifest's byte count is checked
%% against the remaining budget before any file is copied.
-spec import_snapshot(binary(), file:filename(), map()) ->
    {ok, ws()} | {error, term()}.
import_snapshot(Id, Src, Opts) ->
    case {?MODULE:get(Id), barrel_ctx_manifest:read(Src)} of
        {{ok, Ws}, {ok, Root, Arts}} ->
            Bytes = lists:sum([B || #{bytes := B} <- Arts]),
            case Bytes > remaining_bytes(Ws) of
                true ->
                    {error, {over_budget, bytes,
                             #{needed => Bytes,
                               available => remaining_bytes(Ws)}}};
                false ->
                    import_and_attach(Id, Src, Root, Bytes, Opts)
            end;
        {{error, _} = Err, _} ->
            Err;
        {_, {error, _} = Err} ->
            Err
    end.

import_and_attach(Id, Src, Root, Bytes, Opts) ->
    case barrel_ctx_export:import(Src, Opts#{open => false}) of
        {ok, #{name := Name}} ->
            attach(Id, #{context => maps:get(<<"context">>, Root),
                         mode => snapshot, local_db => Name,
                         generation => maps:get(<<"generation">>, Root),
                         bytes => Bytes});
        {error, _} = Err ->
            Err
    end.

%% @doc Save a retrieved set into the working set (see barrel_ctx_slice).
-spec materialize(binary(), map()) -> {ok, map()} | {error, term()}.
materialize(Id, Request) ->
    barrel_ctx_slice:materialize(Id, Request).

%%====================================================================
%% Storage
%%====================================================================

%% @doc Read-modify-write under the document's version (retried on a
%% concurrent update). `Fun(Ws)' returns `{ok, Ws1}', `{ok, Ws1, Extra}'
%% or `{error, _}'.
-spec update(binary(), fun((ws()) -> {ok, ws()} | {ok, ws(), term()} |
                                     {error, term()})) ->
    {ok, ws()} | {ok, ws(), term()} | {error, term()}.
update(Id, Fun) ->
    ok = ensure_store(),
    update(Id, Fun, ?MAX_RETRIES).

update(_Id, _Fun, 0) ->
    {error, too_many_conflicts};
update(Id, Fun, N) ->
    case barrel_docdb:get_doc(?DB, Id) of
        {ok, Doc} ->
            Rev = maps:get(<<"_rev">>, Doc),
            case Fun(from_doc(Doc)) of
                {ok, Ws} -> write(Id, Fun, N, Rev, Ws, none);
                {ok, Ws, Extra} -> write(Id, Fun, N, Rev, Ws, {extra, Extra});
                {error, _} = Err -> Err
            end;
        {error, _} ->
            {error, not_found}
    end.

write(Id, Fun, N, Rev, Ws, Extra) ->
    case barrel_docdb:put_doc(?DB, (to_doc(Ws))#{<<"_rev">> => Rev}) of
        {ok, _} when Extra =:= none -> {ok, Ws};
        {ok, _} -> {ok, Ws, element(2, Extra)};
        {error, conflict} -> update(Id, Fun, N - 1);
        {error, _} = Err -> Err
    end.

-spec remaining_bytes(ws()) -> integer().
remaining_bytes(#{budget := #{bytes := Max}, usage := #{bytes := Used}}) ->
    Max - Used.

add_usage(#{usage := #{bytes := B} = U} = Ws, Delta) ->
    Ws#{usage := U#{bytes := max(0, B + Delta)}}.

%% The store lives under the current ctx_dir: a store left open under
%% another dir is closed and reopened here, never silently reused.
ensure_store() ->
    Dir = filename:join(ctx_dir(), "worksets"),
    case barrel_docdb:db_info(?DB) of
        {ok, #{db_path := Path}} ->
            case same_path(Path, filename:join(Dir, ?DB)) of
                true -> ok;
                false ->
                    _ = barrel_docdb:close_db(?DB),
                    open_store(Dir)
            end;
        {error, not_found} ->
            open_store(Dir)
    end.

open_store(Dir) ->
    case barrel_docdb:create_db(?DB, #{data_dir => Dir}) of
        {ok, _} -> ok;
        {error, already_exists} -> ok
    end.

same_path(A, B) ->
    unicode:characters_to_list(filename:absname(A)) =:=
        unicode:characters_to_list(filename:absname(B)).

ctx_dir() ->
    filename:dirname(barrel_ctx_export:imports_dir()).

%% Documents hold JSON-shaped values: binary keys, modes as binaries,
%% conditions and owners as opaque base64 terms.
to_doc(#{id := Id, owner := Owner, created_at := At, budget := B,
         members := Ms, usage := U}) ->
    #{<<"id">> => Id, <<"type">> => <<"working_set">>,
      <<"owner">> => term_b64(Owner), <<"created_at">> => At,
      <<"budget">> => bin_keys(B), <<"members">> => [member_doc(M) || M <- Ms],
      <<"usage">> => bin_keys(U)}.

from_doc(#{<<"id">> := Id, <<"owner">> := Owner, <<"created_at">> := At,
           <<"budget">> := B, <<"members">> := Ms, <<"usage">> := U}) ->
    #{id => Id, owner => b64_term(Owner), created_at => At,
      budget => atom_keys(B), members => [doc_member(M) || M <- Ms],
      usage => atom_keys(U)}.

member_doc(M) ->
    maps:fold(fun member_field/3, #{}, M).

member_field(mode, V, Acc) -> Acc#{<<"mode">> => atom_to_binary(V)};
member_field(location, V, Acc) -> Acc#{<<"location">> => bin_keys(V)};
member_field(predicate, V, Acc) -> Acc#{<<"predicate">> => term_b64(V)};
member_field(K, V, Acc) -> Acc#{atom_to_binary(K) => V}.

doc_member(D) ->
    maps:fold(fun doc_field/3, #{}, D).

doc_field(<<"mode">>, V, Acc) -> Acc#{mode => binary_to_existing_atom(V)};
doc_field(<<"location">>, V, Acc) -> Acc#{location => atom_keys(V)};
doc_field(<<"predicate">>, V, Acc) -> Acc#{predicate => b64_term(V)};
doc_field(K, V, Acc) -> Acc#{binary_to_existing_atom(K) => V}.

bin_keys(M) -> maps:fold(fun(K, V, A) -> A#{atom_to_binary(K) => V} end,
                         #{}, M).
atom_keys(M) -> maps:fold(fun(K, V, A) ->
                              A#{binary_to_existing_atom(K) => V}
                          end, #{}, M).

term_b64(T) -> base64:encode(term_to_binary(T)).
b64_term(B) -> binary_to_term(base64:decode(B), [safe]).

now_rfc3339() ->
    list_to_binary(calendar:system_time_to_rfc3339(
                     erlang:system_time(second), [{offset, "Z"}])).
