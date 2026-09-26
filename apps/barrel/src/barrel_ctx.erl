%%%-------------------------------------------------------------------
%%% @doc Contexts: named datasets queried together wherever they live.
%%%
%%% This module is the contract; REST (`barrel_server') and MCP map to
%%% it one to one and return the same maps. Every function that takes a
%%% context accepts its id (`ctx_...') or its name. Errors are always
%%% `{error, {Code, Details}}' with a code from {@link
%%% barrel_ctx_error:codes/0}; {@link barrel_ctx_error:to_map/1} gives
%%% the JSON body (code, message, hint, details).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ctx).

-export([capabilities/0]).
-export([discover/2, inspect/1, list/1, register/1, update/2,
         unregister/1, resolve/1]).
-export([query/1]).
-export([create_ws/1, get_ws/1, list_ws/0, delete_ws/1, attach/3,
         detach/2, materialize/2, import/3]).
-export([offline/0, set_offline/1]).

-define(MAX_ROWS, 1000).

-type context_ref() :: binary().
%% A context id (`ctx_...') or a context name.
-type ws_ref() :: binary() | new.
%% A working-set id, or `new' to create one for this call.
-type error() :: {error, barrel_ctx_error:error()}.
-type card() :: barrel_ctx_catalog:card().
-type ws_view() :: #{id := binary(), owner := term(), created_at := binary(),
                     budget := map(), usage := map(), members := [map()],
                     summary := binary()}.
-type query_response() ::
        #{execution := succeeded | partial | failed,
          merge := ordered | grouped | interleave | score,
          sources := [map()], coverage := map(), summary := binary(),
          elapsed_ms := non_neg_integer(), rows => [map()],
          groups => [map()], relevance => boolean(),
          merge_fallback => map(), working_set => binary()}.
-export_type([context_ref/0, ws_ref/0, error/0, ws_view/0,
              query_response/0]).

%%====================================================================
%% Capabilities
%%====================================================================

%% @doc What a query may look like, the merges, limits and budget
%% defaults, and whether the node is offline. Read it before a first
%% query instead of learning the rules from errors.
-spec capabilities() -> map().
capabilities() ->
    #{shapes =>
          [#{shape => ordered_rows,
             example => <<"SELECT id, path, lines FROM c WHERE lines > 300 "
                          "ORDER BY lines DESC LIMIT 10">>,
             requires => [<<"LIMIT n">>, <<"ORDER BY a selected field">>],
             merge => ordered},
           #{shape => unordered_rows,
             example => <<"SELECT id, path FROM c WHERE lines > 300 LIMIT 10">>,
             requires => [<<"LIMIT n">>],
             merge => grouped},
           #{shape => retrieval,
             functions => [bm25_top_k, vector_top_k, hybrid_top_k],
             example => <<"SELECT b.id, b.path, b._score FROM "
                          "bm25_top_k('release upgrade', k => 5) AS b">>,
             requires => [<<"k => n">>],
             merge => <<"grouped; vector_top_k is merged by score when every "
                        "context reports the same embedding">>}],
      merges =>
          #{ordered => <<"rows sorted by the ORDER BY key across contexts">>,
            grouped => <<"one group per context, each in its own order">>,
            interleave => <<"round-robin by rank; presentation only, "
                            "relevance false">>,
            score => <<"global order by vector score; vector_top_k only, "
                       "same embedding fingerprint and cosine">>},
      refused_merges =>
          #{rrf => <<"rank fusion over disjoint corpora is not a relevance "
                     "order">>,
            rerank => <<"no validated reranker yet">>},
      rejected => [<<"SUBSCRIBE">>, <<"OFFSET">>, <<"UNNEST">>,
                   <<"continuation">>],
      bql => <<"Strings use single quotes ('text'); double quotes name "
               "fields. The collection alias is free (FROM c).">>,
      limits =>
          #{max_contexts => env(ctx_max_contexts, 8),
            max_limit => ?MAX_ROWS,
            deadline_ms => #{default => env(ctx_deadline_ms, 5000),
                             max => 60000},
            per_context_timeout_ms =>
                #{default => env(ctx_member_timeout_ms, 4000), max => 60000},
            max_parallel => #{default => env(ctx_max_parallel, 8),
                              max => 16}},
      working_set_budget => maps:with([bytes, contexts, transfer_bytes,
                                       remote_parallel, deadline_ms],
                                      barrel_ctx_ws:budget_defaults()),
      offline => offline()}.

%%====================================================================
%% Cards
%%====================================================================

%% @doc Listed cards where every word of `Query' appears in the name,
%% title, description or topics. A filter, not a ranking. Options:
%% `include_unlisted'.
-spec discover(binary(), map()) -> {ok, [card()]}.
discover(Query, Opts) ->
    barrel_ctx_catalog:discover(Query, Opts).

%% @doc The card of a context, by id or name.
-spec inspect(context_ref()) -> {ok, card()} | error().
inspect(Ref) ->
    case resolve(Ref) of
        {ok, Id} -> ok_or_error(barrel_ctx_catalog:get(Id), Ref);
        {error, _} = Err -> Err
    end.

%% @doc Registered cards sorted by name. Options: `include_unlisted'
%% (default false), `name_prefix'.
-spec list(map()) -> {ok, [card()]}.
list(Opts) ->
    barrel_ctx_catalog:list(Opts).

%% @doc Register a card (`name', `locations', optional `title',
%% `description', `topics', `embedding'). The id is minted unless given.
%% Errors: `invalid_card', `already_exists'.
-spec register(card()) -> {ok, card()} | error().
register(Card) ->
    wrap(barrel_ctx_catalog:register(Card)).

%% @doc Merge `Changes' into a card. Errors: `unknown_context',
%% `ambiguous_context', `invalid_card'.
-spec update(context_ref(), card()) -> {ok, card()} | error().
update(Ref, Changes) ->
    case resolve(Ref) of
        {ok, Id} -> wrap(barrel_ctx_catalog:update(Id, Changes));
        {error, _} = Err -> Err
    end.

%% @doc Remove a card. Working sets that hold it keep their members.
-spec unregister(context_ref()) -> ok | error().
unregister(Ref) ->
    case resolve(Ref) of
        {ok, Id} -> wrap(barrel_ctx_catalog:unregister(Id));
        {error, _} = Err -> Err
    end.

%% @doc The id of a context given its id or name. Errors:
%% `unknown_context' (with close names in `suggestions'),
%% `ambiguous_context' (with the `candidates').
-spec resolve(context_ref()) -> {ok, binary()} | error().
resolve(Ref) when is_binary(Ref), Ref =/= <<>> ->
    case barrel_ctx_catalog:get(Ref) of
        {ok, _} ->
            {ok, Ref};
        {error, _} ->
            {ok, Cards} = barrel_ctx_catalog:list(#{include_unlisted => true}),
            case [C || #{<<"name">> := N} = C <- Cards, N =:= Ref] of
                [#{<<"id">> := Id}] ->
                    {ok, Id};
                [] ->
                    {error, {unknown_context,
                             #{context => Ref,
                               suggestions => suggestions(Ref, Cards)}}};
                Many ->
                    {error, {ambiguous_context,
                             #{name => Ref, candidates => [brief(C)
                                                           || C <- Many]}}}
            end
    end;
resolve(_Ref) ->
    {error, {invalid_argument, #{field => context,
                                 expected => <<"a context name or id">>}}}.

%%====================================================================
%% Query
%%====================================================================

%% @doc Run one BQL statement over `contexts' (names or ids) or over a
%% `working_set'. The answer holds `execution', `merge', `rows' or
%% `groups', one `sources' entry per context (with `name', and an
%% `error' with message and hint when it did not answer), `coverage'
%% and a `summary'. Request keys: see {@link barrel_ctx_query:request()};
%% limits: {@link capabilities/0}. Errors: `invalid_argument',
%% `invalid_query', `limit_required', `limit_too_large',
%% `too_many_contexts', `duplicate_context',
%% `unsupported_federated_query', `merge_not_allowed',
%% `merge_not_supported', `scores_not_comparable', `unknown_context',
%% `ambiguous_context', `unknown_working_set'.
-spec query(barrel_ctx_query:request()) -> {ok, query_response()} | error().
query(Req) ->
    case scope(Req) of
        {ok, Req1} ->
            case barrel_ctx_query:run(Req1) of
                {ok, Resp} ->
                    {ok, barrel_ctx_explain:query(
                           Resp, names_of(Resp), maps:get(query, Req1))};
                {error, _} = Err ->
                    wrap(Err)
            end;
        {error, _} = Err ->
            Err
    end.

%% Names resolve to ids before the executor runs; the count is checked
%% first so a long list is not resolved for nothing.
scope(#{contexts := _, working_set := _}) ->
    wrap({error, {bad_request, scope}});
scope(#{contexts := Refs} = Req) when is_list(Refs), Refs =/= [] ->
    Max = env(ctx_max_contexts, 8),
    case length(Refs) > Max of
        true ->
            {error, {too_many_contexts, #{requested => length(Refs),
                                          max_contexts => Max}}};
        false ->
            case resolve_all(Refs, []) of
                {ok, Ids} -> {ok, Req#{contexts := Ids}};
                {error, _} = Err -> Err
            end
    end;
scope(#{contexts := _}) ->
    wrap({error, {bad_request, contexts}});
scope(Req) ->
    {ok, Req}.

resolve_all([], Acc) ->
    Ids = lists:reverse(Acc),
    case Ids -- lists:usort(Ids) of
        [] -> {ok, Ids};
        [Dup | _] -> {error, {duplicate_context, #{context => Dup}}}
    end;
resolve_all([Ref | Rest], Acc) ->
    case resolve(Ref) of
        {ok, Id} -> resolve_all(Rest, [Id | Acc]);
        {error, _} = Err -> Err
    end.

%%====================================================================
%% Working sets
%%====================================================================

%% @doc Create an empty working set and return its id. Options: `owner',
%% `budget' (any of `bytes', `contexts', `transfer_bytes',
%% `remote_parallel', `deadline_ms'; see {@link capabilities/0} for the
%% defaults). Errors: `invalid_argument'.
-spec create_ws(map()) -> {ok, binary()} | error().
create_ws(Opts) ->
    case check_budget(maps:get(budget, Opts, #{})) of
        {ok, Budget} -> wrap(barrel_ctx_ws:create(Opts#{budget => Budget}));
        {error, _} = Err -> Err
    end.

%% @doc A working set: budget, usage, and each member with its `name',
%% `mode' (`local', `remote', `snapshot', `retrieved_set'),
%% `membership' (what its answers cover), `answers_offline', and a
%% `summary'. Errors: `unknown_working_set'.
-spec get_ws(binary()) -> {ok, ws_view()} | error().
get_ws(WsId) ->
    case barrel_ctx_ws:get(WsId) of
        {ok, Ws} -> {ok, view(Ws)};
        {error, not_found} -> {error, unknown_ws(WsId)}
    end.

%% @doc Every working set on this node, with its member names.
-spec list_ws() -> [map()].
list_ws() ->
    [brief_ws(Id) || Id <- barrel_ctx_ws:list()].

%% @doc Delete a working set and the slices it owns (imported
%% snapshots stay). Errors: `unknown_working_set'.
-spec delete_ws(binary()) -> ok | error().
delete_ws(WsId) ->
    case barrel_ctx_ws:delete(WsId) of
        ok -> ok;
        {error, not_found} -> {error, unknown_ws(WsId)};
        {error, _} = Err -> wrap(Err)
    end.

%% @doc Attach a context to a working set (`new' creates one). Logical:
%% nothing is opened or copied. Options: `mode' (`local' or `remote';
%% default the card's first local location, else its first remote
%% one), `credential_ref'. Returns the working set. Errors:
%% `unknown_context', `ambiguous_context', `unknown_working_set',
%% `already_attached', `no_location', `over_budget'.
-spec attach(ws_ref(), context_ref(), map()) -> {ok, ws_view()} | error().
attach(WsRef, Ref, Opts) ->
    with_ws(WsRef, fun(WsId) ->
        case resolve(Ref) of
            {ok, Id} ->
                {ok, #{<<"locations">> := Locs}} = barrel_ctx_catalog:get(Id),
                case member_for(Id, Locs, Opts) of
                    {ok, Member} ->
                        ws_result(WsId, barrel_ctx_ws:attach(WsId, Member));
                    {error, _} = Err ->
                        wrap(Err)
                end;
            {error, _} = Err ->
                Err
        end
    end).

%% @doc Remove a context from a working set; its slice is deleted.
%% Errors: `unknown_working_set', `not_attached', `unknown_context'.
-spec detach(binary(), context_ref()) -> {ok, ws_view()} | error().
detach(WsId, Ref) ->
    Id = case resolve(Ref) of
        {ok, I} -> I;
        {error, _} -> Ref
    end,
    ws_result(WsId, barrel_ctx_ws:detach(WsId, Id)).

%% @doc Save what a query returns into the working set (`new' creates
%% one): one frozen slice per answering context, holding exactly the
%% documents whose ids the query returned. `Request': `from_query'
%% (`query', `contexts'), optional `include' (`embeddings', default
%% true), `max_bytes', `embedding', `timeout'. Each slice reports
%% `status' (`complete', `empty', `failed' or the source's status) and
%% an `error' with message and hint when nothing was saved. Errors:
%% `offline' (a remote source while offline), and those of {@link
%% query/1}.
-spec materialize(ws_ref(), map()) -> {ok, map()} | error().
materialize(WsRef, #{from_query := #{query := Bql, contexts := Refs}} = Req)
        when is_list(Refs), Refs =/= [] ->
    case resolve_all(Refs, []) of
        {ok, Ids} ->
            case offline_blocks(Req, Ids) of
                [] ->
                    with_ws(WsRef, fun(WsId) ->
                        do_materialize(WsId, Bql, Ids, Req)
                    end);
                Remote ->
                    {error, {offline, #{operation => materialize,
                                        remote_contexts => Remote}}}
            end;
        {error, _} = Err ->
            Err
    end;
materialize(_WsRef, _Req) ->
    {error, {invalid_argument,
             #{field => from_query,
               expected => <<"{query, contexts: [name or id, ...]}">>}}}.

do_materialize(WsId, Bql, Ids, Req) ->
    QReq = maps:merge(#{query => Bql, contexts => Ids, merge => grouped},
                      maps:with([authorize, open_opts, params, offline], Req)),
    case barrel_ctx_query:run(QReq) of
        {ok, #{sources := Sources} = Resp} ->
            Groups = maps:get(groups, Resp, []),
            Slices = [slice(WsId, S, Groups, Req) || S <- Sources],
            {ok, Ws} = barrel_ctx_ws:get(WsId),
            #{usage := #{bytes := Used}, budget := #{bytes := Max}} = Ws,
            Out = #{working_set => WsId, slices => Slices,
                    usage => #{bytes => Used, budget_bytes => Max}},
            {ok, barrel_ctx_explain:materialize(Out, names(Ids))};
        {error, _} = Err ->
            wrap(Err)
    end.

%% @doc Import an exported generation (directory `Dir') into this node,
%% read only, and attach it as a snapshot member (`new' creates the
%% working set). Options: `name' (local database name). Errors:
%% `invalid_snapshot', `already_exists', `over_budget',
%% `unknown_working_set'.
-spec import(ws_ref(), file:filename(), map()) -> {ok, ws_view()} | error().
import(WsRef, Dir, Opts) ->
    with_ws(WsRef, fun(WsId) ->
        ws_result(WsId, barrel_ctx_ws:import_snapshot(WsId, Dir, Opts))
    end).

%%====================================================================
%% Offline mode
%%====================================================================

%% @doc Whether this node answers from local copies only.
-spec offline() -> boolean().
offline() ->
    application:get_env(barrel, ctx_offline, false).

%% @doc Switch offline mode: remote members are then reported
%% `skipped_offline', never contacted and never downloaded.
-spec set_offline(boolean()) -> ok.
set_offline(Flag) when is_boolean(Flag) ->
    application:set_env(barrel, ctx_offline, Flag).

%%====================================================================
%% Internal: working sets
%%====================================================================

with_ws(new, Fun) ->
    case create_ws(#{}) of
        {ok, WsId} -> Fun(WsId);
        {error, _} = Err -> Err
    end;
with_ws(WsId, Fun) when is_binary(WsId) ->
    case barrel_ctx_ws:get(WsId) of
        {ok, _} -> Fun(WsId);
        {error, not_found} -> {error, unknown_ws(WsId)}
    end.

ws_result(WsId, {ok, _Ws}) -> get_ws(WsId);
ws_result(WsId, {error, not_found}) -> {error, unknown_ws(WsId)};
ws_result(_WsId, {error, _} = Err) -> wrap(Err).

unknown_ws(WsId) ->
    {unknown_working_set, #{working_set => WsId,
                            known => lists:sublist(barrel_ctx_ws:list(), 10)}}.

%% Stored working set plus, per member, what it can answer.
view(#{id := WsId} = Ws) ->
    Members = case barrel_ctx_ws:members(WsId) of
        {ok, Resolved} ->
            Names = names([C || #{context := C} <- Resolved]),
            [member_view(M, Names) || M <- Resolved];
        {error, _} ->
            []
    end,
    barrel_ctx_explain:working_set((maps:without([members], Ws))#{
                                     members => Members}).

member_view(#{context := C, mode := Mode, coverage := Cov} = M, Names) ->
    Base = maps:without([open_opts, db_name, predicate, coverage,
                         available], M),
    Base#{name => maps:get(C, Names, null),
          membership => Cov,
          answers_offline => Mode =/= remote andalso
                                 maps:get(available, M, false)}.

brief_ws(WsId) ->
    case barrel_ctx_ws:get(WsId) of
        {ok, #{members := Ms, usage := Usage} = Ws} ->
            Names = names([C || #{context := C} <- Ms]),
            #{id => WsId, owner => maps:get(owner, Ws),
              created_at => maps:get(created_at, Ws), usage => Usage,
              members => [#{context => C, name => maps:get(C, Names, null),
                            mode => Mode}
                          || #{context := C, mode := Mode} <- Ms]};
        {error, _} ->
            #{id => WsId}
    end.

check_budget(B) when is_map(B) ->
    Keys = [bytes, contexts, transfer_bytes, remote_parallel, deadline_ms,
            open_dbs],
    maps:fold(
        fun(_K, _V, {error, _} = Err) ->
                Err;
           (K, V, {ok, Acc}) ->
                case budget_key(K, Keys) of
                    {ok, A} when is_integer(V), V >= 0 -> {ok, Acc#{A => V}};
                    _ -> {error, {invalid_argument,
                                  #{field => <<"budget.", (text(K))/binary>>,
                                    expected => <<"a non-negative integer, "
                                                  "keys: bytes, contexts, "
                                                  "transfer_bytes, "
                                                  "remote_parallel, "
                                                  "deadline_ms">>}}}
                end
        end, {ok, #{}}, B);
check_budget(_B) ->
    {error, {invalid_argument, #{field => budget,
                                 expected => <<"an object">>}}}.

budget_key(K, Keys) when is_atom(K) ->
    case lists:member(K, Keys) of
        true -> {ok, K};
        false -> error
    end;
budget_key(K, Keys) when is_binary(K) ->
    case [A || A <- Keys, atom_to_binary(A) =:= K] of
        [A] -> {ok, A};
        [] -> error
    end.

%% Remote-only contexts of a materialize while offline.
offline_blocks(Req, Ids) ->
    case maps:get(offline, Req, offline()) of
        true ->
            [Id || Id <- Ids,
                   {ok, #{<<"locations">> := Locs}} <-
                       [barrel_ctx_catalog:get(Id)],
                   [] =:= [L || #{<<"kind">> := <<"local">>} = L <- Locs]];
        _ ->
            []
    end.

member_for(Ctx, Locs, Opts) ->
    Local = [L || #{<<"kind">> := <<"local">>} = L <- Locs],
    Remote = [L || #{<<"kind">> := <<"remote">>} = L <- Locs],
    case {maps:get(mode, Opts, default), Local, Remote} of
        {Mode, [#{<<"db">> := Db} | _], _} when Mode =:= local;
                                               Mode =:= default ->
            {ok, #{context => Ctx, mode => local, local_db => Db}};
        {Mode, _, [R | _]} when Mode =:= remote; Mode =:= default ->
            {ok, remote_member(Ctx, R, Opts)};
        {Mode, _, _} ->
            {error, {no_location, Mode}}
    end.

remote_member(Ctx, #{<<"endpoint">> := E, <<"db">> := Db} = R, Opts) ->
    Base = #{context => Ctx, mode => remote,
             location => #{endpoint => E, db => Db}},
    case maps:get(credential_ref, Opts,
                  maps:get(<<"credential_ref">>, R, undefined)) of
        Ref when is_binary(Ref) -> Base#{credential_ref => Ref};
        _ -> Base
    end.

%% One slice per source that answered with ids; others report why not.
slice(WsId, #{context := Ctx, status := ok, location := Loc} = Source,
      Groups, Req) ->
    Ids = [Id || #{context := C, rows := Rows} <- Groups, C =:= Ctx,
                 #{<<"id">> := Id} <- Rows, is_binary(Id)],
    case Ids of
        [] ->
            #{context => Ctx, status => empty};
        _ ->
            SReq = maps:merge(
                     #{context => Ctx, source => slice_source(Ctx, Loc),
                       ids => Ids, observed => observed(Source)},
                     source_opts(Req)),
            case barrel_ctx_slice:materialize(WsId, SReq) of
                {ok, #{slices := [Slice]}} -> Slice;
                {error, Reason} ->
                    #{context => Ctx, status => failed, error => Reason}
            end
    end;
slice(_WsId, #{context := Ctx, status := Status} = Source, _Groups, _Req) ->
    #{context => Ctx, status => Status,
      error => maps:get(error, Source, #{})}.

source_opts(#{open_opts := Open} = Req) ->
    (source_opts(maps:remove(open_opts, Req)))#{source_open_opts => Open};
source_opts(Req) ->
    maps:with([include, max_bytes, embedding, timeout], Req).

slice_source(_Ctx, #{kind := local, db := Db}) ->
    {local, Db};
slice_source(Ctx, #{kind := remote, endpoint := E, db := Db}) ->
    Base = #{endpoint => E, db => Db},
    {remote, case credential_ref(Ctx) of
                 undefined -> Base;
                 Ref -> Base#{credential_ref => Ref}
             end}.

credential_ref(Ctx) ->
    case barrel_ctx_catalog:get(Ctx) of
        {ok, #{<<"locations">> := Locs}} ->
            case [R || #{<<"kind">> := <<"remote">>,
                         <<"credential_ref">> := R} <- Locs] of
                [Ref | _] -> Ref;
                [] -> undefined
            end;
        _ ->
            undefined
    end.

observed(#{version := #{observed := #{instance_id := Iid,
                                      last_seq := Seq}}}) ->
    #{<<"instance_id">> => Iid, <<"last_seq">> => Seq};
observed(_Source) ->
    #{<<"instance_id">> => null, <<"last_seq">> => null}.

%%====================================================================
%% Internal: names and errors
%%====================================================================

names_of(#{sources := Sources}) ->
    names([C || #{context := C} <- Sources]).

names(Ids) ->
    maps:from_list([{Id, card_name(Id)} || Id <- Ids]).

card_name(Id) ->
    case barrel_ctx_catalog:get(Id) of
        {ok, #{<<"name">> := N}} -> N;
        _ -> null
    end.

brief(#{<<"id">> := Id} = C) ->
    maps:with([<<"id">>, <<"name">>, <<"title">>], C#{<<"id">> => Id}).

%% Close names: a substring either way, or a small edit distance.
suggestions(Ref, Cards) ->
    Low = string:lowercase(Ref),
    Scored = [{distance(Low, string:lowercase(N)), C}
              || #{<<"name">> := N} = C <- Cards,
                 close(Low, string:lowercase(N))],
    [#{id => Id, name => N}
     || {_, #{<<"id">> := Id, <<"name">> := N}} <-
            lists:sublist(lists:keysort(1, Scored), 5)].

close(A, B) ->
    string:find(B, A) =/= nomatch orelse string:find(A, B) =/= nomatch
        orelse distance(A, B) =< max(2, byte_size(A) div 3).

%% Levenshtein distance over bytes.
distance(A, B) ->
    First = lists:seq(0, byte_size(B)),
    lists:last(lists:foldl(
        fun({I, Ca}, Prev) -> row(Ca, binary_to_list(B), Prev, [I]) end,
        First, lists:zip(lists:seq(1, byte_size(A)), binary_to_list(A)))).

row(_Ca, [], _Prev, Acc) ->
    lists:reverse(Acc);
row(Ca, [Cb | Bs], [Diag, Up | Prev], [Left | _] = Acc) ->
    Cost = case Ca =:= Cb of true -> 0; false -> 1 end,
    row(Ca, Bs, [Up | Prev], [min(min(Left + 1, Up + 1), Diag + Cost) | Acc]).

ok_or_error({ok, _} = Ok, _Ref) -> Ok;
ok_or_error({error, not_found}, Ref) ->
    {error, {unknown_context, #{context => Ref, suggestions => []}}};
ok_or_error({error, _} = Err, _Ref) -> wrap(Err).

wrap(ok) -> ok;
wrap({ok, _} = Ok) -> Ok;
wrap({error, Reason}) -> {error, barrel_ctx_error:normalize(Reason)}.

text(A) when is_atom(A) -> atom_to_binary(A);
text(B) when is_binary(B) -> B.

env(Key, Default) ->
    application:get_env(barrel, Key, Default).
