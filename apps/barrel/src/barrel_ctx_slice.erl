%%%-------------------------------------------------------------------
%%% @doc Retrieved-set slices (see docs/architecture/contexts.md): an explicit id
%%% list fetched from one source context (a local database through
%%% barrel:get_docs, or a remote Barrel server through _bulk_get) and
%%% frozen into its own local database, one per source context and
%%% working set.
%%%
%%% The size is known from the fetch and checked against the request's
%%% `max_bytes', the working set's transfer budget, and its remaining
%%% bytes before anything is written. The slice is built in a temporary
%%% directory and renamed into place, then attached, only when
%%% complete; its provenance (source context, observed instance id and
%%% last seq, hash of the ids) is stored in the slice (local doc
%%% `_ctx/slice' and a SLICE record) and in the working-set member.
%%% Slices open read only.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ctx_slice).

-export([materialize/2, open_opts/1, provenance/1, remove/1, slices_dir/0,
         slice_name/2]).

-define(RECORD, "SLICE").
-define(TMP_PREFIX, ".tmp-").
-define(PROV_DOC, <<"_ctx/slice">>).
-define(BATCH, 500).

%% @doc Materialize `Request' into working set `WsId'.
%%
%% Request: `context' (source context id), `source' (`{local, DbName}',
%% `{local, Handle}' or `{remote, #{endpoint, db, credential_ref}}'),
%% `ids', optional
%% `include' (`#{embeddings => boolean()}', default true), `max_bytes',
%% `embedding' (record policy for a remote record-mode source),
%% `source_open_opts' (to open a closed local source), `timeout'.
-spec materialize(binary(), map()) -> {ok, map()} | {error, term()}.
materialize(WsId, #{context := Ctx, source := Source, ids := Ids} = Req)
        when is_binary(Ctx), is_list(Ids) ->
    case barrel_ctx_ws:get(WsId) of
        {ok, #{members := Ms} = Ws} ->
            case [M || #{context := C} = M <- Ms, C =:= Ctx] of
                [] -> fetch_and_build(Ws, Source, Req);
                [_] -> {error, {already_attached, Ctx}}
            end;
        {error, _} = Err ->
            Err
    end;
materialize(_WsId, Req) ->
    {error, {invalid_request, Req}}.

fetch_and_build(#{id := WsId} = Ws, Source, Req) ->
    case include_attachments(Req) of
        true ->
            {error, {unsupported, attachments}};
        false ->
            case fetch(Source, Req) of
                {ok, Fetched} ->
                    case check_budget(Ws, Req, maps:get(bytes, Fetched)) of
                        ok -> build(WsId, Req, Fetched);
                        {error, _} = Err -> Err
                    end;
                {error, _} = Err ->
                    Err
            end
    end.

include_attachments(Req) ->
    maps:get(attachments, maps:get(include, Req, #{}), false) =:= true.

embeddings(Req) ->
    maps:get(embeddings, maps:get(include, Req, #{}), true).

%%====================================================================
%% Fetch
%%====================================================================

fetch({local, #{docdb := _} = Db}, Req) ->
    fetch_local(Db, Req);
fetch({local, DbName}, Req) ->
    case barrel_dbs:ensure(DbName, maps:get(source_open_opts, Req, #{})) of
        {ok, Db} -> fetch_local(Db, Req);
        {error, Reason} -> {error, {source_unavailable, Reason}}
    end;
fetch({remote, Loc}, #{ids := Ids} = Req) ->
    Opts = #{include_embedding => embeddings(Req),
             timeout => maps:get(timeout, Req, 4000),
             max_bytes => maps:get(max_bytes, Req, 16777216)},
    case barrel_ctx_remote:bulk_get(Loc, Ids, Opts) of
        {ok, Results, Bytes} ->
            {Found, Missing} = split(Ids, Results),
            {ok, #{found => Found, missing => Missing, vectors => [],
                   policy => remote_policy(Req),
                   dimensions => remote_dimensions(Found),
                   observed => observe_remote(Loc, Req, Opts),
                   source => #{<<"kind">> => <<"remote">>,
                               <<"endpoint">> => maps:get(endpoint, Loc),
                               <<"db">> => maps:get(db, Loc)},
                   bytes => Bytes}};
        {error, #{reason := response_too_large}} when
              is_map_key(max_bytes, Req) ->
            {error, {over_budget, max_bytes,
                     #{limit => maps:get(max_bytes, Req)}}};
        {error, Failure} ->
            {error, {source_unavailable, failure_reason(Failure)}}
    end.

%% Remote failures keep their status for timeouts, else their reason.
failure_reason(#{status := timeout}) -> timeout;
failure_reason(#{status := skipped_budget}) -> node_budget;
failure_reason(#{reason := Reason}) -> Reason.

fetch_local(Db, #{ids := Ids} = Req) ->
    Results = barrel:get_docs(Db, Ids, #{include_embedding =>
                                             embeddings(Req)}),
    {Found, Missing} = split(Ids, Results),
    Vectors = plain_vectors(Db, Found, embeddings(Req)),
    {ok, #{found => Found, missing => Missing, vectors => Vectors,
           policy => maps:get(embedding, Db, undefined),
           dimensions => dimensions(Db),
           observed => observe_local(Db),
           source => #{<<"kind">> => <<"local">>,
                       <<"db">> => maps:get(name, Db)},
           bytes => payload_bytes(Found, Vectors)}}.

split(Ids, Results) ->
    Pairs = lists:zip(Ids, Results),
    {[{Id, Doc} || {Id, {ok, Doc}} <- Pairs],
     [Id || {Id, R} <- Pairs, element(1, R) =:= error]}.

%% A plain database keeps vectors (and their text) only in its vector
%% store; a record database carries them in the documents.
plain_vectors(#{embedding := _}, _Found, _Want) ->
    [];
plain_vectors(_Db, _Found, false) ->
    [];
plain_vectors(Db, Found, true) ->
    lists:filtermap(
        fun({Id, _Doc}) ->
            case barrel:vector_get(Db, Id) of
                {ok, #{vector := V, text := T, metadata := M}} ->
                    {true, {Id, T, M, V}};
                _ ->
                    false
            end
        end, Found).

dimensions(#{dimensions := D}) -> D;
dimensions(#{vstore := VStore}) ->
    {ok, #{dimension := D}} = barrel_vectordb:stats(VStore),
    D.

remote_policy(#{embedding := P}) when is_map(P) ->
    {ok, Policy} = barrel_embedding_policy:validate(P),
    Policy;
remote_policy(_Req) ->
    undefined.

remote_dimensions(Found) ->
    case [length(V) || {_, #{<<"_embedding">> := #{<<"vector">> := V}}}
                           <- Found] of
        [D | _] -> D;
        [] -> undefined
    end.

observe_local(#{docdb := DbBin}) ->
    {ok, Iid} = barrel_docdb:db_instance_id(DbBin),
    {ok, Pid} = barrel_docdb:db_pid(DbBin),
    {ok, StoreRef} = barrel_db_server:get_store_ref(Pid),
    #{<<"instance_id">> => Iid,
      <<"last_seq">> => b64(barrel_changes:get_last_seq(StoreRef, DbBin))}.

%% The caller may pass what its query observed; else ask the source.
observe_remote(_Loc, #{observed := #{} = Observed}, _Opts) ->
    Observed;
observe_remote(Loc, _Req, Opts) ->
    barrel_ctx_remote:observe(Loc, maps:with([timeout], Opts)).

payload_bytes(Found, Vectors) ->
    lists:sum([byte_size(iolist_to_binary(json:encode(Doc)))
               || {_Id, Doc} <- Found]) +
        lists:sum([byte_size(T) + 4 * length(V) + byte_size(term_to_binary(M))
                   || {_Id, T, M, V} <- Vectors]).

check_budget(#{budget := #{transfer_bytes := Transfer}} = Ws, Req, Bytes) ->
    Limit = min(Transfer, maps:get(max_bytes, Req, Transfer)),
    Avail = barrel_ctx_ws:remaining_bytes(Ws),
    if
        Bytes > Limit ->
            {error, {over_budget, transfer_bytes,
                     #{needed => Bytes, limit => Limit}}};
        Bytes > Avail ->
            {error, {over_budget, bytes,
                     #{needed => Bytes, available => Avail}}};
        true ->
            ok
    end.

%%====================================================================
%% Build
%%====================================================================

build(WsId, #{context := Ctx, ids := Ids}, Fetched) ->
    Name = slice_name(WsId, Ctx),
    Tmp = filename:join(slices_dir(), ?TMP_PREFIX ++ binary_to_list(Name)),
    Final = filename:join(slices_dir(), binary_to_list(Name)),
    Derived = derived(Ctx, Ids, Fetched),
    case barrel_dbs:hold(Name, #{owner => {ctx_slice, WsId}}) of
        {ok, #{was_open := false}} ->
            try
                _ = file:del_dir_r(Tmp),
                ok = filelib:ensure_path(Tmp),
                Rec = write_slice(Name, Tmp, Fetched, Derived),
                ok = file:write_file(filename:join(Tmp, ?RECORD),
                                     io_lib:format("~p.~n", [Rec]), [sync]),
                _ = file:del_dir_r(Final),
                ok = file:rename(Tmp, Final),
                attach(WsId, Ctx, Name, Derived, Fetched)
            catch
                throw:{slice_error, Reason} ->
                    _ = file:del_dir_r(Tmp),
                    {error, Reason}
            after
                barrel_dbs:unhold(Name)
            end;
        {ok, #{was_open := true}} ->
            barrel_dbs:unhold(Name),
            {error, {slice_open, Name}};
        {error, _} = Err ->
            Err
    end.

write_slice(Name, Dir, Fetched, Derived) ->
    #{found := Found, vectors := Vectors, policy := Policy,
      dimensions := Dim} = Fetched,
    Mode = mode(Policy, Dim),
    VecCfg = vector_config(Dim),
    Opts0 = #{docdb => #{data_dir => filename:join(Dir, "docdb"),
                         store_opts => #{write_buffer_size => 4194304},
                         att_opts => #{backend => none}},
              vectordb => VecCfg#{db_path => filename:join(Dir, "vectordb")}},
    Opts = case Mode of
        record -> Opts0#{embedding => maps:remove(dimensions,
                                                  Policy#{mode => sync})};
        _ -> Opts0
    end,
    case barrel:open(Name, Opts) of
        {ok, Db} ->
            try
                ok = put_all(Db, [clean(Doc) || {_Id, Doc} <- Found]),
                ok = add_vectors(Db, Vectors),
                ok = barrel_docdb:put_local_doc(Name, ?PROV_DOC, Derived)
            after
                barrel:close(Db)
            end,
            #{name => Name, mode => Mode, vector_config => VecCfg,
              derived => Derived};
        {error, Reason} ->
            throw({slice_error, {open_failed, Reason}})
    end.

mode(Policy, _Dim) when is_map(Policy) -> record;
mode(undefined, undefined) -> docs_only;
mode(undefined, _Dim) -> plain.

%% Slices are small: a memory BM25 index, rebuilt from the stored text at
%% each open, avoids a second RocksDB instance per slice.
vector_config(undefined) -> #{dimension => 1, bm25_backend => memory};
vector_config(Dim) -> #{dimension => Dim, bm25_backend => memory}.

put_all(_Db, []) ->
    ok;
put_all(Db, Docs) ->
    {Batch, Rest} = lists:split(min(?BATCH, length(Docs)), Docs),
    Results = barrel:put_docs(Db, Batch),
    case [R || R <- Results, element(1, R) =/= ok] of
        [] -> put_all(Db, Rest);
        [Err | _] -> throw({slice_error, {write_failed, Err}})
    end.

add_vectors(_Db, []) ->
    ok;
add_vectors(Db, Vectors) ->
    case barrel:vector_add_batch(Db, Vectors) of
        {ok, _} -> ok;
        {error, Reason} -> throw({slice_error, {vector_write_failed, Reason}})
    end.

%% Source metadata is dropped (the slice mints its own versions); a
%% carried embedding becomes a client vector so it indexes as is.
clean(Doc) ->
    maps:fold(fun(<<"_embedding">>, #{<<"vector">> := V}, Acc) ->
                      Acc#{<<"_embedding">> => V};
                 (<<"_", _/binary>>, _, Acc) ->
                      Acc;
                 (K, V, Acc) ->
                      Acc#{K => V}
              end, #{}, Doc).

derived(Ctx, Ids, #{found := Found, missing := Missing, observed := Obs,
                    source := Source, bytes := Bytes}) ->
    Sorted = lists:usort(Ids),
    Hash = crypto:hash(sha256, lists:join(<<"\n">>, Sorted)),
    #{<<"from">> => Ctx, <<"observed">> => Obs,
      <<"selection">> => <<"ids">>,
      <<"ids_hash">> => <<"sha256:", (binary:encode_hex(Hash,
                                                        lowercase))/binary>>,
      <<"docs">> => length(Found), <<"missing">> => Missing,
      <<"source">> => Source, <<"bytes">> => Bytes,
      <<"created_at">> => list_to_binary(
                            calendar:system_time_to_rfc3339(
                              erlang:system_time(second), [{offset, "Z"}]))}.

attach(WsId, Ctx, Name, Derived, #{bytes := Bytes, found := Found}) ->
    Member = #{context => Ctx, mode => retrieved_set, local_db => Name,
               derived => Derived, bytes => Bytes},
    case barrel_ctx_ws:attach(WsId, Member) of
        {ok, #{usage := #{bytes := Used}, budget := #{bytes := Max}}} ->
            {ok, #{working_set => WsId,
                   slices => [#{context => Ctx, local_db => Name,
                                docs => length(Found), bytes => Bytes,
                                status => complete, derived => Derived}],
                   usage => #{bytes => Used, budget_bytes => Max}}};
        {error, _} = Err ->
            ok = remove(Name),
            Err
    end.

%%====================================================================
%% Slices on disk
%%====================================================================

%% @doc One slice per working set and source context.
-spec slice_name(binary(), binary()) -> binary().
slice_name(WsId, Ctx) ->
    <<"wslice_", (short(WsId, <<"ws_">>))/binary, "_",
      (short(Ctx, <<"ctx_">>))/binary>>.

short(Id, Prefix) ->
    Rest = case Id of
        <<Prefix:(byte_size(Prefix))/binary, R/binary>> -> R;
        _ -> Id
    end,
    Clean = << <<C>> || <<C>> <= string:lowercase(Rest),
                        (C >= $a andalso C =< $z) orelse
                        (C >= $0 andalso C =< $9) >>,
    binary:part(Clean, 0, min(8, byte_size(Clean))).

%% @doc barrel:open options for a completed slice (read only).
-spec open_opts(binary()) -> {ok, map()} | {error, not_materialized}.
open_opts(Name) ->
    Dir = filename:join(slices_dir(), binary_to_list(Name)),
    case file:consult(filename:join(Dir, ?RECORD)) of
        {ok, [#{mode := Mode, vector_config := VecCfg}]} ->
            Base = #{read_only => true,
                     docdb => #{data_dir => filename:join(Dir, "docdb"),
                                att_opts => #{backend => none}},
                     vectordb => VecCfg#{db_path => filename:join(Dir,
                                                                  "vectordb")}},
            {ok, case Mode of
                     record -> Base#{embedding => stored};
                     _ -> Base
                 end};
        _ ->
            {error, not_materialized}
    end.

%% @doc The provenance recorded in a completed slice.
-spec provenance(binary()) -> {ok, map()} | {error, not_materialized}.
provenance(Name) ->
    Dir = filename:join(slices_dir(), binary_to_list(Name)),
    case file:consult(filename:join(Dir, ?RECORD)) of
        {ok, [#{derived := D}]} -> {ok, D};
        _ -> {error, not_materialized}
    end.

%% @doc Close and delete a slice. Idempotent.
-spec remove(binary()) -> ok.
remove(Name) ->
    ok = barrel_dbs:close(Name),
    _ = file:del_dir_r(filename:join(slices_dir(), binary_to_list(Name))),
    ok.

-spec slices_dir() -> file:filename().
slices_dir() ->
    filename:join(filename:dirname(barrel_ctx_export:imports_dir()),
                  "slices").

b64(Bin) ->
    base64:encode(Bin, #{mode => urlsafe, padding => false}).
