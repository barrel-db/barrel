%%%-------------------------------------------------------------------
%%% @doc Attachment store dispatcher.
%%%
%%% Selects an attachment backend (a {@link barrel_att_backend}) per database
%%% and routes all attachment calls to it. The backend is chosen from
%%% `att_opts.backend' at {@link open/2} (default `blob', the RocksDB BlobDB
%%% backend; `none' for a database that structurally never stores
%%% attachments -- see {@link barrel_att_store_none}) via
%%% {@link backend_module/1}, resolved to a module and tagged
%%% into the returned `att_ref'. Streaming handles embed their `att_ref', so
%%% streaming calls dispatch to the same backend.
%%%
%%% Backends can be optional sibling apps (e.g. `barrel_att_s3`, kept out of
%%% the default embeddable build so it doesn't pull in `livery_s3'/`livery');
%%% {@link is_available/1} probes for one at runtime via `code:ensure_loaded/1',
%%% the same pattern `barrel_vectordb_index' uses for the optional FAISS
%%% backend, so `open/2' fails cleanly with `{error, {backend_unavailable, _}}'
%%% rather than crashing on an unloaded module.
%%%
%%% Callers (barrel_att, barrel_docdb, barrel_db_server) keep using this module;
%%% the backend split is transparent to them. This dispatcher is also the
%%% single keyspace choke point: every DbName resolves here, so backends
%%% always build blob and att-feed keys with the keyspace.
%%%
%%% An `att_ref' handed out by barrel_docdb carries `db' (the logical
%%% database name, its writer pid and epoch). Backends with staged writes
%%% (`commit/3') then commit inside that writer, never the keyspace's,
%%% and streams answer `{error, db_restarted}' once that writer is gone.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_att_store).

%% API
-export([open/2, close/1]).
-export([backend_module/1, is_available/1]).
-export([put/5, put/6, get/4, delete/4]).
-export([delete_all/3]).
-export([fold/5]).
-export([att_count/2, count_atts/2]).

%% Streaming API
-export([put_stream/5, put_stream/6]).
-export([write_chunk/2, finish_stream/1, abort_stream/1]).
-export([get_stream/4, get_stream/5, read_chunk/1, close_stream/1]).
-export([get_info/4]).

%% Sync support (optional backend callbacks; see barrel_att_backend)
-export([delete/5, att_changes/4, att_floor/2, att_last_hlc/2,
         sweep_att_feed/3,
         rebuild_feed/2, supports_sync/1]).
-export([att_state/4, att_list/3, touch/5, supports_touch/1,
         flush/1, supports_flush/1]).
-export([checkpoint/2]).
-export([destroy/2]).
-export([gc_uploads/3]).

-export_type([att_ref/0, att_stream/0]).

-define(DEFAULT_BACKEND, blob).

-type att_ref() :: #{backend => module(), _ => _}.
-type att_stream() :: #{att_ref := att_ref(), _ => _}.

%%====================================================================
%% API
%%====================================================================

%% @doc Resolve a symbolic backend name to its implementation module.
%% A bare module atom (not `blob'/`s3') passes through unchanged, for
%% back-compat with any caller that already names a module directly.
-spec backend_module(atom()) -> module().
backend_module(blob) -> barrel_att_store_blob;
backend_module(s3) -> barrel_att_s3_store;
backend_module(none) -> barrel_att_store_none;
backend_module(Module) when is_atom(Module) -> Module.

%% @doc Whether a backend's implementation is present in the build.
%% `blob' ships with barrel_docdb itself; other backends are optional
%% sibling apps (opted into the build via their own rebar profile) probed
%% for at runtime, same pattern as `barrel_vectordb_index:is_available/1'
%% for the optional FAISS backend.
-spec is_available(atom()) -> boolean().
is_available(blob) ->
    true;
is_available(none) ->
    true;
is_available(s3) ->
    backend_loaded(barrel_att_s3_store);
is_available(Module) when is_atom(Module) ->
    true.

%% @private A backend module's load state and exports never change once
%% the node is up, so both are cached in a persistent_term after the
%% first check -- code:ensure_loaded/1 and erlang:function_exported/3
%% each cost a round trip through the code server; paying that on every
%% open/checkpoint/destroy/supports_sync call is pure waste.
backend_loaded(Module) ->
    persistent_term_cached({?MODULE, loaded, Module}, fun() ->
        case code:ensure_loaded(Module) of
            {module, _} -> true;
            {error, _} -> false
        end
    end).

exported(Module, Function, Arity) ->
    persistent_term_cached({?MODULE, exported, Module, Function, Arity}, fun() ->
        backend_loaded(Module) andalso erlang:function_exported(Module, Function, Arity)
    end).

persistent_term_cached(Key, ComputeFun) ->
    case persistent_term:get(Key, undefined) of
        undefined ->
            Result = ComputeFun(),
            persistent_term:put(Key, Result),
            Result;
        Result ->
            Result
    end.

-spec open(string(), map()) -> {ok, att_ref()} | {error, term()}.
open(Path, Options) ->
    Backend0 = maps:get(backend, Options, ?DEFAULT_BACKEND),
    case is_available(Backend0) of
        false ->
            {error, {backend_unavailable, Backend0}};
        true ->
            Backend = backend_module(Backend0),
            case Backend:open(Path, Options) of
                {ok, AttRef} -> {ok, AttRef#{backend => Backend}};
                {error, _} = Err -> Err
            end
    end.

-spec close(att_ref()) -> ok.
close(AttRef) ->
    B = backend(AttRef),
    B:close(AttRef).

-spec put(att_ref(), binary(), binary(), binary(), binary()) ->
    {ok, map()} | {error, term()}.
put(AttRef, DbName, DocId, AttName, Data) ->
    put(AttRef, DbName, DocId, AttName, Data, #{}).

-spec put(att_ref(), binary(), binary(), binary(), binary(), map()) ->
    {ok, map()} | {error, term()}.
put(AttRef, DbName, DocId, AttName, Data, Opts) ->
    B = backend(AttRef),
    Ks = barrel_keyspace:resolve(DbName),
    case staged(AttRef) of
        true ->
            commit_staged(AttRef, B:stage_put(AttRef, Ks, DocId, AttName,
                                              Data, Opts));
        false ->
            B:put(AttRef, Ks, DocId, AttName, Data, Opts)
    end.

-spec get(att_ref(), binary(), binary(), binary()) ->
    {ok, binary()} | not_found | {error, term()}.
get(AttRef, DbName, DocId, AttName) ->
    B = backend(AttRef),
    B:get(AttRef, barrel_keyspace:resolve(DbName), DocId, AttName).

-spec delete(att_ref(), binary(), binary(), binary()) -> ok | {error, term()}.
delete(AttRef, DbName, DocId, AttName) ->
    case staged(AttRef) of
        true -> delete(AttRef, DbName, DocId, AttName, #{});
        false ->
            B = backend(AttRef),
            B:delete(AttRef, barrel_keyspace:resolve(DbName), DocId, AttName)
    end.

-spec delete_all(att_ref(), binary(), binary()) -> ok | {error, term()}.
delete_all(AttRef, DbName, DocId) ->
    case staged(AttRef) of
        true ->
            Names = fold(AttRef, DbName, DocId,
                         fun(Name, _Data, Acc) -> {ok, [Name | Acc]} end, []),
            lists:foreach(fun(Name) ->
                              _ = delete(AttRef, DbName, DocId, Name, #{})
                          end, Names);
        false ->
            B = backend(AttRef),
            B:delete_all(AttRef, barrel_keyspace:resolve(DbName), DocId)
    end.

-spec fold(att_ref(), binary(), binary(), fun(), term()) -> term().
fold(AttRef, DbName, DocId, Fun, Acc) ->
    B = backend(AttRef),
    B:fold(AttRef, barrel_keyspace:resolve(DbName), DocId, Fun, Acc).

-spec get_info(att_ref(), binary(), binary(), binary()) ->
    {ok, map()} | {error, term()}.
get_info(AttRef, DbName, DocId, AttName) ->
    B = backend(AttRef),
    B:get_info(AttRef, barrel_keyspace:resolve(DbName), DocId, AttName).

%%====================================================================
%% Streaming API
%%====================================================================

-spec put_stream(att_ref(), binary(), binary(), binary(), binary()) ->
    {ok, att_stream()} | {error, term()}.
put_stream(AttRef, DbName, DocId, AttName, ContentType) ->
    B = backend(AttRef),
    B:put_stream(AttRef, barrel_keyspace:resolve(DbName), DocId, AttName,
                 ContentType).

-spec put_stream(att_ref(), binary(), binary(), binary(), binary(), map()) ->
    {ok, att_stream()} | {error, term()}.
put_stream(AttRef, DbName, DocId, AttName, ContentType, Opts) ->
    B = backend(AttRef),
    B:put_stream(AttRef, barrel_keyspace:resolve(DbName), DocId, AttName,
                 ContentType, Opts).

-spec write_chunk(att_stream(), binary()) -> {ok, att_stream()} | {error, term()}.
write_chunk(Stream, Data) ->
    B = stream_backend(Stream),
    guard(Stream, fun() -> B:write_chunk(Stream, Data) end).

-spec finish_stream(att_stream()) ->
    {ok, map()} | {ok, ignored} | {error, term()}.
finish_stream(#{att_ref := AttRef} = Stream) ->
    B = stream_backend(Stream),
    case staged(AttRef) of
        true ->
            guard(Stream, fun() ->
                commit_staged(AttRef, B:stage_finish(Stream))
            end);
        false ->
            guard(Stream, fun() -> B:finish_stream(Stream) end)
    end;
finish_stream(Stream) ->
    B = stream_backend(Stream),
    B:finish_stream(Stream).

-spec abort_stream(att_stream()) -> ok.
abort_stream(Stream) ->
    B = stream_backend(Stream),
    case guard(Stream, fun() -> B:abort_stream(Stream) end) of
        ok -> ok;
        {error, db_restarted} -> ok   % its store is closed
    end.

-spec get_stream(att_ref(), binary(), binary(), binary()) ->
    {ok, att_stream()} | {error, term()}.
get_stream(AttRef, DbName, DocId, AttName) ->
    B = backend(AttRef),
    B:get_stream(AttRef, barrel_keyspace:resolve(DbName), DocId,
                 AttName).

%% @doc Open a read stream over a byte range of an attachment.
%% `offset' (default 0) is where reading starts and `length' (default
%% `infinity') caps the bytes returned. Backends exporting
%% `seek_stream/2' jump straight to the right chunk; others are read
%% from the start and the leading bytes dropped.
-spec get_stream(att_ref(), binary(), binary(), binary(), map()) ->
    {ok, att_stream()} | {error, term()}.
get_stream(AttRef, DbName, DocId, AttName, Opts) ->
    Offset = maps:get(offset, Opts, 0),
    Length = maps:get(length, Opts, infinity),
    case get_stream(AttRef, DbName, DocId, AttName) of
        {ok, Stream} ->
            B = stream_backend(Stream),
            _ = code:ensure_loaded(B),
            {ok, Stream1, Skip} =
                case Offset > 0 andalso erlang:function_exported(B, seek_stream, 2) of
                    true -> B:seek_stream(Stream, Offset);
                    false -> {ok, Stream, Offset}
                end,
            {ok, Stream1#{range => #{skip => Skip, left => Length}}};
        {error, _} = Error ->
            Error
    end.

-spec read_chunk(att_stream()) -> {ok, binary(), att_stream()} | eof | {error, term()}.
read_chunk(Stream) ->
    case guard(Stream, fun() -> do_read_chunk(Stream) end) of
        {error, db_restarted} = Error ->
            ok = close_stream(Stream),
            Error;
        Result ->
            Result
    end.

do_read_chunk(#{range := #{left := 0}} = Stream) ->
    ok = close_stream(Stream),
    eof;
do_read_chunk(#{range := #{skip := Skip, left := Left}} = Stream) ->
    B = stream_backend(Stream),
    case B:read_chunk(Stream) of
        {ok, Chunk, Stream2} when byte_size(Chunk) =< Skip ->
            do_read_chunk(Stream2#{range => #{skip => Skip - byte_size(Chunk),
                                           left => Left}});
        {ok, Chunk, Stream2} ->
            <<_:Skip/binary, Rest/binary>> = Chunk,
            {Out, Left2} = take_range(Rest, Left),
            {ok, Out, Stream2#{range => #{skip => 0, left => Left2}}};
        Other ->
            Other
    end;
do_read_chunk(Stream) ->
    B = stream_backend(Stream),
    B:read_chunk(Stream).

take_range(Bin, infinity) ->
    {Bin, infinity};
take_range(Bin, Left) when byte_size(Bin) =< Left ->
    {Bin, Left - byte_size(Bin)};
take_range(Bin, Left) ->
    <<Out:Left/binary, _/binary>> = Bin,
    {Out, 0}.

-spec close_stream(att_stream()) -> ok.
close_stream(Stream) ->
    B = stream_backend(Stream),
    try B:close_stream(Stream)
    catch error:badarg -> ok   % its store is closed
    end.

%%====================================================================
%% Internal
%%====================================================================

%% @doc Delete with options (origin_hlc for replicated deletes).
-spec delete(att_ref(), binary(), binary(), binary(), map()) ->
    ok | {error, term()}.
delete(AttRef, DbName, DocId, AttName, Opts) ->
    B = backend(AttRef),
    Ks = barrel_keyspace:resolve(DbName),
    case staged(AttRef) of
        true ->
            commit_staged(AttRef, B:stage_delete(AttRef, Ks, DocId, AttName,
                                                 Opts));
        false ->
            B:delete(AttRef, Ks, DocId, AttName, Opts)
    end.

%% @doc Attachment feed entries since an HLC (exclusive).
-spec att_changes(att_ref(), binary(), term(), map()) ->
    {ok, [map()], term()} | {error, term()}.
att_changes(AttRef, DbName, Since, Opts) ->
    B = backend(AttRef),
    B:att_changes(AttRef, barrel_keyspace:resolve(DbName), Since, Opts).

-spec att_floor(att_ref(), binary()) -> term() | undefined.
att_floor(AttRef, DbName) ->
    B = backend(AttRef),
    B:att_floor(AttRef, barrel_keyspace:resolve(DbName)).

%% @doc HLC of the newest attachment feed row, or min() for a backend
%% without a local feed.
-spec att_last_hlc(att_ref(), binary()) -> barrel_hlc:timestamp().
att_last_hlc(AttRef, DbName) ->
    B = backend(AttRef),
    case exported(B, att_last_hlc, 2) of
        true -> B:att_last_hlc(AttRef, barrel_keyspace:resolve(DbName));
        false -> barrel_hlc:min()
    end.

-spec sweep_att_feed(att_ref(), binary(), term()) ->
    {ok, map()} | {error, term()}.
sweep_att_feed(AttRef, DbName, Cutoff) ->
    B = backend(AttRef),
    B:sweep_att_feed(AttRef, barrel_keyspace:resolve(DbName), Cutoff).

-spec rebuild_feed(att_ref(), binary()) -> {ok, map()} | {error, term()}.
rebuild_feed(AttRef, DbName) ->
    B = backend(AttRef),
    Ks = barrel_keyspace:resolve(DbName),
    case maps:is_key(db, AttRef) andalso exported(B, rebuild_feed, 3) of
        true ->
            in_writer(AttRef, fun(Alloc) -> B:rebuild_feed(AttRef, Ks, Alloc) end);
        false ->
            B:rebuild_feed(AttRef, Ks)
    end.

%% @doc Remove staged uploads older than MaxAgeMs (backends exporting
%% gc_uploads/3; the database writer calls it).
-spec gc_uploads(att_ref(), binary(), non_neg_integer()) ->
    {ok, map()} | {error, term()}.
gc_uploads(AttRef, DbName, MaxAgeMs) ->
    B = backend(AttRef),
    case exported(B, gc_uploads, 3) of
        true -> B:gc_uploads(AttRef, barrel_keyspace:resolve(DbName), MaxAgeMs);
        false -> {ok, #{uploads_removed => 0}}
    end.

%% @doc Current feed index entry of one attachment; `unsupported' for
%% a backend without a feed index.
-spec att_state(att_ref(), binary(), binary(), binary()) ->
    {ok, map()} | not_found | unsupported.
att_state(AttRef, DbName, DocId, AttName) ->
    B = backend(AttRef),
    case exported(B, att_state, 4) of
        true -> B:att_state(AttRef, barrel_keyspace:resolve(DbName), DocId,
                            AttName);
        false -> unsupported
    end.

%% @doc The attachment count of a database: exact once counted,
%% `not_counted' before, `unsupported' for a backend without one.
-spec att_count(att_ref(), binary()) -> {ok, non_neg_integer()} | not_counted | unsupported.
att_count(AttRef, DbName) ->
    B = backend(AttRef),
    case exported(B, att_count, 2) of
        true -> B:att_count(AttRef, barrel_keyspace:resolve(DbName));
        false -> unsupported
    end.

%% @doc Count the attachments of a database written before the counter.
-spec count_atts(att_ref(), binary()) -> ok | {error, term()}.
count_atts(AttRef, DbName) ->
    B = backend(AttRef),
    case exported(B, count_atts, 2) of
        true -> B:count_atts(AttRef, barrel_keyspace:resolve(DbName));
        false -> {error, unsupported}
    end.

%% @doc Current feed index entries of one document's attachments.
-spec att_list(att_ref(), binary(), binary()) ->
    {ok, [map()]} | {error, unsupported}.
att_list(AttRef, DbName, DocId) ->
    B = backend(AttRef),
    case exported(B, att_list, 3) of
        true -> {ok, B:att_list(AttRef, barrel_keyspace:resolve(DbName), DocId)};
        false -> {error, unsupported}
    end.

%% @doc Record a newer origin for bytes already stored (Opts:
%% origin_hlc, digest). {error, bytes_needed} when the stored digest
%% differs, {error, unsupported} for a backend without it.
-spec touch(att_ref(), binary(), binary(), binary(), map()) ->
    {ok, touched} | {ok, ignored} | {error, term()}.
touch(AttRef, DbName, DocId, AttName, Opts) ->
    B = backend(AttRef),
    Ks = barrel_keyspace:resolve(DbName),
    case {staged(AttRef) andalso exported(B, stage_touch, 5),
          exported(B, touch, 5)} of
        {true, _} ->
            commit_staged(AttRef, B:stage_touch(AttRef, Ks, DocId, AttName,
                                                Opts));
        {false, true} ->
            B:touch(AttRef, Ks, DocId, AttName, Opts);
        {false, false} ->
            {error, unsupported}
    end.

-spec supports_touch(att_ref()) -> boolean().
supports_touch(AttRef) ->
    B = backend(AttRef),
    exported(B, touch, 5) orelse exported(B, stage_touch, 5).

%% @doc Make every acknowledged attachment write durable.
%% {error, unsupported} for a backend without it.
-spec flush(att_ref()) -> ok | {error, term()}.
flush(AttRef) ->
    B = backend(AttRef),
    case exported(B, flush, 1) of
        true -> B:flush(AttRef);
        false -> {error, unsupported}
    end.

-spec supports_flush(att_ref()) -> boolean().
supports_flush(AttRef) ->
    exported(backend(AttRef), flush, 1).

%% @doc Whether this database's backend supports attachment sync.
-spec supports_sync(att_ref()) -> boolean().
supports_sync(AttRef) ->
    exported(backend(AttRef), att_changes, 4).

%% @doc Hard-link snapshot of the attachment store into Path
%% (timeline forks). {error, unsupported} for backends without it.
-spec checkpoint(att_ref(), string()) -> ok | {error, term()}.
checkpoint(AttRef, Path) ->
    B = backend(AttRef),
    case exported(B, checkpoint, 2) of
        true -> B:checkpoint(AttRef, Path);
        false -> {error, unsupported}
    end.

%% @doc Erase anything the backend owns beyond its local directory (e.g.
%% S3 objects). {error, unsupported} for backends without it -- callers
%% (delete_db) treat that as nothing extra to do.
-spec destroy(att_ref(), binary()) -> ok | {error, term()}.
destroy(AttRef, DbName) ->
    B = backend(AttRef),
    case exported(B, destroy, 2) of
        true -> B:destroy(AttRef, barrel_keyspace:resolve(DbName));
        false -> {error, unsupported}
    end.

%% Staged writes: the backend splits stage/commit and the ref names
%% the database whose writer commits.
staged(#{db := _} = AttRef) ->
    exported(backend(AttRef), commit, 3);
staged(_AttRef) ->
    false.

commit_staged(AttRef, {commit, Commit}) ->
    B = backend(AttRef),
    in_writer(AttRef, fun(Alloc) -> B:commit(AttRef, Commit, Alloc) end);
commit_staged(_AttRef, Other) ->
    Other.

in_writer(#{db := #{pid := Pid, epoch := Epoch}}, Fun) ->
    barrel_db_server:att_commit(Pid, Epoch, Fun).

%% A stream (or ref) opened through a database answers db_restarted
%% once that database's writer is gone: its store handle is closed.
guard(#{att_ref := #{db := #{pid := Pid, name := Name}}}, Fun) ->
    case current(Pid, Name) of
        false ->
            {error, db_restarted};
        true ->
            try Fun()
            catch error:badarg:Stack ->
                case current(Pid, Name) of
                    true -> erlang:raise(error, badarg, Stack);
                    false -> {error, db_restarted}
                end
            end
    end;
guard(_Stream, Fun) ->
    Fun().

current(Pid, Name) ->
    is_process_alive(Pid)
        andalso persistent_term:get({barrel_db, Name}, undefined) =:= Pid.

backend(#{backend := B}) -> B;
backend(_) -> backend_module(?DEFAULT_BACKEND).

stream_backend(#{att_ref := AttRef}) -> backend(AttRef);
stream_backend(_) -> backend_module(?DEFAULT_BACKEND).
