%%%-------------------------------------------------------------------
%%% @doc Attachment store dispatcher.
%%%
%%% Selects an attachment backend (a {@link barrel_att_backend}) per database
%%% and routes all attachment calls to it. The backend is chosen from
%%% `att_opts.backend' at {@link open/2} (default `blob', the RocksDB BlobDB
%%% backend) via {@link backend_module/1}, resolved to a module and tagged
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
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_att_store).

%% API
-export([open/2, close/1]).
-export([backend_module/1, is_available/1]).
-export([put/5, put/6, get/4, delete/4]).
-export([delete_all/3]).
-export([fold/5]).

%% Streaming API
-export([put_stream/5, put_stream/6]).
-export([write_chunk/2, finish_stream/1, abort_stream/1]).
-export([get_stream/4, read_chunk/1, close_stream/1]).
-export([get_info/4]).

%% Sync support (optional backend callbacks; see barrel_att_backend)
-export([delete/5, att_changes/4, att_floor/2, sweep_att_feed/3,
         rebuild_feed/2, supports_sync/1]).
-export([checkpoint/2]).
-export([destroy/2]).

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
backend_module(Module) when is_atom(Module) -> Module.

%% @doc Whether a backend's implementation is present in the build.
%% `blob' ships with barrel_docdb itself; other backends are optional
%% sibling apps (opted into the build via their own rebar profile) probed
%% for at runtime, same pattern as `barrel_vectordb_index:is_available/1'
%% for the optional FAISS backend.
-spec is_available(atom()) -> boolean().
is_available(blob) ->
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
    B = backend(AttRef),
    B:put(AttRef, barrel_keyspace:resolve(DbName), DocId, AttName, Data).

-spec put(att_ref(), binary(), binary(), binary(), binary(), map()) ->
    {ok, map()} | {error, term()}.
put(AttRef, DbName, DocId, AttName, Data, Opts) ->
    B = backend(AttRef),
    B:put(AttRef, barrel_keyspace:resolve(DbName), DocId, AttName, Data,
          Opts).

-spec get(att_ref(), binary(), binary(), binary()) ->
    {ok, binary()} | not_found | {error, term()}.
get(AttRef, DbName, DocId, AttName) ->
    B = backend(AttRef),
    B:get(AttRef, barrel_keyspace:resolve(DbName), DocId, AttName).

-spec delete(att_ref(), binary(), binary(), binary()) -> ok | {error, term()}.
delete(AttRef, DbName, DocId, AttName) ->
    B = backend(AttRef),
    B:delete(AttRef, barrel_keyspace:resolve(DbName), DocId, AttName).

-spec delete_all(att_ref(), binary(), binary()) -> ok | {error, term()}.
delete_all(AttRef, DbName, DocId) ->
    B = backend(AttRef),
    B:delete_all(AttRef, barrel_keyspace:resolve(DbName), DocId).

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
    B:write_chunk(Stream, Data).

-spec finish_stream(att_stream()) ->
    {ok, map()} | {ok, ignored} | {error, term()}.
finish_stream(Stream) ->
    B = stream_backend(Stream),
    B:finish_stream(Stream).

-spec abort_stream(att_stream()) -> ok.
abort_stream(Stream) ->
    B = stream_backend(Stream),
    B:abort_stream(Stream).

-spec get_stream(att_ref(), binary(), binary(), binary()) ->
    {ok, att_stream()} | {error, term()}.
get_stream(AttRef, DbName, DocId, AttName) ->
    B = backend(AttRef),
    B:get_stream(AttRef, barrel_keyspace:resolve(DbName), DocId,
                 AttName).

-spec read_chunk(att_stream()) -> {ok, binary(), att_stream()} | eof | {error, term()}.
read_chunk(Stream) ->
    B = stream_backend(Stream),
    B:read_chunk(Stream).

-spec close_stream(att_stream()) -> ok.
close_stream(Stream) ->
    B = stream_backend(Stream),
    B:close_stream(Stream).

%%====================================================================
%% Internal
%%====================================================================

%% @doc Delete with options (origin_hlc for replicated deletes).
-spec delete(att_ref(), binary(), binary(), binary(), map()) ->
    ok | {error, term()}.
delete(AttRef, DbName, DocId, AttName, Opts) ->
    B = backend(AttRef),
    B:delete(AttRef, barrel_keyspace:resolve(DbName), DocId, AttName,
             Opts).

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

-spec sweep_att_feed(att_ref(), binary(), term()) ->
    {ok, map()} | {error, term()}.
sweep_att_feed(AttRef, DbName, Cutoff) ->
    B = backend(AttRef),
    B:sweep_att_feed(AttRef, barrel_keyspace:resolve(DbName), Cutoff).

-spec rebuild_feed(att_ref(), binary()) -> {ok, map()} | {error, term()}.
rebuild_feed(AttRef, DbName) ->
    B = backend(AttRef),
    B:rebuild_feed(AttRef, barrel_keyspace:resolve(DbName)).

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

backend(#{backend := B}) -> B;
backend(_) -> backend_module(?DEFAULT_BACKEND).

stream_backend(#{att_ref := AttRef}) -> backend(AttRef);
stream_backend(_) -> backend_module(?DEFAULT_BACKEND).
