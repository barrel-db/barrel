%%%-------------------------------------------------------------------
%%% @doc Attachment store dispatcher.
%%%
%%% Selects an attachment backend (a {@link barrel_att_backend}) per database
%%% and routes all attachment calls to it. The backend is chosen from
%%% `att_opts.backend' at {@link open/2} (default `barrel_att_store_blob', the
%%% RocksDB BlobDB backend) and tagged into the returned `att_ref'. Streaming
%%% handles embed their `att_ref', so streaming calls dispatch to the same
%%% backend.
%%%
%%% Callers (barrel_att, barrel_docdb, barrel_db_server) keep using this module;
%%% the backend split is transparent to them.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_att_store).

%% API
-export([open/2, close/1]).
-export([put/5, put/6, get/4, delete/4]).
-export([delete_all/3]).
-export([fold/5]).

%% Streaming API
-export([put_stream/5, put_stream/6]).
-export([write_chunk/2, finish_stream/1, abort_stream/1]).
-export([get_stream/4, read_chunk/1, close_stream/1]).
-export([get_info/4]).

-export_type([att_ref/0, att_stream/0]).

-define(DEFAULT_BACKEND, barrel_att_store_blob).

-type att_ref() :: #{backend => module(), _ => _}.
-type att_stream() :: #{att_ref := att_ref(), _ => _}.

%%====================================================================
%% API
%%====================================================================

-spec open(string(), map()) -> {ok, att_ref()} | {error, term()}.
open(Path, Options) ->
    Backend = maps:get(backend, Options, ?DEFAULT_BACKEND),
    case Backend:open(Path, Options) of
        {ok, AttRef} -> {ok, AttRef#{backend => Backend}};
        {error, _} = Err -> Err
    end.

-spec close(att_ref()) -> ok.
close(AttRef) ->
    B = backend(AttRef),
    B:close(AttRef).

-spec put(att_ref(), binary(), binary(), binary(), binary()) ->
    {ok, map()} | {error, term()}.
put(AttRef, DbName, DocId, AttName, Data) ->
    B = backend(AttRef),
    B:put(AttRef, DbName, DocId, AttName, Data).

-spec put(att_ref(), binary(), binary(), binary(), binary(), map()) ->
    {ok, map()} | {error, term()}.
put(AttRef, DbName, DocId, AttName, Data, Opts) ->
    B = backend(AttRef),
    B:put(AttRef, DbName, DocId, AttName, Data, Opts).

-spec get(att_ref(), binary(), binary(), binary()) ->
    {ok, binary()} | {error, term()}.
get(AttRef, DbName, DocId, AttName) ->
    B = backend(AttRef),
    B:get(AttRef, DbName, DocId, AttName).

-spec delete(att_ref(), binary(), binary(), binary()) -> ok | {error, term()}.
delete(AttRef, DbName, DocId, AttName) ->
    B = backend(AttRef),
    B:delete(AttRef, DbName, DocId, AttName).

-spec delete_all(att_ref(), binary(), binary()) -> ok | {error, term()}.
delete_all(AttRef, DbName, DocId) ->
    B = backend(AttRef),
    B:delete_all(AttRef, DbName, DocId).

-spec fold(att_ref(), binary(), binary(), fun(), term()) -> term().
fold(AttRef, DbName, DocId, Fun, Acc) ->
    B = backend(AttRef),
    B:fold(AttRef, DbName, DocId, Fun, Acc).

-spec get_info(att_ref(), binary(), binary(), binary()) ->
    {ok, map()} | {error, term()}.
get_info(AttRef, DbName, DocId, AttName) ->
    B = backend(AttRef),
    B:get_info(AttRef, DbName, DocId, AttName).

%%====================================================================
%% Streaming API
%%====================================================================

-spec put_stream(att_ref(), binary(), binary(), binary(), binary()) ->
    {ok, att_stream()} | {error, term()}.
put_stream(AttRef, DbName, DocId, AttName, ContentType) ->
    B = backend(AttRef),
    B:put_stream(AttRef, DbName, DocId, AttName, ContentType).

-spec put_stream(att_ref(), binary(), binary(), binary(), binary(), map()) ->
    {ok, att_stream()} | {error, term()}.
put_stream(AttRef, DbName, DocId, AttName, ContentType, Opts) ->
    B = backend(AttRef),
    B:put_stream(AttRef, DbName, DocId, AttName, ContentType, Opts).

-spec write_chunk(att_stream(), binary()) -> {ok, att_stream()} | {error, term()}.
write_chunk(Stream, Data) ->
    B = stream_backend(Stream),
    B:write_chunk(Stream, Data).

-spec finish_stream(att_stream()) -> {ok, map()} | {error, term()}.
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
    B:get_stream(AttRef, DbName, DocId, AttName).

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

backend(#{backend := B}) -> B;
backend(_) -> ?DEFAULT_BACKEND.

stream_backend(#{att_ref := AttRef}) -> backend(AttRef);
stream_backend(_) -> ?DEFAULT_BACKEND.
