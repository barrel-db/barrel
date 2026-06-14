%%%-------------------------------------------------------------------
%%% @doc Attachment storage backend behaviour.
%%%
%%% An attachment backend stores document attachment bytes (and the small
%%% metadata needed to describe them) under a database/document/name key space.
%%% `barrel_att_store_blob' is the default backend (a RocksDB BlobDB instance).
%%% `barrel_att_store' is a thin dispatcher that selects a backend per database
%%% from `att_opts.backend' and routes calls to it, so additional backends (for
%%% example an object store via barrel_objectdb / S3) can be added without
%%% touching the document layer.
%%%
%%% `AttRef' is the opaque handle returned by {@link open/2}; the dispatcher tags
%%% it with the backend module. `Stream' is the opaque streaming handle returned
%%% by {@link put_stream/5} / {@link get_stream/4} and embeds its `att_ref'.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_att_backend).

-callback open(Path :: string(), Options :: map()) ->
    {ok, AttRef :: map()} | {error, term()}.

-callback close(AttRef :: map()) -> ok.

-callback put(AttRef :: map(), DbName :: binary(), DocId :: binary(),
              AttName :: binary(), Data :: binary()) ->
    {ok, Info :: map()} | {error, term()}.

-callback put(AttRef :: map(), DbName :: binary(), DocId :: binary(),
              AttName :: binary(), Data :: binary(), Opts :: map()) ->
    {ok, Info :: map()} | {error, term()}.

-callback get(AttRef :: map(), DbName :: binary(), DocId :: binary(),
              AttName :: binary()) ->
    {ok, binary()} | {error, term()}.

-callback delete(AttRef :: map(), DbName :: binary(), DocId :: binary(),
                 AttName :: binary()) ->
    ok | {error, term()}.

-callback delete_all(AttRef :: map(), DbName :: binary(), DocId :: binary()) ->
    ok | {error, term()}.

-callback fold(AttRef :: map(), DbName :: binary(), DocId :: binary(),
               Fun :: fun(), Acc :: term()) ->
    term().

-callback get_info(AttRef :: map(), DbName :: binary(), DocId :: binary(),
                   AttName :: binary()) ->
    {ok, map()} | {error, term()}.

-callback put_stream(AttRef :: map(), DbName :: binary(), DocId :: binary(),
                     AttName :: binary(), ContentType :: binary()) ->
    {ok, map()} | {error, term()}.

-callback put_stream(AttRef :: map(), DbName :: binary(), DocId :: binary(),
                     AttName :: binary(), ContentType :: binary(), Opts :: map()) ->
    {ok, map()} | {error, term()}.

-callback write_chunk(Stream :: map(), Data :: binary()) ->
    {ok, map()} | {error, term()}.

-callback finish_stream(Stream :: map()) -> {ok, map()} | {error, term()}.

-callback abort_stream(Stream :: map()) -> ok.

-callback get_stream(AttRef :: map(), DbName :: binary(), DocId :: binary(),
                     AttName :: binary()) ->
    {ok, map()} | {error, term()}.

-callback read_chunk(Stream :: map()) ->
    {ok, binary(), map()} | eof | {error, term()}.

-callback close_stream(Stream :: map()) -> ok.
