%%%-------------------------------------------------------------------
%%% @doc Attachment storage backend behaviour.
%%%
%%% An attachment backend stores document attachment bytes (and the small
%%% metadata needed to describe them) under a database/document/name key space.
%%% `barrel_att_store_blob' is the default backend (a RocksDB BlobDB instance).
%%% `barrel_att_store' is a thin dispatcher that selects a backend per database
%%% from `att_opts.backend' and routes calls to it, so additional backends (for
%%% example a local filesystem or an S3 object store) can be added without
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

%%====================================================================
%% Optional: sync support (the attachment change feed)
%%====================================================================
%% Backends without these degrade gracefully: attachment sync reports
%% att_sync => skipped for them.

-callback delete(AttRef :: map(), DbName :: binary(), DocId :: binary(),
                 AttName :: binary(), Opts :: map()) ->
    ok | {error, term()}.

-callback att_changes(AttRef :: map(), DbName :: binary(),
                      Since :: term(), Opts :: map()) ->
    {ok, [map()], LastSeq :: term()} | {error, term()}.

-callback att_floor(AttRef :: map(), DbName :: binary()) ->
    term() | undefined.

-callback sweep_att_feed(AttRef :: map(), DbName :: binary(),
                         Cutoff :: term()) ->
    {ok, map()} | {error, term()}.

-callback rebuild_feed(AttRef :: map(), DbName :: binary()) ->
    {ok, map()} | {error, term()}.

-optional_callbacks([delete/5, att_changes/4, att_floor/2,
                     sweep_att_feed/3, rebuild_feed/2]).
