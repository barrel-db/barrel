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
%%% `AttRef' is the opaque handle returned by the `open' callback; the dispatcher tags
%%% it with the backend module (and, for a database's handle, `db': its
%%% logical name, writer pid and epoch). `Stream' is the opaque streaming handle returned
%%% by the `put_stream' / `get_stream' callbacks and embeds its `att_ref'.
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

%% Releases what a read stream holds (safe to call more than once, and
%% after eof).
-callback close_stream(Stream :: map()) -> ok.

%% Position a fresh read stream at byte Offset. Returns the stream moved
%% to the chunk holding Offset and the bytes still to drop from that
%% chunk. Backends without it are read from the start and skipped.
-callback seek_stream(Stream :: map(), Offset :: non_neg_integer()) ->
    {ok, map(), Skip :: non_neg_integer()}.

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

%% Current feed index entry of one attachment (deletes included).
-callback att_state(AttRef :: map(), DbName :: binary(), DocId :: binary(),
                    AttName :: binary()) ->
    {ok, map()} | not_found.

%% Current feed index entries of one document's attachments.
-callback att_list(AttRef :: map(), DbName :: binary(), DocId :: binary()) ->
    [map()].

%% Record a newer origin for bytes already stored (digest must match),
%% without sending them again. Answers {error, bytes_needed} otherwise.
-callback touch(AttRef :: map(), DbName :: binary(), DocId :: binary(),
                AttName :: binary(), Opts :: map()) ->
    {ok, touched} | {ok, ignored} | {error, term()}.

%% Make every acknowledged write durable (replication checkpoints wait
%% on it). Backends without it cannot be a target under att_durability
%% required.
-callback flush(AttRef :: map()) -> ok | {error, term()}.

%% Hard-link snapshot of the whole attachment store into a new
%% directory (timeline forks). Backends without it cannot be branched.
-callback checkpoint(AttRef :: map(), Path :: string()) ->
    ok | {error, term()}.

%% Erase everything owned by this store beyond its local directory (e.g.
%% remote objects) before delete_db removes that directory. Backends
%% without it need no extra step: local deletion already covers them.
-callback destroy(AttRef :: map(), DbName :: binary()) ->
    ok | {error, term()}.

%%====================================================================
%% Optional: staged writes committed by the database writer
%%====================================================================
%% A backend exporting commit/3 splits each write: the stage callbacks
%% run in the caller and write bulk bytes where no reader sees them; the
%% dispatcher then runs commit/3 inside the database's writer process,
%% which serializes the last-write-wins check, the metadata and feed
%% batch and the cleanup of the replaced data. Alloc(Floor) returns a
%% feed HLC above Floor and above every HLC the database wrote. Without
%% commit/3 (or for a store used without a database) put/6, delete/5 and
%% finish_stream/1 are called directly and the backend serializes itself.

-callback stage_put(AttRef :: map(), DbName :: binary(), DocId :: binary(),
                    AttName :: binary(), Data :: binary(), Opts :: map()) ->
    {commit, Commit :: term()} | {error, term()}.

-callback stage_finish(Stream :: map()) ->
    {commit, Commit :: term()} | {error, term()}.

-callback stage_delete(AttRef :: map(), DbName :: binary(), DocId :: binary(),
                       AttName :: binary(), Opts :: map()) ->
    {commit, Commit :: term()}.

-callback stage_touch(AttRef :: map(), DbName :: binary(), DocId :: binary(),
                      AttName :: binary(), Opts :: map()) ->
    {commit, Commit :: term()}.

-callback commit(AttRef :: map(), Commit :: term(),
                 Alloc :: fun((term()) -> term())) ->
    {ok, map()} | {ok, ignored} | ok | {error, term()}.

%% rebuild_feed/2 run in the writer, with feed HLCs from Alloc.
-callback rebuild_feed(AttRef :: map(), DbName :: binary(),
                       Alloc :: fun((term()) -> term())) ->
    {ok, map()} | {error, term()}.

%% Drop staged uploads older than MaxAgeMs; called by the writer at open
%% and on the retention timer.
-callback gc_uploads(AttRef :: map(), DbName :: binary(),
                     MaxAgeMs :: non_neg_integer()) ->
    {ok, map()}.

-optional_callbacks([delete/5, att_changes/4, att_floor/2,
                     sweep_att_feed/3, rebuild_feed/2, checkpoint/2,
                     destroy/2, seek_stream/2,
                     att_state/4, att_list/3, touch/5, flush/1,
                     stage_put/6, stage_finish/1, stage_delete/5,
                     stage_touch/5, commit/3,
                     rebuild_feed/3, gc_uploads/3]).
