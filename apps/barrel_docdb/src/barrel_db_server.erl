%%%-------------------------------------------------------------------
%%% @doc barrel_db_server - Individual database server process
%%%
%%% Manages a single database instance. Each database has its own
%%% gen_server process that handles all operations for that database.
%%% Opens both a document store (regular RocksDB) and an attachment
%%% store (RocksDB with BlobDB enabled).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_db_server).

-behaviour(gen_server).

-include("barrel_docdb.hrl").

%% API
-export([start_link/2, start_link/3, release_stale_refs/1]).
-export([info/1, stop/1]).
-export([get_store_ref/1, get_att_ref/1]).
-export([sweep_ttl/1]).
-export([source_id/1, writer_id/1, alloc_hlc/1, alloc_hlc/2, stub_census/2]).
-export([att_commit/3]).

%% Document API
-export([
    put_doc/3,
    put_doc/4,
    put_docs/3,
    put_docs/4,
    get_doc/3,
    get_docs/3,
    delete_doc/3,
    fold_docs/3,
    fold_docs/4,
    resolve_conflict/4,
    get_conflicts/2
]).

%% One document's batch, built in the writer or from a request prepared
%% in the caller (tests compare the two)
-export([doc_write_ops/5]).

%% Tagged outbox API (acks are writes, so they go through the server)
-export([outbox_ack/3]).

%% Embedding column (computed vectors written back by an indexer)
-export([set_doc_embedding/4]).

%% Retention sweep (also runs on a timer; exposed for tests and tools)
-export([sweep_retention/1, sweep_retention/2]).

%% Timeline fork (checkpoint both stores from inside the writer)
-export([checkpoint_to/3]).

%% Replication API
-export([
    put_version/5,
    get_doc_state/2,
    put_doc_state/4,
    put_doc_state/5,
    repair_doc_bodies/3,
    diff_states/2,
    diff_versions/2
]).

%% Local document API (for checkpoints, not replicated)
-export([
    put_local_doc/3,
    put_local_doc/4,
    get_local_doc/2,
    delete_local_doc/2,
    fold_local_docs/4
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2]).

%% Write requests taken from the mailbox and not yet built, and the
%% current state of their documents, read in one call per batch of
%% requests (an id is read again once it is written).
-record(fill, {
    pending = [] :: list(),
    cache = #{} :: #{binary() => map() | undefined}
}).

%% Groups handed to the committer and not yet written.
-define(MAX_INFLIGHT, 8).
-define(DEFAULT_WRITE_CHUNK, 16).

-record(state, {
    name :: binary(),
    keyspace :: binary(),  %% name used for storage keys (parent's on a branch)
    parent :: binary() | undefined,  %% timeline parent (branches only)
    fork_hlc :: barrel_hlc:timestamp() | undefined,  %% fork instant (branches only)
    config :: map(),
    db_path :: string(),
    store_ref :: barrel_store_rocksdb:db_ref() | undefined,
    att_ref :: barrel_att_store:att_ref() | undefined,
    %% EncryptedEnv shared by both stores. The NIF frees the underlying
    %% env when the handle is garbage collected, so it must stay
    %% referenced for the whole db lifetime (also carried in the store
    %% and att refs).
    env :: rocksdb:env_handle() | undefined,
    %% Refreshes the storage gauges (size, compaction backlog)
    stats_timer :: reference() | undefined,
    stats_interval = 60000 :: non_neg_integer(),
    retention_period :: non_neg_integer(),  %% Retention window in seconds (0 = infinite)
    retention_timer :: reference() | undefined,  %% Timer for periodic retention sweeps
    retention_interval :: pos_integer(),  %% Interval between sweeps in ms
    %% abandoned-upload sweep when no retention timer runs (it does it)
    upload_gc_timer :: reference() | undefined,
    ttl_sweep_interval = 0 :: non_neg_integer(),  %% Doc TTL sweep interval in ms (0 = off)
    ttl_sweep_batch = 512 :: pos_integer(),  %% Max expired docs per sweep pass
    ttl_timer :: reference() | undefined,  %% Timer for periodic TTL sweeps
    %% In-flight retention/ttl workers, one per kind, run off the
    %% writer loop. #{Kind => {Pid, Waiters, Periodic}}: Waiters are the
    %% gen_server Froms to reply to; Periodic marks a worker that a timer
    %% contributed to, so its completion re-arms the periodic timer.
    sweep_workers = #{} :: #{atom() => {pid(), [term()], boolean()}},
    max_group = 256 :: pos_integer(),  %% max write requests per batch
    %% documents after which an unsynced group goes to the committer, so
    %% the writer builds the next group while this one is written
    write_chunk = ?DEFAULT_WRITE_CHUNK :: pos_integer(),
    %% read_only => true in the config: writes are refused and no
    %% retention or TTL timer runs
    read_only = false :: boolean(),
    write_groups = #{groups => 0, requests => 0, max_size => 0} :: map(),
    %% Writes the groups' batches and answers their callers while the
    %% writer builds the next group; `inflight' holds the groups handed
    %% to it and not yet written, oldest first, with their doc ids.
    committer :: pid() | undefined,
    inflight = [] :: [{reference(), #{binary() => true}}]
}).

%% A write request in a group commit. `seg' holds the items built in the
%% current group, `done' the results of items committed by earlier
%% groups, both newest first. `more' marks a request that continues in
%% the next group.
-record(wreq, {
    from :: gen_server:from(),
    kind :: put_doc | put_docs | delete_doc | outbox_ack,
    opts = #{} :: map(),
    pending = [] :: [term()],
    seg = [] :: [{term(), list() | boolean(), term()}],  %% ops, or whether it had ops
    done = [] :: [term()],
    more = false :: boolean(),
    sync = false :: boolean()   %% the request or one of its docs asked for sync
}).

%% A document write prepared in the calling process: the parts of its
%% batch that depend on neither the database state nor the HLC.
-record(prep, {
    ks :: binary(),               %% keyspace the keys are built for
    rec :: map(),                 %% barrel_doc:make_doc_record/1
    cbor :: binary(),             %% body, plain CBOR
    change_cbor :: binary(),      %% body, indexed CBOR (change row)
    paths :: [{[term()], term()}] | undefined,  %% live body paths
    topics :: [binary()],         %% feed and notification topics
    feed_heads :: [{binary(), binary()}],  %% feed row keys before the HLC
    index_ops :: [tuple()]        %% path index of a body indexed from scratch
}).

%% A group of write requests committed in one batch (reqs newest first).
-record(grp, {
    reqs = [] :: [#wreq{}],
    ids = #{} :: #{binary() => true},
    count = 0 :: non_neg_integer(),
    items = 0 :: non_neg_integer(),
    sync = false :: boolean(),
    %% ops of the built items not yet sent to the committer (newest
    %% first); a synced group sends them in chunks while it is built, so
    %% the committer builds the batch meanwhile (`ref' names that batch)
    unsent = [] :: [list()],
    unsent_n = 0 :: non_neg_integer(),
    ref :: reference() | undefined
}).

%% Options of the removed compaction check and revision filter; still
%% accepted, and reported once at open.
%% Storage gauges refresh every minute; `stats_interval => 0' turns it off.
-define(DEFAULT_STATS_INTERVAL, 60000).

-define(OBSOLETE_OPTIONS, [compaction_interval, compaction_size_threshold,
                           prune_depth]).

%% Default retention settings: 30 days, swept every 10 minutes.
%% retention_period 0 means infinite (no sweeper started).
-define(DEFAULT_RETENTION_PERIOD, 2592000).  %% 30 days in seconds
-define(DEFAULT_RETENTION_INTERVAL, 600000).  %% 10 minutes in ms

%% Staged attachment uploads older than this (seconds) are abandoned.
-define(DEFAULT_ATT_UPLOAD_TIMEOUT, 3600).

%% Live conflict siblings per document are bounded; beyond the bound
%% the lowest versions are dropped deterministically.
-define(DEFAULT_MAX_CONFLICT_VERSIONS, 8).

%%====================================================================
%% API functions
%%====================================================================

%% @doc Start the database server
-spec start_link(binary(), map()) -> {ok, pid()} | {error, term()}.
start_link(Name, Config) ->
    gen_server:start_link(?MODULE, [Name, Config], []).

%% @doc Start the writer under its database supervisor `Sup'.
-spec start_link(binary(), map(), pid()) -> {ok, pid()} | {error, term()}.
start_link(Name, Config, Sup) ->
    gen_server:start_link(?MODULE, [Name, Config, Sup], []).

%% @doc Get database info
-spec info(pid()) -> {ok, map()} | {error, term()}.
info(Pid) ->
    gen_server:call(Pid, info).

%% @doc Checkpoint both stores into a branch's directories, minting
%% the fork instant inside the writer: every write applied before this
%% call is in the checkpoint and every later one has a larger HLC.
%% Blocks the writer for the memtable flush + hard links.
-spec checkpoint_to(pid(), string(), string()) ->
    {ok, barrel_hlc:timestamp()} | {error, term()}.
checkpoint_to(Pid, DocsPath, AttPath) ->
    gen_server:call(Pid, {checkpoint_to, DocsPath, AttPath}, infinity).

%% @doc Get the document store reference
-spec get_store_ref(pid()) -> {ok, barrel_store_rocksdb:db_ref()} | {error, term()}.
get_store_ref(Pid) ->
    gen_server:call(Pid, get_store_ref).

%% @doc Get the attachment store reference
-spec get_att_ref(pid()) -> {ok, barrel_att_store:att_ref()} | {error, term()}.
get_att_ref(Pid) ->
    gen_server:call(Pid, get_att_ref).


%% @doc Run an attachment commit in the writer. Fun gets Alloc(Floor),
%% a feed HLC above Floor and every HLC this database wrote. Epoch is
%% the writer id the caller started under: a writer that died or
%% restarted since answers `{error, db_restarted}', and a read-only
%% database `{error, read_only}'.
-spec att_commit(pid(), binary() | undefined,
                 fun((fun((barrel_hlc:timestamp()) ->
                             barrel_hlc:timestamp())) -> term())) -> term().
att_commit(Pid, Epoch, Fun) ->
    try gen_server:call(Pid, {att_commit, Epoch, Fun}, infinity)
    catch exit:_ -> {error, db_restarted}
    end.

%% @doc Stop the database server
-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:stop(Pid).

%%====================================================================
%% Document API functions
%%====================================================================

%% @doc Put a document (create or update)
-spec put_doc(pid(), map(), map()) -> {ok, map()} | {error, term()}.
put_doc(Pid, Doc, Opts) ->
    gen_server:call(Pid, {put_doc, Doc, Opts}).

%% @doc put_doc/3 with the document prepared in the calling process, so
%% the writer only reads, checks, stamps and batches it.
-spec put_doc(pid(), binary(), map(), map()) -> {ok, map()} | {error, term()}.
put_doc(Pid, DbName, Doc, Opts) ->
    gen_server:call(Pid, {put_doc, prepare(DbName, Doc), Opts}).

%% @doc Put multiple documents (batch write). A document may come as
%% `{Doc, DocOpts}' with its own `outbox' and `sync' options.
-spec put_docs(pid(), [map() | {map(), map()}], map()) ->
    [{ok, map()} | {error, term()}].
put_docs(Pid, Docs, Opts) ->
    gen_server:call(Pid, {put_docs, Docs, Opts}).

%% @doc put_docs/3 with the documents prepared in the calling process.
-spec put_docs(pid(), binary(), [map() | {map(), map()}], map()) ->
    [{ok, map()} | {error, term()}].
put_docs(Pid, DbName, Docs, Opts) ->
    gen_server:call(Pid, {put_docs, [prepare_entry(DbName, D) || D <- Docs],
                          Opts}).

prepare_entry(DbName, {Doc, DocOpts}) when is_map(DocOpts) ->
    {prepare(DbName, Doc), DocOpts};
prepare_entry(DbName, Doc) ->
    prepare(DbName, Doc).

%% @doc Get a document
%% When raw_body => true in Opts, returns {ok, CborBin, Meta} for zero-copy responses
-spec get_doc(pid(), binary(), map()) -> {ok, map()} | {ok, binary(), map()} | {error, not_found} | {error, term()}.
get_doc(Pid, DocId, Opts) ->
    gen_server:call(Pid, {get_doc, DocId, Opts}).

%% @doc Get multiple documents (batch read)
-spec get_docs(pid(), [binary()], map()) -> [{ok, map()} | {error, not_found} | {error, term()}].
get_docs(Pid, DocIds, Opts) ->
    gen_server:call(Pid, {get_docs, DocIds, Opts}).

%% @doc Delete a document
-spec delete_doc(pid(), binary(), map()) -> {ok, map()} | {error, term()}.
delete_doc(Pid, DocId, Opts) ->
    gen_server:call(Pid, {delete_doc, DocId, Opts}).

%% @doc Fold over all documents
-spec fold_docs(pid(), fun(), term()) -> {ok, term()}.
fold_docs(Pid, Fun, Acc) ->
    gen_server:call(Pid, {fold_docs, Fun, Acc}, infinity).

%% @doc Fold over all documents with options
-spec fold_docs(pid(), fun(), term(), map()) -> {ok, term()}.
fold_docs(Pid, Fun, Acc, Opts) when is_map(Opts) ->
    gen_server:call(Pid, {fold_docs, Fun, Acc, Opts}, infinity).

%% @doc Get conflicts for a document
%% Returns list of conflicting revision IDs (excluding the winner)
-spec get_conflicts(pid(), binary()) -> {ok, [binary()]} | {error, term()}.
get_conflicts(Pid, DocId) ->
    gen_server:call(Pid, {get_conflicts, DocId}).

%% @doc Resolve a conflict by choosing a winning revision or providing a merged doc
%% Resolution can be:
%%   - {choose, Rev} - keep this revision as winner, delete other branches
%%   - {merge, Doc} - create new revision merging all conflicts
-spec resolve_conflict(pid(), binary(), binary(), {choose, binary()} | {merge, map()}) ->
    {ok, map()} | {error, term()}.
resolve_conflict(Pid, DocId, BaseRev, Resolution) ->
    gen_server:call(Pid, {resolve_conflict, DocId, BaseRev, Resolution}).

%% @doc Acknowledge processed outbox entries by their exact HLC keys.
%% Deleting a key that was already replaced by a newer write is a no-op,
%% so acks never lose work (see barrel_outbox).
-spec outbox_ack(pid(), barrel_outbox:tag(), [barrel_hlc:timestamp()]) ->
    ok | {error, term()}.
outbox_ack(Pid, Tag, Hlcs) ->
    gen_server:call(Pid, {outbox_ack, Tag, Hlcs}).

%% @doc Store a computed embedding as the document's embedding column.
%% CAS on the expected revision: embeddings are derived data, so this
%% never bumps the revision or emits a change; a conflict means a newer
%% write exists (which re-drives the computing indexer anyway).
-spec set_doc_embedding(pid(), binary(), binary(), [number()]) ->
    ok | {error, conflict | not_found | term()}.
set_doc_embedding(Pid, DocId, ExpectedRev, Vector) ->
    gen_server:call(Pid, {set_doc_embedding, DocId, ExpectedRev, Vector}).

%% @doc Run a retention sweep now. Prunes history entries, superseded
%% version bodies and expired tombstones older than the retention
%% window, then advances the history floor. No-op when retention is
%% infinite (retention_period 0).
-spec sweep_retention(pid()) -> {ok, map()} | {error, term()}.
sweep_retention(Pid) ->
    gen_server:call(Pid, sweep_retention, infinity).

%% @doc Run a retention sweep as if the wall clock read `NowMs' (unix ms).
%% Test hook: the window is judged against a chosen instant.
-spec sweep_retention(pid(), non_neg_integer()) -> {ok, map()} | {error, term()}.
sweep_retention(Pid, NowMs) when is_integer(NowMs), NowMs >= 0 ->
    gen_server:call(Pid, {sweep_retention, NowMs}, infinity).

%% @doc Run one doc TTL sweep pass now (test and ops hook). Returns
%% the number of docs tombstoned.
-spec sweep_ttl(pid()) -> {ok, non_neg_integer()}.
sweep_ttl(Pid) ->
    gen_server:call(Pid, ttl_sweep, infinity).

%%====================================================================
%% Replication API functions
%%====================================================================

%% @doc Put a document with explicit revision history (for replication)
%% Returns {ok, DocId, RevId} on success.
-spec put_version(pid(), map(), binary(), binary(), boolean()) ->
    {ok, binary(), binary()} | {error, term()}.
put_version(Pid, Doc, VersionToken, VVBin, Deleted) ->
    gen_server:call(Pid, {put_version, Doc, VersionToken, VVBin, Deleted}).

%% @doc A document's full replication state and live bodies.
-spec get_doc_state(pid(), binary()) -> {ok, map()} | {error, term()}.
get_doc_state(Pid, DocId) ->
    gen_server:call(Pid, {get_doc_state, DocId}).

%% @doc Merge a peer's full document state. `unchanged' when this
%% database already held the merged state (nothing written).
-spec put_doc_state(pid(), binary(), binary(), #{binary() => map()}) ->
    {ok | unchanged, binary(), binary()} | {error, term()}.
put_doc_state(Pid, DocId, StateBin, Bodies) ->
    put_doc_state(Pid, DocId, StateBin, Bodies, #{}).

%% @doc put_doc_state/4 with the client vectors the peer holds, by
%% version token.
-spec put_doc_state(pid(), binary(), binary(), #{binary() => map()},
                    #{binary() => [number()]}) ->
    {ok | unchanged, binary(), binary()} | {error, term()}.
put_doc_state(Pid, DocId, StateBin, Bodies, Vectors) ->
    gen_server:call(Pid, {put_doc_state, DocId, StateBin, Bodies, Vectors}).

%% @doc Rewrite the bodies of live versions this database stores
%% differently (a token names one body); the state does not change.
-spec repair_doc_bodies(pid(), binary(), #{binary() => map()}) ->
    {ok, binary(), [binary()]} | {error, term()}.
repair_doc_bodies(Pid, DocId, Bodies) ->
    gen_server:call(Pid, {repair_doc_bodies, DocId, Bodies}).

%% @doc Which offered states differ: #{DocId => Digest} in,
%% #{DocId => missing | have} out.
-spec diff_states(pid(), #{binary() => binary()}) ->
    {ok, #{binary() => missing | have}} | {error, term()}.
diff_states(Pid, DigestMap) ->
    gen_server:call(Pid, {diff_states, DigestMap}).

%% @doc Get revisions difference (for replication)
%% Returns {ok, Missing, PossibleAncestors}
-spec diff_versions(pid(), #{binary() => binary()}) ->
    {ok, #{binary() => missing | have}} | {error, term()}.
diff_versions(Pid, TokenMap) when is_map(TokenMap) ->
    gen_server:call(Pid, {diff_versions, TokenMap}).

%%====================================================================
%% Local Document API functions
%%====================================================================

%% @doc Put a local document (not replicated)
-spec put_local_doc(pid(), binary(), map()) -> ok | {error, term()}.
put_local_doc(Pid, DocId, Doc) ->
    gen_server:call(Pid, {put_local_doc, DocId, Doc}).

%% @doc put_local_doc/3 with options: `sync => true' waits for the disk.
-spec put_local_doc(pid(), binary(), map(), map()) -> ok | {error, term()}.
put_local_doc(Pid, DocId, Doc, Opts) ->
    gen_server:call(Pid, {put_local_doc, DocId, Doc, Opts}).

%% @doc Get a local document
-spec get_local_doc(pid(), binary()) -> {ok, map()} | {error, not_found}.
get_local_doc(Pid, DocId) ->
    gen_server:call(Pid, {get_local_doc, DocId}).

%% @doc Delete a local document
-spec delete_local_doc(pid(), binary()) -> ok | {error, not_found}.
delete_local_doc(Pid, DocId) ->
    gen_server:call(Pid, {delete_local_doc, DocId}).

%% @doc Fold over local documents with a given prefix
-spec fold_local_docs(pid(), binary(), fun((binary(), map(), term()) -> term()), term()) ->
    {ok, term()} | {error, term()}.
fold_local_docs(Pid, Prefix, Fun, Acc) ->
    gen_server:call(Pid, {fold_local_docs, Prefix, Fun, Acc}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%% @doc Initialize the database server
init([Name, Config, Sup]) ->
    case init([Name, Config]) of
        {ok, _} = Ok ->
            ok = barrel_db_watch:writer_started(Name, Config, Sup),
            Ok;
        Other ->
            Other
    end;
init([Name, Config]) ->
    process_flag(trap_exit, true),

    %% Validate the channels config before opening anything: a bad
    %% pattern set must fail the open, not the first write.
    case barrel_channel:compile(maps:get(channels, Config, #{})) of
        {ok, CompiledChannels} ->
            init(Name, Config, CompiledChannels);
        {error, ChanReason} ->
            {stop, {invalid_channels, ChanReason}}
    end.

init(Name, Config, CompiledChannels) ->
    %% Get data directory from config. Fall back to the `data_dir' app env so a
    %% node can relocate ALL of its dbs (including internal ones created with no
    %% opts) off the shared default - lets two local nodes use distinct dirs.
    DefaultDataDir = application:get_env(barrel_docdb, data_dir, "/tmp/barrel_data"),
    DataDir = maps:get(data_dir, Config, DefaultDataDir),
    DbPath = filename:join([DataDir, binary_to_list(Name)]),

    %% A previous instance that crashed without terminate leaves its
    %% store handles in persistent_term: the RocksDB LOCKs stay held and
    %% the database cannot reopen. Release them when their owner is gone.
    ok = release_stale_refs(Name),

    %% Branch identity comes from the TIMELINE sidecar file: it must be
    %% readable before RocksDB opens.
    case barrel_keyspace:read_meta(DbPath) of
        {ok, #{keyspace := SidecarKs, kind := import}} ->
            %% an imported copy: source keyspace, no timeline parent
            init(Name, Config, CompiledChannels, DbPath,
                 SidecarKs, undefined, undefined);
        {ok, #{keyspace := SidecarKs, parent := SidecarParent,
               fork_hlc := SidecarForkHlc}} ->
            init(Name, Config, CompiledChannels, DbPath,
                 SidecarKs, SidecarParent,
                 barrel_hlc:decode(SidecarForkHlc));
        not_found ->
            init(Name, Config, CompiledChannels, DbPath,
                 Name, undefined, undefined);
        {error, corrupt_timeline_meta} ->
            {stop, corrupt_timeline_meta}
    end.

init(Name, Config, CompiledChannels, DbPath, Keyspace, Parent, ForkHlc) ->
    %% Encryption resolves on the KEYSPACE (a branch opens with its
    %% parent's key: the checkpoint is ciphertext under it) and is
    %% checked against the CRYPTO marker before RocksDB opens, so a
    %% wrong key or an encrypted/plaintext mismatch fails cleanly
    %% instead of surfacing as corruption.
    case init_encryption(Config, Keyspace, DbPath) of
        {ok, Crypto} ->
            init(Name, Config, CompiledChannels, DbPath, Keyspace,
                 Parent, ForkHlc, Crypto);
        {error, EncReason} ->
            {stop, EncReason}
    end.

init(Name, Config, CompiledChannels, DbPath, Keyspace, Parent, ForkHlc, Crypto) ->
    Env = crypto_env(Crypto),
    %% Open document store (RocksDB with column families including body CF with BlobDB)
    DocStorePath = filename:join(DbPath, "docs"),
    StoreOpts0 = maps:get(store_opts, Config, #{}),
    ReadOnly = maps:get(read_only, Config, false) =:= true,
    StoreOpts = add_env_opt(StoreOpts0#{read_only => ReadOnly}, Env),
    case barrel_store_rocksdb:open(DocStorePath, StoreOpts) of
        {ok, StoreRef} ->
            %% Open attachment store (separate RocksDB with BlobDB). db_name
            %% (the resolved keyspace, the name embedded in keys) lets a backend that needs a stable identity at
            %% open time -- e.g. barrel_att_s3, seeding its S3 key prefix on
            %% first open -- derive one without every backend needing its
            %% own convention for it.
            AttStorePath = filename:join(DbPath, "attachments"),
            %% A backend that writes outside RocksDB (S3) cannot use the
            %% env; it gets the key itself.
            AttOpts = add_crypto_opt(
                        add_env_opt((maps:get(att_opts, Config, #{}))#{
                                        db_name => Keyspace,
                                        read_only => ReadOnly},
                                    Env),
                        Crypto),
            case barrel_att_store:open(AttStorePath, AttOpts) of
                {ok, AttRef} ->
                    %% Register in persistent_term for lookup
                    %% barrel_store is used by barrel_doc_body_store for body CF access.
                    %% The keyspace installs BEFORE the store ref so no
                    %% reader ever sees a store without its keyspace.
                    case Keyspace of
                        Name -> ok;
                        _ -> ok = barrel_keyspace:install(Name, Keyspace)
                    end,
                    persistent_term:put({barrel_db, Name}, self()),
                    persistent_term:put({barrel_store, Name}, StoreRef),
                    persistent_term:put({barrel_att_ref, Name}, AttRef),
                    persistent_term:put({barrel_source, Name},
                                        source_id(ReadOnly, StoreRef, Name,
                                                  DbPath)),
                    ok = barrel_doc_chain:open(ReadOnly, StoreRef, Keyspace),
                    ok = init_writer(ReadOnly, StoreRef, AttRef, Name, Keyspace),
                    _ = gc_att_uploads(ReadOnly, AttRef, Name, Config),
                    ok = barrel_channel:install(Name, CompiledChannels),

                    StatsInterval = maps:get(stats_interval, Config,
                                             ?DEFAULT_STATS_INTERVAL),
                    %% A read-only store does not change: db_info reads its
                    %% numbers on demand, no refresh runs.
                    StatsTimer = case ReadOnly of
                        true -> undefined;
                        false -> arm_stats(StatsInterval,
                                           min(StatsInterval, 1000))
                    end,

                    %% Retention settings; the sweeper only runs for a
                    %% finite window
                    RetentionPeriod = maps:get(retention_period, Config,
                                               ?DEFAULT_RETENTION_PERIOD),
                    RetentionInterval = maps:get(retention_check_interval, Config,
                                                 ?DEFAULT_RETENTION_INTERVAL),
                    RetentionTimer = case RetentionPeriod of
                        0 -> undefined;
                        _ -> arm_unless(ReadOnly, RetentionInterval,
                                        retention_sweep)
                    end,
                    UploadGcTimer = case RetentionTimer of
                        undefined -> arm_unless(ReadOnly, RetentionInterval,
                                                upload_gc);
                        _ -> undefined
                    end,

                    %% Doc TTL sweeper (opt-in): turns lazily expired
                    %% docs into real tombstones
                    TtlInterval = maps:get(ttl_sweep_interval, Config, 0),
                    TtlBatch = maps:get(ttl_sweep_batch, Config, 512),
                    TtlTimer = case TtlInterval of
                        0 -> undefined;
                        _ -> arm_unless(ReadOnly, TtlInterval, ttl_sweep)
                    end,

                    case [K || K <- ?OBSOLETE_OPTIONS, maps:is_key(K, Config)] of
                        [] -> ok;
                        Obsolete ->
                            logger:warning(
                              "Database ~s: ~p no longer have an effect: "
                              "RocksDB compacts on its own and no compaction "
                              "filter runs", [Name, Obsolete])
                    end,
                    case maps:is_key(conflict_merger, Config) of
                        true ->
                            logger:warning(
                              "Database ~s: conflict_merger is no longer "
                              "used; set conflict_resolver instead", [Name]);
                        false ->
                            ok
                    end,
                    logger:info("Database ~s started at ~s", [Name, DbPath]),
                    {ok, #state{
                        name = Name,
                        keyspace = Keyspace,
                        parent = Parent,
                        fork_hlc = ForkHlc,
                        config = Config,
                        db_path = DbPath,
                        store_ref = StoreRef,
                        att_ref = AttRef,
                        env = Env,
                        stats_timer = StatsTimer,
                        stats_interval = StatsInterval,
                        retention_period = RetentionPeriod,
                        retention_timer = RetentionTimer,
                        retention_interval = RetentionInterval,
                        upload_gc_timer = UploadGcTimer,
                        ttl_sweep_interval = TtlInterval,
                        ttl_sweep_batch = TtlBatch,
                        ttl_timer = TtlTimer,
                        max_group = maps:get(max_group, Config, 256),
                        write_chunk = maps:get(write_chunk, Config,
                                               ?DEFAULT_WRITE_CHUNK),
                        read_only = ReadOnly,
                        committer = start_committer(ReadOnly)
                    }};
                {error, AttReason} ->
                    %% Close document store if attachment store fails
                    barrel_store_rocksdb:close(StoreRef),
                    {stop, {att_store_open_failed, AttReason}}
            end;
        {error, Reason} ->
            {stop, {store_open_failed, Reason}}
    end.

%% @doc Handle synchronous calls. A read-only database refuses every
%% write before it reaches the store or a write group (group_commit/3
%% only drains the mailbox of a writable database).
handle_call(Req, From, #state{read_only = true} = State) ->
    case write_request(Req) of
        true -> {reply, {error, read_only}, State};
        false -> handle_call_rw(Req, From, State)
    end;
handle_call(Req, From, State) ->
    case group_write(Req) of
        true -> handle_call_rw(Req, From, State);
        false -> handle_call_rw(Req, From, await_commit(State))
    end.

%% Writes taken into a group commit; any other request first waits for
%% the group being written, so it sees every answered write.
group_write({put_doc, _, _}) -> true;
group_write({put_docs, _, _}) -> true;
group_write({delete_doc, _, _}) -> true;
group_write({outbox_ack, _, _}) -> true;
group_write(_) -> false.

write_request({put_doc, _, _}) -> true;
write_request({put_docs, _, _}) -> true;
write_request({delete_doc, _, _}) -> true;
write_request({outbox_ack, _, _}) -> true;
write_request({set_doc_embedding, _, _, _}) -> true;
write_request(sweep_retention) -> true;
write_request({sweep_retention, _}) -> true;
write_request(ttl_sweep) -> true;
write_request({resolve_conflict, _, _, _}) -> true;
write_request({put_version, _, _, _, _}) -> true;
write_request({put_doc_state, _, _, _, _}) -> true;
write_request({repair_doc_bodies, _, _}) -> true;
write_request({put_local_doc, _, _}) -> true;
write_request({put_local_doc, _, _, _}) -> true;
write_request({delete_local_doc, _}) -> true;
write_request({att_commit, _, _}) -> true;
write_request(_) -> false.

handle_call_rw(info, _From, #state{name = Name, keyspace = Keyspace,
                                   parent = Parent, fork_hlc = ForkHlc,
                                   config = Config, db_path = DbPath,
                                   store_ref = StoreRef, att_ref = AttRef,
                                   retention_period = RetentionPeriod,
                                   write_groups = WriteGroups} = State) ->
    AttFloor = case barrel_att_store:supports_sync(AttRef) of
        true -> barrel_att_store:att_floor(AttRef, Name);
        false -> undefined
    end,
    Info0 = #{
        name => Name,
        keyspace => Keyspace,
        read_only => maps:get(read_only, Config, false),
        config => Config,
        db_path => DbPath,
        pid => self(),
        retention_period => RetentionPeriod,
        history_floor => barrel_history:history_floor(StoreRef, Name),
        att_floor => AttFloor,
        write_groups => WriteGroups,
        storage => (barrel_store_rocksdb:storage_stats(StoreRef))#{
                       documents => doc_count_value(StoreRef, Name),
                       attachments => att_count_value(AttRef, Name)}
    },
    Info = case Parent of
        undefined -> Info0;
        _ -> Info0#{parent => Parent, fork_hlc => ForkHlc}
    end,
    {reply, {ok, Info}, State};

handle_call_rw({checkpoint_to, DocsPath, AttPath}, _From,
               #state{store_ref = StoreRef, att_ref = AttRef} = State) ->
    ForkHlc = alloc_hlc(State#state.name),
    Result = case barrel_store_rocksdb:checkpoint(StoreRef, DocsPath) of
        ok ->
            case barrel_att_store:checkpoint(AttRef, AttPath) of
                ok -> {ok, ForkHlc};
                {error, _} = AttErr -> AttErr
            end;
        {error, _} = Err ->
            Err
    end,
    {reply, Result, State};

handle_call_rw(get_store_ref, _From, #state{store_ref = StoreRef} = State) ->
    {reply, {ok, StoreRef}, State};

handle_call_rw(get_att_ref, _From, #state{att_ref = AttRef} = State) ->
    {reply, {ok, AttRef}, State};

%% Document writes: committed with the writes waiting behind them
handle_call_rw({put_doc, _, _} = Req, From, State) ->
    {noreply, group_commit(Req, From, State)};

handle_call_rw({put_docs, _, _} = Req, From, State) ->
    {noreply, group_commit(Req, From, State)};

handle_call_rw({delete_doc, _, _} = Req, From, State) ->
    {noreply, group_commit(Req, From, State)};

handle_call_rw({outbox_ack, _, _} = Req, From, State) ->
    {noreply, group_commit(Req, From, State)};

handle_call_rw({get_doc, DocId, Opts}, _From,
               #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_get_doc(StoreRef, DbName, DocId, Opts),
    {reply, Result, State};

handle_call_rw({get_docs, DocIds, Opts}, _From,
               #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_get_docs(StoreRef, DbName, DocIds, Opts),
    {reply, Result, State};

handle_call_rw({fold_docs, Fun, Acc}, From, State) ->
    handle_call_rw({fold_docs, Fun, Acc, #{}}, From, State);

handle_call_rw({fold_docs, Fun, Acc, Opts}, _From,
               #state{name = DbName, store_ref = StoreRef} = State) ->
    %% Fallback path (store ref not yet published): reuse the caller-side
    %% fold so prefix bounding and snapshot reads match the fast path.
    Result = barrel_docdb_reader:fold_docs(StoreRef, DbName, Fun, Acc, Opts),
    {reply, Result, State};

%% Attachment commits: serialized here, with feed HLCs from the writer
handle_call_rw({att_commit, Epoch, Fun}, _From, #state{name = Name} = State) ->
    Result = case persistent_term:get({barrel_writer, Name}, undefined) of
        Epoch ->
            try Fun(fun(Floor) -> alloc_hlc(Name, Floor) end)
            catch
                throw:{error, _} = Err -> Err;
                Class:Reason ->
                    logger:error("Database ~s: attachment commit failed: ~p:~p",
                                 [Name, Class, Reason]),
                    {error, {att_commit_failed, Reason}}
            end;
        _ ->
            {error, db_restarted}
    end,
    {reply, Result, State};

%% Embedding column write-back
handle_call_rw({set_doc_embedding, DocId, ExpectedRev, Vector}, _From,
               #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_set_doc_embedding(StoreRef, DbName, DocId, ExpectedRev, Vector),
    {reply, Result, State};

%% Retention sweep (manual trigger; also runs on the timer). Runs in the
%% shared worker so it does not block the writer loop; the reply is deferred
%% until the sweep finishes.
handle_call_rw(sweep_retention, From, State) ->
    handle_call_rw({sweep_retention, erlang:system_time(millisecond)}, From,
                   State);
handle_call_rw({sweep_retention, _NowMs}, _From,
               #state{retention_period = 0} = State) ->
    {reply, {ok, #{retention => infinite}}, State};
handle_call_rw({sweep_retention, NowMs}, From,
               #state{name = DbName, store_ref = StoreRef, att_ref = AttRef,
                      retention_period = RetentionPeriod} = State) ->
    Fun = fun() -> do_retention_sweep(StoreRef, AttRef, DbName,
                                      RetentionPeriod, purge_after(State),
                                      NowMs, State#state.config) end,
    {noreply, start_sweep(retention, Fun, From, false, State)};

%% Doc TTL sweep (manual trigger; also runs on the timer).
handle_call_rw(ttl_sweep, From,
               #state{name = DbName, store_ref = StoreRef,
                      ttl_sweep_batch = Batch} = State) ->
    Fun = fun() -> do_ttl_sweep(StoreRef, DbName, Batch) end,
    {noreply, start_sweep(ttl, Fun, From, false, State)};

%% Conflict operations
handle_call_rw({get_conflicts, DocId}, _From,
               #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_get_conflicts(StoreRef, DbName, DocId),
    {reply, Result, State};

handle_call_rw({resolve_conflict, DocId, BaseRev, Resolution}, _From,
               #state{name = DbName, store_ref = StoreRef,
                      config = Config} = State) ->
    Result = try
        do_resolve_conflict(StoreRef, DbName, Config, DocId, BaseRev,
                            Resolution)
    catch
        throw:{error, clock_skew} = Skew -> Skew;
        error:Reason -> {error, {invalid_resolution, Reason}}
    end,
    {reply, Result, State};

%% Replication operations
handle_call_rw({put_version, Doc, VersionToken, VVBin, Deleted}, _From,
               #state{name = DbName, store_ref = StoreRef,
                      config = Config} = State) ->
    Result = try
        do_put_version(StoreRef, DbName, Config, Doc,
                       barrel_version:from_token(VersionToken),
                       barrel_vv:decode(VVBin), Deleted)
    catch
        throw:{error, clock_skew} ->
            {error, clock_skew};
        Class:Reason ->
            {error, {invalid_version, {Class, Reason}}}
    end,
    {reply, Result, State};

handle_call_rw({put_doc_state, DocId, StateBin, Bodies, Vectors}, _From,
               #state{name = DbName, store_ref = StoreRef,
                      config = Config} = State) ->
    Result = try
        do_put_state(StoreRef, DbName, Config, DocId, StateBin, Bodies,
                     Vectors)
    catch
        throw:{error, clock_skew} -> {error, clock_skew};
        Class:Reason -> {error, {invalid_state, {Class, Reason}}}
    end,
    {reply, Result, State};

handle_call_rw({repair_doc_bodies, DocId, Bodies}, _From,
               #state{name = DbName, store_ref = StoreRef,
                      config = Config} = State) ->
    Result = try
        do_repair_bodies(StoreRef, DbName, Config, DocId, Bodies)
    catch
        Class:Reason -> {error, {invalid_state, {Class, Reason}}}
    end,
    {reply, Result, State};

handle_call_rw({get_doc_state, DocId}, _From,
               #state{name = DbName, store_ref = StoreRef} = State) ->
    {reply, do_get_state(StoreRef, DbName, DocId), State};

handle_call_rw({diff_states, DigestMap}, _From,
               #state{name = DbName, store_ref = StoreRef} = State) ->
    {reply, do_diff_states(StoreRef, DbName, DigestMap), State};

handle_call_rw({diff_versions, TokenMap}, _From,
               #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = try
        do_diff_versions(StoreRef, DbName, TokenMap)
    catch
        Class:Reason ->
            {error, {invalid_version, {Class, Reason}}}
    end,
    {reply, Result, State};

%% Local document operations
handle_call_rw({put_local_doc, DocId, Doc}, _From,
               #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_put_local_doc(StoreRef, DbName, DocId, Doc, #{}),
    {reply, Result, State};
handle_call_rw({put_local_doc, DocId, Doc, Opts}, _From,
               #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_put_local_doc(StoreRef, DbName, DocId, Doc, Opts),
    {reply, Result, State};

handle_call_rw({get_local_doc, DocId}, _From,
               #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_get_local_doc(StoreRef, DbName, DocId),
    {reply, Result, State};

handle_call_rw({delete_local_doc, DocId}, _From,
               #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_delete_local_doc(StoreRef, DbName, DocId),
    {reply, Result, State};

handle_call_rw({fold_local_docs, Prefix, Fun, Acc}, _From,
               #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_fold_local_docs(StoreRef, DbName, Prefix, Fun, Acc),
    {reply, Result, State};

handle_call_rw(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

%% @doc Handle asynchronous casts
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @doc Handle other messages
handle_info(stats_tick, #state{name = Name, store_ref = StoreRef,
                               stats_interval = Interval} = State) ->
    ok = barrel_metrics:set_db_storage(Name,
                                       barrel_store_rocksdb:storage_stats(StoreRef)),
    State1 = publish_att_count(publish_doc_count(State)),
    {noreply, State1#state{stats_timer = arm_stats(Interval)}};

%% The periodic retention/ttl work runs in a monitored worker so it never
%% blocks the writer loop. The periodic timer is re-armed when the worker
%% finishes.
handle_info(retention_sweep, #state{name = DbName, store_ref = StoreRef,
                                    att_ref = AttRef,
                                    retention_period = RetentionPeriod}
                             = State) ->
    Fun = fun() -> do_retention_sweep(StoreRef, AttRef, DbName,
                                      RetentionPeriod, purge_after(State),
                                      erlang:system_time(millisecond),
                                      State#state.config) end,
    {noreply, start_sweep(retention, Fun, none, true, State)};

handle_info(upload_gc, #state{name = DbName, att_ref = AttRef,
                              config = Config,
                              retention_interval = Interval} = State) ->
    %% upload rows only: cheap enough to run in the writer
    _ = gc_att_uploads(AttRef, DbName, Config),
    {noreply, State#state{upload_gc_timer =
                              erlang:send_after(Interval, self(), upload_gc)}};

handle_info(ttl_sweep, #state{name = DbName, store_ref = StoreRef,
                              ttl_sweep_batch = Batch} = State) ->
    Fun = fun() -> do_ttl_sweep(StoreRef, DbName, Batch) end,
    {noreply, start_sweep(ttl, Fun, none, true, State)};

handle_info({sweep_done, Kind, {writer_apply, Apply}}, State) ->
    %% The worker only read; the writer applies, re-checking each item
    %% against the current state. Long work comes back in chunks so
    %% writes interleave.
    {noreply, run_apply(Kind, Apply, detach_worker(Kind, State))};
handle_info({sweep_continue, Kind, Apply}, State) ->
    {noreply, run_apply(Kind, Apply, State)};
handle_info({sweep_done, Kind, Result}, State) ->
    {noreply, finish_sweep(Kind, Result, State)};

handle_info({'DOWN', _Ref, process, Pid, Reason},
            #state{sweep_workers = Workers} = State) ->
    %% Workers wrap their work in try/catch and always report via sweep_done,
    %% so a DOWN normally arrives after the kind is already cleared. This is
    %% the safety net for a worker that died without reporting.
    case sweep_kind_of_pid(Pid, Workers) of
        {ok, Kind} ->
            {noreply, finish_sweep(Kind, {error, {sweep_crashed, Reason}},
                                   State)};
        error ->
            {noreply, State}
    end;

handle_info({committed, Ref, _MoreSeg},
            #state{inflight = [{Ref, _} | Rest]} = State) ->
    {noreply, State#state{inflight = Rest}};

handle_info({'EXIT', Pid, Reason}, #state{committer = Pid} = State) ->
    {stop, {committer_down, Reason}, State#state{committer = undefined,
                                                 inflight = []}};
handle_info(_Info, State) ->
    {noreply, State}.

%% @doc Clean up when terminating
terminate(Reason, #state{committer = Committer} = State) when is_pid(Committer) ->
    #state{} = await_commit(State),
    unlink(Committer),
    exit(Committer, kill),
    terminate(Reason, State#state{committer = undefined});
terminate(_Reason, #state{name = Name, store_ref = StoreRef, att_ref = AttRef,
                          retention_timer = RetentionTimer,
                          stats_timer = StatsTimer,
                          upload_gc_timer = UploadGcTimer,
                          ttl_timer = TtlTimer,
                          sweep_workers = Workers}) ->
    %% Workers only read; stop them before the stores close
    ok = stop_workers(Workers),
    _ = case TtlTimer of
            undefined -> ok;
            _ -> erlang:cancel_timer(TtlTimer)
        end,
    _ = case StatsTimer of
            undefined -> ok;
            _ -> erlang:cancel_timer(StatsTimer)
        end,
    _ = case UploadGcTimer of
            undefined -> ok;
            _ -> erlang:cancel_timer(UploadGcTimer)
        end,
    %% Cancel retention timer
    _ =
      case RetentionTimer of
          undefined -> ok;
          _ -> erlang:cancel_timer(RetentionTimer)
      end,
    %% Unregister BEFORE closing either store so caller-side readers
    %% (barrel_docdb_reader, barrel_doc_body_store) stop picking up a ref
    %% that is about to be closed, and a read that fails on a closed store
    %% finds the database already gone. In-flight readers that already
    %% hold the ref are handled by the reader's badarg guard.
    persistent_term:erase({barrel_db, Name}),
    persistent_term:erase({barrel_store, Name}),
    persistent_term:erase({barrel_att_ref, Name}),
    persistent_term:erase({barrel_source, Name}),
    persistent_term:erase({barrel_writer, Name}),
    ok = barrel_doc_chain:close(StoreRef),
    ok = barrel_keyspace:uninstall(Name),
    ok = barrel_channel:uninstall(Name),
    %% Close attachment store
    _ = 
      case AttRef of
          undefined -> ok;
          _ -> barrel_att_store:close(AttRef)
      end,
    %% Close document store (includes body CF)
    _ =
      case StoreRef of
          undefined -> ok;
          _ -> barrel_store_rocksdb:close(StoreRef)
      end,
    logger:info("Database ~s stopped", [Name]),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

stop_workers(Workers) ->
    Pids = [Pid || {Pid, _, _} <- maps:values(Workers), is_pid(Pid)],
    MRefs = [begin
                 MRef = erlang:monitor(process, Pid),
                 exit(Pid, kill),
                 MRef
             end || Pid <- Pids],
    lists:foreach(fun(MRef) ->
                      receive {'DOWN', MRef, process, _, _} -> ok
                      after 5000 -> ok
                      end
                  end, MRefs).

%% @private Resolve the encryption spec against the CRYPTO marker file
%% (a cleartext key-check token next to TIMELINE) and build the
%% EncryptedEnv. Fail-closed matrix: wrong key -> wrong_encryption_key,
%% plaintext open of an encrypted db -> db_is_encrypted, encrypted open
%% of an existing plaintext db -> cannot_encrypt_existing_db.
init_encryption(Config, Keyspace, DbPath) ->
    Spec = maps:get(encryption, Config, disabled),
    case barrel_keyprovider:key_for_db(Spec, Keyspace) of
        {ok, plaintext} ->
            case filelib:is_regular(crypto_marker_path(DbPath)) of
                true -> {error, db_is_encrypted};
                false -> {ok, undefined}
            end;
        {ok, Key} ->
            check_crypto_marker(DbPath, Key,
                                maps:get(read_only, Config, false));
        {error, Reason} ->
            {error, {encryption_key_error, Reason}}
    end.

check_crypto_marker(DbPath, Key, ReadOnly) ->
    MarkerPath = crypto_marker_path(DbPath),
    case file:read_file(MarkerPath) of
        {ok, Token} ->
            case barrel_crypto:key_check_verify(Key, Token) of
                true -> new_encrypted_env(Key);
                false -> {error, wrong_encryption_key}
            end;
        {error, enoent} ->
            case {filelib:is_dir(filename:join(DbPath, "docs")), ReadOnly} of
                {true, _} ->
                    {error, cannot_encrypt_existing_db};
                {false, true} ->
                    {error, {read_only_store_missing, DbPath}};
                {false, _} ->
                    case write_crypto_marker(MarkerPath, Key) of
                        ok ->
                            new_encrypted_env(Key);
                        {error, Reason} ->
                            {error, {crypto_marker_write_failed, Reason}}
                    end
            end;
        {error, Reason} ->
            {error, {crypto_marker_read_failed, Reason}}
    end.

%% rocksdb:new_env/1 cannot fail per its contract. The key travels with
%% the env for backends that encrypt outside RocksDB.
new_encrypted_env(Key) ->
    {ok, Env} = rocksdb:new_env({encrypted, Key}),
    {ok, #{env => Env, key => Key}}.

%% Written atomically (temp + rename) once, at the first encrypted open.
write_crypto_marker(MarkerPath, Key) ->
    Token = barrel_crypto:key_check_new(Key),
    ok = filelib:ensure_dir(MarkerPath),
    Tmp = MarkerPath ++ ".tmp",
    case file:write_file(Tmp, Token) of
        ok -> file:rename(Tmp, MarkerPath);
        {error, _} = Err -> Err
    end.

crypto_marker_path(DbPath) ->
    filename:join(DbPath, "CRYPTO").

add_env_opt(Opts, undefined) -> Opts;
add_env_opt(Opts, Env) -> Opts#{env => Env}.

crypto_env(undefined) -> undefined;
crypto_env(#{env := Env}) -> Env.

add_crypto_opt(Opts, undefined) -> Opts;
add_crypto_opt(Opts, #{key := Key}) -> Opts#{crypto => #{key => Key}}.

%% @doc Start (or join) the single worker for a sweep kind. `From' is a
%% gen_server From to reply to (or `none' for a timer-driven run); `Periodic'
%% marks a timer contribution so the worker re-arms the periodic timer when
%% it finishes. At most one worker per kind runs at a time.
start_sweep(Kind, Fun, From, Periodic, #state{sweep_workers = Workers} = State) ->
    case maps:find(Kind, Workers) of
        {ok, {Pid, Waiters, WasPeriodic}} ->
            %% Already running: attach this caller and OR in the periodic flag.
            Workers1 = Workers#{Kind => {Pid, add_waiter(From, Waiters),
                                         WasPeriodic orelse Periodic}},
            State#state{sweep_workers = Workers1};
        error ->
            Server = self(),
            %% linked so it dies with a killed writer; DOWN reports it
            {Pid, _MRef} = spawn_opt(fun() ->
                Result = try Fun()
                         catch C:E:St -> {error, {sweep_failed, C, E, St}}
                         end,
                Server ! {sweep_done, Kind, Result}
            end, [link, monitor]),
            Workers1 = Workers#{Kind => {Pid, add_waiter(From, []), Periodic}},
            State#state{sweep_workers = Workers1}
    end.

add_waiter(none, Waiters) -> Waiters;
add_waiter(From, Waiters) -> [From | Waiters].

%% A sweep worker finished: reply to any waiting callers, and re-arm the
%% periodic timer when a timer contributed to this run.
finish_sweep(Kind, Result, #state{sweep_workers = Workers} = State) ->
    case maps:take(Kind, Workers) of
        {{_Pid, Waiters, Periodic}, Workers1} ->
            lists:foreach(fun(From) -> gen_server:reply(From, Result) end,
                          lists:reverse(Waiters)),
            State1 = State#state{sweep_workers = Workers1},
            case Periodic of
                true -> reschedule_sweep(Kind, Result, State1);
                false -> State1
            end;
        error ->
            State
    end.

%% The worker that reported is gone: keep the kind (and its waiters)
%% busy while the writer applies, without a pid a DOWN could match.
detach_worker(Kind, #state{sweep_workers = Workers} = State) ->
    case maps:find(Kind, Workers) of
        {ok, {_Pid, Waiters, Periodic}} ->
            State#state{sweep_workers =
                            Workers#{Kind => {undefined, Waiters, Periodic}}};
        error ->
            State
    end.

run_apply(Kind, Apply, State) ->
    State1 = await_commit(State),
    Step = try Apply()
           catch C:E:St -> {done, {error, {sweep_failed, C, E, St}}}
           end,
    case Step of
        {done, Result} ->
            finish_sweep(Kind, Result, State1);
        {more, Next} ->
            self() ! {sweep_continue, Kind, Next},
            State1
    end.

%% Re-arm the periodic timer for a kind. A full TTL batch comes back quickly
%% to drain the backlog; everything else waits the configured interval.
reschedule_sweep(retention, _Result, #state{retention_interval = I} = State) ->
    State#state{retention_timer =
                    erlang:send_after(I, self(), retention_sweep)};
reschedule_sweep(ttl, Result, #state{ttl_sweep_interval = Interval,
                                     ttl_sweep_batch = Batch} = State) ->
    NextIn = case Result of
        {ok, Swept} when is_integer(Swept), Swept >= Batch -> 100;
        _ -> Interval
    end,
    State#state{ttl_timer = erlang:send_after(NextIn, self(), ttl_sweep)}.

%% Re-arm the storage gauge refresh. The first refresh after open comes
%% within a second, so a new database reports its size right away.
arm_stats(Interval) -> arm_stats(Interval, Interval).

arm_stats(0, _Delay) -> undefined;
arm_stats(_Interval, Delay) -> erlang:send_after(Delay, self(), stats_tick).

sweep_kind_of_pid(Pid, Workers) ->
    Match = [K || {K, {P, _, _}} <- maps:to_list(Workers), P =:= Pid],
    case Match of
        [Kind | _] -> {ok, Kind};
        [] -> error
    end.

%% @private Release the store handles and registrations leaked by a
%% crashed instance, even if callers still hold them (their next call
%% fails as on a closed database). Its helpers are linked and died with
%% it. A live owner keeps everything (the open then fails on the
%% RocksDB lock, the correct answer for a double open).
-spec release_stale_refs(binary()) -> ok.
release_stale_refs(Name) ->
    Owner = persistent_term:get({barrel_db, Name}, undefined),
    case is_pid(Owner) andalso is_process_alive(Owner) of
        true ->
            ok;
        false ->
            StoreRef = persistent_term:get({barrel_store, Name}, undefined),
            AttRef = persistent_term:get({barrel_att_ref, Name}, undefined),
            case {StoreRef, AttRef} of
                {undefined, undefined} ->
                    ok;
                _ ->
                    logger:warning(
                        "Database ~s: releasing store handles leaked by a "
                        "crashed instance", [Name]),
                    persistent_term:erase({barrel_db, Name}),
                    persistent_term:erase({barrel_store, Name}),
                    persistent_term:erase({barrel_att_ref, Name}),
                    persistent_term:erase({barrel_source, Name}),
                    persistent_term:erase({barrel_writer, Name}),
                    ok = barrel_keyspace:uninstall(Name),
                    ok = barrel_channel:uninstall(Name),
                    close_stale(fun barrel_att_store:close/1, AttRef),
                    close_stale(fun barrel_store_rocksdb:close/1, StoreRef)
            end
    end.

close_stale(_Close, undefined) ->
    ok;
close_stale(Close, Ref) ->
    _ = try Close(Ref) catch _:_ -> ok end,
    ok.

%%====================================================================
%% Retention Sweep
%%====================================================================

%% @doc Prune everything older than the retention window and advance
%% the history floor. Walks the retained history up to the cutoff; each
%% swept entry drops its history row, plus:
%% - the superseded chain row and archived body of a version that is no
%%   longer current (live conflict siblings are kept until resolved)
%% - the whole document when the entry is the current version of an
%%   unconflicted tombstone (full forget: entity, bodies, chain, feed
%%   row)
%% One pass of the doc TTL sweeper: fold the expiry index up to now,
%% re-check each doc's entity (a rewrite may have bumped or cleared the
%% TTL, leaving a stale row), tombstone the ones still due through the
%% normal delete path (feed, channels, and replication all correct),
%% and drop stale rows.
do_ttl_sweep(StoreRef, DbName, Batch) ->
    Keys = ttl_due_keys(StoreRef, DbName, Batch),
    {writer_apply, fun() -> {done, ttl_apply(StoreRef, DbName, Keys)} end}.

ttl_due_keys(StoreRef, DbName, Batch) ->
    Ks = barrel_keyspace:resolve(DbName),
    NowMs = erlang:system_time(millisecond),
    Start = barrel_store_keys:doc_expiry_prefix(Ks),
    End = barrel_store_keys:doc_expiry_upto(Ks, NowMs),
    Fold = fun(Key, _Value, Acc) ->
        case length(Acc) >= Batch of
            true -> {stop, Acc};
            false -> {ok, [Key | Acc]}
        end
    end,
    lists:reverse(
        barrel_store_rocksdb:fold_range(StoreRef, Start, End, Fold, [])).

%% In the writer: re-read each doc, delete the ones still due.
ttl_apply(StoreRef, DbName, Keys) ->
    Ks = barrel_keyspace:resolve(DbName),
    Swept = lists:foldl(
        fun(Key, N) ->
            {ExpiresMs, DocId} =
                barrel_store_keys:decode_doc_expiry_key(Ks, Key),
            Due = case read_current(StoreRef, DbName, DocId) of
                #{expires_at := E} when E =:= ExpiresMs -> due;
                undefined -> stale;
                _Bumped -> stale
            end,
            case Due of
                due ->
                    case do_delete_doc(StoreRef, DbName, DocId, #{}) of
                        {ok, _} -> N + 1;
                        {error, not_found} ->
                            drop_expiry_row(StoreRef, Key),
                            N
                    end;
                stale ->
                    drop_expiry_row(StoreRef, Key),
                    N
            end
        end, 0, Keys),
    {ok, Swept}.

drop_expiry_row(StoreRef, Key) ->
    ok = barrel_store_rocksdb:write_batch(StoreRef, [{delete, Key}], #{}).

%% All deletes and the floor advance commit in one batch; re-running a
%% sweep is a no-op (deletes of missing keys do nothing).
%% Seconds after which a delete's stub is purged; 0 (default) keeps
%% stubs for good.
purge_after(#state{config = Config}) ->
    maps:get(tombstone_purge_after, Config, 0).

%% In the writer: drop staged uploads older than att_upload_timeout.
gc_att_uploads(true, _AttRef, _DbName, _Config) ->
    0;
gc_att_uploads(false, AttRef, DbName, Config) ->
    gc_att_uploads(AttRef, DbName, Config).

gc_att_uploads(AttRef, DbName, Config) ->
    MaxAgeMs = maps:get(att_upload_timeout, Config,
                        ?DEFAULT_ATT_UPLOAD_TIMEOUT) * 1000,
    {ok, #{uploads_removed := N}} =
        barrel_att_store:gc_uploads(AttRef, DbName, MaxAgeMs),
    N.

do_retention_sweep(StoreRef, AttRef, DbName, RetentionPeriod, PurgeAfter,
                   NowMs, Config) ->
    Ks = barrel_keyspace:resolve(DbName),
    CutoffMs = NowMs - RetentionPeriod * 1000,
    Cutoff = barrel_hlc:from_wall_time(CutoffMs),
    Zero = #{history_swept => 0, superseded_removed => 0,
             docs_stubbed => 0, docs_purged => 0},
    Purge = purge_candidates(StoreRef, DbName, PurgeAfter, NowMs),
    Entries = lists:reverse(barrel_history:fold(
        StoreRef, DbName,
        fun(Entry, Acc) -> {ok, [Entry | Acc]} end,
        [],
        #{to => Cutoff})),
    %% Channel leave rows are events, not state: age them out
    {LeaveOps, LeavesSwept} = sweep_channel_leaves(StoreRef, DbName, Cutoff),
    FloorOp = {put, barrel_store_keys:db_meta(Ks, <<"history_floor">>),
               barrel_hlc:encode(Cutoff)},
    Final = fun(Stats0) ->
        %% in the writer, serialized with attachment commits: feed
        %% tombstones follow the stub purge window (kept for good when 0,
        %% so a resync from first still sees every delete) and abandoned
        %% uploads go
        AttSwept = case PurgeAfter > 0
                        andalso barrel_att_store:supports_sync(AttRef) of
            true ->
                AttCutoff = barrel_hlc:from_wall_time(
                              NowMs - PurgeAfter * 1000),
                {ok, #{tombstones_swept := N}} =
                    barrel_att_store:sweep_att_feed(AttRef, DbName, AttCutoff),
                N;
            false ->
                0
        end,
        Uploads = gc_att_uploads(AttRef, DbName, Config),
        {PurgeOps, Stats} = purge_ops(StoreRef, DbName, Purge, Stats0),
        ok = barrel_store_rocksdb:write_batch(StoreRef,
                                              [FloorOp | LeaveOps ++ PurgeOps],
                                              #{}),
        %% the stub gauges are measured off the writer: a feed scan
        _ = spawn(fun() ->
                      try stub_census(StoreRef, DbName) of
                          {Stubs, StubBytes} ->
                              barrel_metrics:set_db_stubs(DbName, Stubs, StubBytes)
                      catch _:_ -> ok   % the database closed meanwhile
                      end
                  end),
        {ok, Stats#{floor => Cutoff, channel_leaves_swept => LeavesSwept,
                    att_tombstones_swept => AttSwept,
                    att_uploads_removed => Uploads}}
    end,
    {writer_apply,
     fun() -> retention_apply(StoreRef, DbName, Entries, Zero, Final) end}.

-define(RETENTION_CHUNK, 256).

%% In the writer: build each entry's ops from the current state, one
%% batch per chunk; the floor moves only after the last chunk.
retention_apply(StoreRef, DbName, Entries, Stats, Final) ->
    {Chunk, Rest} = lists:split(min(?RETENTION_CHUNK, length(Entries)),
                                Entries),
    {Ops, Stats1} = lists:foldl(
        fun(Entry, {OpsAcc, StatsAcc}) ->
            {EntryOps, StatsAcc2} =
                sweep_entry_ops(StoreRef, DbName, Entry, StatsAcc),
            {EntryOps ++ OpsAcc, StatsAcc2}
        end,
        {[], Stats}, Chunk),
    ok = case Ops of
        [] -> ok;
        _ -> barrel_store_rocksdb:write_batch(StoreRef, Ops, #{})
    end,
    case Rest of
        [] -> {done, Final(Stats1)};
        _ -> {more, fun() -> retention_apply(StoreRef, DbName, Rest, Stats1,
                                             Final) end}
    end.

%% @private Delete leave rows older than the cutoff in every configured
%% channel (member rows are live state and are never age-swept).
sweep_channel_leaves(StoreRef, DbName, Cutoff) ->
    Ks = barrel_keyspace:resolve(DbName),
    lists:foldl(
        fun(Channel, {OpsAcc, CountAcc}) ->
            Start = barrel_store_keys:channel_prefix(Ks, Channel),
            End = barrel_store_keys:channel_key(Ks, Channel, Cutoff),
            barrel_store_rocksdb:fold_range(
                StoreRef, Start, End,
                fun(Key, Value, {InnerOps, InnerCount}) ->
                    case barrel_channel:decode_row(Value) of
                        #{flag := leave} ->
                            {ok, {[{delete, Key} | InnerOps],
                                  InnerCount + 1}};
                        _ ->
                            {ok, {InnerOps, InnerCount}}
                    end
                end,
                {OpsAcc, CountAcc})
        end,
        {[], 0},
        barrel_channel:names(DbName)).

%% @private Ops for one expired history entry.
sweep_entry_ops(StoreRef, DbName,
                #{hlc := Hlc, id := DocId, version := Token}, Stats) ->
    Ks = barrel_keyspace:resolve(DbName),
    HistDel = {delete, barrel_store_keys:history_key(Ks, Hlc)},
    VersionEnc = barrel_version:encode(barrel_version:from_token(Token)),
    EntityKey = barrel_store_keys:doc_entity(Ks, DocId),
    case barrel_store_rocksdb:get_entity(StoreRef, EntityKey) of
        not_found ->
            %% Doc already fully forgotten
            {[HistDel], inc_sweep(history_swept, Stats)};
        {ok, Columns} ->
            CurrentEnc = proplists:get_value(?COL_VERSION, Columns),
            Deleted = proplists:get_value(?COL_DELETED, Columns, <<"false">>)
                          =:= <<"true">>,
            NConflicts = proplists:get_value(?COL_NCONFLICTS, Columns, 0),
            case CurrentEnc of
                VersionEnc when Deleted andalso NConflicts =:= 0 ->
                    %% Expired delete: keep a stub (its full replicated
                    %% state and feed row), drop the bodies and history
                    {stub_ops(StoreRef, DbName, DocId) ++ [HistDel],
                     inc_sweep(docs_stubbed, Stats)};
                VersionEnc ->
                    %% Still current (live doc, or tombstone with
                    %% unresolved conflicts): state is retained
                    {[HistDel], inc_sweep(history_swept, Stats)};
                _Other ->
                    sweep_old_version_ops(StoreRef, DbName, DocId,
                                          VersionEnc, HistDel, Stats)
            end
    end.

%% @private A version that is no longer current: drop its superseded
%% chain row and archived body. Live conflict siblings stay until
%% resolved.
sweep_old_version_ops(StoreRef, DbName, DocId, VersionEnc, HistDel, Stats) ->
    Ks = barrel_keyspace:resolve(DbName),
    ChainKey = barrel_store_keys:doc_version(Ks, DocId, VersionEnc),
    case barrel_store_rocksdb:get(StoreRef, ChainKey) of
        {ok, <<1:8, _/binary>>} ->
            %% Live conflict sibling
            {[HistDel], inc_sweep(history_swept, Stats)};
        {ok, _Superseded} ->
            BodyKey = barrel_store_keys:doc_body_rev(Ks, DocId, VersionEnc),
            {[HistDel, {delete, ChainKey}, {body_delete, BodyKey},
              barrel_doc_vectors:delete_op(Ks, DocId, VersionEnc)],
             inc_sweep(superseded_removed, Stats)};
        not_found ->
            {[HistDel], inc_sweep(history_swept, Stats)}
    end.

%% @private A deleted document becomes a stub: the entity (with its
%% context) and the feed row stay, so a peer pulling from the start gets
%% the delete and an old copy is still recognised as replaced; the
%% current body, the chain rows and archived bodies go.
stub_ops(StoreRef, DbName, DocId) ->
    Ks = barrel_keyspace:resolve(DbName),
    [{body_delete, barrel_store_keys:doc_body(Ks, DocId)}
     | chain_delete_ops(StoreRef, Ks, DocId)].

chain_delete_ops(StoreRef, Ks, DocId) ->
    barrel_doc_chain:fold(
        StoreRef, Ks, DocId,
        fun(Key, VEnc, _Value, Acc) ->
            {ok, [{delete, Key},
                  {body_delete, barrel_store_keys:doc_body_rev(Ks, DocId, VEnc)},
                  barrel_doc_vectors:delete_op(Ks, DocId, VEnc)
                  | Acc]}
        end,
        []).

%% @doc Count the delete stubs and the bytes they hold: a stub is a
%% deleted document without a body; its bytes are its entity (state)
%% and its change row.
stub_census(StoreRef, DbName) ->
    Ks = barrel_keyspace:resolve(DbName),
    {ok, Census, _} = barrel_changes:fold_changes_compact(
        StoreRef, DbName, first,
        fun({DocId, Hlc, _Rev, true, _N}, {Count, Bytes} = Acc) ->
               case barrel_store_rocksdb:body_get(
                      StoreRef, barrel_store_keys:doc_body(Ks, DocId)) of
                   not_found ->
                       EntityKey = barrel_store_keys:doc_entity(Ks, DocId),
                       FeedKey = barrel_store_keys:doc_hlc(Ks, Hlc),
                       Size = raw_size(StoreRef, EntityKey) + byte_size(EntityKey)
                           + raw_size(StoreRef, FeedKey) + byte_size(FeedKey),
                       {ok, {Count + 1, Bytes + Size}};
                   _ ->
                       {ok, Acc}
               end;
           (_Change, Acc) ->
               {ok, Acc}
        end,
        {0, 0}),
    Census.

raw_size(StoreRef, Key) ->
    case barrel_store_rocksdb:get(StoreRef, Key) of
        {ok, V} -> byte_size(V);
        _ -> 0
    end.

%% @private Deleted documents whose feed row is older than the purge
%% window (tombstone_purge_after seconds): read in the worker, re-checked
%% in the writer.
purge_candidates(_StoreRef, _DbName, 0, _NowMs) ->
    [];
purge_candidates(StoreRef, DbName, PurgeAfter, NowMs) ->
    Cutoff = barrel_hlc:from_wall_time(NowMs - PurgeAfter * 1000),
    {ok, Candidates, _} = barrel_changes:fold_changes_compact(
        StoreRef, DbName, first,
        fun({DocId, Hlc, _Rev, true, 0}, Acc) ->
               case barrel_hlc:less(Hlc, Cutoff) of
                   true -> {ok, [{DocId, Hlc} | Acc]};
                   false -> {stop, Acc}
               end;
           ({_DocId, Hlc, _Rev, _Deleted, _N}, Acc) ->
               case barrel_hlc:less(Hlc, Cutoff) of
                   true -> {ok, Acc};
                   false -> {stop, Acc}
               end
        end,
        []),
    lists:reverse(Candidates).

%% In the writer: purge a stub only if it is still the same delete.
purge_ops(StoreRef, DbName, Candidates, Stats) ->
    Ks = barrel_keyspace:resolve(DbName),
    lists:foldl(
        fun({DocId, Hlc}, {OpsAcc, StatsAcc}) ->
            case barrel_store_rocksdb:get_entity(
                   StoreRef, barrel_store_keys:doc_entity(Ks, DocId)) of
                {ok, Columns} ->
                    Same = barrel_hlc:decode(proplists:get_value(?COL_HLC, Columns))
                               =:= Hlc
                        andalso proplists:get_value(?COL_DELETED, Columns)
                               =:= <<"true">>
                        andalso proplists:get_value(?COL_NCONFLICTS, Columns, 0)
                               =:= 0,
                    case Same of
                        true ->
                            {forget_doc_ops(StoreRef, DbName, DocId, Columns)
                                 ++ OpsAcc,
                             inc_sweep(docs_purged, StatsAcc)};
                        false ->
                            {OpsAcc, StatsAcc}
                    end;
                not_found ->
                    {OpsAcc, StatsAcc}
            end
        end,
        {[], Stats}, Candidates).

%% @private Full forget of an expired tombstone: entity, current body,
%% every chain row and archived body, and the live-feed row. The path
%% index rows were already removed when the doc was deleted.
forget_doc_ops(StoreRef, DbName, DocId, Columns) ->
    Ks = barrel_keyspace:resolve(DbName),
    ChangeHlc = barrel_hlc:decode(proplists:get_value(?COL_HLC, Columns)),
    ChainOps = barrel_doc_chain:fold(
        StoreRef, Ks, DocId,
        fun(Key, VEnc, _Value, Acc) ->
            {ok, [{delete, Key},
                  {body_delete,
                   barrel_store_keys:doc_body_rev(Ks, DocId, VEnc)},
                  barrel_doc_vectors:delete_op(Ks, DocId, VEnc)
                  | Acc]}
        end,
        []),
    %% Channel rows live exactly at the doc's current change HLC (the
    %% writer invariant), so the forget covers them too
    ChannelDels = [{delete,
                    barrel_store_keys:channel_key(Ks, C, ChangeHlc)}
                   || C <- barrel_channel:names(DbName)],
    [{entity_delete, barrel_store_keys:doc_entity(Ks, DocId)},
     {body_delete, barrel_store_keys:doc_body(Ks, DocId)},
     {delete, barrel_store_keys:doc_hlc(Ks, ChangeHlc)}
     | ChannelDels ++ ChainOps].

inc_sweep(Key, Stats) ->
    maps:update_with(Key, fun(N) -> N + 1 end, Stats).

%%====================================================================
%% Document Operations
%%====================================================================

%% Wide column names for the document entity are shared with the caller-side
%% reader; they live in barrel_docdb.hrl (?COL_VERSION, ?COL_VV, ...).

%%====================================================================
%% Group commit
%%====================================================================

%% @private Commit a write request together with the write requests
%% already waiting in the mailbox: one batch, one sync. Each request is
%% built in arrival order and answered after the batch is written.
group_commit(Req, From, State) ->
    case new_wreq(Req, From) of
        {ok, W} ->
            run_group(W, State);
        {error, _} = Err ->
            gen_server:reply(From, Err),
            State
    end.

new_wreq({put_doc, Doc, Opts}, From) ->
    wreq(put_doc, From, Opts, [{doc, Doc, #{}}]);
new_wreq({put_docs, Docs, Opts}, From) ->
    wreq(put_docs, From, Opts, [doc_item(D) || D <- Docs]);
new_wreq({delete_doc, DocId, Opts}, From) ->
    wreq(delete_doc, From, Opts, [{delete, DocId}]);
new_wreq({outbox_ack, Tag, Hlcs}, From) ->
    {ok, #wreq{from = From, kind = outbox_ack, pending = [{ack, Tag, Hlcs}]}}.

%% A put_docs entry: a document, or a document with its own options.
doc_item({prepared, _} = Prepared) -> {doc, Prepared, #{}};
doc_item({Doc, DocOpts}) when is_map(DocOpts) -> {doc, Doc, DocOpts};
doc_item(Doc) -> {doc, Doc, #{}}.

wreq(Kind, From, Opts, Items) ->
    case validate_prov_opt(Opts) of
        {ok, Opts1} ->
            {ok, #wreq{from = From, kind = Kind, opts = Opts1, pending = Items,
                       sync = sync_opt(Opts1)}};
        {error, _} = Err ->
            Err
    end.

%% A request that touches a doc id already written in the group closes
%% it and opens the next one, so its read sees the earlier write. A
%% group is handed to the committer while the writer builds the next.
run_group(W, State) ->
    {Fill, State1} = fetch(#fill{pending = [W]}, State),
    run_group(Fill, #grp{}, State1).

run_group(#fill{pending = []}, #grp{} = G, State) ->
    dispatch_group(G, State);
run_group(Fill0, G0, State0) ->
    {Grp, Carry, Fill1, State} = drain(G0, Fill0, State0),
    %% a read of an id this group writes, taken while it was built, is stale
    Fill = forget(Grp, Fill1),
    case {Carry, Fill} of
        {none, #fill{pending = []}} ->
            last_group(Grp, State);
        {none, _} ->
            run_group(Fill, #grp{}, dispatch_group(Grp, State));
        {#wreq{done = Done}, _} ->
            {State1, MoreSeg} = commit_group(Grp, State),
            #fill{pending = Pending} = Fill,
            run_group(Fill#fill{pending = [Carry#wreq{done = MoreSeg ++ Done}
                                           | Pending]},
                      #grp{}, State1)
    end.

%% With nothing else to build or being written, the writer writes the
%% group itself: a lone writer does not pay the handover.
last_group(#grp{ref = undefined} = Grp, #state{inflight = []} = State) ->
    write_here(Grp, State);
last_group(Grp, State) ->
    dispatch_group(Grp, State).

write_here(#grp{reqs = []}, State) ->
    State;
write_here(#grp{reqs = Reqs, count = N, unsent = Unsent},
           #state{name = DbName, store_ref = StoreRef} = State) ->
    {Sync, Plan} = group_plan(Reqs),
    Batch = build_batch(StoreRef, lists:append(lists:reverse(Unsent))),
    _ = write_and_answer(StoreRef, DbName, Batch, Sync, Plan),
    ok = barrel_metrics:observe_write_group(DbName, N),
    count_group(N, State).

forget(#grp{ids = Ids}, #fill{cache = Cache} = Fill) ->
    Fill#fill{cache = maps:without(maps:keys(Ids), Cache)}.

%% Take the write requests waiting in the mailbox, without blocking,
%% then read the current state of the documents they write.
fetch(#fill{pending = Pending0, cache = Cache} = Fill,
      #state{max_group = Max} = State0) ->
    Pending = Pending0 ++ take_writes(Max - length(Pending0)),
    Ids = lists:usort(lists:flatmap(fun wreq_ids/1, Pending)) -- maps:keys(Cache),
    State = await_ids(Ids, State0),
    {Fill#fill{pending = Pending, cache = maps:merge(Cache, prefetch(Ids, State))},
     State}.

take_writes(N) when N =< 0 ->
    [];
take_writes(N) ->
    receive
        {'$gen_call', From, {put_doc, _, _} = Req} -> take_write(Req, From, N);
        {'$gen_call', From, {put_docs, _, _} = Req} -> take_write(Req, From, N);
        {'$gen_call', From, {delete_doc, _, _} = Req} -> take_write(Req, From, N);
        {'$gen_call', From, {outbox_ack, _, _} = Req} -> take_write(Req, From, N)
    after 0 ->
        []
    end.

take_write(Req, From, N) ->
    case new_wreq(Req, From) of
        {ok, W} ->
            [W | take_writes(N - 1)];
        {error, _} = Err ->
            gen_server:reply(From, Err),
            take_writes(N)
    end.

%% Ids a request reads, when known before it is built.
wreq_ids(#wreq{pending = Items}) ->
    [Id || Item <- Items, Id <- item_id(Item)].

item_id({doc, {prepared, #prep{rec = #{id := Id}}}, _}) -> [Id];
item_id({doc, _, _}) -> [];
item_id({delete, Id}) -> [Id];
item_id({ack, _, _}) -> [].

await_ids(Ids, #state{inflight = Inflight} = State) ->
    case lists:any(fun({_Ref, Written}) ->
                       lists:any(fun(Id) -> is_map_key(Id, Written) end, Ids)
                   end, Inflight) of
        true -> await_commit(State);
        false -> State
    end.

prefetch([], _State) ->
    #{};
prefetch(Ids, #state{store_ref = StoreRef, keyspace = Ks}) ->
    read_currents(StoreRef, Ks, Ids).

%% Build the waiting requests into the group, taking more from the
%% mailbox when they run out. Stops when the group is full.
drain(#grp{count = N} = G, Fill, #state{max_group = Max} = State) when N >= Max ->
    {G, none, Fill, State};
drain(#grp{items = I, sync = false} = G, Fill, #state{write_chunk = Chunk} = State)
  when I >= Chunk ->
    {G, none, Fill, State};
drain(G, #fill{pending = []} = Fill0, State0) ->
    case fetch(Fill0, State0) of
        {#fill{pending = []} = Fill, State} -> {G, none, Fill, State};
        {Fill, State} -> drain(G, Fill, State)
    end;
drain(G, #fill{pending = [W | Rest]} = Fill, State) ->
    case add_req(W, G, Fill#fill{pending = Rest}, State) of
        {G1, none, Fill1, State1} -> drain(G1, Fill1, State1);
        Closed -> Closed
    end.

%% Build the request's items into the group. Returns {Group, none, Fill,
%% State}, or {Group, Carry, Fill, State} when an item's id is already
%% written in the group. An id of a group being committed is read after
%% that commit.
add_req(#wreq{pending = [], sync = WSync} = W,
        #grp{reqs = Reqs, count = N, sync = GSync} = G, Fill, State) ->
    {G#grp{reqs = [W | Reqs], count = N + 1, sync = GSync orelse WSync},
     none, Fill, State};
add_req(#wreq{pending = [Item | Rest], seg = Seg, opts = Opts} = W,
        #grp{ids = Ids} = G, #fill{cache = Cache} = Fill,
        #state{name = DbName, keyspace = Ks, store_ref = StoreRef} = State0) ->
    case prepare_item(Item, Ks) of
        {error, _} = Err ->
            add_req(W#wreq{pending = Rest, seg = [{Err, [], none} | Seg]}, G,
                    Fill, State0);
        {Id, _, _} when is_map_key(Id, Ids) ->
            close_group(W, G, Fill, State0);
        {Id, Prepared, ItemOpts} ->
            {Old, State} = current(Id, Cache, State0),
            {_, ItemOps, _} = Built =
                build_item(Prepared, Old, StoreRef, DbName,
                           maps:merge(Opts, ItemOpts)),
            W1 = W#wreq{pending = Rest, seg = [Built | Seg],
                        sync = W#wreq.sync orelse sync_opt(ItemOpts)},
            G1 = G#grp{ids = mark_id(Id, Built, Ids), items = G#grp.items + 1},
            add_req(W1, stream(add_ops(ItemOps, G1), W1, State),
                    Fill#fill{cache = maps:remove(Id, Cache)}, State)
    end.

add_ops([], G) ->
    G;
add_ops(Ops, #grp{unsent = Unsent, unsent_n = N} = G) ->
    G#grp{unsent = [Ops | Unsent], unsent_n = N + 1}.

%% A synced group sends its ops to the committer every write_chunk
%% documents: its batch is built while the rest of the group is.
stream(#grp{unsent_n = N, sync = GSync} = G, #wreq{sync = WSync},
       #state{write_chunk = Chunk} = State)
  when N >= Chunk, GSync orelse WSync ->
    send_ops(G, State);
stream(G, _W, _State) ->
    G.

send_ops(#grp{unsent = Unsent, ref = Ref0} = G,
         #state{committer = Committer, store_ref = StoreRef}) ->
    Ref = case Ref0 of
        undefined -> make_ref();
        _ -> Ref0
    end,
    Committer ! {ops, Ref, StoreRef, lists:append(lists:reverse(Unsent))},
    G#grp{unsent = [], unsent_n = 0, ref = Ref}.

%% The document's state read with its batch, else read now.
current(none, _Cache, State) ->
    {none, State};
current(Id, Cache, State) when is_map_key(Id, Cache) ->
    {{read, map_get(Id, Cache)}, State};
current(Id, _Cache, State) ->
    {unread, await_id(Id, State)}.

await_id(Id, State) ->
    await_ids([Id], State).

sync_opt(#{sync := true}) -> true;
sync_opt(_Opts) -> false.

close_group(#wreq{seg = []} = W, G, Fill, State) ->
    {G, W, Fill, State};
close_group(#wreq{} = W, #grp{reqs = Reqs, count = N} = G, Fill, State) ->
    {G#grp{reqs = [W#wreq{more = true} | Reqs], count = N + 1},
     W#wreq{seg = []}, Fill, State}.

%% Wait until the committer has written and answered every group handed
%% to it.
await_commit(#state{inflight = []} = State) ->
    State;
await_commit(State) ->
    await_commit(await_oldest(State)).

await_oldest(#state{inflight = [{Ref, _} | Rest]} = State) ->
    receive
        {committed, Ref, _MoreSeg} -> State#state{inflight = Rest}
    end.

%% Wait for the group Ref and return the results of its request that
%% continues in the next group.
await_result(Ref, #state{inflight = [{Ref, _} | Rest]} = State) ->
    receive
        {committed, Ref, MoreSeg} -> {State#state{inflight = Rest}, MoreSeg}
    end;
await_result(Ref, State) ->
    await_result(Ref, await_oldest(State)).

mark_id(none, _Built, Ids) -> Ids;
mark_id(Id, {{ok, _}, _, _}, Ids) -> Ids#{Id => true};
mark_id(_Id, _Built, Ids) -> Ids.

%% {Id, Prepared, ItemOpts} or {error, Reason}.
prepare_item({doc, Doc, DocOpts}, Ks) ->
    case valid_doc_opts(maps:to_list(DocOpts)) of
        true -> prepare_doc_item(Doc, DocOpts, Ks);
        false -> {error, {invalid_doc_opts, DocOpts}}
    end;
prepare_item({delete, DocId} = Item, _Ks) ->
    {DocId, Item, #{}};
prepare_item({ack, _Tag, _Hlcs} = Item, _Ks) ->
    {none, Item, #{}}.

%% A document prepared for another keyspace is prepared again here.
prepare_doc_item({prepared, #prep{ks = Ks, rec = #{id := DocId}} = P}, DocOpts, Ks) ->
    {DocId, P, DocOpts};
prepare_doc_item({prepared, #prep{rec = #{id := DocId} = Rec}}, DocOpts, Ks) ->
    {DocId, prepare_record(Ks, Rec), DocOpts};
prepare_doc_item({prepared, {error, _} = Err}, _DocOpts, _Ks) ->
    Err;
prepare_doc_item(Doc, DocOpts, Ks) ->
    case prepare_ks(Ks, Doc) of
        #prep{rec = #{id := DocId}} = P -> {DocId, P, DocOpts};
        {error, _} = Err -> Err
    end.

%% A document's own options: its outbox tags and whether it is synced.
valid_doc_opts([]) -> true;
valid_doc_opts([{outbox, Tags} | Rest]) when is_list(Tags) -> valid_doc_opts(Rest);
valid_doc_opts([{sync, Sync} | Rest]) when is_boolean(Sync) -> valid_doc_opts(Rest);
valid_doc_opts(_Opts) -> false.

%% Build one item: {Result, Ops, NotifyInfo}. A failed item adds no ops.
%% `Read' is {read, Old} when the document was read with its batch.
build_item(Item, unread, StoreRef, DbName, Opts) ->
    try read_item(Item, StoreRef, DbName) of
        Old -> build_item(Item, {read, Old}, StoreRef, DbName, Opts)
    catch
        _:Reason -> {{error, Reason}, [], none}
    end;
build_item(#prep{} = P, {read, Old}, StoreRef, DbName, Opts) ->
    try
        {Ops, Notify} = build_prepared_ops(StoreRef, DbName, P, Old, Opts,
                                           alloc_hlc(DbName)),
        {write_result(Notify, Opts), Ops, Notify}
    catch
        throw:{error, _} = Err -> {Err, [], none};
        _:Reason -> {{error, Reason}, [], none}
    end;
build_item({delete, DocId}, {read, Old}, StoreRef, DbName, Opts) ->
    try build_delete_ops(StoreRef, DbName, DocId, Old, Opts) of
        {ok, Ops, Notify} -> {write_result(Notify, Opts), Ops, Notify};
        {error, _} = Err -> {Err, [], none}
    catch
        throw:{error, _} = Err -> {Err, [], none};
        _:Reason -> {{error, Reason}, [], none}
    end;
build_item({ack, Tag, Hlcs}, none, _StoreRef, DbName, _Opts) ->
    Ks = barrel_keyspace:resolve(DbName),
    {ok, [{delete, barrel_store_keys:outbox_key(Ks, Tag, Hlc)} || Hlc <- Hlcs],
     none}.

read_item(#prep{rec = #{id := DocId}}, StoreRef, DbName) ->
    read_current(StoreRef, DbName, DocId);
read_item({delete, DocId}, StoreRef, DbName) ->
    read_current(StoreRef, DbName, DocId).

%% return_hlc => true adds the write's change HLC (internal atom key),
%% used by callers that ack outbox entries.
write_result({DocId, NewToken, NextHlc, Deleted, DocBody, _Topics}, Opts) ->
    write_result({DocId, NewToken, NextHlc, Deleted, DocBody}, Opts);
write_result({DocId, NewToken, NextHlc, _Deleted, _DocBody}, Opts) ->
    Result = #{<<"id">> => DocId, <<"ok">> => true, <<"rev">> => NewToken},
    case maps:get(return_hlc, Opts, false) of
        true -> {ok, Result#{hlc => NextHlc}};
        false -> {ok, Result}
    end.

%% Hand the group to the committer, keeping at most ?MAX_INFLIGHT
%% groups in its queue.
dispatch_group(#grp{reqs = []}, State) ->
    State;
dispatch_group(#grp{reqs = Reqs, count = N, ids = Ids, ref = Ref0,
                    unsent = Unsent}, State0) ->
    #state{name = DbName, store_ref = StoreRef, committer = Committer,
           inflight = Inflight} = State = await_room(State0),
    {Sync, Plan} = group_plan(Reqs),
    Tail = lists:append(lists:reverse(Unsent)),
    %% an unsynced group's batch is built here, the committer writes it
    {Ref, Batch} = case Ref0 of
        undefined -> {make_ref(), build_batch(StoreRef, Tail)};
        _ -> {Ref0, {tail, Tail}}
    end,
    Committer ! {commit, Ref, StoreRef, DbName, Batch, Sync, Plan},
    ok = barrel_metrics:observe_write_group(DbName, N),
    (count_group(N, State))#state{inflight = Inflight ++ [{Ref, Ids}]}.

build_batch(_StoreRef, []) ->
    none;
build_batch(StoreRef, Ops) ->
    try barrel_store_rocksdb:build_batch(StoreRef, Ops)
    catch Class:Reason -> {error, {Class, Reason}}
    end.

add_to_batch(StoreRef, {batch, _} = Batch, Ops) ->
    try barrel_store_rocksdb:add_to_batch(StoreRef, Batch, Ops)
    catch Class:Reason -> {error, {Class, Reason}}
    end;
add_to_batch(_StoreRef, {error, _} = Err, _Ops) ->
    Err.

await_room(#state{inflight = Inflight} = State)
  when length(Inflight) >= ?MAX_INFLIGHT ->
    await_room(await_oldest(State));
await_room(State) ->
    State.

%% Commit the group and wait for it: returns the results of the request
%% that continues in the next group.
commit_group(#grp{reqs = []}, State) ->
    {State, []};
commit_group(Grp, State0) ->
    #state{inflight = Inflight} = State = dispatch_group(Grp, State0),
    {Ref, _} = lists:last(Inflight),
    await_result(Ref, State).

%% Whether any request or document of the group asked for sync, and the
%% requests in arrival order without their ops.
group_plan(Reqs0) ->
    Reqs = lists:reverse(Reqs0),
    Sync = lists:any(fun(#wreq{sync = S}) -> S end, Reqs),
    {Sync, [W#wreq{seg = [{R, ItemOps =/= [], Notify}
                          || {R, ItemOps, Notify} <- Seg]}
            || #wreq{seg = Seg} = W <- Reqs]}.

%% Write the group's batch, synced when one of its requests asked for
%% it, then notify and answer its requests in arrival order. Returns the
%% results of a request that continues in the next group.
write_and_answer(StoreRef, DbName, Batch, Sync, Plan) ->
    Written = write_group(StoreRef, Batch, Sync),
    ok = notify_written(Written, DbName, Plan),
    lists:foldl(fun(W, Acc) -> finish_req(W, Written, Acc) end, [], Plan).

start_committer(true) ->
    undefined;
start_committer(false) ->
    Server = self(),
    spawn_link(fun() -> committer_loop(Server, none) end).

%% Groups are written one after the other, in the order the writer
%% built them, so feed rows become visible in HLC order. `Open' is the
%% batch of a synced group whose ops are still arriving.
committer_loop(Server, Open) ->
    receive
        {ops, Ref, StoreRef, Ops} ->
            committer_loop(Server, {Ref, extend(StoreRef, Open, Ref, Ops)});
        {commit, Ref, StoreRef, DbName, Batch0, Sync, Plan} ->
            Batch = seal(StoreRef, Open, Ref, Batch0),
            MoreSeg = write_and_answer(StoreRef, DbName, Batch, Sync, Plan),
            Server ! {committed, Ref, MoreSeg},
            committer_loop(Server, none)
    end.

extend(StoreRef, none, _Ref, Ops) ->
    build_batch(StoreRef, Ops);
extend(StoreRef, {Ref, Batch}, Ref, Ops) ->
    add_to_batch(StoreRef, Batch, Ops).

seal(_StoreRef, none, _Ref, Batch) ->
    Batch;
seal(StoreRef, {Ref, Batch}, Ref, {tail, Tail}) ->
    add_to_batch(StoreRef, Batch, Tail).

write_group(_StoreRef, [], _Sync) ->
    ok;
write_group(_StoreRef, none, _Sync) ->
    ok;
write_group(_StoreRef, {error, _} = Err, _Sync) ->
    Err;
write_group(StoreRef, Ops, Sync) ->
    try barrel_store_rocksdb:write_batch(StoreRef, Ops, #{sync => Sync}) of
        ok -> ok;
        {error, _} = Err -> Err
    catch
        Class:Reason -> {error, {Class, Reason}}
    end.

finish_req(#wreq{seg = Seg, more = More, done = Done} = W, Written, Acc) ->
    Final = [finish_item(Item, Written) || Item <- lists:reverse(Seg)],
    case More of
        true ->
            lists:reverse(Final);
        false ->
            reply_req(W, lists:reverse(Done) ++ Final),
            Acc
    end.

%% An item that added ops shares the batch's outcome.
finish_item({Result, false, _Notify}, _Written) ->
    Result;
finish_item({Result, true, _Notify}, ok) ->
    Result;
finish_item({_Result, true, _Notify}, {error, _} = Err) ->
    Err.

%% The group's writes reach their subscribers before any caller is answered.
notify_written(ok, DbName, Reqs) ->
    notify_group(DbName, [Notify || #wreq{seg = Seg} <- Reqs,
                                    {_, true, Notify} <- lists:reverse(Seg),
                                    Notify =/= none]);
notify_written({error, _}, _DbName, _Reqs) ->
    ok.

reply_req(#wreq{from = From, kind = put_docs}, Results) ->
    gen_server:reply(From, Results);
reply_req(#wreq{from = From}, [Result]) ->
    gen_server:reply(From, Result).

notify_write(DbName, Note) ->
    notify_group(DbName, [Note]).

count_group(N, #state{write_groups = #{groups := G, requests := R,
                                       max_size := M}} = State) ->
    State#state{write_groups = #{groups => G + 1, requests => R + N,
                                 max_size => max(M, N)}}.

%% @doc Embedding entity columns for a write. Present only when the doc
%% carried an _embedding; absent otherwise, which clears any previous
%% (now stale) vector column since entity_put replaces the entity.
embedding_columns(DocRecord) ->
    case maps:get(embedding, DocRecord, undefined) of
        undefined ->
            [];
        Vector ->
            [{?COL_EMBEDDING, barrel_doc:encode_embedding(Vector)},
             {?COL_EMBEDDING_SRC, maps:get(embedding_src, DocRecord, ?EMBEDDING_SRC_CLIENT)}]
    end.

%% @doc Provenance entity column for a write. Present only when the
%% caller supplied the provenance option; absent otherwise, which
%% clears any previous value since entity_put replaces the entity.
provenance_columns(undefined) -> [];
provenance_columns(ProvEnc) -> [{?COL_PROVENANCE, ProvEnc}].

%% @doc Validate the provenance and expires_at write options (if any);
%% caches the encoded provenance for the op builders.
validate_prov_opt(Opts) ->
    case maps:find(expires_at, Opts) of
        {ok, E} when not (is_integer(E) andalso E >= 0) ->
            {error, {invalid_expires_at, E}};
        _ ->
            case maps:find(provenance, Opts) of
                error ->
                    {ok, Opts};
                {ok, Prov} ->
                    case barrel_provenance:validate(Prov) of
                        {ok, Enc} ->
                            {ok, Opts#{provenance_enc => Enc}};
                        {error, Reason} ->
                            {error, {invalid_provenance, Reason}}
                    end
            end
    end.

%% @doc TTL index maintenance for one write: the row moves with the
%% doc's expires_at so the sweeper folds exactly the expired range.
expiry_index_ops(_Ks, _DocId, Same, Same) ->
    [];
expiry_index_ops(Ks, DocId, 0, New) ->
    [{put, barrel_store_keys:doc_expiry(Ks, New, DocId), <<>>}];
expiry_index_ops(Ks, DocId, Old, 0) ->
    [{delete, barrel_store_keys:doc_expiry(Ks, Old, DocId)}];
expiry_index_ops(Ks, DocId, Old, New) ->
    [{delete, barrel_store_keys:doc_expiry(Ks, Old, DocId)},
     {put, barrel_store_keys:doc_expiry(Ks, New, DocId), <<>>}].

%% A read-only database runs no periodic maintenance.
arm_unless(true, _Interval, _Msg) -> undefined;
arm_unless(false, Interval, Msg) -> erlang:send_after(Interval, self(), Msg).

%% @doc Read a document's current version state (undefined when absent).
read_current(StoreRef, DbName, DocId) ->
    Ks = barrel_keyspace:resolve(DbName),
    DocEntityKey = barrel_store_keys:doc_entity(Ks, DocId),
    case barrel_store_rocksdb:get_entity(StoreRef, DocEntityKey) of
        {ok, Columns} ->
            current_state(Columns, barrel_store_rocksdb:body_get(StoreRef,
                                     barrel_store_keys:doc_body(Ks, DocId)));
        not_found ->
            undefined
    end.

%% @doc read_current/3 of several documents, two reads in all:
%% #{DocId => State | undefined}. A document whose read fails is left
%% out (it is read again, alone, when written).
read_currents(StoreRef, Ks, DocIds) ->
    Entities = barrel_store_rocksdb:multi_get_entity(
                 StoreRef, [barrel_store_keys:doc_entity(Ks, Id) || Id <- DocIds]),
    Read = lists:zip(DocIds, Entities),
    Found = [{Id, Columns} || {Id, {ok, Columns}} <- Read],
    Bodies = case Found of
        [] -> [];
        _ -> barrel_store_rocksdb:body_multi_get(
                 StoreRef, [barrel_store_keys:doc_body(Ks, Id) || {Id, _} <- Found],
                 point)
    end,
    maps:from_list(
      [{Id, undefined} || {Id, not_found} <- Read] ++
      [{Id, current_state(Columns, Body)}
       || {{Id, Columns}, Body} <- lists:zip(Found, Bodies),
          Body =:= not_found orelse element(1, Body) =:= ok]).

current_state(Columns, BodyRead) ->
            VV = case proplists:get_value(?COL_VV, Columns, <<>>) of
                <<>> -> barrel_vv:new();
                VVBin -> barrel_vv:decode(VVBin)
            end,
            {Body, BodyCbor} = case BodyRead of
                {ok, OldCborBin} ->
                    {barrel_docdb_codec_cbor:decode_any(OldCborBin), OldCborBin};
                not_found ->
                    {undefined, undefined}
            end,
            #{
                version => barrel_version:decode(
                               proplists:get_value(?COL_VERSION, Columns)),
                hlc => barrel_hlc:decode(proplists:get_value(?COL_HLC, Columns)),
                vv => VV,
                deleted => bin_to_deleted(
                               proplists:get_value(?COL_DELETED, Columns, <<"false">>)),
                num_conflicts => proplists:get_value(?COL_NCONFLICTS, Columns, 0),
                created_at => proplists:get_value(?COL_CREATED_AT, Columns, <<>>),
                expires_at => proplists:get_value(?COL_EXPIRES_AT, Columns, 0),
                tier => proplists:get_value(?COL_TIER, Columns, 0),
                ctx => proplists:get_value(?COL_CTX, Columns, undefined),
                body => Body,
                body_cbor => BodyCbor,
                embedding => stored_embedding(Columns)
            }.

stored_embedding(Columns) ->
    case proplists:get_value(?COL_EMBEDDING, Columns, <<>>) of
        <<>> -> undefined;
        Bin -> {Bin, proplists:get_value(?COL_EMBEDDING_SRC, Columns,
                                         ?EMBEDDING_SRC_CLIENT)}
    end.

%% @doc This database's stable source id (the version author).
%% Per-database, not per-node: replicas on the same node must detect
%% each other's divergent writes as concurrent.
source_id(DbName) ->
    persistent_term:get({barrel_source, DbName}).

%% @private Load or create the source id (persisted in db meta).
%% A read-only store cannot mint: the stored id, else the one the import
%% sidecar carries, else an in-memory one (it never authors a version).
source_id(false, StoreRef, LogicalName, _DbPath) ->
    ensure_source_id(StoreRef, LogicalName);
source_id(true, StoreRef, LogicalName, DbPath) ->
    Key = barrel_store_keys:db_meta(LogicalName, <<"source_id">>),
    case {barrel_store_rocksdb:get(StoreRef, Key),
          barrel_keyspace:read_meta(DbPath)} of
        {{ok, SourceId}, _} -> SourceId;
        {not_found, {ok, #{source_id := SourceId}}} -> SourceId;
        {not_found, _} -> new_source_id()
    end.

new_source_id() ->
    binary:encode_hex(crypto:strong_rand_bytes(8), lowercase).

ensure_source_id(StoreRef, LogicalName) ->
    %% Deliberately keyed by the LOGICAL name, never the keyspace: a
    %% branch checkpoint carries only the parent-keyed source_id, so
    %% this lookup misses on a fresh branch and mints a new author.
    Key = barrel_store_keys:db_meta(LogicalName, <<"source_id">>),
    case barrel_store_rocksdb:get(StoreRef, Key) of
        {ok, SourceId} ->
            SourceId;
        not_found ->
            SourceId = new_source_id(),
            ok = barrel_store_rocksdb:put(StoreRef, Key, SourceId),
            SourceId
    end.

%% @doc The writer id authoring this database's versions. A new one is
%% minted on every writable open, so a crash, a restored backup or a
%% copied store can never reuse an id whose versions peers already hold.
-spec writer_id(binary()) -> binary().
writer_id(DbName) ->
    persistent_term:get({barrel_writer, DbName}).

%% @private Mint the writer id (synced, before any write) and load the
%% feed clock floor. A read-only store never authors.
init_writer(true, _StoreRef, _AttRef, _Name, _Keyspace) ->
    ok;
init_writer(false, StoreRef, AttRef, Name, Keyspace) ->
    WriterId = <<"w", (binary:encode_hex(crypto:strong_rand_bytes(8),
                                         lowercase))/binary>>,
    ok = barrel_store_rocksdb:put(
           StoreRef, barrel_store_keys:db_meta(Name, <<"writer_id">>),
           WriterId, [{sync, true}]),
    persistent_term:put({barrel_writer, Name}, WriterId),
    %% the floor covers the attachment feed too; a clock that can't
    %% reach it fails writes with clock_skew (alloc_hlc), data stays
    High = lists:foldl(fun max_hlc/2, barrel_hlc:min(),
                       [stored_hlc(StoreRef, barrel_store_keys:db_meta(
                                               Keyspace, <<"hlc_high">>)),
                        stored_hlc(StoreRef,
                                   barrel_store_keys:db_last_hlc(Keyspace)),
                        barrel_att_store:att_last_hlc(AttRef, Name)]),
    put({?MODULE, hlc_high, Name}, High),
    ok.

stored_hlc(StoreRef, Key) ->
    case barrel_store_rocksdb:get(StoreRef, Key) of
        {ok, Enc} -> barrel_hlc:decode(Enc);
        not_found -> barrel_hlc:min()
    end.

max_hlc(A, B) ->
    case barrel_hlc:less(A, B) of
        true -> B;
        false -> A
    end.

%% @doc Allocate the HLC of a feed write. Only the writer allocates
%% (anywhere else this fails), and every HLC is above every HLC this
%% database ever wrote, so a clock that went back after a restart can't
%% place a change behind a downstream checkpoint. Throws
%% `{error, clock_skew}' when the clock can't get past that floor.
-spec alloc_hlc(binary()) -> barrel_hlc:timestamp().
alloc_hlc(DbName) ->
    alloc_hlc(DbName, barrel_hlc:min()).

%% @doc alloc_hlc/1, also above Floor (an attachment's stored origin).
-spec alloc_hlc(binary(), barrel_hlc:timestamp()) -> barrel_hlc:timestamp().
alloc_hlc(DbName, Floor) ->
    Key = {?MODULE, hlc_high, DbName},
    case get(Key) of
        undefined ->
            erlang:error({hlc_alloc_outside_writer, DbName});
        High ->
            Hlc = above(max_hlc(High, Floor), barrel_hlc:new_hlc()),
            put(Key, Hlc),
            Hlc
    end.

above(High, Hlc) ->
    case barrel_hlc:less(High, Hlc) of
        true ->
            Hlc;
        false ->
            _ = barrel_hlc:sync_hlc(High),
            Hlc2 = barrel_hlc:new_hlc(),
            case barrel_hlc:less(High, Hlc2) of
                true -> Hlc2;
                false -> throw({error, clock_skew})
            end
    end.

catch_error(Fun) ->
    try Fun()
    catch throw:{error, _} = Err -> Err
    end.

%%--------------------------------------------------------------------
%% Replicated document state (barrel_live) over the stored document:
%% the entity holds the winner, conflict chain rows the other live
%% versions, COL_CTX the versions seen.
%%--------------------------------------------------------------------

%% @doc The document's barrel_live state. A document written before
%% COL_CTX existed is converted: only the exact versions it stores
%% (current, conflicts, superseded ancestors) count as seen.
load_live(_StoreRef, _DbName, _DocId, undefined) ->
    barrel_live:new();
load_live(StoreRef, DbName, DocId,
          #{version := W, deleted := D, vv := VV, num_conflicts := N} = Old) ->
    Siblings = case N of
        0 -> [];
        _ -> conflict_siblings(StoreRef, DbName, DocId)
    end,
    Live = maps:from_list(
             [{W, #{deleted => D, vv => VV}}
              | [{V, #{deleted => SD, vv => SVV}}
                 || {V, #{deleted := SD, vv := SVV}} <- Siblings]]),
    case maps:get(ctx, Old, undefined) of
        undefined ->
            barrel_live:convert(maps:to_list(Live),
                                chain_versions(StoreRef, DbName, DocId));
        CtxBin ->
            barrel_live:decode_ctx(CtxBin, Live)
    end.

%% Every version in the document's chain, superseded or conflict.
chain_versions(StoreRef, DbName, DocId) ->
    Ks = barrel_keyspace:resolve(DbName),
    barrel_doc_chain:fold(
      StoreRef, Ks, DocId,
      fun(_Key, VEnc, _Value, Acc) ->
          {ok, [barrel_version:decode(VEnc) | Acc]}
      end, []).

%% @doc What a local write replaces, and the state it applies to.
%% Throws {error, conflict} when the caller's rev or observed set does
%% not allow the write.
local_state(StoreRef, DbName, DocId, Old, RevToken, ObservedTokens) ->
    Live = load_live(StoreRef, DbName, DocId, Old),
    try
        Rev = case RevToken of
            undefined -> undefined;
            _ -> barrel_version:from_token(RevToken)
        end,
        Observed = case ObservedTokens of
            undefined -> undefined;
            _ -> [barrel_version:from_token(T) || T <- ObservedTokens]
        end,
        barrel_live:replaces(Live, Rev, Observed)
    of
        {ok, Replaces} -> {Live, Replaces};
        {error, conflict} -> throw({error, conflict})
    catch
        error:badarg -> throw({error, conflict})
    end.

%% @doc An HLC for a new local version: above every live version (an
%% automatic resolution can sort a few logical steps above the clock).
above_live(DbName, Live, Hlc) ->
    case [H || {H, _} <- barrel_live:tokens(Live), not barrel_hlc:less(H, Hlc)] of
        [] ->
            Hlc;
        Higher ->
            %% a live version the clock may not reach: refuse, don't spin
            sync_or_throw(lists:max(Higher)),
            above_live(DbName, Live, alloc_hlc(DbName))
    end.

%% Advance the clock to a remote HLC; throws {error, clock_skew} when
%% the clock rejects it (too far ahead).
sync_or_throw(Hlc) ->
    case barrel_hlc:sync_hlc(Hlc) of
        {ok, _} -> ok;
        {error, clock_skew} -> throw({error, clock_skew})
    end.

%% @doc A local write's new state, entity vector, conflict count,
%% context column and chain rows: every previous live version becomes
%% superseded if replaced, else stays a conflict.
local_write_state(Ks, DocId, Live, Replaces, NewVersion, Deleted) ->
    NewLive = barrel_live:author(Live, NewVersion, Deleted, Replaces),
    #{vv := NewVV} = maps:get(NewVersion, barrel_live:live(NewLive)),
    Chain = [{put, barrel_store_keys:doc_version(Ks, DocId,
                                                 barrel_version:encode(T)),
              sibling_entry(case lists:member(T, Replaces) of
                                true -> superseded;
                                false -> conflict
                            end, D, VV)}
             || {T, #{deleted := D, vv := VV}}
                    <- maps:to_list(barrel_live:live(Live))],
    {NewVV, map_size(barrel_live:live(NewLive)) - 1,
     {?COL_CTX, barrel_live:encode_ctx(NewLive)}, Chain}.

%% @doc Sibling entry stored in the per-doc version chain (0x1D):
%% `<<Flag:8, Deleted:8, VV/binary>>' where Flag 0 = superseded (covered
%% by the current winner) and 1 = conflict (a live concurrent sibling).
sibling_entry(Flag, Deleted, VV) ->
    FlagByte = case Flag of superseded -> 0; conflict -> 1 end,
    DelByte = case Deleted of true -> 1; false -> 0 end,
    <<FlagByte:8, DelByte:8, (barrel_vv:encode(VV))/binary>>.

decode_sibling_entry(<<FlagByte:8, DelByte:8, VVBin/binary>>) ->
    Flag = case FlagByte of 1 -> conflict; _ -> superseded end,
    #{flag => Flag, deleted => DelByte =:= 1, vv => barrel_vv:decode(VVBin)}.

%% @doc Build the atomic op list for a local write (create or update).
%% Issues the version: for local writes the version HLC and the change
%% sequence HLC are the same fresh HLC. The superseded old winner goes to
%% the version chain and its body to the version-keyed archive.
%% Returns {AllOps, NotifyInfo} with NotifyInfo =
%% {DocId, NewToken, NextHlc, Deleted, DocBody}.
%% @private Channel feed ops for an applied write. Topics are derived
%% from the bodies only when channels are configured; tombstone bodies
%% have no topics.
channel_ops(DbName, NewHlc, DocInfo, DocBody, OldHlc, OldDocBody,
            OldDeleted) ->
    case barrel_channel:channels(DbName) of
        Empty when map_size(Empty) =:= 0 ->
            [];
        _ ->
            NewTopics = case maps:get(deleted, DocInfo) of
                true -> [];
                false -> barrel_channel:topics(DocBody)
            end,
            OldTopics = case OldDeleted of
                true -> [];
                false -> barrel_channel:topics(OldDocBody)
            end,
            barrel_channel:write_ops(DbName, NewHlc, DocInfo, NewTopics,
                                     OldHlc, OldTopics)
    end.

build_write_ops(StoreRef, DbName, DocRecord, Old, Opts, NextHlc0) ->
    Ks = barrel_keyspace:resolve(DbName),
    #{id := DocId, deleted := Deleted, doc := DocBody} = DocRecord,
    DocEntityKey = barrel_store_keys:doc_entity(Ks, DocId),

    {Live, Replaces} = local_state(StoreRef, DbName, DocId, Old,
                                   maps:get(expected_version, DocRecord,
                                            undefined),
                                   maps:get(observed, DocRecord, undefined)),
    NextHlc = above_live(DbName, Live, NextHlc0),
    NewVersion = barrel_version:new(NextHlc, writer_id(DbName)),
    NewToken = barrel_version:to_token(NewVersion),

    {OldVersion, OldHlc, _OldVV, OldDeleted, OldDocBody, OldDocBodyCbor,
     _OldNConflicts, CreatedAt, ExpiresAt, Tier} =
        case Old of
            undefined ->
                {undefined, undefined, barrel_vv:new(), false, undefined,
                 undefined, 0, barrel_hlc:encode(NextHlc), 0, 0};
            #{version := OV, hlc := OH, vv := OVV, deleted := OD,
              body := OB, body_cbor := OBC, num_conflicts := ONC,
              created_at := OCA, expires_at := OEA, tier := OT} ->
                {OV, OH, OVV, OD, OB, OBC, ONC, OCA, OEA, OT}
        end,

    {NewVV, NConflicts, CtxCol, LiveChainOps} =
        local_write_state(Ks, DocId, Live, Replaces, NewVersion, Deleted),

    ProvEnc = maps:get(provenance_enc, Opts, undefined),
    %% Doc TTL: absent = preserve, 0 = clear, N = expire at N (unix ms)
    NewExpires = maps:get(expires_at, Opts, ExpiresAt),
    ExpiryOps = expiry_index_ops(Ks, DocId, ExpiresAt, NewExpires),
    DocColumns = [
        {?COL_VERSION, barrel_version:encode(NewVersion)},
        {?COL_DELETED, deleted_to_bin(Deleted)},
        {?COL_HLC, barrel_hlc:encode(NextHlc)},
        {?COL_VV, barrel_vv:encode(NewVV)},
        {?COL_NCONFLICTS, NConflicts},
        CtxCol,
        {?COL_CREATED_AT, CreatedAt},
        {?COL_EXPIRES_AT, NewExpires},
        {?COL_TIER, Tier}
    ] ++ provenance_columns(ProvEnc) ++ embedding_columns(DocRecord),
    DocOps = [{entity_put, DocEntityKey, DocColumns}],

    %% Change tracking info (rev carries the opaque version token)
    DocInfo = #{
        id => DocId,
        rev => NewToken,
        deleted => Deleted,
        num_conflicts => NConflicts,
        hlc => NextHlc
    },

    %% Supersede the live-feed row of the previous write
    HlcDeleteOps = case OldHlc of
        undefined -> [];
        _ -> [{delete, barrel_store_keys:doc_hlc(Ks, OldHlc)}]
    end,

    %% Path index operations (if not deleted)
    PathIndexOps = case Deleted of
        true when OldDocBody =/= undefined ->
            case barrel_ars_index:get_doc_paths(StoreRef, DbName, DocId) of
                {ok, OldPaths} -> barrel_ars_index:remove_doc_ops(DbName, DocId, OldPaths);
                not_found -> []
            end;
        true ->
            [];
        false when OldDocBody =:= undefined ->
            barrel_ars_index:index_doc_ops(DbName, DocId, DocBody);
        false ->
            case OldDeleted of
                true ->
                    %% Re-creating a deleted document. Delete only removed the
                    %% path index, not the body CF, so a body diff would see no
                    %% change and never re-add the paths. Index from scratch.
                    barrel_ars_index:index_doc_ops(DbName, DocId, DocBody);
                false ->
                    barrel_ars_index:update_doc_ops(DbName, DocId, OldDocBody, DocBody)
            end
    end,

    %% Change entry operations
    ChangeInfo = DocInfo#{doc => DocBody},
    ChangeOps = barrel_changes:write_change_ops(DbName, NextHlc, ChangeInfo),

    %% Path-indexed change operations (for efficient filtered queries)
    PathHlcOps = case OldHlc of
        undefined ->
            barrel_changes:write_path_index_ops(DbName, NextHlc, ChangeInfo);
        _ ->
            barrel_changes:update_path_index_ops(DbName, NextHlc, ChangeInfo,
                                                  OldHlc, OldDocBody)
    end,

    CborBody = barrel_docdb_codec_cbor:encode_cbor(DocBody),

    %% The old winner is superseded: archive its body under its version
    %% and record it in the per-doc version chain
    {ArchiveOps, ChainOps} = case OldVersion of
        undefined ->
            {[], []};
        _ ->
            OldVersionEnc = barrel_version:encode(OldVersion),
            Archive = case OldDocBodyCbor of
                undefined -> [];
                %% a delete's entity holds the body it replaced
                _ when OldDeleted -> [];
                OldCbor ->
                    [{body_put,
                      barrel_store_keys:doc_body_rev(Ks, DocId, OldVersionEnc),
                      OldCbor}
                     | barrel_doc_vectors:archive_ops(Ks, DocId, Old)]
            end,
            {Archive, LiveChainOps}
    end,

    %% Tagged outbox entries (generic durable work queue; see barrel_outbox)
    OutboxOps = barrel_outbox:write_ops(DbName, maps:get(outbox, Opts, []),
                                        NextHlc, OldHlc, DocId, NewToken, Deleted),

    %% Retained history entry (resolutions pass cause `resolve')
    HistoryOps = barrel_history:write_ops(
        DbName, NextHlc, DocId, NewVersion, Deleted,
        maps:get(history_cause, Opts, local), NewVV, ProvEnc),

    %% Per-channel feed rows (write-time partial sync)
    ChannelOps = channel_ops(DbName, NextHlc, DocInfo, DocBody, OldHlc,
                             OldDocBody, OldDeleted),

    %% Write new body to current location (no version in key)
    BodyKey = barrel_store_keys:doc_body(Ks, DocId),
    BodyOp = {body_put, BodyKey, CborBody},
    AllOps = DocOps ++ HlcDeleteOps ++ PathIndexOps ++ ChangeOps ++ PathHlcOps
        ++ ArchiveOps ++ ChainOps ++ OutboxOps ++ HistoryOps ++ ChannelOps
        ++ ExpiryOps ++ [BodyOp] ++ doc_count_ops(Ks, Old, Deleted),
    {AllOps, {DocId, NewToken, NextHlc, Deleted, DocBody}}.

%% @doc Prepare a document write for a database (runs in the caller).
-spec prepare(binary(), map() | binary()) ->
    {prepared, #prep{} | {error, term()}}.
prepare(DbName, Doc) ->
    {prepared, prepare_ks(barrel_keyspace:resolve(DbName), Doc)}.

prepare_ks(Ks, Doc) ->
    try barrel_doc:make_doc_record(barrel_doc:to_map(Doc)) of
        DocRecord -> prepare_record(Ks, DocRecord)
    catch
        _:Reason -> {error, Reason}
    end.

%% The values build_write_ops/6 derives from the body alone.
prepare_record(Ks, #{id := DocId, deleted := Deleted, doc := DocBody} = Rec) ->
    Paths = live_paths(Deleted, DocBody),
    Topics = barrel_changes:doc_topics(#{id => DocId, deleted => Deleted,
                                         doc => DocBody}),
    #prep{ks = Ks,
          rec = Rec,
          cbor = barrel_docdb_codec_cbor:encode_cbor(DocBody),
          change_cbor = barrel_docdb_codec_cbor:encode(DocBody),
          paths = Paths,
          topics = Topics,
          feed_heads = barrel_changes:feed_heads(
                         Ks, barrel_changes:topics_prefixes(Topics)),
          index_ops = fresh_index_ops(Ks, DocId, Paths)}.

live_paths(true, _DocBody) -> undefined;
live_paths(false, DocBody) -> barrel_ars:analyze(DocBody).

fresh_index_ops(_Ks, _DocId, undefined) -> [];
fresh_index_ops(Ks, DocId, Paths) -> barrel_ars_index:index_paths_ops(Ks, DocId, Paths).

%% @doc The batch of one document write and its notification, with the
%% given HLC: `inline' builds it the way the writer always did,
%% `prepared' from a request prepared in the caller.
-spec doc_write_ops(inline | prepared, binary(), map(), map(),
                    barrel_hlc:timestamp()) -> {list(), tuple()}.
doc_write_ops(Mode, DbName, Doc, Opts0, Hlc) ->
    StoreRef = persistent_term:get({barrel_store, DbName}),
    {ok, Opts} = validate_prov_opt(Opts0),
    {prepared, #prep{rec = #{id := DocId} = Rec} = P} = prepare(DbName, Doc),
    Old = read_current(StoreRef, DbName, DocId),
    doc_write_ops(Mode, StoreRef, DbName, P, Rec, Old, Opts, Hlc).

doc_write_ops(inline, StoreRef, DbName, _P, Rec, Old, Opts, Hlc) ->
    build_write_ops(StoreRef, DbName, Rec, Old, Opts, Hlc);
doc_write_ops(prepared, StoreRef, DbName, P, _Rec, Old, Opts, Hlc) ->
    build_prepared_ops(StoreRef, DbName, P, Old, Opts, Hlc).

%% @doc build_write_ops/6 from a prepared write: only what needs the
%% current state or the HLC is computed here. Same ops, same order.
build_prepared_ops(StoreRef, DbName, #prep{rec = DocRecord} = P, Old, Opts,
                   NextHlc0) ->
    Ks = barrel_keyspace:resolve(DbName),
    #prep{cbor = CborBody, change_cbor = ChangeCbor, paths = NewPaths,
          topics = Topics, feed_heads = Heads, index_ops = FreshIndexOps} = P,
    #{id := DocId, deleted := Deleted, doc := DocBody} = DocRecord,
    DocEntityKey = barrel_store_keys:doc_entity(Ks, DocId),

    {Live, Replaces} = local_state(StoreRef, DbName, DocId, Old,
                                   maps:get(expected_version, DocRecord,
                                            undefined),
                                   maps:get(observed, DocRecord, undefined)),
    NextHlc = above_live(DbName, Live, NextHlc0),
    NewVersion = barrel_version:new(NextHlc, writer_id(DbName)),
    NewToken = barrel_version:to_token(NewVersion),

    {OldVersion, OldHlc, _OldVV, OldDeleted, OldDocBody, OldDocBodyCbor,
     _OldNConflicts, CreatedAt, ExpiresAt, Tier} =
        case Old of
            undefined ->
                {undefined, undefined, barrel_vv:new(), false, undefined,
                 undefined, 0, barrel_hlc:encode(NextHlc), 0, 0};
            #{version := OV, hlc := OH, vv := OVV, deleted := OD,
              body := OB, body_cbor := OBC, num_conflicts := ONC,
              created_at := OCA, expires_at := OEA, tier := OT} ->
                {OV, OH, OVV, OD, OB, OBC, ONC, OCA, OEA, OT}
        end,

    {NewVV, NConflicts, CtxCol, LiveChainOps} =
        local_write_state(Ks, DocId, Live, Replaces, NewVersion, Deleted),

    ProvEnc = maps:get(provenance_enc, Opts, undefined),
    NewExpires = maps:get(expires_at, Opts, ExpiresAt),
    ExpiryOps = expiry_index_ops(Ks, DocId, ExpiresAt, NewExpires),
    DocColumns = [
        {?COL_VERSION, barrel_version:encode(NewVersion)},
        {?COL_DELETED, deleted_to_bin(Deleted)},
        {?COL_HLC, barrel_hlc:encode(NextHlc)},
        {?COL_VV, barrel_vv:encode(NewVV)},
        {?COL_NCONFLICTS, NConflicts},
        CtxCol,
        {?COL_CREATED_AT, CreatedAt},
        {?COL_EXPIRES_AT, NewExpires},
        {?COL_TIER, Tier}
    ] ++ provenance_columns(ProvEnc) ++ embedding_columns(DocRecord),
    DocOps = [{entity_put, DocEntityKey, DocColumns}],

    DocInfo = #{
        id => DocId,
        rev => NewToken,
        deleted => Deleted,
        num_conflicts => NConflicts,
        hlc => NextHlc
    },

    HlcDeleteOps = case OldHlc of
        undefined -> [];
        _ -> [{delete, barrel_store_keys:doc_hlc(Ks, OldHlc)}]
    end,

    %% The old body is analysed once, for the index diff and the feed rows
    OldPaths = case OldDocBody of
        undefined -> undefined;
        _ -> barrel_ars:analyze(OldDocBody)
    end,

    PathIndexOps = case Deleted of
        true when OldDocBody =/= undefined ->
            case barrel_ars_index:get_doc_paths(StoreRef, DbName, DocId) of
                {ok, StoredPaths} ->
                    barrel_ars_index:remove_doc_ops(DbName, DocId, StoredPaths);
                not_found -> []
            end;
        true ->
            [];
        false when OldDocBody =:= undefined ->
            FreshIndexOps;
        false when OldDeleted ->
            FreshIndexOps;
        false ->
            barrel_ars_index:update_paths_ops(DbName, DocId, OldPaths, NewPaths)
    end,

    ChangeOps = barrel_changes:write_change_ops(DbName, NextHlc, DocInfo,
                                                ChangeCbor),

    PathHlcOps = case {OldHlc, OldPaths} of
        {undefined, _} ->
            barrel_changes:write_feed_ops(NextHlc, DocInfo, Heads);
        {_, undefined} ->
            barrel_changes:update_feed_ops(DbName, NextHlc, DocInfo, Heads,
                                           OldHlc, []);
        {_, _} ->
            OldPrefixes = barrel_changes:topics_prefixes(
                            barrel_ars:paths_to_topics(OldPaths)),
            barrel_changes:update_feed_ops(DbName, NextHlc, DocInfo, Heads,
                                           OldHlc, OldPrefixes)
    end,

    {ArchiveOps, ChainOps} = case OldVersion of
        undefined ->
            {[], []};
        _ ->
            OldVersionEnc = barrel_version:encode(OldVersion),
            Archive = case OldDocBodyCbor of
                undefined -> [];
                %% a delete's entity holds the body it replaced
                _ when OldDeleted -> [];
                OldCbor ->
                    [{body_put,
                      barrel_store_keys:doc_body_rev(Ks, DocId, OldVersionEnc),
                      OldCbor}
                     | barrel_doc_vectors:archive_ops(Ks, DocId, Old)]
            end,
            {Archive, LiveChainOps}
    end,

    OutboxOps = barrel_outbox:write_ops(DbName, maps:get(outbox, Opts, []),
                                        NextHlc, OldHlc, DocId, NewToken, Deleted),

    HistoryOps = barrel_history:write_ops(
        DbName, NextHlc, DocId, NewVersion, Deleted,
        maps:get(history_cause, Opts, local), NewVV, ProvEnc),

    ChannelOps = channel_ops(DbName, NextHlc, DocInfo, DocBody, OldHlc,
                             OldDocBody, OldDeleted),

    BodyOp = {body_put, barrel_store_keys:doc_body(Ks, DocId), CborBody},
    AllOps = DocOps ++ HlcDeleteOps ++ PathIndexOps ++ ChangeOps ++ PathHlcOps
        ++ ArchiveOps ++ ChainOps ++ OutboxOps ++ HistoryOps ++ ChannelOps
        ++ ExpiryOps ++ [BodyOp] ++ doc_count_ops(Ks, Old, Deleted),
    {AllOps, {DocId, NewToken, NextHlc, Deleted, DocBody, Topics}}.

%% @doc Store a computed embedding in the doc entity (CAS on revision).
%% Read-modify-write of the entity columns is safe here: this runs in
%% the database writer, which serializes all entity writes.
do_set_doc_embedding(StoreRef, DbName, DocId, ExpectedRev, Vector) ->
    Ks = barrel_keyspace:resolve(DbName),
    DocEntityKey = barrel_store_keys:doc_entity(Ks, DocId),
    case barrel_store_rocksdb:get_entity(StoreRef, DocEntityKey) of
        {ok, Columns} ->
            CurrentToken = barrel_version:to_token(
                barrel_version:decode(
                    proplists:get_value(?COL_VERSION, Columns))),
            case CurrentToken of
                ExpectedRev ->
                    Base = [C || {K, _} = C <- Columns,
                                 K =/= ?COL_EMBEDDING,
                                 K =/= ?COL_EMBEDDING_SRC],
                    NewColumns = Base ++
                        [{?COL_EMBEDDING, barrel_doc:encode_embedding(Vector)},
                         {?COL_EMBEDDING_SRC, ?EMBEDDING_SRC_COMPUTED}],
                    barrel_store_rocksdb:put_entity(StoreRef, DocEntityKey,
                                                    NewColumns);
                _OtherRev ->
                    {error, conflict}
            end;
        not_found ->
            {error, not_found};
        {error, _} = Err ->
            Err
    end.

%% @doc Get a document by ID. The read implementation lives in
%% barrel_docdb_reader so callers can run it outside this process; the
%% server delegates to keep a single code path.
do_get_doc(StoreRef, DbName, DocId, Opts) ->
    barrel_docdb_reader:get_doc(StoreRef, DbName, DocId, Opts).

%% @doc Get multiple documents by ID (batch read). Delegates to the
%% caller-side reader, same as do_get_doc/4.
do_get_docs(StoreRef, DbName, DocIds, Opts) ->
    barrel_docdb_reader:get_docs(StoreRef, DbName, DocIds, Opts).

%% @doc Delete a document: a versioned tombstone write.
%% Options:
%%   - rev: binary() - expected version token (optional, for conflict detection)
%%   - sync: boolean() - if true, sync to disk before returning (default: false)
do_delete_doc(StoreRef, DbName, DocId, Opts) ->
    case catch_error(fun() -> build_delete_ops(StoreRef, DbName, DocId, Opts) end) of
        {ok, AllOps, Notify} ->
            WriteOpts = #{sync => maps:get(sync, Opts, false)},
            ok = barrel_store_rocksdb:write_batch(StoreRef, AllOps, WriteOpts),
            notify_write(DbName, Notify),
            write_result(Notify, Opts);
        {error, _} = Err ->
            Err
    end.

%% @doc Build the ops of a versioned tombstone write. Throws
%% {error, {conflict, CurrentToken}} when the rev option does not match.
build_delete_ops(StoreRef, DbName, DocId, Opts) ->
    build_delete_ops(StoreRef, DbName, DocId,
                     read_current(StoreRef, DbName, DocId), Opts).

build_delete_ops(StoreRef, DbName, DocId, Current, Opts) ->
    Ks = barrel_keyspace:resolve(DbName),
    case Current of
        undefined ->
            {error, not_found};
        #{version := OldVersion, hlc := OldHlc,
          deleted := OldDeleted,
          created_at := CreatedAt, expires_at := ExpiresAt, tier := Tier,
          body := OldDocBody, body_cbor := OldDocBodyCbor} ->
            CurrentToken = barrel_version:to_token(OldVersion),
            %% a delete without a rev deletes the winner, as before
            Rev = maps:get(rev, Opts, CurrentToken),
            {Live, Replaces} =
                try local_state(StoreRef, DbName, DocId, Current, Rev,
                                maps:get(observed, Opts, undefined))
                catch throw:{error, conflict} ->
                    throw({error, {conflict, CurrentToken}})
                end,

            NextHlc = above_live(DbName, Live, alloc_hlc(DbName)),
            NewVersion = barrel_version:new(NextHlc, writer_id(DbName)),
            NewToken = barrel_version:to_token(NewVersion),
            {NewVV, NConflicts, CtxCol, LiveChainOps} =
                local_write_state(Ks, DocId, Live, Replaces, NewVersion, true),
            DocEntityKey = barrel_store_keys:doc_entity(Ks, DocId),

            ProvEnc = maps:get(provenance_enc, Opts, undefined),
            %% a tombstone has no TTL; drop any pending expiry row
            ExpiryOps = expiry_index_ops(Ks, DocId, ExpiresAt, 0),
            DocColumns = [
                {?COL_VERSION, barrel_version:encode(NewVersion)},
                {?COL_DELETED, <<"true">>},
                {?COL_HLC, barrel_hlc:encode(NextHlc)},
                {?COL_VV, barrel_vv:encode(NewVV)},
                {?COL_NCONFLICTS, NConflicts},
                CtxCol,
                {?COL_CREATED_AT, CreatedAt},
                {?COL_EXPIRES_AT, 0},
                {?COL_TIER, Tier}
            ] ++ provenance_columns(ProvEnc),
            DocOps = [{entity_put, DocEntityKey, DocColumns}],

            %% Supersede the live-feed row of the previous write
            HlcDeleteOps = [{delete, barrel_store_keys:doc_hlc(Ks, OldHlc)}],

            %% Path index removal operations
            PathIndexOps = case barrel_ars_index:get_doc_paths(StoreRef, DbName, DocId) of
                {ok, Paths} -> barrel_ars_index:remove_doc_ops(DbName, DocId, Paths);
                not_found -> []
            end,

            %% Build DocInfo for change tracking
            NewDocInfo = #{
                id => DocId,
                rev => NewToken,
                deleted => true,
                num_conflicts => NConflicts,
                hlc => NextHlc
            },

            %% Change entry operations
            ChangeOps = barrel_changes:write_change_ops(DbName, NextHlc, NewDocInfo),

            %% Path-indexed change operations for delete
            PathHlcOps = barrel_changes:update_path_index_ops(DbName, NextHlc, NewDocInfo,
                                                               OldHlc, OldDocBody),

            %% The superseded winner joins the version chain, body archived
            OldVersionEnc = barrel_version:encode(OldVersion),
            ArchiveOps = case OldDocBodyCbor of
                undefined -> [];
                _ when OldDeleted -> [];
                OldCbor ->
                    [{body_put,
                      barrel_store_keys:doc_body_rev(Ks, DocId, OldVersionEnc),
                      OldCbor}
                     | barrel_doc_vectors:archive_ops(Ks, DocId, Current)]
            end,
            ChainOps = LiveChainOps,

            %% Tagged outbox entries (deletes replace the pending entry too)
            OutboxOps = barrel_outbox:write_ops(DbName, maps:get(outbox, Opts, []),
                                                NextHlc, OldHlc, DocId, NewToken, true),

            %% Retained history entry for the tombstone
            HistoryOps = barrel_history:write_ops(
                DbName, NextHlc, DocId, NewVersion, true, local, NewVV,
                ProvEnc),

            %% The tombstone lands in the channels the doc was in
            ChannelOps = channel_ops(DbName, NextHlc, NewDocInfo, undefined,
                                     OldHlc, OldDocBody, OldDeleted),

            AllOps = DocOps ++ HlcDeleteOps ++ PathIndexOps ++ ChangeOps ++ PathHlcOps
                ++ ArchiveOps ++ ChainOps ++ OutboxOps ++ HistoryOps
                ++ ChannelOps ++ ExpiryOps ++ doc_count_ops(Ks, Current, true),
            {ok, AllOps, {DocId, NewToken, NextHlc, true, #{}}}
    end.

%%====================================================================
%% Replication Operations
%%====================================================================

%% @doc Apply a version from a peer that sends one version at a time
%% (put_version/5: old servers, old barrel-lite). It is ignored when this
%% document has seen that exact version, otherwise kept as a conflict;
%% it never removes anything (barrel_live:legacy_arrival/4).
do_put_version(StoreRef, DbName, Config, Doc, Version, RemoteVV, Deleted) ->
    %% Advance the local clock past the remote write so subsequent local
    %% change HLCs stay strictly increasing; a rejected one writes nothing
    sync_or_throw(barrel_version:hlc(Version)),
    {DocId, DocBody} = id_and_body(Doc),
    Old = read_current(StoreRef, DbName, DocId),
    Live0 = load_live(StoreRef, DbName, DocId, Old),
    Live1 = barrel_live:legacy_arrival(Live0, Version, Deleted, RemoteVV),
    Local = live_vectors(StoreRef, DbName, DocId, Old, Live0),
    Vectors = maps:merge(carried_vector(Version, Deleted, Doc), Local),
    case commit_state(StoreRef, DbName, Config, DocId, Old, Live0, Live1,
                      #{Version => DocBody},
                      (replication_opts(Config))#{vectors => Vectors,
                                                  stored => maps:keys(Local)}) of
        {unchanged, DocId, Token} -> {ok, DocId, Token};
        Written -> Written
    end.

%% @doc Merge a peer's full document state (put_version/6): the join of
%% the two states, then eviction and automatic resolution.
do_put_state(StoreRef, DbName, Config, DocId, StateBin, RemoteBodies,
             RemoteVectors) ->
    Remote = barrel_live:decode_state(StateBin),
    lists:foreach(fun({H, _}) -> sync_or_throw(H) end,
                  barrel_live:tokens(Remote)),
    Old = read_current(StoreRef, DbName, DocId),
    Live0 = load_live(StoreRef, DbName, DocId, Old),
    Live1 = barrel_live:merge(Live0, Remote),
    Bodies = maps:from_list(
               [{barrel_version:from_token(T), B}
                || {T, B} <- maps:to_list(RemoteBodies)]),
    %% a new live version without its body would be stored empty
    Known = barrel_live:tokens(Live0),
    case [barrel_version:to_token(T)
          || {T, #{deleted := false}} <- maps:to_list(barrel_live:live(Live1)),
             not lists:member(T, Known), not maps:is_key(T, Bodies)] of
        [] ->
            Local = live_vectors(StoreRef, DbName, DocId, Old, Live0),
            Vectors = maps:merge(remote_vectors(RemoteVectors), Local),
            commit_state(StoreRef, DbName, Config, DocId, Old, Live0, Live1,
                         Bodies, (replication_opts(Config))#{vectors => Vectors,
                                                             stored => maps:keys(Local)});
        Missing ->
            {error, {missing_bodies, lists:sort(Missing)}}
    end.

%% @doc A document's full state for replication: its barrel_live state
%% and the body of every live version, under one read in the writer.
do_get_state(StoreRef, DbName, DocId) ->
    case read_current(StoreRef, DbName, DocId) of
        undefined ->
            {error, not_found};
        Old ->
            Live = load_live(StoreRef, DbName, DocId, Old),
            Bodies = maps:from_list(
                       [{barrel_version:to_token(T), version_body(StoreRef, DbName,
                                                                  DocId, Old, T)}
                        || T <- barrel_live:tokens(Live)]),
            Vectors = maps:from_list(
                        [{barrel_version:to_token(V), Vec}
                         || {V, Vec} <- maps:to_list(
                                          live_vectors(StoreRef, DbName, DocId,
                                                       Old, Live))]),
            {ok, #{id => DocId, state => barrel_live:encode_state(Live),
                   digest => barrel_live:digest(Live), bodies => Bodies,
                   vectors => Vectors}}
    end.

%% @doc Rewrite live versions stored with another body (never to `#{}'),
%% in one write: the winner is re-indexed and moves in the feed.
do_repair_bodies(StoreRef, DbName, Config, DocId, RemoteBodies)
        when is_map(RemoteBodies) ->
    case read_current(StoreRef, DbName, DocId) of
        undefined ->
            {error, not_found};
        Old ->
            Live = load_live(StoreRef, DbName, DocId, Old),
            Info = barrel_live:live(Live),
            Fix = maps:from_list(
                    [{T, B} || {Tok, B} <- maps:to_list(RemoteBodies),
                               is_map(B), map_size(B) > 0,
                               T <- [barrel_version:from_token(Tok)],
                               live_body(Info, T),
                               version_body(StoreRef, DbName, DocId, Old,
                                            T) =/= B]),
            case map_size(Fix) of
                0 ->
                    {ok, DocId, []};
                _ ->
                    BodyOf = fun(T) -> maps:get(T, Fix) end,
                    {ok, DocId, _} = do_write_state(
                        StoreRef, DbName, DocId, Old, Live, Live, BodyOf,
                        (replication_opts(Config))#{repair => Fix,
                                                    vectors => live_vectors(
                                                                 StoreRef, DbName,
                                                                 DocId, Old, Live)}),
                    {ok, DocId, lists:sort([barrel_version:to_token(T)
                                            || T <- maps:keys(Fix)])}
            end
    end.

live_body(Info, T) ->
    case Info of
        #{T := #{deleted := false}} -> true;
        _ -> false
    end.

replication_opts(Config) ->
    #{cause => replicated,
      outbox => maps:get(outbox_tags_on_replication, Config, [])}.

%% Client vectors travel with their version; computed ones are derived
%% data each replica recomputes. The client vectors of a document's live
%% versions (the current one from the entity, the others from the
%% archive), keyed by version.
live_vectors(StoreRef, DbName, DocId, Old, Live) ->
    Versions = [T || {T, #{deleted := false}}
                         <- maps:to_list(barrel_live:live(Live))],
    barrel_doc_vectors:versions(StoreRef, barrel_keyspace:resolve(DbName),
                                DocId, Old, Versions).

remote_vectors(Wire) when is_map(Wire) ->
    maps:from_list([{barrel_version:from_token(T), Vec}
                    || {T, Vec} <- maps:to_list(Wire), is_vector(Vec)]);
remote_vectors(_) ->
    #{}.

%% The one-version path carries the vector in the document, as a read
%% with include_embedding returns it.
carried_vector(Version, false, #{<<"_embedding">> := #{<<"vector">> := Vec,
                                                       <<"source">> := ?EMBEDDING_SRC_CLIENT}}) ->
    case is_vector(Vec) of
        true -> #{Version => Vec};
        false -> #{}
    end;
carried_vector(_Version, _Deleted, _Doc) ->
    #{}.

is_vector(Vec) ->
    barrel_doc_vectors:is_vector(Vec).

id_and_body(Doc) ->
    DocMap = barrel_doc:to_map(Doc),
    DocId = case maps:find(<<"id">>, DocMap) of
        {ok, Id} -> Id;
        error -> erlang:error({missing_id, DocMap})
    end,
    {DocId, barrel_doc:doc_without_meta(DocMap)}.

%% @doc Bound the live versions, resolve automatically when configured,
%% then write the document's new state.
commit_state(StoreRef, DbName, Config, DocId, Old, Live0, Live1, Bodies, Opts) ->
    Max = maps:get(max_conflict_versions, Config, ?DEFAULT_MAX_CONFLICT_VERSIONS),
    Live2 = barrel_live:evict(Live1, Max + 1, fun old_peer_version/1),
    BodyOf = fun(T) ->
        case Bodies of
            #{T := B} -> B;
            _ -> version_body(StoreRef, DbName, DocId, Old, T)
        end
    end,
    Vectors = maps:get(vectors, Opts, #{}),
    {Live3, AutoBodies, AutoVectors} =
        auto_resolve(Config, DocId, Live2, BodyOf, Vectors),
    BodyOf2 = fun(T) ->
        case AutoBodies of
            #{T := B} -> B;
            _ -> BodyOf(T)
        end
    end,
    write_state(StoreRef, DbName, DocId, Old, Live0, Live3, BodyOf2,
                Opts#{vectors => maps:merge(Vectors, AutoVectors)}).

%% A version written by a peer older than barrel_live: neither a writer
%% id (w + 16 hex) nor an automatic resolution. Evicted first.
old_peer_version({_Hlc, <<"w", Hex:16/binary>>}) ->
    not is_hex(Hex);
old_peer_version({_Hlc, <<"auto:", _/binary>>}) ->
    false;
old_peer_version(_) ->
    true.

is_hex(Bin) ->
    lists:all(fun(C) -> (C >= $0 andalso C =< $9) orelse (C >= $a andalso C =< $f) end,
              binary_to_list(Bin)).

%% @doc The configured resolver over the live versions, when there is a
%% conflict. Returns the new state, the body of the resolution and its
%% client vector: an input's when the result is that input's body.
auto_resolve(Config, DocId, Live, BodyOf, Vectors) ->
    case {maps:get(conflict_resolver, Config, undefined),
          barrel_live:auto_inputs(Live)} of
        {undefined, _} ->
            {Live, #{}, #{}};
        {_, []} ->
            {Live, #{}, #{}};
        {Fun, Inputs} ->
            LiveInfo = barrel_live:live(Live),
            Versions = [#{token => barrel_version:to_token(T),
                          deleted => maps:get(deleted, maps:get(T, LiveInfo)),
                          body => BodyOf(T)} || T <- Inputs],
            case resolver_result(Fun, DocId, Versions, LiveInfo) of
                keep ->
                    {Live, #{}, #{}};
                {Deleted, Body} ->
                    Hash = crypto:hash(sha256, canonical({Deleted, Body})),
                    case barrel_live:auto_step(Live, Deleted, Hash) of
                        {ok, Live1, Token} ->
                            {Live1, #{Token => Body},
                             resolved_vector(Token, Deleted, Body, Inputs,
                                             LiveInfo, BodyOf, Vectors)};
                        {noop, Live1} -> {Live1, #{}, #{}};
                        {stop, disagreement} ->
                            logger:warning("resolver_disagreement on ~p: replicas "
                                           "run different conflict_resolver "
                                           "functions", [DocId]),
                            {Live, #{}, #{}}
                    end
            end
    end.

%% A resolution whose body is a live input's keeps that input's client
%% vector; a new body has none.
resolved_vector(_Token, true, _Body, _Inputs, _LiveInfo, _BodyOf, _Vectors) ->
    #{};
resolved_vector(Token, false, Body, Inputs, LiveInfo, BodyOf, Vectors) ->
    case [Vec || T <- Inputs, #{T := Vec} <- [Vectors],
                 not maps:get(deleted, maps:get(T, LiveInfo)),
                 BodyOf(T) =:= Body] of
        [Vec | _] -> #{Token => Vec};
        [] -> #{}
    end.

resolver_result(Fun, DocId, Versions, LiveInfo) ->
    try call_resolver(Fun, Versions) of
        keep -> keep;
        delete -> {true, #{}};
        {merge, Body} when is_map(Body) -> {false, barrel_doc:doc_without_meta(Body)};
        {choose, Tok} ->
            case [V || #{token := T} = V <- Versions, T =:= Tok] of
                [#{deleted := D, body := B}] -> {D, B};
                [] -> keep
            end;
        _ -> keep
    catch
        Class:Reason ->
            _ = LiveInfo,
            logger:warning("conflict_resolver failed for ~p: ~p:~p",
                           [DocId, Class, Reason]),
            keep
    end.

call_resolver({M, F}, Versions) -> M:F(Versions);
call_resolver(Fun, Versions) when is_function(Fun, 1) -> Fun(Versions).

%% A canonical encoding of a resolver result, the same on every node:
%% map keys sorted, every value tagged.
canonical(T) -> iolist_to_binary(canon(T)).
canon(M) when is_map(M) ->
    [$m, integer_to_binary(map_size(M)), $:,
     [[canon(K), canon(V)] || {K, V} <- lists:sort(maps:to_list(M))]];
canon(L) when is_list(L) -> [$l, integer_to_binary(length(L)), $:, [canon(X) || X <- L]];
canon(B) when is_binary(B) -> [$b, integer_to_binary(byte_size(B)), $:, B];
canon(I) when is_integer(I) -> [$i, integer_to_binary(I), $;];
canon(F) when is_float(F) -> [$f, float_to_binary(F, [short]), $;];
canon(A) when is_atom(A) -> canon(atom_to_binary(A));
canon(T) when is_tuple(T) -> [$t, canon(tuple_to_list(T))].

%% @doc The body of a live version: the current body for the winner,
%% the archived body for a conflict sibling; a delete has none.
%% a delete keeps the replaced body in the entity; the delete has none
version_body(_StoreRef, _DbName, _DocId, #{version := V, deleted := true}, V) ->
    #{};
version_body(_StoreRef, _DbName, _DocId, #{version := V, body := B}, V) ->
    case B of undefined -> #{}; _ -> B end;
version_body(StoreRef, DbName, DocId, _Old, T) ->
    case read_version_body(StoreRef, DbName, DocId, T) of
        {ok, B} -> B;
        {error, _} -> #{}
    end.

%% @doc Write a document's new barrel_live state in one batch: entity
%% (winner, vector, conflict count, context), conflict and superseded
%% chain rows, bodies, feed row, indexes, channels, history. A state
%% that did not change writes nothing.
write_state(StoreRef, DbName, DocId, Old, Live0, NewLive, BodyOf, Opts) ->
    Unchanged = Old =/= undefined
        andalso maps:get(ctx, Old) =/= undefined
        andalso barrel_live:digest(Live0) =:= barrel_live:digest(NewLive)
        andalso not vectors_arrived(Old, NewLive, Opts),
    case Unchanged of
        true ->
            {unchanged, DocId, barrel_version:to_token(maps:get(version, Old))};
        false ->
            do_write_state(StoreRef, DbName, DocId, Old, Live0, NewLive,
                           BodyOf, Opts)
    end.

%% A peer sent a client vector this database does not hold: the winner's
%% or a live sibling's.
vectors_arrived(Old, NewLive, Opts) ->
    Vectors = maps:get(vectors, Opts, #{}),
    Stored = maps:get(stored, Opts, maps:keys(Vectors)),
    vector_arrived(Old, Vectors)
        orelse lists:any(fun(T) -> maps:is_key(T, Vectors)
                                       andalso not lists:member(T, Stored)
                         end, maps:get(conflicts, barrel_live:view(NewLive))).

vector_arrived(#{version := V, deleted := false, embedding := E}, Vectors) ->
    maps:is_key(V, Vectors) andalso not is_client(E);
vector_arrived(_Old, _Vectors) ->
    false.

is_client({_, ?EMBEDDING_SRC_CLIENT}) -> true;
is_client(_) -> false.

%% The winner's embedding columns: its client vector when known, else
%% the stored ones while the winner and its body stay the same. A
%% delete and a new body have none.
state_embedding(_W, true, _Changed, _Old, _Vectors) ->
    [];
state_embedding(W, false, Changed, Old, Vectors) ->
    case {Vectors, Changed, Old} of
        {#{W := Vec}, _, _} ->
            [{?COL_EMBEDDING, barrel_doc:encode_embedding(Vec)},
             {?COL_EMBEDDING_SRC, ?EMBEDDING_SRC_CLIENT}];
        {_, false, #{embedding := {Bin, Src}}} ->
            [{?COL_EMBEDDING, Bin}, {?COL_EMBEDDING_SRC, Src}];
        _ ->
            []
    end.

%% The live-document count is exact once a database has been counted
%% (marker set): a database written before the counter existed is counted
%% once, in a worker, then kept by every write. Until then it is unknown.
publish_doc_count(#state{name = Name, store_ref = StoreRef,
                         read_only = ReadOnly} = State) ->
    Ks = barrel_keyspace:resolve(Name),
    case doc_count(StoreRef, Ks) of
        {ok, N} ->
            ok = barrel_metrics:set_db_docs(Name, N),
            State;
        not_counted when ReadOnly ->
            State;
        not_counted ->
            start_sweep(doc_count, fun() -> count_docs(StoreRef, Ks) end,
                        none, false, State)
    end.

publish_att_count(#state{att_ref = undefined} = State) ->
    State;
publish_att_count(#state{name = Name, att_ref = AttRef, read_only = ReadOnly} = State) ->
    case barrel_att_store:att_count(AttRef, Name) of
        {ok, N} ->
            ok = barrel_metrics:set_db_attachments(Name, N),
            State;
        not_counted when not ReadOnly ->
            start_sweep(att_count, fun() -> barrel_att_store:count_atts(AttRef, Name) end,
                        none, false, State);
        _ ->
            State
    end.

att_count_value(undefined, _Name) -> undefined;
att_count_value(AttRef, Name) ->
    case barrel_att_store:att_count(AttRef, Name) of
        {ok, N} -> N;
        _ -> undefined
    end.

doc_count_value(StoreRef, Name) ->
    case doc_count(StoreRef, barrel_keyspace:resolve(Name)) of
        {ok, N} -> N;
        not_counted -> undefined
    end.

doc_count(StoreRef, Ks) ->
    case barrel_store_rocksdb:get(StoreRef, docs_counted_key(Ks)) of
        {ok, _} ->
            case barrel_store_rocksdb:get(StoreRef, barrel_store_keys:db_docs_count(Ks)) of
                {ok, Bin} -> {ok, max(0, binary_to_integer(Bin))};
                not_found -> {ok, 0}
            end;
        not_found ->
            not_counted
    end.

docs_counted_key(Ks) ->
    barrel_store_keys:db_docs_counted(Ks).

%% Count the live documents of a snapshot and add the difference to the
%% counter as it was in that snapshot. Writes after the snapshot already
%% moved the counter; merges add up in any order, so none is lost.
count_docs(StoreRef, Ks) ->
    {ok, Snap} = barrel_store_rocksdb:snapshot(StoreRef),
    try
        Live = barrel_store_rocksdb:fold_range_with_snapshot(
                 StoreRef, barrel_store_keys:doc_entity_prefix(Ks),
                 barrel_store_keys:doc_entity_end(Ks),
                 fun(_K, V, N) -> {ok, N + entity_live(V)} end, 0, Snap),
        C0 = case barrel_store_rocksdb:get_with_snapshot(
                    StoreRef, barrel_store_keys:db_docs_count(Ks), Snap) of
            {ok, Bin} -> binary_to_integer(Bin);
            not_found -> 0
        end,
        barrel_store_rocksdb:write_batch(
          StoreRef, [{merge, barrel_store_keys:db_docs_count(Ks), Live - C0},
                     {put, docs_counted_key(Ks), <<>>}], #{sync => true})
    after
        barrel_store_rocksdb:release_snapshot(Snap)
    end.

%% The deleted byte follows the version in an encoded entity.
entity_live(<<VerLen:16, _:VerLen/binary, 0:8, _/binary>>) -> 1;
entity_live(_) -> 0.

%% The live-document counter moves in the batch that changes the document:
%% +1 when it becomes live (new, or put back after a delete), -1 when its
%% winner becomes a delete. An absent document is not live.
doc_count_ops(Ks, Old, NewDeleted) ->
    case is_live(NewDeleted) - was_live(Old) of
        0 -> [];
        Delta -> [{merge, barrel_store_keys:db_docs_count(Ks), Delta}]
    end.

was_live(undefined) -> 0;
was_live(#{deleted := true}) -> 0;
was_live(_) -> 1.

is_live(true) -> 0;
is_live(_) -> 1.

do_write_state(StoreRef, DbName, DocId, Old, Live0, NewLive, BodyOf, Opts) ->
    Ks = barrel_keyspace:resolve(DbName),
    case barrel_live:view(NewLive) of
        #{winner := undefined} -> erlang:error({empty_live, DocId});
        _ -> ok
    end,
    #{winner := W, deleted := WDel, conflicts := Sibs} = barrel_live:view(NewLive),
    Info1 = barrel_live:live(NewLive),
    Info0 = barrel_live:live(Live0),
    #{vv := WVV} = maps:get(W, Info1),
    ChangeHlc = alloc_hlc(DbName),
    Token = barrel_version:to_token(W),
    {OldW, OldHlc, OldBody, OldCbor, OldDeleted, CreatedAt, ExpiresAt, Tier} =
        case Old of
            undefined ->
                {undefined, undefined, undefined, undefined, false,
                 barrel_hlc:encode(ChangeHlc), 0, 0};
            #{version := OV, hlc := OH, body := OB, body_cbor := OBC,
              deleted := OD, created_at := OCA, expires_at := OEA, tier := OT} ->
                {OV, OH, OB, OBC, OD, OCA, OEA, OT}
        end,
    %% a body repair rewrites the winner like a new one, same version
    Repair = maps:get(repair, Opts, #{}),
    WinnerChanged = W =/= OldW orelse maps:is_key(W, Repair),
    WBody = case WinnerChanged of
        true -> BodyOf(W);
        false -> OldBody
    end,
    DocColumns = [
        {?COL_VERSION, barrel_version:encode(W)},
        {?COL_DELETED, deleted_to_bin(WDel)},
        {?COL_HLC, barrel_hlc:encode(ChangeHlc)},
        {?COL_VV, barrel_vv:encode(WVV)},
        {?COL_NCONFLICTS, length(Sibs)},
        {?COL_CTX, barrel_live:encode_ctx(NewLive)},
        {?COL_CREATED_AT, CreatedAt},
        {?COL_EXPIRES_AT, ExpiresAt},
        {?COL_TIER, Tier}
    ] ++ state_embedding(W, WDel, WinnerChanged, Old,
                         maps:get(vectors, Opts, #{})),
    DocOps = [{entity_put, barrel_store_keys:doc_entity(Ks, DocId), DocColumns}],
    ChainKey = fun(T) -> barrel_store_keys:doc_version(Ks, DocId,
                                                       barrel_version:encode(T)) end,
    BodyRevKey = fun(T) -> barrel_store_keys:doc_body_rev(Ks, DocId,
                                                          barrel_version:encode(T)) end,
    Vectors = maps:get(vectors, Opts, #{}),
    Stored = maps:get(stored, Opts, maps:keys(Vectors)),
    %% live siblings: conflict rows; new ones bring their body; a vector
    %% this database did not hold is kept next to the body
    SibOps = lists:flatmap(
        fun(T) ->
            #{deleted := D, vv := VV} = maps:get(T, Info1),
            Row = {put, ChainKey(T), sibling_entry(conflict, D, VV)},
            Known = maps:is_key(T, Info0),
            VecOps = case Vectors of
                #{T := Vec} when not D ->
                    case Known andalso lists:member(T, Stored) of
                        true -> [];
                        false -> [barrel_doc_vectors:put_op(Ks, DocId, T, Vec)]
                    end;
                _ -> []
            end,
            case Known andalso not maps:is_key(T, Repair) of
                true -> [Row | VecOps];
                false -> [Row, {body_put, BodyRevKey(T),
                                barrel_docdb_codec_cbor:encode_cbor(BodyOf(T))}
                          | VecOps]
            end
        end, Sibs),
    %% versions no longer live: superseded rows (their bodies stay archived
    %% until retention)
    GoneOps = [{put, ChainKey(T), sibling_entry(superseded, D, VV)}
               || {T, #{deleted := D, vv := VV}} <- maps:to_list(Info0),
                  not maps:is_key(T, Info1)],
    %% the winner lives in the entity, not in the chain; its archived
    %% vector moves into the entity
    WinnerRowOps = case maps:is_key(W, Info0) andalso W =/= OldW of
        true -> [{delete, ChainKey(W)},
                 barrel_doc_vectors:delete_op(Ks, DocId, barrel_version:encode(W))];
        false -> []
    end,
    %% the previous winner's body moves to the archive when it stops
    %% being the winner (a delete has none: its entity still holds the
    %% body it replaced)
    ArchiveOps = case {WinnerChanged, OldW, OldCbor, OldDeleted} of
        {true, OldV, Cbor, false} when OldV =/= undefined, OldV =/= W,
                                       Cbor =/= undefined ->
            [{body_put, BodyRevKey(OldV), Cbor}
             | outgoing_vector_ops(Ks, DocId, OldV, Old, Vectors)];
        _ -> []
    end,
    DocInfo = #{id => DocId, rev => Token, deleted => WDel,
                num_conflicts => length(Sibs), hlc => ChangeHlc},
    HlcDeleteOps = case OldHlc of
        undefined -> [];
        _ -> [{delete, barrel_store_keys:doc_hlc(Ks, OldHlc)}]
    end,
    ChangeInfo = DocInfo#{doc => WBody},
    ChangeOps = barrel_changes:write_change_ops(DbName, ChangeHlc, ChangeInfo),
    {IndexOps, PathHlcOps, ChannelOps, BodyOps} =
        case WinnerChanged of
            true ->
                PIdx = winner_index_ops(StoreRef, DbName, DocId, Old, WDel, WBody),
                PHlc = case OldHlc of
                    undefined ->
                        barrel_changes:write_path_index_ops(DbName, ChangeHlc,
                                                           ChangeInfo);
                    _ ->
                        barrel_changes:update_path_index_ops(DbName, ChangeHlc,
                                                            ChangeInfo, OldHlc,
                                                            OldBody)
                end,
                Chan = channel_ops(DbName, ChangeHlc, DocInfo, WBody, OldHlc,
                                   OldBody, OldDeleted),
                Body = [{body_put, barrel_store_keys:doc_body(Ks, DocId),
                         barrel_docdb_codec_cbor:encode_cbor(
                           case WBody of undefined -> #{}; _ -> WBody end)}],
                {PIdx, PHlc, Chan, Body};
            false ->
                PHlc = barrel_changes:update_path_index_ops(DbName, ChangeHlc,
                                                           ChangeInfo, OldHlc,
                                                           OldBody),
                Chan = barrel_channel:move_ops(
                         fun(Key) -> barrel_store_rocksdb:get(StoreRef, Key) end,
                         DbName, OldHlc, ChangeHlc, DocInfo),
                {[], PHlc, Chan, []}
        end,
    %% history: one entry per version new to this document
    New = [T || T <- maps:keys(Info1), not maps:is_key(T, Info0)],
    Cause = maps:get(cause, Opts, replicated),
    HistoryOps = lists:append(
        [begin
             #{deleted := D, vv := VV} = maps:get(T, Info1),
             Hlc = case T of W -> ChangeHlc; _ -> alloc_hlc(DbName) end,
             barrel_history:write_ops(DbName, Hlc, DocId, T, D, Cause, VV)
         end || T <- New]),
    OutboxOps = case WinnerChanged of
        true -> barrel_outbox:write_ops(DbName, maps:get(outbox, Opts, []),
                                        ChangeHlc, OldHlc, DocId, Token, WDel);
        false -> []
    end,
    AllOps = DocOps ++ HlcDeleteOps ++ IndexOps ++ ChangeOps ++ PathHlcOps
        ++ ArchiveOps ++ SibOps ++ GoneOps ++ WinnerRowOps ++ HistoryOps
        ++ OutboxOps ++ ChannelOps ++ BodyOps ++ doc_count_ops(Ks, Old, WDel),
    ok = barrel_store_rocksdb:write_batch(StoreRef, AllOps, #{}),
    notify_subscribers(DbName, DocId, Token, ChangeHlc, WDel, WBody),
    {ok, DocId, Token}.

%% The client vector of the version that stops being current: the one a
%% peer sent for it, else the one its entity held.
outgoing_vector_ops(Ks, DocId, OldV, Old, Vectors) ->
    case Vectors of
        #{OldV := Vec} -> [barrel_doc_vectors:put_op(Ks, DocId, OldV, Vec)];
        _ -> barrel_doc_vectors:archive_ops(Ks, DocId, Old)
    end.

winner_index_ops(StoreRef, DbName, DocId, Old, NewDeleted, NewBody) ->
    {OldBody, OldDeleted} = case Old of
        undefined -> {undefined, false};
        #{body := OB, deleted := OD} -> {OB, OD}
    end,
    case NewDeleted of
        true when OldBody =/= undefined, not OldDeleted ->
            case barrel_ars_index:get_doc_paths(StoreRef, DbName, DocId) of
                {ok, OldPaths} -> barrel_ars_index:remove_doc_ops(DbName, DocId, OldPaths);
                not_found -> []
            end;
        true ->
            [];
        false when OldBody =:= undefined; OldDeleted ->
            barrel_ars_index:index_doc_ops(DbName, DocId, NewBody);
        false ->
            barrel_ars_index:update_doc_ops(DbName, DocId, OldBody, NewBody)
    end.

%% @doc The replication diff for peers that send one version at a time:
%% `have' when this document has seen the offered version.
do_diff_versions(StoreRef, DbName, TokenMap) ->
    Result = maps:map(
        fun(DocId, Token) ->
            Version = barrel_version:from_token(Token),
            case read_current(StoreRef, DbName, DocId) of
                undefined ->
                    missing;
                Old ->
                    case barrel_live:seen(load_live(StoreRef, DbName, DocId, Old),
                                          Version) of
                        true -> have;
                        false -> missing
                    end
            end
        end,
        TokenMap),
    {ok, Result}.

%% @doc The full-state diff: ids whose state differs from the offered
%% digest (or that this database doesn't have).
do_diff_states(StoreRef, DbName, DigestMap) ->
    {ok, maps:map(
           fun(DocId, Digest) ->
               case read_current(StoreRef, DbName, DocId) of
                   undefined -> missing;
                   Old ->
                       case barrel_live:digest(
                              load_live(StoreRef, DbName, DocId, Old)) of
                           Digest -> have;
                           _ -> missing
                       end
               end
           end, DigestMap)}.

%% @doc Put a local document (per-database)
do_put_local_doc(StoreRef, DbName, DocId, Doc, Opts) ->
    Ks = barrel_keyspace:resolve(DbName),
    Key = barrel_store_keys:local_doc_key(Ks, DocId),
    Value = term_to_binary(Doc),
    ok = barrel_store_rocksdb:local_put(StoreRef, Key, Value,
                                        [{sync, maps:get(sync, Opts, false)}]),
    ok.

%% @doc Get a local document (per-database)
do_get_local_doc(StoreRef, DbName, DocId) ->
    Ks = barrel_keyspace:resolve(DbName),
    Key = barrel_store_keys:local_doc_key(Ks, DocId),
    case barrel_store_rocksdb:local_get(StoreRef, Key) of
        {ok, Value} ->
            {ok, binary_to_term(Value)};
        not_found ->
            {error, not_found}
    end.

%% @doc Delete a local document (per-database)
do_delete_local_doc(StoreRef, DbName, DocId) ->
    Ks = barrel_keyspace:resolve(DbName),
    Key = barrel_store_keys:local_doc_key(Ks, DocId),
    case barrel_store_rocksdb:local_get(StoreRef, Key) of
        {ok, _} ->
            ok = barrel_store_rocksdb:local_delete(StoreRef, Key),
            ok;
        not_found ->
            {error, not_found}
    end.

%% @doc Fold over local documents matching a prefix
%% The prefix is applied to the DocId portion (after DbName).
do_fold_local_docs(StoreRef, DbName, DocIdPrefix, Fun, Acc0) ->
    Ks = barrel_keyspace:resolve(DbName),
    %% Build full key prefix: DbName + 0 + DocIdPrefix
    KeyPrefix = barrel_store_keys:local_doc_key(Ks, DocIdPrefix),
    %% Fold over matching keys, decode values, extract DocId
    DbNameLen = byte_size(Ks),
    Result = barrel_store_rocksdb:local_fold(
        StoreRef,
        KeyPrefix,
        fun(Key, Value, Acc) ->
            %% Extract DocId from key (skip DbName + 0)
            <<_:DbNameLen/binary, 0, DocId/binary>> = Key,
            Doc = binary_to_term(Value),
            Fun(DocId, Doc, Acc)
        end,
        Acc0
    ),
    {ok, Result}.

%%====================================================================
%% Subscription Notifications
%%====================================================================

%% @doc Notify the subscribers of one write.
notify_subscribers(DbName, DocId, Rev, Hlc, Deleted, DocBody) ->
    notify_group(DbName, [{DocId, Rev, Hlc, Deleted, DocBody}]).

%% @doc Notify the subscribers of a commit's writes, in commit order: one
%% cast per subscription manager, and nothing at all when the database
%% has no subscription.
notify_group(_DbName, []) ->
    ok;
notify_group(DbName, Notes) ->
    notify_group(DbName, Notes, has_subs(?SUB_DBS_TAB, DbName),
                 has_subs(?QUERY_SUB_DBS_TAB, DbName)).

notify_group(_DbName, _Notes, false, false) ->
    ok;
notify_group(DbName, Notes, PathSubs, QuerySubs) ->
    ok = notify_paths(PathSubs, DbName, Notes),
    notify_queries(QuerySubs, DbName, Notes).

notify_paths(false, _DbName, _Notes) ->
    ok;
notify_paths(true, DbName, Notes) ->
    barrel_sub:notify(DbName, [path_change(N) || N <- Notes]).

%% Deleted docs are announced under their id; live ones under their paths.
path_change({DocId, Rev, Hlc, Deleted, _DocBody, Topics}) ->
    {Topics, #{id => DocId, rev => Rev, hlc => Hlc, deleted => Deleted,
               paths => Topics}};
path_change({DocId, Rev, Hlc, true, _DocBody}) ->
    {[DocId], #{id => DocId, rev => Rev, hlc => Hlc, deleted => true,
                paths => [DocId]}};
path_change({DocId, Rev, Hlc, false, DocBody}) ->
    Topics = barrel_ars:paths_to_topics(barrel_ars:analyze(DocBody)),
    {Topics, #{id => DocId, rev => Rev, hlc => Hlc, deleted => false,
               paths => Topics}}.

%% Query subscribers only see live documents.
notify_queries(false, _DbName, _Notes) ->
    ok;
notify_queries(true, DbName, Notes) ->
    case [{Id, Rev, Body} || Note <- Notes,
                             {Id, Rev, false, Body} <- [live_note(Note)]] of
        [] -> ok;
        Live -> barrel_query_sub:notify_changes(DbName, Live)
    end.

live_note({Id, Rev, _Hlc, Deleted, Body, _Topics}) -> {Id, Rev, Deleted, Body};
live_note({Id, Rev, _Hlc, Deleted, Body}) -> {Id, Rev, Deleted, Body}.

%% A missing table means its manager is down: it holds no subscription.
has_subs(Tab, DbName) ->
    try ets:member(Tab, DbName)
    catch error:badarg -> false
    end.

%%====================================================================
%% Wide Column Helpers
%%====================================================================

%% @doc Convert boolean to binary for wide column storage
-spec deleted_to_bin(boolean()) -> binary().
deleted_to_bin(true) -> <<"true">>;
deleted_to_bin(false) -> <<"false">>.

%% @doc Convert binary back to boolean from wide column storage
-spec bin_to_deleted(binary()) -> boolean().
bin_to_deleted(<<"true">>) -> true;
bin_to_deleted(<<"false">>) -> false;
bin_to_deleted(_) -> false.


%% Conflict decoration for reads (maybe_add_conflicts*) moved to
%% barrel_docdb_reader with the read paths.

%%====================================================================
%% Conflict Operations
%%====================================================================

%% @doc Get list of conflicting version tokens for a document.
%% Fast path: the entity carries the live sibling count, so conflict-free
%% documents never scan the version chain.
do_get_conflicts(StoreRef, DbName, DocId) ->
    case read_current(StoreRef, DbName, DocId) of
        undefined ->
            {error, not_found};
        #{num_conflicts := 0} ->
            {ok, []};
        #{} ->
            {ok, [barrel_version:to_token(V)
                  || {V, _Entry} <- conflict_siblings(StoreRef, DbName, DocId)]}
    end.

%% @doc Scan the version chain for live conflict siblings.
%% Returns [{version(), sibling_entry_map()}].
conflict_siblings(StoreRef, DbName, DocId) ->
    Ks = barrel_keyspace:resolve(DbName),
    Fold = fun(_Key, VersionEnc, Value, Acc) ->
        Entry = decode_sibling_entry(Value),
        case maps:get(flag, Entry) of
            conflict ->
                {ok, [{barrel_version:decode(VersionEnc), Entry} | Acc]};
            superseded ->
                {ok, Acc}
        end
    end,
    lists:reverse(barrel_doc_chain:fold(StoreRef, Ks, DocId, Fold, [])).

%% @doc Resolve a conflict with a new version replacing the versions the
%% caller observed. `Observed' is the list of tokens read (new form), or
%% the current winner's token (compatibility: replaces every current
%% version, even one that arrived after the caller's read).
%% Resolution: {choose, Token} (keeps that version's body and deleted
%% flag), {merge, Body}, or delete.
do_resolve_conflict(StoreRef, DbName, Config, DocId, BaseRevOrObserved,
                    Resolution) ->
    case read_current(StoreRef, DbName, DocId) of
        undefined ->
            {error, not_found};
        #{version := Current} = Old ->
            CurrentToken = barrel_version:to_token(Current),
            Live = load_live(StoreRef, DbName, DocId, Old),
            Replaces = case BaseRevOrObserved of
                CurrentToken ->
                    barrel_live:tokens(Live);
                Observed when is_list(Observed) ->
                    In = [barrel_version:from_token(T) || T <- Observed],
                    [T || T <- barrel_live:tokens(Live), lists:member(T, In)];
                _ ->
                    {error, {conflict, CurrentToken}}
            end,
            case Replaces of
                {error, _} = Err ->
                    Err;
                [] ->
                    {error, conflict};
                [_] when is_binary(BaseRevOrObserved) ->
                    {error, no_conflicts};
                _ ->
                    resolve_with(StoreRef, DbName, Config, DocId, Old, Live,
                                 Replaces,
                                 Resolution)
            end
    end.

resolve_with(StoreRef, DbName, Config, DocId, Old, Live, Replaces, Resolution) ->
    Info = barrel_live:live(Live),
    Local = live_vectors(StoreRef, DbName, DocId, Old, Live),
    Picked = case Resolution of
        {merge, Doc} when is_map(Doc) ->
            {ok, false, barrel_doc:doc_without_meta(Doc),
             barrel_doc_vectors:client_vector(Doc)};
        delete ->
            {ok, true, #{}, undefined};
        {choose, Tok} ->
            case [T || T <- Replaces, barrel_version:to_token(T) =:= Tok] of
                [T] ->
                    #{deleted := D} = maps:get(T, Info),
                    {ok, D, version_body(StoreRef, DbName, DocId, Old, T),
                     maps:get(T, Local, undefined)};
                [] ->
                    {error, {unknown_version, Tok}}
            end;
        _ ->
            {error, {invalid_resolution, Resolution}}
    end,
    case Picked of
        {ok, Deleted, Body, Vec} ->
            Hlc = above_live(DbName, Live, alloc_hlc(DbName)),
            Token = barrel_version:new(Hlc, writer_id(DbName)),
            NewLive = barrel_live:author(Live, Token, Deleted, Replaces),
            BodyOf = fun(T) when T =:= Token -> Body;
                        (T) -> version_body(StoreRef, DbName, DocId, Old, T)
                     end,
            Vectors = case Vec of
                undefined -> Local;
                _ -> Local#{Token => Vec}
            end,
            {_, DocId, Rev} = write_state(StoreRef, DbName, DocId, Old, Live,
                                          NewLive, BodyOf,
                                          #{cause => resolve, vectors => Vectors,
                                            stored => maps:keys(Local),
                                            outbox => resolve_tags(Config)}),
            {ok, #{id => DocId, rev => Rev,
                   conflicts_resolved => length(Replaces) - 1}};
        {error, _} = Err ->
            Err
    end.

%% A resolution is not issued through the layer that tags its own writes
%% (barrel adds its index tag to put_doc): tag it like a replicated write,
%% so a record database indexes the resolved version.
resolve_tags(Config) ->
    maps:get(outbox_tags_on_replication, Config, []).

%% @private Read the archived body of a non-current version.
read_version_body(StoreRef, DbName, DocId, Version) ->
    Ks = barrel_keyspace:resolve(DbName),
    Key = barrel_store_keys:doc_body_rev(Ks, DocId,
                                         barrel_version:encode(Version)),
    case barrel_store_rocksdb:body_get(StoreRef, Key) of
        {ok, CborBin} -> {ok, barrel_docdb_codec_cbor:decode_any(CborBin)};
        not_found -> {error, {version_body_missing, barrel_version:to_token(Version)}};
        {error, _} = Err -> Err
    end.
