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
-export([start_link/2]).
-export([info/1, stop/1]).
-export([get_store_ref/1, get_att_ref/1]).

%% Document API
-export([
    put_doc/3,
    put_docs/3,
    get_doc/3,
    get_docs/3,
    delete_doc/3,
    fold_docs/3,
    fold_docs/4,
    resolve_conflict/4,
    get_conflicts/2
]).

%% Tagged outbox API (acks are writes, so they go through the server)
-export([outbox_ack/3]).

%% Embedding column (computed vectors written back by an indexer)
-export([set_doc_embedding/4]).

%% Retention sweep (also runs on a timer; exposed for tests and tools)
-export([sweep_retention/1]).

%% Replication API
-export([
    put_version/5,
    diff_versions/2
]).

%% Local document API (for checkpoints, not replicated)
-export([
    put_local_doc/3,
    get_local_doc/2,
    delete_local_doc/2,
    fold_local_docs/4
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2]).

-record(state, {
    name :: binary(),
    config :: map(),
    db_path :: string(),
    store_ref :: barrel_store_rocksdb:db_ref() | undefined,
    att_ref :: barrel_att_store:att_ref() | undefined,
    filter_pid :: pid() | undefined,  %% Compaction filter handler for this database
    compaction_timer :: reference() | undefined,  %% Timer for periodic compaction checks
    compaction_interval :: pos_integer(),  %% Interval between checks in ms (default: 1 hour)
    compaction_size_threshold :: pos_integer(),  %% Size threshold in bytes to trigger compaction
    retention_period :: non_neg_integer(),  %% Retention window in seconds (0 = infinite)
    retention_timer :: reference() | undefined,  %% Timer for periodic retention sweeps
    retention_interval :: pos_integer()  %% Interval between sweeps in ms
}).

%% Default compaction settings
-define(DEFAULT_COMPACTION_INTERVAL, 3600000).  %% 1 hour in ms
-define(DEFAULT_COMPACTION_SIZE_THRESHOLD, 1073741824).  %% 1 GB in bytes

%% Default retention settings: 30 days, swept every 10 minutes.
%% retention_period 0 means infinite (no sweeper started).
-define(DEFAULT_RETENTION_PERIOD, 2592000).  %% 30 days in seconds
-define(DEFAULT_RETENTION_INTERVAL, 600000).  %% 10 minutes in ms

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

%% @doc Get database info
-spec info(pid()) -> {ok, map()} | {error, term()}.
info(Pid) ->
    gen_server:call(Pid, info).

%% @doc Get the document store reference
-spec get_store_ref(pid()) -> {ok, barrel_store_rocksdb:db_ref()} | {error, term()}.
get_store_ref(Pid) ->
    gen_server:call(Pid, get_store_ref).

%% @doc Get the attachment store reference
-spec get_att_ref(pid()) -> {ok, barrel_att_store:att_ref()} | {error, term()}.
get_att_ref(Pid) ->
    gen_server:call(Pid, get_att_ref).


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

%% @doc Put multiple documents (batch write)
-spec put_docs(pid(), [map()], map()) -> [{ok, map()} | {error, term()}].
put_docs(Pid, Docs, Opts) ->
    gen_server:call(Pid, {put_docs, Docs, Opts}).

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

%%====================================================================
%% Replication API functions
%%====================================================================

%% @doc Put a document with explicit revision history (for replication)
%% Returns {ok, DocId, RevId} on success.
-spec put_version(pid(), map(), binary(), binary(), boolean()) ->
    {ok, binary(), binary()} | {error, term()}.
put_version(Pid, Doc, VersionToken, VVBin, Deleted) ->
    gen_server:call(Pid, {put_version, Doc, VersionToken, VVBin, Deleted}).

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
init([Name, Config]) ->
    process_flag(trap_exit, true),

    %% Get data directory from config. Fall back to the `data_dir' app env so a
    %% node can relocate ALL of its dbs (including internal ones created with no
    %% opts) off the shared default - lets two local nodes use distinct dirs.
    DefaultDataDir = application:get_env(barrel_docdb, data_dir, "/tmp/barrel_data"),
    DataDir = maps:get(data_dir, Config, DefaultDataDir),
    DbPath = filename:join([DataDir, binary_to_list(Name)]),

    %% A previous instance that crashed without terminate leaves its
    %% store handle in persistent_term: the RocksDB LOCK stays held and
    %% the database cannot reopen. Release it when its owner is gone.
    ok = release_stale_refs(Name),

    %% Start compaction filter handler BEFORE opening RocksDB
    %% (handler pid is passed to CF options)
    PruneDepth = maps:get(prune_depth, Config, 1000),
    {ok, FilterPid} = barrel_compaction_filter:start_link(#{
        db_name => Name,
        prune_depth => PruneDepth
    }),

    %% Open document store (RocksDB with column families including body CF with BlobDB)
    DocStorePath = filename:join(DbPath, "docs"),
    StoreOpts0 = maps:get(store_opts, Config, #{}),
    %% Pass the filter handler to the store options
    StoreOpts = StoreOpts0#{compaction_filter_handler => FilterPid},
    case barrel_store_rocksdb:open(DocStorePath, StoreOpts) of
        {ok, StoreRef} ->
            %% Open attachment store (separate RocksDB with BlobDB)
            AttStorePath = filename:join(DbPath, "attachments"),
            AttOpts = maps:get(att_opts, Config, #{}),
            case barrel_att_store:open(AttStorePath, AttOpts) of
                {ok, AttRef} ->
                    %% Register in persistent_term for lookup
                    %% barrel_store is used by barrel_doc_body_store for body CF access
                    %% and by compaction filter handler for body deletion
                    persistent_term:put({barrel_db, Name}, self()),
                    persistent_term:put({barrel_store, Name}, StoreRef),
                    persistent_term:put({barrel_source, Name},
                                        ensure_source_id(StoreRef, Name)),

                    %% Compaction settings from config (or defaults)
                    CompactionInterval = maps:get(compaction_interval, Config,
                                                  ?DEFAULT_COMPACTION_INTERVAL),
                    CompactionThreshold = maps:get(compaction_size_threshold, Config,
                                                   ?DEFAULT_COMPACTION_SIZE_THRESHOLD),

                    %% Start periodic compaction check timer
                    TimerRef = erlang:send_after(CompactionInterval, self(), compaction_check),

                    %% Retention settings; the sweeper only runs for a
                    %% finite window
                    RetentionPeriod = maps:get(retention_period, Config,
                                               ?DEFAULT_RETENTION_PERIOD),
                    RetentionInterval = maps:get(retention_check_interval, Config,
                                                 ?DEFAULT_RETENTION_INTERVAL),
                    RetentionTimer = case RetentionPeriod of
                        0 -> undefined;
                        _ -> erlang:send_after(RetentionInterval, self(),
                                               retention_sweep)
                    end,

                    logger:info("Database ~s started at ~s", [Name, DbPath]),
                    {ok, #state{
                        name = Name,
                        config = Config,
                        db_path = DbPath,
                        store_ref = StoreRef,
                        att_ref = AttRef,
                        filter_pid = FilterPid,
                        compaction_timer = TimerRef,
                        compaction_interval = CompactionInterval,
                        compaction_size_threshold = CompactionThreshold,
                        retention_period = RetentionPeriod,
                        retention_timer = RetentionTimer,
                        retention_interval = RetentionInterval
                    }};
                {error, AttReason} ->
                    %% Close document store if attachment store fails
                    barrel_store_rocksdb:close(StoreRef),
                    exit(FilterPid, shutdown),
                    {stop, {att_store_open_failed, AttReason}}
            end;
        {error, Reason} ->
            exit(FilterPid, shutdown),
            {stop, {store_open_failed, Reason}}
    end.

%% @doc Handle synchronous calls
handle_call(info, _From, #state{name = Name, config = Config, db_path = DbPath,
                                store_ref = StoreRef,
                                retention_period = RetentionPeriod} = State) ->
    Info = #{
        name => Name,
        config => Config,
        db_path => DbPath,
        pid => self(),
        retention_period => RetentionPeriod,
        history_floor => barrel_history:history_floor(StoreRef, Name)
    },
    {reply, {ok, Info}, State};

handle_call(get_store_ref, _From, #state{store_ref = StoreRef} = State) ->
    {reply, {ok, StoreRef}, State};

handle_call(get_att_ref, _From, #state{att_ref = AttRef} = State) ->
    {reply, {ok, AttRef}, State};

%% Document operations
handle_call({put_doc, Doc, Opts}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_put_doc(StoreRef, DbName, Doc, Opts),
    {reply, Result, State};

handle_call({put_docs, Docs, Opts}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_put_docs(StoreRef, DbName, Docs, Opts),
    {reply, Result, State};

handle_call({get_doc, DocId, Opts}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_get_doc(StoreRef, DbName, DocId, Opts),
    {reply, Result, State};

handle_call({get_docs, DocIds, Opts}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_get_docs(StoreRef, DbName, DocIds, Opts),
    {reply, Result, State};

handle_call({delete_doc, DocId, Opts}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_delete_doc(StoreRef, DbName, DocId, Opts),
    {reply, Result, State};

handle_call({fold_docs, Fun, Acc}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_fold_docs(StoreRef, DbName, Fun, Acc, #{}),
    {reply, Result, State};

handle_call({fold_docs, Fun, Acc, Opts}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_fold_docs(StoreRef, DbName, Fun, Acc, Opts),
    {reply, Result, State};

%% Embedding column write-back
handle_call({set_doc_embedding, DocId, ExpectedRev, Vector}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_set_doc_embedding(StoreRef, DbName, DocId, ExpectedRev, Vector),
    {reply, Result, State};

%% Retention sweep (manual trigger; also runs on the timer)
handle_call(sweep_retention, _From,
            #state{name = DbName, store_ref = StoreRef,
                   retention_period = RetentionPeriod} = State) ->
    Result = case RetentionPeriod of
        0 -> {ok, #{retention => infinite}};
        _ -> do_retention_sweep(StoreRef, DbName, RetentionPeriod)
    end,
    {reply, Result, State};

%% Tagged outbox operations
handle_call({outbox_ack, Tag, Hlcs}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = case [{delete, barrel_store_keys:outbox_key(DbName, Tag, Hlc)}
                   || Hlc <- Hlcs] of
        [] -> ok;
        Ops -> barrel_store_rocksdb:write_batch(StoreRef, Ops, #{})
    end,
    {reply, Result, State};

%% Conflict operations
handle_call({get_conflicts, DocId}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_get_conflicts(StoreRef, DbName, DocId),
    {reply, Result, State};

handle_call({resolve_conflict, DocId, BaseRev, Resolution}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_resolve_conflict(StoreRef, DbName, DocId, BaseRev, Resolution),
    {reply, Result, State};

%% Replication operations
handle_call({put_version, Doc, VersionToken, VVBin, Deleted}, _From,
            #state{name = DbName, store_ref = StoreRef,
                   config = Config} = State) ->
    Result = try
        do_put_version(StoreRef, DbName, Config, Doc,
                       barrel_version:from_token(VersionToken),
                       barrel_vv:decode(VVBin), Deleted)
    catch
        Class:Reason ->
            {error, {invalid_version, {Class, Reason}}}
    end,
    {reply, Result, State};

handle_call({diff_versions, TokenMap}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = try
        do_diff_versions(StoreRef, DbName, TokenMap)
    catch
        Class:Reason ->
            {error, {invalid_version, {Class, Reason}}}
    end,
    {reply, Result, State};

%% Local document operations
handle_call({put_local_doc, DocId, Doc}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_put_local_doc(StoreRef, DbName, DocId, Doc),
    {reply, Result, State};

handle_call({get_local_doc, DocId}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_get_local_doc(StoreRef, DbName, DocId),
    {reply, Result, State};

handle_call({delete_local_doc, DocId}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_delete_local_doc(StoreRef, DbName, DocId),
    {reply, Result, State};

handle_call({fold_local_docs, Prefix, Fun, Acc}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_fold_local_docs(StoreRef, DbName, Prefix, Fun, Acc),
    {reply, Result, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

%% @doc Handle asynchronous casts
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @doc Handle other messages
handle_info(compaction_check, #state{store_ref = StoreRef,
                                      compaction_interval = Interval,
                                      compaction_size_threshold = Threshold} = State) ->
    %% Check database size and trigger compaction if needed
    NewState = do_check_compaction(StoreRef, Threshold, State),
    %% Restart timer for next check
    TimerRef = erlang:send_after(Interval, self(), compaction_check),
    {noreply, NewState#state{compaction_timer = TimerRef}};

handle_info(retention_sweep, #state{name = DbName, store_ref = StoreRef,
                                    retention_period = RetentionPeriod,
                                    retention_interval = Interval} = State) ->
    {ok, _Stats} = do_retention_sweep(StoreRef, DbName, RetentionPeriod),
    TimerRef = erlang:send_after(Interval, self(), retention_sweep),
    {noreply, State#state{retention_timer = TimerRef}};

handle_info(_Info, State) ->
    {noreply, State}.

%% @doc Clean up when terminating
terminate(_Reason, #state{name = Name, store_ref = StoreRef, att_ref = AttRef,
                          filter_pid = FilterPid,
                          compaction_timer = CompactionTimer,
                          retention_timer = RetentionTimer}) ->
    %% Cancel compaction timer
    _ =
      case CompactionTimer of
          undefined -> ok;
          _ -> erlang:cancel_timer(CompactionTimer)
      end,
    %% Cancel retention timer
    _ =
      case RetentionTimer of
          undefined -> ok;
          _ -> erlang:cancel_timer(RetentionTimer)
      end,
    %% Stop compaction filter handler
    _ = 
      case FilterPid of
          undefined -> ok;
          _ ->
              try exit(FilterPid, shutdown)
              catch _:_ -> ok
              end
      end,
    %% Close attachment store
    _ = 
      case AttRef of
          undefined -> ok;
          _ -> barrel_att_store:close(AttRef)
      end,
    %% Unregister BEFORE closing the store so caller-side readers
    %% (barrel_docdb_reader, barrel_doc_body_store) stop picking up a ref
    %% that is about to be closed. In-flight readers that already hold the
    %% ref are handled by the reader's badarg guard.
    persistent_term:erase({barrel_db, Name}),
    persistent_term:erase({barrel_store, Name}),
    persistent_term:erase({barrel_source, Name}),
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

%% @doc Check database size and trigger compaction if threshold exceeded
%% Called periodically by the compaction_check timer.
do_check_compaction(StoreRef, Threshold, State) ->
    #state{name = Name} = State,
    case barrel_store_rocksdb:get_db_size(StoreRef) of
        {ok, Size} when Size >= Threshold ->
            logger:info("Database ~s size ~p bytes exceeds threshold ~p, triggering compaction",
                        [Name, Size, Threshold]),
            case barrel_store_rocksdb:compact_default_cf(StoreRef) of
                ok ->
                    logger:info("Database ~s compaction completed", [Name]),
                    State;
                {error, Reason} ->
                    logger:warning("Database ~s compaction failed: ~p", [Name, Reason]),
                    State
            end;
        {ok, Size} ->
            logger:debug("Database ~s size ~p bytes below threshold ~p, skipping compaction",
                         [Name, Size, Threshold]),
            State;
        {error, Reason} ->
            logger:warning("Failed to get database ~s size: ~p", [Name, Reason]),
            State
    end.

%% @private Release the store handle and registrations leaked by a
%% crashed instance. A live owner keeps everything (the open below then
%% fails on the RocksDB lock, which is the correct answer for a
%% double open).
release_stale_refs(Name) ->
    Owner = persistent_term:get({barrel_db, Name}, undefined),
    OwnerAlive = is_pid(Owner) andalso is_process_alive(Owner),
    case persistent_term:get({barrel_store, Name}, undefined) of
        undefined ->
            ok;
        _StoreRef when OwnerAlive ->
            ok;
        StoreRef ->
            logger:warning(
                "Database ~s: releasing store handle leaked by a crashed "
                "instance", [Name]),
            _ = try barrel_store_rocksdb:close(StoreRef)
                catch _:_ -> ok
                end,
            persistent_term:erase({barrel_store, Name}),
            persistent_term:erase({barrel_db, Name}),
            persistent_term:erase({barrel_source, Name}),
            ok
    end.

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
%% All deletes and the floor advance commit in one batch; re-running a
%% sweep is a no-op (deletes of missing keys do nothing).
do_retention_sweep(StoreRef, DbName, RetentionPeriod) ->
    CutoffMs = erlang:system_time(millisecond) - RetentionPeriod * 1000,
    Cutoff = barrel_hlc:from_wall_time(CutoffMs),
    Zero = #{history_swept => 0, superseded_removed => 0,
             docs_forgotten => 0},
    {Ops, Stats} = barrel_history:fold(
        StoreRef, DbName,
        fun(Entry, {OpsAcc, StatsAcc}) ->
            {EntryOps, StatsAcc2} =
                sweep_entry_ops(StoreRef, DbName, Entry, StatsAcc),
            {ok, {EntryOps ++ OpsAcc, StatsAcc2}}
        end,
        {[], Zero},
        #{to => Cutoff}),
    FloorOp = {put, barrel_store_keys:db_meta(DbName, <<"history_floor">>),
               barrel_hlc:encode(Cutoff)},
    ok = barrel_store_rocksdb:write_batch(StoreRef, [FloorOp | Ops], #{}),
    {ok, Stats#{floor => Cutoff}}.

%% @private Ops for one expired history entry.
sweep_entry_ops(StoreRef, DbName,
                #{hlc := Hlc, id := DocId, version := Token}, Stats) ->
    HistDel = {delete, barrel_store_keys:history_key(DbName, Hlc)},
    VersionEnc = barrel_version:encode(barrel_version:from_token(Token)),
    EntityKey = barrel_store_keys:doc_entity(DbName, DocId),
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
                    %% Expired tombstone: the database forgets the doc
                    {forget_doc_ops(StoreRef, DbName, DocId, Columns)
                         ++ [HistDel],
                     inc_sweep(docs_forgotten, Stats)};
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
    ChainKey = barrel_store_keys:doc_version(DbName, DocId, VersionEnc),
    case barrel_store_rocksdb:get(StoreRef, ChainKey) of
        {ok, <<1:8, _/binary>>} ->
            %% Live conflict sibling
            {[HistDel], inc_sweep(history_swept, Stats)};
        {ok, _Superseded} ->
            BodyKey = barrel_store_keys:doc_body_rev(DbName, DocId, VersionEnc),
            {[HistDel, {delete, ChainKey}, {body_delete, BodyKey}],
             inc_sweep(superseded_removed, Stats)};
        not_found ->
            {[HistDel], inc_sweep(history_swept, Stats)}
    end.

%% @private Full forget of an expired tombstone: entity, current body,
%% every chain row and archived body, and the live-feed row. The path
%% index rows were already removed when the doc was deleted.
forget_doc_ops(StoreRef, DbName, DocId, Columns) ->
    ChangeHlc = barrel_hlc:decode(proplists:get_value(?COL_HLC, Columns)),
    Start = barrel_store_keys:doc_version_prefix(DbName, DocId),
    End = barrel_store_keys:doc_version_end(DbName, DocId),
    ChainOps = barrel_store_rocksdb:fold_range(
        StoreRef, Start, End,
        fun(Key, _Value, Acc) ->
            VEnc = barrel_store_keys:decode_doc_version_key(DbName, DocId, Key),
            {ok, [{delete, Key},
                  {body_delete,
                   barrel_store_keys:doc_body_rev(DbName, DocId, VEnc)}
                  | Acc]}
        end,
        []),
    [{entity_delete, barrel_store_keys:doc_entity(DbName, DocId)},
     {body_delete, barrel_store_keys:doc_body(DbName, DocId)},
     {delete, barrel_store_keys:doc_hlc(DbName, ChangeHlc)}
     | ChainOps].

inc_sweep(Key, Stats) ->
    maps:update_with(Key, fun(N) -> N + 1 end, Stats).

%%====================================================================
%% Document Operations
%%====================================================================

%% Wide column names for the document entity are shared with the caller-side
%% reader; they live in barrel_docdb.hrl (?COL_VERSION, ?COL_VV, ...).

%% @doc Put a document (create or update)
%% Accepts: Erlang map, indexed CBOR binary, or plain CBOR binary
%% Options:
%%   - sync: boolean() - if true, sync to disk before returning (default: false)
do_put_doc(StoreRef, DbName, Doc, Opts) ->
    %% Normalize input: map, indexed binary, or plain CBOR -> map for processing
    DocMap = barrel_doc:to_map(Doc),
    DocRecord = barrel_doc:make_doc_record(DocMap),
    Old = read_current(StoreRef, DbName, maps:get(id, DocRecord)),
    case cas_check(Old, maps:get(expected_version, DocRecord)) of
        ok ->
            {AllOps, {DocId, NewToken, NextHlc, Deleted, DocBody}} =
                build_write_ops(StoreRef, DbName, DocRecord, Old, Opts),
            Sync = maps:get(sync, Opts, false),
            ok = barrel_store_rocksdb:write_batch(StoreRef, AllOps, #{sync => Sync}),
            notify_subscribers(DbName, DocId, NewToken, NextHlc, Deleted, DocBody),
            %% return_hlc => true adds the write's change HLC (internal atom
            %% key), used by callers that ack outbox entries.
            Result = #{
                <<"id">> => DocId,
                <<"ok">> => true,
                <<"rev">> => NewToken
            },
            case maps:get(return_hlc, Opts, false) of
                true -> {ok, Result#{hlc => NextHlc}};
                false -> {ok, Result}
            end;
        {error, conflict} ->
            {error, conflict}
    end.

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

%% @doc Read a document's current version state (undefined when absent).
read_current(StoreRef, DbName, DocId) ->
    DocEntityKey = barrel_store_keys:doc_entity(DbName, DocId),
    case barrel_store_rocksdb:get_entity(StoreRef, DocEntityKey) of
        {ok, Columns} ->
            VV = case proplists:get_value(?COL_VV, Columns, <<>>) of
                <<>> -> barrel_vv:new();
                VVBin -> barrel_vv:decode(VVBin)
            end,
            {Body, BodyCbor} = case barrel_store_rocksdb:body_get(StoreRef,
                            barrel_store_keys:doc_body(DbName, DocId)) of
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
                body => Body,
                body_cbor => BodyCbor
            };
        not_found ->
            undefined
    end.

%% @doc This database's stable source id (the version author).
%% Per-database, not per-node: replicas on the same node must detect
%% each other's divergent writes as concurrent.
source_id(DbName) ->
    persistent_term:get({barrel_source, DbName}).

%% @private Load or create the source id (persisted in db meta).
ensure_source_id(StoreRef, DbName) ->
    Key = barrel_store_keys:db_meta(DbName, <<"source_id">>),
    case barrel_store_rocksdb:get(StoreRef, Key) of
        {ok, SourceId} ->
            SourceId;
        not_found ->
            SourceId = binary:encode_hex(crypto:strong_rand_bytes(8), lowercase),
            ok = barrel_store_rocksdb:put(StoreRef, Key, SourceId),
            SourceId
    end.

%% @doc CAS check for local writes: the caller must supply the current
%% winner's version token when updating a live document. Creating a new
%% document, or recreating over a tombstone, needs no token.
cas_check(undefined, _Expected) ->
    ok;
cas_check(#{deleted := true}, undefined) ->
    ok;
cas_check(_Old, undefined) ->
    {error, conflict};
cas_check(#{version := Current}, ExpectedToken) ->
    case barrel_version:to_token(Current) =:= ExpectedToken of
        true -> ok;
        false -> {error, conflict}
    end.

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
build_write_ops(StoreRef, DbName, DocRecord, Old, Opts) ->
    #{id := DocId, deleted := Deleted, doc := DocBody} = DocRecord,
    DocEntityKey = barrel_store_keys:doc_entity(DbName, DocId),

    NextHlc = barrel_hlc:new_hlc(),
    NewVersion = barrel_version:new(NextHlc, source_id(DbName)),
    NewToken = barrel_version:to_token(NewVersion),

    {OldVersion, OldHlc, OldVV, OldDeleted, OldDocBody, OldDocBodyCbor,
     NConflicts, CreatedAt, ExpiresAt, Tier} =
        case Old of
            undefined ->
                {undefined, undefined, barrel_vv:new(), false, undefined,
                 undefined, 0, barrel_hlc:encode(NextHlc), 0, 0};
            #{version := OV, hlc := OH, vv := OVV, deleted := OD,
              body := OB, body_cbor := OBC, num_conflicts := ONC,
              created_at := OCA, expires_at := OEA, tier := OT} ->
                {OV, OH, OVV, OD, OB, OBC, ONC, OCA, OEA, OT}
        end,

    NewVV = barrel_vv:bump(OldVV, NewVersion),

    DocColumns = [
        {?COL_VERSION, barrel_version:encode(NewVersion)},
        {?COL_DELETED, deleted_to_bin(Deleted)},
        {?COL_HLC, barrel_hlc:encode(NextHlc)},
        {?COL_VV, barrel_vv:encode(NewVV)},
        {?COL_NCONFLICTS, NConflicts},
        {?COL_CREATED_AT, CreatedAt},
        {?COL_EXPIRES_AT, ExpiresAt},
        {?COL_TIER, Tier}
    ] ++ embedding_columns(DocRecord),
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
        _ -> [{delete, barrel_store_keys:doc_hlc(DbName, OldHlc)}]
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
                OldCbor ->
                    [{body_put,
                      barrel_store_keys:doc_body_rev(DbName, DocId, OldVersionEnc),
                      OldCbor}]
            end,
            Chain = [{put,
                      barrel_store_keys:doc_version(DbName, DocId, OldVersionEnc),
                      sibling_entry(superseded, OldDeleted, OldVV)}],
            {Archive, Chain}
    end,

    %% Tagged outbox entries (generic durable work queue; see barrel_outbox)
    OutboxOps = barrel_outbox:write_ops(DbName, maps:get(outbox, Opts, []),
                                        NextHlc, OldHlc, DocId, NewToken, Deleted),

    %% Retained history entry (resolutions pass cause `resolve')
    HistoryOps = barrel_history:write_ops(
        DbName, NextHlc, DocId, NewVersion, Deleted,
        maps:get(history_cause, Opts, local), NewVV),

    %% Write new body to current location (no version in key)
    BodyKey = barrel_store_keys:doc_body(DbName, DocId),
    BodyOp = {body_put, BodyKey, CborBody},
    AllOps = DocOps ++ HlcDeleteOps ++ PathIndexOps ++ ChangeOps ++ PathHlcOps
        ++ ArchiveOps ++ ChainOps ++ OutboxOps ++ HistoryOps ++ [BodyOp],
    {AllOps, {DocId, NewToken, NextHlc, Deleted, DocBody}}.

%% @doc Put multiple documents in a single batch (batch write)
%% Options:
%%   - sync: boolean() - if true, sync to disk before returning (default: false)
do_put_docs(StoreRef, DbName, Docs, Opts) ->
    Sync = maps:get(sync, Opts, false),
    %% Process each document to build operations and metadata
    {AllOps, Notifications} = lists:foldl(
        fun(Doc, {OpsAcc, NotifyAcc}) ->
            case prepare_doc_ops(StoreRef, DbName, Doc, Opts) of
                {ok, Ops, NotifyInfo} ->
                    {OpsAcc ++ Ops, [NotifyInfo | NotifyAcc]};
                {error, _Reason} = Err ->
                    %% Skip failed docs, include error in notifications
                    {OpsAcc, [{error, Err} | NotifyAcc]}
            end
        end,
        {[], []},
        Docs
    ),

    %% Write all operations in a single batch (includes body CF writes)
    case AllOps of
        [] -> ok;
        _ ->
            WriteOpts = #{sync => Sync},
            ok = barrel_store_rocksdb:write_batch(StoreRef, AllOps, WriteOpts)
    end,

    %% Notify subscribers and build results (reverse to maintain order)
    ReturnHlc = maps:get(return_hlc, Opts, false),
    Results = lists:map(
        fun({error, Err}) ->
            Err;
           ({DocId, NewToken, NextHlc, Deleted, DocBody}) ->
            notify_subscribers(DbName, DocId, NewToken, NextHlc, Deleted, DocBody),
            Result = #{<<"id">> => DocId, <<"ok">> => true, <<"rev">> => NewToken},
            case ReturnHlc of
                true -> {ok, Result#{hlc => NextHlc}};
                false -> {ok, Result}
            end
        end,
        lists:reverse(Notifications)
    ),
    Results.

%% @doc Prepare document operations without writing.
%% Batch-level Opts carry the outbox tags applied to every doc in the batch.
%% Returns {ok, Ops, NotifyInfo} or {error, Reason}
prepare_doc_ops(StoreRef, DbName, Doc, Opts) ->
    try
        DocMap = barrel_doc:to_map(Doc),
        DocRecord = barrel_doc:make_doc_record(DocMap),
        Old = read_current(StoreRef, DbName, maps:get(id, DocRecord)),
        case cas_check(Old, maps:get(expected_version, DocRecord)) of
            ok -> ok;
            {error, conflict} -> throw(conflict)
        end,
        {AllOps, NotifyInfo} =
            build_write_ops(StoreRef, DbName, DocRecord, Old, Opts),
        {ok, AllOps, NotifyInfo}
    catch
        _:Reason ->
            {error, Reason}
    end.

%% @doc Store a computed embedding in the doc entity (CAS on revision).
%% Read-modify-write of the entity columns is safe here: this runs in
%% the database writer, which serializes all entity writes.
do_set_doc_embedding(StoreRef, DbName, DocId, ExpectedRev, Vector) ->
    DocEntityKey = barrel_store_keys:doc_entity(DbName, DocId),
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
    case read_current(StoreRef, DbName, DocId) of
        undefined ->
            {error, not_found};
        #{version := OldVersion, hlc := OldHlc, vv := OldVV,
          deleted := OldDeleted, num_conflicts := NConflicts,
          created_at := CreatedAt, expires_at := ExpiresAt, tier := Tier,
          body := OldDocBody, body_cbor := OldDocBodyCbor} ->
            CurrentToken = barrel_version:to_token(OldVersion),
            case maps:get(rev, Opts, undefined) of
                undefined -> ok;
                CurrentToken -> ok;
                _Other -> throw({error, {conflict, CurrentToken}})
            end,

            NextHlc = barrel_hlc:new_hlc(),
            NewVersion = barrel_version:new(NextHlc, source_id(DbName)),
            NewToken = barrel_version:to_token(NewVersion),
            NewVV = barrel_vv:bump(OldVV, NewVersion),
            DocEntityKey = barrel_store_keys:doc_entity(DbName, DocId),

            DocColumns = [
                {?COL_VERSION, barrel_version:encode(NewVersion)},
                {?COL_DELETED, <<"true">>},
                {?COL_HLC, barrel_hlc:encode(NextHlc)},
                {?COL_VV, barrel_vv:encode(NewVV)},
                {?COL_NCONFLICTS, NConflicts},
                {?COL_CREATED_AT, CreatedAt},
                {?COL_EXPIRES_AT, ExpiresAt},
                {?COL_TIER, Tier}
            ],
            DocOps = [{entity_put, DocEntityKey, DocColumns}],

            %% Supersede the live-feed row of the previous write
            HlcDeleteOps = [{delete, barrel_store_keys:doc_hlc(DbName, OldHlc)}],

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
                OldCbor ->
                    [{body_put,
                      barrel_store_keys:doc_body_rev(DbName, DocId, OldVersionEnc),
                      OldCbor}]
            end,
            ChainOps = [{put,
                         barrel_store_keys:doc_version(DbName, DocId, OldVersionEnc),
                         sibling_entry(superseded, OldDeleted, OldVV)}],

            %% Tagged outbox entries (deletes replace the pending entry too)
            OutboxOps = barrel_outbox:write_ops(DbName, maps:get(outbox, Opts, []),
                                                NextHlc, OldHlc, DocId, NewToken, true),

            %% Retained history entry for the tombstone
            HistoryOps = barrel_history:write_ops(
                DbName, NextHlc, DocId, NewVersion, true, local, NewVV),

            %% Write batch atomically
            AllOps = DocOps ++ HlcDeleteOps ++ PathIndexOps ++ ChangeOps ++ PathHlcOps
                ++ ArchiveOps ++ ChainOps ++ OutboxOps ++ HistoryOps,
            WriteOpts = #{sync => maps:get(sync, Opts, false)},
            ok = barrel_store_rocksdb:write_batch(StoreRef, AllOps, WriteOpts),

            %% Notify path subscribers
            notify_subscribers(DbName, DocId, NewToken, NextHlc, true, #{}),

            DeleteResult = #{<<"id">> => DocId, <<"ok">> => true,
                             <<"rev">> => NewToken},
            case maps:get(return_hlc, Opts, false) of
                true -> {ok, DeleteResult#{hlc => NextHlc}};
                false -> {ok, DeleteResult}
            end
    end.

%% @doc Fold over all documents (using wide column entity)
%% Options:
%%   - include_deleted: boolean() - include deleted documents (default: false)
%% Note: Uses regular key iteration since wide columns are stored per-key
do_fold_docs(StoreRef, DbName, Fun, Acc, Opts) ->
    StartKey = barrel_store_keys:doc_entity_prefix(DbName),
    EndKey = barrel_store_keys:doc_entity_end(DbName),
    PrefixLen = byte_size(StartKey),
    IncludeDeleted = maps:get(include_deleted, Opts, false),

    FoldFun = fun(Key, _Value, AccIn) ->
        %% Extract DocId from key (after prefix)
        DocId = binary:part(Key, PrefixLen, byte_size(Key) - PrefixLen),
        %% Get entity to access columns
        case barrel_store_rocksdb:get_entity(StoreRef, Key) of
            {ok, Columns} ->
                Rev = barrel_version:to_token(
                    barrel_version:decode(
                        proplists:get_value(?COL_VERSION, Columns))),
                Deleted = bin_to_deleted(proplists:get_value(?COL_DELETED, Columns, <<"false">>)),
                case {Deleted, IncludeDeleted} of
                    {true, false} ->
                        %% Skip deleted documents
                        {ok, AccIn};
                    _ ->
                        %% Get document body from body CF (current body, no rev in key)
                        BodyKey = barrel_store_keys:doc_body(DbName, DocId),
                        DocBody = case barrel_store_rocksdb:body_get(StoreRef, BodyKey) of
                            {ok, CborBin} -> barrel_docdb_codec_cbor:decode_any(CborBin);
                            not_found -> #{}
                        end,
                        Doc = DocBody#{
                            <<"id">> => DocId,
                            <<"_rev">> => Rev
                        },
                        case Fun(Doc, AccIn) of
                            {ok, AccOut} -> {ok, AccOut};
                            {stop, AccOut} -> {stop, AccOut};
                            stop -> {stop, AccIn}
                        end
                end;
            not_found ->
                {ok, AccIn}
        end
    end,

    FinalAcc = barrel_store_rocksdb:fold_range(StoreRef, StartKey, EndKey, FoldFun, Acc),
    {ok, FinalAcc}.

%%====================================================================
%% Replication Operations
%%====================================================================

%% @doc Apply a replicated version (the VV protocol write).
%% The remote version and its vector are preserved; only the change
%% sequence HLC is issued locally. Outcomes:
%% - already contained: idempotent no-op
%% - remote VV dominates: fast-forward, old winner superseded
%% - concurrent: the per-db conflict_merger may resolve it with a
%%   merged body; otherwise LWW by version, the loser stays live as a
%%   conflict sibling (bounded by max_conflict_versions) and the
%%   vectors merge
do_put_version(StoreRef, DbName, Config, Doc, Version, RemoteVV, Deleted) ->
    %% Advance the local clock past the remote write so subsequent local
    %% change HLCs stay strictly increasing
    _ = barrel_hlc:sync_hlc(barrel_version:hlc(Version)),
    DocMap = barrel_doc:to_map(Doc),
    DocId = case maps:find(<<"id">>, DocMap) of
        {ok, Id} -> Id;
        error -> erlang:error({missing_id, DocMap})
    end,
    DocBody = barrel_doc:doc_without_meta(DocMap),
    case read_current(StoreRef, DbName, DocId) of
        undefined ->
            apply_remote_current(StoreRef, DbName, DocId, DocBody, Version,
                                 barrel_vv:bump(RemoteVV, Version), Deleted,
                                 undefined, 0, []);
        #{version := LocalV, vv := LocalVV} = Old ->
            case barrel_vv:contains(LocalVV, Version) of
                true ->
                    %% Idempotent redelivery: we already cover it
                    {ok, DocId, barrel_version:to_token(LocalV)};
                false ->
                    MergedVV = barrel_vv:merge(LocalVV,
                                               barrel_vv:bump(RemoteVV, Version)),
                    case barrel_vv:compare(RemoteVV, LocalVV) of
                        dominates ->
                            %% Fast-forward: old winner superseded
                            Chain = [chain_op(DbName, DocId, LocalV,
                                              superseded, Old)],
                            apply_remote_current(StoreRef, DbName, DocId,
                                                 DocBody, Version, MergedVV,
                                                 Deleted, Old,
                                                 maps:get(num_conflicts, Old),
                                                 Chain);
                        _Concurrent ->
                            handle_concurrent(StoreRef, DbName, Config, DocId,
                                              DocBody, Version, RemoteVV,
                                              Deleted, Old, MergedVV)
                    end
            end
    end.

%% @private A concurrent remote version: the merger hook gets the first
%% say; without one (or when it declines, crashes, or a side is a
%% tombstone) the deterministic LWW conflict path applies.
handle_concurrent(StoreRef, DbName, Config, DocId, DocBody, Version, RemoteVV,
                  Deleted, #{version := LocalV} = Old, MergedVV) ->
    case try_merge(Config, DocId, Old, DocBody, Deleted) of
        {merge, MergedBody} ->
            apply_merged(StoreRef, DbName, DocId, MergedBody, DocBody,
                         Version, RemoteVV, Deleted, Old, MergedVV);
        conflict ->
            NConflicts0 = maps:get(num_conflicts, Old) + 1,
            Max = maps:get(max_conflict_versions, Config,
                           ?DEFAULT_MAX_CONFLICT_VERSIONS),
            case barrel_version:max(LocalV, Version) of
                Version ->
                    %% Remote wins; local winner stays live as a
                    %% conflict sibling
                    {DropOps, NConflicts} = enforce_conflict_bound(
                        StoreRef, DbName, DocId, LocalV, NConflicts0, Max),
                    Chain = [chain_op(DbName, DocId, LocalV, conflict, Old)],
                    apply_remote_current(StoreRef, DbName, DocId, DocBody,
                                         Version, MergedVV, Deleted, Old,
                                         NConflicts, Chain ++ DropOps);
                _LocalWins ->
                    {DropOps, NConflicts} = enforce_conflict_bound(
                        StoreRef, DbName, DocId, Version, NConflicts0, Max),
                    keep_local_add_conflict(StoreRef, DbName, DocId, DocBody,
                                            Version, RemoteVV, Deleted, Old,
                                            MergedVV, NConflicts, DropOps)
            end
    end.

%% @private Ask the configured merger to resolve a concurrent write.
%% Only meaningful when both sides are live documents. Any non-merge
%% answer or a crash falls back to the conflict path.
try_merge(Config, DocId, #{deleted := LocalDeleted, body := LocalBody},
          RemoteBody, RemoteDeleted) ->
    case maps:get(conflict_merger, Config, undefined) of
        undefined ->
            conflict;
        _ when LocalDeleted; RemoteDeleted ->
            conflict;
        Merger ->
            try call_merger(Merger, DocId, LocalBody, RemoteBody) of
                {merge, Merged} when is_map(Merged) -> {merge, Merged};
                _ -> conflict
            catch
                Class:Reason ->
                    logger:warning(
                        "conflict_merger failed for ~p: ~p:~p, "
                        "falling back to conflict", [DocId, Class, Reason]),
                    conflict
            end
    end.

call_merger({M, F}, DocId, LocalBody, RemoteBody) ->
    M:F(DocId, LocalBody, RemoteBody);
call_merger(Fun, DocId, LocalBody, RemoteBody) when is_function(Fun, 3) ->
    Fun(DocId, LocalBody, RemoteBody).

%% @private Apply a merger resolution in one batch: the remote version
%% is archived as superseded (with its own replicated history entry),
%% and a new local resolving write whose vector covers both sides
%% becomes current. The resolving write replicates like any other.
apply_merged(StoreRef, DbName, DocId, MergedBody, RemoteBody, Version,
             RemoteVV, RemoteDeleted, Old, MergedVV) ->
    %% The remote arrival: superseded chain row, archived body, history
    RemoteChangeHlc = barrel_hlc:new_hlc(),
    VersionEnc = barrel_version:encode(Version),
    SiblingVV = barrel_vv:bump(RemoteVV, Version),
    RemoteOps = [
        {put, barrel_store_keys:doc_version(DbName, DocId, VersionEnc),
         sibling_entry(superseded, RemoteDeleted, SiblingVV)},
        {body_put, barrel_store_keys:doc_body_rev(DbName, DocId, VersionEnc),
         barrel_docdb_codec_cbor:encode_cbor(RemoteBody)}
    ] ++ barrel_history:write_ops(DbName, RemoteChangeHlc, DocId, Version,
                                  RemoteDeleted, replicated, SiblingVV),

    %% The resolving write goes through the ordinary local pipeline
    %% with the merged vector as its base (its bump then covers both
    %% sides); the old local winner is archived by build_write_ops
    DocRecord0 = barrel_doc:make_doc_record(MergedBody#{<<"id">> => DocId}),
    DocRecord = DocRecord0#{expected_version => undefined},
    Old2 = Old#{vv := MergedVV},
    {WriteOps, {DocId, NewToken, NextHlc, Deleted, NotifyBody}} =
        build_write_ops(StoreRef, DbName, DocRecord, Old2,
                        #{history_cause => resolve}),
    ok = barrel_store_rocksdb:write_batch(StoreRef, RemoteOps ++ WriteOps, #{}),
    notify_subscribers(DbName, DocId, NewToken, NextHlc, Deleted, NotifyBody),
    {ok, DocId, NewToken}.

%% @private Deterministically bound the live conflict siblings: when a
%% new sibling would push the count past Max, the lowest versions (the
%% new one included) are dropped with their archived bodies. Every
%% replica seeing the same sibling set drops the same versions.
enforce_conflict_bound(_StoreRef, _DbName, _DocId, _NewSibling, NConflicts,
                       Max) when NConflicts =< Max ->
    {[], NConflicts};
enforce_conflict_bound(StoreRef, DbName, DocId, NewSibling, NConflicts, Max) ->
    Existing = [V || {V, _Entry} <- conflict_siblings(StoreRef, DbName, DocId)],
    Sorted = lists:sort(
        fun(A, B) -> barrel_version:compare(A, B) =:= lt end,
        [NewSibling | Existing]),
    Victims = lists:sublist(Sorted, NConflicts - Max),
    Ops = lists:flatmap(
        fun(V) ->
            VEnc = barrel_version:encode(V),
            [{delete, barrel_store_keys:doc_version(DbName, DocId, VEnc)},
             {body_delete,
              barrel_store_keys:doc_body_rev(DbName, DocId, VEnc)}]
        end,
        Victims),
    {Ops, Max}.

%% @private Chain-row op for a version that is no longer current.
chain_op(DbName, DocId, Version, Flag, Old) ->
    {put,
     barrel_store_keys:doc_version(DbName, DocId, barrel_version:encode(Version)),
     sibling_entry(Flag, maps:get(deleted, Old), maps:get(vv, Old))}.

%% @private The remote version becomes the current winner.
apply_remote_current(StoreRef, DbName, DocId, DocBody, Version, VV, Deleted,
                     Old, NConflicts, ChainOps) ->
    ChangeHlc = barrel_hlc:new_hlc(),
    Token = barrel_version:to_token(Version),
    {OldHlc, OldDocBody, OldDocBodyCbor, CreatedAt, ExpiresAt, Tier} =
        case Old of
            undefined ->
                {undefined, undefined, undefined,
                 barrel_hlc:encode(ChangeHlc), 0, 0};
            #{hlc := OH, body := OB, body_cbor := OBC,
              created_at := OCA, expires_at := OEA, tier := OT} ->
                {OH, OB, OBC, OCA, OEA, OT}
        end,
    DocColumns = [
        {?COL_VERSION, barrel_version:encode(Version)},
        {?COL_DELETED, deleted_to_bin(Deleted)},
        {?COL_HLC, barrel_hlc:encode(ChangeHlc)},
        {?COL_VV, barrel_vv:encode(VV)},
        {?COL_NCONFLICTS, NConflicts},
        {?COL_CREATED_AT, CreatedAt},
        {?COL_EXPIRES_AT, ExpiresAt},
        {?COL_TIER, Tier}
    ],
    DocOps = [{entity_put, barrel_store_keys:doc_entity(DbName, DocId),
               DocColumns}],
    DocInfo = #{id => DocId, rev => Token, deleted => Deleted,
                num_conflicts => NConflicts, hlc => ChangeHlc},
    HlcDeleteOps = case OldHlc of
        undefined -> [];
        _ -> [{delete, barrel_store_keys:doc_hlc(DbName, OldHlc)}]
    end,
    PathIndexOps = case Deleted of
        true when OldDocBody =/= undefined ->
            case barrel_ars_index:get_doc_paths(StoreRef, DbName, DocId) of
                {ok, OldPaths} ->
                    barrel_ars_index:remove_doc_ops(DbName, DocId, OldPaths);
                not_found -> []
            end;
        true ->
            [];
        false when OldDocBody =:= undefined ->
            barrel_ars_index:index_doc_ops(DbName, DocId, DocBody);
        false ->
            case maps:get(deleted, Old) of
                true ->
                    barrel_ars_index:index_doc_ops(DbName, DocId, DocBody);
                false ->
                    barrel_ars_index:update_doc_ops(DbName, DocId,
                                                    OldDocBody, DocBody)
            end
    end,
    ChangeInfo = DocInfo#{doc => DocBody},
    ChangeOps = barrel_changes:write_change_ops(DbName, ChangeHlc, ChangeInfo),
    PathHlcOps = case OldHlc of
        undefined ->
            barrel_changes:write_path_index_ops(DbName, ChangeHlc, ChangeInfo);
        _ ->
            barrel_changes:update_path_index_ops(DbName, ChangeHlc, ChangeInfo,
                                                 OldHlc, OldDocBody)
    end,
    ArchiveOps = case {Old, OldDocBodyCbor} of
        {undefined, _} -> [];
        {_, undefined} -> [];
        {#{version := OldVersion}, OldCbor} ->
            [{body_put,
              barrel_store_keys:doc_body_rev(
                  DbName, DocId, barrel_version:encode(OldVersion)),
              OldCbor}]
    end,
    HistoryOps = barrel_history:write_ops(
        DbName, ChangeHlc, DocId, Version, Deleted, replicated, VV),
    CborBody = barrel_docdb_codec_cbor:encode_cbor(DocBody),
    BodyOp = {body_put, barrel_store_keys:doc_body(DbName, DocId), CborBody},
    AllOps = DocOps ++ HlcDeleteOps ++ PathIndexOps ++ ChangeOps ++ PathHlcOps
        ++ ArchiveOps ++ ChainOps ++ HistoryOps ++ [BodyOp],
    ok = barrel_store_rocksdb:write_batch(StoreRef, AllOps, #{}),
    notify_subscribers(DbName, DocId, Token, ChangeHlc, Deleted, DocBody),
    {ok, DocId, Token}.

%% @private The local winner stays; the remote version is recorded as a
%% live conflict sibling (its body archived, queryable). The doc's state
%% changed (vector, conflict count), so a change event still fires.
%% ExtraOps carry sibling-bound drops, applied after the sibling puts.
keep_local_add_conflict(StoreRef, DbName, DocId, RemoteBody, Version, RemoteVV,
                        RemoteDeleted, Old, MergedVV, NConflicts, ExtraOps) ->
    #{version := LocalV, hlc := OldHlc, deleted := LocalDeleted,
      body := LocalBody, created_at := CreatedAt, expires_at := ExpiresAt,
      tier := Tier} = Old,
    ChangeHlc = barrel_hlc:new_hlc(),
    LocalToken = barrel_version:to_token(LocalV),
    DocColumns = [
        {?COL_VERSION, barrel_version:encode(LocalV)},
        {?COL_DELETED, deleted_to_bin(LocalDeleted)},
        {?COL_HLC, barrel_hlc:encode(ChangeHlc)},
        {?COL_VV, barrel_vv:encode(MergedVV)},
        {?COL_NCONFLICTS, NConflicts},
        {?COL_CREATED_AT, CreatedAt},
        {?COL_EXPIRES_AT, ExpiresAt},
        {?COL_TIER, Tier}
    ],
    DocOps = [{entity_put, barrel_store_keys:doc_entity(DbName, DocId),
               DocColumns}],
    DocInfo = #{id => DocId, rev => LocalToken, deleted => LocalDeleted,
                num_conflicts => NConflicts, hlc => ChangeHlc},
    HlcDeleteOps = [{delete, barrel_store_keys:doc_hlc(DbName, OldHlc)}],
    ChangeInfo = DocInfo#{doc => LocalBody},
    ChangeOps = barrel_changes:write_change_ops(DbName, ChangeHlc, ChangeInfo),
    PathHlcOps = barrel_changes:update_path_index_ops(DbName, ChangeHlc,
                                                      ChangeInfo, OldHlc,
                                                      LocalBody),
    %% The remote loser: conflict chain row + archived body
    VersionEnc = barrel_version:encode(Version),
    SiblingVV = barrel_vv:bump(RemoteVV, Version),
    SiblingEntry = <<1:8, %% conflict
                     (case RemoteDeleted of true -> 1; false -> 0 end):8,
                     (barrel_vv:encode(SiblingVV))/binary>>,
    ChainOps = [{put, barrel_store_keys:doc_version(DbName, DocId, VersionEnc),
                 SiblingEntry}],
    ArchiveOps = [{body_put,
                   barrel_store_keys:doc_body_rev(DbName, DocId, VersionEnc),
                   barrel_docdb_codec_cbor:encode_cbor(RemoteBody)}],
    %% History records the arrival of the losing sibling
    HistoryOps = barrel_history:write_ops(
        DbName, ChangeHlc, DocId, Version, RemoteDeleted, replicated,
        SiblingVV),
    AllOps = DocOps ++ HlcDeleteOps ++ ChangeOps ++ PathHlcOps
        ++ ChainOps ++ ArchiveOps ++ HistoryOps ++ ExtraOps,
    ok = barrel_store_rocksdb:write_batch(StoreRef, AllOps, #{}),
    notify_subscribers(DbName, DocId, LocalToken, ChangeHlc, LocalDeleted,
                       LocalBody),
    {ok, DocId, LocalToken}.

%% @doc The replication diff: which offered versions does this database
%% not cover yet? `have' means the doc's version vector contains the
%% offered version.
do_diff_versions(StoreRef, DbName, TokenMap) ->
    Result = maps:map(
        fun(DocId, Token) ->
            Version = barrel_version:from_token(Token),
            case read_current(StoreRef, DbName, DocId) of
                undefined ->
                    missing;
                #{vv := LocalVV} ->
                    case barrel_vv:contains(LocalVV, Version) of
                        true -> have;
                        false -> missing
                    end
            end
        end,
        TokenMap),
    {ok, Result}.

%% @doc Put a local document (per-database)
do_put_local_doc(StoreRef, DbName, DocId, Doc) ->
    Key = barrel_store_keys:local_doc_key(DbName, DocId),
    Value = term_to_binary(Doc),
    ok = barrel_store_rocksdb:local_put(StoreRef, Key, Value),
    ok.

%% @doc Get a local document (per-database)
do_get_local_doc(StoreRef, DbName, DocId) ->
    Key = barrel_store_keys:local_doc_key(DbName, DocId),
    case barrel_store_rocksdb:local_get(StoreRef, Key) of
        {ok, Value} ->
            {ok, binary_to_term(Value)};
        not_found ->
            {error, not_found}
    end.

%% @doc Delete a local document (per-database)
do_delete_local_doc(StoreRef, DbName, DocId) ->
    Key = barrel_store_keys:local_doc_key(DbName, DocId),
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
    %% Build full key prefix: DbName + 0 + DocIdPrefix
    KeyPrefix = barrel_store_keys:local_doc_key(DbName, DocIdPrefix),
    %% Fold over matching keys, decode values, extract DocId
    DbNameLen = byte_size(DbName),
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

%% @doc Notify subscribers of document changes
%% Extracts paths from document, matches against subscriptions,
%% and sends notifications to matching subscribers.
notify_subscribers(DbName, DocId, Rev, Hlc, Deleted, DocBody) ->
    %% Extract paths from document body
    Topics = case Deleted of
        true ->
            %% For deleted docs, just use the doc ID as a path
            [DocId];
        false ->
            Paths = barrel_ars:analyze(DocBody),
            barrel_ars:paths_to_topics(Paths)
    end,

    %% Find matching path subscribers
    Pids = barrel_sub:match(DbName, Topics),

    %% Build notification
    Notification = {barrel_change, DbName, #{
        id => DocId,
        rev => Rev,
        hlc => Hlc,
        deleted => Deleted,
        paths => Topics
    }},

    %% Send to each path subscriber
    _ = [Pid ! Notification || Pid <- Pids],

    %% Notify query subscribers (only for non-deleted docs)
    case Deleted of
        true ->
            ok;
        false ->
            barrel_query_sub:notify_change(DbName, DocId, Rev, DocBody)
    end,
    ok.

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
    Start = barrel_store_keys:doc_version_prefix(DbName, DocId),
    End = barrel_store_keys:doc_version_end(DbName, DocId),
    Fold = fun(Key, Value, Acc) ->
        Entry = decode_sibling_entry(Value),
        case maps:get(flag, Entry) of
            conflict ->
                VersionEnc = barrel_store_keys:decode_doc_version_key(
                                 DbName, DocId, Key),
                {ok, [{barrel_version:decode(VersionEnc), Entry} | Acc]};
            superseded ->
                {ok, Acc}
        end
    end,
    lists:reverse(barrel_store_rocksdb:fold_range(StoreRef, Start, End, Fold, [])).

%% @doc Resolve a conflict with a resolving local write.
%% Resolution types:
%%   {choose, Token} - the chosen sibling's body becomes the new current
%%   {merge, Doc}    - the supplied body becomes the new current
%% Either way ONE new version is written whose version vector covers the
%% winner and every conflict sibling, and all siblings flip to
%% superseded: the resolution is an ordinary write that replicates.
do_resolve_conflict(StoreRef, DbName, DocId, BaseRev, Resolution) ->
    case read_current(StoreRef, DbName, DocId) of
        undefined ->
            {error, not_found};
        #{num_conflicts := 0} ->
            {error, no_conflicts};
        #{version := Current, vv := CurrentVV} = Old ->
            CurrentToken = barrel_version:to_token(Current),
            case BaseRev of
                CurrentToken ->
                    Siblings = conflict_siblings(StoreRef, DbName, DocId),
                    apply_resolution(StoreRef, DbName, DocId, Old, CurrentVV,
                                     Siblings, Resolution);
                _ ->
                    {error, {conflict, CurrentToken}}
            end
    end.

%% @private Apply a conflict resolution as a superseding write.
apply_resolution(StoreRef, DbName, DocId, Old, CurrentVV, Siblings, Resolution) ->
    ResolvedBody = case Resolution of
        {merge, Doc} when is_map(Doc) ->
            {ok, barrel_doc:doc_without_meta(Doc)};
        {choose, Token} ->
            CurrentToken = barrel_version:to_token(maps:get(version, Old)),
            case Token of
                CurrentToken ->
                    {ok, maps:get(body, Old, #{})};
                _ ->
                    case lists:filter(
                             fun({V, _E}) ->
                                 barrel_version:to_token(V) =:= Token
                             end, Siblings) of
                        [{ChosenVersion, _Entry}] ->
                            read_version_body(StoreRef, DbName, DocId,
                                              ChosenVersion);
                        [] ->
                            {error, {unknown_version, Token}}
                    end
            end;
        _ ->
            {error, {invalid_resolution, Resolution}}
    end,
    case ResolvedBody of
        {ok, Body} ->
            %% Version vector of the resolving write covers everything
            MergedVV = lists:foldl(
                fun({_V, #{vv := SibVV}}, Acc) -> barrel_vv:merge(Acc, SibVV) end,
                CurrentVV,
                Siblings),
            DocRecord0 = barrel_doc:make_doc_record(Body#{<<"id">> => DocId}),
            DocRecord = DocRecord0#{expected_version => undefined},
            %% Reuse the ordinary write pipeline with the merged VV and
            %% conflict flips added on top
            Old2 = Old#{vv := MergedVV, num_conflicts := 0},
            {AllOps0, {DocId, NewToken, NextHlc, Deleted, DocBody}} =
                build_write_ops(StoreRef, DbName, DocRecord, Old2,
                                #{history_cause => resolve}),
            %% Flip every conflict sibling to superseded
            FlipOps = [{put,
                        barrel_store_keys:doc_version(
                            DbName, DocId, barrel_version:encode(V)),
                        sibling_entry(superseded, maps:get(deleted, E),
                                      maps:get(vv, E))}
                       || {V, E} <- Siblings],
            ok = barrel_store_rocksdb:write_batch(
                     StoreRef, AllOps0 ++ FlipOps, #{}),
            notify_subscribers(DbName, DocId, NewToken, NextHlc, Deleted, DocBody),
            {ok, #{id => DocId, rev => NewToken,
                   conflicts_resolved => length(Siblings)}};
        {error, _} = Err ->
            Err
    end.

%% @private Read the archived body of a non-current version.
read_version_body(StoreRef, DbName, DocId, Version) ->
    Key = barrel_store_keys:doc_body_rev(DbName, DocId,
                                         barrel_version:encode(Version)),
    case barrel_store_rocksdb:body_get(StoreRef, Key) of
        {ok, CborBin} -> {ok, barrel_docdb_codec_cbor:decode_any(CborBin)};
        not_found -> {error, {version_body_missing, barrel_version:to_token(Version)}};
        {error, _} = Err -> Err
    end.
