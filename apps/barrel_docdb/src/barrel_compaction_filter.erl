%%%-------------------------------------------------------------------
%%% @doc RocksDB compaction filter for revision tree pruning
%%%
%%% This module implements revision pruning during RocksDB compaction.
%%% Each database has its own compaction filter handler that:
%%%
%%% 1. Processes doc_entity keys during compaction
%%% 2. Decodes the revtree from entity columns
%%% 3. Prunes old revisions based on depth
%%% 4. Deletes body entries for pruned revisions
%%% 5. Returns updated entity with pruned revtree
%%%
%%% The filter is configured on the default column family where
%%% document entities are stored. Body deletions are issued via
%%% normal write_batch and cleaned up by RocksDB in subsequent
%%% compaction cycles.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_compaction_filter).

-behaviour(gen_server).

%% API
-export([start_link/1]).
-export([get_stats/1, reset_stats/1]).
-export([set_prune_depth/2, get_prune_depth/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(DEFAULT_PRUNE_DEPTH, 1000).

-record(state, {
    db_name :: binary(),
    prune_depth :: non_neg_integer(),
    %% Stats
    filter_calls = 0 :: non_neg_integer(),
    entities_processed = 0 :: non_neg_integer(),
    revisions_pruned = 0 :: non_neg_integer()
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start a compaction filter handler for a database
%% Options:
%%   - db_name: The database name (required)
%%   - prune_depth: Max revisions to keep per branch (default: 1000)
%%
%% Note: The db_ref is looked up from persistent_term at runtime since
%% the filter handler must be started before RocksDB is opened (to pass
%% the handler pid to the CF options).
-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

%% @doc Get filter statistics for debugging
-spec get_stats(pid()) -> map().
get_stats(Pid) ->
    gen_server:call(Pid, get_stats).

%% @doc Reset filter statistics
-spec reset_stats(pid()) -> ok.
reset_stats(Pid) ->
    gen_server:call(Pid, reset_stats).

%% @doc Set the prune depth
-spec set_prune_depth(pid(), non_neg_integer()) -> ok.
set_prune_depth(Pid, Depth) ->
    gen_server:call(Pid, {set_prune_depth, Depth}).

%% @doc Get the current prune depth
-spec get_prune_depth(pid()) -> non_neg_integer().
get_prune_depth(Pid) ->
    gen_server:call(Pid, get_prune_depth).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Opts) ->
    DbName = maps:get(db_name, Opts),
    PruneDepth = maps:get(prune_depth, Opts, ?DEFAULT_PRUNE_DEPTH),
    {ok, #state{
        db_name = DbName,
        prune_depth = PruneDepth
    }}.

handle_call(get_stats, _From, State) ->
    #state{filter_calls = Calls,
           entities_processed = Entities,
           revisions_pruned = Pruned} = State,
    Stats = #{
        filter_calls => Calls,
        entities_processed => Entities,
        revisions_pruned => Pruned
    },
    {reply, Stats, State};

handle_call(reset_stats, _From, State) ->
    {reply, ok, State#state{
        filter_calls = 0,
        entities_processed = 0,
        revisions_pruned = 0
    }};

handle_call({set_prune_depth, Depth}, _From, State) ->
    {reply, ok, State#state{prune_depth = Depth}};

handle_call(get_prune_depth, _From, #state{prune_depth = Depth} = State) ->
    {reply, Depth, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

%% @doc Handle compaction filter batch requests from RocksDB
%% Message format: {compaction_filter, BatchRef, Keys}
%% where Keys is a list of {Level, Key, Value} tuples
handle_info({compaction_filter, BatchRef, Keys}, State) ->
    {Decisions, NewState} = filter_batch(Keys, State),
    rocksdb:compaction_filter_reply(BatchRef, Decisions),
    {noreply, NewState#state{filter_calls = State#state.filter_calls + 1}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Filter a batch of keys from compaction
filter_batch(Keys, State) ->
    {Decisions, FinalState} = lists:foldl(
        fun({_Level, Key, Value}, {DecAcc, SAcc}) ->
            {Decision, NewSAcc} = filter_key(Key, Value, SAcc),
            {[Decision | DecAcc], NewSAcc}
        end,
        {[], State},
        Keys
    ),
    {lists:reverse(Decisions), FinalState}.

%% @private Filter a single key-value pair
%% Returns: {keep | remove | {change_value, NewBinary}, State}
filter_key(Key, Value, State) ->
    #state{db_name = DbName} = State,
    case barrel_store_keys:parse_key(Key) of
        {doc_entity, DbName, DocId} ->
            %% This is a document entity for our database - process it
            process_doc_entity(DocId, Value, State);
        {doc_entity, _OtherDb, _DocId} ->
            %% Different database, keep as-is
            {keep, State};
        _ ->
            %% Not a doc_entity key, keep as-is
            {keep, State}
    end.

%% @private Process a document entity, pruning old revisions
process_doc_entity(DocId, EntityBin, State) ->
    #state{db_name = DbName, prune_depth = Depth} = State,

    %% Look up db_ref from persistent_term (registered by barrel_db_server)
    case persistent_term:get({barrel_store, DbName}, undefined) of
        undefined ->
            %% Store not yet registered, skip pruning
            {keep, State};
        DbRef ->
            do_process_doc_entity(DbRef, DbName, DocId, EntityBin, Depth, State)
    end.

%% @private Rev-tree pruning is gone (version-vector storage keeps
%% per-doc chains in their own keyspace; the retention sweeper owns
%% their lifecycle). The filter is kept as a pass-through seam.
do_process_doc_entity(_DbRef, _DbName, _DocId, _EntityBin, _Depth, State) ->
    NewState = State#state{
        entities_processed = State#state.entities_processed + 1
    },
    {keep, NewState}.
