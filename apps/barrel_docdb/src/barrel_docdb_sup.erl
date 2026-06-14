%%%-------------------------------------------------------------------
%%% @doc barrel_docdb top-level supervisor
%%%
%%% Supervises all barrel_docdb processes.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_docdb_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

%% @doc Start the barrel_docdb supervisor
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% @doc Initialize the supervisor with child specs
-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 60
    },

    %% Prometheus metrics module
    %% Must start first to set up metric declarations
    Metrics = #{
        id => barrel_metrics,
        start => {barrel_metrics, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [barrel_metrics]
    },

    %% Shared RocksDB block cache for all databases
    %% Must start before any database opens
    Cache = #{
        id => barrel_cache,
        start => {barrel_cache, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [barrel_cache]
    },

    %% Global HLC clock for distributed time synchronization
    %% Registered as 'barrel_hlc_clock' for node-wide access
    HlcMaxOffset = application:get_env(barrel_docdb, hlc_max_offset, 0),
    Hlc = #{
        id => barrel_hlc_clock,
        start => {hlc, start_link, [barrel_hlc_clock, fun hlc:physical_clock/0, HlcMaxOffset]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [hlc]
    },

    %% Subscription manager for path-based document subscriptions
    Sub = #{
        id => barrel_sub,
        start => {barrel_sub, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [barrel_sub]
    },

    %% Query subscription manager for query-based document subscriptions
    QuerySub = #{
        id => barrel_query_sub,
        start => {barrel_query_sub, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [barrel_query_sub]
    },

    %% Path dictionary for path ID interning (posting lists)
    PathDict = #{
        id => barrel_path_dict,
        start => {barrel_path_dict, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [barrel_path_dict]
    },

    %% Query cursor manager for chunked query execution
    QueryCursor = #{
        id => barrel_query_cursor,
        start => {barrel_query_cursor, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [barrel_query_cursor]
    },

    %% Parallel worker pool for query processing
    Parallel = #{
        id => barrel_parallel,
        start => {barrel_parallel, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [barrel_parallel]
    },

    %% Database supervisor for managing individual database processes
    %% Note: Each database starts its own compaction filter handler in barrel_db_server
    DbSup = #{
        id => barrel_db_sup,
        start => {barrel_db_sup, start_link, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [barrel_db_sup]
    },

    %% Replication task manager for persistent replication tasks
    RepTasks = #{
        id => barrel_rep_tasks,
        start => {barrel_rep_tasks, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [barrel_rep_tasks]
    },

    %% Child specs (embedded-only; no HTTP server)
    ChildSpecs = [Metrics, Cache, Hlc, Sub, QuerySub, PathDict, QueryCursor, Parallel, DbSup, RepTasks],

    {ok, {SupFlags, ChildSpecs}}.
