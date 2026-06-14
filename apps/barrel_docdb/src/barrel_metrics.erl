%%%-------------------------------------------------------------------
%%% @doc OpenTelemetry metrics for barrel_docdb
%%%
%%% Provides metrics for monitoring:
%%% - Document operations (put, get, delete)
%%% - Query performance
%%% - Replication status and throughput
%%% - Storage utilization
%%% - HTTP request latencies
%%% - Peer connectivity
%%%
%%% Metrics are exposed via the '/metrics' HTTP endpoint in Prometheus
%%% text format for scraping.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_metrics).

-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([setup/0]).

%% Metric recording functions
-export([
    %% Document operations
    inc_doc_ops/2,
    inc_doc_ops/3,
    observe_doc_latency/3,

    %% Query operations
    inc_query_ops/1,
    observe_query_latency/2,
    observe_query_results/2,
    inc_query_timeouts/0,

    %% Replication
    inc_rep_docs/2,
    inc_rep_errors/1,
    set_rep_lag/2,
    set_rep_active/2,

    %% Storage
    set_db_docs/2,
    set_db_size/2,
    set_db_attachments/2
]).

%% Export function
-export([export/0, export_text/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).
-define(METER_NAME, barrel_docdb).

%%====================================================================
%% Metric definitions
%%====================================================================

-define(METRICS, [
    %% Document operation counters
    {counter, barrel_doc_operations,
     <<"Total number of document operations">>},

    %% Document operation latency histogram
    {histogram, barrel_doc_operation_duration_seconds,
     <<"Document operation duration in seconds">>,
     [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]},

    %% Query counters
    {counter, barrel_query_operations,
     <<"Total number of query operations">>},

    %% Query latency histogram
    {histogram, barrel_query_duration_seconds,
     <<"Query duration in seconds">>,
     [0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]},

    %% Query result count histogram
    {histogram, barrel_query_results_count,
     <<"Number of results returned per query">>,
     [1, 10, 50, 100, 500, 1000, 5000]},

    %% Query timeout counter (pipeline / pool deadline exceeded)
    {counter, barrel_query_timeouts,
     <<"Total query operations aborted due to timeout">>},

    %% Replication document counter
    {counter, barrel_replication_docs,
     <<"Total documents replicated">>},

    %% Replication error counter
    {counter, barrel_replication_errors,
     <<"Total replication errors">>},

    %% Replication lag gauge
    {gauge, barrel_replication_lag_seconds,
     <<"Replication lag in seconds">>},

    %% Active replications gauge
    {gauge, barrel_replication_active,
     <<"Whether replication is active (1) or not (0)">>},

    %% Database document count gauge
    {gauge, barrel_db_documents_total,
     <<"Total number of documents in database">>},

    %% Database size gauge
    {gauge, barrel_db_size_bytes,
     <<"Database size in bytes">>},

    %% Database attachment count gauge
    {gauge, barrel_db_attachments_total,
     <<"Total number of attachments in database">>}
]).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Setup all metrics - call this during application startup
setup() ->
    %% The instrument application keeps its metric registry across a
    %% barrel_docdb stop/start, but the exemplar reservoir ETS table is
    %% created lazily and owned by the first caller. Recreate it here so a
    %% metric recorded right after a restart never hits a missing table.
    ok = instrument_exemplar:init_table(),
    Meter = instrument_meter:get_meter(?METER_NAME),
    lists:foreach(fun(Metric) -> declare_metric(Meter, Metric) end, ?METRICS),
    ok.

%%====================================================================
%% Document Operations
%%====================================================================

%% @doc Increment document operation counter
-spec inc_doc_ops(binary(), atom()) -> ok.
inc_doc_ops(Db, Op) ->
    inc_doc_ops(Db, Op, 1).

-spec inc_doc_ops(binary(), atom(), pos_integer()) -> ok.
inc_doc_ops(Db, Op, Count) ->
    Attrs = #{db => Db, operation => Op},
    case instrument_meter:get_instrument(barrel_doc_operations) of
        undefined -> ok;
        Instrument -> instrument_meter:add(Instrument, Count, Attrs)
    end,
    ok.

%% @doc Record document operation latency
-spec observe_doc_latency(binary(), atom(), number()) -> ok.
observe_doc_latency(Db, Op, DurationMs) ->
    Attrs = #{db => Db, operation => Op},
    case instrument_meter:get_instrument(barrel_doc_operation_duration_seconds) of
        undefined -> ok;
        Instrument -> instrument_meter:record(Instrument, DurationMs / 1000, Attrs)
    end,
    ok.

%%====================================================================
%% Query Operations
%%====================================================================

%% @doc Increment query operation counter
-spec inc_query_ops(binary()) -> ok.
inc_query_ops(Db) ->
    Attrs = #{db => Db},
    case instrument_meter:get_instrument(barrel_query_operations) of
        undefined -> ok;
        Instrument -> instrument_meter:add(Instrument, 1, Attrs)
    end,
    ok.

%% @doc Record query latency
-spec observe_query_latency(binary(), number()) -> ok.
observe_query_latency(Db, DurationMs) ->
    Attrs = #{db => Db},
    case instrument_meter:get_instrument(barrel_query_duration_seconds) of
        undefined -> ok;
        Instrument -> instrument_meter:record(Instrument, DurationMs / 1000, Attrs)
    end,
    ok.

%% @doc Record query result count
-spec observe_query_results(binary(), non_neg_integer()) -> ok.
observe_query_results(Db, Count) ->
    Attrs = #{db => Db},
    case instrument_meter:get_instrument(barrel_query_results_count) of
        undefined -> ok;
        Instrument -> instrument_meter:record(Instrument, Count, Attrs)
    end,
    ok.

%% @doc Increment the query timeout counter (pipeline/pool deadline exceeded).
-spec inc_query_timeouts() -> ok.
inc_query_timeouts() ->
    case instrument_meter:get_instrument(barrel_query_timeouts) of
        undefined -> ok;
        Instrument -> instrument_meter:add(Instrument, 1, #{})
    end,
    ok.

%%====================================================================
%% Replication
%%====================================================================

%% @doc Increment replicated document counter
-spec inc_rep_docs(push | pull, pos_integer()) -> ok.
inc_rep_docs(Direction, Count) ->
    Attrs = #{direction => Direction},
    case instrument_meter:get_instrument(barrel_replication_docs) of
        undefined -> ok;
        Instrument -> instrument_meter:add(Instrument, Count, Attrs)
    end,
    ok.

%% @doc Increment replication error counter
-spec inc_rep_errors(binary()) -> ok.
inc_rep_errors(TaskId) ->
    Attrs = #{task_id => TaskId},
    case instrument_meter:get_instrument(barrel_replication_errors) of
        undefined -> ok;
        Instrument -> instrument_meter:add(Instrument, 1, Attrs)
    end,
    ok.

%% @doc Set replication lag
-spec set_rep_lag(binary(), number()) -> ok.
set_rep_lag(TaskId, LagSeconds) ->
    Attrs = #{task_id => TaskId},
    case instrument_meter:get_instrument(barrel_replication_lag_seconds) of
        undefined -> ok;
        Instrument -> instrument_meter:set(Instrument, LagSeconds, Attrs)
    end,
    ok.

%% @doc Set replication active status
-spec set_rep_active(binary(), boolean()) -> ok.
set_rep_active(TaskId, Active) ->
    Value = case Active of true -> 1; false -> 0 end,
    Attrs = #{task_id => TaskId},
    case instrument_meter:get_instrument(barrel_replication_active) of
        undefined -> ok;
        Instrument -> instrument_meter:set(Instrument, Value, Attrs)
    end,
    ok.

%%====================================================================
%% Storage
%%====================================================================

%% @doc Set database document count
-spec set_db_docs(binary(), non_neg_integer()) -> ok.
set_db_docs(Db, Count) ->
    Attrs = #{db => Db},
    case instrument_meter:get_instrument(barrel_db_documents_total) of
        undefined -> ok;
        Instrument -> instrument_meter:set(Instrument, Count, Attrs)
    end,
    ok.

%% @doc Set database size in bytes
-spec set_db_size(binary(), non_neg_integer()) -> ok.
set_db_size(Db, SizeBytes) ->
    Attrs = #{db => Db},
    case instrument_meter:get_instrument(barrel_db_size_bytes) of
        undefined -> ok;
        Instrument -> instrument_meter:set(Instrument, SizeBytes, Attrs)
    end,
    ok.

%% @doc Set database attachment count
-spec set_db_attachments(binary(), non_neg_integer()) -> ok.
set_db_attachments(Db, Count) ->
    Attrs = #{db => Db},
    case instrument_meter:get_instrument(barrel_db_attachments_total) of
        undefined -> ok;
        Instrument -> instrument_meter:set(Instrument, Count, Attrs)
    end,
    ok.

%%====================================================================
%% Export
%%====================================================================

%% @doc Export all metrics in Prometheus text format
-spec export() -> binary().
export() ->
    instrument_prometheus:format().

%% @doc Export all metrics as a binary string
-spec export_text() -> binary().
export_text() ->
    iolist_to_binary(export()).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Ensure instrument application is started
    _ = application:ensure_all_started(instrument),
    setup(),
    {ok, #{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

declare_metric(Meter, {counter, Name, Description}) ->
    instrument_meter:create_counter(Meter, Name, #{description => Description});
declare_metric(Meter, {gauge, Name, Description}) ->
    instrument_meter:create_gauge(Meter, Name, #{description => Description});
declare_metric(Meter, {histogram, Name, Description, Boundaries}) ->
    instrument_meter:create_histogram(Meter, Name,
        #{description => Description, boundaries => Boundaries}).
