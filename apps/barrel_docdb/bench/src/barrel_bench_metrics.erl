%%%-------------------------------------------------------------------
%%% @doc Metrics collection for barrel_bench
%%%
%%% Collects latency measurements and computes summary statistics
%%% including percentiles and throughput.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_bench_metrics).

-export([new/0, record/2, summarize/1]).
-export([percentile/2, avg/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type metrics() :: #{
    latencies := [non_neg_integer()],
    start := integer(),
    count := non_neg_integer()
}.

-type summary() :: #{
    count := non_neg_integer(),
    elapsed_us := non_neg_integer(),
    throughput := float(),
    latency_avg := float(),
    latency_min := non_neg_integer(),
    latency_max := non_neg_integer(),
    latency_p50 := non_neg_integer(),
    latency_p95 := non_neg_integer(),
    latency_p99 := non_neg_integer()
}.

-export_type([metrics/0, summary/0]).

%% @doc Create a new metrics collector
-spec new() -> metrics().
new() ->
    #{
        latencies => [],
        start => erlang:monotonic_time(microsecond),
        count => 0
    }.

%% @doc Record a latency measurement (in microseconds)
-spec record(metrics(), non_neg_integer()) -> metrics().
record(#{latencies := Lats, count := N} = Metrics, LatencyUs) ->
    Metrics#{
        latencies => [LatencyUs | Lats],
        count => N + 1
    }.

%% @doc Compute summary statistics from collected metrics
-spec summarize(metrics()) -> summary().
summarize(#{latencies := [], start := Start, count := 0}) ->
    Elapsed = erlang:monotonic_time(microsecond) - Start,
    #{
        count => 0,
        elapsed_us => Elapsed,
        throughput => 0.0,
        latency_avg => 0.0,
        latency_min => 0,
        latency_max => 0,
        latency_p50 => 0,
        latency_p95 => 0,
        latency_p99 => 0
    };
summarize(#{latencies := Lats, start := Start, count := N}) ->
    Elapsed = erlang:monotonic_time(microsecond) - Start,
    Sorted = lists:sort(Lats),
    #{
        count => N,
        elapsed_us => Elapsed,
        throughput => (N * 1000000) / max(1, Elapsed),
        latency_avg => avg(Lats),
        latency_min => hd(Sorted),
        latency_max => lists:last(Sorted),
        latency_p50 => percentile(Sorted, 0.50),
        latency_p95 => percentile(Sorted, 0.95),
        latency_p99 => percentile(Sorted, 0.99)
    }.

%% @doc Calculate percentile from a sorted list
-spec percentile([non_neg_integer()], float()) -> non_neg_integer().
percentile([], _P) ->
    0;
percentile(Sorted, P) when P >= 0, P =< 1 ->
    N = length(Sorted),
    Index = max(1, ceil(P * N)),
    lists:nth(Index, Sorted).

%% @doc Calculate average of a list
-spec avg([number()]) -> float().
avg([]) ->
    0.0;
avg(List) ->
    lists:sum(List) / length(List).

%%====================================================================
%% EUnit Tests
%%====================================================================

-ifdef(TEST).

new_test() ->
    M = new(),
    ?assert(is_map(M)),
    ?assertEqual([], maps:get(latencies, M)),
    ?assertEqual(0, maps:get(count, M)).

record_test() ->
    M0 = new(),
    M1 = record(M0, 100),
    ?assertEqual(1, maps:get(count, M1)),
    ?assertEqual([100], maps:get(latencies, M1)),

    M2 = record(M1, 200),
    ?assertEqual(2, maps:get(count, M2)),
    ?assertEqual([200, 100], maps:get(latencies, M2)).

percentile_empty_test() ->
    ?assertEqual(0, percentile([], 0.5)).

percentile_single_test() ->
    ?assertEqual(42, percentile([42], 0.5)),
    ?assertEqual(42, percentile([42], 0.99)).

percentile_test() ->
    %% 100 elements: 1..100
    Sorted = lists:seq(1, 100),
    ?assertEqual(50, percentile(Sorted, 0.50)),
    ?assertEqual(95, percentile(Sorted, 0.95)),
    ?assertEqual(99, percentile(Sorted, 0.99)),
    ?assertEqual(100, percentile(Sorted, 1.0)),
    ?assertEqual(1, percentile(Sorted, 0.0)).

avg_empty_test() ->
    ?assertEqual(0.0, avg([])).

avg_test() ->
    ?assertEqual(2.0, avg([1, 2, 3])),
    ?assertEqual(5.0, avg([5])),
    ?assertEqual(50.5, avg(lists:seq(1, 100))).

summarize_empty_test() ->
    M = new(),
    S = summarize(M),
    ?assertEqual(0, maps:get(count, S)),
    ?assertEqual(0.0, maps:get(throughput, S)),
    ?assertEqual(0.0, maps:get(latency_avg, S)).

summarize_test() ->
    M0 = new(),
    %% Record latencies: 10, 20, 30, 40, 50
    M1 = lists:foldl(fun(L, Acc) -> record(Acc, L) end, M0, [10, 20, 30, 40, 50]),
    S = summarize(M1),

    ?assertEqual(5, maps:get(count, S)),
    ?assertEqual(30.0, maps:get(latency_avg, S)),
    ?assertEqual(10, maps:get(latency_min, S)),
    ?assertEqual(50, maps:get(latency_max, S)),
    ?assertEqual(30, maps:get(latency_p50, S)),
    ?assert(maps:get(throughput, S) > 0).

-endif.
