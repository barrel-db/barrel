%%%-------------------------------------------------------------------
%%% @doc Benchmark comparison utility for regression detection
%%%
%%% Compares benchmark results between runs to detect performance
%%% regressions or improvements.
%%%
%%% Usage:
%%%   barrel_vectordb_bench_compare:compare("baseline.json", "current.json").
%%%   barrel_vectordb_bench_compare:compare_with_threshold("baseline.json", "current.json", 0.10).
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_bench_compare).

-export([
    compare/2,
    compare_with_threshold/3,
    load_results/1,
    generate_report/3
]).

-define(DEFAULT_REGRESSION_THRESHOLD, 0.10).  % 10% regression threshold

%% @doc Compare two benchmark result files
-spec compare(string(), string()) -> {ok, map()} | {error, term()}.
compare(BaselineFile, CurrentFile) ->
    compare_with_threshold(BaselineFile, CurrentFile, ?DEFAULT_REGRESSION_THRESHOLD).

%% @doc Compare with custom regression threshold
-spec compare_with_threshold(string(), string(), float()) -> {ok, map()} | {error, term()}.
compare_with_threshold(BaselineFile, CurrentFile, Threshold) ->
    case {load_results(BaselineFile), load_results(CurrentFile)} of
        {{ok, Baseline}, {ok, Current}} ->
            Report = generate_report(Baseline, Current, Threshold),
            print_report(Report),
            {ok, Report};
        {{error, Reason}, _} ->
            {error, {baseline_load_failed, Reason}};
        {_, {error, Reason}} ->
            {error, {current_load_failed, Reason}}
    end.

%% @doc Load benchmark results from JSON file
-spec load_results(string()) -> {ok, map()} | {error, term()}.
load_results(Filename) ->
    case file:read_file(Filename) of
        {ok, Bin} ->
            try
                {ok, json:decode(Bin)}
            catch
                _:Reason -> {error, {json_parse_failed, Reason}}
            end;
        {error, Reason} ->
            {error, {file_read_failed, Reason}}
    end.

%% @doc Generate comparison report
-spec generate_report(map(), map(), float()) -> map().
generate_report(Baseline, Current, Threshold) ->
    BaselineResults = get_results(Baseline),
    CurrentResults = get_results(Current),

    Comparisons = lists:filtermap(fun(BaseResult) ->
        Name = maps:get(<<"name">>, BaseResult),
        case find_result(Name, CurrentResults) of
            {ok, CurrResult} ->
                Comparison = compare_result(BaseResult, CurrResult, Threshold),
                {true, Comparison};
            not_found ->
                false
        end
    end, BaselineResults),

    %% Summary statistics
    Regressions = [C || C <- Comparisons, maps:get(status, C) =:= regression],
    Improvements = [C || C <- Comparisons, maps:get(status, C) =:= improvement],
    Unchanged = [C || C <- Comparisons, maps:get(status, C) =:= unchanged],

    #{
        baseline_timestamp => get_timestamp(Baseline),
        current_timestamp => get_timestamp(Current),
        threshold => Threshold,
        comparisons => Comparisons,
        summary => #{
            total => length(Comparisons),
            regressions => length(Regressions),
            improvements => length(Improvements),
            unchanged => length(Unchanged)
        },
        has_regressions => length(Regressions) > 0
    }.

%%====================================================================
%% Internal Functions
%%====================================================================

get_results(Data) ->
    case maps:get(<<"benchmark_run">>, Data, undefined) of
        undefined -> maps:get(<<"results">>, Data, []);
        Run -> maps:get(<<"results">>, Run, [])
    end.

get_timestamp(Data) ->
    case maps:get(<<"benchmark_run">>, Data, undefined) of
        undefined -> <<"unknown">>;
        Run -> maps:get(<<"timestamp">>, Run, <<"unknown">>)
    end.

find_result(Name, Results) ->
    case lists:filter(fun(R) -> maps:get(<<"name">>, R) =:= Name end, Results) of
        [Result | _] -> {ok, Result};
        [] -> not_found
    end.

compare_result(Baseline, Current, Threshold) ->
    Name = maps:get(<<"name">>, Baseline),

    %% Compare key metrics
    BaseOps = maps:get(<<"ops_per_sec">>, Baseline),
    CurrOps = maps:get(<<"ops_per_sec">>, Current),

    BaseP95 = maps:get(<<"p95_ms">>, Baseline),
    CurrP95 = maps:get(<<"p95_ms">>, Current),

    %% Calculate changes (positive = improvement for ops/sec, negative = improvement for latency)
    OpsChange = (CurrOps - BaseOps) / BaseOps,
    P95Change = (CurrP95 - BaseP95) / BaseP95,

    %% Determine status
    %% Regression if: ops/sec decreased or latency increased beyond threshold
    Status = case {OpsChange < -Threshold, P95Change > Threshold} of
        {true, _} -> regression;
        {_, true} -> regression;
        _ when OpsChange > Threshold -> improvement;
        _ when P95Change < -Threshold -> improvement;
        _ -> unchanged
    end,

    #{
        name => Name,
        status => Status,
        baseline => #{
            ops_per_sec => BaseOps,
            p95_ms => BaseP95,
            mean_ms => maps:get(<<"mean_ms">>, Baseline)
        },
        current => #{
            ops_per_sec => CurrOps,
            p95_ms => CurrP95,
            mean_ms => maps:get(<<"mean_ms">>, Current)
        },
        changes => #{
            ops_per_sec_pct => OpsChange * 100,
            p95_ms_pct => P95Change * 100
        }
    }.

print_report(Report) ->
    #{
        baseline_timestamp := BaseTs,
        current_timestamp := CurrTs,
        threshold := Threshold,
        comparisons := Comparisons,
        summary := Summary,
        has_regressions := HasRegressions
    } = Report,

    io:format("~n========================================~n"),
    io:format("       BENCHMARK COMPARISON REPORT~n"),
    io:format("========================================~n~n"),
    io:format("Baseline: ~s~n", [BaseTs]),
    io:format("Current:  ~s~n", [CurrTs]),
    io:format("Threshold: ~.1f%~n~n", [Threshold * 100]),

    %% Print each comparison
    lists:foreach(fun(C) ->
        print_comparison(C)
    end, Comparisons),

    %% Print summary
    io:format("~n--- SUMMARY ---~n"),
    io:format("Total benchmarks: ~p~n", [maps:get(total, Summary)]),
    io:format("Regressions:      ~p~n", [maps:get(regressions, Summary)]),
    io:format("Improvements:     ~p~n", [maps:get(improvements, Summary)]),
    io:format("Unchanged:        ~p~n", [maps:get(unchanged, Summary)]),

    case HasRegressions of
        true ->
            io:format("~n!! REGRESSIONS DETECTED !!~n");
        false ->
            io:format("~nNo regressions detected.~n")
    end.

print_comparison(#{
    name := Name,
    status := Status,
    baseline := Base,
    current := Curr,
    changes := Changes
}) ->
    StatusStr = case Status of
        regression -> "[REGRESSION]";
        improvement -> "[IMPROVEMENT]";
        unchanged -> "[OK]"
    end,

    io:format("~n~s ~s~n", [StatusStr, Name]),
    io:format("  Throughput: ~.2f -> ~.2f ops/sec (~.1f%)~n", [
        maps:get(ops_per_sec, Base),
        maps:get(ops_per_sec, Curr),
        maps:get(ops_per_sec_pct, Changes)
    ]),
    io:format("  P95 Latency: ~.3f -> ~.3f ms (~.1f%)~n", [
        maps:get(p95_ms, Base),
        maps:get(p95_ms, Curr),
        maps:get(p95_ms_pct, Changes)
    ]).
