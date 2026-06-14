%%%-------------------------------------------------------------------
%%% @doc Changes workload for barrel_bench
%%%
%%% Benchmarks changes feed operations and subscriptions.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_bench_changes).

-export([run/3]).

-define(BENCH_DB, <<"barrel_bench_db">>).

%% @doc Run changes benchmarks
-spec run(pid(), non_neg_integer(), non_neg_integer()) -> map().
run(Db, NumDocs, _Iterations) ->
    io:format("Running changes benchmarks...~n"),

    %% First load documents if not already loaded
    case barrel_docdb:get_doc(Db, <<"user_0">>) of
        {error, not_found} ->
            io:format("  Loading ~p documents...~n", [NumDocs]),
            load_docs(Db, NumDocs);
        {ok, _} ->
            ok
    end,

    %% Benchmark full changes scan
    %% Note: summarize immediately to avoid including other benchmark times
    io:format("  Scanning all changes...~n"),
    FullScanResult = barrel_bench_metrics:summarize(bench_full_changes(Db)),

    %% Benchmark incremental changes (batched reads)
    io:format("  Testing incremental changes...~n"),
    IncrementalResult = barrel_bench_metrics:summarize(bench_incremental_changes(Db, 100)),

    %% Benchmark wildcard path matching
    io:format("  Testing wildcard path matching...~n"),
    WildcardPathResult = barrel_bench_metrics:summarize(bench_wildcard_path_changes(Db)),

    %% Benchmark subscription latency
    SubCount = min(100, NumDocs div 10),
    io:format("  Testing subscription latency (~p docs)...~n", [SubCount]),
    SubLatencyResult = barrel_bench_metrics:summarize(bench_subscription_latency(SubCount)),

    #{
        full_scan => FullScanResult,
        incremental => IncrementalResult,
        wildcard_path => WildcardPathResult,
        subscription => SubLatencyResult
    }.

%%====================================================================
%% Internal functions
%%====================================================================

load_docs(Db, NumDocs) ->
    lists:foreach(fun(I) ->
        Doc = barrel_bench_generator:user_doc(I),
        barrel_docdb:put_doc(Db, Doc)
    end, lists:seq(0, NumDocs - 1)).

bench_full_changes(Db) ->
    Metrics = barrel_bench_metrics:new(),
    {Time, {ok, _Changes, _LastHlc}} = timer:tc(fun() ->
        barrel_docdb:get_changes(Db, first)
    end),
    barrel_bench_metrics:record(Metrics, Time).

bench_incremental_changes(Db, BatchSize) ->
    Metrics = barrel_bench_metrics:new(),
    bench_incremental_loop(Db, first, BatchSize, Metrics).

bench_incremental_loop(Db, Since, BatchSize, Metrics) ->
    {Time, Result} = timer:tc(fun() ->
        barrel_docdb:get_changes(Db, Since, #{limit => BatchSize})
    end),
    case Result of
        {ok, [], _} ->
            Metrics;
        {ok, _Changes, LastHlc} ->
            NewMetrics = barrel_bench_metrics:record(Metrics, Time),
            bench_incremental_loop(Db, LastHlc, BatchSize, NewMetrics)
    end.

%% Benchmark wildcard path matching with # pattern
bench_wildcard_path_changes(Db) ->
    Metrics = barrel_bench_metrics:new(),

    %% Test various wildcard patterns
    %% Documents are structured like: #{<<"type">> => <<"user">>, <<"data">> => ...}
    Patterns = [
        <<"type/#">>,      %% Match all with type field
        <<"data/#">>,      %% Match all with data field
        <<"user/#">>       %% Match user-specific paths (if any)
    ],

    lists:foldl(fun(Pattern, Acc) ->
        {Time, _Result} = timer:tc(fun() ->
            barrel_docdb:get_changes(Db, first, #{paths => [Pattern]})
        end),
        barrel_bench_metrics:record(Acc, Time)
    end, Metrics, Patterns).

bench_subscription_latency(Count) ->
    %% Subscribe to a path pattern that matches our test documents
    %% Documents will have structure: #{<<"bench">> => #{<<"data">> => ...}}
    %% which creates path topic "bench/data" matching pattern "bench/#"
    {ok, SubRef} = barrel_sub:subscribe(?BENCH_DB, <<"bench/#">>, self()),

    Metrics = barrel_bench_metrics:new(),

    %% Insert documents and measure notification latency
    FinalMetrics = lists:foldl(fun(I, Acc) ->
        DocId = <<"sub_", (integer_to_binary(I))/binary>>,
        %% Create doc with path structure that matches subscription
        Doc = #{
            <<"_id">> => DocId,
            <<"bench">> => #{
                <<"data">> => I,
                <<"timestamp">> => erlang:monotonic_time(microsecond)
            }
        },

        StartTime = erlang:monotonic_time(microsecond),
        {ok, _} = barrel_docdb:put_doc(?BENCH_DB, Doc),

        %% Wait for notification
        receive
            {barrel_change, ?BENCH_DB, _Change} ->
                EndTime = erlang:monotonic_time(microsecond),
                Latency = EndTime - StartTime,
                barrel_bench_metrics:record(Acc, Latency)
        after 5000 ->
            %% Timeout - skip this measurement
            Acc
        end
    end, Metrics, lists:seq(1, Count)),

    %% Cleanup subscription
    barrel_sub:unsubscribe(SubRef, self()),

    FinalMetrics.
