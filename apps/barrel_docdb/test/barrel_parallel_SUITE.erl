%%%-------------------------------------------------------------------
%%% @doc Common Test suite for barrel_parallel
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_parallel_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% CT callbacks
-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1
]).

%% Test cases
-export([
    %% pmap tests
    pmap_empty_list/1,
    pmap_single_item/1,
    pmap_small_list_sequential/1,
    pmap_large_list_parallel/1,
    pmap_preserves_order/1,
    pmap_custom_workers/1,
    pmap_handles_worker_error/1,
    pmap_handles_worker_crash/1,
    pmap_raises_query_timeout/1,

    %% pfiltermap tests
    pfiltermap_empty_list/1,
    pfiltermap_filters_false/1,
    pfiltermap_keeps_true_values/1,
    pfiltermap_preserves_order/1,
    pfiltermap_large_list/1,
    pfiltermap_handles_error/1,

    %% concurrency tests
    concurrent_execution/1,
    respects_max_workers/1,

    %% benchmark tests
    benchmark_pmap_vs_map/1,
    benchmark_pfiltermap_vs_filtermap/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [
        {group, pmap},
        {group, pfiltermap},
        {group, concurrency},
        {group, benchmark}
    ].

groups() ->
    [
        %% Note: Cannot run in parallel - tests share the barrel_parallel pool
        %% and error-handling tests can interfere with other tests
        {pmap, [sequence], [
            pmap_empty_list,
            pmap_single_item,
            pmap_small_list_sequential,
            pmap_large_list_parallel,
            pmap_preserves_order,
            pmap_custom_workers,
            pmap_handles_worker_error,
            pmap_handles_worker_crash,
            pmap_raises_query_timeout
        ]},
        {pfiltermap, [sequence], [
            pfiltermap_empty_list,
            pfiltermap_filters_false,
            pfiltermap_keeps_true_values,
            pfiltermap_preserves_order,
            pfiltermap_large_list,
            pfiltermap_handles_error
        ]},
        {concurrency, [], [
            concurrent_execution,
            respects_max_workers
        ]},
        {benchmark, [], [
            benchmark_pmap_vs_map,
            benchmark_pfiltermap_vs_filtermap
        ]}
    ].

init_per_suite(Config) ->
    %% Load barrel_docdb so application:set_env in tests is visible to
    %% the pool's collect_pool_results env lookup.
    _ = application:load(barrel_docdb),
    %% Start the worker pool for tests (may already be started by application)
    case barrel_parallel:start_link() of
        {ok, _Pid} -> ok;
        {error, {already_started, _Pid}} -> ok
    end,
    Config.

end_per_suite(_Config) ->
    %% Don't stop barrel_parallel - it's managed by barrel_docdb_sup
    %% and stopping it here can cause race conditions when the supervisor restarts it
    ok.

%%====================================================================
%% pmap Tests
%%====================================================================

pmap_empty_list(_Config) ->
    ?assertEqual([], barrel_parallel:pmap(fun(X) -> X * 2 end, [])).

pmap_single_item(_Config) ->
    ?assertEqual([10], barrel_parallel:pmap(fun(X) -> X * 2 end, [5])).

pmap_small_list_sequential(_Config) ->
    %% Small lists (<=10) should use sequential map
    Items = lists:seq(1, 5),
    Expected = [2, 4, 6, 8, 10],
    ?assertEqual(Expected, barrel_parallel:pmap(fun(X) -> X * 2 end, Items)).

pmap_large_list_parallel(_Config) ->
    %% Large lists (>10) should use parallel execution
    Items = lists:seq(1, 100),
    Expected = lists:map(fun(X) -> X * 2 end, Items),
    ?assertEqual(Expected, barrel_parallel:pmap(fun(X) -> X * 2 end, Items)).

pmap_preserves_order(_Config) ->
    %% Results must be in same order as input
    Items = lists:seq(1, 50),
    %% Add varying delays to ensure order isn't just luck
    Fun = fun(X) ->
        timer:sleep(rand:uniform(5)),
        X * 3
    end,
    Expected = lists:map(fun(X) -> X * 3 end, Items),
    ?assertEqual(Expected, barrel_parallel:pmap(Fun, Items)).

pmap_custom_workers(_Config) ->
    Items = lists:seq(1, 20),
    Expected = lists:map(fun(X) -> X + 1 end, Items),
    ?assertEqual(Expected, barrel_parallel:pmap(fun(X) -> X + 1 end, Items, 2)).

pmap_handles_worker_error(_Config) ->
    Items = [1, 2, 3, error, 5],
    Fun = fun(error) -> error(intentional_error);
             (X) -> X * 2
          end,
    ?assertError(intentional_error, barrel_parallel:pmap(Fun, Items)).

pmap_handles_worker_crash(_Config) ->
    Items = lists:seq(1, 20),
    Fun = fun(10) -> exit(crash);
             (X) -> X * 2
          end,
    %% exit/1 is caught and re-raised as exit
    ?assertExit(crash, barrel_parallel:pmap(Fun, Items)).

%% When workers exceed `query_timeout_ms', collect_pool_results must
%% raise `error({query_timeout, Info})' instead of returning a partial
%% result silently. Deterministic: workers sleep past the deadline.
pmap_raises_query_timeout(_Config) ->
    Prev = application:get_env(barrel_docdb, query_timeout_ms),
    application:set_env(barrel_docdb, query_timeout_ms, 50),
    ?assertEqual({ok, 50}, application:get_env(barrel_docdb, query_timeout_ms)),
    try
        %% Spawn-fallback path collects per-batch (one batch of
        %% MaxWorkers items at a time), so `total' in the error map
        %% is the batch size, not the original 20. Pool path tracks
        %% the full submission. Either way, the error must include
        %% the timeout, a non-zero pending count, and the configured
        %% deadline.
        Items = lists:seq(1, 20),
        Fun = fun(_) -> timer:sleep(2000), ok end,
        try
            Res = barrel_parallel:pmap(Fun, Items),
            ct:fail({"expected query_timeout error, got result", Res})
        catch
            error:{query_timeout, Info} ->
                ?assertEqual(50, maps:get(timeout_ms, Info)),
                ?assert(maps:get(total, Info) > 0),
                ?assert(maps:get(missing_batches, Info) > 0)
        end
    after
        case Prev of
            undefined -> application:unset_env(barrel_docdb, query_timeout_ms);
            {ok, V}   -> application:set_env(barrel_docdb, query_timeout_ms, V)
        end
    end.

%%====================================================================
%% pfiltermap Tests
%%====================================================================

pfiltermap_empty_list(_Config) ->
    ?assertEqual([], barrel_parallel:pfiltermap(fun(_) -> true end, [])).

pfiltermap_filters_false(_Config) ->
    Items = lists:seq(1, 20),
    %% Filter out odd numbers
    Fun = fun(X) when X rem 2 == 0 -> {true, X};
             (_) -> false
          end,
    Expected = [2, 4, 6, 8, 10, 12, 14, 16, 18, 20],
    ?assertEqual(Expected, barrel_parallel:pfiltermap(Fun, Items)).

pfiltermap_keeps_true_values(_Config) ->
    Items = lists:seq(1, 15),
    %% Transform and filter: keep evens, double them
    Fun = fun(X) when X rem 2 == 0 -> {true, X * 2};
             (_) -> false
          end,
    Expected = [4, 8, 12, 16, 20, 24, 28],
    ?assertEqual(Expected, barrel_parallel:pfiltermap(Fun, Items)).

pfiltermap_preserves_order(_Config) ->
    Items = lists:seq(1, 30),
    Fun = fun(X) when X rem 3 == 0 ->
            timer:sleep(rand:uniform(3)),
            {true, X * 10};
             (_) -> false
          end,
    Expected = [30, 60, 90, 120, 150, 180, 210, 240, 270, 300],
    ?assertEqual(Expected, barrel_parallel:pfiltermap(Fun, Items)).

pfiltermap_large_list(_Config) ->
    Items = lists:seq(1, 1000),
    Fun = fun(X) when X rem 7 == 0 -> {true, X};
             (_) -> false
          end,
    Expected = lists:filtermap(Fun, Items),
    ?assertEqual(Expected, barrel_parallel:pfiltermap(Fun, Items)).

pfiltermap_handles_error(_Config) ->
    Items = [1, 2, 3, bomb, 5, 6, 7, 8, 9, 10, 11, 12],
    Fun = fun(bomb) -> error(boom);
             (X) when X rem 2 == 0 -> {true, X};
             (_) -> false
          end,
    ?assertError(boom, barrel_parallel:pfiltermap(Fun, Items)).

%%====================================================================
%% Concurrency Tests
%%====================================================================

concurrent_execution(_Config) ->
    %% Verify that work actually happens in parallel
    Parent = self(),
    Items = lists:seq(1, 8),

    %% Each worker records its start time
    Fun = fun(X) ->
        Parent ! {started, X, erlang:monotonic_time(millisecond)},
        timer:sleep(50),
        X
    end,

    %% Run with 4 workers
    _Results = barrel_parallel:pmap(Fun, Items, 4),

    %% Collect start times
    StartTimes = collect_messages(started, 8, []),

    %% With 4 workers and 8 items, we should have 2 batches
    %% Items 1-4 should start around the same time
    %% Items 5-8 should start ~50ms later
    Times = lists:sort([{X, T} || {X, T} <- StartTimes]),
    {FirstBatch, SecondBatch} = lists:split(4, Times),

    FirstTimes = [T || {_, T} <- FirstBatch],
    SecondTimes = [T || {_, T} <- SecondBatch],

    FirstAvg = lists:sum(FirstTimes) / 4,
    SecondAvg = lists:sum(SecondTimes) / 4,

    %% Second batch should start at least 40ms after first (allowing some slack)
    ?assert(SecondAvg - FirstAvg >= 40,
            io_lib:format("Expected batches 50ms apart, got ~p ms", [SecondAvg - FirstAvg])).

respects_max_workers(_Config) ->
    %% Test that we never exceed max workers
    Parent = self(),
    Counter = counters:new(1, []),

    Items = lists:seq(1, 20),
    MaxWorkers = 3,

    Fun = fun(X) ->
        counters:add(Counter, 1, 1),
        Current = counters:get(Counter, 1),
        Parent ! {concurrent, Current},
        timer:sleep(20),
        counters:sub(Counter, 1, 1),
        X
    end,

    _Results = barrel_parallel:pmap(Fun, Items, MaxWorkers),

    %% Collect all concurrency readings
    Readings = collect_messages(concurrent, 20, []),
    MaxObserved = lists:max(Readings),

    ?assert(MaxObserved =< MaxWorkers,
            io_lib:format("Max workers exceeded: observed ~p, limit ~p",
                         [MaxObserved, MaxWorkers])).

%%====================================================================
%% Benchmark Tests
%%====================================================================

benchmark_pmap_vs_map(_Config) ->
    %% CPU-bound work simulation - needs to be heavy enough to offset spawn overhead
    Items = lists:seq(1, 200),
    Fun = fun(X) ->
        %% Heavier CPU work to justify parallelization
        lists:foldl(fun(I, Acc) -> Acc + I * X end, 0, lists:seq(1, 1000))
    end,

    %% Warm up
    _ = lists:map(Fun, lists:seq(1, 10)),
    _ = barrel_parallel:pmap(Fun, lists:seq(1, 10)),

    %% Benchmark sequential
    {SeqTime, SeqResult} = timer:tc(fun() -> lists:map(Fun, Items) end),

    %% Benchmark parallel
    {ParTime, ParResult} = timer:tc(fun() -> barrel_parallel:pmap(Fun, Items) end),

    %% Results must be identical
    ?assertEqual(SeqResult, ParResult),

    %% Report times (no assertion on speedup - just informational)
    SeqMs = SeqTime / 1000,
    ParMs = ParTime / 1000,
    Speedup = SeqTime / max(ParTime, 1),
    ct:pal("pmap benchmark (200 items, heavy CPU work):~n"
           "  Sequential: ~.2f ms~n"
           "  Parallel:   ~.2f ms~n"
           "  Speedup:    ~.2fx",
           [SeqMs, ParMs, Speedup]).

benchmark_pfiltermap_vs_filtermap(_Config) ->
    Items = lists:seq(1, 200),
    Fun = fun(X) when X rem 3 == 0 ->
            %% Heavier CPU work for matching items
            Val = lists:foldl(fun(I, Acc) -> Acc + I * X end, 0, lists:seq(1, 500)),
            {true, Val};
             (_) -> false
          end,

    %% Warm up
    _ = lists:filtermap(Fun, lists:seq(1, 10)),
    _ = barrel_parallel:pfiltermap(Fun, lists:seq(1, 10)),

    %% Benchmark sequential
    {SeqTime, SeqResult} = timer:tc(fun() -> lists:filtermap(Fun, Items) end),

    %% Benchmark parallel
    {ParTime, ParResult} = timer:tc(fun() -> barrel_parallel:pfiltermap(Fun, Items) end),

    %% Results must be identical
    ?assertEqual(SeqResult, ParResult),

    SeqMs = SeqTime / 1000,
    ParMs = ParTime / 1000,
    Speedup = SeqTime / max(ParTime, 1),
    ct:pal("pfiltermap benchmark (200 items, 66 matches):~n"
           "  Sequential: ~.2f ms~n"
           "  Parallel:   ~.2f ms~n"
           "  Speedup:    ~.2fx",
           [SeqMs, ParMs, Speedup]).

%%====================================================================
%% Internal functions
%%====================================================================

collect_messages(_Tag, 0, Acc) ->
    lists:reverse(Acc);
collect_messages(Tag, N, Acc) ->
    receive
        {Tag, Value} -> collect_messages(Tag, N - 1, [Value | Acc]);
        {Tag, Key, Value} -> collect_messages(Tag, N - 1, [{Key, Value} | Acc])
    after 5000 ->
        ct:fail("Timeout waiting for message ~p (~p remaining)", [Tag, N])
    end.
