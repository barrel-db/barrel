%%%-------------------------------------------------------------------
%%% @doc Benchmark runner for barrel_vectordb BM25 backends
%%%
%%% Provides performance benchmarks comparing memory vs disk BM25 backends,
%%% measuring throughput, latency percentiles, and early termination metrics.
%%%
%%% Usage:
%%%   rebar3 as bench shell
%%%   barrel_vectordb_bm25_bench:run_all().
%%%   barrel_vectordb_bm25_bench:compare_backends().
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_bm25_bench).

-include_lib("eunit/include/eunit.hrl").

-export([
    run_all/0,
    run_all/1,
    run/1,
    run/2,
    compare_backends/0,
    compare_backends/1
]).

%% Benchmark definitions
-export([
    bench_add_single/1,
    bench_add_batch_100/1,
    bench_add_batch_1000/1,
    bench_search_single_term/1,
    bench_search_multi_term/1,
    bench_search_many_terms/1,
    bench_search_k10/1,
    bench_search_k50/1,
    bench_hybrid_search/1,
    bench_large_index_search/1,
    bench_compaction/1
]).

-define(DEFAULT_OPTS, #{
    warmup_iterations => 5,
    iterations => 50,
    index_size => 1000,           % Docs for search benchmarks
    large_index_size => 10000,    % Docs for large index benchmark
    bm25_backend => disk,         % memory | disk
    output_format => console
}).

%% Sample text corpus for realistic BM25 benchmarks
-define(SAMPLE_TEXTS, [
    <<"Erlang is a programming language used to build massively scalable soft real-time systems">>,
    <<"The BEAM virtual machine executes Erlang and Elixir bytecode with preemptive scheduling">>,
    <<"OTP is a set of Erlang libraries and design principles providing middleware">>,
    <<"Concurrent programming in Erlang uses lightweight processes and message passing">>,
    <<"Pattern matching is a fundamental feature of Erlang for control flow and data extraction">>,
    <<"Hot code loading allows updating code in a running Erlang system without stopping">>,
    <<"Distributed Erlang enables transparent communication between nodes in a cluster">>,
    <<"Supervision trees provide fault tolerance by restarting failed processes automatically">>,
    <<"ETS tables provide fast in-memory storage for concurrent read access">>,
    <<"GenServer is a behavior module for implementing server processes in Erlang">>
]).

-define(SAMPLE_QUERIES, [
    <<"erlang programming">>,
    <<"concurrent processes">>,
    <<"distributed systems">>,
    <<"fault tolerance">>,
    <<"pattern matching">>,
    <<"message passing">>,
    <<"supervision tree">>,
    <<"hot code loading">>,
    <<"BEAM virtual machine">>,
    <<"OTP design principles">>
]).

%%====================================================================
%% EUnit Test (Smoke Test)
%%====================================================================

bm25_benchmark_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
        {"BM25 benchmark smoke test",
         {timeout, 120, fun() ->
             {ok, Result} = run(search_single_term, #{
                 iterations => 3,
                 warmup_iterations => 1,
                 index_size => 100,
                 bm25_backend => disk
             }),
             ?assert(maps:get(ops_per_sec, Result) > 0)
         end}}
     ]}.

%%====================================================================
%% Public API
%%====================================================================

%% @doc Run all BM25 benchmarks with default options
-spec run_all() -> {ok, [map()]}.
run_all() ->
    run_all(?DEFAULT_OPTS).

%% @doc Run all BM25 benchmarks with custom options
-spec run_all(map()) -> {ok, [map()]}.
run_all(Opts) ->
    MergedOpts = maps:merge(?DEFAULT_OPTS, Opts),
    Benchmarks = [
        add_single,
        add_batch_100,
        add_batch_1000,
        search_single_term,
        search_multi_term,
        search_many_terms,
        search_k10,
        search_k50,
        hybrid_search,
        compaction
    ],

    %% Add large index benchmark if requested
    AllBenchmarks = case maps:get(include_large, MergedOpts, false) of
        true -> Benchmarks ++ [large_index_search];
        false -> Benchmarks
    end,

    Results = lists:map(fun(Name) ->
        io:format("~n"),
        {ok, Result} = run(Name, MergedOpts),
        Result
    end, AllBenchmarks),

    print_summary(Results),
    {ok, Results}.

%% @doc Run a single benchmark
-spec run(atom()) -> {ok, map()}.
run(Name) ->
    run(Name, ?DEFAULT_OPTS).

%% @doc Run a single benchmark with options
-spec run(atom(), map()) -> {ok, map()}.
run(Name, Opts) ->
    MergedOpts = maps:merge(?DEFAULT_OPTS, Opts),
    Backend = maps:get(bm25_backend, MergedOpts),
    io:format("=== BM25 Benchmark: ~p (backend=~p) ===~n", [Name, Backend]),

    {Store, TestDir} = setup_bm25_store(MergedOpts),

    try
        BenchFun = get_bench_fun(Name),

        %% Warmup
        WarmupIters = maps:get(warmup_iterations, MergedOpts),
        io:format("Warmup: ~p iterations...~n", [WarmupIters]),
        _ = run_iterations(BenchFun, Store, WarmupIters, MergedOpts),

        %% Run benchmark
        Iterations = maps:get(iterations, MergedOpts),
        io:format("Running: ~p iterations...~n", [Iterations]),
        {Timings, OpCounts, Metrics} = run_iterations(BenchFun, Store, Iterations, MergedOpts),

        %% Calculate stats
        Stats = calculate_stats(Name, Backend, Timings, OpCounts, Metrics, MergedOpts),
        print_stats(Stats),

        cleanup_bm25_store(Store, TestDir),
        {ok, Stats}
    catch
        E:R:ST ->
            cleanup_bm25_store(Store, TestDir),
            io:format("Benchmark ~p failed: ~p:~p~n~p~n", [Name, E, R, ST]),
            {error, {E, R}}
    end.

%% @doc Compare memory vs disk backend performance
-spec compare_backends() -> {ok, map()}.
compare_backends() ->
    compare_backends(?DEFAULT_OPTS).

%% @doc Compare backends with options
-spec compare_backends(map()) -> {ok, map()}.
compare_backends(Opts) ->
    MergedOpts = maps:merge(?DEFAULT_OPTS, Opts),

    io:format("~n========================================~n"),
    io:format("BM25 Backend Comparison~n"),
    io:format("========================================~n"),

    %% Run with memory backend
    io:format("~n--- Memory Backend ---~n"),
    {ok, MemoryResults} = run_all(MergedOpts#{bm25_backend => memory}),

    %% Run with disk backend
    io:format("~n--- Disk Backend ---~n"),
    {ok, DiskResults} = run_all(MergedOpts#{bm25_backend => disk}),

    %% Print comparison
    print_comparison(MemoryResults, DiskResults),

    {ok, #{memory => MemoryResults, disk => DiskResults}}.

%%====================================================================
%% Benchmark Implementations
%%====================================================================

bench_add_single(#{store := Store, dimension := Dim} = Ctx) ->
    ensure_indexed(Ctx),
    Id = make_id(),
    Text = random_text(),
    Vector = random_vector(Dim),
    ok = barrel_vectordb:add_vector(Store, Id, Text, #{}, Vector),
    {1, #{}}.

bench_add_batch_100(#{store := Store, dimension := Dim}) ->
    Docs = make_batch_docs(100),
    lists:foreach(fun({Id, Text, Meta}) ->
        Vector = random_vector(Dim),
        ok = barrel_vectordb:add_vector(Store, Id, Text, Meta, Vector)
    end, Docs),
    {100, #{}}.

bench_add_batch_1000(#{store := Store, dimension := Dim}) ->
    Docs = make_batch_docs(1000),
    lists:foreach(fun({Id, Text, Meta}) ->
        Vector = random_vector(Dim),
        ok = barrel_vectordb:add_vector(Store, Id, Text, Meta, Vector)
    end, Docs),
    {1000, #{}}.

bench_search_single_term(#{store := Store} = Ctx) ->
    ensure_indexed(Ctx),
    Query = <<"erlang">>,
    {ok, _Results} = barrel_vectordb:search_bm25(Store, Query, #{k => 10}),
    {1, #{}}.

bench_search_multi_term(#{store := Store} = Ctx) ->
    ensure_indexed(Ctx),
    Query = random_query(),
    {ok, _Results} = barrel_vectordb:search_bm25(Store, Query, #{k => 10}),
    {1, #{}}.

bench_search_many_terms(#{store := Store} = Ctx) ->
    ensure_indexed(Ctx),
    %% 10-20 term query (simulates real search)
    Query = <<"erlang programming language concurrent distributed fault tolerant systems beam virtual machine otp">>,
    {ok, _Results} = barrel_vectordb:search_bm25(Store, Query, #{k => 10}),
    {1, #{}}.

bench_search_k10(#{store := Store} = Ctx) ->
    ensure_indexed(Ctx),
    Query = random_query(),
    {ok, Results} = barrel_vectordb:search_bm25(Store, Query, #{k => 10}),
    {1, #{result_count => length(Results)}}.

bench_search_k50(#{store := Store} = Ctx) ->
    ensure_indexed(Ctx),
    Query = random_query(),
    {ok, Results} = barrel_vectordb:search_bm25(Store, Query, #{k => 50}),
    {1, #{result_count => length(Results)}}.

bench_hybrid_search(#{store := Store, dimension := Dim} = Ctx) ->
    ensure_indexed(Ctx),
    Query = random_query(),
    Vector = random_vector(Dim),
    %% Add a vector for hybrid search
    ok = barrel_vectordb:add_vector(Store, <<"hybrid_test">>, Query, #{}, Vector),
    case barrel_vectordb:search_hybrid(Store, Query, #{
        k => 10,
        bm25_weight => 0.5,
        vector_weight => 0.5,
        fusion => rrf
    }) of
        {ok, _Results} ->
            {1, #{}};
        {error, embedder_not_configured} ->
            %% Skip hybrid search if no embedder configured (expected in benchmarks)
            {1, #{skipped => true, reason => no_embedder}}
    end.

bench_large_index_search(#{store := Store, large_index_size := LargeSize} = Ctx) ->
    %% Build large index first
    ensure_large_indexed(Ctx, LargeSize),
    Query = random_query(),
    {ok, Results} = barrel_vectordb:search_bm25(Store, Query, #{k => 10}),
    {1, #{result_count => length(Results), index_size => LargeSize}}.

bench_compaction(#{store := Store, dimension := Dim, bm25_backend := disk} = Ctx) ->
    %% Add documents to hot layer
    ensure_indexed(Ctx),
    Docs = make_batch_docs(1000),
    lists:foreach(fun({Id, Text, Meta}) ->
        Vector = random_vector(Dim),
        ok = barrel_vectordb:add_vector(Store, Id, Text, Meta, Vector)
    end, Docs),
    %% Force compaction
    ok = barrel_vectordb_server:bm25_compact(Store),
    {1, #{}};
bench_compaction(#{bm25_backend := memory}) ->
    %% No compaction for memory backend
    {1, #{skipped => true}}.

%%====================================================================
%% Internal Functions
%%====================================================================

setup() ->
    application:ensure_all_started(rocksdb),
    ok.

cleanup(_) ->
    ok.

setup_bm25_store(Opts) ->
    Backend = maps:get(bm25_backend, Opts, disk),
    Dim = maps:get(dimension, Opts, 128),

    TestDir = "/tmp/barrel_bm25_bench_" ++
              integer_to_list(erlang:unique_integer([positive])),
    os:cmd("rm -rf " ++ TestDir),

    StoreName = list_to_atom("bm25_bench_" ++
                             integer_to_list(erlang:unique_integer([positive]))),

    StoreConfig = #{
        name => StoreName,
        path => TestDir,
        dimension => Dim,
        bm25_backend => Backend
    },

    %% Add backend-specific config
    FinalConfig = case Backend of
        memory ->
            StoreConfig#{bm25 => #{k1 => 1.2, b => 0.75}};
        disk ->
            StoreConfig#{bm25_disk => #{
                base_path => filename:join(TestDir, "bm25"),
                hot_max_size => 50000,
                k1 => 1.2,
                b => 0.75
            }}
    end,

    {ok, _Pid} = barrel_vectordb:start_link(FinalConfig),
    {StoreName, TestDir}.

cleanup_bm25_store(Store, TestDir) ->
    catch barrel_vectordb:stop(Store),
    timer:sleep(50),
    os:cmd("rm -rf " ++ TestDir),
    ok.

get_bench_fun(add_single) -> fun bench_add_single/1;
get_bench_fun(add_batch_100) -> fun bench_add_batch_100/1;
get_bench_fun(add_batch_1000) -> fun bench_add_batch_1000/1;
get_bench_fun(search_single_term) -> fun bench_search_single_term/1;
get_bench_fun(search_multi_term) -> fun bench_search_multi_term/1;
get_bench_fun(search_many_terms) -> fun bench_search_many_terms/1;
get_bench_fun(search_k10) -> fun bench_search_k10/1;
get_bench_fun(search_k50) -> fun bench_search_k50/1;
get_bench_fun(hybrid_search) -> fun bench_hybrid_search/1;
get_bench_fun(large_index_search) -> fun bench_large_index_search/1;
get_bench_fun(compaction) -> fun bench_compaction/1.

run_iterations(BenchFun, Store, Iterations, Opts) ->
    IndexSize = maps:get(index_size, Opts, 1000),
    LargeIndexSize = maps:get(large_index_size, Opts, 10000),
    Backend = maps:get(bm25_backend, Opts, disk),
    Dim = maps:get(dimension, Opts, 128),
    Ctx = #{
        store => Store,
        index_size => IndexSize,
        large_index_size => LargeIndexSize,
        bm25_backend => Backend,
        dimension => Dim,
        indexed => false,
        large_indexed => false
    },
    run_iterations(BenchFun, Ctx, Iterations, [], [], []).

run_iterations(_BenchFun, _Ctx, 0, Timings, OpCounts, Metrics) ->
    {lists:reverse(Timings), lists:reverse(OpCounts), Metrics};
run_iterations(BenchFun, Ctx, N, Timings, OpCounts, Metrics) ->
    {Time, {OpCount, IterMetrics}} = timer:tc(fun() -> BenchFun(Ctx) end),
    run_iterations(BenchFun, Ctx#{indexed => true}, N - 1,
                   [Time | Timings], [OpCount | OpCounts], [IterMetrics | Metrics]).

calculate_stats(Name, Backend, Timings, OpCounts, _Metrics, _Opts) ->
    TimingsMs = [T / 1000 || T <- Timings],
    Sorted = lists:sort(TimingsMs),

    TotalOps = lists:sum(OpCounts),
    TotalTimeMs = lists:sum(TimingsMs),
    TotalTimeSec = max(0.001, TotalTimeMs / 1000),

    Len = length(Sorted),

    #{
        name => Name,
        backend => Backend,
        iterations => Len,
        total_ops => TotalOps,
        total_time_ms => TotalTimeMs,

        ops_per_sec => TotalOps / TotalTimeSec,

        min_ms => lists:min(Sorted),
        max_ms => lists:max(Sorted),
        mean_ms => TotalTimeMs / max(1, Len),

        p50_ms => percentile(Sorted, 50),
        p95_ms => percentile(Sorted, 95),
        p99_ms => percentile(Sorted, 99)
    }.

percentile([], _P) -> 0.0;
percentile(Sorted, P) ->
    Len = length(Sorted),
    Index = max(1, min(Len, round(Len * P / 100))),
    lists:nth(Index, Sorted).

print_stats(Stats) ->
    #{
        name := Name,
        backend := Backend,
        iterations := Iters,
        ops_per_sec := OpsPerSec,
        mean_ms := Mean,
        p50_ms := P50,
        p95_ms := P95,
        p99_ms := P99
    } = Stats,

    io:format("~n--- ~p (~p) ---~n", [Name, Backend]),
    io:format("  Iterations:  ~p~n", [Iters]),
    io:format("  Throughput:  ~.1f ops/sec~n", [OpsPerSec]),
    io:format("  Latency:~n"),
    io:format("    Mean:  ~.3f ms~n", [Mean]),
    io:format("    P50:   ~.3f ms~n", [P50]),
    io:format("    P95:   ~.3f ms~n", [P95]),
    io:format("    P99:   ~.3f ms~n", [P99]),

    %% Check P99 target
    case P99 < 10.0 of
        true -> io:format("    [PASS] P99 < 10ms target~n");
        false -> io:format("    [WARN] P99 >= 10ms target~n")
    end.

print_summary(Results) ->
    io:format("~n========================================~n"),
    io:format("Summary~n"),
    io:format("========================================~n"),
    io:format("~-25s ~10s ~10s ~10s~n", ["Benchmark", "ops/sec", "P50 ms", "P99 ms"]),
    io:format("~s~n", [lists:duplicate(60, $-)]),

    lists:foreach(fun(#{name := Name, ops_per_sec := Ops, p50_ms := P50, p99_ms := P99}) ->
        io:format("~-25s ~10.1f ~10.3f ~10.3f~n", [Name, Ops, P50, P99])
    end, Results).

print_comparison(MemoryResults, DiskResults) ->
    io:format("~n========================================~n"),
    io:format("Backend Comparison (Memory vs Disk)~n"),
    io:format("========================================~n"),
    io:format("~-20s ~12s ~12s ~10s~n", ["Benchmark", "Memory", "Disk", "Diff"]),
    io:format("~s~n", [lists:duplicate(60, $-)]),

    %% Pair results by name
    lists:foreach(fun(MemResult) ->
        Name = maps:get(name, MemResult),
        case lists:keyfind(Name, 1, [{maps:get(name, R), R} || R <- DiskResults]) of
            {_, DiskResult} ->
                MemOps = maps:get(ops_per_sec, MemResult),
                DiskOps = maps:get(ops_per_sec, DiskResult),
                Diff = case MemOps of
                    N when N == 0.0 -> 0.0;
                    _ -> (DiskOps - MemOps) / MemOps * 100
                end,
                DiffStr = case Diff >= 0 of
                    true -> io_lib:format("+~.1f%", [Diff]);
                    false -> io_lib:format("~.1f%", [Diff])
                end,
                io:format("~-20s ~10.1f ~10.1f ~10s~n",
                          [Name, MemOps, DiskOps, DiffStr]);
            false ->
                ok
        end
    end, MemoryResults).

%% Helper functions

make_id() ->
    list_to_binary("doc_" ++ integer_to_list(erlang:unique_integer([positive]))).

random_text() ->
    Texts = ?SAMPLE_TEXTS,
    lists:nth(rand:uniform(length(Texts)), Texts).

random_query() ->
    Queries = ?SAMPLE_QUERIES,
    lists:nth(rand:uniform(length(Queries)), Queries).

random_vector(Dim) ->
    Vec = [rand:uniform() - 0.5 || _ <- lists:seq(1, Dim)],
    normalize(Vec).

normalize(Vec) ->
    Norm = math:sqrt(lists:sum([X*X || X <- Vec])),
    case Norm < 1.0e-10 of
        true -> Vec;
        false -> [X / Norm || X <- Vec]
    end.

make_batch_docs(Count) ->
    [{list_to_binary("doc_" ++ integer_to_list(I)),
      generate_text(I),
      #{index => I}}
     || I <- lists:seq(1, Count)].

generate_text(I) ->
    %% Generate varied text for realistic BM25 scoring
    Base = lists:nth((I rem 10) + 1, ?SAMPLE_TEXTS),
    Suffix = list_to_binary(" Document number " ++ integer_to_list(I)),
    <<Base/binary, Suffix/binary>>.

ensure_indexed(#{indexed := true}) ->
    ok;
ensure_indexed(#{store := Store, index_size := Size, dimension := Dim}) ->
    case barrel_vectordb:count(Store) >= Size of
        true -> ok;
        false ->
            Docs = make_batch_docs(Size),
            lists:foreach(fun({Id, Text, Meta}) ->
                %% Use add_vector with random vector for BM25 benchmarks
                %% This avoids needing an embedder
                Vector = random_vector(Dim),
                ok = barrel_vectordb:add_vector(Store, Id, Text, Meta, Vector)
            end, Docs),
            ok
    end.

ensure_large_indexed(#{large_indexed := true}, _Size) ->
    ok;
ensure_large_indexed(#{store := Store, dimension := Dim}, Size) ->
    case barrel_vectordb:count(Store) >= Size of
        true -> ok;
        false ->
            %% Add in batches to avoid memory issues
            BatchSize = 1000,
            NumBatches = (Size + BatchSize - 1) div BatchSize,
            lists:foreach(fun(Batch) ->
                Start = (Batch - 1) * BatchSize + 1,
                End = min(Batch * BatchSize, Size),
                Docs = [{list_to_binary("large_" ++ integer_to_list(I)),
                         generate_text(I),
                         #{index => I}}
                        || I <- lists:seq(Start, End)],
                lists:foreach(fun({Id, Text, Meta}) ->
                    Vector = random_vector(Dim),
                    ok = barrel_vectordb:add_vector(Store, Id, Text, Meta, Vector)
                end, Docs),
                io:format("  Indexed ~p/~p docs~n", [End, Size])
            end, lists:seq(1, NumBatches)),
            ok
    end.
