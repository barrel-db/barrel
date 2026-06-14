%%%-------------------------------------------------------------------
%%% @doc Benchmark runner for barrel_vectordb
%%%
%%% Provides infrastructure for running performance benchmarks and
%%% collecting metrics including throughput, latency percentiles,
%%% and memory usage.
%%%
%%% Usage:
%%%   rebar3 as bench eunit --module=barrel_vectordb_bench
%%%
%%% Or programmatically:
%%%   barrel_vectordb_bench:run_all().
%%%   barrel_vectordb_bench:run(insert_single).
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_bench).

-include_lib("eunit/include/eunit.hrl").

-export([
    run_all/0,
    run_all/1,
    run/1,
    run/2
]).

%% Benchmark definitions
-export([
    bench_insert_single/1,
    bench_insert_batch_10/1,
    bench_insert_batch_100/1,
    bench_insert_batch_1000/1,
    bench_search_k1/1,
    bench_search_k10/1,
    bench_search_k50/1,
    bench_search_filtered/1,
    bench_index_build_1k/1,
    bench_index_build_10k/1,
    bench_get_single/1,
    bench_delete_single/1,
    bench_concurrent_writers/1
]).

-define(DEFAULT_OPTS, #{
    warmup_iterations => 10,
    iterations => 100,
    dimension => 128,
    index_size => 500,         % Size of index for search benchmarks
    output_format => console,  % console | json | csv | all
    output_file => "bench_results"
}).

%%====================================================================
%% EUnit Test Generator
%%====================================================================

%% Smoke test - just verify benchmark infrastructure works
%% For full benchmarks, use: rebar3 as bench shell --eval "barrel_vectordb_bench:run_all()."
benchmark_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
        {"benchmark infrastructure smoke test",
         {timeout, 60, fun() ->
             %% Just run a single quick benchmark to verify everything works
             {ok, Result} = run(insert_single, #{
                 iterations => 5,
                 warmup_iterations => 2,
                 dimension => 32
             }),
             ?assert(maps:get(ops_per_sec, Result) > 0),
             ?assert(maps:get(mean_ms, Result) > 0)
         end}}
     ]}.

%%====================================================================
%% Public API
%%====================================================================

%% @doc Run all benchmarks with default options
-spec run_all() -> {ok, [map()]}.
run_all() ->
    run_all(?DEFAULT_OPTS).

%% @doc Run all benchmarks with custom options
-spec run_all(map()) -> {ok, [map()]}.
run_all(Opts) ->
    MergedOpts = maps:merge(?DEFAULT_OPTS, Opts),
    %% Default benchmark set (reasonable runtime)
    Benchmarks = case maps:get(full, MergedOpts, false) of
        true ->
            %% Full benchmark suite including large index builds
            [
                insert_single,
                insert_batch_10,
                insert_batch_100,
                insert_batch_1000,
                search_k1,
                search_k10,
                search_k50,
                search_filtered,
                get_single,
                delete_single,
                index_build_1k,
                index_build_10k
            ];
        false ->
            %% Standard benchmarks (faster)
            [
                insert_single,
                insert_batch_10,
                insert_batch_100,
                search_k1,
                search_k10,
                search_k50,
                search_filtered,
                get_single,
                delete_single,
                index_build_1k,
                concurrent_writers
            ]
    end,
    Results = lists:map(fun(Name) ->
        {ok, Result} = run(Name, MergedOpts),
        Result
    end, Benchmarks),

    %% Export results
    export_results(Results, MergedOpts),
    {ok, Results}.

%% @doc Run a single benchmark with default options
-spec run(atom()) -> {ok, map()}.
run(Name) ->
    run(Name, ?DEFAULT_OPTS).

%% @doc Run a single benchmark with custom options
-spec run(atom(), map()) -> {ok, map()}.
run(Name, Opts) ->
    MergedOpts = maps:merge(?DEFAULT_OPTS, Opts),
    io:format("~n=== Running benchmark: ~p ===~n", [Name]),

    %% Setup benchmark environment
    {Store, TestDir} = setup_bench_store(MergedOpts),

    try
        %% Get benchmark function
        BenchFun = get_bench_fun(Name),

        %% Run warmup (keep store warm for actual run)
        WarmupIters = maps:get(warmup_iterations, MergedOpts),
        io:format("Warmup: ~p iterations...~n", [WarmupIters]),
        _ = run_iterations(BenchFun, Store, WarmupIters, MergedOpts),

        %% Run actual benchmark (reuse warm store - don't reset!)
        Iterations = maps:get(iterations, MergedOpts),
        io:format("Running: ~p iterations...~n", [Iterations]),
        {Timings, OpCounts} = run_iterations(BenchFun, Store, Iterations, MergedOpts),

        %% Calculate statistics
        Stats = calculate_stats(Name, Timings, OpCounts, MergedOpts),
        print_stats(Stats),

        cleanup_bench_store(Store, TestDir),
        {ok, Stats}
    catch
        E:R:ST ->
            cleanup_bench_store(Store, TestDir),
            io:format("Benchmark ~p failed: ~p:~p~n~p~n", [Name, E, R, ST]),
            {error, {E, R}}
    end.

%%====================================================================
%% Benchmark Implementations
%%====================================================================

bench_insert_single(#{store := Store, dimension := Dim}) ->
    Id = make_id(),
    Vector = random_vector(Dim),
    ok = barrel_vectordb:add_vector(Store, Id, <<"benchmark text">>, #{}, Vector),
    1.  % Return operation count

bench_insert_batch_10(#{store := Store, dimension := Dim}) ->
    Docs = make_batch_docs(10, Dim),
    add_vector_batch(Store, Docs),
    10.

bench_insert_batch_100(#{store := Store, dimension := Dim}) ->
    Docs = make_batch_docs(100, Dim),
    add_vector_batch(Store, Docs),
    100.

bench_insert_batch_1000(#{store := Store, dimension := Dim}) ->
    Docs = make_batch_docs(1000, Dim),
    add_vector_batch(Store, Docs),
    1000.

bench_search_k1(#{store := Store, dimension := Dim, index_size := IndexSize} = Ctx) ->
    ensure_indexed(Ctx, IndexSize),
    Query = random_vector(Dim),
    {ok, _Results} = barrel_vectordb:search_vector(Store, Query, #{k => 1}),
    1.

bench_search_k10(#{store := Store, dimension := Dim, index_size := IndexSize} = Ctx) ->
    ensure_indexed(Ctx, IndexSize),
    Query = random_vector(Dim),
    {ok, _Results} = barrel_vectordb:search_vector(Store, Query, #{k => 10}),
    1.

bench_search_k50(#{store := Store, dimension := Dim, index_size := IndexSize} = Ctx) ->
    ensure_indexed(Ctx, IndexSize),
    Query = random_vector(Dim),
    {ok, _Results} = barrel_vectordb:search_vector(Store, Query, #{k => 50}),
    1.

bench_search_filtered(#{store := Store, dimension := Dim, index_size := IndexSize} = Ctx) ->
    ensure_indexed(Ctx, IndexSize),
    Query = random_vector(Dim),
    Filter = fun(Meta) -> maps:get(even, Meta, false) end,
    {ok, _Results} = barrel_vectordb:search_vector(Store, Query, #{k => 10, filter => Filter}),
    1.

bench_get_single(#{store := Store, index_size := IndexSize} = Ctx) ->
    Size = min(100, IndexSize),
    ensure_indexed(Ctx, Size),
    %% Get a random existing key
    Key = list_to_binary("bench_" ++ integer_to_list(rand:uniform(Size))),
    _ = barrel_vectordb:get(Store, Key),
    1.

bench_delete_single(#{store := Store, dimension := Dim}) ->
    %% Insert then delete
    Id = make_id(),
    Vector = random_vector(Dim),
    ok = barrel_vectordb:add_vector(Store, Id, <<"to delete">>, #{}, Vector),
    ok = barrel_vectordb:delete(Store, Id),
    1.

%% Index build benchmarks measure time to build a fresh index
%% Each iteration creates a new store to measure cold-start performance
bench_index_build_1k(#{dimension := Dim}) ->
    {Store, TestDir} = setup_bench_store(#{dimension => Dim}),
    try
        Docs = make_batch_docs(1000, Dim),
        add_vector_batch(Store, Docs),
        1000
    after
        cleanup_bench_store(Store, TestDir)
    end.

bench_index_build_10k(#{dimension := Dim}) ->
    {Store, TestDir} = setup_bench_store(#{dimension => Dim}),
    try
        Docs = make_batch_docs(10000, Dim),
        add_vector_batch(Store, Docs),
        10000
    after
        cleanup_bench_store(Store, TestDir)
    end.

%% Concurrent writers benchmark - measures throughput under concurrent load
%% This tests how well gen_batch_server batches concurrent writes
bench_concurrent_writers(#{dimension := Dim}) ->
    NumWriters = 10,
    DocsPerWriter = 100,
    TotalDocs = NumWriters * DocsPerWriter,

    {Store, TestDir} = setup_bench_store(#{dimension => Dim}),
    try
        Self = self(),

        %% Spawn writers
        _Pids = [spawn_link(fun() ->
            lists:foreach(fun(N) ->
                Id = iolist_to_binary([<<"cw_">>, integer_to_binary(W),
                                       <<"_">>, integer_to_binary(N)]),
                Vec = random_vector(Dim),
                ok = barrel_vectordb:add_vector(Store, Id, <<"concurrent text">>, #{}, Vec)
            end, lists:seq(1, DocsPerWriter)),
            Self ! {done, W}
        end) || W <- lists:seq(1, NumWriters)],

        %% Wait for all writers to complete
        lists:foreach(fun(_) ->
            receive {done, _} -> ok end
        end, lists:seq(1, NumWriters)),

        TotalDocs
    after
        cleanup_bench_store(Store, TestDir)
    end.

%%====================================================================
%% Internal Functions
%%====================================================================

setup() ->
    application:ensure_all_started(rocksdb),
    ok.

cleanup(_) ->
    ok.

setup_bench_store(Opts) ->
    Dim = maps:get(dimension, Opts),
    %% Use unique directory for each store instance
    TestDir = "/tmp/barrel_vectordb_bench_" ++
              integer_to_list(erlang:unique_integer([positive])),
    %% Ensure clean directory
    os:cmd("rm -rf " ++ TestDir),

    %% Use unique store name to avoid conflicts
    StoreName = list_to_atom("bench_store_" ++
                             integer_to_list(erlang:unique_integer([positive]))),

    {ok, _Pid} = barrel_vectordb:start_link(#{
        name => StoreName,
        path => TestDir,
        dimension => Dim,
        hnsw => #{m => 16, ef_construction => 100}
    }),
    {StoreName, TestDir}.

cleanup_bench_store(Store, TestDir) ->
    catch barrel_vectordb:stop(Store),
    timer:sleep(50),
    os:cmd("rm -rf " ++ TestDir),
    ok.

get_bench_fun(insert_single) -> fun bench_insert_single/1;
get_bench_fun(insert_batch_10) -> fun bench_insert_batch_10/1;
get_bench_fun(insert_batch_100) -> fun bench_insert_batch_100/1;
get_bench_fun(insert_batch_1000) -> fun bench_insert_batch_1000/1;
get_bench_fun(search_k1) -> fun bench_search_k1/1;
get_bench_fun(search_k10) -> fun bench_search_k10/1;
get_bench_fun(search_k50) -> fun bench_search_k50/1;
get_bench_fun(search_filtered) -> fun bench_search_filtered/1;
get_bench_fun(get_single) -> fun bench_get_single/1;
get_bench_fun(delete_single) -> fun bench_delete_single/1;
get_bench_fun(index_build_1k) -> fun bench_index_build_1k/1;
get_bench_fun(index_build_10k) -> fun bench_index_build_10k/1;
get_bench_fun(concurrent_writers) -> fun bench_concurrent_writers/1.

run_iterations(BenchFun, Store, Iterations, Opts) ->
    Dim = maps:get(dimension, Opts),
    IndexSize = maps:get(index_size, Opts, 500),
    Ctx = #{store => Store, dimension => Dim, index_size => IndexSize, indexed => false},
    run_iterations(BenchFun, Ctx, Iterations, [], []).

run_iterations(_BenchFun, _Ctx, 0, Timings, OpCounts) ->
    {lists:reverse(Timings), lists:reverse(OpCounts)};
run_iterations(BenchFun, Ctx, N, Timings, OpCounts) ->
    {Time, OpCount} = timer:tc(fun() -> BenchFun(Ctx) end),
    run_iterations(BenchFun, Ctx#{indexed => true}, N - 1, [Time | Timings], [OpCount | OpCounts]).

calculate_stats(Name, Timings, OpCounts, _Opts) ->
    %% Convert microseconds to milliseconds
    TimingsMs = [T / 1000 || T <- Timings],
    Sorted = lists:sort(TimingsMs),

    TotalOps = lists:sum(OpCounts),
    TotalTimeMs = lists:sum(TimingsMs),
    TotalTimeSec = TotalTimeMs / 1000,

    Len = length(Sorted),

    #{
        name => Name,
        iterations => Len,
        total_ops => TotalOps,
        total_time_ms => TotalTimeMs,

        %% Throughput
        ops_per_sec => TotalOps / TotalTimeSec,

        %% Latency stats (in milliseconds)
        min_ms => lists:min(Sorted),
        max_ms => lists:max(Sorted),
        mean_ms => TotalTimeMs / Len,
        median_ms => percentile(Sorted, 50),

        %% Percentiles
        p50_ms => percentile(Sorted, 50),
        p75_ms => percentile(Sorted, 75),
        p90_ms => percentile(Sorted, 90),
        p95_ms => percentile(Sorted, 95),
        p99_ms => percentile(Sorted, 99),

        %% Standard deviation
        stddev_ms => stddev(TimingsMs),

        %% Timestamp
        timestamp => calendar:universal_time()
    }.

percentile(Sorted, P) ->
    Len = length(Sorted),
    Index = max(1, min(Len, round(Len * P / 100))),
    lists:nth(Index, Sorted).

stddev(Values) ->
    Len = length(Values),
    Mean = lists:sum(Values) / Len,
    Variance = lists:sum([(V - Mean) * (V - Mean) || V <- Values]) / Len,
    math:sqrt(Variance).

print_stats(Stats) ->
    #{
        name := Name,
        iterations := Iters,
        total_ops := TotalOps,
        ops_per_sec := OpsPerSec,
        mean_ms := Mean,
        p50_ms := P50,
        p95_ms := P95,
        p99_ms := P99,
        min_ms := Min,
        max_ms := Max
    } = Stats,

    io:format("~n--- Results for ~p ---~n", [Name]),
    io:format("  Iterations:    ~p~n", [Iters]),
    io:format("  Total ops:     ~p~n", [TotalOps]),
    io:format("  Throughput:    ~.2f ops/sec~n", [OpsPerSec]),
    io:format("  Latency:~n"),
    io:format("    Mean:        ~.3f ms~n", [Mean]),
    io:format("    P50:         ~.3f ms~n", [P50]),
    io:format("    P95:         ~.3f ms~n", [P95]),
    io:format("    P99:         ~.3f ms~n", [P99]),
    io:format("    Min:         ~.3f ms~n", [Min]),
    io:format("    Max:         ~.3f ms~n", [Max]),
    io:format("~n").

export_results(Results, Opts) ->
    Format = maps:get(output_format, Opts),
    BaseFile = maps:get(output_file, Opts),

    case Format of
        console -> ok;
        json -> export_json(Results, BaseFile ++ ".json");
        csv -> export_csv(Results, BaseFile ++ ".csv");
        all ->
            export_json(Results, BaseFile ++ ".json"),
            export_csv(Results, BaseFile ++ ".csv")
    end.

export_json(Results, Filename) ->
    %% Convert to JSON-friendly format
    JsonResults = lists:map(fun(R) ->
        R#{
            name => atom_to_binary(maps:get(name, R), utf8),
            timestamp => iolist_to_binary(format_timestamp(maps:get(timestamp, R)))
        }
    end, Results),

    Data = #{
        benchmark_run => #{
            timestamp => iolist_to_binary(format_timestamp(calendar:universal_time())),
            results => JsonResults
        }
    },

    Json = json:encode(Data),
    file:write_file(Filename, Json),
    io:format("Results exported to: ~s~n", [Filename]).

export_csv(Results, Filename) ->
    Header = "name,iterations,total_ops,ops_per_sec,mean_ms,p50_ms,p95_ms,p99_ms,min_ms,max_ms,timestamp\n",

    Rows = lists:map(fun(R) ->
        io_lib:format("~s,~p,~p,~.2f,~.3f,~.3f,~.3f,~.3f,~.3f,~.3f,~s~n", [
            maps:get(name, R),
            maps:get(iterations, R),
            maps:get(total_ops, R),
            maps:get(ops_per_sec, R),
            maps:get(mean_ms, R),
            maps:get(p50_ms, R),
            maps:get(p95_ms, R),
            maps:get(p99_ms, R),
            maps:get(min_ms, R),
            maps:get(max_ms, R),
            format_timestamp(maps:get(timestamp, R))
        ])
    end, Results),

    file:write_file(Filename, [Header | Rows]),
    io:format("Results exported to: ~s~n", [Filename]).

format_timestamp({{Y, M, D}, {H, Mi, S}}) ->
    io_lib:format("~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0wZ", [Y, M, D, H, Mi, S]).

%% Helper functions
make_id() ->
    list_to_binary("bench_" ++ integer_to_list(erlang:unique_integer([positive]))).

random_vector(Dim) ->
    Vec = [rand:uniform() - 0.5 || _ <- lists:seq(1, Dim)],
    normalize(Vec).

normalize(Vec) ->
    Norm = math:sqrt(lists:sum([X*X || X <- Vec])),
    case Norm < 1.0e-10 of
        true -> Vec;
        false -> [X / Norm || X <- Vec]
    end.

make_batch_docs(Count, Dim) ->
    [{list_to_binary("bench_" ++ integer_to_list(I)),
      list_to_binary("Benchmark document " ++ integer_to_list(I)),
      #{index => I, even => (I rem 2 =:= 0)},
      random_vector(Dim)}
     || I <- lists:seq(1, Count)].

%% Ensure store has indexed data for search benchmarks
ensure_indexed(#{indexed := true}, _Count) ->
    ok;
ensure_indexed(#{store := Store, dimension := Dim}, Count) ->
    case barrel_vectordb:count(Store) >= Count of
        true -> ok;
        false ->
            Docs = make_batch_docs(Count, Dim),
            add_vector_batch(Store, Docs),
            ok
    end.

%% Add a batch of vectors using the bulk insert API
add_vector_batch(Store, Docs) ->
    {ok, _} = barrel_vectordb:add_vector_batch(Store, Docs),
    ok.
