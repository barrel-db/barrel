%%%-------------------------------------------------------------------
%%% @doc Backend comparison benchmarks for barrel_vectordb
%%%
%%% Compares performance between HNSW (pure Erlang) and FAISS (NIF) backends.
%%%
%%% Usage:
%%%   rebar3 as bench,test_faiss shell
%%%   barrel_vectordb_backend_bench:run_all().
%%%   barrel_vectordb_backend_bench:run(insert_single).
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_backend_bench).

-export([
    run_all/0,
    run_all/1,
    run/1,
    run/2,
    compare_report/2
]).

-define(DEFAULT_OPTS, #{
    warmup_iterations => 5,
    iterations => 50,
    dimension => 128,
    index_size => 500
}).

%%====================================================================
%% Public API
%%====================================================================

%% @doc Run all comparison benchmarks
-spec run_all() -> {ok, map()}.
run_all() ->
    run_all(?DEFAULT_OPTS).

%% @doc Run all comparison benchmarks with custom options
-spec run_all(map()) -> {ok, map()}.
run_all(Opts) ->
    MergedOpts = maps:merge(?DEFAULT_OPTS, Opts),

    Benchmarks = [
        insert_single,
        insert_batch_100,
        search_k1,
        search_k10,
        search_k50,
        delete_single,
        index_build_1k
    ],

    io:format("~n"),
    io:format("================================================~n"),
    io:format("    HNSW vs FAISS Backend Comparison~n"),
    io:format("================================================~n"),
    io:format("Dimension: ~p~n", [maps:get(dimension, MergedOpts)]),
    io:format("Iterations: ~p (warmup: ~p)~n", [
        maps:get(iterations, MergedOpts),
        maps:get(warmup_iterations, MergedOpts)
    ]),
    io:format("Index size: ~p vectors~n", [maps:get(index_size, MergedOpts)]),
    io:format("================================================~n"),

    Results = lists:map(fun(Name) ->
        {ok, Result} = run(Name, MergedOpts),
        Result
    end, Benchmarks),

    print_summary(Results),
    {ok, #{benchmarks => Results}}.

%% @doc Run a single comparison benchmark
-spec run(atom()) -> {ok, map()}.
run(Name) ->
    run(Name, ?DEFAULT_OPTS).

%% @doc Run a single comparison benchmark with options
-spec run(atom(), map()) -> {ok, map()}.
run(Name, Opts) ->
    MergedOpts = maps:merge(?DEFAULT_OPTS, Opts),

    io:format("~n--- ~p ---~n", [Name]),

    %% Check FAISS availability
    FaissAvailable = barrel_vectordb_index:is_available(faiss),

    %% Run HNSW benchmark
    io:format("  HNSW:  "),
    HnswResult = run_backend_bench(Name, hnsw, MergedOpts),
    io:format("~.2f ops/sec (p95: ~.3f ms)~n", [
        maps:get(ops_per_sec, HnswResult),
        maps:get(p95_ms, HnswResult)
    ]),

    %% Run FAISS benchmark if available
    FaissResult = case FaissAvailable of
        true ->
            io:format("  FAISS: "),
            R = run_backend_bench(Name, faiss, MergedOpts),
            io:format("~.2f ops/sec (p95: ~.3f ms)~n", [
                maps:get(ops_per_sec, R),
                maps:get(p95_ms, R)
            ]),
            R;
        false ->
            io:format("  FAISS: [not available]~n"),
            undefined
    end,

    %% Calculate comparison
    Comparison = case FaissResult of
        undefined ->
            #{speedup => undefined, latency_ratio => undefined};
        _ ->
            HnswOps = maps:get(ops_per_sec, HnswResult),
            FaissOps = maps:get(ops_per_sec, FaissResult),
            HnswP95 = maps:get(p95_ms, HnswResult),
            FaissP95 = maps:get(p95_ms, FaissResult),
            #{
                speedup => FaissOps / HnswOps,
                latency_ratio => HnswP95 / FaissP95
            }
    end,

    %% Print comparison
    case maps:get(speedup, Comparison) of
        undefined -> ok;
        Speedup when Speedup > 1.0 ->
            io:format("  -> FAISS ~.2fx faster~n", [Speedup]);
        Speedup when Speedup < 1.0 ->
            io:format("  -> HNSW ~.2fx faster~n", [1.0 / Speedup]);
        _ ->
            io:format("  -> Similar performance~n")
    end,

    {ok, #{
        name => Name,
        hnsw => HnswResult,
        faiss => FaissResult,
        comparison => Comparison
    }}.

%% @doc Generate detailed comparison report
-spec compare_report(map(), map()) -> ok.
compare_report(HnswResults, FaissResults) ->
    io:format("~n"),
    io:format("==========================================================~n"),
    io:format("              DETAILED COMPARISON REPORT~n"),
    io:format("==========================================================~n~n"),

    io:format("~-20s | ~-15s | ~-15s | ~-10s~n",
              ["Benchmark", "HNSW (ops/s)", "FAISS (ops/s)", "Speedup"]),
    io:format("~s~n", [lists:duplicate(65, $-)]),

    HnswList = maps:get(benchmarks, HnswResults, []),
    FaissList = maps:get(benchmarks, FaissResults, []),

    lists:foreach(fun(HnswBench) ->
        Name = maps:get(name, HnswBench),
        HnswOps = maps:get(ops_per_sec, maps:get(hnsw, HnswBench)),

        FaissOps = case find_bench(Name, FaissList) of
            {ok, FaissBench} ->
                case maps:get(faiss, FaissBench) of
                    undefined -> undefined;
                    F -> maps:get(ops_per_sec, F)
                end;
            not_found -> undefined
        end,

        Speedup = case FaissOps of
            undefined -> "N/A";
            _ -> io_lib:format("~.2fx", [FaissOps / HnswOps])
        end,

        io:format("~-20s | ~-15.2f | ~-15s | ~-10s~n", [
            Name,
            HnswOps,
            case FaissOps of undefined -> "N/A"; _ -> io_lib:format("~.2f", [FaissOps]) end,
            Speedup
        ])
    end, HnswList),
    ok.

%%====================================================================
%% Internal Functions
%%====================================================================

run_backend_bench(Name, Backend, Opts) ->
    Dim = maps:get(dimension, Opts),
    IndexSize = maps:get(index_size, Opts),
    WarmupIters = maps:get(warmup_iterations, Opts),
    Iterations = maps:get(iterations, Opts),

    %% Setup store with specified backend
    {Store, TestDir} = setup_store(Backend, Dim),

    try
        %% Get benchmark function
        BenchFun = get_bench_fun(Name),
        Ctx = #{
            store => Store,
            dimension => Dim,
            index_size => IndexSize,
            backend => Backend,
            indexed => false
        },

        %% Warmup
        _ = run_iterations(BenchFun, Ctx, WarmupIters),

        %% Run benchmark
        {Timings, OpCounts} = run_iterations(BenchFun, Ctx#{indexed => true}, Iterations),

        %% Calculate stats
        calculate_stats(Timings, OpCounts)
    after
        cleanup_store(Store, TestDir)
    end.

setup_store(Backend, Dim) ->
    TestDir = "/tmp/barrel_backend_bench_" ++
              integer_to_list(erlang:unique_integer([positive])),
    os:cmd("rm -rf " ++ TestDir),

    StoreName = list_to_atom("backend_bench_" ++
                             integer_to_list(erlang:unique_integer([positive]))),

    Config = #{
        name => StoreName,
        path => TestDir,
        dimension => Dim,
        backend => Backend
    },

    {ok, _Pid} = barrel_vectordb:start_link(Config),
    {StoreName, TestDir}.

cleanup_store(Store, TestDir) ->
    catch barrel_vectordb:stop(Store),
    timer:sleep(50),
    os:cmd("rm -rf " ++ TestDir),
    ok.

get_bench_fun(insert_single) -> fun bench_insert_single/1;
get_bench_fun(insert_batch_100) -> fun bench_insert_batch/1;
get_bench_fun(search_k1) -> fun bench_search_k1/1;
get_bench_fun(search_k10) -> fun bench_search_k10/1;
get_bench_fun(search_k50) -> fun bench_search_k50/1;
get_bench_fun(delete_single) -> fun bench_delete/1;
get_bench_fun(index_build_1k) -> fun bench_index_build/1.

bench_insert_single(#{store := Store, dimension := Dim}) ->
    Id = make_id(),
    Vector = random_vector(Dim),
    ok = barrel_vectordb:add_vector(Store, Id, <<"bench">>, #{}, Vector),
    1.

bench_insert_batch(#{store := Store, dimension := Dim}) ->
    Docs = make_batch_docs(100, Dim),
    {ok, _} = barrel_vectordb:add_vector_batch(Store, Docs),
    100.

bench_search_k1(Ctx) ->
    bench_search(Ctx, 1).

bench_search_k10(Ctx) ->
    bench_search(Ctx, 10).

bench_search_k50(Ctx) ->
    bench_search(Ctx, 50).

bench_search(#{store := Store, dimension := Dim, index_size := IndexSize} = Ctx, K) ->
    ensure_indexed(Ctx, IndexSize),
    Query = random_vector(Dim),
    {ok, _Results} = barrel_vectordb:search_vector(Store, Query, #{k => K}),
    1.

bench_delete(#{store := Store, dimension := Dim}) ->
    Id = make_id(),
    Vector = random_vector(Dim),
    ok = barrel_vectordb:add_vector(Store, Id, <<"to delete">>, #{}, Vector),
    ok = barrel_vectordb:delete(Store, Id),
    1.

bench_index_build(#{dimension := Dim, backend := Backend}) ->
    {Store, TestDir} = setup_store(Backend, Dim),
    try
        Docs = make_batch_docs(1000, Dim),
        {ok, _} = barrel_vectordb:add_vector_batch(Store, Docs),
        1000
    after
        cleanup_store(Store, TestDir)
    end.

run_iterations(BenchFun, Ctx, Iterations) ->
    run_iterations(BenchFun, Ctx, Iterations, [], []).

run_iterations(_BenchFun, _Ctx, 0, Timings, OpCounts) ->
    {lists:reverse(Timings), lists:reverse(OpCounts)};
run_iterations(BenchFun, Ctx, N, Timings, OpCounts) ->
    {Time, OpCount} = timer:tc(fun() -> BenchFun(Ctx) end),
    run_iterations(BenchFun, Ctx#{indexed => true}, N - 1, [Time | Timings], [OpCount | OpCounts]).

calculate_stats(Timings, OpCounts) ->
    TimingsMs = [T / 1000 || T <- Timings],
    Sorted = lists:sort(TimingsMs),

    TotalOps = lists:sum(OpCounts),
    TotalTimeMs = lists:sum(TimingsMs),
    TotalTimeSec = TotalTimeMs / 1000,
    Len = length(Sorted),

    #{
        iterations => Len,
        total_ops => TotalOps,
        ops_per_sec => TotalOps / TotalTimeSec,
        mean_ms => TotalTimeMs / Len,
        p50_ms => percentile(Sorted, 50),
        p95_ms => percentile(Sorted, 95),
        p99_ms => percentile(Sorted, 99),
        min_ms => lists:min(Sorted),
        max_ms => lists:max(Sorted)
    }.

percentile(Sorted, P) ->
    Len = length(Sorted),
    Index = max(1, min(Len, round(Len * P / 100))),
    lists:nth(Index, Sorted).

ensure_indexed(#{indexed := true}, _Count) ->
    ok;
ensure_indexed(#{store := Store, dimension := Dim}, Count) ->
    case barrel_vectordb:count(Store) >= Count of
        true -> ok;
        false ->
            Docs = make_batch_docs(Count, Dim),
            {ok, _} = barrel_vectordb:add_vector_batch(Store, Docs),
            ok
    end.

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
    [{list_to_binary("batch_" ++ integer_to_list(I)),
      list_to_binary("Document " ++ integer_to_list(I)),
      #{index => I},
      random_vector(Dim)}
     || I <- lists:seq(1, Count)].

find_bench(Name, BenchList) ->
    case lists:filter(fun(B) -> maps:get(name, B) =:= Name end, BenchList) of
        [B | _] -> {ok, B};
        [] -> not_found
    end.

print_summary(Results) ->
    io:format("~n"),
    io:format("================================================~n"),
    io:format("                    SUMMARY~n"),
    io:format("================================================~n"),
    io:format("~-20s | ~-12s | ~-12s | ~s~n",
              ["Benchmark", "HNSW", "FAISS", "Winner"]),
    io:format("~s~n", [lists:duplicate(55, $-)]),

    lists:foreach(fun(#{name := Name, hnsw := Hnsw, faiss := Faiss}) ->
        HnswOps = maps:get(ops_per_sec, Hnsw),
        {FaissStr, Winner} = case Faiss of
            undefined ->
                {"N/A", "HNSW"};
            _ ->
                FaissOps = maps:get(ops_per_sec, Faiss),
                W = if FaissOps > HnswOps * 1.05 -> "FAISS";
                       HnswOps > FaissOps * 1.05 -> "HNSW";
                       true -> "~same"
                    end,
                {format_ops(FaissOps), W}
        end,
        io:format("~-20s | ~-12s | ~-12s | ~s~n", [Name, format_ops(HnswOps), FaissStr, Winner])
    end, Results),

    io:format("~n(ops/sec - higher is better)~n").

format_ops(Ops) when Ops >= 1000 ->
    io_lib:format("~.1fK", [Ops / 1000]);
format_ops(Ops) ->
    io_lib:format("~B", [round(Ops)]).
