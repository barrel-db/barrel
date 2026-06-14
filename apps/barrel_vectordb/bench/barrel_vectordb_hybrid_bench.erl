%%%-------------------------------------------------------------------
%%% @doc Benchmark: DiskANN (Memory/Disk) vs Pure HNSW
%%%
%%% Compares:
%%% - Build time
%%% - Memory usage (estimated)
%%% - Search latency
%%% - Recall@10
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_hybrid_bench).

-export([run/0, run/1]).

-define(DEFAULT_CONFIG, #{
    dimension => 32,
    num_vectors => 1000,
    num_queries => 50,
    k => 10
}).

%%====================================================================
%% API
%%====================================================================

run() ->
    run(?DEFAULT_CONFIG).

run(Config) ->
    Dim = maps:get(dimension, Config, 64),
    NumVectors = maps:get(num_vectors, Config, 5000),
    NumQueries = maps:get(num_queries, Config, 100),
    K = maps:get(k, Config, 10),

    io:format("~n"),
    io:format("============================================================~n"),
    io:format("       DiskANN vs HNSW Benchmark~n"),
    io:format("============================================================~n"),
    io:format("~n"),
    io:format("Configuration:~n"),
    io:format("  Dimension:     ~p~n", [Dim]),
    io:format("  Vectors:       ~p~n", [NumVectors]),
    io:format("  Queries:       ~p~n", [NumQueries]),
    io:format("  K (top-k):     ~p~n", [K]),
    io:format("~n"),

    %% Seed random for reproducibility
    rand:seed(exsss, {42, 42, 42}),

    %% Generate test data
    io:format("Generating test vectors...~n"),
    Vectors = [{integer_to_binary(I), random_vector(Dim)} || I <- lists:seq(1, NumVectors)],
    Queries = [random_vector(Dim) || _ <- lists:seq(1, NumQueries)],

    %% Build HNSW index
    io:format("~n--- Build Time ---~n"),
    {HnswBuildUs, HnswIndex} = timer:tc(fun() ->
        lists:foldl(
            fun({Id, Vec}, Acc) ->
                barrel_vectordb_hnsw:insert(Acc, Id, Vec)
            end,
            barrel_vectordb_hnsw:new(#{dimension => Dim, m => 16, ef_construction => 100}),
            Vectors
        )
    end),
    io:format("HNSW build:            ~.2f s~n", [HnswBuildUs / 1_000_000]),

    %% Build DiskANN (memory mode) - NO PQ for memory mode (better recall)
    %% Higher R and L params for better graph quality
    R = 32,  %% Increased from 16
    LBuild = 75,  %% Increased from 30
    LSearch = 75,  %% Increased from 30

    DiskannConfig = #{
        dimension => Dim,
        r => R,
        l_build => LBuild,
        l_search => LSearch,
        alpha => 1.2,
        storage_mode => memory,
        use_pq => false,  %% Disable PQ in memory mode for better recall
        assume_normalized => true  %% Benchmark uses normalized vectors
    },
    {DiskannBuildUs, {ok, DiskannIndex}} = timer:tc(fun() ->
        barrel_vectordb_diskann:build(DiskannConfig, Vectors)
    end),
    io:format("DiskANN (memory) build: ~.2f s~n", [DiskannBuildUs / 1_000_000]),

    %% Build DiskANN (disk mode) with PQ for memory savings
    TmpDir = filename:join(["/tmp", "diskann_bench_" ++ integer_to_list(erlang:unique_integer([positive]))]),
    ok = filelib:ensure_dir(filename:join(TmpDir, "dummy")),

    %% PQ config for disk mode
    PQK = case NumVectors < 500 of
        true -> 16;
        _ -> case NumVectors < 5000 of
            true -> 64;
            false -> 256
        end
    end,
    PQM = max(2, min(8, Dim div 4)),

    DiskannDiskConfig = DiskannConfig#{
        storage_mode => disk,
        base_path => TmpDir,
        cache_max_size => min(1000, NumVectors div 10),
        use_pq => true,  %% Enable PQ for disk mode (memory savings)
        pq_m => PQM,
        pq_k => PQK
    },
    {DiskannDiskBuildUs, {ok, DiskannDiskIndex}} = timer:tc(fun() ->
        barrel_vectordb_diskann:build(DiskannDiskConfig, Vectors)
    end),
    io:format("DiskANN (disk) build:   ~.2f s~n", [DiskannDiskBuildUs / 1_000_000]),

    %% Memory usage (estimated)
    io:format("~n--- Memory Usage (Estimated) ---~n"),
    HnswMemBytes = estimate_hnsw_memory(HnswIndex, Dim),
    DiskannMemBytes = estimate_diskann_memory(DiskannIndex, Dim),
    DiskannDiskMemBytes = estimate_diskann_disk_memory(DiskannDiskIndex, Dim),

    io:format("HNSW:            ~.2f MB (~.1f bytes/vector)~n",
              [HnswMemBytes / 1_048_576, HnswMemBytes / NumVectors]),
    io:format("DiskANN (memory): ~.2f MB (~.1f bytes/vector)~n",
              [DiskannMemBytes / 1_048_576, DiskannMemBytes / NumVectors]),
    io:format("DiskANN (disk):   ~.2f MB (~.1f bytes/vector) [vectors on SSD]~n",
              [DiskannDiskMemBytes / 1_048_576, DiskannDiskMemBytes / NumVectors]),

    MemReductionMem = HnswMemBytes / max(1, DiskannMemBytes),
    MemReductionDisk = HnswMemBytes / max(1, DiskannDiskMemBytes),
    io:format("~nMemory reduction vs HNSW:~n"),
    io:format("  DiskANN (memory): ~.1fx~n", [MemReductionMem]),
    io:format("  DiskANN (disk):   ~.1fx~n", [MemReductionDisk]),

    %% Search latency
    io:format("~n--- Search Latency (~p queries) ---~n", [NumQueries]),
    {HnswSearchUs, _} = timer:tc(fun() ->
        [barrel_vectordb_hnsw:search(HnswIndex, Q, K) || Q <- Queries]
    end),
    {DiskannSearchUs, _} = timer:tc(fun() ->
        [barrel_vectordb_diskann:search(DiskannIndex, Q, K) || Q <- Queries]
    end),
    {DiskannDiskSearchUs, _} = timer:tc(fun() ->
        [barrel_vectordb_diskann:search(DiskannDiskIndex, Q, K) || Q <- Queries]
    end),

    HnswLatencyMs = HnswSearchUs / NumQueries / 1000,
    DiskannLatencyMs = DiskannSearchUs / NumQueries / 1000,
    DiskannDiskLatencyMs = DiskannDiskSearchUs / NumQueries / 1000,
    io:format("HNSW avg latency:            ~.3f ms~n", [HnswLatencyMs]),
    io:format("DiskANN (memory) avg latency: ~.3f ms~n", [DiskannLatencyMs]),
    io:format("DiskANN (disk) avg latency:   ~.3f ms~n", [DiskannDiskLatencyMs]),

    %% Recall@K
    io:format("~n--- Recall@~p ---~n", [K]),
    HnswRecalls = [measure_recall(HnswIndex, hnsw, Q, Vectors, K) || Q <- Queries],
    DiskannRecalls = [measure_recall(DiskannIndex, diskann, Q, Vectors, K) || Q <- Queries],
    DiskannDiskRecalls = [measure_recall(DiskannDiskIndex, diskann, Q, Vectors, K) || Q <- Queries],

    AvgHnswRecall = lists:sum(HnswRecalls) / length(HnswRecalls),
    AvgDiskannRecall = lists:sum(DiskannRecalls) / length(DiskannRecalls),
    AvgDiskannDiskRecall = lists:sum(DiskannDiskRecalls) / length(DiskannDiskRecalls),
    io:format("HNSW recall@~p:            ~.1f%~n", [K, AvgHnswRecall * 100]),
    io:format("DiskANN (memory) recall@~p: ~.1f%~n", [K, AvgDiskannRecall * 100]),
    io:format("DiskANN (disk) recall@~p:   ~.1f%~n", [K, AvgDiskannDiskRecall * 100]),

    %% Cleanup
    barrel_vectordb_diskann:close(DiskannDiskIndex),
    os:cmd("rm -rf " ++ TmpDir),

    %% Summary
    io:format("~n============================================================~n"),
    io:format("                        Summary~n"),
    io:format("============================================================~n"),
    io:format("~n"),
    io:format("| Metric              | HNSW          | DiskANN(mem)  | DiskANN(disk) |~n"),
    io:format("|---------------------|---------------|---------------|---------------|~n"),
    io:format("| Build time (s)      | ~6.2f        | ~6.2f        | ~6.2f        |~n",
              [HnswBuildUs / 1_000_000, DiskannBuildUs / 1_000_000, DiskannDiskBuildUs / 1_000_000]),
    io:format("| Memory (MB)         | ~6.2f        | ~6.2f        | ~6.2f        |~n",
              [HnswMemBytes / 1_048_576, DiskannMemBytes / 1_048_576, DiskannDiskMemBytes / 1_048_576]),
    io:format("| Search latency (ms) | ~6.3f        | ~6.3f        | ~6.3f        |~n",
              [HnswLatencyMs, DiskannLatencyMs, DiskannDiskLatencyMs]),
    io:format("| Recall@~p           | ~6.1f%       | ~6.1f%       | ~6.1f%       |~n",
              [K, AvgHnswRecall * 100, AvgDiskannRecall * 100, AvgDiskannDiskRecall * 100]),
    io:format("~n"),
    io:format("Memory reduction: DiskANN(disk) uses ~.1fx less RAM than HNSW~n", [MemReductionDisk]),
    io:format("~n"),

    #{
        hnsw => #{
            build_time_s => HnswBuildUs / 1_000_000,
            memory_mb => HnswMemBytes / 1_048_576,
            latency_ms => HnswLatencyMs,
            recall => AvgHnswRecall
        },
        diskann_memory => #{
            build_time_s => DiskannBuildUs / 1_000_000,
            memory_mb => DiskannMemBytes / 1_048_576,
            latency_ms => DiskannLatencyMs,
            recall => AvgDiskannRecall
        },
        diskann_disk => #{
            build_time_s => DiskannDiskBuildUs / 1_000_000,
            memory_mb => DiskannDiskMemBytes / 1_048_576,
            latency_ms => DiskannDiskLatencyMs,
            recall => AvgDiskannDiskRecall
        }
    }.

%%====================================================================
%% Internal Functions
%%====================================================================

random_vector(Dim) ->
    Vec = [rand:uniform() - 0.5 || _ <- lists:seq(1, Dim)],
    normalize(Vec).

normalize(Vec) ->
    Norm = math:sqrt(lists:sum([V*V || V <- Vec])),
    case Norm < 0.0001 of
        true -> Vec;
        false -> [V / Norm || V <- Vec]
    end.

estimate_hnsw_memory(Index, Dim) ->
    Info = barrel_vectordb_hnsw:info(Index),
    Size = maps:get(size, Info, 0),
    Config = maps:get(config, Info, #{}),
    M = maps:get(m, Config, 16),
    MaxLayer = maps:get(max_layer, Info, 0),

    %% Per vector: quantized vector (dim+4) + norm (8) + neighbors (M * layers * 8)
    VecBytes = Dim + 4,  %% int8 quantized + scale
    NormBytes = 8,
    AvgLayers = 1 + MaxLayer / 2,  %% Average layers per node
    NeighborBytes = M * AvgLayers * 16,  %% ID refs (assume 16 bytes avg)

    Size * (VecBytes + NormBytes + NeighborBytes).

estimate_diskann_memory(Index, Dim) ->
    Info = barrel_vectordb_diskann:info(Index),
    Size = maps:get(size, Info, 0),
    ConfigInfo = maps:get(config, Info, #{}),
    R = maps:get(r, ConfigInfo, 64),
    PQInfo = maps:get(pq, Info, #{enabled => false}),
    PQEnabled = maps:get(enabled, PQInfo, false),

    %% Memory mode: vectors in RAM
    %% Per vector: ID (16) + neighbors (R * 16) + vector or PQ code
    VecBytes = case PQEnabled of
        true ->
            %% With PQ: full vectors still in RAM for memory mode
            Dim * 4 + 8;  %% float32 + PQ code
        false ->
            Dim * 4  %% float32
    end,
    GraphBytes = R * 8,  %% neighbor IDs
    IdBytes = 16,

    Size * (IdBytes + GraphBytes + VecBytes).

estimate_diskann_disk_memory(Index, Dim) ->
    Info = barrel_vectordb_diskann:info(Index),
    Size = maps:get(size, Info, 0),
    ConfigInfo = maps:get(config, Info, #{}),
    R = maps:get(r, ConfigInfo, 64),
    PQInfo = maps:get(pq, Info, #{enabled => false}),
    PQEnabled = maps:get(enabled, PQInfo, false),
    StorageInfo = maps:get(storage, Info, #{}),
    CacheSize = maps:get(cache_size, StorageInfo, 0),

    %% Disk mode: only graph + PQ codes in RAM, vectors on SSD
    %% Per vector in RAM: ID (16) + neighbors (R * 8) + PQ code (M bytes)
    PQBytes = case PQEnabled of
        true -> 8;  %% M bytes for PQ code
        false -> 0
    end,
    GraphBytes = R * 8,  %% neighbor IDs
    IdBytes = 16,

    %% Cache memory (full vectors for cached items)
    CacheMemory = CacheSize * Dim * 4,

    Size * (IdBytes + GraphBytes + PQBytes) + CacheMemory.

measure_recall(Index, Type, Query, Vectors, K) ->
    Results = case Type of
        hnsw -> barrel_vectordb_hnsw:search(Index, Query, K);
        diskann -> barrel_vectordb_diskann:search(Index, Query, K)
    end,
    ResultIds = [Id || {Id, _} <- Results],

    %% Ground truth
    TrueTopK = brute_force_search(Vectors, Query, K),
    TrueIds = [Id || {Id, _} <- TrueTopK],

    Intersection = length([Id || Id <- ResultIds, lists:member(Id, TrueIds)]),
    Intersection / K.

brute_force_search(Vectors, Query, K) ->
    Distances = [{Id, cosine_distance(Query, Vec)} || {Id, Vec} <- Vectors],
    Sorted = lists:sort(fun({_, D1}, {_, D2}) -> D1 =< D2 end, Distances),
    lists:sublist(Sorted, K).

cosine_distance(Vec1, Vec2) ->
    Dot = lists:sum([A * B || {A, B} <- lists:zip(Vec1, Vec2)]),
    Norm1 = math:sqrt(lists:sum([V*V || V <- Vec1])),
    Norm2 = math:sqrt(lists:sum([V*V || V <- Vec2])),
    Denom = Norm1 * Norm2,
    case Denom < 1.0e-10 of
        true -> 1.0;
        false -> 1.0 - (Dot / Denom)
    end.
