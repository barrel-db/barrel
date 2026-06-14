#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa _build/default/lib/barrel_vectordb/ebin -pa _build/default/lib/rocksdb/ebin -pa _build/default/lib/ra/ebin -pa _build/default/lib/aten/ebin -pa _build/default/lib/gen_batch_server/ebin

-mode(compile).

-record(stats, {
    total = 0 :: non_neg_integer(),
    success = 0 :: non_neg_integer(),
    failed = 0 :: non_neg_integer(),
    latencies = [] :: [float()],
    start_time :: integer(),
    end_time :: integer()
}).

main(Args) ->
    %% Parse arguments
    Config = parse_args(Args),

    io:format("~n=== Barrel Memory Load Simulation ===~n~n"),
    io:format("Configuration:~n"),
    io:format("  Users:           ~p~n", [maps:get(users, Config)]),
    io:format("  Memories/user:   ~p~n", [maps:get(memories_per_user, Config)]),
    io:format("  Vector dimension: ~p~n", [maps:get(dimension, Config)]),
    io:format("  Batch size:      ~p~n", [maps:get(batch_size, Config)]),
    io:format("  Initial index:   ~p vectors~n", [maps:get(initial_size, Config)]),
    io:format("  Mode:            ~p~n", [maps:get(mode, Config)]),
    io:format("  Hot layer:       ~p~n~n", [maps:get(hot_layer, Config)]),

    %% Setup
    TmpDir = "/tmp/barrel_memory_bench_" ++ integer_to_list(erlang:unique_integer([positive])),
    ok = filelib:ensure_dir(filename:join(TmpDir, "dummy")),

    try
        run_benchmark(Config, TmpDir)
    after
        %% Cleanup
        os:cmd("rm -rf " ++ TmpDir),
        barrel_vectordb_diskann:close_all_standalone_dbs()
    end.

parse_args(Args) ->
    Defaults = #{
        users => 10,
        memories_per_user => 20,
        dimension => 128,
        batch_size => 1,  %% 1 = single inserts, >1 = batch
        initial_size => 100,
        mode => sequential,  %% sequential | concurrent | burst
        hot_layer => false,  %% Enable hot layer for fast writes
        hot_max_size => 10000
    },
    parse_args(Args, Defaults).

parse_args([], Config) -> Config;
parse_args(["--users", N | Rest], Config) ->
    parse_args(Rest, Config#{users => list_to_integer(N)});
parse_args(["--memories", N | Rest], Config) ->
    parse_args(Rest, Config#{memories_per_user => list_to_integer(N)});
parse_args(["--dimension", N | Rest], Config) ->
    parse_args(Rest, Config#{dimension => list_to_integer(N)});
parse_args(["--batch", N | Rest], Config) ->
    parse_args(Rest, Config#{batch_size => list_to_integer(N)});
parse_args(["--initial", N | Rest], Config) ->
    parse_args(Rest, Config#{initial_size => list_to_integer(N)});
parse_args(["--hot" | Rest], Config) ->
    parse_args(Rest, Config#{hot_layer => true});
parse_args(["--hot-size", N | Rest], Config) ->
    parse_args(Rest, Config#{hot_max_size => list_to_integer(N)});
parse_args(["--mode", M | Rest], Config) ->
    Mode = case M of
        "sequential" -> sequential;
        "concurrent" -> concurrent;
        "burst" -> burst;
        _ -> sequential
    end,
    parse_args(Rest, Config#{mode => Mode});
parse_args(["--help" | _], _Config) ->
    print_help(),
    halt(0);
parse_args([_ | Rest], Config) ->
    parse_args(Rest, Config).

print_help() ->
    io:format("Usage: bench_load_simulation.escript [options]~n~n"),
    io:format("Options:~n"),
    io:format("  --users N       Number of simulated users (default: 10)~n"),
    io:format("  --memories N    Memories per user (default: 20)~n"),
    io:format("  --dimension N   Vector dimension (default: 128)~n"),
    io:format("  --batch N       Batch size, 1=single inserts (default: 1)~n"),
    io:format("  --initial N     Initial index size (default: 100)~n"),
    io:format("  --mode M        Mode: sequential|concurrent|burst (default: sequential)~n"),
    io:format("  --hot           Enable hot layer for fast writes~n"),
    io:format("  --hot-size N    Hot layer max size (default: 10000)~n"),
    io:format("  --help          Show this help~n~n"),
    io:format("Examples:~n"),
    io:format("  # 10 users, single inserts, sequential~n"),
    io:format("  ./bench_load_simulation.escript~n~n"),
    io:format("  # 50 users, concurrent inserts~n"),
    io:format("  ./bench_load_simulation.escript --users 50 --mode concurrent~n~n"),
    io:format("  # With hot layer enabled~n"),
    io:format("  ./bench_load_simulation.escript --users 50 --hot~n~n"),
    io:format("  # Burst mode with batching~n"),
    io:format("  ./bench_load_simulation.escript --users 100 --batch 50 --mode burst~n").

run_benchmark(Config, TmpDir) ->
    Dim = maps:get(dimension, Config),
    InitialSize = maps:get(initial_size, Config),
    HotLayer = maps:get(hot_layer, Config),
    HotMaxSize = maps:get(hot_max_size, Config),

    %% Build initial index
    io:format("Building initial index with ~p vectors...~n", [InitialSize]),
    rand:seed(exsss, {42, 42, 42}),
    InitialVectors = [{make_id(initial, I), random_vector(Dim)}
                      || I <- lists:seq(1, InitialSize)],

    IndexConfig = #{
        dimension => Dim,
        r => 32,
        l_build => 100,
        l_search => 100,
        alpha => 1.2,
        storage_mode => disk,
        base_path => TmpDir,
        use_pq => false,
        hot_layer => HotLayer,
        hot_max_size => HotMaxSize
    },

    {ok, Index0} = barrel_vectordb_diskann:build(IndexConfig, InitialVectors),
    io:format("Initial index ready. Size: ~p~n", [barrel_vectordb_diskann:size(Index0)]),
    case HotLayer of
        true -> io:format("Hot layer: ENABLED (max_size: ~p)~n~n", [HotMaxSize]);
        false -> io:format("Hot layer: disabled~n~n")
    end,

    %% Run based on mode
    Mode = maps:get(mode, Config),
    {Stats, FinalIndex} = case Mode of
        sequential -> run_sequential(Config, Index0);
        concurrent -> run_concurrent(Config, Index0);
        burst -> run_burst(Config, Index0)
    end,

    print_results(Stats, FinalIndex, Config),

    ok = barrel_vectordb_diskann:close(FinalIndex).

%% Sequential mode: one user at a time, single inserts
run_sequential(Config, Index0) ->
    Users = maps:get(users, Config),
    MemoriesPerUser = maps:get(memories_per_user, Config),
    Dim = maps:get(dimension, Config),
    BatchSize = maps:get(batch_size, Config),

    io:format("Running SEQUENTIAL mode...~n"),
    io:format("  Simulating ~p users, ~p memories each~n~n", [Users, MemoriesPerUser]),

    StartTime = erlang:monotonic_time(microsecond),

    {FinalIndex, AllLatencies} = lists:foldl(
        fun(UserId, {AccIndex, AccLats}) ->
            UserMemories = [{make_id(UserId, I), random_vector(Dim)}
                           || I <- lists:seq(1, MemoriesPerUser)],

            case BatchSize of
                1 ->
                    %% Single inserts
                    lists:foldl(
                        fun({Id, Vec}, {Idx, Lats}) ->
                            T1 = erlang:monotonic_time(microsecond),
                            {ok, Idx2} = barrel_vectordb_diskann:insert(Idx, Id, Vec),
                            T2 = erlang:monotonic_time(microsecond),
                            {Idx2, [(T2 - T1) / 1000 | Lats]}
                        end,
                        {AccIndex, AccLats},
                        UserMemories
                    );
                _ ->
                    %% Batch inserts
                    Batches = batch_list(UserMemories, BatchSize),
                    lists:foldl(
                        fun(Batch, {Idx, Lats}) ->
                            T1 = erlang:monotonic_time(microsecond),
                            {ok, Idx2} = barrel_vectordb_diskann:insert_batch(Idx, Batch, #{}),
                            T2 = erlang:monotonic_time(microsecond),
                            LatPerVec = (T2 - T1) / 1000 / length(Batch),
                            NewLats = [LatPerVec || _ <- Batch],
                            {Idx2, NewLats ++ Lats}
                        end,
                        {AccIndex, AccLats},
                        Batches
                    )
            end
        end,
        {Index0, []},
        lists:seq(1, Users)
    ),

    EndTime = erlang:monotonic_time(microsecond),

    Stats = #stats{
        total = Users * MemoriesPerUser,
        success = Users * MemoriesPerUser,
        failed = 0,
        latencies = AllLatencies,
        start_time = StartTime,
        end_time = EndTime
    },

    {Stats, FinalIndex}.

%% Concurrent mode: multiple users inserting in parallel
run_concurrent(Config, Index0) ->
    Users = maps:get(users, Config),
    MemoriesPerUser = maps:get(memories_per_user, Config),
    Dim = maps:get(dimension, Config),

    io:format("Running CONCURRENT mode...~n"),
    io:format("  Simulating ~p concurrent users, ~p memories each~n~n", [Users, MemoriesPerUser]),

    %% Since DiskANN index is not concurrent-safe for writes,
    %% we simulate concurrency by interleaving inserts
    AllMemories = lists:flatten([
        [{UserId, I, random_vector(Dim)} || I <- lists:seq(1, MemoriesPerUser)]
        || UserId <- lists:seq(1, Users)
    ]),

    %% Shuffle to simulate concurrent arrival
    Shuffled = shuffle(AllMemories),

    StartTime = erlang:monotonic_time(microsecond),

    {FinalIndex, AllLatencies} = lists:foldl(
        fun({UserId, MemId, Vec}, {AccIndex, AccLats}) ->
            Id = make_id(UserId, MemId),
            T1 = erlang:monotonic_time(microsecond),
            {ok, Idx2} = barrel_vectordb_diskann:insert(AccIndex, Id, Vec),
            T2 = erlang:monotonic_time(microsecond),
            {Idx2, [(T2 - T1) / 1000 | AccLats]}
        end,
        {Index0, []},
        Shuffled
    ),

    EndTime = erlang:monotonic_time(microsecond),

    Stats = #stats{
        total = Users * MemoriesPerUser,
        success = Users * MemoriesPerUser,
        failed = 0,
        latencies = AllLatencies,
        start_time = StartTime,
        end_time = EndTime
    },

    {Stats, FinalIndex}.

%% Burst mode: all memories inserted in batches
run_burst(Config, Index0) ->
    Users = maps:get(users, Config),
    MemoriesPerUser = maps:get(memories_per_user, Config),
    Dim = maps:get(dimension, Config),
    BatchSize = max(1, maps:get(batch_size, Config)),

    io:format("Running BURST mode...~n"),
    io:format("  Total memories: ~p, batch size: ~p~n~n",
              [Users * MemoriesPerUser, BatchSize]),

    %% Generate all memories
    AllMemories = [{make_id(U, M), random_vector(Dim)}
                   || U <- lists:seq(1, Users),
                      M <- lists:seq(1, MemoriesPerUser)],

    %% Split into batches
    Batches = batch_list(AllMemories, BatchSize),

    StartTime = erlang:monotonic_time(microsecond),

    {FinalIndex, AllLatencies} = lists:foldl(
        fun(Batch, {AccIndex, AccLats}) ->
            T1 = erlang:monotonic_time(microsecond),
            {ok, Idx2} = barrel_vectordb_diskann:insert_batch(AccIndex, Batch, #{}),
            T2 = erlang:monotonic_time(microsecond),
            LatPerVec = (T2 - T1) / 1000 / length(Batch),
            NewLats = [LatPerVec || _ <- Batch],
            {Idx2, NewLats ++ AccLats}
        end,
        {Index0, []},
        Batches
    ),

    EndTime = erlang:monotonic_time(microsecond),

    Stats = #stats{
        total = Users * MemoriesPerUser,
        success = Users * MemoriesPerUser,
        failed = 0,
        latencies = AllLatencies,
        start_time = StartTime,
        end_time = EndTime
    },

    {Stats, FinalIndex}.

print_results(Stats, FinalIndex, Config) ->
    #stats{
        total = Total,
        success = Success,
        failed = Failed,
        latencies = Latencies,
        start_time = StartTime,
        end_time = EndTime
    } = Stats,

    TotalTimeMs = (EndTime - StartTime) / 1000,
    TotalTimeSec = TotalTimeMs / 1000,
    Throughput = Total / TotalTimeSec,

    SortedLats = lists:sort(Latencies),

    P50 = percentile(SortedLats, 50),
    P95 = percentile(SortedLats, 95),
    P99 = percentile(SortedLats, 99),
    AvgLat = lists:sum(Latencies) / length(Latencies),
    MinLat = hd(SortedLats),
    MaxLat = lists:last(SortedLats),

    io:format("~n=== Results ===~n~n"),
    io:format("Operations:~n"),
    io:format("  Total:     ~p~n", [Total]),
    io:format("  Success:   ~p~n", [Success]),
    io:format("  Failed:    ~p~n", [Failed]),
    io:format("~n"),
    io:format("Timing:~n"),
    io:format("  Total time:  ~.2f sec~n", [TotalTimeSec]),
    io:format("  Throughput:  ~.1f inserts/sec~n", [Throughput]),
    io:format("~n"),
    io:format("Latency (ms):~n"),
    io:format("  Min:   ~.2f~n", [MinLat]),
    io:format("  Avg:   ~.2f~n", [AvgLat]),
    io:format("  P50:   ~.2f~n", [P50]),
    io:format("  P95:   ~.2f~n", [P95]),
    io:format("  P99:   ~.2f~n", [P99]),
    io:format("  Max:   ~.2f~n", [MaxLat]),
    io:format("~n"),

    %% Capacity estimation
    io:format("Capacity Estimation:~n"),
    SafeThroughput = Throughput * 0.7,  %% 70% of max for headroom
    io:format("  Sustainable rate: ~.1f inserts/sec (70%% of max)~n", [SafeThroughput]),
    io:format("  Users at 1 mem/sec: ~p concurrent users~n", [trunc(SafeThroughput)]),
    io:format("  Users at 0.1 mem/sec: ~p concurrent users~n", [trunc(SafeThroughput * 10)]),
    io:format("~n"),

    %% Index info including hot layer
    Info = barrel_vectordb_diskann:info(FinalIndex),
    io:format("Index Status:~n"),
    io:format("  Total size: ~p vectors~n", [maps:get(size, Info)]),
    HotInfo = maps:get(hot_layer, Info),
    case maps:get(enabled, HotInfo) of
        true ->
            io:format("  Hot layer size: ~p vectors~n", [maps:get(size, HotInfo)]),
            io:format("  Hot layer fill: ~.1f%%~n", [maps:get(fill_ratio, HotInfo) * 100]);
        false ->
            ok
    end,
    io:format("~n"),

    %% Memory usage
    io:format("Memory Usage:~n"),
    {memory, ProcMem} = erlang:process_info(self(), memory),
    io:format("  Process memory: ~.2f MB~n", [ProcMem / 1024 / 1024]),
    TotalMem = erlang:memory(total),
    io:format("  Total BEAM memory: ~.2f MB~n", [TotalMem / 1024 / 1024]),
    io:format("~n"),

    %% Search performance check
    io:format("Verifying search performance...~n"),
    Dim = maps:get(dimension, Config),
    {SearchTime, _Results} = timer:tc(fun() ->
        [barrel_vectordb_diskann:search(FinalIndex, random_vector(Dim), 10)
         || _ <- lists:seq(1, 100)]
    end),
    AvgSearchMs = SearchTime / 1000 / 100,
    io:format("  Avg search latency: ~.2f ms~n", [AvgSearchMs]),
    io:format("~n=== Benchmark Complete ===~n").

%% Helpers

make_id(User, Memory) ->
    iolist_to_binary(io_lib:format("user~p_mem~p", [User, Memory])).

random_vector(Dim) ->
    Vec = [rand:uniform() - 0.5 || _ <- lists:seq(1, Dim)],
    normalize(Vec).

normalize(Vec) ->
    Norm = math:sqrt(lists:sum([V*V || V <- Vec])),
    case Norm < 0.0001 of
        true -> Vec;
        false -> [V / Norm || V <- Vec]
    end.

shuffle(List) ->
    [X || {_, X} <- lists:sort([{rand:uniform(), E} || E <- List])].

batch_list(List, BatchSize) ->
    batch_list(List, BatchSize, []).

batch_list([], _BatchSize, Acc) ->
    lists:reverse(Acc);
batch_list(List, BatchSize, Acc) ->
    {Batch, Rest} = safe_split(BatchSize, List),
    batch_list(Rest, BatchSize, [Batch | Acc]).

safe_split(N, List) when length(List) =< N ->
    {List, []};
safe_split(N, List) ->
    lists:split(N, List).

percentile(SortedList, P) ->
    Len = length(SortedList),
    case Len of
        0 -> 0.0;
        _ ->
            Idx = max(1, round(Len * P / 100)),
            lists:nth(Idx, SortedList)
    end.
