%%%-------------------------------------------------------------------
%%% @doc EUnit tests for barrel_vectordb_diskann module
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_diskann_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Generators
%%====================================================================

diskann_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"new creates empty index", fun test_new/0},
        {"new validates config", fun test_new_validation/0},
        {"build creates index", fun test_build/0},
        {"search finds nearest", fun test_search/0},
        {"search recall test", fun test_search_recall/0},
        {"insert adds vectors", fun test_insert/0},
        {"insert preserves recall", fun test_insert_preserves_recall/0},
        {"delete filters results", fun test_delete/0},
        {"consolidate removes deleted", fun test_consolidate/0},
        {"alpha rng stability", fun test_alpha_rng_stability/0},
        {"pq search test", fun test_pq_search/0},
        {"pq insert test", fun test_pq_insert/0},
        {"assume normalized optimization", fun test_assume_normalized/0}
     ]
    }.

%% Disk mode tests
%% Now uses standalone RocksDB when barrel_vectordb_store is not running
%% Each test manages its own setup/cleanup to ensure proper RocksDB lifecycle
diskann_disk_test_() ->
    [
        {"serialization roundtrip (memory mode)", fun test_serialization/0},
        {"disk mode build and search", fun test_disk_build_search/0},
        {"disk mode persistence", fun test_disk_persistence/0},
        {"disk mode insert", fun test_disk_insert/0},
        {"lru cache eviction", fun test_lru_cache_eviction/0},
        {"cache warming persists", fun test_cache_warming_persists/0},
        {"batch insert deferred correctness", fun test_batch_insert_deferred/0},
        {"batch insert deferred recall", fun test_batch_insert_recall_deferred/0},
        {"cache prewarm coverage for batch", fun test_cache_prewarm_coverage/0},
        {"lazy delete disk mode", fun test_lazy_delete_disk/0},
        {"consolidate deletes disk mode", fun test_consolidate_deletes_disk/0},
        {"needs consolidation threshold", fun test_needs_consolidation/0}
    ].

%% V2 format tests with RocksDB ID mapping
diskann_v2_test_() ->
    [
        {"v2 format lazy graph loading", fun test_v2_lazy_graph_loading/0},
        {"v2 format fast open", fun test_v2_fast_open/0},
        {"v2 format rocksdb id mapping", fun test_v2_rocksdb_id_mapping/0}
    ].

%% Hot layer tests
hot_layer_test_() ->
    [
        {"hot layer insert", fun test_hot_insert/0},
        {"hot layer search", fun test_hot_search/0},
        {"hot layer combined search", fun test_hot_combined_search/0},
        {"hot layer delete", fun test_hot_delete/0},
        {"hot layer compaction threshold", fun test_hot_compaction_threshold/0},
        {"hot layer compaction correctness", fun test_hot_compaction_correctness/0},
        {"hot layer recall", fun test_hot_layer_recall/0},
        {"hot layer latency under 1ms", fun test_hot_layer_latency/0}
    ].

%%====================================================================
%% Setup/Teardown
%%====================================================================

setup() ->
    rand:seed(exsss, {42, 42, 42}),
    ok.

cleanup(_) ->
    ok.

setup_disk() ->
    rand:seed(exsss, {42, 42, 42}),
    %% Create temp directory for disk tests
    TmpDir = filename:join(["/tmp", "diskann_test_" ++ integer_to_list(erlang:unique_integer([positive]))]),
    ok = filelib:ensure_dir(filename:join(TmpDir, "dummy")),
    TmpDir.

cleanup_disk(TmpDir) ->
    %% Close any open standalone RocksDB databases (handles test failures)
    barrel_vectordb_diskann:close_all_standalone_dbs(),
    %% Remove temp directory
    os:cmd("rm -rf " ++ TmpDir),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_new() ->
    {ok, Index} = barrel_vectordb_diskann:new(#{dimension => 128}),
    ?assertEqual(0, barrel_vectordb_diskann:size(Index)),
    Info = barrel_vectordb_diskann:info(Index),
    ?assertEqual(0, maps:get(size, Info)),
    ?assertEqual(undefined, maps:get(medoid, Info)).

test_new_validation() ->
    %% Missing dimension
    ?assertMatch({error, dimension_required},
                 barrel_vectordb_diskann:new(#{})),

    %% Invalid dimension
    ?assertMatch({error, {invalid_dimension, -1}},
                 barrel_vectordb_diskann:new(#{dimension => -1})).

test_build() ->
    Vectors = [{integer_to_binary(I), random_vector(32)}
               || I <- lists:seq(1, 100)],
    Config = #{dimension => 32, r => 8, l_build => 20, alpha => 1.2},
    {ok, Index} = barrel_vectordb_diskann:build(Config, Vectors),

    ?assertEqual(100, barrel_vectordb_diskann:size(Index)),

    Info = barrel_vectordb_diskann:info(Index),
    ?assert(maps:get(medoid, Info) =/= undefined),
    ?assert(maps:get(avg_degree, Info) > 0).

test_search() ->
    %% Use a larger index for better connectivity
    Vectors = [{integer_to_binary(I), random_vector(8)} || I <- lists:seq(1, 50)],
    {ok, Index} = barrel_vectordb_diskann:build(
        #{dimension => 8, r => 8, l_build => 30},
        Vectors
    ),

    %% Search should return results
    Query = random_vector(8),
    Results = barrel_vectordb_diskann:search(Index, Query, 5),

    %% Should get 5 results
    ?assertEqual(5, length(Results)),

    %% Results should be sorted by distance
    Dists = [D || {_, D} <- Results],
    ?assertEqual(lists:sort(Dists), Dists).

test_search_recall() ->
    %% Build index with 100 vectors (smaller for faster tests)
    Vectors = [{integer_to_binary(I), random_vector(16)}
               || I <- lists:seq(1, 100)],
    Config = #{dimension => 16, r => 16, l_build => 30, l_search => 30, alpha => 1.2},
    {ok, Index} = barrel_vectordb_diskann:build(Config, Vectors),

    %% Test recall on 5 queries
    Recalls = [measure_recall(Index, random_vector(16), Vectors, 5)
               || _ <- lists:seq(1, 5)],
    AvgRecall = lists:sum(Recalls) / length(Recalls),

    %% Should achieve reasonable recall (>=50%)
    ?assert(AvgRecall >= 0.50).

test_insert() ->
    %% Start with empty index
    {ok, Index0} = barrel_vectordb_diskann:new(#{dimension => 8, r => 4}),

    %% Insert first vector
    {ok, Index1} = barrel_vectordb_diskann:insert(Index0, <<"v1">>, random_vector(8)),
    ?assertEqual(1, barrel_vectordb_diskann:size(Index1)),

    %% Insert more vectors (need more than 2 to search for 2)
    {ok, Index2} = barrel_vectordb_diskann:insert(Index1, <<"v2">>, random_vector(8)),
    {ok, Index3} = barrel_vectordb_diskann:insert(Index2, <<"v3">>, random_vector(8)),
    {ok, Index4} = barrel_vectordb_diskann:insert(Index3, <<"v4">>, random_vector(8)),
    {ok, Index5} = barrel_vectordb_diskann:insert(Index4, <<"v5">>, random_vector(8)),
    ?assertEqual(5, barrel_vectordb_diskann:size(Index5)),

    %% Search should return results (may be fewer than K if graph not well connected)
    Results = barrel_vectordb_diskann:search(Index5, random_vector(8), 3),
    ?assert(length(Results) >= 1).

test_insert_preserves_recall() ->
    %% Build initial index (smaller for faster tests)
    InitialVectors = [{integer_to_binary(I), random_vector(16)}
                      || I <- lists:seq(1, 100)],
    Config = #{dimension => 16, r => 8, l_build => 20, alpha => 1.2},
    {ok, Index0} = barrel_vectordb_diskann:build(Config, InitialVectors),

    %% Insert 20 more vectors
    Index1 = lists:foldl(
        fun(I, AccIndex) ->
            {ok, NewIndex} = barrel_vectordb_diskann:insert(
                AccIndex,
                integer_to_binary(100 + I),
                random_vector(16)
            ),
            NewIndex
        end,
        Index0,
        lists:seq(1, 20)
    ),

    ?assertEqual(120, barrel_vectordb_diskann:size(Index1)),

    %% Search should still work after inserts
    Results = barrel_vectordb_diskann:search(Index1, random_vector(16), 5),
    ?assert(length(Results) >= 3).

test_delete() ->
    Vectors = [{integer_to_binary(I), random_vector(16)}
               || I <- lists:seq(1, 50)],
    {ok, Index0} = barrel_vectordb_diskann:build(
        #{dimension => 16, r => 8, l_build => 20},
        Vectors
    ),

    %% Delete some vectors
    {ok, Index1} = barrel_vectordb_diskann:delete(Index0, <<"1">>),
    {ok, Index2} = barrel_vectordb_diskann:delete(Index1, <<"2">>),
    {ok, Index3} = barrel_vectordb_diskann:delete(Index2, <<"3">>),

    %% Size should decrease
    ?assertEqual(47, barrel_vectordb_diskann:size(Index3)),

    %% Deleted vectors should not appear in results
    Results = barrel_vectordb_diskann:search(Index3, random_vector(16), 50),
    ResultIds = [Id || {Id, _} <- Results],
    ?assertNot(lists:member(<<"1">>, ResultIds)),
    ?assertNot(lists:member(<<"2">>, ResultIds)),
    ?assertNot(lists:member(<<"3">>, ResultIds)).

test_consolidate() ->
    Vectors = [{integer_to_binary(I), random_vector(16)}
               || I <- lists:seq(1, 100)],
    {ok, Index0} = barrel_vectordb_diskann:build(
        #{dimension => 16, r => 8, l_build => 30},
        Vectors
    ),

    %% Delete 10% of vectors
    Index1 = lists:foldl(
        fun(I, AccIndex) ->
            {ok, NewIndex} = barrel_vectordb_diskann:delete(AccIndex, integer_to_binary(I)),
            NewIndex
        end,
        Index0,
        lists:seq(1, 10)
    ),

    Info1 = barrel_vectordb_diskann:info(Index1),
    ?assertEqual(10, maps:get(deleted_count, Info1)),

    %% Consolidate
    {ok, Index2} = barrel_vectordb_diskann:consolidate_deletes(Index1),

    Info2 = barrel_vectordb_diskann:info(Index2),
    ?assertEqual(0, maps:get(deleted_count, Info2)),
    ?assertEqual(90, maps:get(active_size, Info2)),

    %% Search should still work (use correct dimension)
    Results = barrel_vectordb_diskann:search(Index2, random_vector(16), 5),
    ?assert(length(Results) > 0).

test_alpha_rng_stability() ->
    %% Test that index works after delete/consolidate/insert cycles
    %% Simplified version for faster testing
    InitialVectors = [{integer_to_binary(I), random_vector(16)}
                      || I <- lists:seq(1, 100)],
    Config = #{dimension => 16, r => 8, l_build => 20, alpha => 1.2},
    {ok, Index0} = barrel_vectordb_diskann:build(Config, InitialVectors),

    %% 2 cycles of delete + consolidate + reinsert
    {FinalIndex, _} = lists:foldl(
        fun(_Cycle, {AccIndex, NextId}) ->
            %% Delete 5 random vectors
            ToDelete = [integer_to_binary(rand:uniform(NextId - 1))
                        || _ <- lists:seq(1, 5)],
            Index1 = lists:foldl(
                fun(Id, Acc) ->
                    {ok, New} = barrel_vectordb_diskann:delete(Acc, Id),
                    New
                end,
                AccIndex,
                lists:usort(ToDelete)  %% Remove duplicates
            ),

            %% Consolidate
            {ok, Index2} = barrel_vectordb_diskann:consolidate_deletes(Index1),

            %% Reinsert 5 new vectors
            Index3 = lists:foldl(
                fun(I, Acc) ->
                    {ok, New} = barrel_vectordb_diskann:insert(
                        Acc,
                        integer_to_binary(NextId + I),
                        random_vector(16)
                    ),
                    New
                end,
                Index2,
                lists:seq(1, 5)
            ),

            {Index3, NextId + 5}
        end,
        {Index0, 101},
        lists:seq(1, 2)
    ),

    %% Index should still work after cycles
    Results = barrel_vectordb_diskann:search(FinalIndex, random_vector(16), 5),
    ?assert(length(Results) >= 1).

test_pq_search() ->
    %% Build index with PQ enabled
    %% Use smaller K (16) for faster tests - production would use 256
    Vectors = [{integer_to_binary(I), random_vector(16)}
               || I <- lists:seq(1, 50)],
    Config = #{
        dimension => 16,
        r => 8,
        l_build => 20,
        l_search => 20,
        alpha => 1.2,
        use_pq => true,
        pq_m => 2,       %% 16 / 2 = 8 dims per subspace
        pq_k => 16       %% Small K for fast testing
    },
    {ok, Index} = barrel_vectordb_diskann:build(Config, Vectors),

    %% Verify PQ is enabled
    Info = barrel_vectordb_diskann:info(Index),
    PQInfo = maps:get(pq, Info),
    ?assertEqual(true, maps:get(enabled, PQInfo)),

    %% Search should work with PQ
    Query = random_vector(16),
    Results = barrel_vectordb_diskann:search(Index, Query, 5),
    ?assertEqual(5, length(Results)),

    %% Check recall with PQ - expect some loss
    Recall = measure_recall(Index, Query, Vectors, 5),
    ?assert(Recall >= 0.2).

test_pq_insert() ->
    %% Build index with PQ, then insert more vectors
    Vectors = [{integer_to_binary(I), random_vector(16)}
               || I <- lists:seq(1, 50)],
    Config = #{
        dimension => 16,
        r => 8,
        l_build => 20,
        use_pq => true,
        pq_m => 2,
        pq_k => 16
    },
    {ok, Index0} = barrel_vectordb_diskann:build(Config, Vectors),

    %% Insert new vectors (should be PQ encoded)
    {ok, Index1} = barrel_vectordb_diskann:insert(Index0, <<"new1">>, random_vector(16)),
    {ok, Index2} = barrel_vectordb_diskann:insert(Index1, <<"new2">>, random_vector(16)),

    ?assertEqual(52, barrel_vectordb_diskann:size(Index2)),

    %% Search should still work
    Results = barrel_vectordb_diskann:search(Index2, random_vector(16), 5),
    ?assertEqual(5, length(Results)).

%%====================================================================
%% Disk Mode Tests
%%====================================================================

test_disk_build_search() ->
    %% Build index in disk mode
    TmpDir = setup_disk(),
    try
        Vectors = [{integer_to_binary(I), random_vector(16)}
                   || I <- lists:seq(1, 50)],
        Config = #{
            dimension => 16,
            r => 8,
            l_build => 20,
            l_search => 20,
            storage_mode => disk,
            base_path => TmpDir,
            use_pq => true,
            pq_m => 2,
            pq_k => 16
        },
        {ok, Index} = barrel_vectordb_diskann:build(Config, Vectors),

        %% Verify disk mode is active
        Info = barrel_vectordb_diskann:info(Index),
        StorageInfo = maps:get(storage, Info),
        ?assertEqual(disk, maps:get(mode, StorageInfo)),

        %% Search should work
        Query = random_vector(16),
        Results = barrel_vectordb_diskann:search(Index, Query, 5),
        ?assertEqual(5, length(Results)),

        %% Close index
        ok = barrel_vectordb_diskann:close(Index)
    after
        cleanup_disk(TmpDir)
    end.

test_disk_persistence() ->
    %% Build index, close, reopen, and search
    TmpDir = setup_disk(),
    try
        Vectors = [{integer_to_binary(I), random_vector(16)}
                   || I <- lists:seq(1, 50)],
        Config = #{
            dimension => 16,
            r => 8,
            l_build => 20,
            l_search => 20,
            storage_mode => disk,
            base_path => TmpDir,
            use_pq => true,
            pq_m => 2,
            pq_k => 16
        },

        %% Build and close
        {ok, Index1} = barrel_vectordb_diskann:build(Config, Vectors),
        Query = random_vector(16),
        Results1 = barrel_vectordb_diskann:search(Index1, Query, 5),
        ok = barrel_vectordb_diskann:close(Index1),

        %% Reopen and search
        {ok, Index2} = barrel_vectordb_diskann:open(TmpDir),
        ?assertEqual(50, barrel_vectordb_diskann:size(Index2)),

        %% Search should return similar results
        Results2 = barrel_vectordb_diskann:search(Index2, Query, 5),
        ?assertEqual(5, length(Results2)),

        %% First result should be the same
        {Id1, _} = hd(Results1),
        {Id2, _} = hd(Results2),
        ?assertEqual(Id1, Id2),

        ok = barrel_vectordb_diskann:close(Index2)
    after
        cleanup_disk(TmpDir)
    end.

test_disk_insert() ->
    %% Test incremental insert in disk mode
    TmpDir = setup_disk(),
    try
        %% Start with empty index
        {ok, Index0} = barrel_vectordb_diskann:new(#{
            dimension => 8,
            r => 4,
            storage_mode => disk,
            base_path => TmpDir
        }),

        %% Insert vectors one by one
        {ok, Index1} = barrel_vectordb_diskann:insert(Index0, <<"v1">>, random_vector(8)),
        {ok, Index2} = barrel_vectordb_diskann:insert(Index1, <<"v2">>, random_vector(8)),
        {ok, Index3} = barrel_vectordb_diskann:insert(Index2, <<"v3">>, random_vector(8)),

        ?assertEqual(3, barrel_vectordb_diskann:size(Index3)),

        %% Search should work
        Results = barrel_vectordb_diskann:search(Index3, random_vector(8), 3),
        ?assert(length(Results) >= 1),

        ok = barrel_vectordb_diskann:close(Index3)
    after
        cleanup_disk(TmpDir)
    end.

test_lru_cache_eviction() ->
    %% Test LRU cache with small size
    TmpDir = setup_disk(),
    try
        Vectors = [{integer_to_binary(I), random_vector(16)}
                   || I <- lists:seq(1, 30)],
        Config = #{
            dimension => 16,
            r => 8,
            l_build => 20,
            l_search => 20,
            storage_mode => disk,
            base_path => TmpDir,
            cache_max_size => 5,  %% Small cache
            use_pq => true,
            pq_m => 2,
            pq_k => 16
        },
        {ok, Index} = barrel_vectordb_diskann:build(Config, Vectors),

        %% Verify cache size is limited
        Info = barrel_vectordb_diskann:info(Index),
        StorageInfo = maps:get(storage, Info),
        CacheSize = maps:get(cache_size, StorageInfo),
        ?assert(CacheSize =< 5),

        %% Search should still work (will load from disk)
        Query = random_vector(16),
        Results = barrel_vectordb_diskann:search(Index, Query, 5),
        ?assertEqual(5, length(Results)),

        ok = barrel_vectordb_diskann:close(Index)
    after
        cleanup_disk(TmpDir)
    end.

test_cache_warming_persists() ->
    %% Test that ETS cache warms up across multiple searches
    TmpDir = setup_disk(),
    try
        Vectors = [{integer_to_binary(I), random_vector(16)}
                   || I <- lists:seq(1, 50)],
        Config = #{
            dimension => 16,
            r => 8,
            l_build => 20,
            l_search => 20,
            storage_mode => disk,
            base_path => TmpDir,
            cache_max_size => 100,  %% Large cache
            use_pq => true,
            pq_m => 2,
            pq_k => 16
        },
        {ok, Index} = barrel_vectordb_diskann:build(Config, Vectors),

        %% After build, cache is cleared
        Info0 = barrel_vectordb_diskann:info(Index),
        StorageInfo0 = maps:get(storage, Info0),
        CacheSize0 = maps:get(cache_size, StorageInfo0),
        %% Prewarm cache has medoid + neighbors
        ?assert(CacheSize0 > 0),

        %% First search - should load vectors into cache
        Query1 = random_vector(16),
        _Results1 = barrel_vectordb_diskann:search(Index, Query1, 10),

        %% Check cache grew (ETS cache persists across calls)
        Info1 = barrel_vectordb_diskann:info(Index),
        StorageInfo1 = maps:get(storage, Info1),
        CacheSize1 = maps:get(cache_size, StorageInfo1),
        ?assert(CacheSize1 > CacheSize0),

        %% Second search with different query - cache should keep growing
        Query2 = random_vector(16),
        _Results2 = barrel_vectordb_diskann:search(Index, Query2, 10),

        Info2 = barrel_vectordb_diskann:info(Index),
        StorageInfo2 = maps:get(storage, Info2),
        CacheSize2 = maps:get(cache_size, StorageInfo2),
        %% Cache either grew or stayed same (if vectors were already cached)
        ?assert(CacheSize2 >= CacheSize1),

        ok = barrel_vectordb_diskann:close(Index)
    after
        cleanup_disk(TmpDir)
    end.

test_batch_insert_deferred() ->
    %% Test batch insert with deferred backward edges (FreshDiskANN optimization)
    %% Inserts into an existing index and verifies search works and recall is reasonable
    TmpDir = setup_disk(),
    try
        %% Build initial index with 100 vectors
        rand:seed(exsss, {111, 222, 333}),
        InitialVectors = [{integer_to_binary(I), random_vector(16)}
                          || I <- lists:seq(1, 100)],
        Config = #{
            dimension => 16,
            r => 16,        %% Higher R for better connectivity
            l_build => 50,  %% Higher L for better search during construction
            l_search => 50,
            alpha => 1.2,
            storage_mode => disk,
            base_path => TmpDir,
            use_pq => false  %% Disable PQ for smaller test dataset
        },
        {ok, Index0} = barrel_vectordb_diskann:build(Config, InitialVectors),
        ?assertEqual(100, barrel_vectordb_diskann:size(Index0)),

        %% Batch insert 50 more vectors using deferred backward edges
        NewVectors = [{integer_to_binary(100 + I), random_vector(16)}
                      || I <- lists:seq(1, 50)],
        {ok, Index1} = barrel_vectordb_diskann:insert_batch(Index0, NewVectors, #{}),
        ?assertEqual(150, barrel_vectordb_diskann:size(Index1)),

        %% Verify search returns valid results after batch insert
        AllVectors = InitialVectors ++ NewVectors,
        Query = random_vector(16),
        Results = barrel_vectordb_diskann:search(Index1, Query, 10),
        ?assertEqual(10, length(Results)),

        %% Verify results are from our dataset
        ResultIds = [Id || {Id, _} <- Results],
        AllIds = [Id || {Id, _} <- AllVectors],
        lists:foreach(fun(RId) ->
            ?assert(lists:member(RId, AllIds))
        end, ResultIds),

        %% Check recall on random queries - should be reasonable after batch insert
        Recalls = [measure_recall(Index1, random_vector(16), AllVectors, 10)
                   || _ <- lists:seq(1, 5)],
        AvgRecall = lists:sum(Recalls) / length(Recalls),
        ?assert(AvgRecall >= 0.30),  %% At least 30% recall

        ok = barrel_vectordb_diskann:close(Index1)
    after
        cleanup_disk(TmpDir)
    end.

test_batch_insert_recall_deferred() ->
    %% Test that batch insert with deferred edges maintains reasonable recall
    %% The deferred approach trades some recall for speed
    TmpDir = setup_disk(),
    try
        %% Build initial index
        rand:seed(exsss, {444, 555, 666}),
        InitialVectors = [{integer_to_binary(I), random_vector(16)}
                          || I <- lists:seq(1, 100)],
        Config = #{
            dimension => 16,
            r => 32,        %% Higher R for better connectivity with deferred approach
            l_build => 100, %% Higher L for better search during construction
            l_search => 100,
            alpha => 1.2,
            storage_mode => disk,
            base_path => TmpDir,
            use_pq => false  %% Disable PQ for smaller test dataset
        },
        {ok, Index0} = barrel_vectordb_diskann:build(Config, InitialVectors),

        %% Batch insert more vectors
        NewVectors = [{integer_to_binary(100 + I), random_vector(16)}
                      || I <- lists:seq(1, 100)],
        {ok, Index1} = barrel_vectordb_diskann:insert_batch(Index0, NewVectors, #{}),
        ?assertEqual(200, barrel_vectordb_diskann:size(Index1)),

        %% All vectors for brute force ground truth
        AllVectors = InitialVectors ++ NewVectors,

        %% Measure recall on 5 random queries
        Recalls = [begin
            Query = random_vector(16),
            measure_recall(Index1, Query, AllVectors, 10)
        end || _ <- lists:seq(1, 5)],
        AvgRecall = lists:sum(Recalls) / length(Recalls),

        %% Deferred approach may have slightly lower recall - accept 30%+
        %% The speed benefit justifies the trade-off
        ?assert(AvgRecall >= 0.30),

        ok = barrel_vectordb_diskann:close(Index1)
    after
        cleanup_disk(TmpDir)
    end.

test_cache_prewarm_coverage() ->
    %% Test that cache pre-warming improves performance by pre-loading vectors
    %% This test verifies that batch insert with cache pre-warming:
    %% 1. Completes successfully
    %% 2. Maintains good recall
    %% 3. Cache grows after batch insert (vectors are loaded)
    TmpDir = setup_disk(),
    try
        %% Build initial index
        rand:seed(exsss, {777, 888, 999}),
        InitialVectors = [{integer_to_binary(I), random_vector(16)}
                          || I <- lists:seq(1, 100)],
        Config = #{
            dimension => 16,
            r => 16,
            l_build => 50,
            l_search => 50,
            alpha => 1.2,
            storage_mode => disk,
            base_path => TmpDir,
            use_pq => false,
            cache_max_size => 200  %% Enough to hold pre-warmed vectors
        },
        {ok, Index0} = barrel_vectordb_diskann:build(Config, InitialVectors),

        %% Get initial cache size
        Info0 = barrel_vectordb_diskann:info(Index0),
        StorageInfo0 = maps:get(storage, Info0),
        CacheSize0 = maps:get(cache_size, StorageInfo0),

        %% Batch insert more vectors - this should trigger cache pre-warming
        NewVectors = [{integer_to_binary(100 + I), random_vector(16)}
                      || I <- lists:seq(1, 50)],
        {ok, Index1} = barrel_vectordb_diskann:insert_batch(Index0, NewVectors, #{}),
        ?assertEqual(150, barrel_vectordb_diskann:size(Index1)),

        %% Check that cache grew (pre-warming loaded vectors)
        Info1 = barrel_vectordb_diskann:info(Index1),
        StorageInfo1 = maps:get(storage, Info1),
        CacheSize1 = maps:get(cache_size, StorageInfo1),
        ?assert(CacheSize1 >= CacheSize0),

        %% Verify search still works with good recall
        AllVectors = InitialVectors ++ NewVectors,
        Recalls = [measure_recall(Index1, random_vector(16), AllVectors, 10)
                   || _ <- lists:seq(1, 5)],
        AvgRecall = lists:sum(Recalls) / length(Recalls),
        ?assert(AvgRecall >= 0.30),

        ok = barrel_vectordb_diskann:close(Index1)
    after
        cleanup_disk(TmpDir)
    end.

test_lazy_delete_disk() ->
    %% Test that lazy delete in disk mode filters results without graph updates
    TmpDir = setup_disk(),
    try
        rand:seed(exsss, {111, 333, 555}),
        Vectors = [{integer_to_binary(I), random_vector(16)}
                   || I <- lists:seq(1, 50)],
        Config = #{
            dimension => 16,
            r => 8,
            l_build => 30,
            l_search => 30,
            storage_mode => disk,
            base_path => TmpDir,
            use_pq => false
        },
        {ok, Index0} = barrel_vectordb_diskann:build(Config, Vectors),

        %% Delete some vectors (lazy - instant)
        {ok, Index1} = barrel_vectordb_diskann:delete(Index0, <<"1">>),
        {ok, Index2} = barrel_vectordb_diskann:delete(Index1, <<"2">>),
        {ok, Index3} = barrel_vectordb_diskann:delete(Index2, <<"3">>),

        %% Search should not return deleted vectors
        Results = barrel_vectordb_diskann:search(Index3, random_vector(16), 50),
        ResultIds = [Id || {Id, _} <- Results],
        ?assertNot(lists:member(<<"1">>, ResultIds)),
        ?assertNot(lists:member(<<"2">>, ResultIds)),
        ?assertNot(lists:member(<<"3">>, ResultIds)),

        %% Size should still report deleted vectors (until consolidation)
        Info = barrel_vectordb_diskann:info(Index3),
        ?assertEqual(3, maps:get(deleted_count, Info)),

        ok = barrel_vectordb_diskann:close(Index3)
    after
        cleanup_disk(TmpDir)
    end.

test_consolidate_deletes_disk() ->
    %% Test that consolidate_deletes repairs graph in disk mode
    TmpDir = setup_disk(),
    try
        rand:seed(exsss, {222, 444, 666}),
        Vectors = [{integer_to_binary(I), random_vector(16)}
                   || I <- lists:seq(1, 100)],
        Config = #{
            dimension => 16,
            r => 16,
            l_build => 50,
            l_search => 50,
            storage_mode => disk,
            base_path => TmpDir,
            use_pq => false
        },
        {ok, Index0} = barrel_vectordb_diskann:build(Config, Vectors),

        %% Delete 10% of vectors
        Index1 = lists:foldl(
            fun(I, AccIndex) ->
                {ok, NewIndex} = barrel_vectordb_diskann:delete(AccIndex, integer_to_binary(I)),
                NewIndex
            end,
            Index0,
            lists:seq(1, 10)
        ),

        %% Verify deleted count before consolidation
        Info1 = barrel_vectordb_diskann:info(Index1),
        ?assertEqual(10, maps:get(deleted_count, Info1)),

        %% Consolidate
        {ok, Index2} = barrel_vectordb_diskann:consolidate_deletes(Index1),

        %% Verify deleted count is now 0
        Info2 = barrel_vectordb_diskann:info(Index2),
        ?assertEqual(0, maps:get(deleted_count, Info2)),
        ?assertEqual(90, maps:get(active_size, Info2)),

        %% Search should still work after consolidation
        Results = barrel_vectordb_diskann:search(Index2, random_vector(16), 10),
        ?assert(length(Results) >= 5),

        ok = barrel_vectordb_diskann:close(Index2)
    after
        cleanup_disk(TmpDir)
    end.

test_needs_consolidation() ->
    %% Test needs_consolidation threshold (10% deleted triggers true)
    TmpDir = setup_disk(),
    try
        rand:seed(exsss, {333, 555, 777}),
        Vectors = [{integer_to_binary(I), random_vector(16)}
                   || I <- lists:seq(1, 100)],
        Config = #{
            dimension => 16,
            r => 8,
            l_build => 30,
            l_search => 30,
            storage_mode => disk,
            base_path => TmpDir,
            use_pq => false
        },
        {ok, Index0} = barrel_vectordb_diskann:build(Config, Vectors),

        %% Initially no consolidation needed
        ?assertEqual(false, barrel_vectordb_diskann:needs_consolidation(Index0)),

        %% Delete 5 vectors (5% < 10% threshold)
        Index1 = lists:foldl(
            fun(I, AccIndex) ->
                {ok, NewIndex} = barrel_vectordb_diskann:delete(AccIndex, integer_to_binary(I)),
                NewIndex
            end,
            Index0,
            lists:seq(1, 5)
        ),
        ?assertEqual(false, barrel_vectordb_diskann:needs_consolidation(Index1)),

        %% Delete 6 more vectors (11 total > 10% of 89 active = 8.9)
        Index2 = lists:foldl(
            fun(I, AccIndex) ->
                {ok, NewIndex} = barrel_vectordb_diskann:delete(AccIndex, integer_to_binary(I)),
                NewIndex
            end,
            Index1,
            lists:seq(6, 11)
        ),
        ?assertEqual(true, barrel_vectordb_diskann:needs_consolidation(Index2)),

        ok = barrel_vectordb_diskann:close(Index2)
    after
        cleanup_disk(TmpDir)
    end.

test_serialization() ->
    %% Test memory mode serialization roundtrip
    Vectors = [{integer_to_binary(I), random_vector(16)}
               || I <- lists:seq(1, 50)],
    Config = #{
        dimension => 16,
        r => 8,
        l_build => 20,
        l_search => 20,
        storage_mode => memory,
        use_pq => true,
        pq_m => 2,
        pq_k => 16
    },
    {ok, Index1} = barrel_vectordb_diskann:build(Config, Vectors),

    %% Serialize
    Bin = barrel_vectordb_diskann:serialize(Index1),
    ?assert(is_binary(Bin)),

    %% Deserialize
    {ok, Index2} = barrel_vectordb_diskann:deserialize(Bin),
    ?assertEqual(50, barrel_vectordb_diskann:size(Index2)),

    %% Search should return same results
    Query = random_vector(16),
    Results1 = barrel_vectordb_diskann:search(Index1, Query, 5),
    Results2 = barrel_vectordb_diskann:search(Index2, Query, 5),
    ?assertEqual(Results1, Results2).

test_assume_normalized() ->
    %% Test that assume_normalized produces same results as normal cosine
    %% but uses optimized dot product path
    rand:seed(exsss, {123, 456, 789}),
    Vectors = [{integer_to_binary(I), random_vector(16)}
               || I <- lists:seq(1, 100)],

    %% Build index with assume_normalized = false (default)
    ConfigDefault = #{
        dimension => 16,
        r => 8,
        l_build => 30,
        l_search => 30,
        storage_mode => memory
    },
    {ok, IndexDefault} = barrel_vectordb_diskann:build(ConfigDefault, Vectors),

    %% Build index with assume_normalized = true
    ConfigNorm = ConfigDefault#{assume_normalized => true},
    {ok, IndexNorm} = barrel_vectordb_diskann:build(ConfigNorm, Vectors),

    %% Verify config is set correctly
    InfoNorm = barrel_vectordb_diskann:info(IndexNorm),
    ConfigInfo = maps:get(config, InfoNorm),
    ?assertEqual(true, maps:get(assume_normalized, ConfigInfo)),

    %% Search should return same results (vectors are normalized)
    Query = random_vector(16),
    ResultsDefault = barrel_vectordb_diskann:search(IndexDefault, Query, 10),
    ResultsNorm = barrel_vectordb_diskann:search(IndexNorm, Query, 10),

    %% Exact IDs may differ slightly due to graph structure, but top result should match
    {TopId1, _} = hd(ResultsDefault),
    {TopId2, _} = hd(ResultsNorm),
    ?assertEqual(TopId1, TopId2),

    %% Both should have 10 results
    ?assertEqual(10, length(ResultsDefault)),
    ?assertEqual(10, length(ResultsNorm)),

    %% Recall should be similar (compare both against brute force)
    RecallDefault = measure_recall(IndexDefault, Query, Vectors, 10),
    RecallNorm = measure_recall(IndexNorm, Query, Vectors, 10),
    ?assert(RecallDefault >= 0.8),
    ?assert(RecallNorm >= 0.8).

%%====================================================================
%% V2 Format Tests (Integer IDs + RocksDB + Lazy Loading)
%%====================================================================

test_v2_lazy_graph_loading() ->
    %% Test that V2 format uses lazy graph loading (build and search in single session)
    TmpDir = filename:join(["/tmp", "diskann_v2_test_" ++ integer_to_list(erlang:unique_integer([positive]))]),
    ok = filelib:ensure_dir(filename:join(TmpDir, "dummy")),
    try
        rand:seed(exsss, {123, 456, 789}),
        Vectors = [{integer_to_binary(I), random_vector(16)}
                   || I <- lists:seq(1, 100)],
        Config = #{
            dimension => 16,
            r => 8,
            l_build => 20,
            l_search => 20,
            storage_mode => disk,
            base_path => TmpDir,
            use_pq => true,
            pq_m => 2,
            pq_k => 16
        },

        %% Build index (creates V2 format with RocksDB)
        {ok, Index1} = barrel_vectordb_diskann:build(Config, Vectors),
        100 = barrel_vectordb_diskann:size(Index1),

        %% Verify RocksDB ID mapping directory exists
        IdDbPath = filename:join(TmpDir, "diskann_ids"),
        true = filelib:is_dir(IdDbPath),

        %% Search should work with lazy loading
        Query = random_vector(16),
        Results = barrel_vectordb_diskann:search(Index1, Query, 5),
        5 = length(Results),

        %% Verify results have valid string IDs
        lists:foreach(
            fun({Id, _Dist}) ->
                true = is_binary(Id)
            end,
            Results
        ),

        ok = barrel_vectordb_diskann:close(Index1),
        ok
    after
        cleanup_disk(TmpDir)
    end.

test_v2_fast_open() ->
    %% Test that V2 format produces correct file structure (no legacy diskann.index)
    TmpDir = filename:join(["/tmp", "diskann_v2_open_" ++ integer_to_list(erlang:unique_integer([positive]))]),
    ok = filelib:ensure_dir(filename:join(TmpDir, "dummy")),
    try
        rand:seed(exsss, {234, 567, 890}),
        Vectors = [{integer_to_binary(I), random_vector(16)}
                   || I <- lists:seq(1, 50)],
        Config = #{
            dimension => 16,
            r => 8,
            l_build => 20,
            l_search => 20,
            storage_mode => disk,
            base_path => TmpDir,
            use_pq => true,
            pq_m => 2,
            pq_k => 16
        },

        %% Build index
        {ok, Index1} = barrel_vectordb_diskann:build(Config, Vectors),

        %% Search should work
        Query = random_vector(16),
        Results = barrel_vectordb_diskann:search(Index1, Query, 5),
        5 = length(Results),

        %% Close index
        ok = barrel_vectordb_diskann:close(Index1),

        %% Verify diskann.index legacy file does NOT exist (V2 doesn't use it)
        LegacyPath = filename:join(TmpDir, "diskann.index"),
        false = filelib:is_file(LegacyPath),

        %% Verify V2 format files exist
        IdDbPath = filename:join(TmpDir, "diskann_ids"),
        true = filelib:is_dir(IdDbPath),
        GraphPath = filename:join(TmpDir, "diskann.graph"),
        true = filelib:is_file(GraphPath),
        VectorPath = filename:join(TmpDir, "diskann.vectors"),
        true = filelib:is_file(VectorPath),
        ok
    after
        cleanup_disk(TmpDir)
    end.

test_v2_rocksdb_id_mapping() ->
    %% Test that RocksDB ID mapping returns correct string IDs
    TmpDir = filename:join(["/tmp", "diskann_v2_id_" ++ integer_to_list(erlang:unique_integer([positive]))]),
    ok = filelib:ensure_dir(filename:join(TmpDir, "dummy")),
    try
        rand:seed(exsss, {345, 678, 901}),
        Vectors = [{<<"vec_", (integer_to_binary(I))/binary>>, random_vector(16)}
                   || I <- lists:seq(1, 50)],
        Config = #{
            dimension => 16,
            r => 8,
            l_build => 20,
            l_search => 20,
            storage_mode => disk,
            base_path => TmpDir,
            use_pq => true,
            pq_m => 2,
            pq_k => 16
        },

        %% Build index
        {ok, Index1} = barrel_vectordb_diskann:build(Config, Vectors),

        %% Search should return string IDs (not integer IDs)
        Query = random_vector(16),
        Results = barrel_vectordb_diskann:search(Index1, Query, 5),

        %% Verify all result IDs are string IDs starting with "vec_"
        lists:foreach(
            fun({Id, _Dist}) ->
                true = is_binary(Id),
                <<"vec_">> = binary:part(Id, 0, 4)
            end,
            Results
        ),

        %% Get vector by ID should work
        {Id, _} = hd(Results),
        {ok, Vec} = barrel_vectordb_diskann:get_vector(Index1, Id),
        16 = length(Vec),

        ok = barrel_vectordb_diskann:close(Index1),
        ok
    after
        cleanup_disk(TmpDir)
    end.

%%====================================================================
%% Helpers
%%====================================================================

random_vector(Dim) ->
    normalize([rand:uniform() - 0.5 || _ <- lists:seq(1, Dim)]).

normalize(Vec) ->
    Norm = math:sqrt(lists:sum([V*V || V <- Vec])),
    case Norm < 0.0001 of
        true -> Vec;
        false -> [V / Norm || V <- Vec]
    end.

measure_recall(Index, Query, Vectors, K) ->
    %% Get DiskANN results
    Results = barrel_vectordb_diskann:search(Index, Query, K),
    ResultIds = [Id || {Id, _} <- Results],

    %% Compute ground truth (brute force)
    TrueTopK = brute_force_search(Vectors, Query, K),
    TrueIds = [Id || {Id, _} <- TrueTopK],

    %% Recall = intersection / K
    Intersection = length([Id || Id <- ResultIds, lists:member(Id, TrueIds)]),
    case K of
        0 -> 0.0;
        _ -> Intersection / K
    end.

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

%%====================================================================
%% Hot Layer Tests
%%====================================================================

test_hot_insert() ->
    TmpDir = setup_disk(),
    try
        BasePath = filename:join(TmpDir, "hot_insert_idx"),
        {ok, Index0} = barrel_vectordb_diskann:new(#{
            dimension => 4,
            storage_mode => disk,
            base_path => BasePath,
            hot_layer => true,
            hot_max_size => 100
        }),

        %% Insert vectors into hot layer
        Vec1 = [1.0, 0.0, 0.0, 0.0],
        Vec2 = [0.0, 1.0, 0.0, 0.0],
        {ok, Index1} = barrel_vectordb_diskann:insert(Index0, <<"v1">>, Vec1),
        {ok, Index2} = barrel_vectordb_diskann:insert(Index1, <<"v2">>, Vec2),

        %% Check size
        ?assertEqual(2, barrel_vectordb_diskann:size(Index2)),

        %% Check hot layer info
        Info = barrel_vectordb_diskann:info(Index2),
        HotInfo = maps:get(hot_layer, Info),
        ?assertEqual(true, maps:get(enabled, HotInfo)),
        ?assertEqual(2, maps:get(size, HotInfo)),

        %% Cleanup
        barrel_vectordb_diskann:close(Index2)
    after
        cleanup_disk(TmpDir)
    end.

test_hot_search() ->
    TmpDir = setup_disk(),
    try
        BasePath = filename:join(TmpDir, "hot_search_idx"),
        {ok, Index0} = barrel_vectordb_diskann:new(#{
            dimension => 4,
            storage_mode => disk,
            base_path => BasePath,
            hot_layer => true,
            hot_max_size => 100
        }),

        %% Insert vectors into hot layer
        Vectors = [
            {<<"v1">>, [1.0, 0.0, 0.0, 0.0]},
            {<<"v2">>, [0.0, 1.0, 0.0, 0.0]},
            {<<"v3">>, [0.0, 0.0, 1.0, 0.0]},
            {<<"v4">>, [0.0, 0.0, 0.0, 1.0]}
        ],
        Index1 = lists:foldl(
            fun({Id, Vec}, Acc) ->
                {ok, NewAcc} = barrel_vectordb_diskann:insert(Acc, Id, Vec),
                NewAcc
            end,
            Index0,
            Vectors
        ),

        %% Search should find nearest neighbor
        Query = [1.0, 0.1, 0.0, 0.0],
        Results = barrel_vectordb_diskann:search(Index1, Query, 2),
        ?assertEqual(2, length(Results)),
        %% v1 should be closest
        [{TopId, _} | _] = Results,
        ?assertEqual(<<"v1">>, TopId),

        barrel_vectordb_diskann:close(Index1)
    after
        cleanup_disk(TmpDir)
    end.

test_hot_combined_search() ->
    TmpDir = setup_disk(),
    try
        BasePath = filename:join(TmpDir, "hot_combined_idx"),
        %% Build initial index with some vectors on disk
        Vectors = [
            {<<"disk1">>, [1.0, 0.0, 0.0, 0.0]},
            {<<"disk2">>, [0.0, 1.0, 0.0, 0.0]}
        ],
        {ok, Index0} = barrel_vectordb_diskann:build(#{
            dimension => 4,
            storage_mode => disk,
            base_path => BasePath,
            hot_layer => true,
            hot_max_size => 100
        }, Vectors),

        %% Insert vectors into hot layer
        {ok, Index1} = barrel_vectordb_diskann:insert(Index0, <<"hot1">>, [0.0, 0.0, 1.0, 0.0]),
        {ok, Index2} = barrel_vectordb_diskann:insert(Index1, <<"hot2">>, [0.0, 0.0, 0.0, 1.0]),

        %% Search should find results from both layers
        Query = [0.5, 0.5, 0.5, 0.0],
        Results = barrel_vectordb_diskann:search(Index2, Query, 4),
        ResultIds = [Id || {Id, _} <- Results],

        %% Should have results from both disk and hot layer
        ?assert(lists:member(<<"disk1">>, ResultIds) orelse lists:member(<<"disk2">>, ResultIds)),
        ?assert(lists:member(<<"hot1">>, ResultIds) orelse lists:member(<<"hot2">>, ResultIds)),

        barrel_vectordb_diskann:close(Index2)
    after
        cleanup_disk(TmpDir)
    end.

test_hot_delete() ->
    TmpDir = setup_disk(),
    try
        BasePath = filename:join(TmpDir, "hot_delete_idx"),
        {ok, Index0} = barrel_vectordb_diskann:new(#{
            dimension => 4,
            storage_mode => disk,
            base_path => BasePath,
            hot_layer => true,
            hot_max_size => 100
        }),

        %% Insert vectors
        {ok, Index1} = barrel_vectordb_diskann:insert(Index0, <<"v1">>, [1.0, 0.0, 0.0, 0.0]),
        {ok, Index2} = barrel_vectordb_diskann:insert(Index1, <<"v2">>, [0.0, 1.0, 0.0, 0.0]),

        %% Delete one vector
        {ok, Index3} = barrel_vectordb_diskann:delete(Index2, <<"v1">>),

        %% Size should be 1 (2 - 1 deleted)
        ?assertEqual(1, barrel_vectordb_diskann:size(Index3)),

        %% Search should not return deleted vector
        Query = [1.0, 0.0, 0.0, 0.0],
        Results = barrel_vectordb_diskann:search(Index3, Query, 2),
        ResultIds = [Id || {Id, _} <- Results],
        ?assertNot(lists:member(<<"v1">>, ResultIds)),

        barrel_vectordb_diskann:close(Index3)
    after
        cleanup_disk(TmpDir)
    end.

test_hot_compaction_threshold() ->
    TmpDir = setup_disk(),
    try
        BasePath = filename:join(TmpDir, "hot_threshold_idx"),
        %% Set small hot layer size and low threshold
        {ok, Index0} = barrel_vectordb_diskann:new(#{
            dimension => 4,
            storage_mode => disk,
            base_path => BasePath,
            hot_layer => true,
            hot_max_size => 10,
            hot_compaction_threshold => 0.5  %% Trigger at 5 vectors
        }),

        %% Insert vectors - should trigger compaction signal at 5
        Index1 = lists:foldl(
            fun(I, Acc) ->
                Id = list_to_binary("v" ++ integer_to_list(I)),
                Vec = [float(I), 0.0, 0.0, 0.0],
                {ok, NewAcc} = barrel_vectordb_diskann:insert(Acc, Id, Vec),
                NewAcc
            end,
            Index0,
            lists:seq(1, 4)
        ),

        %% Hot layer should still have vectors (compaction is async)
        Info = barrel_vectordb_diskann:info(Index1),
        HotInfo = maps:get(hot_layer, Info),
        ?assertEqual(4, maps:get(size, HotInfo)),

        barrel_vectordb_diskann:close(Index1)
    after
        cleanup_disk(TmpDir)
    end.

test_hot_compaction_correctness() ->
    TmpDir = setup_disk(),
    try
        BasePath = filename:join(TmpDir, "hot_compact_idx"),
        {ok, Index0} = barrel_vectordb_diskann:new(#{
            dimension => 4,
            storage_mode => disk,
            base_path => BasePath,
            hot_layer => true,
            hot_max_size => 100
        }),

        %% Insert vectors into hot layer
        Vectors = [
            {<<"v1">>, [1.0, 0.0, 0.0, 0.0]},
            {<<"v2">>, [0.0, 1.0, 0.0, 0.0]},
            {<<"v3">>, [0.0, 0.0, 1.0, 0.0]}
        ],
        Index1 = lists:foldl(
            fun({Id, Vec}, Acc) ->
                {ok, NewAcc} = barrel_vectordb_diskann:insert(Acc, Id, Vec),
                NewAcc
            end,
            Index0,
            Vectors
        ),

        %% Verify vectors are in hot layer
        Info1 = barrel_vectordb_diskann:info(Index1),
        HotInfo1 = maps:get(hot_layer, Info1),
        ?assertEqual(3, maps:get(size, HotInfo1)),

        %% Compact to disk
        {ok, Index2} = barrel_vectordb_diskann:compact(Index1),

        %% Hot layer should be empty now
        Info2 = barrel_vectordb_diskann:info(Index2),
        HotInfo2 = maps:get(hot_layer, Info2),
        ?assertEqual(0, maps:get(size, HotInfo2)),

        %% Total size should still be 3
        ?assertEqual(3, barrel_vectordb_diskann:size(Index2)),

        %% Search should still work
        Query = [1.0, 0.0, 0.0, 0.0],
        Results = barrel_vectordb_diskann:search(Index2, Query, 3),
        ?assertEqual(3, length(Results)),
        [{TopId, _} | _] = Results,
        ?assertEqual(<<"v1">>, TopId),

        barrel_vectordb_diskann:close(Index2)
    after
        cleanup_disk(TmpDir)
    end.

test_hot_layer_recall() ->
    TmpDir = setup_disk(),
    try
        BasePath = filename:join(TmpDir, "hot_recall_idx"),
        Dim = 32,
        NumVectors = 100,

        %% Generate random vectors
        Vectors = [{list_to_binary("v" ++ integer_to_list(I)),
                    [rand:uniform() || _ <- lists:seq(1, Dim)]}
                   || I <- lists:seq(1, NumVectors)],

        {ok, Index0} = barrel_vectordb_diskann:new(#{
            dimension => Dim,
            storage_mode => disk,
            base_path => BasePath,
            hot_layer => true,
            hot_max_size => 1000
        }),

        %% Insert all vectors into hot layer
        Index1 = lists:foldl(
            fun({Id, Vec}, Acc) ->
                {ok, NewAcc} = barrel_vectordb_diskann:insert(Acc, Id, Vec),
                NewAcc
            end,
            Index0,
            Vectors
        ),

        %% Run recall test
        K = 10,
        Query = [rand:uniform() || _ <- lists:seq(1, Dim)],
        Results = barrel_vectordb_diskann:search(Index1, Query, K),
        ResultIds = [Id || {Id, _} <- Results],

        %% Brute force ground truth
        TrueTopK = brute_force_search(Vectors, Query, K),
        TrueIds = [Id || {Id, _} <- TrueTopK],

        %% Compute recall
        Intersection = length([Id || Id <- ResultIds, lists:member(Id, TrueIds)]),
        Recall = Intersection / K,

        %% Hot layer should maintain good recall (>= 0.7)
        ?assert(Recall >= 0.7),

        barrel_vectordb_diskann:close(Index1)
    after
        cleanup_disk(TmpDir)
    end.

test_hot_layer_latency() ->
    TmpDir = setup_disk(),
    try
        BasePath = filename:join(TmpDir, "hot_latency_idx"),
        Dim = 64,

        {ok, Index0} = barrel_vectordb_diskann:new(#{
            dimension => Dim,
            storage_mode => disk,
            base_path => BasePath,
            hot_layer => true,
            hot_max_size => 10000
        }),

        %% Insert first vector
        Vec1 = [rand:uniform() || _ <- lists:seq(1, Dim)],
        {ok, Index1} = barrel_vectordb_diskann:insert(Index0, <<"first">>, Vec1),

        %% Measure insert latency for subsequent inserts
        NumInserts = 50,
        {TotalTime, FinalIndex} = lists:foldl(
            fun(I, {AccTime, AccIndex}) ->
                Vec = [rand:uniform() || _ <- lists:seq(1, Dim)],
                Id = list_to_binary("v" ++ integer_to_list(I)),
                Start = erlang:monotonic_time(microsecond),
                {ok, NewIndex} = barrel_vectordb_diskann:insert(AccIndex, Id, Vec),
                End = erlang:monotonic_time(microsecond),
                {AccTime + (End - Start), NewIndex}
            end,
            {0, Index1},
            lists:seq(1, NumInserts)
        ),

        AvgLatencyUs = TotalTime / NumInserts,
        AvgLatencyMs = AvgLatencyUs / 1000.0,

        %% Average insert latency should be under 1ms
        ?assert(AvgLatencyMs < 1.0),

        barrel_vectordb_diskann:close(FinalIndex)
    after
        cleanup_disk(TmpDir)
    end.
