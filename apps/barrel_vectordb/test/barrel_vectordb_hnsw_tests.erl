%%%-------------------------------------------------------------------
%%% @doc EUnit tests for barrel_vectordb_hnsw module
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_hnsw_tests).

-include_lib("eunit/include/eunit.hrl").
-include("barrel_vectordb.hrl").

%%====================================================================
%% Test Generators
%%====================================================================

hnsw_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"new creates empty index", fun test_new/0},
        {"insert adds vectors", fun test_insert/0},
        {"search finds nearest neighbors", fun test_search/0},
        {"delete removes vectors", fun test_delete/0},
        {"serialization round trip", fun test_serialization/0},
        {"cosine distance correct", fun test_cosine_distance/0},
        {"euclidean distance correct", fun test_euclidean_distance/0},
        {"multi-layer index works", fun test_multi_layer/0},
        {"large scale recall", fun test_large_scale_recall/0},
        {"ef_search parameter", fun test_ef_search/0},
        {"delete maintains search quality", fun test_delete_search_quality/0}
     ]
    }.

%%====================================================================
%% Setup/Teardown
%%====================================================================

setup() ->
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_new() ->
    Index = barrel_vectordb_hnsw:new(),
    ?assertEqual(0, barrel_vectordb_hnsw:size(Index)),
    Info = barrel_vectordb_hnsw:info(Index),
    ?assertEqual(undefined, maps:get(entry_point, Info)),
    ?assertEqual(0, maps:get(size, Info)).

test_insert() ->
    Index0 = barrel_vectordb_hnsw:new(#{dimension => 3}),

    %% Insert first vector
    Vec1 = [1.0, 0.0, 0.0],
    Index1 = barrel_vectordb_hnsw:insert(Index0, <<"v1">>, Vec1),
    ?assertEqual(1, barrel_vectordb_hnsw:size(Index1)),

    %% Insert second vector
    Vec2 = [0.0, 1.0, 0.0],
    Index2 = barrel_vectordb_hnsw:insert(Index1, <<"v2">>, Vec2),
    ?assertEqual(2, barrel_vectordb_hnsw:size(Index2)),

    %% Insert third vector
    Vec3 = [0.0, 0.0, 1.0],
    Index3 = barrel_vectordb_hnsw:insert(Index2, <<"v3">>, Vec3),
    ?assertEqual(3, barrel_vectordb_hnsw:size(Index3)),

    %% Verify nodes exist
    ?assertMatch({ok, _}, barrel_vectordb_hnsw:get_node(Index3, <<"v1">>)),
    ?assertMatch({ok, _}, barrel_vectordb_hnsw:get_node(Index3, <<"v2">>)),
    ?assertMatch({ok, _}, barrel_vectordb_hnsw:get_node(Index3, <<"v3">>)).

test_search() ->
    %% Create index with some vectors
    Index0 = barrel_vectordb_hnsw:new(#{dimension => 3}),
    Vectors = [
        {<<"a">>, [1.0, 0.0, 0.0]},
        {<<"b">>, [0.9, 0.1, 0.0]},
        {<<"c">>, [0.0, 1.0, 0.0]},
        {<<"d">>, [0.0, 0.0, 1.0]},
        {<<"e">>, [0.5, 0.5, 0.0]}
    ],

    Index = lists:foldl(
        fun({Id, Vec}, Acc) ->
            barrel_vectordb_hnsw:insert(Acc, Id, Vec)
        end,
        Index0,
        Vectors
    ),

    %% Search for vector similar to [1.0, 0.0, 0.0]
    Query = [1.0, 0.0, 0.0],
    Results = barrel_vectordb_hnsw:search(Index, Query, 3),

    %% Should return top 3 results
    ?assertEqual(3, length(Results)),

    %% First result should be "a" (exact match)
    [{FirstId, FirstDist} | _] = Results,
    ?assertEqual(<<"a">>, FirstId),
    ?assert(FirstDist < 0.01),  %% Very close to 0

    %% Second should be "b" (very similar)
    [{_, _}, {SecondId, _} | _] = Results,
    ?assertEqual(<<"b">>, SecondId).

test_delete() ->
    Index0 = barrel_vectordb_hnsw:new(#{dimension => 3}),

    %% Insert vectors
    Index1 = barrel_vectordb_hnsw:insert(Index0, <<"v1">>, [1.0, 0.0, 0.0]),
    Index2 = barrel_vectordb_hnsw:insert(Index1, <<"v2">>, [0.0, 1.0, 0.0]),
    ?assertEqual(2, barrel_vectordb_hnsw:size(Index2)),

    %% Delete one
    Index3 = barrel_vectordb_hnsw:delete(Index2, <<"v1">>),
    ?assertEqual(1, barrel_vectordb_hnsw:size(Index3)),
    ?assertEqual(not_found, barrel_vectordb_hnsw:get_node(Index3, <<"v1">>)),
    ?assertMatch({ok, _}, barrel_vectordb_hnsw:get_node(Index3, <<"v2">>)).

test_serialization() ->
    %% Create index with vectors
    Index0 = barrel_vectordb_hnsw:new(#{dimension => 3}),
    Index1 = barrel_vectordb_hnsw:insert(Index0, <<"v1">>, [1.0, 0.0, 0.0]),
    Index2 = barrel_vectordb_hnsw:insert(Index1, <<"v2">>, [0.0, 1.0, 0.0]),

    %% Serialize
    Binary = barrel_vectordb_hnsw:serialize(Index2),
    ?assert(is_binary(Binary)),

    %% Deserialize
    {ok, Restored} = barrel_vectordb_hnsw:deserialize(Binary),
    ?assertEqual(2, barrel_vectordb_hnsw:size(Restored)),

    %% Search should work
    Results = barrel_vectordb_hnsw:search(Restored, [1.0, 0.0, 0.0], 1),
    ?assertEqual(1, length(Results)),
    [{ResultId, ResultDist}] = Results,
    ?assertEqual(<<"v1">>, ResultId),
    ?assert(is_float(ResultDist)).

test_cosine_distance() ->
    %% Same vector should have distance 0
    Vec1 = [1.0, 0.0, 0.0],
    ?assertEqual(0.0, barrel_vectordb_hnsw:cosine_distance(Vec1, Vec1)),

    %% Orthogonal vectors should have distance 1
    Vec2 = [0.0, 1.0, 0.0],
    Distance = barrel_vectordb_hnsw:cosine_distance(Vec1, Vec2),
    ?assert(abs(Distance - 1.0) < 0.0001),

    %% Opposite vectors should have distance 2
    Vec3 = [-1.0, 0.0, 0.0],
    Distance2 = barrel_vectordb_hnsw:cosine_distance(Vec1, Vec3),
    ?assert(abs(Distance2 - 2.0) < 0.0001),

    %% Similar vectors should have low distance
    Vec4 = [0.9, 0.1, 0.0],
    Distance3 = barrel_vectordb_hnsw:cosine_distance(Vec1, Vec4),
    ?assert(Distance3 < 0.1).

test_euclidean_distance() ->
    %% Same vector should have distance 0
    Vec1 = [1.0, 0.0, 0.0],
    ?assertEqual(0.0, barrel_vectordb_hnsw:euclidean_distance(Vec1, Vec1)),

    %% Unit vectors at right angles
    Vec2 = [0.0, 1.0, 0.0],
    Distance = barrel_vectordb_hnsw:euclidean_distance(Vec1, Vec2),
    Expected = math:sqrt(2.0),
    ?assert(abs(Distance - Expected) < 0.0001),

    %% Known distance
    Vec3 = [4.0, 0.0, 0.0],
    Vec4 = [0.0, 3.0, 0.0],
    Distance2 = barrel_vectordb_hnsw:euclidean_distance(Vec3, Vec4),
    ?assertEqual(5.0, Distance2).  %% 3-4-5 triangle

test_multi_layer() ->
    %% Insert enough vectors to trigger multi-layer structure
    Index0 = barrel_vectordb_hnsw:new(#{dimension => 8, m => 4, ef_construction => 20}),

    %% Generate random vectors
    Vectors = [
        {list_to_binary("v" ++ integer_to_list(I)),
         [rand:uniform() || _ <- lists:seq(1, 8)]}
        || I <- lists:seq(1, 50)
    ],

    Index = lists:foldl(
        fun({Id, Vec}, Acc) ->
            barrel_vectordb_hnsw:insert(Acc, Id, Vec)
        end,
        Index0,
        Vectors
    ),

    ?assertEqual(50, barrel_vectordb_hnsw:size(Index)),

    %% Search should still work
    QueryVec = [0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5],
    Results = barrel_vectordb_hnsw:search(Index, QueryVec, 5),
    ?assertEqual(5, length(Results)),

    %% Results should be sorted by distance
    Distances = [D || {_, D} <- Results],
    SortedDistances = lists:sort(Distances),
    ?assertEqual(SortedDistances, Distances).

test_large_scale_recall() ->
    %% Test recall on 1000+ vectors
    %% Using 32-dim vectors for reasonable test speed
    Dim = 32,
    N = 1000,
    K = 10,

    Index0 = barrel_vectordb_hnsw:new(#{
        dimension => Dim,
        m => 16,
        ef_construction => 100
    }),

    %% Generate random vectors with known seed for reproducibility
    rand:seed(exsss, {1, 2, 3}),
    Vectors = [
        {list_to_binary("v" ++ integer_to_list(I)),
         normalize([rand:uniform() - 0.5 || _ <- lists:seq(1, Dim)])}
        || I <- lists:seq(1, N)
    ],

    Index = lists:foldl(
        fun({Id, Vec}, Acc) ->
            barrel_vectordb_hnsw:insert(Acc, Id, Vec)
        end,
        Index0,
        Vectors
    ),

    ?assertEqual(N, barrel_vectordb_hnsw:size(Index)),

    %% Query with one of the existing vectors - should find itself first
    {QueryId, QueryVec} = lists:nth(500, Vectors),
    Results = barrel_vectordb_hnsw:search(Index, QueryVec, K),

    ?assertEqual(K, length(Results)),

    %% First result should be exact match (allowing small quantization error)
    [{FirstId, FirstDist} | _] = Results,
    ?assertEqual(QueryId, FirstId),
    ?assert(FirstDist < 0.01),  %% Quantized vectors have small error

    %% Compute recall: how many of HNSW results are in true top-K
    TrueTopK = brute_force_search(Vectors, QueryVec, K),
    TrueIds = [Id || {Id, _} <- TrueTopK],
    HnswIds = [Id || {Id, _} <- Results],
    Recall = length([Id || Id <- HnswIds, lists:member(Id, TrueIds)]) / K,

    %% HNSW should achieve at least 90% recall
    ?assert(Recall >= 0.9).

test_ef_search() ->
    %% Test that ef_search parameter affects search quality
    Dim = 16,
    N = 200,

    Index0 = barrel_vectordb_hnsw:new(#{
        dimension => Dim,
        m => 8,
        ef_construction => 50
    }),

    rand:seed(exsss, {4, 5, 6}),
    Vectors = [
        {list_to_binary("v" ++ integer_to_list(I)),
         normalize([rand:uniform() - 0.5 || _ <- lists:seq(1, Dim)])}
        || I <- lists:seq(1, N)
    ],

    Index = lists:foldl(
        fun({Id, Vec}, Acc) ->
            barrel_vectordb_hnsw:insert(Acc, Id, Vec)
        end,
        Index0,
        Vectors
    ),

    %% Query vector
    QueryVec = normalize([rand:uniform() - 0.5 || _ <- lists:seq(1, Dim)]),
    K = 5,

    %% Search with low ef_search
    Results1 = barrel_vectordb_hnsw:search(Index, QueryVec, K, #{ef_search => 10}),
    ?assertEqual(K, length(Results1)),

    %% Search with high ef_search
    Results2 = barrel_vectordb_hnsw:search(Index, QueryVec, K, #{ef_search => 100}),
    ?assertEqual(K, length(Results2)),

    %% Both should return valid results (sorted by distance)
    Distances1 = [D || {_, D} <- Results1],
    Distances2 = [D || {_, D} <- Results2],
    ?assertEqual(lists:sort(Distances1), Distances1),
    ?assertEqual(lists:sort(Distances2), Distances2),

    %% Higher ef_search should generally find same or better results
    %% (first result distance should be <= for high ef_search)
    [{_, Dist1} | _] = Results1,
    [{_, Dist2} | _] = Results2,
    ?assert(Dist2 =< Dist1 + 0.01).  %% Allow small tolerance

test_delete_search_quality() ->
    %% Test that search still works well after many deletions
    Dim = 8,
    N = 100,

    Index0 = barrel_vectordb_hnsw:new(#{
        dimension => Dim,
        m => 8,
        ef_construction => 50
    }),

    rand:seed(exsss, {7, 8, 9}),
    Vectors = [
        {list_to_binary("v" ++ integer_to_list(I)),
         normalize([rand:uniform() - 0.5 || _ <- lists:seq(1, Dim)])}
        || I <- lists:seq(1, N)
    ],

    Index1 = lists:foldl(
        fun({Id, Vec}, Acc) ->
            barrel_vectordb_hnsw:insert(Acc, Id, Vec)
        end,
        Index0,
        Vectors
    ),

    %% Delete 50% of vectors (every other one)
    ToDelete = [Id || {Id, _} <- Vectors,
                      begin
                          IdStr = binary_to_list(Id),
                          NumStr = string:substr(IdStr, 2),  %% Remove "v" prefix
                          (list_to_integer(NumStr) rem 2) == 0
                      end],
    Index2 = lists:foldl(
        fun(Id, Acc) ->
            barrel_vectordb_hnsw:delete(Acc, Id)
        end,
        Index1,
        ToDelete
    ),

    ?assertEqual(N - length(ToDelete), barrel_vectordb_hnsw:size(Index2)),

    %% Search with a query vector
    QueryVec = normalize([rand:uniform() - 0.5 || _ <- lists:seq(1, Dim)]),
    Results = barrel_vectordb_hnsw:search(Index2, QueryVec, 10),

    %% Should return some results
    ?assert(length(Results) > 0),

    %% Results should be sorted by distance
    Distances = [D || {_, D} <- Results],
    ?assertEqual(lists:sort(Distances), Distances),

    %% No deleted vectors should appear in results
    ResultIds = [Id || {Id, _} <- Results],
    DeletedInResults = [Id || Id <- ResultIds, lists:member(Id, ToDelete)],
    ?assertEqual([], DeletedInResults),

    %% Verify deleted nodes are actually gone
    lists:foreach(fun(Id) ->
        ?assertEqual(not_found, barrel_vectordb_hnsw:get_node(Index2, Id))
    end, lists:sublist(ToDelete, 5)).

%%====================================================================
%% Helpers
%%====================================================================

%% Normalize vector to unit length
normalize(Vec) ->
    Norm = math:sqrt(lists:sum([V*V || V <- Vec])),
    case Norm < 0.0001 of
        true -> Vec;
        false -> [V / Norm || V <- Vec]
    end.

%% Brute force search for ground truth
brute_force_search(Vectors, Query, K) ->
    Distances = [{Id, barrel_vectordb_hnsw:cosine_distance(Query, Vec)}
                 || {Id, Vec} <- Vectors],
    Sorted = lists:sort(fun({_, D1}, {_, D2}) -> D1 =< D2 end, Distances),
    lists:sublist(Sorted, K).

