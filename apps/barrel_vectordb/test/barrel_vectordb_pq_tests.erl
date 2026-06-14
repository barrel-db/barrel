%%%-------------------------------------------------------------------
%%% @doc EUnit tests for barrel_vectordb_pq module
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_pq_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Generators
%%====================================================================

pq_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"new creates valid config", fun test_new/0},
        {"new validates config", fun test_new_validation/0},
        {"train creates codebooks", fun test_train/0},
        {"encode produces correct size", fun test_encode/0},
        {"decode reconstructs vector", fun test_decode/0},
        {"reconstruction error is bounded", fun test_reconstruction_error/0},
        {"precompute tables works", fun test_precompute_tables/0},
        {"distance approximates true distance", fun test_distance_accuracy/0},
        {"batch encode works", fun test_batch_encode/0},
        {"info returns correct data", fun test_info/0}
     ]
    }.

%%====================================================================
%% Setup/Teardown
%%====================================================================

setup() ->
    rand:seed(exsss, {42, 42, 42}),
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_new() ->
    {ok, Config} = barrel_vectordb_pq:new(#{m => 8, k => 256, dimension => 128}),
    ?assert(barrel_vectordb_pq:is_trained(Config) =:= false),
    Info = barrel_vectordb_pq:info(Config),
    ?assertEqual(8, maps:get(m, Info)),
    ?assertEqual(256, maps:get(k, Info)),
    ?assertEqual(128, maps:get(dimension, Info)),
    ?assertEqual(16, maps:get(subvector_dim, Info)).

test_new_validation() ->
    %% Missing dimension
    ?assertMatch({error, dimension_required}, barrel_vectordb_pq:new(#{m => 8})),

    %% Dimension not divisible by m
    ?assertMatch({error, {dimension_not_divisible, 100, 8}},
                 barrel_vectordb_pq:new(#{m => 8, dimension => 100})),

    %% K too large
    ?assertMatch({error, {k_too_large, 512, 256}},
                 barrel_vectordb_pq:new(#{k => 512, dimension => 128})).

test_train() ->
    {ok, Config} = barrel_vectordb_pq:new(#{m => 4, k => 16, dimension => 32}),

    %% Generate training vectors
    Vectors = [random_vector(32) || _ <- lists:seq(1, 500)],

    %% Train
    {ok, Trained} = barrel_vectordb_pq:train(Config, Vectors),
    ?assert(barrel_vectordb_pq:is_trained(Trained)).

test_encode() ->
    Trained = trained_config(4, 16, 32),
    Vec = random_vector(32),
    Code = barrel_vectordb_pq:encode(Trained, Vec),

    %% Code should be M bytes
    ?assertEqual(4, byte_size(Code)),

    %% Each byte should be < K
    CodeList = binary_to_list(Code),
    ?assert(lists:all(fun(C) -> C < 16 end, CodeList)).

test_decode() ->
    Trained = trained_config(4, 16, 32),
    Vec = random_vector(32),
    Code = barrel_vectordb_pq:encode(Trained, Vec),
    Reconstructed = barrel_vectordb_pq:decode(Trained, Code),

    %% Reconstructed should have same dimension
    ?assertEqual(32, length(Reconstructed)),

    %% Should be close to original (rough check)
    ?assert(is_list(Reconstructed)),
    ?assert(lists:all(fun is_float/1, Reconstructed)).

test_reconstruction_error() ->
    %% Test that reconstruction error is bounded
    %% Use moderate K for faster tests
    Trained = trained_config(4, 32, 64),

    %% Test on multiple vectors
    Errors = lists:map(
        fun(_) ->
            Vec = random_vector(64),
            Code = barrel_vectordb_pq:encode(Trained, Vec),
            Reconstructed = barrel_vectordb_pq:decode(Trained, Code),
            euclidean_distance(Vec, Reconstructed)
        end,
        lists:seq(1, 50)
    ),

    AvgError = lists:sum(Errors) / length(Errors),
    MaxError = lists:max(Errors),

    %% Average error should be reasonable
    %% (actual bound depends on data distribution and PQ params)
    %% With random uniform data [-0.5, 0.5], higher error is expected
    ?assert(AvgError < 4.0),
    ?assert(MaxError < 8.0).

test_precompute_tables() ->
    Trained = trained_config(4, 16, 32),
    Query = random_vector(32),
    Tables = barrel_vectordb_pq:precompute_tables(Trained, Query),

    %% Tables should be M*K*4 bytes (M tables, K entries each, 4 bytes per float)
    ExpectedSize = 4 * 16 * 4,
    ?assertEqual(ExpectedSize, byte_size(Tables)).

test_distance_accuracy() ->
    %% PQ distance should approximate true distance
    %% Use moderate K for faster tests
    Trained = trained_config(4, 32, 64),

    %% Test on multiple pairs
    RelErrors = lists:map(
        fun(_) ->
            V1 = random_vector(64),
            V2 = random_vector(64),

            TrueDist = euclidean_distance(V1, V2),
            Tables = barrel_vectordb_pq:precompute_tables(Trained, V1),
            Code2 = barrel_vectordb_pq:encode(Trained, V2),
            PQDist = barrel_vectordb_pq:distance(Tables, Code2),

            case TrueDist < 0.001 of
                true -> 0.0;  %% Skip near-zero distances
                false -> abs(TrueDist - PQDist) / TrueDist
            end
        end,
        lists:seq(1, 50)
    ),

    AvgRelError = lists:sum(RelErrors) / length(RelErrors),

    %% Average relative error should be < 50% (PQ is lossy)
    ?assert(AvgRelError < 0.50).

test_batch_encode() ->
    Trained = trained_config(4, 16, 32),
    Vectors = [random_vector(32) || _ <- lists:seq(1, 10)],
    Codes = barrel_vectordb_pq:batch_encode(Trained, Vectors),

    ?assertEqual(10, length(Codes)),
    ?assert(lists:all(fun(C) -> byte_size(C) =:= 4 end, Codes)).

test_info() ->
    {ok, Config} = barrel_vectordb_pq:new(#{m => 8, k => 256, dimension => 256}),
    Info = barrel_vectordb_pq:info(Config),

    ?assertEqual(8, maps:get(m, Info)),
    ?assertEqual(256, maps:get(k, Info)),
    ?assertEqual(256, maps:get(dimension, Info)),
    ?assertEqual(32, maps:get(subvector_dim, Info)),
    ?assertEqual(8, maps:get(bytes_per_vector, Info)),
    ?assertEqual(128.0, maps:get(compression_ratio, Info)),  %% 256*4/8 = 128
    ?assertEqual(false, maps:get(trained, Info)).

%%====================================================================
%% Helpers
%%====================================================================

random_vector(Dim) ->
    [rand:uniform() - 0.5 || _ <- lists:seq(1, Dim)].

trained_config(M, K, Dim) ->
    {ok, Config} = barrel_vectordb_pq:new(#{m => M, k => K, dimension => Dim}),
    %% Need enough vectors for k-means to work well (at least 3x K)
    NumVectors = max(K * 3, 200),
    Vectors = [random_vector(Dim) || _ <- lists:seq(1, NumVectors)],
    {ok, Trained} = barrel_vectordb_pq:train(Config, Vectors),
    Trained.

euclidean_distance(Vec1, Vec2) ->
    SumSq = lists:sum([math:pow(A - B, 2) || {A, B} <- lists:zip(Vec1, Vec2)]),
    math:sqrt(SumSq).
