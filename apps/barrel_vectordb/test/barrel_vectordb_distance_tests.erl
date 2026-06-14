%%%-------------------------------------------------------------------
%%% @doc EUnit tests for barrel_vectordb_distance module
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_distance_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Generators
%%====================================================================

distance_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"dot product list", fun test_dot_product_list/0},
        {"dot product binary", fun test_dot_product_binary/0},
        {"cosine distance list", fun test_cosine_distance_list/0},
        {"cosine distance binary", fun test_cosine_distance_binary/0},
        {"cosine distance normalized", fun test_cosine_distance_normalized/0},
        {"euclidean distance list", fun test_euclidean_distance_list/0},
        {"euclidean distance binary", fun test_euclidean_distance_binary/0},
        {"list to binary roundtrip", fun test_list_binary_roundtrip/0},
        {"simd info", fun test_simd_info/0},
        {"mixed vector types", fun test_mixed_vectors/0}
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

test_dot_product_list() ->
    %% Simple vectors: 1*1 + 2*2 + 3*3 + 4*4 = 1 + 4 + 9 + 16 = 30
    ?assertEqual(30.0, barrel_vectordb_distance:dot_product([1.0, 2.0, 3.0, 4.0], [1.0, 2.0, 3.0, 4.0])),

    %% Orthogonal vectors
    ?assertEqual(0.0, barrel_vectordb_distance:dot_product([1.0, 0.0], [0.0, 1.0])),

    %% Opposite vectors
    ?assertEqual(-14.0, barrel_vectordb_distance:dot_product([1.0, 2.0, 3.0], [-1.0, -2.0, -3.0])).

test_dot_product_binary() ->
    Vec1 = barrel_vectordb_distance:to_binary([1.0, 2.0, 3.0, 4.0]),
    Vec2 = barrel_vectordb_distance:to_binary([1.0, 2.0, 3.0, 4.0]),

    Result = barrel_vectordb_distance:dot_product(Vec1, Vec2),
    ?assert(abs(Result - 30.0) < 0.001).

test_cosine_distance_list() ->
    %% Identical vectors -> distance = 0
    Vec = [1.0, 2.0, 3.0],
    Result1 = barrel_vectordb_distance:cosine_distance(Vec, Vec),
    ?assert(abs(Result1) < 0.001),

    %% Orthogonal vectors -> distance = 1
    V1 = [1.0, 0.0],
    V2 = [0.0, 1.0],
    Result2 = barrel_vectordb_distance:cosine_distance(V1, V2),
    ?assert(abs(Result2 - 1.0) < 0.001),

    %% Opposite vectors -> distance = 2
    V3 = [1.0, 0.0],
    V4 = [-1.0, 0.0],
    Result3 = barrel_vectordb_distance:cosine_distance(V3, V4),
    ?assert(abs(Result3 - 2.0) < 0.001).

test_cosine_distance_binary() ->
    Vec1 = barrel_vectordb_distance:to_binary([1.0, 2.0, 3.0]),
    Vec2 = barrel_vectordb_distance:to_binary([1.0, 2.0, 3.0]),

    Result = barrel_vectordb_distance:cosine_distance(Vec1, Vec2),
    ?assert(abs(Result) < 0.001).

test_cosine_distance_normalized() ->
    %% Pre-normalized vectors
    Vec1 = normalize([1.0, 2.0, 3.0]),
    Vec2 = normalize([1.0, 2.0, 3.0]),

    %% Same result as regular cosine distance for normalized vectors
    Result1 = barrel_vectordb_distance:cosine_distance_normalized(Vec1, Vec2),
    Result2 = barrel_vectordb_distance:cosine_distance(Vec1, Vec2),

    ?assert(abs(Result1 - Result2) < 0.001).

test_euclidean_distance_list() ->
    %% Same point -> distance = 0
    Result1 = barrel_vectordb_distance:euclidean_distance([1.0, 2.0, 3.0], [1.0, 2.0, 3.0]),
    ?assert(abs(Result1) < 0.001),

    %% Unit distance
    Result2 = barrel_vectordb_distance:euclidean_distance([0.0, 0.0], [1.0, 0.0]),
    ?assert(abs(Result2 - 1.0) < 0.001),

    %% 3-4-5 triangle
    Result3 = barrel_vectordb_distance:euclidean_distance([0.0, 0.0], [3.0, 4.0]),
    ?assert(abs(Result3 - 5.0) < 0.001).

test_euclidean_distance_binary() ->
    Vec1 = barrel_vectordb_distance:to_binary([0.0, 0.0]),
    Vec2 = barrel_vectordb_distance:to_binary([3.0, 4.0]),

    Result = barrel_vectordb_distance:euclidean_distance(Vec1, Vec2),
    ?assert(abs(Result - 5.0) < 0.001).

test_list_binary_roundtrip() ->
    Original = [1.5, 2.5, 3.5, 4.5, 5.5],
    Bin = barrel_vectordb_distance:to_binary(Original),
    Recovered = barrel_vectordb_distance:from_binary(Bin),

    %% Check lengths match
    ?assertEqual(length(Original), length(Recovered)),

    %% Check values match (within float precision)
    lists:foreach(
        fun({O, R}) ->
            ?assert(abs(O - R) < 0.0001)
        end,
        lists:zip(Original, Recovered)
    ).

test_simd_info() ->
    Info = barrel_vectordb_distance:simd_info(),
    ?assert(is_map(Info)),
    ?assert(maps:is_key(backend, Info)),

    Backend = maps:get(backend, Info),
    ?assert(Backend =:= avx2 orelse Backend =:= neon orelse Backend =:= scalar orelse Backend =:= erlang).

test_mixed_vectors() ->
    %% Test that we can mix list and binary vectors
    List = [1.0, 2.0, 3.0],
    Bin = barrel_vectordb_distance:to_binary([1.0, 2.0, 3.0]),

    %% List + Binary
    Result1 = barrel_vectordb_distance:dot_product(List, Bin),
    %% Binary + List
    Result2 = barrel_vectordb_distance:dot_product(Bin, List),
    %% Both list
    Result3 = barrel_vectordb_distance:dot_product(List, List),

    ?assert(abs(Result1 - Result3) < 0.001),
    ?assert(abs(Result2 - Result3) < 0.001).

%%====================================================================
%% Helpers
%%====================================================================

normalize(Vec) ->
    Norm = math:sqrt(lists:sum([V*V || V <- Vec])),
    case Norm < 0.0001 of
        true -> Vec;
        false -> [V / Norm || V <- Vec]
    end.
