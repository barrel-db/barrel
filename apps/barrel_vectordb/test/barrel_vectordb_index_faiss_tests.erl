%%%-------------------------------------------------------------------
%%% @doc Tests for FAISS backend (barrel_vectordb_index_faiss)
%%%
%%% These tests require barrel_faiss to be available.
%%% Run with: rebar3 as test_faiss eunit --module=barrel_vectordb_index_faiss_tests
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_index_faiss_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test setup
%%====================================================================

-define(DIM, 4).
-define(V1, [1.0, 0.0, 0.0, 0.0]).
-define(V2, [0.0, 1.0, 0.0, 0.0]).
-define(V3, [0.5, 0.5, 0.0, 0.0]).
-define(V4, [0.0, 0.0, 1.0, 0.0]).

%% Check if FAISS is available before running tests
faiss_available() ->
    barrel_vectordb_index:is_available(faiss).

%%====================================================================
%% FAISS backend tests
%%====================================================================

faiss_test_() ->
    case faiss_available() of
        true ->
            {foreach,
             fun() -> ok end,
             fun(_) -> ok end,
             [
                {"new creates empty index", fun test_faiss_new/0},
                {"insert adds vector", fun test_faiss_insert/0},
                {"search finds neighbors", fun test_faiss_search/0},
                {"search empty returns empty", fun test_faiss_search_empty/0},
                {"delete soft-deletes vector", fun test_faiss_delete/0},
                {"size returns active count", fun test_faiss_size/0},
                {"info returns metadata", fun test_faiss_info/0},
                {"serialization roundtrip", fun test_faiss_serialization/0},
                {"id mapping works correctly", fun test_faiss_id_mapping/0},
                {"update replaces vector", fun test_faiss_update/0},
                {"deleted count tracks deletions", fun test_faiss_deleted_count/0},
                {"compact rebuilds index", fun test_faiss_compact/0},
                {"cosine distance normalization", fun test_faiss_cosine/0}
             ]};
        false ->
            {"FAISS not available - skipping tests", fun() -> ok end}
    end.

test_faiss_new() ->
    {ok, Index} = barrel_vectordb_index_faiss:new(#{dimension => ?DIM}),
    ?assertEqual(0, barrel_vectordb_index_faiss:size(Index)),
    ok = barrel_vectordb_index_faiss:close(Index).

test_faiss_insert() ->
    {ok, Index0} = barrel_vectordb_index_faiss:new(#{dimension => ?DIM}),
    {ok, Index1} = barrel_vectordb_index_faiss:insert(Index0, <<"id1">>, ?V1),
    ?assertEqual(1, barrel_vectordb_index_faiss:size(Index1)),

    {ok, Index2} = barrel_vectordb_index_faiss:insert(Index1, <<"id2">>, ?V2),
    ?assertEqual(2, barrel_vectordb_index_faiss:size(Index2)),
    ok = barrel_vectordb_index_faiss:close(Index2).

test_faiss_search() ->
    {ok, Index0} = barrel_vectordb_index_faiss:new(#{dimension => ?DIM}),
    {ok, Index1} = barrel_vectordb_index_faiss:insert(Index0, <<"id1">>, ?V1),
    {ok, Index2} = barrel_vectordb_index_faiss:insert(Index1, <<"id2">>, ?V2),
    {ok, Index3} = barrel_vectordb_index_faiss:insert(Index2, <<"id3">>, ?V3),

    %% Search for vector similar to V1
    Results = barrel_vectordb_index_faiss:search(Index3, ?V1, 2),
    ?assertEqual(2, length(Results)),

    %% V1 should be closest to itself (distance ~ 0)
    [{TopId, TopDist} | _] = Results,
    ?assertEqual(<<"id1">>, TopId),
    ?assert(TopDist < 0.01),  %% Very close to 0
    ok = barrel_vectordb_index_faiss:close(Index3).

test_faiss_search_empty() ->
    {ok, Index} = barrel_vectordb_index_faiss:new(#{dimension => ?DIM}),
    Results = barrel_vectordb_index_faiss:search(Index, ?V1, 5),
    ?assertEqual([], Results),
    ok = barrel_vectordb_index_faiss:close(Index).

test_faiss_delete() ->
    {ok, Index0} = barrel_vectordb_index_faiss:new(#{dimension => ?DIM}),
    {ok, Index1} = barrel_vectordb_index_faiss:insert(Index0, <<"id1">>, ?V1),
    {ok, Index2} = barrel_vectordb_index_faiss:insert(Index1, <<"id2">>, ?V2),
    ?assertEqual(2, barrel_vectordb_index_faiss:size(Index2)),

    %% Soft delete
    {ok, Index3} = barrel_vectordb_index_faiss:delete(Index2, <<"id1">>),
    ?assertEqual(1, barrel_vectordb_index_faiss:size(Index3)),

    %% Search should not find deleted vector
    Results = barrel_vectordb_index_faiss:search(Index3, ?V1, 2),
    Ids = [Id || {Id, _} <- Results],
    ?assertNot(lists:member(<<"id1">>, Ids)),
    ok = barrel_vectordb_index_faiss:close(Index3).

test_faiss_size() ->
    {ok, Index0} = barrel_vectordb_index_faiss:new(#{dimension => ?DIM}),
    ?assertEqual(0, barrel_vectordb_index_faiss:size(Index0)),

    {ok, Index1} = barrel_vectordb_index_faiss:insert(Index0, <<"id1">>, ?V1),
    ?assertEqual(1, barrel_vectordb_index_faiss:size(Index1)),

    {ok, Index2} = barrel_vectordb_index_faiss:insert(Index1, <<"id2">>, ?V2),
    ?assertEqual(2, barrel_vectordb_index_faiss:size(Index2)),
    ok = barrel_vectordb_index_faiss:close(Index2).

test_faiss_info() ->
    {ok, Index} = barrel_vectordb_index_faiss:new(#{dimension => ?DIM}),
    Info = barrel_vectordb_index_faiss:info(Index),

    ?assert(is_map(Info)),
    ?assertEqual(faiss, maps:get(backend, Info)),
    ?assertEqual(?DIM, maps:get(dimension, Info)),
    ?assertEqual(0, maps:get(size, Info)),
    ok = barrel_vectordb_index_faiss:close(Index).

test_faiss_serialization() ->
    {ok, Index0} = barrel_vectordb_index_faiss:new(#{dimension => ?DIM}),
    {ok, Index1} = barrel_vectordb_index_faiss:insert(Index0, <<"id1">>, ?V1),
    {ok, Index2} = barrel_vectordb_index_faiss:insert(Index1, <<"id2">>, ?V2),

    %% Serialize
    Binary = barrel_vectordb_index_faiss:serialize(Index2),
    ?assert(is_binary(Binary)),
    ok = barrel_vectordb_index_faiss:close(Index2),

    %% Deserialize
    {ok, RestoredIndex} = barrel_vectordb_index_faiss:deserialize(Binary),
    ?assertEqual(2, barrel_vectordb_index_faiss:size(RestoredIndex)),

    %% Search should work on restored index
    Results = barrel_vectordb_index_faiss:search(RestoredIndex, ?V1, 2),
    ?assertEqual(2, length(Results)),
    ok = barrel_vectordb_index_faiss:close(RestoredIndex).

test_faiss_id_mapping() ->
    {ok, Index0} = barrel_vectordb_index_faiss:new(#{dimension => ?DIM}),

    %% Insert with various binary IDs
    {ok, Index1} = barrel_vectordb_index_faiss:insert(Index0, <<"doc-001">>, ?V1),
    {ok, Index2} = barrel_vectordb_index_faiss:insert(Index1, <<"doc-002">>, ?V2),
    {ok, Index3} = barrel_vectordb_index_faiss:insert(Index2, <<"uuid-abc-123">>, ?V3),

    %% Search and verify IDs are returned correctly
    Results = barrel_vectordb_index_faiss:search(Index3, ?V1, 3),
    Ids = [Id || {Id, _} <- Results],

    ?assert(lists:member(<<"doc-001">>, Ids)),
    ?assert(lists:member(<<"doc-002">>, Ids)),
    ?assert(lists:member(<<"uuid-abc-123">>, Ids)),
    ok = barrel_vectordb_index_faiss:close(Index3).

test_faiss_update() ->
    {ok, Index0} = barrel_vectordb_index_faiss:new(#{dimension => ?DIM}),
    {ok, Index1} = barrel_vectordb_index_faiss:insert(Index0, <<"id1">>, ?V1),

    %% Re-insert with different vector (update)
    {ok, Index2} = barrel_vectordb_index_faiss:insert(Index1, <<"id1">>, ?V2),

    %% Size should still be 1
    ?assertEqual(1, barrel_vectordb_index_faiss:size(Index2)),

    %% Search should find updated vector (closer to V2)
    Results = barrel_vectordb_index_faiss:search(Index2, ?V2, 1),
    [{Id, Dist}] = Results,
    ?assertEqual(<<"id1">>, Id),
    ?assert(Dist < 0.01),  %% Very close to V2
    ok = barrel_vectordb_index_faiss:close(Index2).

test_faiss_deleted_count() ->
    {ok, Index0} = barrel_vectordb_index_faiss:new(#{dimension => ?DIM}),
    {ok, Index1} = barrel_vectordb_index_faiss:insert(Index0, <<"id1">>, ?V1),
    {ok, Index2} = barrel_vectordb_index_faiss:insert(Index1, <<"id2">>, ?V2),

    ?assertEqual(0, barrel_vectordb_index_faiss:deleted_count(Index2)),

    {ok, Index3} = barrel_vectordb_index_faiss:delete(Index2, <<"id1">>),
    ?assertEqual(1, barrel_vectordb_index_faiss:deleted_count(Index3)),

    {ok, Index4} = barrel_vectordb_index_faiss:delete(Index3, <<"id2">>),
    ?assertEqual(2, barrel_vectordb_index_faiss:deleted_count(Index4)),
    ok = barrel_vectordb_index_faiss:close(Index4).

test_faiss_compact() ->
    {ok, Index0} = barrel_vectordb_index_faiss:new(#{dimension => ?DIM}),
    {ok, Index1} = barrel_vectordb_index_faiss:insert(Index0, <<"id1">>, ?V1),
    {ok, Index2} = barrel_vectordb_index_faiss:insert(Index1, <<"id2">>, ?V2),
    {ok, Index3} = barrel_vectordb_index_faiss:insert(Index2, <<"id3">>, ?V3),

    %% Delete one
    {ok, Index4} = barrel_vectordb_index_faiss:delete(Index3, <<"id2">>),
    ?assertEqual(2, barrel_vectordb_index_faiss:size(Index4)),
    ?assertEqual(1, barrel_vectordb_index_faiss:deleted_count(Index4)),

    %% Compact to remove deleted
    {ok, Compacted} = barrel_vectordb_index_faiss:compact(Index4),
    ?assertEqual(2, barrel_vectordb_index_faiss:size(Compacted)),
    ?assertEqual(0, barrel_vectordb_index_faiss:deleted_count(Compacted)),

    %% Search should still work
    Results = barrel_vectordb_index_faiss:search(Compacted, ?V1, 2),
    ?assertEqual(2, length(Results)),
    Ids = [Id || {Id, _} <- Results],
    ?assert(lists:member(<<"id1">>, Ids)),
    ?assertNot(lists:member(<<"id2">>, Ids)),
    ok = barrel_vectordb_index_faiss:close(Compacted).

test_faiss_cosine() ->
    %% Test cosine distance normalization
    {ok, Index0} = barrel_vectordb_index_faiss:new(#{dimension => ?DIM, distance_fn => cosine}),

    %% Insert vectors that differ only in magnitude
    {ok, Index1} = barrel_vectordb_index_faiss:insert(Index0, <<"unit">>, [1.0, 0.0, 0.0, 0.0]),
    {ok, Index2} = barrel_vectordb_index_faiss:insert(Index1, <<"scaled">>, [10.0, 0.0, 0.0, 0.0]),

    %% Search with unit vector - both should be equidistant (cosine ignores magnitude)
    Results = barrel_vectordb_index_faiss:search(Index2, [1.0, 0.0, 0.0, 0.0], 2),
    [{_, Dist1}, {_, Dist2}] = Results,

    %% Both distances should be very close to 0 (parallel vectors)
    ?assert(Dist1 < 0.01),
    ?assert(Dist2 < 0.01),
    ok = barrel_vectordb_index_faiss:close(Index2).

%%====================================================================
%% Store integration tests with FAISS backend
%%====================================================================

faiss_store_test_() ->
    case faiss_available() of
        true ->
            {foreach,
             fun setup_faiss_store/0,
             fun cleanup_faiss_store/1,
             [
                {"store with faiss backend", fun test_faiss_store_basic/0},
                {"store search with faiss", fun test_faiss_store_search/0},
                {"store persistence with faiss", fun test_faiss_store_persistence/0}
             ]};
        false ->
            {"FAISS not available - skipping store tests", fun() -> ok end}
    end.

setup_faiss_store() ->
    TestDir = "/tmp/barrel_vectordb_faiss_test_" ++
              integer_to_list(erlang:unique_integer([positive])),

    %% Mock embedder for deterministic tests
    (catch meck:unload(barrel_embed)),
    timer:sleep(10),
    meck:new(barrel_embed, [passthrough, no_link]),
    meck:expect(barrel_embed, init, fun(_Config) ->
        {ok, #{providers => [], dimension => 4, batch_size => 32}}
    end),
    meck:expect(barrel_embed, embed, fun(Text, _State) ->
        Hash = erlang:phash2(Text, 1000000),
        Vec = [Hash / 1000000.0, (Hash rem 1000) / 1000.0,
               ((Hash rem 100) / 100.0), ((Hash rem 10) / 10.0)],
        {ok, Vec}
    end),

    {ok, _Pid} = barrel_vectordb:start_link(#{
        name => faiss_test_store,
        path => TestDir,
        dimension => 4,
        backend => faiss
    }),
    TestDir.

cleanup_faiss_store(TestDir) ->
    catch barrel_vectordb:stop(faiss_test_store),
    timer:sleep(50),
    catch meck:unload(barrel_embed),
    os:cmd("rm -rf " ++ TestDir),
    ok.

test_faiss_store_basic() ->
    %% Add document
    ok = barrel_vectordb:add_vector(faiss_test_store, <<"doc1">>,
                                     <<"test document">>, #{type => test},
                                     [1.0, 0.0, 0.0, 0.0]),

    %% Get document
    {ok, Doc} = barrel_vectordb:get(faiss_test_store, <<"doc1">>),
    ?assertEqual(<<"doc1">>, maps:get(key, Doc)),
    ?assertEqual(<<"test document">>, maps:get(text, Doc)),

    %% Count
    ?assertEqual(1, barrel_vectordb:count(faiss_test_store)),

    %% Stats should show faiss backend
    {ok, Stats} = barrel_vectordb:stats(faiss_test_store),
    IndexInfo = maps:get(index, Stats),
    ?assertEqual(faiss, maps:get(backend, IndexInfo)).

test_faiss_store_search() ->
    %% Add multiple documents
    ok = barrel_vectordb:add_vector(faiss_test_store, <<"v1">>, <<"vec1">>, #{},
                                     [1.0, 0.0, 0.0, 0.0]),
    ok = barrel_vectordb:add_vector(faiss_test_store, <<"v2">>, <<"vec2">>, #{},
                                     [0.0, 1.0, 0.0, 0.0]),
    ok = barrel_vectordb:add_vector(faiss_test_store, <<"v3">>, <<"vec3">>, #{},
                                     [0.5, 0.5, 0.0, 0.0]),

    %% Search
    {ok, Results} = barrel_vectordb:search_vector(faiss_test_store,
                                                   [1.0, 0.0, 0.0, 0.0],
                                                   #{k => 2}),
    ?assertEqual(2, length(Results)),

    %% First result should be v1 (exact match)
    [First | _] = Results,
    ?assertEqual(<<"v1">>, maps:get(key, First)).

test_faiss_store_persistence() ->
    %% Add document
    ok = barrel_vectordb:add_vector(faiss_test_store, <<"persist1">>,
                                     <<"persistent doc">>, #{},
                                     [0.25, 0.25, 0.25, 0.25]),

    %% Checkpoint
    ok = barrel_vectordb:checkpoint(faiss_test_store),

    %% Document should still be accessible
    {ok, Doc} = barrel_vectordb:get(faiss_test_store, <<"persist1">>),
    ?assertEqual(<<"persistent doc">>, maps:get(text, Doc)).
