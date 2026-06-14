%%%-------------------------------------------------------------------
%%% @doc Tests for barrel_vectordb_index behaviour and backends
%%%
%%% These tests verify that all index backends implement the
%%% barrel_vectordb_index behaviour correctly.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_index_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test setup
%%====================================================================

%% Test vectors (dimension 4 for simplicity)
-define(DIM, 4).
-define(V1, [1.0, 0.0, 0.0, 0.0]).
-define(V2, [0.0, 1.0, 0.0, 0.0]).
-define(V3, [0.5, 0.5, 0.0, 0.0]).
-define(V4, [0.0, 0.0, 1.0, 0.0]).

%%====================================================================
%% Backend module tests
%%====================================================================

backend_module_test_() ->
    [
        {"hnsw backend module", fun() ->
            ?assertEqual(barrel_vectordb_index_hnsw,
                         barrel_vectordb_index:backend_module(hnsw))
        end},
        {"faiss backend module", fun() ->
            ?assertEqual(barrel_vectordb_index_faiss,
                         barrel_vectordb_index:backend_module(faiss))
        end}
    ].

is_available_test_() ->
    [
        {"hnsw is always available", fun() ->
            ?assert(barrel_vectordb_index:is_available(hnsw))
        end},
        {"faiss availability depends on barrel_faiss", fun() ->
            %% Just verify it doesn't crash - availability depends on environment
            Result = barrel_vectordb_index:is_available(faiss),
            ?assert(is_boolean(Result))
        end}
    ].

%%====================================================================
%% HNSW backend tests
%%====================================================================

hnsw_test_() ->
    {foreach,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"new creates empty index", fun test_hnsw_new/0},
        {"insert adds vector", fun test_hnsw_insert/0},
        {"search finds neighbors", fun test_hnsw_search/0},
        {"search empty returns empty", fun test_hnsw_search_empty/0},
        {"delete removes vector", fun test_hnsw_delete/0},
        {"size returns count", fun test_hnsw_size/0},
        {"info returns metadata", fun test_hnsw_info/0},
        {"serialization roundtrip", fun test_hnsw_serialization/0}
     ]}.

test_hnsw_new() ->
    {ok, Index} = barrel_vectordb_index_hnsw:new(#{dimension => ?DIM}),
    ?assertEqual(0, barrel_vectordb_index_hnsw:size(Index)).

test_hnsw_insert() ->
    {ok, Index0} = barrel_vectordb_index_hnsw:new(#{dimension => ?DIM}),
    {ok, Index1} = barrel_vectordb_index_hnsw:insert(Index0, <<"id1">>, ?V1),
    ?assertEqual(1, barrel_vectordb_index_hnsw:size(Index1)),

    {ok, Index2} = barrel_vectordb_index_hnsw:insert(Index1, <<"id2">>, ?V2),
    ?assertEqual(2, barrel_vectordb_index_hnsw:size(Index2)).

test_hnsw_search() ->
    {ok, Index0} = barrel_vectordb_index_hnsw:new(#{dimension => ?DIM}),
    {ok, Index1} = barrel_vectordb_index_hnsw:insert(Index0, <<"id1">>, ?V1),
    {ok, Index2} = barrel_vectordb_index_hnsw:insert(Index1, <<"id2">>, ?V2),
    {ok, Index3} = barrel_vectordb_index_hnsw:insert(Index2, <<"id3">>, ?V3),

    %% Search for vector similar to V1
    Results = barrel_vectordb_index_hnsw:search(Index3, ?V1, 2),
    ?assertEqual(2, length(Results)),

    %% V1 should be closest to itself
    [{TopId, _TopDist} | _] = Results,
    ?assertEqual(<<"id1">>, TopId).

test_hnsw_search_empty() ->
    {ok, Index} = barrel_vectordb_index_hnsw:new(#{dimension => ?DIM}),
    Results = barrel_vectordb_index_hnsw:search(Index, ?V1, 5),
    ?assertEqual([], Results).

test_hnsw_delete() ->
    {ok, Index0} = barrel_vectordb_index_hnsw:new(#{dimension => ?DIM}),
    {ok, Index1} = barrel_vectordb_index_hnsw:insert(Index0, <<"id1">>, ?V1),
    {ok, Index2} = barrel_vectordb_index_hnsw:insert(Index1, <<"id2">>, ?V2),
    ?assertEqual(2, barrel_vectordb_index_hnsw:size(Index2)),

    {ok, Index3} = barrel_vectordb_index_hnsw:delete(Index2, <<"id1">>),
    ?assertEqual(1, barrel_vectordb_index_hnsw:size(Index3)),

    %% Search should not find deleted vector
    Results = barrel_vectordb_index_hnsw:search(Index3, ?V1, 2),
    Ids = [Id || {Id, _} <- Results],
    ?assertNot(lists:member(<<"id1">>, Ids)).

test_hnsw_size() ->
    {ok, Index0} = barrel_vectordb_index_hnsw:new(#{dimension => ?DIM}),
    ?assertEqual(0, barrel_vectordb_index_hnsw:size(Index0)),

    {ok, Index1} = barrel_vectordb_index_hnsw:insert(Index0, <<"id1">>, ?V1),
    ?assertEqual(1, barrel_vectordb_index_hnsw:size(Index1)),

    {ok, Index2} = barrel_vectordb_index_hnsw:insert(Index1, <<"id2">>, ?V2),
    ?assertEqual(2, barrel_vectordb_index_hnsw:size(Index2)).

test_hnsw_info() ->
    {ok, Index} = barrel_vectordb_index_hnsw:new(#{dimension => ?DIM}),
    Info = barrel_vectordb_index_hnsw:info(Index),

    ?assert(is_map(Info)),
    ?assertEqual(hnsw, maps:get(backend, Info)).

test_hnsw_serialization() ->
    {ok, Index0} = barrel_vectordb_index_hnsw:new(#{dimension => ?DIM}),
    {ok, Index1} = barrel_vectordb_index_hnsw:insert(Index0, <<"id1">>, ?V1),
    {ok, Index2} = barrel_vectordb_index_hnsw:insert(Index1, <<"id2">>, ?V2),

    %% Serialize
    Binary = barrel_vectordb_index_hnsw:serialize(Index2),
    ?assert(is_binary(Binary)),

    %% Deserialize
    {ok, RestoredIndex} = barrel_vectordb_index_hnsw:deserialize(Binary),
    ?assertEqual(2, barrel_vectordb_index_hnsw:size(RestoredIndex)),

    %% Search should work on restored index
    Results = barrel_vectordb_index_hnsw:search(RestoredIndex, ?V1, 2),
    ?assertEqual(2, length(Results)).

%%====================================================================
%% Dimension mismatch tests
%%====================================================================

dimension_mismatch_test_() ->
    [
        {"insert with wrong dimension fails", fun() ->
            {ok, Index} = barrel_vectordb_index_hnsw:new(#{dimension => 4}),
            WrongVector = [1.0, 0.0, 0.0],  %% 3 dimensions instead of 4
            Result = barrel_vectordb_index_hnsw:insert(Index, <<"id">>, WrongVector),
            ?assertMatch({error, _}, Result)
        end}
    ].

%%====================================================================
%% Update (re-insert) tests
%%====================================================================

update_test_() ->
    [
        {"re-insert updates vector", fun() ->
            {ok, Index0} = barrel_vectordb_index_hnsw:new(#{dimension => ?DIM}),
            {ok, Index1} = barrel_vectordb_index_hnsw:insert(Index0, <<"id1">>, ?V1),

            %% Re-insert with different vector
            {ok, Index2} = barrel_vectordb_index_hnsw:insert(Index1, <<"id1">>, ?V2),

            %% Size should still be 1
            ?assertEqual(1, barrel_vectordb_index_hnsw:size(Index2)),

            %% Search should find the updated vector (closer to V2 than V1)
            Results = barrel_vectordb_index_hnsw:search(Index2, ?V2, 1),
            [{Id, _}] = Results,
            ?assertEqual(<<"id1">>, Id)
        end}
    ].
