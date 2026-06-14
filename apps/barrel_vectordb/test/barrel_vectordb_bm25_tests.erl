%%%-------------------------------------------------------------------
%%% @doc Unit tests for barrel_vectordb_bm25
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_bm25_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Generators
%%====================================================================

bm25_test_() ->
    [
        {"new creates empty index", fun test_new/0},
        {"new with custom config", fun test_new_custom/0},
        {"add single document", fun test_add_single/0},
        {"add multiple documents", fun test_add_multiple/0},
        {"add overwrites existing doc", fun test_add_overwrite/0},
        {"remove document", fun test_remove/0},
        {"remove non-existent doc", fun test_remove_not_found/0},
        {"search empty index", fun test_search_empty/0},
        {"search single term", fun test_search_single_term/0},
        {"search multiple terms", fun test_search_multiple_terms/0},
        {"search returns sorted results", fun test_search_sorted/0},
        {"search respects k limit", fun test_search_k_limit/0},
        {"search case insensitive", fun test_search_case_insensitive/0},
        {"get_vector returns sparse vector", fun test_get_vector/0},
        {"get_vector not found", fun test_get_vector_not_found/0},
        {"encode creates sparse vector", fun test_encode/0},
        {"stats returns index info", fun test_stats/0},
        {"tokenizer handles punctuation", fun test_tokenizer_punctuation/0},
        {"tokenizer min length filter", fun test_tokenizer_min_length/0}
    ].

%%====================================================================
%% Tests
%%====================================================================

test_new() ->
    Index = barrel_vectordb_bm25:new(),
    Stats = barrel_vectordb_bm25:stats(Index),
    ?assertEqual(0, maps:get(total_docs, Stats)),
    ?assertEqual(0, maps:get(vocab_size, Stats)).

test_new_custom() ->
    Index = barrel_vectordb_bm25:new(#{k1 => 1.5, b => 0.5}),
    Stats = barrel_vectordb_bm25:stats(Index),
    Config = maps:get(config, Stats),
    ?assertEqual(1.5, maps:get(k1, Config)),
    ?assertEqual(0.5, maps:get(b, Config)).

test_add_single() ->
    Index0 = barrel_vectordb_bm25:new(),
    Index1 = barrel_vectordb_bm25:add(Index0, <<"doc1">>, <<"Hello world">>),
    Stats = barrel_vectordb_bm25:stats(Index1),
    ?assertEqual(1, maps:get(total_docs, Stats)),
    ?assertEqual(2, maps:get(vocab_size, Stats)),
    ?assertEqual(2, maps:get(total_tokens, Stats)).

test_add_multiple() ->
    Index0 = barrel_vectordb_bm25:new(),
    Index1 = barrel_vectordb_bm25:add(Index0, <<"doc1">>, <<"Hello world">>),
    Index2 = barrel_vectordb_bm25:add(Index1, <<"doc2">>, <<"Hello there">>),
    Index3 = barrel_vectordb_bm25:add(Index2, <<"doc3">>, <<"Goodbye world">>),
    Stats = barrel_vectordb_bm25:stats(Index3),
    ?assertEqual(3, maps:get(total_docs, Stats)),
    ?assertEqual(4, maps:get(vocab_size, Stats)).  %% hello, world, there, goodbye

test_add_overwrite() ->
    Index0 = barrel_vectordb_bm25:new(),
    Index1 = barrel_vectordb_bm25:add(Index0, <<"doc1">>, <<"Hello world">>),
    Index2 = barrel_vectordb_bm25:add(Index1, <<"doc1">>, <<"Goodbye moon">>),
    Stats = barrel_vectordb_bm25:stats(Index2),
    ?assertEqual(1, maps:get(total_docs, Stats)),
    %% Should have goodbye, moon (not hello, world)
    Results = barrel_vectordb_bm25:search(Index2, <<"hello">>, 10),
    ?assertEqual([], Results),
    Results2 = barrel_vectordb_bm25:search(Index2, <<"goodbye">>, 10),
    ?assertEqual(1, length(Results2)).

test_remove() ->
    Index0 = barrel_vectordb_bm25:new(),
    Index1 = barrel_vectordb_bm25:add(Index0, <<"doc1">>, <<"Hello world">>),
    Index2 = barrel_vectordb_bm25:add(Index1, <<"doc2">>, <<"Hello there">>),
    Index3 = barrel_vectordb_bm25:remove(Index2, <<"doc1">>),
    Stats = barrel_vectordb_bm25:stats(Index3),
    ?assertEqual(1, maps:get(total_docs, Stats)),
    ?assertEqual(2, maps:get(vocab_size, Stats)).  %% hello, there

test_remove_not_found() ->
    Index0 = barrel_vectordb_bm25:new(),
    Index1 = barrel_vectordb_bm25:add(Index0, <<"doc1">>, <<"Hello world">>),
    Index2 = barrel_vectordb_bm25:remove(Index1, <<"doc_not_exist">>),
    Stats = barrel_vectordb_bm25:stats(Index2),
    ?assertEqual(1, maps:get(total_docs, Stats)).

test_search_empty() ->
    Index = barrel_vectordb_bm25:new(),
    Results = barrel_vectordb_bm25:search(Index, <<"hello">>, 10),
    ?assertEqual([], Results).

test_search_single_term() ->
    Index0 = barrel_vectordb_bm25:new(),
    Index1 = barrel_vectordb_bm25:add(Index0, <<"doc1">>, <<"Hello world">>),
    Index2 = barrel_vectordb_bm25:add(Index1, <<"doc2">>, <<"Goodbye world">>),
    Results = barrel_vectordb_bm25:search(Index2, <<"hello">>, 10),
    ?assertEqual(1, length(Results)),
    [{DocId, Score}] = Results,
    ?assertEqual(<<"doc1">>, DocId),
    ?assert(Score > 0).

test_search_multiple_terms() ->
    Index0 = barrel_vectordb_bm25:new(),
    Index1 = barrel_vectordb_bm25:add(Index0, <<"doc1">>, <<"Hello beautiful world">>),
    Index2 = barrel_vectordb_bm25:add(Index1, <<"doc2">>, <<"Hello there">>),
    %% Search for "hello world" should rank doc1 higher (has both terms)
    Results = barrel_vectordb_bm25:search(Index2, <<"hello world">>, 10),
    ?assertEqual(2, length(Results)),
    [{TopDocId, _} | _] = Results,
    ?assertEqual(<<"doc1">>, TopDocId).

test_search_sorted() ->
    Index0 = barrel_vectordb_bm25:new(),
    Index1 = barrel_vectordb_bm25:add(Index0, <<"doc1">>, <<"apple">>),
    Index2 = barrel_vectordb_bm25:add(Index1, <<"doc2">>, <<"apple apple">>),
    Index3 = barrel_vectordb_bm25:add(Index2, <<"doc3">>, <<"apple apple apple">>),
    Results = barrel_vectordb_bm25:search(Index3, <<"apple">>, 10),
    ?assertEqual(3, length(Results)),
    %% Check scores are in descending order
    Scores = [S || {_, S} <- Results],
    ?assertEqual(Scores, lists:reverse(lists:sort(Scores))).

test_search_k_limit() ->
    Index0 = barrel_vectordb_bm25:new(),
    Index1 = barrel_vectordb_bm25:add(Index0, <<"doc1">>, <<"test">>),
    Index2 = barrel_vectordb_bm25:add(Index1, <<"doc2">>, <<"test">>),
    Index3 = barrel_vectordb_bm25:add(Index2, <<"doc3">>, <<"test">>),
    Index4 = barrel_vectordb_bm25:add(Index3, <<"doc4">>, <<"test">>),
    Results = barrel_vectordb_bm25:search(Index4, <<"test">>, 2),
    ?assertEqual(2, length(Results)).

test_search_case_insensitive() ->
    Index0 = barrel_vectordb_bm25:new(),
    Index1 = barrel_vectordb_bm25:add(Index0, <<"doc1">>, <<"Hello World">>),
    Results1 = barrel_vectordb_bm25:search(Index1, <<"hello">>, 10),
    Results2 = barrel_vectordb_bm25:search(Index1, <<"HELLO">>, 10),
    Results3 = barrel_vectordb_bm25:search(Index1, <<"HeLLo">>, 10),
    ?assertEqual(1, length(Results1)),
    ?assertEqual(1, length(Results2)),
    ?assertEqual(1, length(Results3)).

test_get_vector() ->
    Index0 = barrel_vectordb_bm25:new(),
    Index1 = barrel_vectordb_bm25:add(Index0, <<"doc1">>, <<"Hello world">>),
    {ok, Vector} = barrel_vectordb_bm25:get_vector(Index1, <<"doc1">>),
    ?assert(is_map(Vector)),
    ?assert(maps:is_key(<<"hello">>, Vector)),
    ?assert(maps:is_key(<<"world">>, Vector)),
    ?assert(maps:get(<<"hello">>, Vector) > 0),
    ?assert(maps:get(<<"world">>, Vector) > 0).

test_get_vector_not_found() ->
    Index = barrel_vectordb_bm25:new(),
    Result = barrel_vectordb_bm25:get_vector(Index, <<"doc1">>),
    ?assertEqual({error, not_found}, Result).

test_encode() ->
    Index0 = barrel_vectordb_bm25:new(),
    Index1 = barrel_vectordb_bm25:add(Index0, <<"doc1">>, <<"Hello world">>),
    Index2 = barrel_vectordb_bm25:add(Index1, <<"doc2">>, <<"Goodbye world">>),
    %% Encode new text using index statistics
    Vector = barrel_vectordb_bm25:encode(Index2, <<"Hello there">>),
    ?assert(is_map(Vector)),
    ?assert(maps:is_key(<<"hello">>, Vector)),
    ?assert(maps:is_key(<<"there">>, Vector)).

test_stats() ->
    Index0 = barrel_vectordb_bm25:new(),
    Index1 = barrel_vectordb_bm25:add(Index0, <<"doc1">>, <<"Hello world">>),
    Index2 = barrel_vectordb_bm25:add(Index1, <<"doc2">>, <<"Hello there friend">>),
    Stats = barrel_vectordb_bm25:stats(Index2),
    ?assertEqual(2, maps:get(total_docs, Stats)),
    ?assertEqual(5, maps:get(total_tokens, Stats)),
    ?assertEqual(4, maps:get(vocab_size, Stats)),  %% hello, world, there, friend
    ?assertEqual(2.5, maps:get(avg_doc_length, Stats)).

test_tokenizer_punctuation() ->
    Index0 = barrel_vectordb_bm25:new(),
    Index1 = barrel_vectordb_bm25:add(Index0, <<"doc1">>, <<"Hello, world! How are you?">>),
    Stats = barrel_vectordb_bm25:stats(Index1),
    ?assertEqual(5, maps:get(vocab_size, Stats)),  %% hello, world, how, are, you
    Results = barrel_vectordb_bm25:search(Index1, <<"hello">>, 10),
    ?assertEqual(1, length(Results)).

test_tokenizer_min_length() ->
    Index0 = barrel_vectordb_bm25:new(#{min_term_length => 3}),
    Index1 = barrel_vectordb_bm25:add(Index0, <<"doc1">>, <<"I am a test">>),
    Stats = barrel_vectordb_bm25:stats(Index1),
    ?assertEqual(1, maps:get(vocab_size, Stats)),  %% only "test" (length >= 3)
    Results = barrel_vectordb_bm25:search(Index1, <<"am">>, 10),
    ?assertEqual([], Results).  %% "am" filtered out
