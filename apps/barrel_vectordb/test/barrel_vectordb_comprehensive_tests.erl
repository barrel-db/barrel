%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for barrel_vectordb inspired by ChromaDB
%%%
%%% Test categories:
%%% - Basic CRUD operations
%%% - Batch operations with various sizes
%%% - Search accuracy and distance ordering
%%% - Metadata filtering
%%% - Edge cases (duplicates, empty stores, dimension mismatches)
%%% - Invariants (count validation, data integrity)
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_comprehensive_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Generators
%%====================================================================

comprehensive_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     {foreach,
      fun setup_test/0,
      fun cleanup_test/1,
      [
        %% Basic CRUD
        {"add and retrieve single document", fun test_add_single/0},
        {"add with explicit vector", fun test_add_with_vector/0},
        {"get non-existent returns not_found", fun test_get_not_found/0},
        {"delete removes document", fun test_delete/0},
        {"delete non-existent is ok", fun test_delete_nonexistent/0},
        {"update by re-adding with same id", fun test_update/0},

        %% Batch operations (inspired by ChromaDB test_add_* patterns)
        {"add batch small (5 docs)", fun test_batch_small/0},
        {"add batch medium (50 docs)", fun test_batch_medium/0},

        %% Search accuracy (inspired by ChromaDB ann_accuracy invariant)
        {"search returns correct order by distance", fun test_search_ordering/0},
        {"search exact match has highest score", fun test_search_exact_match/0},
        {"search k limits results", fun test_search_k_limit/0},
        {"search with vector query", fun test_search_vector/0},
        {"search empty store returns empty", fun test_search_empty/0},

        %% Metadata filtering (inspired by ChromaDB test_filtering)
        {"filter by equality", fun test_filter_equality/0},
        {"filter by custom function", fun test_filter_function/0},
        {"filter returns subset", fun test_filter_subset/0},

        %% Edge cases
        {"dimension mismatch rejected", fun test_dimension_mismatch/0},
        {"duplicate id overwrites", fun test_duplicate_id/0},
        {"empty text handled", fun test_empty_text/0},
        {"unicode text handled", fun test_unicode_text/0},
        {"large metadata handled", fun test_large_metadata/0},

        %% Invariants (inspired by ChromaDB invariants.py)
        {"count matches actual documents", fun test_invariant_count/0},
        {"no duplicate ids in results", fun test_invariant_no_duplicates/0},
        {"data integrity after operations", fun test_invariant_data_integrity/0},
        {"score ordering invariant", fun test_invariant_score_ordering/0}
      ]
     }
    }.

%%====================================================================
%% Setup/Teardown
%%====================================================================

setup() ->
    application:ensure_all_started(rocksdb),
    ok.

cleanup(_) ->
    ok.

setup_test() ->
    TestDir = "/tmp/barrel_vectordb_comprehensive_" ++
              integer_to_list(erlang:unique_integer([positive])),

    %% Ensure meck is clean
    (catch meck:unload(barrel_embed)),
    timer:sleep(10),

    %% Mock embedder with deterministic vectors based on text hash
    meck:new(barrel_embed, [passthrough, no_link]),
    meck:expect(barrel_embed, init, fun(_Config) ->
        {ok, #{providers => [], dimension => 8, batch_size => 32}}
    end),
    meck:expect(barrel_embed, embed, fun(Text, _State) ->
        {ok, text_to_vector(Text, 8)}
    end),
    meck:expect(barrel_embed, embed_batch, fun(Texts, _State) ->
        Vectors = [text_to_vector(T, 8) || T <- Texts],
        {ok, Vectors}
    end),
    meck:expect(barrel_embed, info, fun(_State) ->
        #{providers => [], dimension => 8}
    end),

    %% Start the store
    {ok, Pid} = barrel_vectordb:start_link(#{
        name => comprehensive_test_store,
        path => TestDir,
        dimension => 8,
        hnsw => #{m => 8, ef_construction => 50}
    }),
    {Pid, TestDir}.

cleanup_test({_Pid, TestDir}) ->
    catch barrel_vectordb:stop(comprehensive_test_store),
    timer:sleep(50),
    (catch meck:unload(barrel_embed)),
    os:cmd("rm -rf " ++ TestDir),
    ok.

%% Generate deterministic normalized vector from text
text_to_vector(Text, Dim) ->
    Hash = erlang:phash2(Text, 1000000),
    RawVec = [math:sin(Hash + I * 0.7) || I <- lists:seq(1, Dim)],
    normalize(RawVec).

%% L2 normalize a vector
normalize(Vec) ->
    Norm = math:sqrt(lists:sum([X*X || X <- Vec])),
    case Norm < 1.0e-10 of
        true -> Vec;
        false -> [X / Norm || X <- Vec]
    end.

%% Check if two vectors are approximately equal (for float32 storage)
vectors_approx_equal(V1, V2) ->
    vectors_approx_equal(V1, V2, 1.0e-5).

vectors_approx_equal([], [], _Eps) ->
    true;
vectors_approx_equal([A | As], [B | Bs], Eps) ->
    abs(A - B) < Eps andalso vectors_approx_equal(As, Bs, Eps);
vectors_approx_equal(_, _, _) ->
    false.

%%====================================================================
%% Basic CRUD Tests
%%====================================================================

test_add_single() ->
    ok = barrel_vectordb:add(comprehensive_test_store,
                             <<"doc1">>,
                             <<"Hello world">>,
                             #{type => greeting}),

    {ok, Doc} = barrel_vectordb:get(comprehensive_test_store, <<"doc1">>),
    ?assertEqual(<<"doc1">>, maps:get(key, Doc)),
    ?assertEqual(<<"Hello world">>, maps:get(text, Doc)),
    ?assertEqual(#{type => greeting}, maps:get(metadata, Doc)),
    ?assertEqual(8, length(maps:get(vector, Doc))).

test_add_with_vector() ->
    Vector = normalize([1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
    ok = barrel_vectordb:add_vector(comprehensive_test_store,
                                    <<"vec1">>,
                                    <<"explicit vector">>,
                                    #{source => explicit},
                                    Vector),

    {ok, Doc} = barrel_vectordb:get(comprehensive_test_store, <<"vec1">>),
    ?assertEqual(Vector, maps:get(vector, Doc)).

test_get_not_found() ->
    ?assertEqual(not_found,
                 barrel_vectordb:get(comprehensive_test_store, <<"nonexistent">>)).

test_delete() ->
    ok = barrel_vectordb:add(comprehensive_test_store,
                             <<"to_delete">>,
                             <<"delete me">>,
                             #{}),
    ?assertMatch({ok, _}, barrel_vectordb:get(comprehensive_test_store, <<"to_delete">>)),

    ok = barrel_vectordb:delete(comprehensive_test_store, <<"to_delete">>),
    ?assertEqual(not_found, barrel_vectordb:get(comprehensive_test_store, <<"to_delete">>)).

test_delete_nonexistent() ->
    %% Deleting non-existent document should succeed (idempotent)
    ok = barrel_vectordb:delete(comprehensive_test_store, <<"never_existed">>).

test_update() ->
    %% Add initial document
    ok = barrel_vectordb:add(comprehensive_test_store,
                             <<"update_me">>,
                             <<"version 1">>,
                             #{version => 1}),

    {ok, Doc1} = barrel_vectordb:get(comprehensive_test_store, <<"update_me">>),
    ?assertEqual(<<"version 1">>, maps:get(text, Doc1)),
    ?assertEqual(#{version => 1}, maps:get(metadata, Doc1)),

    %% Update by re-adding with same ID
    ok = barrel_vectordb:add(comprehensive_test_store,
                             <<"update_me">>,
                             <<"version 2">>,
                             #{version => 2}),

    {ok, Doc2} = barrel_vectordb:get(comprehensive_test_store, <<"update_me">>),
    ?assertEqual(<<"version 2">>, maps:get(text, Doc2)),
    ?assertEqual(#{version => 2}, maps:get(metadata, Doc2)).

%%====================================================================
%% Batch Operations Tests (inspired by ChromaDB test_add_* patterns)
%%====================================================================

test_batch_small() ->
    Docs = [{list_to_binary("batch_s_" ++ integer_to_list(I)),
             list_to_binary("Small batch doc " ++ integer_to_list(I)),
             #{index => I}}
            || I <- lists:seq(1, 5)],

    {ok, #{inserted := 5}} = barrel_vectordb:add_batch(comprehensive_test_store, Docs),
    ?assertEqual(5, barrel_vectordb:count(comprehensive_test_store)),

    %% Verify each document
    lists:foreach(fun({Id, Text, Meta}) ->
        {ok, Doc} = barrel_vectordb:get(comprehensive_test_store, Id),
        ?assertEqual(Text, maps:get(text, Doc)),
        ?assertEqual(Meta, maps:get(metadata, Doc))
    end, Docs).

test_batch_medium() ->
    Docs = [{list_to_binary("batch_m_" ++ integer_to_list(I)),
             list_to_binary("Medium batch document number " ++ integer_to_list(I)),
             #{index => I, batch => medium}}
            || I <- lists:seq(1, 50)],

    {ok, #{inserted := 50}} = barrel_vectordb:add_batch(comprehensive_test_store, Docs),
    ?assertEqual(50, barrel_vectordb:count(comprehensive_test_store)).

%%====================================================================
%% Search Accuracy Tests (inspired by ChromaDB ann_accuracy invariant)
%%====================================================================

test_search_ordering() ->
    %% Add documents with known vectors at different distances
    V1 = normalize([1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
    V2 = normalize([0.9, 0.1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
    V3 = normalize([0.7, 0.3, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
    V4 = normalize([0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),

    ok = barrel_vectordb:add_vector(comprehensive_test_store, <<"s1">>, <<"closest">>, #{}, V1),
    ok = barrel_vectordb:add_vector(comprehensive_test_store, <<"s2">>, <<"close">>, #{}, V2),
    ok = barrel_vectordb:add_vector(comprehensive_test_store, <<"s3">>, <<"medium">>, #{}, V3),
    ok = barrel_vectordb:add_vector(comprehensive_test_store, <<"s4">>, <<"far">>, #{}, V4),

    %% Search with query vector = V1
    {ok, Results} = barrel_vectordb:search_vector(comprehensive_test_store, V1, #{k => 4}),

    %% Verify results are ordered by score (descending)
    Scores = [maps:get(score, R) || R <- Results],
    SortedScores = lists:reverse(lists:sort(Scores)),
    ?assertEqual(SortedScores, Scores),

    %% First result should be exact match
    [First | _] = Results,
    ?assertEqual(<<"s1">>, maps:get(key, First)),
    ?assert(maps:get(score, First) > 0.99).

test_search_exact_match() ->
    Vec = normalize([0.5, 0.5, 0.5, 0.5, 0.0, 0.0, 0.0, 0.0]),
    ok = barrel_vectordb:add_vector(comprehensive_test_store, <<"exact">>, <<"exact match">>, #{}, Vec),
    ok = barrel_vectordb:add_vector(comprehensive_test_store, <<"other">>, <<"other">>, #{},
                                    normalize([0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0])),

    {ok, Results} = barrel_vectordb:search_vector(comprehensive_test_store, Vec, #{k => 2}),

    [First | _] = Results,
    ?assertEqual(<<"exact">>, maps:get(key, First)),
    %% Score should be very close to 1.0 (cosine similarity)
    ?assert(maps:get(score, First) > 0.999).

test_search_k_limit() ->
    %% Add 10 documents
    lists:foreach(fun(I) ->
        Id = list_to_binary("k_" ++ integer_to_list(I)),
        Vec = normalize([float(I)/10.0 | lists:duplicate(7, 0.0)]),
        ok = barrel_vectordb:add_vector(comprehensive_test_store, Id, <<"text">>, #{}, Vec)
    end, lists:seq(1, 10)),

    %% Search with k=3
    {ok, Results3} = barrel_vectordb:search_vector(comprehensive_test_store,
                                                    normalize([1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
                                                    #{k => 3}),
    ?assertEqual(3, length(Results3)),

    %% Search with k=5
    {ok, Results5} = barrel_vectordb:search_vector(comprehensive_test_store,
                                                    normalize([1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
                                                    #{k => 5}),
    ?assertEqual(5, length(Results5)).

test_search_vector() ->
    ok = barrel_vectordb:add_vector(comprehensive_test_store, <<"sv1">>, <<"doc1">>, #{},
                                    normalize([1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])),
    ok = barrel_vectordb:add_vector(comprehensive_test_store, <<"sv2">>, <<"doc2">>, #{},
                                    normalize([0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])),

    QueryVec = normalize([1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
    {ok, Results} = barrel_vectordb:search_vector(comprehensive_test_store, QueryVec, #{k => 2}),

    ?assertEqual(2, length(Results)),
    [First, Second] = Results,
    ?assertEqual(<<"sv1">>, maps:get(key, First)),
    ?assertEqual(<<"sv2">>, maps:get(key, Second)).

test_search_empty() ->
    %% Search in empty store
    {ok, Results} = barrel_vectordb:search_vector(comprehensive_test_store,
                                                   [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                                                   #{k => 5}),
    ?assertEqual([], Results).

%%====================================================================
%% Metadata Filtering Tests (inspired by ChromaDB test_filtering)
%%====================================================================

test_filter_equality() ->
    ok = barrel_vectordb:add_vector(comprehensive_test_store, <<"f1">>, <<"t1">>,
                                    #{type => apple}, normalize([1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])),
    ok = barrel_vectordb:add_vector(comprehensive_test_store, <<"f2">>, <<"t2">>,
                                    #{type => banana}, normalize([0.9, 0.1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])),
    ok = barrel_vectordb:add_vector(comprehensive_test_store, <<"f3">>, <<"t3">>,
                                    #{type => apple}, normalize([0.8, 0.2, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])),

    %% Filter for type=apple
    Filter = fun(Meta) -> maps:get(type, Meta, undefined) =:= apple end,
    {ok, Results} = barrel_vectordb:search_vector(comprehensive_test_store,
                                                   normalize([1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
                                                   #{k => 5, filter => Filter}),

    ?assertEqual(2, length(Results)),
    Ids = [maps:get(key, R) || R <- Results],
    ?assert(lists:member(<<"f1">>, Ids)),
    ?assert(lists:member(<<"f3">>, Ids)),
    ?assertNot(lists:member(<<"f2">>, Ids)).

test_filter_function() ->
    ok = barrel_vectordb:add_vector(comprehensive_test_store, <<"ff1">>, <<"t1">>,
                                    #{score => 10}, normalize([1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])),
    ok = barrel_vectordb:add_vector(comprehensive_test_store, <<"ff2">>, <<"t2">>,
                                    #{score => 50}, normalize([0.9, 0.1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])),
    ok = barrel_vectordb:add_vector(comprehensive_test_store, <<"ff3">>, <<"t3">>,
                                    #{score => 100}, normalize([0.8, 0.2, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])),

    %% Filter for score > 25
    Filter = fun(Meta) -> maps:get(score, Meta, 0) > 25 end,
    {ok, Results} = barrel_vectordb:search_vector(comprehensive_test_store,
                                                   normalize([1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
                                                   #{k => 5, filter => Filter}),

    ?assertEqual(2, length(Results)),
    Ids = [maps:get(key, R) || R <- Results],
    ?assert(lists:member(<<"ff2">>, Ids)),
    ?assert(lists:member(<<"ff3">>, Ids)).

test_filter_subset() ->
    %% Add 10 documents, 5 with tag=included
    lists:foreach(fun(I) ->
        Id = list_to_binary("fs_" ++ integer_to_list(I)),
        Tag = case I rem 2 of 0 -> included; 1 -> excluded end,
        Vec = normalize([float(I)/10.0 | lists:duplicate(7, 0.0)]),
        ok = barrel_vectordb:add_vector(comprehensive_test_store, Id, <<"text">>, #{tag => Tag}, Vec)
    end, lists:seq(1, 10)),

    Filter = fun(Meta) -> maps:get(tag, Meta, undefined) =:= included end,
    {ok, Results} = barrel_vectordb:search_vector(comprehensive_test_store,
                                                   normalize([1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
                                                   #{k => 10, filter => Filter}),

    ?assertEqual(5, length(Results)),
    lists:foreach(fun(R) ->
        ?assertEqual(included, maps:get(tag, maps:get(metadata, R)))
    end, Results).

%%====================================================================
%% Edge Cases Tests
%%====================================================================

test_dimension_mismatch() ->
    %% Try to add vector with wrong dimension (expecting 8, providing 3)
    WrongDimVec = [1.0, 0.0, 0.0],
    Result = barrel_vectordb:add_vector(comprehensive_test_store,
                                        <<"wrong_dim">>,
                                        <<"text">>,
                                        #{},
                                        WrongDimVec),
    ?assertMatch({error, {dimension_mismatch, 8, 3}}, Result).

test_duplicate_id() ->
    Vec1 = normalize([1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
    Vec2 = normalize([0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),

    ok = barrel_vectordb:add_vector(comprehensive_test_store, <<"dup">>, <<"first">>, #{v => 1}, Vec1),
    ok = barrel_vectordb:add_vector(comprehensive_test_store, <<"dup">>, <<"second">>, #{v => 2}, Vec2),

    %% Should have the second version (update semantics)
    {ok, Doc} = barrel_vectordb:get(comprehensive_test_store, <<"dup">>),
    ?assertEqual(<<"second">>, maps:get(text, Doc)),
    ?assertEqual(#{v => 2}, maps:get(metadata, Doc)),
    %% Vector should be Vec2 (latest stored) - exact match with 64-bit storage
    ?assertEqual(Vec2, maps:get(vector, Doc)),

    %% Count should be 1 (not 2) - HNSW now properly handles updates
    ?assertEqual(1, barrel_vectordb:count(comprehensive_test_store)).

test_empty_text() ->
    Vec = normalize([1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
    ok = barrel_vectordb:add_vector(comprehensive_test_store, <<"empty_text">>, <<"">>, #{}, Vec),

    {ok, Doc} = barrel_vectordb:get(comprehensive_test_store, <<"empty_text">>),
    ?assertEqual(<<>>, maps:get(text, Doc)).

test_unicode_text() ->
    Vec = normalize([1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
    UnicodeText = <<"こんにちは世界 🌍 مرحبا"/utf8>>,
    ok = barrel_vectordb:add_vector(comprehensive_test_store, <<"unicode">>, UnicodeText, #{}, Vec),

    {ok, Doc} = barrel_vectordb:get(comprehensive_test_store, <<"unicode">>),
    ?assertEqual(UnicodeText, maps:get(text, Doc)).

test_large_metadata() ->
    Vec = normalize([1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
    LargeMeta = #{
        field1 => binary:copy(<<"x">>, 1000),
        field2 => lists:seq(1, 100),
        field3 => #{nested => #{deep => <<"value">>}},
        field4 => [#{a => 1}, #{b => 2}, #{c => 3}]
    },
    ok = barrel_vectordb:add_vector(comprehensive_test_store, <<"large_meta">>, <<"text">>, LargeMeta, Vec),

    {ok, Doc} = barrel_vectordb:get(comprehensive_test_store, <<"large_meta">>),
    ?assertEqual(LargeMeta, maps:get(metadata, Doc)).

%%====================================================================
%% Invariant Tests (inspired by ChromaDB invariants.py)
%%====================================================================

test_invariant_count() ->
    %% Count should always match actual documents
    ?assertEqual(0, barrel_vectordb:count(comprehensive_test_store)),

    %% Add documents
    lists:foreach(fun(I) ->
        Id = list_to_binary("ic_" ++ integer_to_list(I)),
        Vec = normalize([float(I)/10.0 | lists:duplicate(7, 0.0)]),
        ok = barrel_vectordb:add_vector(comprehensive_test_store, Id, <<"text">>, #{}, Vec),
        ?assertEqual(I, barrel_vectordb:count(comprehensive_test_store))
    end, lists:seq(1, 10)),

    %% Delete some
    ok = barrel_vectordb:delete(comprehensive_test_store, <<"ic_5">>),
    ?assertEqual(9, barrel_vectordb:count(comprehensive_test_store)),

    ok = barrel_vectordb:delete(comprehensive_test_store, <<"ic_3">>),
    ?assertEqual(8, barrel_vectordb:count(comprehensive_test_store)).

test_invariant_no_duplicates() ->
    %% Search results should never contain duplicate IDs
    lists:foreach(fun(I) ->
        Id = list_to_binary("nd_" ++ integer_to_list(I)),
        Vec = normalize([float(I)/100.0 | lists:duplicate(7, float(I)/100.0)]),
        ok = barrel_vectordb:add_vector(comprehensive_test_store, Id, <<"text">>, #{}, Vec)
    end, lists:seq(1, 20)),

    {ok, Results} = barrel_vectordb:search_vector(comprehensive_test_store,
                                                   normalize([0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5]),
                                                   #{k => 15}),

    Ids = [maps:get(key, R) || R <- Results],
    UniqueIds = lists:usort(Ids),
    ?assertEqual(length(Ids), length(UniqueIds)).

test_invariant_data_integrity() ->
    %% Data retrieved should match data stored
    TestData = [
        {<<"di1">>, <<"Hello World">>, #{type => greeting, count => 1}},
        {<<"di2">>, <<"Goodbye">>, #{type => farewell, count => 2}},
        {<<"di3">>, <<"Test 123">>, #{type => test, count => 3}}
    ],

    %% Store with explicit vectors
    lists:foreach(fun({Id, Text, Meta}) ->
        Vec = text_to_vector(Text, 8),
        ok = barrel_vectordb:add_vector(comprehensive_test_store, Id, Text, Meta, Vec)
    end, TestData),

    %% Verify each document
    lists:foreach(fun({Id, ExpText, ExpMeta}) ->
        {ok, Doc} = barrel_vectordb:get(comprehensive_test_store, Id),
        ?assertEqual(Id, maps:get(key, Doc)),
        ?assertEqual(ExpText, maps:get(text, Doc)),
        ?assertEqual(ExpMeta, maps:get(metadata, Doc)),
        %% Vectors stored with 32-bit precision - use approximate comparison
        ExpVec = text_to_vector(ExpText, 8),
        ActualVec = maps:get(vector, Doc),
        ?assert(vectors_approx_equal(ExpVec, ActualVec))
    end, TestData).

test_invariant_score_ordering() ->
    %% Scores must always be in descending order
    lists:foreach(fun(I) ->
        Id = list_to_binary("so_" ++ integer_to_list(I)),
        %% Create vectors at various distances from query
        Vec = normalize([float(I)/30.0, float(30-I)/30.0 | lists:duplicate(6, 0.0)]),
        ok = barrel_vectordb:add_vector(comprehensive_test_store, Id, <<"text">>, #{}, Vec)
    end, lists:seq(1, 30)),

    QueryVec = normalize([1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
    {ok, Results} = barrel_vectordb:search_vector(comprehensive_test_store, QueryVec, #{k => 20}),

    Scores = [maps:get(score, R) || R <- Results],

    %% Verify scores are sorted descending
    SortedDesc = lists:reverse(lists:sort(Scores)),
    ?assertEqual(SortedDesc, Scores),

    %% Verify no score exceeds 1.0 or is negative
    lists:foreach(fun(S) ->
        ?assert(S =< 1.0),
        ?assert(S >= -1.0)  %% Cosine similarity can be negative for opposite vectors
    end, Scores).
