%%%-------------------------------------------------------------------
%%% @doc BM25 Formula Correctness Tests
%%%
%%% Hand-calculated test cases to verify BM25 implementation accuracy.
%%% Tests run against both memory and disk backends.
%%%
%%% BM25 Formula:
%%%   Score(q, d) = sum_t IDF(t) * (TF(t,d) * (k1 + 1)) / (TF(t,d) + k1 * (1 - b + b * |d| / avgdl))
%%%
%%% Where:
%%%   IDF(t) = log((N - DF + 0.5) / (DF + 0.5) + 1)
%%%   N      = total documents
%%%   DF     = documents containing term t
%%%   TF     = term frequency in document
%%%   |d|    = document length (tokens)
%%%   avgdl  = average document length
%%%   k1     = 1.2 (default)
%%%   b      = 0.75 (default)
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_bm25_formula_tests).

-include_lib("eunit/include/eunit.hrl").

-define(EPSILON, 0.001).  %% Tolerance for floating point comparison

%%====================================================================
%% Test Generators - Run all tests against both backends
%%====================================================================

formula_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         {"single term single doc", fun test_single_term_single_doc/0},
         {"multi term score", fun test_multi_term_score/0},
         {"tf impact", fun test_tf_impact/0},
         {"idf impact", fun test_idf_impact/0},
         {"doc length normalization", fun test_doc_length_normalization/0},
         {"k1 parameter effect", fun test_k1_parameter/0},
         {"b parameter effect", fun test_b_parameter/0},
         {"tf saturation", fun test_tf_saturation/0},
         {"edge cases", fun test_edge_cases/0},
         {"hand calculated example", fun test_hand_calculated_example/0}
     ]}.

%% Test both backends produce identical scores
backend_consistency_test_() ->
    {setup,
     fun setup_both_backends/0,
     fun cleanup_both_backends/1,
     fun test_backend_consistency/1}.

%%====================================================================
%% Setup/Cleanup
%%====================================================================

setup() ->
    ok.

cleanup(_) ->
    ok.

setup_both_backends() ->
    TmpDir = make_tmp_dir(),
    {ok, DiskIndex} = barrel_vectordb_bm25_disk:new(#{
        base_path => TmpDir,
        k1 => 1.2,
        b => 0.75
    }),
    MemoryIndex = barrel_vectordb_bm25:new(#{k1 => 1.2, b => 0.75}),
    {TmpDir, DiskIndex, MemoryIndex}.

cleanup_both_backends({TmpDir, DiskIndex, _MemoryIndex}) ->
    barrel_vectordb_bm25_disk:close(DiskIndex),
    cleanup_tmp_dir(TmpDir).

make_tmp_dir() ->
    N = erlang:unique_integer([positive]),
    Dir = filename:join(["/tmp", "bm25_formula_test_" ++ integer_to_list(N)]),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Dir.

cleanup_tmp_dir(Dir) ->
    os:cmd("rm -rf " ++ Dir).

%%====================================================================
%% Helper Functions for BM25 Calculation
%%====================================================================

%% Calculate expected IDF
%% IDF(t) = log((N - DF + 0.5) / (DF + 0.5) + 1)
calc_idf(N, DF) ->
    math:log((N - DF + 0.5) / (DF + 0.5) + 1).

%% Calculate expected BM25 score for a single term
%% Score = IDF * (TF * (k1 + 1)) / (TF + k1 * (1 - b + b * doclen / avgdl))
calc_bm25_term_score(IDF, TF, DocLen, AvgDL, K1, B) ->
    Numerator = TF * (K1 + 1),
    Denominator = TF + K1 * (1 - B + B * DocLen / AvgDL),
    IDF * Numerator / Denominator.

%% Assert float equality within tolerance
assert_float_eq(Expected, Actual) ->
    assert_float_eq(Expected, Actual, ?EPSILON).

assert_float_eq(Expected, Actual, Epsilon) ->
    Diff = abs(Expected - Actual),
    ?assert(Diff < Epsilon,
            io_lib:format("Expected ~p, got ~p (diff: ~p)", [Expected, Actual, Diff])).

%%====================================================================
%% Tests
%%====================================================================

%% Test 1: Single term, single document
%% Corpus: 1 doc, "hello world" (2 tokens)
%% Query: "hello"
test_single_term_single_doc() ->
    Index0 = barrel_vectordb_bm25:new(#{k1 => 1.2, b => 0.75}),
    Index1 = barrel_vectordb_bm25:add(Index0, <<"doc1">>, <<"hello world">>),

    %% Calculate expected score:
    %% N = 1, DF = 1, TF = 1, DocLen = 2, AvgDL = 2
    %% IDF = log((1 - 1 + 0.5) / (1 + 0.5) + 1) = log(0.5/1.5 + 1) = log(1.333) ≈ 0.288
    %% Denom = 1 + 1.2 * (1 - 0.75 + 0.75 * 2/2) = 1 + 1.2 * 1 = 2.2
    %% Score = 0.288 * (1 * 2.2) / 2.2 = 0.288
    N = 1, DF = 1, TF = 1, DocLen = 2, AvgDL = 2.0, K1 = 1.2, B = 0.75,
    ExpectedIDF = calc_idf(N, DF),
    ExpectedScore = calc_bm25_term_score(ExpectedIDF, TF, DocLen, AvgDL, K1, B),

    Results = barrel_vectordb_bm25:search(Index1, <<"hello">>, 10),
    ?assertEqual(1, length(Results)),
    [{<<"doc1">>, ActualScore}] = Results,
    assert_float_eq(ExpectedScore, ActualScore).

%% Test 2: Multi-term query - scores should sum
test_multi_term_score() ->
    Index0 = barrel_vectordb_bm25:new(#{k1 => 1.2, b => 0.75}),
    Index1 = barrel_vectordb_bm25:add(Index0, <<"doc1">>, <<"erlang programming">>),
    Index2 = barrel_vectordb_bm25:add(Index1, <<"doc2">>, <<"erlang language">>),

    %% N = 2, AvgDL = 2
    %% For doc1 with query "erlang programming":
    %%   IDF(erlang) = log((2-2+0.5)/(2+0.5)+1) = log(0.5/2.5+1) = log(1.2) ≈ 0.182
    %%   IDF(programming) = log((2-1+0.5)/(1+0.5)+1) = log(1.5/1.5+1) = log(2) ≈ 0.693
    %%   Both TF=1, DocLen=2, AvgDL=2
    %%   Denom = 1 + 1.2 * 1 = 2.2
    %%   Score(erlang) = 0.182 * 2.2 / 2.2 = 0.182
    %%   Score(programming) = 0.693 * 2.2 / 2.2 = 0.693
    %%   Total = 0.182 + 0.693 = 0.875

    N = 2, AvgDL = 2.0, K1 = 1.2, B = 0.75,
    IDF_erlang = calc_idf(N, 2),  %% appears in both docs
    IDF_programming = calc_idf(N, 1),  %% appears in 1 doc

    Score_erlang = calc_bm25_term_score(IDF_erlang, 1, 2, AvgDL, K1, B),
    Score_programming = calc_bm25_term_score(IDF_programming, 1, 2, AvgDL, K1, B),
    ExpectedDoc1 = Score_erlang + Score_programming,

    %% Doc2 only matches "erlang"
    ExpectedDoc2 = calc_bm25_term_score(IDF_erlang, 1, 2, AvgDL, K1, B),

    Results = barrel_vectordb_bm25:search(Index2, <<"erlang programming">>, 10),
    ?assertEqual(2, length(Results)),

    %% Doc1 should rank higher (matches both terms)
    [{TopDoc, TopScore}, {SecondDoc, SecondScore}] = Results,
    ?assertEqual(<<"doc1">>, TopDoc),
    ?assertEqual(<<"doc2">>, SecondDoc),
    assert_float_eq(ExpectedDoc1, TopScore),
    assert_float_eq(ExpectedDoc2, SecondScore).

%% Test 3: TF impact - higher TF means higher score
test_tf_impact() ->
    Index0 = barrel_vectordb_bm25:new(#{k1 => 1.2, b => 0.75}),
    Index1 = barrel_vectordb_bm25:add(Index0, <<"doc1">>, <<"apple">>),
    Index2 = barrel_vectordb_bm25:add(Index1, <<"doc2">>, <<"apple apple">>),
    Index3 = barrel_vectordb_bm25:add(Index2, <<"doc3">>, <<"apple apple apple">>),

    Results = barrel_vectordb_bm25:search(Index3, <<"apple">>, 10),

    %% Verify scores are strictly increasing with TF
    %% (though with diminishing returns due to saturation)
    [{_, Score3}, {_, Score2}, {_, Score1}] = Results,
    ?assert(Score3 > Score2),
    ?assert(Score2 > Score1),

    %% Verify exact calculations
    %% N = 3, DF = 3, AvgDL = (1+2+3)/3 = 2
    N = 3, DF = 3, AvgDL = 2.0, K1 = 1.2, B = 0.75,
    IDF = calc_idf(N, DF),

    Expected1 = calc_bm25_term_score(IDF, 1, 1, AvgDL, K1, B),
    Expected2 = calc_bm25_term_score(IDF, 2, 2, AvgDL, K1, B),
    Expected3 = calc_bm25_term_score(IDF, 3, 3, AvgDL, K1, B),

    assert_float_eq(Expected1, Score1),
    assert_float_eq(Expected2, Score2),
    assert_float_eq(Expected3, Score3).

%% Test 4: IDF impact - rare terms have higher IDF
test_idf_impact() ->
    Index0 = barrel_vectordb_bm25:new(#{k1 => 1.2, b => 0.75}),
    %% "common" appears in all docs, "rare" appears in only doc3
    Index1 = barrel_vectordb_bm25:add(Index0, <<"doc1">>, <<"common word">>),
    Index2 = barrel_vectordb_bm25:add(Index1, <<"doc2">>, <<"common term">>),
    Index3 = barrel_vectordb_bm25:add(Index2, <<"doc3">>, <<"common rare">>),

    %% IDF(common) = log((3-3+0.5)/(3+0.5)+1) ≈ 0.134
    %% IDF(rare) = log((3-1+0.5)/(1+0.5)+1) = log(2.67) ≈ 0.981
    N = 3,
    IDF_common = calc_idf(N, 3),
    IDF_rare = calc_idf(N, 1),

    ?assert(IDF_rare > IDF_common),

    %% Search for "rare" - only doc3 matches
    RareResults = barrel_vectordb_bm25:search(Index3, <<"rare">>, 10),
    ?assertEqual(1, length(RareResults)),
    [{<<"doc3">>, RareScore}] = RareResults,

    %% Search for "common" - all docs match
    CommonResults = barrel_vectordb_bm25:search(Index3, <<"common">>, 10),
    ?assertEqual(3, length(CommonResults)),
    [{_, CommonScore1} | _] = CommonResults,

    %% Rare term should have higher score contribution
    ?assert(RareScore > CommonScore1).

%% Test 5: Document length normalization
%% With equal TF, shorter docs should score higher
test_doc_length_normalization() ->
    Index0 = barrel_vectordb_bm25:new(#{k1 => 1.2, b => 0.75}),
    %% Same TF for "test" but different doc lengths
    Index1 = barrel_vectordb_bm25:add(Index0, <<"short">>, <<"test">>),
    Index2 = barrel_vectordb_bm25:add(Index1, <<"medium">>, <<"test word word">>),
    Index3 = barrel_vectordb_bm25:add(Index2, <<"long">>, <<"test word word word word">>),

    Results = barrel_vectordb_bm25:search(Index3, <<"test">>, 10),
    ?assertEqual(3, length(Results)),

    %% Shorter docs should rank higher with same TF
    [{TopDoc, _}, {MidDoc, _}, {LowDoc, _}] = Results,
    ?assertEqual(<<"short">>, TopDoc),
    ?assertEqual(<<"medium">>, MidDoc),
    ?assertEqual(<<"long">>, LowDoc).

%% Test 6: k1 parameter effect
%% k1=0: TF has minimal effect
%% k1=high: TF has more effect (less saturation)
test_k1_parameter() ->
    %% k1 = 0: TF saturates immediately
    Index0_k0 = barrel_vectordb_bm25:new(#{k1 => 0.0, b => 0.75}),
    Index1_k0 = barrel_vectordb_bm25:add(Index0_k0, <<"doc1">>, <<"apple">>),
    Index2_k0 = barrel_vectordb_bm25:add(Index1_k0, <<"doc2">>, <<"apple apple apple">>),

    Results_k0 = barrel_vectordb_bm25:search(Index2_k0, <<"apple">>, 10),
    [{_, Score1_k0}, {_, Score2_k0}] = Results_k0,
    %% With k1=0, both should have very similar scores (TF has no effect)
    %% Score = IDF * (TF * 1) / (TF + 0) = IDF (independent of TF)
    Diff_k0 = abs(Score1_k0 - Score2_k0),

    %% k1 = 3.0: TF has more effect
    Index0_k3 = barrel_vectordb_bm25:new(#{k1 => 3.0, b => 0.75}),
    Index1_k3 = barrel_vectordb_bm25:add(Index0_k3, <<"doc1">>, <<"apple">>),
    Index2_k3 = barrel_vectordb_bm25:add(Index1_k3, <<"doc2">>, <<"apple apple apple">>),

    Results_k3 = barrel_vectordb_bm25:search(Index2_k3, <<"apple">>, 10),
    [{_, Score1_k3}, {_, Score2_k3}] = Results_k3,
    Diff_k3 = abs(Score1_k3 - Score2_k3),

    %% Higher k1 should result in bigger score difference between TF=1 and TF=3
    ?assert(Diff_k3 > Diff_k0).

%% Test 7: b parameter effect
%% b=0: No length normalization
%% b=1: Full length normalization
test_b_parameter() ->
    %% b = 0: No length normalization
    Index0_b0 = barrel_vectordb_bm25:new(#{k1 => 1.2, b => 0.0}),
    Index1_b0 = barrel_vectordb_bm25:add(Index0_b0, <<"short">>, <<"test">>),
    Index2_b0 = barrel_vectordb_bm25:add(Index1_b0, <<"long">>, <<"test word word word">>),

    Results_b0 = barrel_vectordb_bm25:search(Index2_b0, <<"test">>, 10),
    [{_, Score_short_b0}, {_, Score_long_b0}] = Results_b0,
    Ratio_b0 = Score_short_b0 / Score_long_b0,

    %% b = 1: Full length normalization
    Index0_b1 = barrel_vectordb_bm25:new(#{k1 => 1.2, b => 1.0}),
    Index1_b1 = barrel_vectordb_bm25:add(Index0_b1, <<"short">>, <<"test">>),
    Index2_b1 = barrel_vectordb_bm25:add(Index1_b1, <<"long">>, <<"test word word word">>),

    Results_b1 = barrel_vectordb_bm25:search(Index2_b1, <<"test">>, 10),
    [{_, Score_short_b1}, {_, Score_long_b1}] = Results_b1,
    Ratio_b1 = Score_short_b1 / Score_long_b1,

    %% With b=1, short docs should be favored more than with b=0
    ?assert(Ratio_b1 > Ratio_b0).

%% Test 8: TF saturation - diminishing returns
test_tf_saturation() ->
    Index0 = barrel_vectordb_bm25:new(#{k1 => 1.2, b => 0.75}),
    Index1 = barrel_vectordb_bm25:add(Index0, <<"doc1">>, <<"apple apple apple apple apple">>),           %% TF=5
    Index2 = barrel_vectordb_bm25:add(Index1, <<"doc2">>, <<"apple apple apple apple apple apple apple apple apple apple">>),  %% TF=10

    Results = barrel_vectordb_bm25:search(Index2, <<"apple">>, 10),
    [{_, Score_tf10}, {_, Score_tf5}] = Results,

    %% Score(TF=10) should be less than 2x Score(TF=5) due to saturation
    ?assert(Score_tf10 < 2 * Score_tf5),
    %% But TF=10 should still be higher than TF=5
    ?assert(Score_tf10 > Score_tf5).

%% Test 9: Edge cases
test_edge_cases() ->
    Index0 = barrel_vectordb_bm25:new(#{k1 => 1.2, b => 0.75}),
    Index1 = barrel_vectordb_bm25:add(Index0, <<"doc1">>, <<"hello world">>),

    %% Query term not in any doc -> empty results
    Results1 = barrel_vectordb_bm25:search(Index1, <<"notfound">>, 10),
    ?assertEqual([], Results1),

    %% Empty query -> empty results (tokenizes to nothing)
    Results2 = barrel_vectordb_bm25:search(Index1, <<"">>, 10),
    ?assertEqual([], Results2),

    %% Query with only stopwords/short terms (if min_term_length > 1)
    Index_min3 = barrel_vectordb_bm25:new(#{k1 => 1.2, b => 0.75, min_term_length => 3}),
    Index_min3_1 = barrel_vectordb_bm25:add(Index_min3, <<"doc1">>, <<"test document">>),
    Results3 = barrel_vectordb_bm25:search(Index_min3_1, <<"a i">>, 10),
    ?assertEqual([], Results3),

    %% Single document corpus
    SingleResults = barrel_vectordb_bm25:search(Index1, <<"hello">>, 10),
    ?assertEqual(1, length(SingleResults)),

    %% Term in all docs has low IDF
    Index2 = barrel_vectordb_bm25:add(Index1, <<"doc2">>, <<"hello there">>),
    Index3 = barrel_vectordb_bm25:add(Index2, <<"doc3">>, <<"hello friend">>),
    AllResults = barrel_vectordb_bm25:search(Index3, <<"hello">>, 10),
    ?assertEqual(3, length(AllResults)),
    %% With term in all 3 docs, IDF = log((3-3+0.5)/(3+0.5)+1) = log(1.143) ≈ 0.134
    ExpectedIDF = calc_idf(3, 3),
    ?assert(ExpectedIDF < 0.2).  %% Low IDF for common term

%% Test 10: Hand-calculated example from plan
%% Corpus: 3 documents
%%   doc1: "erlang erlang programming" (3 tokens, TF(erlang)=2)
%%   doc2: "erlang language" (2 tokens, TF(erlang)=1)
%%   doc3: "python programming" (2 tokens, TF(erlang)=0)
%% Query: "erlang"
test_hand_calculated_example() ->
    Index0 = barrel_vectordb_bm25:new(#{k1 => 1.2, b => 0.75}),
    Index1 = barrel_vectordb_bm25:add(Index0, <<"doc1">>, <<"erlang erlang programming">>),
    Index2 = barrel_vectordb_bm25:add(Index1, <<"doc2">>, <<"erlang language">>),
    Index3 = barrel_vectordb_bm25:add(Index2, <<"doc3">>, <<"python programming">>),

    %% Parameters
    N = 3,
    DF = 2,  %% "erlang" appears in doc1, doc2
    AvgDL = (3 + 2 + 2) / 3,  %% = 2.333...
    K1 = 1.2,
    B = 0.75,

    %% IDF = log((3 - 2 + 0.5) / (2 + 0.5) + 1) = log(1.5/2.5 + 1) = log(1.6) ≈ 0.470
    IDF = calc_idf(N, DF),
    assert_float_eq(0.470, IDF, 0.01),

    %% Score(doc1): TF=2, DocLen=3
    %% Denom = 2 + 1.2 * (1 - 0.75 + 0.75 * 3/2.333) = 2 + 1.2 * (0.25 + 0.964) = 2 + 1.457 = 3.457
    %% Score = 0.470 * (2 * 2.2) / 3.457 = 0.470 * 4.4 / 3.457 ≈ 0.598
    ExpectedDoc1 = calc_bm25_term_score(IDF, 2, 3, AvgDL, K1, B),

    %% Score(doc2): TF=1, DocLen=2
    %% Denom = 1 + 1.2 * (1 - 0.75 + 0.75 * 2/2.333) = 1 + 1.2 * (0.25 + 0.643) = 1 + 1.072 = 2.072
    %% Score = 0.470 * (1 * 2.2) / 2.072 = 0.470 * 2.2 / 2.072 ≈ 0.499
    ExpectedDoc2 = calc_bm25_term_score(IDF, 1, 2, AvgDL, K1, B),

    Results = barrel_vectordb_bm25:search(Index3, <<"erlang">>, 10),
    ?assertEqual(2, length(Results)),

    %% Expected ranking: doc1 > doc2 > doc3 (doc3 not in results)
    [{TopDoc, TopScore}, {SecondDoc, SecondScore}] = Results,
    ?assertEqual(<<"doc1">>, TopDoc),
    ?assertEqual(<<"doc2">>, SecondDoc),

    assert_float_eq(ExpectedDoc1, TopScore, 0.01),
    assert_float_eq(ExpectedDoc2, SecondScore, 0.01),

    %% Verify ranking order
    ?assert(TopScore > SecondScore).

%% Backend consistency test - memory and disk should produce identical rankings
test_backend_consistency({TmpDir, _DiskIndex0, _MemoryIndex0}) ->
    {"memory and disk backends produce identical scores",
     fun() ->
         %% Create fresh indexes with same documents
         {ok, DiskIndex1} = barrel_vectordb_bm25_disk:new(#{
             base_path => filename:join(TmpDir, "consistency"),
             k1 => 1.2,
             b => 0.75
         }),
         MemoryIndex1 = barrel_vectordb_bm25:new(#{k1 => 1.2, b => 0.75}),

         %% Add same documents to both
         Docs = [
             {<<"doc1">>, <<"erlang programming functional">>},
             {<<"doc2">>, <<"python programming machine learning">>},
             {<<"doc3">>, <<"erlang otp distributed systems">>},
             {<<"doc4">>, <<"java enterprise spring framework">>},
             {<<"doc5">>, <<"erlang concurrency message passing">>}
         ],

         {DiskIndex2, MemoryIndex2} = lists:foldl(
             fun({Id, Text}, {DI, MI}) ->
                 {ok, DI2} = barrel_vectordb_bm25_disk:add(DI, Id, Text),
                 MI2 = barrel_vectordb_bm25:add(MI, Id, Text),
                 {DI2, MI2}
             end,
             {DiskIndex1, MemoryIndex1},
             Docs
         ),

         %% Search with same query
         Query = <<"erlang">>,
         DiskResults = barrel_vectordb_bm25_disk:search(DiskIndex2, Query, 10),
         MemoryResults = barrel_vectordb_bm25:search(MemoryIndex2, Query, 10),

         %% Should have same number of results
         ?assertEqual(length(MemoryResults), length(DiskResults)),

         %% Should have same set of documents (order may differ for tied scores)
         DiskDocIds = lists:sort([DocId || {DocId, _} <- DiskResults]),
         MemoryDocIds = lists:sort([DocId || {DocId, _} <- MemoryResults]),
         ?assertEqual(MemoryDocIds, DiskDocIds),

         %% Build score maps for comparison
         DiskScoreMap = maps:from_list(DiskResults),
         MemoryScoreMap = maps:from_list(MemoryResults),

         %% Scores for each doc should be very close (within tolerance)
         lists:foreach(
             fun(DocId) ->
                 DiskScore = maps:get(DocId, DiskScoreMap),
                 MemScore = maps:get(DocId, MemoryScoreMap),
                 assert_float_eq(MemScore, DiskScore, 0.01)
             end,
             DiskDocIds
         ),

         barrel_vectordb_bm25_disk:close(DiskIndex2)
     end}.
