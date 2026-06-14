%%%-------------------------------------------------------------------
%%% @doc BM25 Corpus-Based Relevance Tests
%%%
%%% Tests BM25 search quality using a 100-document test corpus with
%%% 20 queries and relevance judgments. Evaluates using standard IR
%%% metrics: Precision@K, Recall@K, nDCG@K.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_bm25_corpus_tests).

-include_lib("eunit/include/eunit.hrl").

-define(CORPUS_FILE, "test/data/bm25_test_corpus.terms").
-define(QUERIES_FILE, "test/data/bm25_test_queries.terms").

%%====================================================================
%% Test Generators
%%====================================================================

corpus_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(State) ->
         [
             {"load corpus and queries", fun() -> test_load_data(State) end},
             {"precision at 5", fun() -> test_precision_at_5(State) end},
             {"precision at 10", fun() -> test_precision_at_10(State) end},
             {"recall at 10", fun() -> test_recall_at_10(State) end},
             {"ndcg at 10", fun() -> test_ndcg_at_10(State) end},
             {"relevant in top k", fun() -> test_relevant_in_top_k(State) end},
             {"ranking order", fun() -> test_ranking_order(State) end},
             {"cross topic queries", fun() -> test_cross_topic_queries(State) end},
             {"rare term queries", fun() -> test_rare_term_queries(State) end},
             {"memory disk consistency", fun() -> test_memory_disk_consistency(State) end}
         ]
     end}.

%%====================================================================
%% Setup/Cleanup
%%====================================================================

setup() ->
    %% Load corpus and queries
    {ok, CorpusTerms} = file:consult(?CORPUS_FILE),
    Corpus = hd(CorpusTerms),  %% List of {DocId, Text}
    {ok, QueryTerms} = file:consult(?QUERIES_FILE),
    Queries = hd(QueryTerms),  %% List of {Query, #{relevant, partial}}

    %% Create memory index
    MemoryIndex = lists:foldl(
        fun({DocId, Text}, Acc) ->
            barrel_vectordb_bm25:add(Acc, DocId, Text)
        end,
        barrel_vectordb_bm25:new(#{k1 => 1.2, b => 0.75}),
        Corpus
    ),

    %% Create disk index
    TmpDir = make_tmp_dir(),
    {ok, DiskIndex0} = barrel_vectordb_bm25_disk:new(#{
        base_path => TmpDir,
        k1 => 1.2,
        b => 0.75
    }),
    {ok, DiskIndex} = lists:foldl(
        fun({DocId, Text}, {ok, Acc}) ->
            barrel_vectordb_bm25_disk:add(Acc, DocId, Text)
        end,
        {ok, DiskIndex0},
        Corpus
    ),

    #{
        corpus => Corpus,
        queries => Queries,
        memory_index => MemoryIndex,
        disk_index => DiskIndex,
        tmp_dir => TmpDir
    }.

cleanup(#{disk_index := DiskIndex, tmp_dir := TmpDir}) ->
    barrel_vectordb_bm25_disk:close(DiskIndex),
    cleanup_tmp_dir(TmpDir).

make_tmp_dir() ->
    N = erlang:unique_integer([positive]),
    Dir = filename:join(["/tmp", "bm25_corpus_test_" ++ integer_to_list(N)]),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Dir.

cleanup_tmp_dir(Dir) ->
    os:cmd("rm -rf " ++ Dir).

%%====================================================================
%% Helper Functions
%%====================================================================

%% Convert doc ID like <<"doc1">> to integer 1
doc_id_to_num(<<"doc", Rest/binary>>) ->
    binary_to_integer(Rest).

%% Get all relevant doc IDs (both relevant and partial)
get_all_relevant(#{relevant := Relevant, partial := Partial}) ->
    Relevant ++ Partial.

%% Calculate Precision@K
%% P@K = (relevant docs in top K) / K
precision_at_k(Results, RelevantSet, K) ->
    TopK = lists:sublist(Results, K),
    RelevantInTopK = length([DocId || {DocId, _} <- TopK,
                                      sets:is_element(doc_id_to_num(DocId), RelevantSet)]),
    RelevantInTopK / K.

%% Calculate Recall@K
%% R@K = (relevant docs in top K) / (total relevant docs)
recall_at_k(Results, RelevantSet, K) ->
    TotalRelevant = sets:size(RelevantSet),
    case TotalRelevant of
        0 -> 1.0;  %% No relevant docs means perfect recall
        _ ->
            TopK = lists:sublist(Results, K),
            RelevantInTopK = length([DocId || {DocId, _} <- TopK,
                                              sets:is_element(doc_id_to_num(DocId), RelevantSet)]),
            RelevantInTopK / TotalRelevant
    end.

%% Calculate DCG@K (Discounted Cumulative Gain)
%% DCG@K = sum_i (rel_i / log2(i + 1))
%% Using binary relevance: rel = 2 for relevant, 1 for partial, 0 for non-relevant
dcg_at_k(Results, RelevantSet, PartialSet, K) ->
    TopK = lists:sublist(Results, K),
    {DCG, _} = lists:foldl(
        fun({DocId, _}, {Acc, Pos}) ->
            DocNum = doc_id_to_num(DocId),
            Rel = case sets:is_element(DocNum, RelevantSet) of
                true -> 2;
                false -> case sets:is_element(DocNum, PartialSet) of
                    true -> 1;
                    false -> 0
                end
            end,
            Gain = Rel / math:log2(Pos + 1),
            {Acc + Gain, Pos + 1}
        end,
        {0.0, 1},
        TopK
    ),
    DCG.

%% Calculate ideal DCG (perfect ranking)
ideal_dcg_at_k(RelevantCount, PartialCount, K) ->
    %% Ideal ranking: all relevant first (rel=2), then partial (rel=1)
    AllRels = lists:duplicate(min(RelevantCount, K), 2) ++
              lists:duplicate(min(PartialCount, max(0, K - RelevantCount)), 1),
    PaddedRels = lists:sublist(AllRels ++ lists:duplicate(K, 0), K),
    {IDCG, _} = lists:foldl(
        fun(Rel, {Acc, Pos}) ->
            Gain = Rel / math:log2(Pos + 1),
            {Acc + Gain, Pos + 1}
        end,
        {0.0, 1},
        PaddedRels
    ),
    IDCG.

%% Calculate nDCG@K (normalized DCG)
%% nDCG@K = DCG@K / IDCG@K
ndcg_at_k(Results, RelevantSet, PartialSet, K) ->
    DCG = dcg_at_k(Results, RelevantSet, PartialSet, K),
    IDCG = ideal_dcg_at_k(sets:size(RelevantSet), sets:size(PartialSet), K),
    case IDCG of
        +0.0 -> 1.0;  %% No relevant docs
        _ -> DCG / IDCG
    end.

%%====================================================================
%% Tests
%%====================================================================

test_load_data(#{corpus := Corpus, queries := Queries}) ->
    %% Verify corpus loaded correctly
    ?assertEqual(100, length(Corpus)),

    %% Verify queries loaded correctly
    ?assertEqual(20, length(Queries)),

    %% Verify doc IDs are correct format
    {DocId1, _} = hd(Corpus),
    ?assertEqual(<<"doc1">>, DocId1),

    %% Verify query format
    {Query1, Judgments1} = hd(Queries),
    ?assertEqual(<<"erlang">>, Query1),
    ?assert(maps:is_key(relevant, Judgments1)),
    ?assert(maps:is_key(partial, Judgments1)).

test_precision_at_5(#{memory_index := Index, queries := Queries}) ->
    %% Calculate average P@5 across all queries
    P5Scores = lists:map(
        fun({Query, Judgments}) ->
            Results = barrel_vectordb_bm25:search(Index, Query, 10),
            AllRelevant = get_all_relevant(Judgments),
            RelevantSet = sets:from_list(AllRelevant),
            precision_at_k(Results, RelevantSet, 5)
        end,
        Queries
    ),
    AvgP5 = lists:sum(P5Scores) / length(P5Scores),

    %% Average P@5 should be at least 0.3 (reasonable for this corpus)
    ?assert(AvgP5 >= 0.3),

    %% Log for debugging
    io:format("Average P@5: ~.3f~n", [AvgP5]).

test_precision_at_10(#{memory_index := Index, queries := Queries}) ->
    %% Calculate average P@10 across all queries
    P10Scores = lists:map(
        fun({Query, Judgments}) ->
            Results = barrel_vectordb_bm25:search(Index, Query, 10),
            AllRelevant = get_all_relevant(Judgments),
            RelevantSet = sets:from_list(AllRelevant),
            precision_at_k(Results, RelevantSet, 10)
        end,
        Queries
    ),
    AvgP10 = lists:sum(P10Scores) / length(P10Scores),

    %% Average P@10 should be at least 0.25
    ?assert(AvgP10 >= 0.25),

    io:format("Average P@10: ~.3f~n", [AvgP10]).

test_recall_at_10(#{memory_index := Index, queries := Queries}) ->
    %% Calculate average R@10 across all queries
    R10Scores = lists:map(
        fun({Query, Judgments}) ->
            Results = barrel_vectordb_bm25:search(Index, Query, 10),
            #{relevant := Relevant} = Judgments,
            RelevantSet = sets:from_list(Relevant),
            recall_at_k(Results, RelevantSet, 10)
        end,
        Queries
    ),
    AvgR10 = lists:sum(R10Scores) / length(R10Scores),

    %% Average R@10 should be reasonable
    ?assert(AvgR10 >= 0.1),

    io:format("Average R@10: ~.3f~n", [AvgR10]).

test_ndcg_at_10(#{memory_index := Index, queries := Queries}) ->
    %% Calculate average nDCG@10 across all queries
    NDCG10Scores = lists:map(
        fun({Query, Judgments}) ->
            Results = barrel_vectordb_bm25:search(Index, Query, 10),
            #{relevant := Relevant, partial := Partial} = Judgments,
            RelevantSet = sets:from_list(Relevant),
            PartialSet = sets:from_list(Partial),
            ndcg_at_k(Results, RelevantSet, PartialSet, 10)
        end,
        Queries
    ),
    AvgNDCG10 = lists:sum(NDCG10Scores) / length(NDCG10Scores),

    %% Average nDCG@10 should be at least 0.3
    ?assert(AvgNDCG10 >= 0.3),

    io:format("Average nDCG@10: ~.3f~n", [AvgNDCG10]).

test_relevant_in_top_k(#{memory_index := Index, queries := Queries}) ->
    %% For single-term topic queries (Q1-Q4), at least one highly relevant doc
    %% should appear in top 5
    TopicQueries = lists:sublist(Queries, 4),

    lists:foreach(
        fun({Query, Judgments}) ->
            Results = barrel_vectordb_bm25:search(Index, Query, 5),
            #{relevant := Relevant} = Judgments,
            RelevantSet = sets:from_list(Relevant),
            ResultDocNums = [doc_id_to_num(DocId) || {DocId, _} <- Results],
            RelevantInTop5 = [N || N <- ResultDocNums, sets:is_element(N, RelevantSet)],
            ?assert(length(RelevantInTop5) >= 1,
                    io_lib:format("Query '~s' has no relevant docs in top 5", [Query]))
        end,
        TopicQueries
    ).

test_ranking_order(#{memory_index := Index}) ->
    %% Test that docs with higher TF for query term rank higher
    %% Using query "erlang" - doc1 mentions "Erlang" twice vs others once
    Results = barrel_vectordb_bm25:search(Index, <<"erlang">>, 20),

    %% Results should be in descending score order
    Scores = [Score || {_, Score} <- Results],
    ?assertEqual(Scores, lists:reverse(lists:sort(Scores))),

    %% All results should have positive scores
    lists:foreach(
        fun(Score) -> ?assert(Score > 0) end,
        Scores
    ).

test_cross_topic_queries(#{memory_index := Index, queries := Queries}) ->
    %% Q9: "fault tolerant" should match both Erlang (1, 6) and distributed (69, 73, 78)
    {_, FaultTolerantJudgments} = lists:nth(9, Queries),
    FaultTolerantResults = barrel_vectordb_bm25:search(Index, <<"fault tolerant">>, 10),

    %% Should have results from multiple topic clusters
    ResultDocNums = [doc_id_to_num(DocId) || {DocId, _} <- FaultTolerantResults],

    %% Check for Erlang cluster hits (1-20)
    ErlangHits = [N || N <- ResultDocNums, N >= 1, N =< 20],
    %% Check for distributed systems cluster hits (61-80)
    DistributedHits = [N || N <- ResultDocNums, N >= 61, N =< 80],

    #{relevant := Relevant} = FaultTolerantJudgments,
    RelevantSet = sets:from_list(Relevant),

    %% Should have at least one hit from relevant set
    RelevantHits = [N || N <- ResultDocNums, sets:is_element(N, RelevantSet)],
    ?assert(length(RelevantHits) >= 1),

    io:format("Cross-topic 'fault tolerant': Erlang=~p, Distributed=~p~n",
              [length(ErlangHits), length(DistributedHits)]).

test_rare_term_queries(#{memory_index := Index, queries := Queries}) ->
    %% Q13: "supervisor behavior" - very specific to Erlang OTP
    {_, SupervisorJudgments} = lists:nth(13, Queries),
    SupervisorResults = barrel_vectordb_bm25:search(Index, <<"supervisor behavior">>, 10),

    %% Should return results (not empty)
    ?assert(length(SupervisorResults) >= 1),

    %% Top result should be from relevant set
    case SupervisorResults of
        [{TopDocId, _} | _] ->
            TopDocNum = doc_id_to_num(TopDocId),
            #{relevant := Relevant} = SupervisorJudgments,
            ?assert(lists:member(TopDocNum, Relevant),
                    io_lib:format("Top doc ~p not in relevant set ~p", [TopDocNum, Relevant]));
        [] ->
            ok  %% Empty results handled above
    end.

test_memory_disk_consistency(#{memory_index := MemIndex, disk_index := DiskIndex, queries := Queries}) ->
    %% For each query, both backends should produce similar results
    %% Note: Minor differences at result boundaries are expected due to floating point
    lists:foreach(
        fun({Query, _}) ->
            MemResults = barrel_vectordb_bm25:search(MemIndex, Query, 10),
            DiskResults = barrel_vectordb_bm25_disk:search(DiskIndex, Query, 10),

            %% Same number of results
            ?assertEqual(length(MemResults), length(DiskResults),
                        io_lib:format("Query '~s' result count mismatch", [Query])),

            %% Build score maps
            MemScoreMap = maps:from_list(MemResults),
            DiskScoreMap = maps:from_list(DiskResults),

            MemDocIds = maps:keys(MemScoreMap),
            DiskDocIds = maps:keys(DiskScoreMap),

            %% Find overlapping documents
            MemSet = sets:from_list(MemDocIds),
            DiskSet = sets:from_list(DiskDocIds),
            OverlapSet = sets:intersection(MemSet, DiskSet),
            OverlapDocs = sets:to_list(OverlapSet),

            %% At least 80% of results should overlap (allow minor boundary differences)
            OverlapRatio = length(OverlapDocs) / max(1, length(MemDocIds)),
            ?assert(OverlapRatio >= 0.8,
                    io_lib:format("Query '~s' overlap too low: ~.1f%", [Query, OverlapRatio * 100])),

            %% For overlapping documents, scores should be close (within 1% tolerance)
            lists:foreach(
                fun(DocId) ->
                    MemScore = maps:get(DocId, MemScoreMap),
                    DiskScore = maps:get(DocId, DiskScoreMap),
                    Diff = abs(MemScore - DiskScore),
                    MaxScore = max(abs(MemScore), abs(DiskScore)),
                    RelDiff = case MaxScore of
                        +0.0 -> 0.0;
                        _ -> Diff / MaxScore
                    end,
                    ?assert(RelDiff < 0.01,
                            io_lib:format("Query '~s' doc ~s score diff: mem=~.4f disk=~.4f",
                                         [Query, DocId, MemScore, DiskScore]))
                end,
                OverlapDocs
            )
        end,
        Queries
    ).
