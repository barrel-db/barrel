%%%-------------------------------------------------------------------
%%% @doc BM25 sparse embeddings (pure Erlang, in-memory)
%%%
%%% Implements BM25 (Best Matching 25) algorithm for sparse text retrieval.
%%% No external dependencies - runs entirely in Erlang.
%%%
%%% == Important: In-Memory Index ==
%%% This module maintains an **in-memory** index. The index is not persisted
%%% and will be lost when the process terminates. This design is intentional:
%%%
%%% - Simple and fast for small/medium datasets
%%% - Suitable for hybrid search (combine with dense vectors)
%%% - Can be rebuilt from documents stored in barrel_vectordb on startup
%%% - No additional storage overhead
%%%
%%% For large datasets requiring persistence, consider rebuilding the index
%%% from your document store on application startup.
%%%
%%% == Use Cases ==
%%% - Hybrid search: combine BM25 keyword scores with dense vector similarity
%%% - Re-ranking: use BM25 to re-rank dense search results
%%% - Keyword filtering: pre-filter candidates before expensive dense search
%%% - Small datasets: where the index comfortably fits in memory
%%%
%%% == Usage ==
%%% ```
%%% %% Create a new BM25 index
%%% Index = barrel_vectordb_bm25:new().
%%%
%%% %% Add documents
%%% Index1 = barrel_vectordb_bm25:add(Index, <<"doc1">>, <<"Hello world">>).
%%% Index2 = barrel_vectordb_bm25:add(Index1, <<"doc2">>, <<"Hello there">>).
%%%
%%% %% Search
%%% Results = barrel_vectordb_bm25:search(Index2, <<"hello">>, 10).
%%% %% => [{<<"doc1">>, 0.85}, {<<"doc2">>, 0.82}]
%%%
%%% %% Get sparse vector for a document
%%% {ok, SparseVec} = barrel_vectordb_bm25:get_vector(Index2, <<"doc1">>).
%%% %% => #{<<"hello">> => 0.5, <<"world">> => 0.8}
%%%
%%% %% Hybrid search example (combine with dense vectors)
%%% DenseResults = barrel_vectordb:search(Store, Query, #{k => 100}),
%%% BM25Results = barrel_vectordb_bm25:search(Index, Query, 100),
%%% HybridResults = combine_scores(DenseResults, BM25Results, 0.5).
%%% '''
%%%
%%% == Algorithm ==
%%% BM25 score = sum(IDF(t) * TF(t,d) * (k1 + 1) / (TF(t,d) + k1 * (1 - b + b * |d| / avgdl)))
%%%
%%% Where:
%%% - IDF(t) = log((N - n(t) + 0.5) / (n(t) + 0.5) + 1)
%%% - TF(t,d) = term frequency of t in document d
%%% - N = total number of documents
%%% - n(t) = number of documents containing term t
%%% - |d| = length of document d (in tokens)
%%% - avgdl = average document length
%%% - k1 = 1.2 (term frequency saturation parameter)
%%% - b = 0.75 (length normalization parameter)
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_bm25).

%% API
-export([
    new/0,
    new/1,
    add/3,
    remove/2,
    search/3,
    get_vector/2,
    encode/2,
    stats/1
]).

%% Types
-type bm25_index() :: #{
    docs := #{binary() => doc_info()},
    vocab := #{binary() => vocab_info()},
    total_docs := non_neg_integer(),
    total_tokens := non_neg_integer(),
    config := bm25_config()
}.

-type doc_info() :: #{
    terms := #{binary() => pos_integer()},  %% term => count
    length := non_neg_integer()
}.

-type vocab_info() :: #{
    doc_freq := non_neg_integer()  %% number of docs containing this term
}.

-type bm25_config() :: #{
    k1 := float(),           %% term frequency saturation (default: 1.2)
    b := float(),            %% length normalization (default: 0.75)
    min_term_length := pos_integer(),  %% minimum token length (default: 1)
    lowercase := boolean()   %% lowercase tokens (default: true)
}.

-type sparse_vector() :: #{binary() => float()}.

-export_type([bm25_index/0, bm25_config/0, sparse_vector/0]).

%% Default parameters
-define(DEFAULT_K1, 1.2).
-define(DEFAULT_B, 0.75).
-define(DEFAULT_MIN_TERM_LENGTH, 1).

%%====================================================================
%% API
%%====================================================================

%% @doc Create a new empty BM25 index with default parameters.
-spec new() -> bm25_index().
new() ->
    new(#{}).

%% @doc Create a new empty BM25 index with custom parameters.
-spec new(map()) -> bm25_index().
new(Config) ->
    #{
        docs => #{},
        vocab => #{},
        total_docs => 0,
        total_tokens => 0,
        config => #{
            k1 => maps:get(k1, Config, ?DEFAULT_K1),
            b => maps:get(b, Config, ?DEFAULT_B),
            min_term_length => maps:get(min_term_length, Config, ?DEFAULT_MIN_TERM_LENGTH),
            lowercase => maps:get(lowercase, Config, true)
        }
    }.

%% @doc Add a document to the index.
-spec add(bm25_index(), binary(), binary()) -> bm25_index().
add(Index, DocId, Text) ->
    #{docs := Docs, vocab := Vocab, total_docs := N,
      total_tokens := TotalTokens, config := Config} = Index,

    %% Remove existing doc if present
    {Docs1, Vocab1, N1, TotalTokens1} = case maps:get(DocId, Docs, undefined) of
        undefined ->
            {Docs, Vocab, N, TotalTokens};
        ExistingDoc ->
            remove_doc_from_index(DocId, ExistingDoc, Docs, Vocab, N, TotalTokens)
    end,

    %% Tokenize the text
    Terms = tokenize(Text, Config),
    TermCounts = count_terms(Terms),
    DocLength = length(Terms),

    %% Create doc info
    DocInfo = #{
        terms => TermCounts,
        length => DocLength
    },

    %% Update vocabulary with document frequencies
    Vocab2 = maps:fold(
        fun(Term, _Count, VocabAcc) ->
            case maps:get(Term, VocabAcc, undefined) of
                undefined ->
                    VocabAcc#{Term => #{doc_freq => 1}};
                #{doc_freq := DF} ->
                    VocabAcc#{Term => #{doc_freq => DF + 1}}
            end
        end,
        Vocab1,
        TermCounts
    ),

    Index#{
        docs => Docs1#{DocId => DocInfo},
        vocab => Vocab2,
        total_docs => N1 + 1,
        total_tokens => TotalTokens1 + DocLength
    }.

%% @doc Remove a document from the index.
-spec remove(bm25_index(), binary()) -> bm25_index().
remove(Index, DocId) ->
    #{docs := Docs, vocab := Vocab, total_docs := N, total_tokens := TotalTokens} = Index,

    case maps:get(DocId, Docs, undefined) of
        undefined ->
            Index;
        DocInfo ->
            {Docs1, Vocab1, N1, TotalTokens1} =
                remove_doc_from_index(DocId, DocInfo, Docs, Vocab, N, TotalTokens),
            Index#{
                docs => Docs1,
                vocab => Vocab1,
                total_docs => N1,
                total_tokens => TotalTokens1
            }
    end.

%% @doc Search the index with a query.
%% Returns list of {DocId, Score} sorted by score descending.
-spec search(bm25_index(), binary(), pos_integer()) -> [{binary(), float()}].
search(Index, Query, K) ->
    #{docs := Docs, vocab := Vocab, total_docs := N,
      total_tokens := TotalTokens, config := Config} = Index,

    case N of
        0 -> [];
        _ ->
            %% Tokenize query
            QueryTerms = tokenize(Query, Config),
            QueryTermCounts = count_terms(QueryTerms),

            %% Calculate average document length
            AvgDL = TotalTokens / N,

            %% Score each document
            Scores = maps:fold(
                fun(DocId, DocInfo, Acc) ->
                    Score = score_document(DocInfo, QueryTermCounts, Vocab, N, AvgDL, Config),
                    case Score > 0 of
                        true -> [{DocId, Score} | Acc];
                        false -> Acc
                    end
                end,
                [],
                Docs
            ),

            %% Sort by score descending and take top K
            Sorted = lists:sort(fun({_, S1}, {_, S2}) -> S1 > S2 end, Scores),
            lists:sublist(Sorted, K)
    end.

%% @doc Get the sparse vector representation of a document.
%% Returns term weights based on BM25 scoring.
-spec get_vector(bm25_index(), binary()) -> {ok, sparse_vector()} | {error, not_found}.
get_vector(Index, DocId) ->
    #{docs := Docs, vocab := Vocab, total_docs := N,
      total_tokens := TotalTokens, config := Config} = Index,

    case maps:get(DocId, Docs, undefined) of
        undefined ->
            {error, not_found};
        DocInfo ->
            AvgDL = case N of
                0 -> 1;
                _ -> TotalTokens / N
            end,
            Vector = compute_doc_vector(DocInfo, Vocab, N, AvgDL, Config),
            {ok, Vector}
    end.

%% @doc Encode text into a sparse vector without adding to index.
%% Uses current index statistics for IDF calculation.
-spec encode(bm25_index(), binary()) -> sparse_vector().
encode(Index, Text) ->
    #{vocab := Vocab, total_docs := N,
      total_tokens := TotalTokens, config := Config} = Index,

    Terms = tokenize(Text, Config),
    TermCounts = count_terms(Terms),
    DocLength = length(Terms),

    DocInfo = #{terms => TermCounts, length => DocLength},
    AvgDL = case N of
        0 -> DocLength;
        _ -> TotalTokens / N
    end,

    %% For encoding, we use N+1 to account for the "virtual" document
    compute_doc_vector(DocInfo, Vocab, max(N, 1), AvgDL, Config).

%% @doc Get index statistics.
-spec stats(bm25_index()) -> map().
stats(Index) ->
    #{docs := _Docs, vocab := Vocab, total_docs := N,
      total_tokens := TotalTokens, config := Config} = Index,
    #{
        total_docs => N,
        total_tokens => TotalTokens,
        vocab_size => maps:size(Vocab),
        avg_doc_length => case N of 0 -> 0; _ -> TotalTokens / N end,
        config => Config
    }.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private
remove_doc_from_index(DocId, DocInfo, Docs, Vocab, N, TotalTokens) ->
    #{terms := Terms, length := DocLength} = DocInfo,

    %% Update vocabulary
    Vocab1 = maps:fold(
        fun(Term, _Count, VocabAcc) ->
            case maps:get(Term, VocabAcc, undefined) of
                undefined ->
                    VocabAcc;
                #{doc_freq := 1} ->
                    maps:remove(Term, VocabAcc);
                #{doc_freq := DF} ->
                    VocabAcc#{Term => #{doc_freq => DF - 1}}
            end
        end,
        Vocab,
        Terms
    ),

    {maps:remove(DocId, Docs), Vocab1, N - 1, TotalTokens - DocLength}.

%% @private
tokenize(Text, Config) ->
    #{min_term_length := MinLen, lowercase := Lowercase} = Config,

    %% Convert to lowercase if configured
    Text1 = case Lowercase of
        true -> string:lowercase(Text);
        false -> Text
    end,

    %% Split on non-alphanumeric characters
    Tokens = re:split(Text1, <<"[^a-zA-Z0-9]+">>, [{return, binary}, trim]),

    %% Filter by minimum length
    [T || T <- Tokens, byte_size(T) >= MinLen].

%% @private
count_terms(Terms) ->
    lists:foldl(
        fun(Term, Acc) ->
            maps:update_with(Term, fun(C) -> C + 1 end, 1, Acc)
        end,
        #{},
        Terms
    ).

%% @private
score_document(DocInfo, QueryTermCounts, Vocab, N, AvgDL, Config) ->
    #{terms := DocTerms, length := DocLength} = DocInfo,
    #{k1 := K1, b := B} = Config,

    maps:fold(
        fun(QueryTerm, _QueryCount, Score) ->
            case maps:get(QueryTerm, DocTerms, 0) of
                0 -> Score;
                TF ->
                    %% Get document frequency
                    DF = case maps:get(QueryTerm, Vocab, undefined) of
                        undefined -> 0;
                        #{doc_freq := D} -> D
                    end,

                    %% Calculate IDF
                    IDF = math:log((N - DF + 0.5) / (DF + 0.5) + 1),

                    %% Calculate BM25 term score
                    Numerator = TF * (K1 + 1),
                    Denominator = TF + K1 * (1 - B + B * DocLength / AvgDL),
                    TermScore = IDF * Numerator / Denominator,

                    Score + TermScore
            end
        end,
        0.0,
        QueryTermCounts
    ).

%% @private
compute_doc_vector(DocInfo, Vocab, N, AvgDL, Config) ->
    #{terms := DocTerms, length := DocLength} = DocInfo,
    #{k1 := K1, b := B} = Config,

    maps:fold(
        fun(Term, TF, Acc) ->
            %% Get document frequency (default to 1 for unknown terms)
            DF = case maps:get(Term, Vocab, undefined) of
                undefined -> 1;
                #{doc_freq := D} -> D
            end,

            %% Calculate IDF
            IDF = math:log((N - DF + 0.5) / (DF + 0.5) + 1),

            %% Calculate BM25 term weight
            Numerator = TF * (K1 + 1),
            Denominator = TF + K1 * (1 - B + B * DocLength / AvgDL),
            Weight = IDF * Numerator / Denominator,

            case Weight > 0 of
                true -> Acc#{Term => Weight};
                false -> Acc
            end
        end,
        #{},
        DocTerms
    ).
