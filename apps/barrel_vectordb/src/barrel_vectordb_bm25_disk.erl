%%%-------------------------------------------------------------------
%%% @doc Disk-Native BM25 Backend
%%%
%%% Implements BM25 text search with disk-native storage using:
%%% - Block-Max MaxScore algorithm for early termination
%%% - Hot layer for fast writes, background compaction to disk
%%% - RocksDB for term/doc ID mapping
%%% - mmap for block-max index reads
%%%
%%% Architecture:
%%% - Hot layer (RAM): Recent documents, sub-ms write latency
%%% - Disk layer (SSD): Compressed postings with block-max index
%%% - RocksDB: term string <-> int ID, doc string <-> int ID mapping
%%% - ETS: Doc lengths and stats (small, hot)
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_bm25_disk).

%% API
-export([
    new/1,
    add/3,
    remove/2,
    search/3,
    search_with_metrics/3,
    get_vector/2,
    encode/2,
    stats/1,
    %% Persistence
    open/1,
    close/1,
    sync/1,
    %% Index management
    compact/1,
    build/2,
    info/1
]).

-define(DEFAULT_K1, 1.2).
-define(DEFAULT_B, 0.75).
-define(DEFAULT_HOT_MAX_SIZE, 50000).
-define(DEFAULT_HOT_COMPACTION_THRESHOLD, 0.8).
-define(DEFAULT_BLOCK_SIZE, 128).

%% RocksDB column family names
-define(CF_TERMS_FWD, "terms_fwd").  %% term string -> term int ID
-define(CF_TERMS_REV, "terms_rev").  %% term int ID -> term string
-define(CF_DOCS_FWD, "docs_fwd").    %% doc string ID -> doc int ID
-define(CF_DOCS_REV, "docs_rev").    %% doc int ID -> doc string ID

-record(bm25_disk_config, {
    k1 = ?DEFAULT_K1 :: float(),
    b = ?DEFAULT_B :: float(),
    min_term_length = 1 :: pos_integer(),
    lowercase = true :: boolean(),
    block_size = ?DEFAULT_BLOCK_SIZE :: pos_integer()
}).

-record(bm25_disk_index, {
    config :: #bm25_disk_config{},
    base_path :: binary(),

    %% Global stats
    total_docs = 0 :: non_neg_integer(),
    total_tokens = 0 :: non_neg_integer(),

    %% ID mapping counters
    next_term_int_id = 0 :: non_neg_integer(),
    next_doc_int_id = 0 :: non_neg_integer(),

    %% RocksDB handles for ID mapping
    id_db :: rocksdb:db_handle() | undefined,
    cf_terms_fwd :: rocksdb:cf_handle() | undefined,
    cf_terms_rev :: rocksdb:cf_handle() | undefined,
    cf_docs_fwd :: rocksdb:cf_handle() | undefined,
    cf_docs_rev :: rocksdb:cf_handle() | undefined,
    id_db_standalone = false :: boolean(),

    %% File I/O
    file_handle :: term() | undefined,

    %% ETS tables for hot data (doc lengths, term stats)
    doc_stats_table :: ets:tid() | undefined,   %% {DocIntId, Length}
    term_stats_table :: ets:tid() | undefined,  %% {TermIntId, DocFreq}

    %% Hot layer (in-memory, for fast writes)
    hot_enabled = true :: boolean(),
    hot_max_size = ?DEFAULT_HOT_MAX_SIZE :: non_neg_integer(),
    hot_compaction_threshold = ?DEFAULT_HOT_COMPACTION_THRESHOLD :: float(),

    %% Hot layer data
    %% hot_postings: #{TermIntId => [{DocIntId, TF}]}
    hot_postings = #{} :: #{non_neg_integer() => [{non_neg_integer(), pos_integer()}]},
    %% hot_docs: #{DocIntId => #{TermIntId => TF}}
    hot_docs = #{} :: #{non_neg_integer() => #{non_neg_integer() => pos_integer()}},
    %% hot_doc_lengths: #{DocIntId => Length}
    hot_doc_lengths = #{} :: #{non_neg_integer() => non_neg_integer()},
    hot_size = 0 :: non_neg_integer(),
    hot_tokens = 0 :: non_neg_integer(),

    %% Disk layer stats (from file header)
    disk_doc_count = 0 :: non_neg_integer(),
    disk_term_count = 0 :: non_neg_integer(),
    disk_total_tokens = 0 :: non_neg_integer(),

    %% Block-max index (loaded from disk, kept in memory for search)
    blockmax_index = #{} :: #{non_neg_integer() => [map()]},

    %% Compaction state
    compaction_in_progress = false :: boolean()
}).

-type bm25_disk_index() :: #bm25_disk_index{}.
-type sparse_vector() :: #{binary() => float()}.

-export_type([bm25_disk_index/0, sparse_vector/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Create a new disk-native BM25 index
%% Options:
%%   - base_path: Path for disk storage (required)
%%   - k1: BM25 k1 parameter (default: 1.2)
%%   - b: BM25 b parameter (default: 0.75)
%%   - min_term_length: Minimum token length (default: 1)
%%   - lowercase: Lowercase tokens (default: true)
%%   - hot_max_size: Max docs in hot layer (default: 50000)
%%   - hot_compaction_threshold: Trigger compaction at this % (default: 0.8)
%%   - block_size: Documents per posting block (default: 128)
-spec new(map()) -> {ok, bm25_disk_index()} | {error, term()}.
new(Options) ->
    BasePath = maps:get(base_path, Options, undefined),
    case BasePath of
        undefined ->
            {error, base_path_required};
        _ ->
            BasePathBin = to_binary(BasePath),
            Config = #bm25_disk_config{
                k1 = maps:get(k1, Options, ?DEFAULT_K1),
                b = maps:get(b, Options, ?DEFAULT_B),
                min_term_length = maps:get(min_term_length, Options, 1),
                lowercase = maps:get(lowercase, Options, true),
                block_size = maps:get(block_size, Options, ?DEFAULT_BLOCK_SIZE)
            },
            HotMaxSize = maps:get(hot_max_size, Options, ?DEFAULT_HOT_MAX_SIZE),
            HotThreshold = maps:get(hot_compaction_threshold, Options, ?DEFAULT_HOT_COMPACTION_THRESHOLD),

            create_index(BasePathBin, Config, HotMaxSize, HotThreshold)
    end.

%% @doc Open an existing disk-native BM25 index
-spec open(binary() | string()) -> {ok, bm25_disk_index()} | {error, term()}.
open(Path) ->
    BasePathBin = to_binary(Path),
    case barrel_vectordb_bm25_disk_file:open(BasePathBin) of
        {ok, FileHandle} ->
            Header = barrel_vectordb_bm25_disk_file:read_header(FileHandle),
            Config = #bm25_disk_config{
                k1 = maps:get(k1, Header, ?DEFAULT_K1),
                b = maps:get(b, Header, ?DEFAULT_B),
                block_size = maps:get(block_size, Header, ?DEFAULT_BLOCK_SIZE)
            },
            case open_id_db(BasePathBin) of
                {ok, IdDb, CfTermsFwd, CfTermsRev, CfDocsFwd, CfDocsRev, Standalone} ->
                    %% Load counters from RocksDB
                    NextTermId = get_next_id(IdDb, CfTermsRev),
                    NextDocId = get_next_id(IdDb, CfDocsRev),

                    %% Create ETS tables
                    DocStatsTable = ets:new(bm25_doc_stats, [set, public]),
                    TermStatsTable = ets:new(bm25_term_stats, [set, public]),

                    %% Load block-max index
                    {ok, BlockmaxIndex} = barrel_vectordb_bm25_disk_file:read_blockmax_index(FileHandle),

                    %% Load doc stats from disk (if present)
                    load_doc_stats_from_disk(FileHandle, DocStatsTable),

                    {ok, #bm25_disk_index{
                        config = Config,
                        base_path = BasePathBin,
                        total_docs = maps:get(doc_count, Header, 0),
                        total_tokens = maps:get(total_tokens, Header, 0),
                        next_term_int_id = NextTermId,
                        next_doc_int_id = NextDocId,
                        id_db = IdDb,
                        cf_terms_fwd = CfTermsFwd,
                        cf_terms_rev = CfTermsRev,
                        cf_docs_fwd = CfDocsFwd,
                        cf_docs_rev = CfDocsRev,
                        id_db_standalone = Standalone,
                        file_handle = FileHandle,
                        doc_stats_table = DocStatsTable,
                        term_stats_table = TermStatsTable,
                        disk_doc_count = maps:get(doc_count, Header, 0),
                        disk_term_count = maps:get(term_count, Header, 0),
                        disk_total_tokens = maps:get(total_tokens, Header, 0),
                        blockmax_index = BlockmaxIndex
                    }};
                {error, _} = Error ->
                    barrel_vectordb_bm25_disk_file:close(FileHandle),
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

%% @doc Close the index
-spec close(bm25_disk_index()) -> ok.
close(#bm25_disk_index{file_handle = FileHandle, id_db = IdDb,
                        id_db_standalone = Standalone,
                        doc_stats_table = DocStats, term_stats_table = TermStats}) ->
    %% Close file handle
    case FileHandle of
        undefined -> ok;
        _ -> barrel_vectordb_bm25_disk_file:close(FileHandle)
    end,

    %% Close RocksDB if standalone
    _ = case Standalone andalso IdDb =/= undefined of
        true -> rocksdb:close(IdDb);
        false -> ok
    end,

    %% Delete ETS tables
    catch ets:delete(DocStats),
    catch ets:delete(TermStats),
    ok.

%% @doc Sync to disk
-spec sync(bm25_disk_index()) -> ok.
sync(#bm25_disk_index{file_handle = FileHandle, id_db = IdDb}) ->
    case FileHandle of
        undefined -> ok;
        _ -> barrel_vectordb_bm25_disk_file:sync(FileHandle)
    end,
    case IdDb of
        undefined -> ok;
        _ ->
            catch rocksdb:sync_wal(IdDb),
            ok
    end.

%% @doc Add a document to the index
-spec add(bm25_disk_index(), binary(), binary()) -> {ok, bm25_disk_index()} | {error, term()}.
add(Index, DocId, Text) ->
    #bm25_disk_index{
        config = Config,
        hot_docs = HotDocs,
        hot_doc_lengths = HotDocLengths,
        hot_size = HotSize,
        hot_tokens = HotTokens,
        hot_max_size = HotMaxSize,
        hot_compaction_threshold = HotThreshold,
        total_docs = TotalDocs,
        total_tokens = TotalTokens
    } = Index,

    %% Get or create doc int ID
    {DocIntId, Index1} = get_or_create_doc_int_id(Index, DocId),

    %% Check if doc already exists in hot layer
    {Index2, WasInHot, _OldLength, OldTokens} = case maps:get(DocIntId, HotDocs, undefined) of
        undefined ->
            {Index1, false, 0, 0};
        OldTerms ->
            %% Remove old doc from hot layer
            OldLen = maps:get(DocIntId, HotDocLengths, 0),
            OldToks = maps:fold(fun(_, TF, Acc) -> Acc + TF end, 0, OldTerms),
            Index1a = remove_doc_from_hot(Index1, DocIntId, OldTerms),
            {Index1a, true, OldLen, OldToks}
    end,

    %% Tokenize text
    Terms = tokenize(Text, Config),
    TermCounts = count_terms(Terms),
    DocLength = length(Terms),
    NewTokens = lists:sum(maps:values(TermCounts)),

    %% Get or create term int IDs
    {TermIntIds, Index3} = get_or_create_term_int_ids(Index2, maps:keys(TermCounts)),

    %% Build term int counts map
    TermIntCounts = maps:fold(
        fun(Term, Count, Acc) ->
            TermIntId = maps:get(Term, TermIntIds),
            Acc#{TermIntId => Count}
        end,
        #{},
        TermCounts
    ),

    %% Add to hot layer
    NewHotPostings = maps:fold(
        fun(TermIntId, TF, Acc) ->
            Existing = maps:get(TermIntId, Acc, []),
            Acc#{TermIntId => [{DocIntId, TF} | Existing]}
        end,
        Index3#bm25_disk_index.hot_postings,
        TermIntCounts
    ),

    NewHotDocs = (Index3#bm25_disk_index.hot_docs)#{DocIntId => TermIntCounts},
    NewHotDocLengths = (Index3#bm25_disk_index.hot_doc_lengths)#{DocIntId => DocLength},

    %% Update stats
    {NewTotalDocs, NewHotSize} = case WasInHot of
        true -> {TotalDocs, HotSize};  %% Doc was already counted
        false -> {TotalDocs + 1, HotSize + 1}
    end,
    NewTotalTokens = TotalTokens - OldTokens + NewTokens,
    NewHotTokens = HotTokens - OldTokens + NewTokens,

    Index4 = Index3#bm25_disk_index{
        hot_postings = NewHotPostings,
        hot_docs = NewHotDocs,
        hot_doc_lengths = NewHotDocLengths,
        hot_size = NewHotSize,
        hot_tokens = NewHotTokens,
        total_docs = NewTotalDocs,
        total_tokens = NewTotalTokens
    },

    %% Check if compaction needed
    case NewHotSize >= HotMaxSize * HotThreshold of
        true ->
            %% Trigger compaction
            compact(Index4);
        false ->
            {ok, Index4}
    end.

%% @doc Remove a document from the index
-spec remove(bm25_disk_index(), binary()) -> {ok, bm25_disk_index()} | {error, not_found}.
remove(Index, DocId) ->
    case get_doc_int_id(Index, DocId) of
        {ok, DocIntId} ->
            #bm25_disk_index{
                hot_docs = HotDocs,
                hot_doc_lengths = HotDocLengths,
                hot_size = HotSize,
                hot_tokens = HotTokens,
                total_docs = TotalDocs,
                total_tokens = TotalTokens
            } = Index,

            case maps:get(DocIntId, HotDocs, undefined) of
                undefined ->
                    %% Doc not in hot layer, mark for deletion in disk layer
                    %% TODO: Implement disk deletion tracking
                    {error, not_found};
                TermCounts ->
                    %% Remove from hot layer
                    OldLength = maps:get(DocIntId, HotDocLengths, 0),
                    OldTokens = maps:fold(fun(_, TF, Acc) -> Acc + TF end, 0, TermCounts),

                    Index1 = remove_doc_from_hot(Index, DocIntId, TermCounts),

                    {ok, Index1#bm25_disk_index{
                        hot_docs = maps:remove(DocIntId, HotDocs),
                        hot_doc_lengths = maps:remove(DocIntId, HotDocLengths),
                        hot_size = HotSize - 1,
                        hot_tokens = HotTokens - OldTokens,
                        total_docs = TotalDocs - 1,
                        total_tokens = TotalTokens - OldLength
                    }}
            end;
        {error, not_found} ->
            {error, not_found}
    end.

%% @doc Search the index
-spec search(bm25_disk_index(), binary(), pos_integer()) -> [{binary(), float()}].
search(Index, Query, K) ->
    {Results, _Metrics} = search_with_metrics(Index, Query, K),
    Results.

%% @doc Search the index and return metrics for debugging/tuning
%% Returns {Results, Metrics} where Metrics includes block skip statistics
-spec search_with_metrics(bm25_disk_index(), binary(), pos_integer()) ->
    {[{binary(), float()}], map()}.
search_with_metrics(Index, Query, K) ->
    #bm25_disk_index{
        config = Config,
        total_docs = TotalDocs,
        total_tokens = TotalTokens
    } = Index,

    case TotalDocs of
        0 -> {[], #{blocks_total => 0, blocks_scanned => 0, blocks_skipped => 0}};
        _ ->
            %% Tokenize query
            QueryTerms = tokenize(Query, Config),
            QueryTermCounts = count_terms(QueryTerms),

            %% Get term int IDs for query terms
            {TermIntIds, _} = get_term_int_ids(Index, maps:keys(QueryTermCounts)),

            %% Calculate avgdl
            AvgDL = TotalTokens / TotalDocs,

            %% Search hot layer
            HotResults = search_hot_layer(Index, TermIntIds, AvgDL),

            %% Search disk layer using Block-Max MaxScore (with metrics)
            {DiskResults, DiskMetrics} = search_disk_layer_with_metrics(Index, TermIntIds, AvgDL, K),

            %% Merge results
            MergedResults = merge_search_results(HotResults, DiskResults),

            %% Sort and take top K
            Sorted = lists:sort(fun({_, S1}, {_, S2}) -> S1 > S2 end, MergedResults),
            TopK = lists:sublist(Sorted, K),

            %% Convert back to string IDs
            Results = [{get_doc_string_id(Index, DocIntId), Score} || {DocIntId, Score} <- TopK],

            %% Add additional metrics
            Metrics = DiskMetrics#{
                query_terms => maps:size(TermIntIds),
                hot_results => length(HotResults),
                disk_results => length(DiskResults),
                total_results => length(TopK)
            },

            {Results, Metrics}
    end.

%% @doc Get sparse vector representation of a document
-spec get_vector(bm25_disk_index(), binary()) -> {ok, sparse_vector()} | {error, not_found}.
get_vector(Index, DocId) ->
    case get_doc_int_id(Index, DocId) of
        {ok, DocIntId} ->
            #bm25_disk_index{
                config = Config,
                hot_docs = HotDocs,
                hot_doc_lengths = HotDocLengths,
                total_docs = TotalDocs,
                total_tokens = TotalTokens
            } = Index,

            case maps:get(DocIntId, HotDocs, undefined) of
                undefined ->
                    %% TODO: Load from disk layer
                    {error, not_found};
                TermIntCounts ->
                    DocLength = maps:get(DocIntId, HotDocLengths, 0),
                    AvgDL = case TotalDocs of
                        0 -> 1;
                        _ -> TotalTokens / TotalDocs
                    end,

                    Vector = compute_doc_vector(Index, TermIntCounts, DocLength, AvgDL, Config),
                    {ok, Vector}
            end;
        {error, not_found} ->
            {error, not_found}
    end.

%% @doc Encode text into sparse vector without adding to index
-spec encode(bm25_disk_index(), binary()) -> sparse_vector().
encode(Index, Text) ->
    #bm25_disk_index{
        config = Config,
        total_docs = TotalDocs,
        total_tokens = TotalTokens
    } = Index,

    Terms = tokenize(Text, Config),
    TermCounts = count_terms(Terms),
    DocLength = length(Terms),

    AvgDL = case TotalDocs of
        0 -> DocLength;
        _ -> TotalTokens / TotalDocs
    end,

    %% Get term int IDs (don't create new ones for encode)
    {TermIntIds, _} = get_term_int_ids(Index, maps:keys(TermCounts)),

    %% Build term int counts for known terms only
    TermIntCounts = maps:fold(
        fun(Term, Count, Acc) ->
            case maps:get(Term, TermIntIds, undefined) of
                undefined -> Acc;
                TermIntId -> Acc#{TermIntId => Count}
            end
        end,
        #{},
        TermCounts
    ),

    compute_doc_vector(Index, TermIntCounts, DocLength, AvgDL, Config).

%% @doc Get index statistics
-spec stats(bm25_disk_index()) -> map().
stats(#bm25_disk_index{
    config = Config,
    total_docs = TotalDocs,
    total_tokens = TotalTokens,
    hot_size = HotSize,
    hot_tokens = HotTokens,
    disk_doc_count = DiskDocCount,
    disk_term_count = DiskTermCount,
    next_term_int_id = NextTermId
}) ->
    #{
        total_docs => TotalDocs,
        total_tokens => TotalTokens,
        vocab_size => NextTermId,
        avg_doc_length => case TotalDocs of 0 -> 0; _ -> TotalTokens / TotalDocs end,
        hot_docs => HotSize,
        hot_tokens => HotTokens,
        disk_docs => DiskDocCount,
        disk_terms => DiskTermCount,
        config => #{
            k1 => Config#bm25_disk_config.k1,
            b => Config#bm25_disk_config.b,
            block_size => Config#bm25_disk_config.block_size
        }
    }.

%% @doc Get detailed index info
-spec info(bm25_disk_index()) -> map().
info(Index) ->
    Stats = stats(Index),
    Stats#{
        base_path => Index#bm25_disk_index.base_path,
        hot_enabled => Index#bm25_disk_index.hot_enabled,
        hot_max_size => Index#bm25_disk_index.hot_max_size,
        compaction_in_progress => Index#bm25_disk_index.compaction_in_progress
    }.

%% @doc Build index from list of documents
%% Docs format: [{DocId, Text}, ...]
-spec build(bm25_disk_index(), [{binary(), binary()}]) -> {ok, bm25_disk_index()} | {error, term()}.
build(Index, []) ->
    {ok, Index};
build(Index, [{DocId, Text} | Rest]) ->
    case add(Index, DocId, Text) of
        {ok, Index1} ->
            build(Index1, Rest);
        {error, _} = Error ->
            Error
    end.

%% @doc Compact hot layer to disk
-spec compact(bm25_disk_index()) -> {ok, bm25_disk_index()} | {error, term()}.
compact(#bm25_disk_index{hot_size = 0} = Index) ->
    %% Nothing to compact
    {ok, Index};
compact(#bm25_disk_index{compaction_in_progress = true} = Index) ->
    %% Already compacting
    {ok, Index};
compact(Index) ->
    Index1 = Index#bm25_disk_index{compaction_in_progress = true},

    try
        %% 1. Build sorted postings per term from hot layer
        SortedPostings = build_sorted_postings(Index1),

        %% 2. Chunk into blocks and compute max impact per block
        {BlockMaxIndex, PostingBlocks} = build_block_max_index(Index1, SortedPostings),

        %% 3. Write postings to disk
        {ok, NewFileHandle} = write_postings_to_disk(Index1, PostingBlocks),

        %% 4. Write block-max index
        {ok, NewFileHandle2} = barrel_vectordb_bm25_disk_file:write_blockmax_index(
            NewFileHandle, BlockMaxIndex),

        %% 5. Update header stats
        #bm25_disk_index{
            total_docs = TotalDocs,
            total_tokens = TotalTokens,
            next_term_int_id = NextTermId
        } = Index1,

        {ok, NewFileHandle3} = barrel_vectordb_bm25_disk_file:update_stats(NewFileHandle2, #{
            doc_count => TotalDocs,
            term_count => NextTermId,
            total_tokens => TotalTokens,
            avgdl => case TotalDocs of 0 -> 0.0; _ -> TotalTokens / TotalDocs end
        }),

        %% 6. Clear hot layer
        Index2 = Index1#bm25_disk_index{
            file_handle = NewFileHandle3,
            hot_postings = #{},
            hot_docs = #{},
            hot_doc_lengths = #{},
            hot_size = 0,
            hot_tokens = 0,
            disk_doc_count = TotalDocs,
            disk_term_count = NextTermId,
            disk_total_tokens = TotalTokens,
            blockmax_index = BlockMaxIndex,
            compaction_in_progress = false
        },

        {ok, Index2}
    catch
        _:Reason ->
            {error, {compaction_failed, Reason}}
    end.

%%====================================================================
%% Internal Functions - Index Creation
%%====================================================================

create_index(BasePathBin, Config, HotMaxSize, HotThreshold) ->
    %% Create directory
    ok = filelib:ensure_dir(filename:join(BasePathBin, "dummy")),

    %% Create file handle
    FileConfig = #{
        k1 => Config#bm25_disk_config.k1,
        b => Config#bm25_disk_config.b,
        block_size => Config#bm25_disk_config.block_size
    },
    case barrel_vectordb_bm25_disk_file:create(BasePathBin, FileConfig) of
        {ok, FileHandle} ->
            %% Open/create RocksDB for ID mapping
            case open_id_db(BasePathBin) of
                {ok, IdDb, CfTermsFwd, CfTermsRev, CfDocsFwd, CfDocsRev, Standalone} ->
                    %% Create ETS tables
                    DocStatsTable = ets:new(bm25_doc_stats, [set, public]),
                    TermStatsTable = ets:new(bm25_term_stats, [set, public]),

                    {ok, #bm25_disk_index{
                        config = Config,
                        base_path = BasePathBin,
                        id_db = IdDb,
                        cf_terms_fwd = CfTermsFwd,
                        cf_terms_rev = CfTermsRev,
                        cf_docs_fwd = CfDocsFwd,
                        cf_docs_rev = CfDocsRev,
                        id_db_standalone = Standalone,
                        file_handle = FileHandle,
                        doc_stats_table = DocStatsTable,
                        term_stats_table = TermStatsTable,
                        hot_max_size = HotMaxSize,
                        hot_compaction_threshold = HotThreshold
                    }};
                {error, _} = Error ->
                    barrel_vectordb_bm25_disk_file:close(FileHandle),
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

open_id_db(BasePathBin) ->
    DbPath = filename:join(BasePathBin, "bm25.ids"),
    DbPathList = binary_to_list(DbPath),

    CfNames = ["default", ?CF_TERMS_FWD, ?CF_TERMS_REV, ?CF_DOCS_FWD, ?CF_DOCS_REV],
    CfOpts = [{create_if_missing, true}],
    CfDescriptors = [{Name, CfOpts} || Name <- CfNames],

    DbOpts = [{create_if_missing, true}, {create_missing_column_families, true}],

    case rocksdb:open(DbPathList, DbOpts, CfDescriptors) of
        {ok, Db, [_DefaultCf, CfTermsFwd, CfTermsRev, CfDocsFwd, CfDocsRev]} ->
            {ok, Db, CfTermsFwd, CfTermsRev, CfDocsFwd, CfDocsRev, true};
        {error, _} = Error ->
            Error
    end.

get_next_id(Db, CfRev) ->
    %% Scan reverse CF to find highest ID
    case rocksdb:iterator(Db, CfRev, []) of
        {ok, Iter} ->
            try
                case rocksdb:iterator_move(Iter, last) of
                    {ok, KeyBin, _} ->
                        <<Id:64/big>> = KeyBin,
                        Id + 1;
                    {error, invalid_iterator} ->
                        0
                end
            after
                rocksdb:iterator_close(Iter)
            end;
        {error, _} ->
            0
    end.

load_doc_stats_from_disk(_FileHandle, _DocStatsTable) ->
    %% TODO: Implement loading doc stats from disk
    ok.

%%====================================================================
%% Internal Functions - ID Mapping
%%====================================================================

get_or_create_doc_int_id(#bm25_disk_index{
    id_db = Db,
    cf_docs_fwd = CfFwd,
    cf_docs_rev = CfRev,
    next_doc_int_id = NextId
} = Index, DocId) ->
    case rocksdb:get(Db, CfFwd, DocId, []) of
        {ok, IntIdBin} ->
            <<IntId:64/big>> = IntIdBin,
            {IntId, Index};
        not_found ->
            IntId = NextId,
            IntIdBin = <<IntId:64/big>>,
            ok = rocksdb:put(Db, CfFwd, DocId, IntIdBin, []),
            ok = rocksdb:put(Db, CfRev, IntIdBin, DocId, []),
            {IntId, Index#bm25_disk_index{next_doc_int_id = NextId + 1}}
    end.

get_doc_int_id(#bm25_disk_index{id_db = Db, cf_docs_fwd = CfFwd}, DocId) ->
    case rocksdb:get(Db, CfFwd, DocId, []) of
        {ok, IntIdBin} ->
            <<IntId:64/big>> = IntIdBin,
            {ok, IntId};
        not_found ->
            {error, not_found}
    end.

get_doc_string_id(#bm25_disk_index{id_db = Db, cf_docs_rev = CfRev}, DocIntId) ->
    IntIdBin = <<DocIntId:64/big>>,
    case rocksdb:get(Db, CfRev, IntIdBin, []) of
        {ok, DocId} -> DocId;
        not_found -> <<"unknown">>
    end.

get_or_create_term_int_ids(Index, Terms) ->
    lists:foldl(
        fun(Term, {AccMap, AccIndex}) ->
            {IntId, NewIndex} = get_or_create_term_int_id(AccIndex, Term),
            {AccMap#{Term => IntId}, NewIndex}
        end,
        {#{}, Index},
        Terms
    ).

get_or_create_term_int_id(#bm25_disk_index{
    id_db = Db,
    cf_terms_fwd = CfFwd,
    cf_terms_rev = CfRev,
    next_term_int_id = NextId
} = Index, Term) ->
    case rocksdb:get(Db, CfFwd, Term, []) of
        {ok, IntIdBin} ->
            <<IntId:64/big>> = IntIdBin,
            {IntId, Index};
        not_found ->
            IntId = NextId,
            IntIdBin = <<IntId:64/big>>,
            ok = rocksdb:put(Db, CfFwd, Term, IntIdBin, []),
            ok = rocksdb:put(Db, CfRev, IntIdBin, Term, []),
            {IntId, Index#bm25_disk_index{next_term_int_id = NextId + 1}}
    end.

get_term_int_ids(Index, Terms) ->
    lists:foldl(
        fun(Term, {AccMap, AccIndex}) ->
            case get_term_int_id(AccIndex, Term) of
                {ok, IntId} ->
                    {AccMap#{Term => IntId}, AccIndex};
                {error, not_found} ->
                    {AccMap, AccIndex}
            end
        end,
        {#{}, Index},
        Terms
    ).

get_term_int_id(#bm25_disk_index{id_db = Db, cf_terms_fwd = CfFwd}, Term) ->
    case rocksdb:get(Db, CfFwd, Term, []) of
        {ok, IntIdBin} ->
            <<IntId:64/big>> = IntIdBin,
            {ok, IntId};
        not_found ->
            {error, not_found}
    end.

get_term_string_id(#bm25_disk_index{id_db = Db, cf_terms_rev = CfRev}, TermIntId) ->
    IntIdBin = <<TermIntId:64/big>>,
    case rocksdb:get(Db, CfRev, IntIdBin, []) of
        {ok, Term} -> Term;
        not_found -> <<"unknown">>
    end.

%%====================================================================
%% Internal Functions - Tokenization
%%====================================================================

tokenize(Text, #bm25_disk_config{min_term_length = MinLen, lowercase = Lowercase}) ->
    Text1 = case Lowercase of
        true ->
            try
                string:lowercase(Text)
            catch
                _:_ ->
                    %% Fallback for non-Unicode strings
                    string:to_lower(binary_to_list(Text))
            end;
        false -> Text
    end,
    Tokens = re:split(Text1, <<"[^a-zA-Z0-9]+">>, [{return, binary}, trim]),
    [T || T <- Tokens, byte_size(T) >= MinLen].

count_terms(Terms) ->
    lists:foldl(
        fun(Term, Acc) ->
            maps:update_with(Term, fun(C) -> C + 1 end, 1, Acc)
        end,
        #{},
        Terms
    ).

%%====================================================================
%% Internal Functions - Hot Layer Operations
%%====================================================================

remove_doc_from_hot(Index, DocIntId, TermCounts) ->
    #bm25_disk_index{hot_postings = HotPostings} = Index,

    NewHotPostings = maps:fold(
        fun(TermIntId, _TF, Acc) ->
            case maps:get(TermIntId, Acc, []) of
                [] -> Acc;
                Postings ->
                    Filtered = [{D, T} || {D, T} <- Postings, D =/= DocIntId],
                    case Filtered of
                        [] -> maps:remove(TermIntId, Acc);
                        _ -> Acc#{TermIntId => Filtered}
                    end
            end
        end,
        HotPostings,
        TermCounts
    ),

    Index#bm25_disk_index{hot_postings = NewHotPostings}.

%%====================================================================
%% Internal Functions - Search
%%====================================================================

search_hot_layer(Index, TermIntIds, AvgDL) ->
    #bm25_disk_index{
        config = Config,
        hot_postings = HotPostings,
        hot_doc_lengths = HotDocLengths,
        total_docs = TotalDocs
    } = Index,

    %% Collect all doc IDs that match any query term
    AllDocIds = maps:fold(
        fun(_, TermIntId, Acc) ->
            case maps:get(TermIntId, HotPostings, []) of
                [] -> Acc;
                Postings ->
                    DocIds = [DocId || {DocId, _} <- Postings],
                    sets:union(Acc, sets:from_list(DocIds))
            end
        end,
        sets:new(),
        TermIntIds
    ),

    %% Score each document
    lists:filtermap(
        fun(DocIntId) ->
            DocLength = maps:get(DocIntId, HotDocLengths, 0),
            Score = score_document_hot(Index, DocIntId, TermIntIds, DocLength, AvgDL, TotalDocs, Config),
            case Score > 0 of
                true -> {true, {DocIntId, Score}};
                false -> false
            end
        end,
        sets:to_list(AllDocIds)
    ).

score_document_hot(Index, DocIntId, TermIntIds, DocLength, AvgDL, N, Config) ->
    #bm25_disk_index{hot_postings = HotPostings} = Index,
    #bm25_disk_config{k1 = K1, b = B} = Config,

    maps:fold(
        fun(_, TermIntId, Score) ->
            case maps:get(TermIntId, HotPostings, []) of
                [] -> Score;
                Postings ->
                    case lists:keyfind(DocIntId, 1, Postings) of
                        false -> Score;
                        {_, TF} ->
                            %% Calculate document frequency for this term
                            DF = length(Postings),
                            IDF = math:log((N - DF + 0.5) / (DF + 0.5) + 1),
                            Numerator = TF * (K1 + 1),
                            Denominator = TF + K1 * (1 - B + B * DocLength / max(AvgDL, 1)),
                            Score + IDF * Numerator / Denominator
                    end
            end
        end,
        0.0,
        TermIntIds
    ).

search_disk_layer_with_metrics(Index, TermIntIds, AvgDL, K) ->
    #bm25_disk_index{
        config = Config,
        blockmax_index = BlockMaxIndex,
        file_handle = FileHandle,
        total_docs = TotalDocs
    } = Index,

    case maps:size(BlockMaxIndex) of
        0 ->
            %% No disk data yet
            {[], #{blocks_total => 0, blocks_scanned => 0, blocks_skipped => 0}};
        _ ->
            %% Get blocks for each query term
            TermBlocks = maps:fold(
                fun(_, TermIntId, Acc) ->
                    case maps:get(TermIntId, BlockMaxIndex, []) of
                        [] -> Acc;
                        Blocks -> [{TermIntId, Blocks} | Acc]
                    end
                end,
                [],
                TermIntIds
            ),

            case TermBlocks of
                [] -> {[], #{blocks_total => 0, blocks_scanned => 0, blocks_skipped => 0}};
                _ ->
                    %% Count total blocks
                    TotalBlocks = lists:sum([length(Blocks) || {_, Blocks} <- TermBlocks]),
                    %% Use Block-Max MaxScore algorithm with metrics
                    {Results, Scanned, Skipped} = maxscore_search_with_metrics(
                        Index, TermBlocks, AvgDL, TotalDocs, K, Config, FileHandle),
                    Metrics = #{
                        blocks_total => TotalBlocks,
                        blocks_scanned => Scanned,
                        blocks_skipped => Skipped,
                        skip_rate => case TotalBlocks of
                            0 -> 0.0;
                            _ -> Skipped / TotalBlocks * 100
                        end
                    },
                    {Results, Metrics}
            end
    end.

%% Block-Max MaxScore search algorithm with metrics tracking
maxscore_search_with_metrics(_Index, [], _AvgDL, _N, _K, _Config, _FileHandle) ->
    {[], 0, 0};
maxscore_search_with_metrics(Index, TermBlocks, AvgDL, N, K, Config, FileHandle) ->
    #bm25_disk_config{k1 = K1, b = B} = Config,

    %% Compute IDF for each term upfront
    TermBlocksWithIDF = lists:map(
        fun({TermIntId, Blocks}) ->
            DF = get_term_doc_freq(Index, TermIntId),
            IDF = math:log((N - DF + 0.5) / (DF + 0.5) + 1),
            MaxImpact = lists:max([maps:get(max_impact, Blk) || Blk <- Blocks]),
            {TermIntId, Blocks, IDF, MaxImpact}
        end,
        TermBlocks
    ),

    %% Sort by IDF * MaxImpact
    SortedTermBlocks = lists:sort(
        fun({_, _, IDF1, Max1}, {_, _, IDF2, Max2}) ->
            (IDF1 * Max1) >= (IDF2 * Max2)
        end,
        TermBlocksWithIDF
    ),

    %% Process with metrics tracking
    {FinalResults, _, Scanned, Skipped} = lists:foldl(
        fun({TermIntId, Blocks, IDF, _MaxImpact}, {AccResults, AccThreshold, AccScanned, AccSkipped}) ->
            {NewResults, NewThreshold, BlocksScanned, BlocksSkipped} =
                process_blocks_with_metrics(Blocks, TermIntId, IDF, AvgDL, K1, B, FileHandle, Index,
                                            AccResults, AccThreshold, K, 0, 0),
            {NewResults, NewThreshold, AccScanned + BlocksScanned, AccSkipped + BlocksSkipped}
        end,
        {[], 0.0, 0, 0},
        SortedTermBlocks
    ),

    {FinalResults, Scanned, Skipped}.

process_blocks_with_metrics([], _TermIntId, _IDF, _AvgDL, _K1, _B, _FileHandle, _Index,
                            Results, Threshold, _K, Scanned, Skipped) ->
    {Results, Threshold, Scanned, Skipped};
process_blocks_with_metrics([Block | Rest], TermIntId, IDF, AvgDL, K1, B, FileHandle, Index,
                            Results, Threshold, K, Scanned, Skipped) ->
    #{max_impact := MaxImpact, offset := Offset, size := Size} = Block,

    %% Skip block if max impact can't beat threshold
    case MaxImpact * IDF < Threshold of
        true ->
            %% Block skipped
            process_blocks_with_metrics(Rest, TermIntId, IDF, AvgDL, K1, B, FileHandle, Index,
                                        Results, Threshold, K, Scanned, Skipped + 1);
        false ->
            %% Read and score postings
            case barrel_vectordb_bm25_disk_file:read_postings(FileHandle, Offset, Size) of
                {ok, Postings} ->
                    {NewResults, NewThreshold} = score_postings(
                        Postings, TermIntId, IDF, AvgDL, K1, B, Index,
                        Results, Threshold, K),
                    process_blocks_with_metrics(Rest, TermIntId, IDF, AvgDL, K1, B, FileHandle, Index,
                                                NewResults, NewThreshold, K, Scanned + 1, Skipped);
                {error, _} ->
                    process_blocks_with_metrics(Rest, TermIntId, IDF, AvgDL, K1, B, FileHandle, Index,
                                                Results, Threshold, K, Scanned, Skipped)
            end
    end.

score_postings([], _TermIntId, _IDF, _AvgDL, _K1, _B, _Index, Results, Threshold, _K) ->
    {Results, Threshold};
score_postings([{DocIntId, TF} | Rest], TermIntId, IDF, AvgDL, K1, B, Index,
               Results, Threshold, K) ->
    %% Get doc length
    DocLength = get_doc_length(Index, DocIntId),

    %% Calculate BM25 score for this term
    Numerator = TF * (K1 + 1),
    Denominator = TF + K1 * (1 - B + B * DocLength / max(AvgDL, 1)),
    TermScore = IDF * Numerator / Denominator,

    %% Update or create doc score in results
    {NewResults, NewThreshold} = update_results(DocIntId, TermScore, Results, Threshold, K),

    score_postings(Rest, TermIntId, IDF, AvgDL, K1, B, Index,
                   NewResults, NewThreshold, K).

update_results(DocIntId, TermScore, Results, Threshold, K) ->
    %% Find existing score for doc or create new entry
    case lists:keyfind(DocIntId, 1, Results) of
        {DocIntId, OldScore} ->
            NewScore = OldScore + TermScore,
            NewResults = lists:keyreplace(DocIntId, 1, Results, {DocIntId, NewScore}),
            {NewResults, Threshold};
        false ->
            NewResults = [{DocIntId, TermScore} | Results],
            case length(NewResults) > K of
                true ->
                    %% Sort and drop lowest
                    Sorted = lists:sort(fun({_, S1}, {_, S2}) -> S1 > S2 end, NewResults),
                    Trimmed = lists:sublist(Sorted, K),
                    {_, MinScore} = lists:last(Trimmed),
                    {Trimmed, MinScore};
                false ->
                    {NewResults, Threshold}
            end
    end.

get_term_doc_freq(#bm25_disk_index{hot_postings = HotPostings, blockmax_index = BlockMaxIndex}, TermIntId) ->
    HotDF = case maps:get(TermIntId, HotPostings, []) of
        [] -> 0;
        Postings -> length(Postings)
    end,
    DiskDF = case maps:get(TermIntId, BlockMaxIndex, []) of
        [] -> 0;
        Blocks ->
            %% Sum doc counts from all blocks
            lists:sum([maps:get(doc_end, B) - maps:get(doc_start, B) + 1 || B <- Blocks])
    end,
    HotDF + DiskDF.

get_doc_length(#bm25_disk_index{hot_doc_lengths = HotLengths, doc_stats_table = DocStatsTable}, DocIntId) ->
    case maps:get(DocIntId, HotLengths, undefined) of
        undefined ->
            case ets:lookup(DocStatsTable, DocIntId) of
                [{_, Length}] -> Length;
                [] -> 0
            end;
        Length ->
            Length
    end.

merge_search_results(HotResults, DiskResults) ->
    %% Merge by doc ID, taking max score
    Combined = HotResults ++ DiskResults,
    maps:to_list(
        lists:foldl(
            fun({DocIntId, Score}, Acc) ->
                case maps:get(DocIntId, Acc, undefined) of
                    undefined -> Acc#{DocIntId => Score};
                    OldScore -> Acc#{DocIntId => max(OldScore, Score)}
                end
            end,
            #{},
            Combined
        )
    ).

%%====================================================================
%% Internal Functions - Vector Computation
%%====================================================================

compute_doc_vector(Index, TermIntCounts, DocLength, AvgDL, Config) ->
    #bm25_disk_config{k1 = K1, b = B} = Config,
    #bm25_disk_index{total_docs = N} = Index,

    maps:fold(
        fun(TermIntId, TF, Acc) ->
            DF = get_term_doc_freq(Index, TermIntId),
            IDF = math:log((N - DF + 0.5) / (DF + 0.5) + 1),
            Numerator = TF * (K1 + 1),
            Denominator = TF + K1 * (1 - B + B * DocLength / max(AvgDL, 1)),
            Weight = IDF * Numerator / Denominator,
            case Weight > 0 of
                true ->
                    Term = get_term_string_id(Index, TermIntId),
                    Acc#{Term => Weight};
                false ->
                    Acc
            end
        end,
        #{},
        TermIntCounts
    ).

%%====================================================================
%% Internal Functions - Compaction
%%====================================================================

build_sorted_postings(#bm25_disk_index{hot_postings = HotPostings}) ->
    maps:map(
        fun(_TermIntId, Postings) ->
            lists:sort(fun({D1, _}, {D2, _}) -> D1 =< D2 end, Postings)
        end,
        HotPostings
    ).

build_block_max_index(#bm25_disk_index{config = Config, total_docs = N, total_tokens = TotalTokens},
                      SortedPostings) ->
    #bm25_disk_config{k1 = K1, b = B, block_size = BlockSize} = Config,
    AvgDL = case N of 0 -> 1; _ -> TotalTokens / N end,

    %% Process each term
    {BlockMaxIndex, PostingBlocks, _FinalOffset} = maps:fold(
        fun(TermIntId, Postings, {AccIndex, AccBlocks, AccOffset}) ->
            %% Calculate IDF for this term
            DF = length(Postings),
            IDF = math:log((N - DF + 0.5) / (DF + 0.5) + 1),

            %% Chunk into blocks
            Chunks = chunk_postings(Postings, BlockSize),

            %% Build block entries
            {TermBlocks, TermPostingBlocks, NewOffset} = lists:foldl(
                fun(Chunk, {BlockAcc, PostingAcc, OffsetAcc}) ->
                    %% Compute max impact for this block
                    MaxImpact = compute_max_impact(Chunk, IDF, K1, B, AvgDL),

                    {DocStart, _} = hd(Chunk),
                    {DocEnd, _} = lists:last(Chunk),

                    %% Encode posting block
                    EncodedBlock = barrel_vectordb_bm25_disk_file:encode_posting_block(Chunk),
                    PaddedBlock = barrel_vectordb_bm25_disk_file:pad_to_sector(EncodedBlock),
                    BlockSize2 = byte_size(PaddedBlock),

                    BlockEntry = #{
                        max_impact => MaxImpact,
                        doc_start => DocStart,
                        doc_end => DocEnd,
                        offset => OffsetAcc,
                        size => BlockSize2
                    },

                    {[BlockEntry | BlockAcc],
                     [{OffsetAcc, PaddedBlock} | PostingAcc],
                     OffsetAcc + BlockSize2}
                end,
                {[], [], AccOffset},
                Chunks
            ),

            {AccIndex#{TermIntId => lists:reverse(TermBlocks)},
             TermPostingBlocks ++ AccBlocks,
             NewOffset}
        end,
        {#{}, [], 4096},  %% Start after header sector
        SortedPostings
    ),

    {BlockMaxIndex, PostingBlocks}.

chunk_postings(Postings, BlockSize) ->
    chunk_postings(Postings, BlockSize, []).

chunk_postings([], _BlockSize, Acc) ->
    lists:reverse(Acc);
chunk_postings(Postings, BlockSize, Acc) ->
    {Chunk, Rest} = lists:split(min(BlockSize, length(Postings)), Postings),
    chunk_postings(Rest, BlockSize, [Chunk | Acc]).

compute_max_impact(Postings, IDF, K1, B, _AvgDL) ->
    lists:foldl(
        fun({_DocIntId, TF}, MaxAcc) ->
            %% Use worst-case doc length (0) for max impact
            %% This gives an upper bound on the score
            Numerator = TF * (K1 + 1),
            Denominator = TF + K1 * (1 - B),  %% Assuming DocLength = 0
            Impact = IDF * Numerator / Denominator,
            max(MaxAcc, Impact)
        end,
        0.0,
        Postings
    ).

write_postings_to_disk(#bm25_disk_index{file_handle = FileHandle}, PostingBlocks) ->
    lists:foreach(
        fun({Offset, Block}) ->
            ok = barrel_vectordb_bm25_disk_file:write_block(FileHandle, Offset, Block)
        end,
        PostingBlocks
    ),
    {ok, FileHandle}.

%%====================================================================
%% Internal Functions - Utilities
%%====================================================================

to_binary(Path) when is_binary(Path) -> Path;
to_binary(Path) when is_list(Path) -> list_to_binary(Path).
