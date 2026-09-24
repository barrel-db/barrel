%%%-------------------------------------------------------------------
%%% @doc Disk-Native BM25 Backend
%%%
%%% Durable state lives in the `bm25.ids' RocksDB, written per document
%%% in one batch: id mappings, the forward index (doc to term counts),
%%% document frequencies, global stats and the set of documents changed
%%% since the last compaction. The flat files (postings + block-max
%%% index) are a derived segment, rebuilt from the forward index by
%%% compaction and ignored when a compaction did not finish.
%%%
%%% Search reads the segment (Block-Max pruning) plus a hot layer of
%%% documents changed since the last compaction; segment postings of
%%% changed or removed documents are masked.
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
    open/2,
    close/1,
    sync/1,
    %% Rebuild of a store that predates the durable format
    rebuild_required/1,
    reset/1,
    mark_rebuilt/1,
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

%% Stored block bounds are float32: widen them so pruning stays exact.
-define(BOUND_MARGIN, 1.001).

%% RocksDB column family names
-define(CF_TERMS_FWD, "terms_fwd").  %% term string -> term int ID
-define(CF_TERMS_REV, "terms_rev").  %% term int ID -> term string
-define(CF_DOCS_FWD, "docs_fwd").    %% doc string ID -> doc int ID
-define(CF_DOCS_REV, "docs_rev").    %% doc int ID -> doc string ID
-define(CF_DOC_TERMS, "doc_terms").  %% doc int ID -> <<Len:32, varint term/tf pairs>>
-define(CF_TERM_DF, "term_df").      %% term int ID -> <<DF:64>>
-define(CF_PENDING, "pending").      %% doc int ID -> <<>>, changed since compaction

%% Keys in the default column family
-define(KEY_FORMAT, <<"format">>).    %% present on stores with the durable format
-define(KEY_STATS, <<"stats">>).      %% <<TotalDocs:64, TotalTokens:64>>
-define(KEY_SEGMENT, <<"segment">>).  %% present when the flat files are complete
-define(KEY_REBUILD, <<"rebuild">>).  %% present until a rebuild from source ends
-define(FORMAT, <<2:8>>).

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

    %% Global stats (persisted under ?KEY_STATS)
    total_docs = 0 :: non_neg_integer(),
    total_tokens = 0 :: non_neg_integer(),

    %% ID mapping counters
    next_term_int_id = 0 :: non_neg_integer(),
    next_doc_int_id = 0 :: non_neg_integer(),

    %% RocksDB handles
    id_db :: rocksdb:db_handle() | undefined,
    cf_default :: rocksdb:cf_handle() | undefined,
    cf_terms_fwd :: rocksdb:cf_handle() | undefined,
    cf_terms_rev :: rocksdb:cf_handle() | undefined,
    cf_docs_fwd :: rocksdb:cf_handle() | undefined,
    cf_docs_rev :: rocksdb:cf_handle() | undefined,
    cf_doc_terms :: rocksdb:cf_handle() | undefined,
    cf_term_df :: rocksdb:cf_handle() | undefined,
    cf_pending :: rocksdb:cf_handle() | undefined,
    id_db_standalone = false :: boolean(),
    %% EncryptedEnv used by the ids RocksDB; kept referenced here (the
    %% NIF frees the env when the handle is garbage collected)
    id_db_env :: term() | undefined,

    %% File I/O
    file_handle :: term() | undefined,

    %% Cache of doc lengths for segment scoring: {DocIntId, Length}
    doc_stats_table :: ets:tid() | undefined,

    %% Hot layer: live documents changed since the last compaction
    hot_enabled = true :: boolean(),
    hot_max_size = ?DEFAULT_HOT_MAX_SIZE :: non_neg_integer(),
    hot_compaction_threshold = ?DEFAULT_HOT_COMPACTION_THRESHOLD :: float(),
    %% hot_postings: #{TermIntId => [{DocIntId, TF}]}
    hot_postings = #{} :: #{non_neg_integer() => [{non_neg_integer(), pos_integer()}]},
    %% hot_docs: #{DocIntId => #{TermIntId => TF}}
    hot_docs = #{} :: #{non_neg_integer() => #{non_neg_integer() => pos_integer()}},
    %% hot_doc_lengths: #{DocIntId => Length}
    hot_doc_lengths = #{} :: #{non_neg_integer() => non_neg_integer()},
    hot_size = 0 :: non_neg_integer(),
    hot_tokens = 0 :: non_neg_integer(),

    %% Docs changed or removed since the last compaction: their segment
    %% postings are stale (mirrors the pending column family)
    masked = #{} :: #{non_neg_integer() => true},

    %% Segment stats (from the last compaction)
    disk_doc_count = 0 :: non_neg_integer(),
    disk_term_count = 0 :: non_neg_integer(),
    disk_total_tokens = 0 :: non_neg_integer(),

    %% Block-max index of the segment, kept in memory for search
    blockmax_index = #{} :: #{non_neg_integer() => [map()]},
    segment_valid = false :: boolean(),

    %% Set on a store that predates the durable format
    rebuild_required = false :: boolean(),

    %% Opened read only: nothing on disk is created or rewritten
    read_only = false :: boolean(),

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
    case maps:get(base_path, Options, undefined) of
        undefined ->
            {error, base_path_required};
        BasePath ->
            Config = #bm25_disk_config{
                k1 = maps:get(k1, Options, ?DEFAULT_K1),
                b = maps:get(b, Options, ?DEFAULT_B),
                min_term_length = maps:get(min_term_length, Options, 1),
                lowercase = maps:get(lowercase, Options, true),
                block_size = maps:get(block_size, Options, ?DEFAULT_BLOCK_SIZE)
            },
            create_index(to_binary(BasePath), Config, Options)
    end.

%% @doc Open an existing disk-native BM25 index
-spec open(binary() | string()) -> {ok, bm25_disk_index()} | {error, term()}.
open(Path) ->
    open(Path, #{}).

%% @doc Open with options: `crypto => none | #{key := <<_:256>>,
%% env => rocksdb env}' (the key encrypts the flat files, the env is the
%% EncryptedEnv for the bm25.ids RocksDB), plus the tokenizer and hot
%% layer options of {@link new/1}. A store that predates the durable
%% format opens empty with {@link rebuild_required/1} true.
-spec open(binary() | string(), map()) ->
    {ok, bm25_disk_index()} | {error, term()}.
open(Path, Opts) ->
    BasePathBin = to_binary(Path),
    Crypto = maps:get(crypto, Opts, none),
    ReadOnly = maps:get(read_only, Opts, false),
    case barrel_vectordb_bm25_disk_file:open(BasePathBin,
                                             #{crypto => file_crypto(Crypto),
                                               read_only => ReadOnly}) of
        {ok, FileHandle} ->
            Header = barrel_vectordb_bm25_disk_file:read_header(FileHandle),
            Config = #bm25_disk_config{
                k1 = maps:get(k1, Header, ?DEFAULT_K1),
                b = maps:get(b, Header, ?DEFAULT_B),
                min_term_length = maps:get(min_term_length, Opts, 1),
                lowercase = maps:get(lowercase, Opts, true),
                block_size = maps:get(block_size, Header, ?DEFAULT_BLOCK_SIZE)
            },
            case open_id_db(BasePathBin, Crypto, ReadOnly) of
                {ok, Handles} ->
                    Index1 = init_index(Handles, BasePathBin, Config,
                                        FileHandle, Opts),
                    try load_index(Index1) of
                        {ok, _} = Ok ->
                            Ok;
                        {error, _} = LoadError ->
                            _ = close(Index1),
                            LoadError
                    catch
                        Class:Reason:St ->
                            _ = close(Index1),
                            erlang:raise(Class, Reason, St)
                    end;
                {error, _} = Error ->
                    barrel_vectordb_bm25_disk_file:close(FileHandle),
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

%% @doc Close the index. Every acknowledged write is already in the ids
%% RocksDB; the hot layer is reloaded from it at the next open.
-spec close(bm25_disk_index()) -> ok.
close(#bm25_disk_index{file_handle = FileHandle, id_db = IdDb,
                        id_db_standalone = Standalone,
                        doc_stats_table = DocStats}) ->
    case FileHandle of
        undefined -> ok;
        _ -> barrel_vectordb_bm25_disk_file:close(FileHandle)
    end,
    _ = case Standalone andalso IdDb =/= undefined of
        true -> rocksdb:close(IdDb);
        false -> ok
    end,
    catch ets:delete(DocStats),
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

%% @doc True when the store predates the durable format (or a rebuild
%% did not finish): the caller re-adds every document after {@link
%% reset/1}, then calls {@link mark_rebuilt/1}.
-spec rebuild_required(bm25_disk_index()) -> boolean().
rebuild_required(#bm25_disk_index{rebuild_required = R}) -> R.

%% @doc Drop every indexed document (id mappings are kept).
-spec reset(bm25_disk_index()) -> {ok, bm25_disk_index()}.
reset(#bm25_disk_index{id_db = Db, cf_default = CfD, cf_doc_terms = CfDT,
                       cf_term_df = CfDF, cf_pending = CfP,
                       doc_stats_table = DocStats} = Index) ->
    ok = rocksdb:delete(Db, CfD, ?KEY_SEGMENT, [{sync, true}]),
    lists:foreach(fun(Cf) -> ok = clear_cf(Db, Cf) end, [CfDT, CfDF, CfP]),
    ok = rocksdb:put(Db, CfD, ?KEY_STATS, encode_stats(0, 0), [{sync, true}]),
    true = ets:delete_all_objects(DocStats),
    {ok, Index#bm25_disk_index{
           total_docs = 0, total_tokens = 0,
           hot_postings = #{}, hot_docs = #{}, hot_doc_lengths = #{},
           hot_size = 0, hot_tokens = 0, masked = #{},
           blockmax_index = #{}, segment_valid = false,
           disk_doc_count = 0, disk_term_count = 0, disk_total_tokens = 0}}.

%% @doc Compact and clear the rebuild marker.
-spec mark_rebuilt(bm25_disk_index()) -> {ok, bm25_disk_index()} | {error, term()}.
mark_rebuilt(#bm25_disk_index{id_db = Db, cf_default = CfD} = Index) ->
    case compact(Index) of
        {ok, Index1} ->
            ok = rocksdb:delete(Db, CfD, ?KEY_REBUILD, [{sync, true}]),
            {ok, Index1#bm25_disk_index{rebuild_required = false}};
        {error, _} = Error ->
            Error
    end.

%% @doc Add (or replace) a document
-spec add(bm25_disk_index(), binary(), binary()) -> {ok, bm25_disk_index()} | {error, term()}.
add(#bm25_disk_index{config = Config} = Index, DocId, Text) ->
    {DocIntId, Index1} = get_or_create_doc_int_id(Index, DocId),
    Terms = tokenize(Text, Config),
    TermCounts = count_terms(Terms),
    DocLength = length(Terms),
    {TermIntIds, Index2} = get_or_create_term_int_ids(Index1, maps:keys(TermCounts)),
    NewCounts = maps:fold(
        fun(Term, Count, Acc) -> Acc#{maps:get(Term, TermIntIds) => Count} end,
        #{}, TermCounts),
    {DocsDelta, OldLength, OldCounts} = case read_doc_terms(Index2, DocIntId) of
        {ok, OldLen, Old} -> {0, OldLen, Old};
        not_found -> {1, 0, #{}}
    end,
    TotalDocs = Index2#bm25_disk_index.total_docs + DocsDelta,
    TotalTokens = Index2#bm25_disk_index.total_tokens - OldLength + DocLength,
    DfDelta = df_delta(OldCounts, NewCounts),
    ok = commit_doc(Index2, DocIntId, {put, DocLength, NewCounts}, DfDelta,
                    TotalDocs, TotalTokens),
    true = ets:insert(Index2#bm25_disk_index.doc_stats_table, {DocIntId, DocLength}),
    Index3 = hot_put(hot_drop(Index2, DocIntId), DocIntId, DocLength, NewCounts),
    Index4 = Index3#bm25_disk_index{
        total_docs = TotalDocs,
        total_tokens = TotalTokens,
        masked = (Index3#bm25_disk_index.masked)#{DocIntId => true}
    },
    maybe_compact(Index4).

%% @doc Remove a document from the index
-spec remove(bm25_disk_index(), binary()) -> {ok, bm25_disk_index()} | {error, not_found}.
remove(Index, DocId) ->
    case get_doc_int_id(Index, DocId) of
        {ok, DocIntId} ->
            case read_doc_terms(Index, DocIntId) of
                {ok, OldLength, OldCounts} ->
                    TotalDocs = Index#bm25_disk_index.total_docs - 1,
                    TotalTokens = Index#bm25_disk_index.total_tokens - OldLength,
                    ok = commit_doc(Index, DocIntId, delete,
                                    df_delta(OldCounts, #{}),
                                    TotalDocs, TotalTokens),
                    true = ets:delete(Index#bm25_disk_index.doc_stats_table, DocIntId),
                    Index1 = hot_drop(Index, DocIntId),
                    {ok, Index1#bm25_disk_index{
                           total_docs = TotalDocs,
                           total_tokens = TotalTokens,
                           masked = (Index1#bm25_disk_index.masked)#{DocIntId => true}}};
                not_found ->
                    {error, not_found}
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
search_with_metrics(#bm25_disk_index{total_docs = 0}, _Query, _K) ->
    {[], #{blocks_total => 0, blocks_scanned => 0, blocks_skipped => 0}};
search_with_metrics(#bm25_disk_index{config = Config, total_docs = N,
                                     total_tokens = TotalTokens} = Index,
                    Query, K) ->
    QueryTerms = maps:keys(count_terms(tokenize(Query, Config))),
    {TermIntIds, _} = get_term_int_ids(Index, QueryTerms),
    AvgDL = TotalTokens / N,
    Terms = lists:filtermap(
        fun(TermIntId) ->
            case get_term_doc_freq(Index, TermIntId) of
                0 -> false;
                DF -> {true, {TermIntId, idf(N, DF)}}
            end
        end,
        lists:usort(maps:values(TermIntIds))),
    HotScores = search_hot_layer(Index, Terms, AvgDL),
    {Scores, DiskMetrics} = search_disk_layer(Index, Terms, AvgDL, K, HotScores),
    TopK = top_k(Scores, K),
    Results = [{get_doc_string_id(Index, DocIntId), Score} || {DocIntId, Score} <- TopK],
    Metrics = DiskMetrics#{
        query_terms => maps:size(TermIntIds),
        hot_results => maps:size(HotScores),
        disk_results => maps:size(Scores) - maps:size(HotScores),
        total_results => length(TopK)
    },
    {Results, Metrics}.

%% @doc Get sparse vector representation of a document
-spec get_vector(bm25_disk_index(), binary()) -> {ok, sparse_vector()} | {error, not_found}.
get_vector(#bm25_disk_index{config = Config} = Index, DocId) ->
    case get_doc_int_id(Index, DocId) of
        {ok, DocIntId} ->
            case read_doc_terms(Index, DocIntId) of
                {ok, DocLength, TermIntCounts} ->
                    {ok, compute_doc_vector(Index, TermIntCounts, DocLength,
                                            avgdl(Index, 1), Config)};
                not_found ->
                    {error, not_found}
            end;
        {error, not_found} ->
            {error, not_found}
    end.

%% @doc Encode text into sparse vector without adding to index
-spec encode(bm25_disk_index(), binary()) -> sparse_vector().
encode(#bm25_disk_index{config = Config} = Index, Text) ->
    Terms = tokenize(Text, Config),
    TermCounts = count_terms(Terms),
    DocLength = length(Terms),
    {TermIntIds, _} = get_term_int_ids(Index, maps:keys(TermCounts)),
    %% Known terms only
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
    compute_doc_vector(Index, TermIntCounts, DocLength,
                       avgdl(Index, DocLength), Config).

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
    next_term_int_id = NextTermId,
    rebuild_required = RebuildRequired
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
        rebuild_required => RebuildRequired,
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

%% @doc Rewrite the segment from the forward index (every live document)
%% and clear the hot layer. The segment marker is dropped before the
%% files are touched and restored after they are synced, so an
%% interrupted compaction is redone at the next open.
-spec compact(bm25_disk_index()) -> {ok, bm25_disk_index()} | {error, term()}.
compact(#bm25_disk_index{segment_valid = true, masked = Masked} = Index)
  when map_size(Masked) =:= 0 ->
    {ok, Index};
compact(Index) ->
    try
        {ok, do_compact(Index)}
    catch
        _:Reason ->
            {error, {compaction_failed, Reason}}
    end.

%%====================================================================
%% Internal Functions - Index Creation and Loading
%%====================================================================

create_index(BasePathBin, Config, Options) ->
    ok = filelib:ensure_dir(filename:join(BasePathBin, "dummy")),
    Crypto = maps:get(crypto, Options, none),
    FileConfig = #{
        k1 => Config#bm25_disk_config.k1,
        b => Config#bm25_disk_config.b,
        block_size => Config#bm25_disk_config.block_size,
        crypto => file_crypto(Crypto)
    },
    case barrel_vectordb_bm25_disk_file:create(BasePathBin, FileConfig) of
        {ok, FileHandle} ->
            case open_id_db(BasePathBin, Crypto, false) of
                {ok, Handles} ->
                    Index1 = init_index(Handles, BasePathBin, Config,
                                        FileHandle, Options),
                    #bm25_disk_index{id_db = Db, cf_default = CfD} = Index1,
                    ok = rocksdb:put(Db, CfD, ?KEY_FORMAT, ?FORMAT, [{sync, true}]),
                    {ok, Index2} = reset(Index1),
                    %% An empty segment is complete
                    compact(Index2);
                {error, _} = Error ->
                    barrel_vectordb_bm25_disk_file:close(FileHandle),
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

init_index(#{db := Db, cfs := [CfD, CfTermsFwd, CfTermsRev, CfDocsFwd,
                                CfDocsRev, CfDocTerms, CfTermDf, CfPending],
             env := IdEnv},
           BasePathBin, Config, FileHandle, Opts) ->
    #bm25_disk_index{
        config = Config,
        base_path = BasePathBin,
        id_db = Db,
        cf_default = CfD,
        cf_terms_fwd = CfTermsFwd,
        cf_terms_rev = CfTermsRev,
        cf_docs_fwd = CfDocsFwd,
        cf_docs_rev = CfDocsRev,
        cf_doc_terms = CfDocTerms,
        cf_term_df = CfTermDf,
        cf_pending = CfPending,
        id_db_standalone = true,
        id_db_env = IdEnv,
        read_only = maps:get(read_only, Opts, false),
        file_handle = FileHandle,
        doc_stats_table = ets:new(bm25_doc_stats, [set, public]),
        next_term_int_id = get_next_id(Db, CfTermsRev),
        next_doc_int_id = get_next_id(Db, CfDocsRev),
        hot_max_size = maps:get(hot_max_size, Opts, ?DEFAULT_HOT_MAX_SIZE),
        hot_compaction_threshold = maps:get(hot_compaction_threshold, Opts,
                                            ?DEFAULT_HOT_COMPACTION_THRESHOLD)
    }.

%% Durable format: stats, pending docs into the hot layer, segment if
%% complete (else rebuilt now). No format marker: a store written before
%% the forward index existed, its segment cannot be trusted.
load_index(#bm25_disk_index{id_db = Db, cf_default = CfD,
                             read_only = ReadOnly} = Index) ->
    case {rocksdb:get(Db, CfD, ?KEY_FORMAT, []), ReadOnly} of
        {{ok, ?FORMAT}, _} ->
            load_durable(Index);
        {not_found, true} ->
            {error, {read_only_upgrade_needed, bm25_format}};
        {not_found, false} ->
            ok = rocksdb:put(Db, CfD, ?KEY_REBUILD, <<>>, [{sync, true}]),
            ok = rocksdb:put(Db, CfD, ?KEY_FORMAT, ?FORMAT, [{sync, true}]),
            {ok, Index1} = reset(Index),
            {ok, Index1#bm25_disk_index{rebuild_required = true}}
    end.

load_durable(#bm25_disk_index{id_db = Db, cf_default = CfD} = Index) ->
    {TotalDocs, TotalTokens} = case rocksdb:get(Db, CfD, ?KEY_STATS, []) of
        {ok, StatsBin} -> decode_stats(StatsBin);
        not_found -> {0, 0}
    end,
    Rebuild = rocksdb:get(Db, CfD, ?KEY_REBUILD, []) =/= not_found,
    Index1 = Index#bm25_disk_index{total_docs = TotalDocs,
                                   total_tokens = TotalTokens,
                                   rebuild_required = Rebuild},
    case rocksdb:get(Db, CfD, ?KEY_SEGMENT, []) of
        {ok, _} ->
            {ok, BlockmaxIndex} = barrel_vectordb_bm25_disk_file:read_blockmax_index(
                                    Index1#bm25_disk_index.file_handle),
            Header = barrel_vectordb_bm25_disk_file:read_header(
                       Index1#bm25_disk_index.file_handle),
            {ok, load_pending(Index1#bm25_disk_index{
                   blockmax_index = BlockmaxIndex,
                   segment_valid = true,
                   disk_doc_count = maps:get(doc_count, Header, 0),
                   disk_term_count = maps:get(term_count, Header, 0),
                   disk_total_tokens = maps:get(total_tokens, Header, 0)})};
        not_found when Index1#bm25_disk_index.read_only ->
            {error, {read_only_upgrade_needed, bm25_segment}};
        not_found ->
            %% Interrupted compaction: every live doc must be rewritten
            compact(load_pending(Index1))
    end.

%% Replay the documents changed since the last compaction.
load_pending(#bm25_disk_index{id_db = Db, cf_pending = CfP} = Index) ->
    fold_cf(Db, CfP,
            fun(<<DocIntId:64/big>>, _, Acc) ->
                    Acc1 = Acc#bm25_disk_index{
                             masked = (Acc#bm25_disk_index.masked)#{DocIntId => true}},
                    case read_doc_terms(Acc1, DocIntId) of
                        {ok, Len, Counts} -> hot_put(Acc1, DocIntId, Len, Counts);
                        not_found -> Acc1
                    end
            end,
            Index).

%% `none | #{key := <<_:256>>, env => Env}': the key encrypts the flat
%% files, the EncryptedEnv covers the bm25.ids RocksDB (terms and doc
%% ids are plaintext content otherwise). A standalone caller passing
%% only the key gets an env minted here.
file_crypto(none) -> none;
file_crypto(#{key := Key}) -> #{key => Key}.

id_db_env(none) -> undefined;
id_db_env(#{env := Env}) -> Env;
id_db_env(#{key := Key}) ->
    {ok, Env} = rocksdb:new_env({encrypted, Key}),
    Env.

%% Column families missing on an older store are created at a writable
%% open; a read-only open reports them.
open_id_db(BasePathBin, Crypto, ReadOnly) ->
    DbPath = filename:join(BasePathBin, "bm25.ids"),
    CfNames = ["default", ?CF_TERMS_FWD, ?CF_TERMS_REV, ?CF_DOCS_FWD,
               ?CF_DOCS_REV, ?CF_DOC_TERMS, ?CF_TERM_DF, ?CF_PENDING],
    CfDescriptors = [{Name, [{create_if_missing, true}]} || Name <- CfNames],
    DbOpts0 = [{create_if_missing, true}, {create_missing_column_families, true}],
    IdEnv = id_db_env(Crypto),
    DbOpts = case IdEnv of
        undefined -> DbOpts0;
        _ -> [{env, IdEnv} | DbOpts0]
    end,
    case open_id_rocksdb(ReadOnly, binary_to_list(DbPath), DbOpts,
                         CfDescriptors) of
        {ok, Db, Cfs} ->
            {ok, #{db => Db, cfs => Cfs, env => IdEnv}};
        {error, _} = Error ->
            Error
    end.

open_id_rocksdb(true, Path, DbOpts, CFs) ->
    barrel_vectordb_ro:open(Path, DbOpts, CFs);
open_id_rocksdb(false, Path, DbOpts, CFs) ->
    rocksdb:open(Path, DbOpts, CFs).

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

fold_cf(Db, Cf, Fun, Acc0) ->
    {ok, Iter} = rocksdb:iterator(Db, Cf, []),
    try
        fold_iter(Iter, rocksdb:iterator_move(Iter, first), Fun, Acc0)
    after
        rocksdb:iterator_close(Iter)
    end.

fold_iter(Iter, {ok, Key, Value}, Fun, Acc) ->
    fold_iter(Iter, rocksdb:iterator_move(Iter, next), Fun, Fun(Key, Value, Acc));
fold_iter(_Iter, {error, _}, _Fun, Acc) ->
    Acc.

clear_cf(Db, Cf) ->
    Keys = fold_cf(Db, Cf, fun(Key, _, Acc) -> [Key | Acc] end, []),
    {ok, Batch} = rocksdb:batch(),
    try
        lists:foreach(fun(Key) -> ok = rocksdb:batch_delete(Batch, Cf, Key) end, Keys),
        rocksdb:write_batch(Db, Batch, [{sync, true}])
    after
        rocksdb:release_batch(Batch)
    end.

%%====================================================================
%% Internal Functions - Durable Document State
%%====================================================================

encode_stats(TotalDocs, TotalTokens) ->
    <<TotalDocs:64/big, TotalTokens:64/big>>.

decode_stats(<<TotalDocs:64/big, TotalTokens:64/big>>) ->
    {TotalDocs, TotalTokens}.

encode_doc_terms(Length, Counts) ->
    Pairs = lists:append([[T, TF] || {T, TF} <- lists:sort(maps:to_list(Counts))]),
    <<Length:32/big,
      (barrel_vectordb_bm25_disk_file:varint_encode_list(Pairs))/binary>>.

decode_doc_terms(<<Length:32/big, PairsBin/binary>>) ->
    {Length, decode_pairs(PairsBin, #{})}.

decode_pairs(<<>>, Acc) ->
    Acc;
decode_pairs(Bin, Acc) ->
    {T, Rest1} = barrel_vectordb_bm25_disk_file:varint_decode(Bin),
    {TF, Rest2} = barrel_vectordb_bm25_disk_file:varint_decode(Rest1),
    decode_pairs(Rest2, Acc#{T => TF}).

read_doc_terms(#bm25_disk_index{id_db = Db, cf_doc_terms = Cf}, DocIntId) ->
    case rocksdb:get(Db, Cf, <<DocIntId:64/big>>, []) of
        {ok, Bin} ->
            {Length, Counts} = decode_doc_terms(Bin),
            {ok, Length, Counts};
        not_found ->
            not_found
    end.

%% Per-term document frequency change between two versions of a doc.
df_delta(OldCounts, NewCounts) ->
    D0 = maps:fold(fun(T, _, Acc) -> Acc#{T => -1} end, #{}, OldCounts),
    D1 = maps:fold(fun(T, _, Acc) -> maps:update_with(T, fun(V) -> V + 1 end, 1, Acc) end,
                   D0, NewCounts),
    maps:filter(fun(_, V) -> V =/= 0 end, D1).

%% One atomic batch per document: forward index, document frequencies,
%% stats and the pending marker.
commit_doc(#bm25_disk_index{id_db = Db, cf_default = CfD, cf_doc_terms = CfDT,
                            cf_term_df = CfDF, cf_pending = CfP} = Index,
           DocIntId, Op, DfDelta, TotalDocs, TotalTokens) ->
    Key = <<DocIntId:64/big>>,
    {ok, Batch} = rocksdb:batch(),
    try
        ok = batch_doc(Batch, CfDT, Key, Op),
        maps:foreach(
          fun(TermIntId, Delta) ->
                  TKey = <<TermIntId:64/big>>,
                  ok = batch_df(Batch, CfDF, TKey,
                                get_term_doc_freq(Index, TermIntId) + Delta)
          end, DfDelta),
        ok = rocksdb:batch_put(Batch, CfP, Key, <<>>),
        ok = rocksdb:batch_put(Batch, CfD, ?KEY_STATS,
                               encode_stats(TotalDocs, TotalTokens)),
        rocksdb:write_batch(Db, Batch, [])
    after
        rocksdb:release_batch(Batch)
    end.

batch_doc(Batch, Cf, Key, {put, Length, Counts}) ->
    rocksdb:batch_put(Batch, Cf, Key, encode_doc_terms(Length, Counts));
batch_doc(Batch, Cf, Key, delete) ->
    rocksdb:batch_delete(Batch, Cf, Key).

batch_df(Batch, Cf, TKey, DF) when DF =< 0 ->
    rocksdb:batch_delete(Batch, Cf, TKey);
batch_df(Batch, Cf, TKey, DF) ->
    rocksdb:batch_put(Batch, Cf, TKey, <<DF:64/big>>).

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

hot_put(#bm25_disk_index{hot_postings = HotPostings, hot_docs = HotDocs,
                         hot_doc_lengths = HotLengths,
                         hot_tokens = HotTokens} = Index,
        DocIntId, DocLength, Counts) ->
    NewPostings = maps:fold(
        fun(TermIntId, TF, Acc) ->
            Acc#{TermIntId => [{DocIntId, TF} | maps:get(TermIntId, Acc, [])]}
        end,
        HotPostings, Counts),
    NewDocs = HotDocs#{DocIntId => Counts},
    Index#bm25_disk_index{
        hot_postings = NewPostings,
        hot_docs = NewDocs,
        hot_doc_lengths = HotLengths#{DocIntId => DocLength},
        hot_size = maps:size(NewDocs),
        hot_tokens = HotTokens + DocLength
    }.

hot_drop(#bm25_disk_index{hot_docs = HotDocs} = Index, DocIntId) ->
    case maps:find(DocIntId, HotDocs) of
        {ok, Counts} -> hot_drop(Index, DocIntId, Counts);
        error -> Index
    end.

hot_drop(#bm25_disk_index{hot_postings = HotPostings, hot_docs = HotDocs,
                          hot_doc_lengths = HotLengths,
                          hot_tokens = HotTokens} = Index,
         DocIntId, Counts) ->
    NewPostings = maps:fold(
        fun(TermIntId, _TF, Acc) ->
            case [P || {D, _} = P <- maps:get(TermIntId, Acc, []), D =/= DocIntId] of
                [] -> maps:remove(TermIntId, Acc);
                Filtered -> Acc#{TermIntId => Filtered}
            end
        end,
        HotPostings, Counts),
    NewDocs = maps:remove(DocIntId, HotDocs),
    Index#bm25_disk_index{
        hot_postings = NewPostings,
        hot_docs = NewDocs,
        hot_doc_lengths = maps:remove(DocIntId, HotLengths),
        hot_size = maps:size(NewDocs),
        hot_tokens = HotTokens - maps:get(DocIntId, HotLengths, 0)
    }.

maybe_compact(#bm25_disk_index{hot_size = HotSize, hot_max_size = Max,
                               hot_compaction_threshold = Threshold} = Index)
  when HotSize >= Max * Threshold ->
    compact(Index);
maybe_compact(Index) ->
    {ok, Index}.

%%====================================================================
%% Internal Functions - Search
%%====================================================================

idf(N, DF) ->
    math:log((N - DF + 0.5) / (DF + 0.5) + 1).

avgdl(#bm25_disk_index{total_docs = 0}, Default) -> Default;
avgdl(#bm25_disk_index{total_docs = N, total_tokens = T}, _Default) -> T / N.

term_score(TF, IDF, DocLength, AvgDL, K1, B) ->
    IDF * TF * (K1 + 1) / (TF + K1 * (1 - B + B * DocLength / max(AvgDL, 1))).

search_hot_layer(#bm25_disk_index{config = #bm25_disk_config{k1 = K1, b = B},
                                  hot_postings = HotPostings,
                                  hot_doc_lengths = HotLengths},
                 Terms, AvgDL) ->
    lists:foldl(
        fun({TermIntId, IDF}, Acc) ->
            lists:foldl(
                fun({DocIntId, TF}, Acc1) ->
                    S = term_score(TF, IDF, maps:get(DocIntId, HotLengths, 0),
                                   AvgDL, K1, B),
                    maps:update_with(DocIntId, fun(V) -> V + S end, S, Acc1)
                end,
                Acc, maps:get(TermIntId, HotPostings, []))
        end,
        #{}, Terms).

%% Block-Max pruning over the segment. A block is skipped only when
%% its bound plus the best other terms cannot reach the current k-th
%% partial score (a lower bound of the final one), so top-k is exact.
search_disk_layer(#bm25_disk_index{blockmax_index = BlockMax} = Index,
                  Terms, AvgDL, K, Scores0) ->
    TermBlocks0 = [{TermIntId, IDF, Blocks, IDF * max_bound(Blocks)}
                   || {TermIntId, IDF} <- Terms,
                      Blocks <- [maps:get(TermIntId, BlockMax, [])],
                      Blocks =/= []],
    TermBlocks = lists:sort(fun({_, _, _, M1}, {_, _, _, M2}) -> M1 >= M2 end,
                            TermBlocks0),
    TotalBlocks = lists:sum([length(Blocks) || {_, _, Blocks, _} <- TermBlocks]),
    {Scores, Scanned, Skipped} = lists:foldl(
        fun({TermIntId, IDF, Blocks, _}, {Acc, Sc, Sk}) ->
            Rest = lists:sum([M || {T, _, _, M} <- TermBlocks, T =/= TermIntId]),
            Threshold = kth_score(Acc, K),
            process_blocks(Blocks, IDF, Rest, Threshold, AvgDL, Index, Acc, Sc, Sk)
        end,
        {Scores0, 0, 0}, TermBlocks),
    {Scores, #{blocks_total => TotalBlocks,
               blocks_scanned => Scanned,
               blocks_skipped => Skipped,
               skip_rate => case TotalBlocks of
                                0 -> 0.0;
                                _ -> Skipped / TotalBlocks * 100
                            end}}.

max_bound(Blocks) ->
    lists:max([maps:get(max_impact, Blk) || Blk <- Blocks]) * ?BOUND_MARGIN.

kth_score(Scores, K) when map_size(Scores) < K ->
    0.0;
kth_score(Scores, K) ->
    lists:nth(K, lists:sort(fun(A, B) -> A >= B end, maps:values(Scores))).

process_blocks([], _IDF, _Rest, _Threshold, _AvgDL, _Index, Acc, Sc, Sk) ->
    {Acc, Sc, Sk};
process_blocks([#{max_impact := Bound, offset := Offset, size := Size} | Blocks],
               IDF, Rest, Threshold, AvgDL, Index, Acc, Sc, Sk) ->
    case IDF * Bound * ?BOUND_MARGIN + Rest < Threshold of
        true ->
            process_blocks(Blocks, IDF, Rest, Threshold, AvgDL, Index, Acc, Sc, Sk + 1);
        false ->
            {ok, Postings} = barrel_vectordb_bm25_disk_file:read_postings(
                               Index#bm25_disk_index.file_handle, Offset, Size),
            Acc1 = score_postings(Postings, IDF, AvgDL, Index, Acc),
            process_blocks(Blocks, IDF, Rest, Threshold, AvgDL, Index, Acc1, Sc + 1, Sk)
    end.

score_postings(Postings, IDF, AvgDL,
               #bm25_disk_index{config = #bm25_disk_config{k1 = K1, b = B},
                                masked = Masked} = Index,
               Acc0) ->
    lists:foldl(
        fun({DocIntId, _TF}, Acc) when is_map_key(DocIntId, Masked) ->
                Acc;
           ({DocIntId, TF}, Acc) ->
                S = term_score(TF, IDF, get_doc_length(Index, DocIntId), AvgDL, K1, B),
                maps:update_with(DocIntId, fun(V) -> V + S end, S, Acc)
        end,
        Acc0, Postings).

top_k(Scores, K) ->
    Sorted = lists:sort(fun({D1, S1}, {D2, S2}) -> {S1, D2} >= {S2, D1} end,
                        maps:to_list(Scores)),
    lists:sublist(Sorted, K).

get_term_doc_freq(#bm25_disk_index{id_db = Db, cf_term_df = Cf}, TermIntId) ->
    case rocksdb:get(Db, Cf, <<TermIntId:64/big>>, []) of
        {ok, <<DF:64/big>>} -> DF;
        not_found -> 0
    end.

get_doc_length(#bm25_disk_index{doc_stats_table = DocStats} = Index, DocIntId) ->
    case ets:lookup(DocStats, DocIntId) of
        [{_, Length}] ->
            Length;
        [] ->
            Length = case read_doc_terms(Index, DocIntId) of
                {ok, L, _} -> L;
                not_found -> 0
            end,
            true = ets:insert(DocStats, {DocIntId, Length}),
            Length
    end.

%%====================================================================
%% Internal Functions - Vector Computation
%%====================================================================

compute_doc_vector(#bm25_disk_index{total_docs = N} = Index, TermIntCounts,
                   DocLength, AvgDL, #bm25_disk_config{k1 = K1, b = B}) ->
    maps:fold(
        fun(TermIntId, TF, Acc) ->
            DF = get_term_doc_freq(Index, TermIntId),
            Weight = term_score(TF, idf(N, DF), DocLength, AvgDL, K1, B),
            case Weight > 0 of
                true -> Acc#{get_term_string_id(Index, TermIntId) => Weight};
                false -> Acc
            end
        end,
        #{},
        TermIntCounts
    ).

%%====================================================================
%% Internal Functions - Compaction
%%====================================================================

do_compact(#bm25_disk_index{id_db = Db, cf_default = CfD, cf_doc_terms = CfDT,
                            cf_pending = CfP, doc_stats_table = DocStats,
                            config = Config, masked = Masked,
                            total_docs = TotalDocs, total_tokens = TotalTokens,
                            next_term_int_id = NextTermId} = Index) ->
    %% 1. The flat files are about to change: mark them incomplete
    ok = rocksdb:delete(Db, CfD, ?KEY_SEGMENT, [{sync, true}]),

    %% 2. Invert the forward index (keys ascend, so postings do too)
    Inverted = fold_cf(Db, CfDT,
        fun(<<DocIntId:64/big>>, Value, Acc) ->
                {Length, Counts} = decode_doc_terms(Value),
                true = ets:insert(DocStats, {DocIntId, Length}),
                maps:fold(fun(T, TF, A) -> A#{T => [{DocIntId, TF} | maps:get(T, A, [])]} end,
                          Acc, Counts)
        end, #{}),
    SortedPostings = maps:map(fun(_, Ps) -> lists:reverse(Ps) end, Inverted),

    %% 3. Write postings, block-max index and header, then sync. Offsets
    %% restart at the first sector, so the static-mode nonce rotates.
    {BlockMaxIndex, PostingBlocks} = build_block_max_index(Config, SortedPostings),
    FileHandle0 = barrel_vectordb_bm25_disk_file:rotate_data_nonce(
                    Index#bm25_disk_index.file_handle),
    {ok, FileHandle1} = write_postings_to_disk(FileHandle0, PostingBlocks),
    {ok, FileHandle2} = barrel_vectordb_bm25_disk_file:write_blockmax_index(
                          FileHandle1, BlockMaxIndex),
    {ok, FileHandle3} = barrel_vectordb_bm25_disk_file:update_stats(FileHandle2, #{
        doc_count => TotalDocs,
        term_count => NextTermId,
        total_tokens => TotalTokens,
        avgdl => case TotalDocs of 0 -> 0.0; _ -> TotalTokens / TotalDocs end
    }),
    ok = barrel_vectordb_bm25_disk_file:sync(FileHandle3),

    %% 4. Segment complete: restore the marker, clear pending docs
    {ok, Batch} = rocksdb:batch(),
    try
        ok = rocksdb:batch_put(Batch, CfD, ?KEY_SEGMENT, <<>>),
        maps:foreach(fun(DocIntId, _) ->
                             ok = rocksdb:batch_delete(Batch, CfP, <<DocIntId:64/big>>)
                     end, Masked),
        ok = rocksdb:write_batch(Db, Batch, [{sync, true}])
    after
        rocksdb:release_batch(Batch)
    end,

    Index#bm25_disk_index{
        file_handle = FileHandle3,
        hot_postings = #{},
        hot_docs = #{},
        hot_doc_lengths = #{},
        hot_size = 0,
        hot_tokens = 0,
        masked = #{},
        disk_doc_count = TotalDocs,
        disk_term_count = NextTermId,
        disk_total_tokens = TotalTokens,
        blockmax_index = BlockMaxIndex,
        segment_valid = true,
        compaction_in_progress = false
    }.

%% Block bounds hold the TF part only (IDF changes as docs come and
%% go); search multiplies by the current IDF.
build_block_max_index(#bm25_disk_config{k1 = K1, b = B, block_size = BlockSize},
                      SortedPostings) ->
    {BlockMaxIndex, PostingBlocks, _FinalOffset} = maps:fold(
        fun(TermIntId, Postings, {AccIndex, AccBlocks, AccOffset}) ->
            {TermBlocks, TermPostingBlocks, NewOffset} = lists:foldl(
                fun(Chunk, {BlockAcc, PostingAcc, OffsetAcc}) ->
                    {DocStart, _} = hd(Chunk),
                    {DocEnd, _} = lists:last(Chunk),
                    PaddedBlock = barrel_vectordb_bm25_disk_file:pad_to_sector(
                                    barrel_vectordb_bm25_disk_file:encode_posting_block(Chunk)),
                    Size = byte_size(PaddedBlock),
                    BlockEntry = #{
                        max_impact => compute_max_impact(Chunk, K1, B),
                        doc_start => DocStart,
                        doc_end => DocEnd,
                        offset => OffsetAcc,
                        size => Size
                    },
                    {[BlockEntry | BlockAcc],
                     [{OffsetAcc, PaddedBlock} | PostingAcc],
                     OffsetAcc + Size}
                end,
                {[], [], AccOffset},
                chunk_postings(Postings, BlockSize, [])
            ),
            {AccIndex#{TermIntId => lists:reverse(TermBlocks)},
             TermPostingBlocks ++ AccBlocks,
             NewOffset}
        end,
        {#{}, [], 4096},  %% Start after header sector
        SortedPostings
    ),
    {BlockMaxIndex, PostingBlocks}.

chunk_postings([], _BlockSize, Acc) ->
    lists:reverse(Acc);
chunk_postings(Postings, BlockSize, Acc) ->
    {Chunk, Rest} = lists:split(min(BlockSize, length(Postings)), Postings),
    chunk_postings(Rest, BlockSize, [Chunk | Acc]).

%% Upper bound of the TF part over any doc length (length 0).
compute_max_impact(Postings, K1, B) ->
    lists:foldl(
        fun({_DocIntId, TF}, MaxAcc) ->
            max(MaxAcc, TF * (K1 + 1) / (TF + K1 * (1 - B)))
        end,
        0.0,
        Postings
    ).

write_postings_to_disk(FileHandle, PostingBlocks) ->
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
