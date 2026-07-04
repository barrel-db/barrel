%%%-------------------------------------------------------------------
%%% @doc Per-store gen_batch_server managing RocksDB, vector index, and embeddings
%%%
%%% Each store runs as a separate gen_batch_server registered under its name.
%%% Handles all document operations, search, and embedding coordination.
%%%
%%% Uses gen_batch_server to automatically batch concurrent write operations
%%% into single atomic RocksDB WriteBatch operations, improving throughput
%%% under concurrent load.
%%%
%%% Supports pluggable vector index backends:
%%% - hnsw: Pure Erlang HNSW implementation (default)
%%% - faiss: Facebook FAISS via NIF binding (optional, requires barrel_faiss)
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_server).
-behaviour(gen_batch_server).

-include("barrel_vectordb.hrl").

%% Long timeout for operations that may take a while (1 hour in ms)
%% Using explicit value instead of infinity for gen_batch_server compatibility
-define(LONG_TIMEOUT, 3600000).

%% API
-export([
    start_link/2,
    stop/1,
    add/4,
    add_vector/5,
    add_batch/2,
    add_vector_batch/2,
    add_index_only/4,
    add_index_only_batch/2,
    get/2,
    update/4,
    upsert/4,
    delete/2,
    peek/2,
    search/3,
    search_vector/3,
    search_bm25/3,
    search_hybrid/3,
    embed/2,
    embed_batch/2,
    stats/1,
    count/1,
    embedder_info/1,
    checkpoint/1,
    bm25_info/1,
    bm25_compact/1
]).

%% gen_batch_server callbacks
-export([init/1, handle_batch/2, terminate/2]).

-record(state, {
    name :: atom(),
    db :: rocksdb:db_handle(),
    cf_vectors :: rocksdb:cf_handle(),
    cf_metadata :: rocksdb:cf_handle(),
    cf_text :: rocksdb:cf_handle(),
    cf_hnsw :: rocksdb:cf_handle(),
    index :: term(),                      %% Vector index (HNSW or FAISS state)
    index_module :: module(),             %% Index backend module
    dimension :: pos_integer(),
    embed_state :: map(),
    config :: map(),
    %% BM25 support
    bm25_index :: term() | undefined,     %% BM25 index state
    bm25_backend :: memory | disk | none, %% BM25 backend type
    %% Document backend for text + metadata. `undefined' = this store's own
    %% RocksDB column families (default). `{Module, Ctx}' = an external
    %% barrel_vectordb_docstore (vectors always stay local).
    docstore :: undefined | {module(), term()}
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start a named store.
%% Config options:
%%   - db_path: RocksDB storage path
%%   - dimension: Vector dimension
%%   - hnsw: HNSW index configuration
%%   - batch: gen_batch_server options (max_batch_size, min_batch_size)
%%
%% Default batch settings optimized for vector DB workloads:
%%   - min_batch_size: 4 (responsive for single inserts, batches concurrent ones)
%%   - max_batch_size: 256 (reasonable upper bound for memory/latency)
-spec start_link(atom(), map()) -> {ok, pid()} | {error, term()}.
start_link(Name, Config) ->
    BatchOpts = maps:get(batch, Config, #{}),
    %% Apply sensible defaults for vector DB workload
    DefaultBatch = #{min_batch_size => 4, max_batch_size => 256},
    MergedBatch = maps:merge(DefaultBatch, BatchOpts),
    GBOpts = maps:fold(fun
        (max_batch_size, V, Acc) -> [{max_batch_size, V} | Acc];
        (min_batch_size, V, Acc) -> [{min_batch_size, V} | Acc];
        (_, _, Acc) -> Acc
    end, [], MergedBatch),
    gen_batch_server:start_link({local, Name}, ?MODULE, {Name, Config},
                                [{gen_batch_server, GBOpts}]).

%% @doc Stop a store.
-spec stop(atom() | pid()) -> ok.
stop(Store) ->
    gen_batch_server:stop(Store).

%% @doc Add document with auto-embedding.
-spec add(atom() | pid(), binary(), binary(), map()) -> ok | {error, term()}.
add(Store, Id, Text, Metadata) ->
    gen_batch_server:call(Store, {add, Id, Text, Metadata}, ?LONG_TIMEOUT).

%% @doc Add document with explicit vector.
-spec add_vector(atom() | pid(), binary(), binary(), map(), [float()]) -> ok | {error, term()}.
add_vector(Store, Id, Text, Metadata, Vector) ->
    gen_batch_server:call(Store, {add_vector, Id, Text, Metadata, Vector}, ?LONG_TIMEOUT).

%% @doc Add multiple documents.
-spec add_batch(atom() | pid(), [{binary(), binary(), map()}]) ->
    {ok, #{inserted := non_neg_integer()}} | {error, term()}.
add_batch(Store, Docs) ->
    gen_batch_server:call(Store, {add_batch, Docs}, ?LONG_TIMEOUT).

%% @doc Add multiple documents with pre-computed vectors (bulk insert).
-spec add_vector_batch(atom() | pid(), [{binary(), binary(), map(), [float()]}]) ->
    {ok, #{inserted := non_neg_integer()}} | {error, term()}.
add_vector_batch(Store, Docs) ->
    gen_batch_server:call(Store, {add_vector_batch, Docs}, ?LONG_TIMEOUT).

%% @doc Index a vector without storing text/metadata (see barrel_vectordb).
-spec add_index_only(atom() | pid(), binary(), binary(), [float()]) ->
    ok | {error, term()}.
add_index_only(Store, Id, Text, Vector) ->
    gen_batch_server:call(Store, {index_only, Id, Text, Vector}, ?LONG_TIMEOUT).

%% @doc Index multiple vectors without storing text/metadata.
-spec add_index_only_batch(atom() | pid(), [{binary(), binary(), [float()]}]) ->
    {ok, #{inserted := non_neg_integer()}} | {error, term()}.
add_index_only_batch(Store, Entries) ->
    gen_batch_server:call(Store, {index_only_batch, Entries}, ?LONG_TIMEOUT).

%% @doc Get document by ID.
-spec get(atom() | pid(), binary()) -> {ok, map()} | not_found | {error, term()}.
get(Store, Id) ->
    gen_batch_server:call(Store, {get, Id}).

%% @doc Update document metadata (re-embeds the text).
-spec update(atom() | pid(), binary(), binary(), map()) -> ok | not_found | {error, term()}.
update(Store, Id, Text, Metadata) ->
    gen_batch_server:call(Store, {update, Id, Text, Metadata}, ?LONG_TIMEOUT).

%% @doc Insert or update document.
-spec upsert(atom() | pid(), binary(), binary(), map()) -> ok | {error, term()}.
upsert(Store, Id, Text, Metadata) ->
    gen_batch_server:call(Store, {upsert, Id, Text, Metadata}, ?LONG_TIMEOUT).

%% @doc Delete document.
-spec delete(atom() | pid(), binary()) -> ok | {error, term()}.
delete(Store, Id) ->
    gen_batch_server:call(Store, {delete, Id}).

%% @doc Peek at documents (sample without search).
-spec peek(atom() | pid(), pos_integer()) -> {ok, [map()]}.
peek(Store, Limit) ->
    gen_batch_server:call(Store, {peek, Limit}).

%% @doc Search with text query.
-spec search(atom() | pid(), binary(), map()) -> {ok, [map()]} | {error, term()}.
search(Store, Query, Opts) ->
    gen_batch_server:call(Store, {search, Query, Opts}, ?LONG_TIMEOUT).

%% @doc Search with vector query.
-spec search_vector(atom() | pid(), [float()], map()) -> {ok, [map()]} | {error, term()}.
search_vector(Store, Vector, Opts) ->
    gen_batch_server:call(Store, {search_vector, Vector, Opts}, ?LONG_TIMEOUT).

%% @doc Search with BM25 text query.
%% Opts: #{k => integer()}
-spec search_bm25(atom() | pid(), binary(), map()) -> {ok, [{binary(), float()}]} | {error, term()}.
search_bm25(Store, Query, Opts) ->
    gen_batch_server:call(Store, {search_bm25, Query, Opts}, ?LONG_TIMEOUT).

%% @doc Hybrid search combining BM25 and vector search.
%% Opts: #{k => integer(), bm25_weight => float(), vector_weight => float(), fusion => rrf | linear}
-spec search_hybrid(atom() | pid(), binary(), map()) -> {ok, [map()]} | {error, term()}.
search_hybrid(Store, Query, Opts) ->
    gen_batch_server:call(Store, {search_hybrid, Query, Opts}, ?LONG_TIMEOUT).

%% @doc Get BM25 index information.
-spec bm25_info(atom() | pid()) -> {ok, map()} | {error, bm25_not_enabled}.
bm25_info(Store) ->
    gen_batch_server:call(Store, bm25_info).

%% @doc Trigger BM25 index compaction (disk backend only).
-spec bm25_compact(atom() | pid()) -> ok | {error, term()}.
bm25_compact(Store) ->
    gen_batch_server:call(Store, bm25_compact, ?LONG_TIMEOUT).

%% @doc Embed single text.
-spec embed(atom() | pid(), binary()) -> {ok, [float()]} | {error, term()}.
embed(Store, Text) ->
    gen_batch_server:call(Store, {embed, Text}, ?LONG_TIMEOUT).

%% @doc Embed multiple texts.
-spec embed_batch(atom() | pid(), [binary()]) -> {ok, [[float()]]} | {error, term()}.
embed_batch(Store, Texts) ->
    gen_batch_server:call(Store, {embed_batch, Texts}, ?LONG_TIMEOUT).

%% @doc Get store statistics.
-spec stats(atom() | pid()) -> {ok, map()}.
stats(Store) ->
    gen_batch_server:call(Store, stats).

%% @doc Get document count.
-spec count(atom() | pid()) -> non_neg_integer().
count(Store) ->
    gen_batch_server:call(Store, count).

%% @doc Get embedder information.
-spec embedder_info(atom() | pid()) -> {ok, map()}.
embedder_info(Store) ->
    gen_batch_server:call(Store, embedder_info).

%% @doc Checkpoint HNSW index to disk.
-spec checkpoint(atom() | pid()) -> ok.
checkpoint(Store) ->
    gen_batch_server:call(Store, checkpoint).

%%====================================================================
%% gen_batch_server callbacks
%%====================================================================

init({Name, Config}) ->
    process_flag(trap_exit, true),

    DbPath = maps:get(db_path, Config, "priv/barrel_vectordb_data"),
    Dimension = maps:get(dimension, Config, ?DEFAULT_DIMENSION),

    %% Document backend: undefined (own RocksDB CFs) by default, or an external
    %% barrel_vectordb_docstore. Raises on misconfiguration so the start fails.
    Docstore = init_docstore(Name, Config),

    %% Select index backend (default: hnsw)
    Backend = maps:get(backend, Config, hnsw),
    IndexModule = barrel_vectordb_index:backend_module(Backend),

    %% Get backend-specific configuration
    IndexConfig = case Backend of
        hnsw -> maps:get(hnsw, Config, #{});
        faiss -> maps:get(faiss, Config, #{});
        diskann ->
            DiskAnnConfig = maps:get(diskann, Config, #{}),
            %% Set default base_path relative to db_path if not specified
            case maps:is_key(base_path, DiskAnnConfig) of
                true -> DiskAnnConfig;
                false -> DiskAnnConfig#{base_path => filename:join(DbPath, "diskann")}
            end
    end,

    %% Initialize embedder
    EmbedConfig = Config#{dimensions => Dimension},
    case barrel_embed:init(EmbedConfig) of
        {ok, EmbedState} ->
            case init_rocksdb(DbPath) of
                {ok, Db, CfHandles} ->
                    %% Load or create vector index
                    case load_or_create_index(Db, CfHandles, Dimension, IndexConfig, IndexModule) of
                        {ok, Index} ->
                            %% Initialize BM25 index if configured
                            BM25Backend = maps:get(bm25_backend, Config, none),
                            case init_bm25(DbPath, BM25Backend, Config) of
                                {ok, BM25Index} ->
                                    State = #state{
                                        name = Name,
                                        db = Db,
                                        cf_vectors = maps:get(vectors, CfHandles),
                                        cf_metadata = maps:get(metadata, CfHandles),
                                        cf_text = maps:get(text, CfHandles),
                                        cf_hnsw = maps:get(hnsw, CfHandles),
                                        index = Index,
                                        index_module = IndexModule,
                                        dimension = Dimension,
                                        embed_state = EmbedState,
                                        config = Config,
                                        bm25_index = BM25Index,
                                        bm25_backend = BM25Backend,
                                        docstore = Docstore
                                    },
                                    {ok, State};
                                {error, BM25Error} ->
                                    {stop, {bm25_init_failed, BM25Error}}
                            end;
                        {error, IndexError} ->
                            {stop, {index_init_failed, IndexError}}
                    end;
                {error, Reason} ->
                    {stop, {db_open_failed, Reason}}
            end;
        {error, EmbedError} ->
            {stop, {embed_init_failed, EmbedError}}
    end.

%% @doc Handle a batch of operations.
%% Partitions operations into reads (processed immediately) and writes (batched atomically).
handle_batch(Ops, State) ->
    %% Separate reads from writes
    {Reads, Writes} = partition_ops(Ops),

    %% Process reads immediately (they don't modify state)
    {ReadActions, State1} = process_reads(Reads, State),

    %% Process writes atomically in a single batch
    {WriteActions, State2} = process_writes_atomic(Writes, State1),

    {ok, ReadActions ++ WriteActions, State2}.

%% Partition operations into reads and writes
partition_ops(Ops) ->
    lists:partition(fun(Op) ->
        case Op of
            {_, _, {add, _, _, _}} -> false;
            {_, _, {add_vector, _, _, _, _}} -> false;
            {_, _, {add_batch, _}} -> false;
            {_, _, {add_vector_batch, _}} -> false;
            {_, _, {index_only, _, _, _}} -> false;
            {_, _, {index_only_batch, _}} -> false;
            _ -> true  % reads: get, search, peek, stats, count, delete, update, upsert
        end
    end, Ops).

%% Process read operations (and delete/update/upsert which need immediate state access)
process_reads(Reads, State) ->
    lists:foldl(fun(Op, {AccActions, AccState}) ->
        {Action, NewState} = process_single_op(Op, AccState),
        {[Action | AccActions], NewState}
    end, {[], State}, Reads).

%% Process a single operation (for reads and non-batched operations)
process_single_op({call, From, {get, Id}}, State) ->
    Result = do_get(Id, State),
    {{reply, From, Result}, State};

process_single_op({call, From, {delete, Id}}, State) ->
    case do_delete(Id, State) of
        {ok, NewState} ->
            {{reply, From, ok}, NewState};
        {error, _} = Error ->
            {{reply, From, Error}, State}
    end;

process_single_op({call, From, {update, Id, Text, Metadata}}, State) ->
    case do_get(Id, State) of
        {ok, _Existing} ->
            case do_delete(Id, State) of
                {ok, State1} ->
                    case do_embed(Text, State1) of
                        {ok, Vector} ->
                            case do_add(Id, Text, Metadata, Vector, State1) of
                                {ok, NewState} ->
                                    {{reply, From, ok}, NewState};
                                {error, _} = Error ->
                                    {{reply, From, Error}, State}
                            end;
                        {error, _} = Error ->
                            {{reply, From, Error}, State}
                    end;
                {error, _} = Error ->
                    {{reply, From, Error}, State}
            end;
        not_found ->
            {{reply, From, not_found}, State};
        {error, _} = Error ->
            {{reply, From, Error}, State}
    end;

process_single_op({call, From, {upsert, Id, Text, Metadata}}, State) ->
    case do_get(Id, State) of
        {ok, _Existing} ->
            case do_delete(Id, State) of
                {ok, State1} ->
                    case do_embed(Text, State1) of
                        {ok, Vector} ->
                            case do_add(Id, Text, Metadata, Vector, State1) of
                                {ok, NewState} ->
                                    {{reply, From, ok}, NewState};
                                {error, _} = Error ->
                                    {{reply, From, Error}, State}
                            end;
                        {error, _} = Error ->
                            {{reply, From, Error}, State}
                    end;
                {error, _} = Error ->
                    {{reply, From, Error}, State}
            end;
        not_found ->
            case do_embed(Text, State) of
                {ok, Vector} ->
                    case do_add(Id, Text, Metadata, Vector, State) of
                        {ok, NewState} ->
                            {{reply, From, ok}, NewState};
                        {error, _} = Error ->
                            {{reply, From, Error}, State}
                    end;
                {error, _} = Error ->
                    {{reply, From, Error}, State}
            end;
        {error, _} = Error ->
            {{reply, From, Error}, State}
    end;

process_single_op({call, From, {peek, Limit}}, State) ->
    Result = do_peek(Limit, State),
    {{reply, From, Result}, State};

process_single_op({call, From, {search, Query, Opts}}, State) ->
    case do_embed(Query, State) of
        {ok, Vector} ->
            Result = do_search(Vector, Opts, State),
            {{reply, From, Result}, State};
        {error, _} = Error ->
            {{reply, From, Error}, State}
    end;

process_single_op({call, From, {search_vector, Vector, Opts}}, #state{dimension = Dim} = State) ->
    case length(Vector) of
        Dim ->
            Result = do_search(Vector, Opts, State),
            {{reply, From, Result}, State};
        Other ->
            {{reply, From, {error, {dimension_mismatch, Dim, Other}}}, State}
    end;

process_single_op({call, From, {embed, Text}}, State) ->
    Result = do_embed(Text, State),
    {{reply, From, Result}, State};

process_single_op({call, From, {embed_batch, Texts}}, State) ->
    Result = do_embed_batch(Texts, State),
    {{reply, From, Result}, State};

process_single_op({call, From, stats}, #state{index = Index, index_module = Mod,
                                                dimension = Dim, config = Config} = State) ->
    Stats = #{
        dimension => Dim,
        count => Mod:size(Index),
        index => Mod:info(Index),
        config => Config
    },
    {{reply, From, {ok, Stats}}, State};

process_single_op({call, From, count}, #state{index = Index, index_module = Mod} = State) ->
    {{reply, From, Mod:size(Index)}, State};

process_single_op({call, From, embedder_info}, #state{embed_state = EmbedState} = State) ->
    Info = barrel_embed:info(EmbedState),
    {{reply, From, {ok, Info}}, State};

process_single_op({call, From, checkpoint}, #state{db = Db, cf_hnsw = CfHnsw,
                                                    index = Index, index_module = Mod} = State) ->
    _ = persist_index_meta(Db, CfHnsw, Index, Mod),
    {{reply, From, ok}, State};

%% BM25 search
process_single_op({call, From, {search_bm25, Query, Opts}}, State) ->
    Result = do_search_bm25(State, Query, Opts),
    {{reply, From, Result}, State};

%% Hybrid search (BM25 + vector)
process_single_op({call, From, {search_hybrid, Query, Opts}}, State) ->
    case do_embed(Query, State) of
        {ok, Vector} ->
            Result = do_search_hybrid(State, Query, Opts, Vector),
            {{reply, From, Result}, State};
        {error, _} = Error ->
            {{reply, From, Error}, State}
    end;

%% BM25 info
process_single_op({call, From, bm25_info}, State) ->
    Result = do_bm25_info(State),
    {{reply, From, Result}, State};

%% BM25 compaction
process_single_op({call, From, bm25_compact}, State) ->
    case do_bm25_compact(State) of
        {ok, NewState} ->
            {{reply, From, ok}, NewState};
        {error, _} = Error ->
            {{reply, From, Error}, State}
    end;

process_single_op({call, From, _Unknown}, State) ->
    {{reply, From, {error, unknown_request}}, State}.

%% Process write operations atomically in a single RocksDB batch
process_writes_atomic([], State) ->
    {[], State};
process_writes_atomic(Writes, #state{db = Db, cf_vectors = CfV, cf_metadata = CfM,
                                      cf_text = CfT, docstore = Docstore,
                                      index = Index, index_module = Mod, dimension = Dim} = State) ->
    {ok, Batch} = rocksdb:batch(),

    %% First, prepare all embeddings if needed (this is done before batch to avoid
    %% partial writes on embedding failures)
    case prepare_writes(Writes, State) of
        {ok, PreparedWrites} ->
            %% Check if module supports batch insert (e.g., DiskANN)
            SupportsBatchInsert = erlang:function_exported(Mod, insert_batch, 3),

            %% Apply writes to RocksDB batch and collect vectors for index.
            %% DocPairs holds {Id, Text, Metadata} only for an external docstore
            %% (empty for the default, which writes text/metadata into the batch).
            %% Bm25Pairs holds {Id, Text} for successfully applied writes, used to
            %% update the BM25 index after the batch commits (the atomic write path
            %% indexes BM25 here; do_add only covers update/upsert).
            {VectorsForIndex, DocPairs, Bm25Pairs, Replies0} = lists:foldl(
                fun({From, WriteOp, _PreparedData}, {AccVectors, AccDocs, AccBm25, AccReplies}) ->
                    case apply_write_to_rocksdb_batch(WriteOp, Batch, CfV, CfM, CfT, Dim, Docstore) of
                        {ok, {Id, Vector}, Docs} ->
                            {[{Id, Vector} | AccVectors], Docs ++ AccDocs,
                             bm25_pairs_of(WriteOp) ++ AccBm25, [{From, ok} | AccReplies]};
                        {ok, VectorList, Docs} when is_list(VectorList) ->
                            %% Batch insert - return stats
                            Count = length(VectorList),
                            {VectorList ++ AccVectors, Docs ++ AccDocs,
                             bm25_pairs_of(WriteOp) ++ AccBm25,
                             [{From, {ok, #{inserted => Count}}} | AccReplies]};
                        {error, Reason} ->
                            {AccVectors, AccDocs, AccBm25, [{From, {error, Reason}} | AccReplies]}
                    end
                end,
                {[], [], [], []},
                PreparedWrites
            ),

            %% Insert into index (batch or individual)
            case insert_vectors_to_index(lists:reverse(VectorsForIndex), Index, Mod, SupportsBatchInsert) of
                {ok, NewIndex} ->
                    %% Commit the RocksDB batch atomically, then (for an external
                    %% docstore only) write text/metadata. Vector-first, doc-second.
                    case rocksdb:write_batch(Db, Batch, []) of
                        ok ->
                            %% BM25 indexes text and is independent of the docstore,
                            %% so update it once the rocksdb write succeeds.
                            {ok, NewBM25} = bm25_add_all(State#state.bm25_index,
                                                         lists:reverse(Bm25Pairs)),
                            StateOk = State#state{index = NewIndex, bm25_index = NewBM25},
                            case docstore_multi_put(Docstore, lists:reverse(DocPairs)) of
                                ok ->
                                    Replies = [{reply, From, Reply} || {From, Reply} <- Replies0],
                                    {Replies, StateOk};
                                {error, DsReason} ->
                                    ErrorReplies = [{reply, From, {error, {docstore_error, DsReason}}}
                                                   || {From, _} <- Replies0],
                                    {ErrorReplies, StateOk}
                            end;
                        {error, Reason} ->
                            ErrorReplies = [{reply, From, {error, {db_error, Reason}}}
                                           || {From, _} <- Replies0],
                            {ErrorReplies, State}
                    end;
                {error, Reason} ->
                    ErrorReplies = [{reply, From, {error, {index_error, Reason}}}
                                   || {From, _} <- Replies0],
                    {ErrorReplies, State}
            end;
        {error, From, Reason, SuccessfulPreps} ->
            %% Embedding failed for one operation, fail that one and process others
            ErrorReply = {reply, From, {error, Reason}},
            case SuccessfulPreps of
                [] ->
                    {[ErrorReply], State};
                _ ->
                    %% Process successful preps
                    {OkReplies, NewState} = process_writes_atomic(
                        [{call, F, Op} || {F, Op, _} <- SuccessfulPreps],
                        State),
                    {[ErrorReply | OkReplies], NewState}
            end
    end.

%% Apply write to RocksDB batch only (no index update yet). Returns the vector(s)
%% for the index plus the {Id, Text, Metadata} pairs to write to an external
%% docstore (empty for the default, which writes them into the batch here).
apply_write_to_rocksdb_batch({add_vector, Id, Text, Metadata, Vector}, Batch, CfV, CfM, CfT, Dim, Docstore) ->
    case length(Vector) of
        Dim ->
            VectorBin = encode_vector(Vector),
            ok = rocksdb:batch_put(Batch, CfV, Id, VectorBin),
            DocPairs = put_docdata_to_batch(Docstore, Batch, CfM, CfT, Id, Text, Metadata),
            {ok, {Id, Vector}, DocPairs};
        Other ->
            {error, {dimension_mismatch, Dim, Other}}
    end;
apply_write_to_rocksdb_batch({index_only, Id, _Text, Vector}, Batch, CfV, CfM, CfT, Dim, _Docstore) ->
    case length(Vector) of
        Dim ->
            %% Vector CF only: it stays authoritative for index rebuild.
            %% Clear any stale text/metadata rows from a previous full add
            %% so reads never pair the new vector with old doc data. The
            %% external docstore is never written (the caller owns docs).
            VectorBin = encode_vector(Vector),
            ok = rocksdb:batch_put(Batch, CfV, Id, VectorBin),
            ok = rocksdb:batch_delete(Batch, CfM, Id),
            ok = rocksdb:batch_delete(Batch, CfT, Id),
            {ok, {Id, Vector}, []};
        Other ->
            {error, {dimension_mismatch, Dim, Other}}
    end;
apply_write_to_rocksdb_batch({index_only_batch, Entries}, Batch, CfV, CfM, CfT, Dim, _Docstore) ->
    Results = lists:map(
        fun({Id, _Text, Vector}) ->
            case length(Vector) of
                Dim ->
                    VectorBin = encode_vector(Vector),
                    ok = rocksdb:batch_put(Batch, CfV, Id, VectorBin),
                    ok = rocksdb:batch_delete(Batch, CfM, Id),
                    ok = rocksdb:batch_delete(Batch, CfT, Id),
                    {ok, {Id, Vector}, []};
                Other ->
                    {error, {dimension_mismatch, Dim, Other}}
            end
        end,
        Entries
    ),
    case [E || {error, _} = E <- Results] of
        [] ->
            Vectors = [{Id, Vec} || {ok, {Id, Vec}, _} <- Results],
            {ok, Vectors, []};
        [FirstError | _] ->
            FirstError
    end;
apply_write_to_rocksdb_batch({add_vector_batch, Docs}, Batch, CfV, CfM, CfT, Dim, Docstore) ->
    %% Process batch - collect all vectors
    Results = lists:map(
        fun({Id, Text, Metadata, Vector}) ->
            case length(Vector) of
                Dim ->
                    VectorBin = encode_vector(Vector),
                    ok = rocksdb:batch_put(Batch, CfV, Id, VectorBin),
                    DocPairs = put_docdata_to_batch(Docstore, Batch, CfM, CfT, Id, Text, Metadata),
                    {ok, {Id, Vector}, DocPairs};
                Other ->
                    {error, {dimension_mismatch, Dim, Other}}
            end
        end,
        Docs
    ),
    %% Check for any errors
    case [E || {error, _} = E <- Results] of
        [] ->
            Vectors = [{Id, Vec} || {ok, {Id, Vec}, _} <- Results],
            DocPairs = lists:append([DP || {ok, _, DP} <- Results]),
            {ok, Vectors, DocPairs};
        [FirstError | _] ->
            FirstError
    end.

%% @private For the default (undefined) docstore, write metadata + text into the
%% same RocksDB batch as the vector (atomic) and return no external pairs. For an
%% external docstore, skip the column families and return the pair to write after
%% the batch commits.
put_docdata_to_batch(undefined, Batch, CfM, CfT, Id, Text, Metadata) ->
    MetadataBin = term_to_binary(Metadata),
    ok = rocksdb:batch_put(Batch, CfM, Id, MetadataBin),
    ok = rocksdb:batch_put(Batch, CfT, Id, Text),
    [];
put_docdata_to_batch({_Mod, _Ctx}, _Batch, _CfM, _CfT, Id, Text, Metadata) ->
    [{Id, Text, Metadata}].

%%====================================================================
%% Document backend (text + metadata) helpers
%%====================================================================

%% @private Initialise the document backend from store config. `undefined' keeps
%% text/metadata in this store's RocksDB CFs; otherwise start the configured
%% barrel_vectordb_docstore module.
init_docstore(Name, Config) ->
    case maps:get(docstore, Config, undefined) of
        undefined ->
            undefined;
        {Module, ModConfig} when is_atom(Module), is_map(ModConfig) ->
            start_docstore(Module, Name, ModConfig);
        Module when is_atom(Module) ->
            start_docstore(Module, Name, #{})
    end.

start_docstore(Module, Name, ModConfig) ->
    case Module:init(Name, ModConfig) of
        {ok, Ctx} -> {Module, Ctx};
        {error, Reason} -> error({docstore_init_failed, Module, Reason})
    end.

%% @private Write {Id, Text, Metadata} pairs to an external docstore (no-op for
%% the default, which already wrote them into the RocksDB batch).
docstore_multi_put(undefined, _Pairs) ->
    ok;
docstore_multi_put({Module, Ctx}, Pairs) ->
    Module:multi_put(Ctx, Pairs).

%% @private Fetch one document's metadata + text. Returns {ok, Metadata, Text}.
docstore_get(undefined, Db, CfM, CfT, Id) ->
    case {rocksdb:get(Db, CfM, Id, []), rocksdb:get(Db, CfT, Id, [])} of
        {{ok, MetadataBin}, {ok, Text}} ->
            {ok, binary_to_term(MetadataBin), Text};
        _ ->
            not_found
    end;
docstore_get({Module, Ctx}, _Db, _CfM, _CfT, Id) ->
    case Module:get(Ctx, Id) of
        {ok, Text, Metadata} -> {ok, Metadata, Text};
        not_found -> not_found;
        {error, _} = Err -> Err
    end.

%% @private Add metadata + text deletes to the batch for the default docstore.
delete_docdata_from_batch(undefined, Batch, CfM, CfT, Id) ->
    ok = rocksdb:batch_delete(Batch, CfM, Id),
    ok = rocksdb:batch_delete(Batch, CfT, Id),
    ok;
delete_docdata_from_batch({_Module, _Ctx}, _Batch, _CfM, _CfT, _Id) ->
    ok.

docstore_delete(undefined, _Id) ->
    ok;
docstore_delete({Module, Ctx}, Id) ->
    Module:delete(Ctx, Id).

%% @private Batch-fetch metadata + text for peek, returning two lists in the
%% rocksdb:multi_get shape so combine_peek_results stays backend-agnostic.
docstore_multi_fetch(undefined, Db, CfM, CfT, Keys) ->
    {rocksdb:multi_get(Db, CfM, Keys, []), rocksdb:multi_get(Db, CfT, Keys, [])};
docstore_multi_fetch({Module, Ctx}, _Db, _CfM, _CfT, Keys) ->
    Results = Module:multi_get(Ctx, Keys),
    {[meta_result(R) || R <- Results], [text_result(R) || R <- Results]}.

%% @private Batch-fetch for search, honouring the need-meta / include-text flags.
docstore_search_fetch(undefined, Db, CfM, CfT, Ids, NeedMeta, IncludeText) ->
    Metas = case NeedMeta of
        true -> rocksdb:multi_get(Db, CfM, Ids, []);
        false -> [not_needed || _ <- Ids]
    end,
    Texts = case IncludeText of
        true -> rocksdb:multi_get(Db, CfT, Ids, []);
        false -> [not_needed || _ <- Ids]
    end,
    {Metas, Texts};
docstore_search_fetch({Module, Ctx}, _Db, _CfM, _CfT, Ids, NeedMeta, IncludeText) ->
    Results = Module:multi_get(Ctx, Ids),
    Metas = case NeedMeta of
        true -> [meta_result(R) || R <- Results];
        false -> [not_needed || _ <- Ids]
    end,
    Texts = case IncludeText of
        true -> [text_result(R) || R <- Results];
        false -> [not_needed || _ <- Ids]
    end,
    {Metas, Texts}.

%% @private Re-encode external docstore results into the CF result shape so the
%% combine_*_results functions (which binary_to_term the metadata) are unchanged.
meta_result({ok, _Text, Metadata}) -> {ok, term_to_binary(Metadata)};
meta_result(_) -> not_found.

text_result({ok, Text, _Metadata}) -> {ok, Text};
text_result(_) -> not_found.

docstore_terminate(undefined) ->
    ok;
docstore_terminate({Module, Ctx}) ->
    Module:terminate(Ctx).

%% Insert vectors to index - use batch insert if available
insert_vectors_to_index([], Index, _Mod, _SupportsBatch) ->
    {ok, Index};
insert_vectors_to_index(Vectors, Index, Mod, true) ->
    %% Use batch insert
    Mod:insert_batch(Index, Vectors, #{});
insert_vectors_to_index(Vectors, Index, Mod, false) ->
    %% Fall back to individual inserts
    lists:foldl(
        fun({Id, Vector}, {ok, AccIndex}) ->
            Mod:insert(AccIndex, Id, Vector);
           (_, {error, _} = Err) ->
            Err
        end,
        {ok, Index},
        Vectors
    ).

%% Prepare writes by computing embeddings where needed
prepare_writes(Writes, State) ->
    prepare_writes(Writes, State, []).

prepare_writes([], _State, Acc) ->
    {ok, lists:reverse(Acc)};
prepare_writes([{call, From, {add, Id, Text, Metadata}} | Rest], State, Acc) ->
    case do_embed(Text, State) of
        {ok, Vector} ->
            prepare_writes(Rest, State, [{From, {add_vector, Id, Text, Metadata, Vector}, prepared} | Acc]);
        {error, Reason} ->
            {error, From, Reason, lists:reverse(Acc)}
    end;
prepare_writes([{call, From, {add_vector, Id, Text, Metadata, Vector}} | Rest], State, Acc) ->
    prepare_writes(Rest, State, [{From, {add_vector, Id, Text, Metadata, Vector}, prepared} | Acc]);
prepare_writes([{call, From, {add_batch, Docs}} | Rest], State, Acc) ->
    case prepare_batch_embeddings(Docs, State) of
        {ok, PreparedDocs} ->
            prepare_writes(Rest, State, [{From, {add_vector_batch, PreparedDocs}, prepared} | Acc]);
        {error, Reason} ->
            {error, From, Reason, lists:reverse(Acc)}
    end;
prepare_writes([{call, From, {add_vector_batch, Docs}} | Rest], State, Acc) ->
    prepare_writes(Rest, State, [{From, {add_vector_batch, Docs}, prepared} | Acc]);
prepare_writes([{call, From, {index_only, _Id, _Text, _Vector} = Op} | Rest], State, Acc) ->
    %% Explicit vector, nothing to embed
    prepare_writes(Rest, State, [{From, Op, prepared} | Acc]);
prepare_writes([{call, From, {index_only_batch, _Entries} = Op} | Rest], State, Acc) ->
    prepare_writes(Rest, State, [{From, Op, prepared} | Acc]).

%% Prepare embeddings for batch add
prepare_batch_embeddings(Docs, State) ->
    Texts = [Text || {_Id, Text, _Meta} <- Docs],
    case do_embed_batch(Texts, State) of
        {ok, Vectors} ->
            PreparedDocs = lists:zipwith(fun({Id, Text, Meta}, Vector) ->
                {Id, Text, Meta, Vector}
            end, Docs, Vectors),
            {ok, PreparedDocs};
        {error, Reason} ->
            {error, Reason}
    end.

terminate(_Reason, #state{db = Db, index = Index, index_module = Mod, cf_hnsw = CfHnsw,
                          docstore = Docstore}) ->
    %% Persist index metadata before closing
    _ = persist_index_meta(Db, CfHnsw, Index, Mod),
    %% Close index if backend supports it (e.g., FAISS releases NIF resources)
    _ = maybe_close_index(Mod, Index),
    _ = rocksdb:close(Db),
    _ = docstore_terminate(Docstore),
    ok.

%% Close index if the backend module supports close/1
maybe_close_index(Mod, Index) ->
    case erlang:function_exported(Mod, close, 1) of
        true -> Mod:close(Index);
        false -> ok
    end.

%%====================================================================
%% Internal Functions - Database
%%====================================================================

%% Initialize RocksDB with column families
init_rocksdb(DbPath) ->
    %% Ensure directory exists
    ok = filelib:ensure_dir(DbPath ++ "/"),

    Options = [{create_if_missing, true}, {create_missing_column_families, true}],

    CfDefs = [
        {?CF_DEFAULT, []},
        {?CF_VECTORS, []},
        {?CF_METADATA, []},
        {?CF_TEXT, []},
        {?CF_HNSW, []}
    ],

    case rocksdb:open(DbPath, Options, CfDefs) of
        {ok, Db, [_Default, CfVectors, CfMetadata, CfText, CfHnsw]} ->
            {ok, Db, #{
                vectors => CfVectors,
                metadata => CfMetadata,
                text => CfText,
                hnsw => CfHnsw
            }};
        {error, Reason} ->
            {error, Reason}
    end.

%% Load existing index or create new one
load_or_create_index(Db, CfHandles, Dimension, IndexConfig, IndexModule) ->
    case IndexModule of
        barrel_vectordb_diskann ->
            %% DiskANN manages its own persistence - use open/new directly
            load_or_create_diskann(IndexConfig, Dimension);
        _ ->
            %% HNSW/FAISS - rebuild from vectors stored in RocksDB
            CfHnsw = maps:get(hnsw, CfHandles),
            CfVectors = maps:get(vectors, CfHandles),
            case load_index_meta(Db, CfHnsw) of
                {ok, _IndexMeta} ->
                    rebuild_from_vectors(Db, CfVectors, Dimension, IndexConfig, IndexModule);
                not_found ->
                    IndexModule:new(IndexConfig#{dimension => Dimension})
            end
    end.

%% DiskANN-specific loading: try open existing, else create new
load_or_create_diskann(Config, Dimension) ->
    BasePath = maps:get(base_path, Config),
    ok = filelib:ensure_dir(filename:join(BasePath, "dummy")),
    case barrel_vectordb_diskann:open(BasePath) of
        {ok, Index} ->
            {ok, Index};
        {error, _} ->
            %% No existing index or failed to open - create new
            barrel_vectordb_diskann:new(Config#{dimension => Dimension, storage_mode => disk})
    end.

%% Load index metadata from storage
load_index_meta(Db, CfHnsw) ->
    case rocksdb:get(Db, CfHnsw, ?HNSW_META_KEY, []) of
        {ok, Binary} ->
            try
                Meta = binary_to_term(Binary),
                {ok, Meta}
            catch
                _:_ -> not_found
            end;
        not_found ->
            not_found;
        {error, _} ->
            not_found
    end.

%% Persist index metadata
persist_index_meta(Db, CfHnsw, Index, Mod) ->
    %% Use the index module's info function to get metadata
    Info = Mod:info(Index),
    Meta = #{
        size => maps:get(size, Info, Mod:size(Index)),
        dimension => maps:get(dimension, Info, undefined),
        backend => maps:get(backend, Info, hnsw)
    },
    Binary = term_to_binary(Meta),
    rocksdb:put(Db, CfHnsw, ?HNSW_META_KEY, Binary, []).

%% Rebuild index from stored vectors (index is in-memory only, rebuilt from RocksDB)
rebuild_from_vectors(Db, CfVectors, Dimension, IndexConfig, IndexModule) ->
    case IndexModule:new(IndexConfig#{dimension => Dimension}) of
        {ok, Index} ->
            case rocksdb:iterator(Db, CfVectors, []) of
                {ok, Iter} ->
                    try
                        rebuild_loop(Iter, rocksdb:iterator_move(Iter, first), Index, IndexModule)
                    after
                        rocksdb:iterator_close(Iter)
                    end;
                {error, _} ->
                    {ok, Index}
            end;
        {error, _} = Error ->
            Error
    end.

rebuild_loop(_Iter, {error, _}, Index, _Mod) ->
    {ok, Index};
rebuild_loop(Iter, {ok, Key, VectorBin}, Index, Mod) ->
    Vector = decode_vector(VectorBin),
    case Mod:insert(Index, Key, Vector) of
        {ok, NewIndex} ->
            rebuild_loop(Iter, rocksdb:iterator_move(Iter, next), NewIndex, Mod);
        {error, Reason} ->
            error_logger:warning_msg("Failed to insert vector ~p during rebuild: ~p~n",
                                     [Key, Reason]),
            rebuild_loop(Iter, rocksdb:iterator_move(Iter, next), Index, Mod)
    end.

%%====================================================================
%% Internal Functions - Operations
%%====================================================================

%% Embed text using the configured provider
do_embed(Text, #state{embed_state = EmbedState}) ->
    barrel_embed:embed(Text, EmbedState).

%% Embed batch of texts
do_embed_batch(Texts, #state{embed_state = EmbedState}) ->
    barrel_embed:embed_batch(Texts, EmbedState).

%% Add a document
do_add(Id, Text, Metadata, Vector, #state{db = Db, cf_vectors = CfV,
                                          cf_metadata = CfM, cf_text = CfT,
                                          index = Index, index_module = Mod,
                                          bm25_index = BM25Index,
                                          docstore = Docstore} = State) ->
    VectorBin = encode_vector(Vector),

    {ok, Batch} = rocksdb:batch(),
    ok = rocksdb:batch_put(Batch, CfV, Id, VectorBin),
    DocPairs = put_docdata_to_batch(Docstore, Batch, CfM, CfT, Id, Text, Metadata),

    %% Index updated in-memory only (rebuilt from vectors on startup)
    case Mod:insert(Index, Id, Vector) of
        {ok, NewIndex} ->
            case rocksdb:write_batch(Db, Batch, []) of
                ok ->
                    case docstore_multi_put(Docstore, DocPairs) of
                        ok ->
                            %% Also add to BM25 index if enabled
                            case bm25_add(BM25Index, Id, Text) of
                                {ok, NewBM25Index} ->
                                    {ok, State#state{index = NewIndex, bm25_index = NewBM25Index}};
                                {error, Reason} ->
                                    {error, {bm25_error, Reason}}
                            end;
                        {error, DsReason} ->
                            {error, {docstore_error, DsReason}}
                    end;
                {error, Reason} ->
                    {error, {db_error, Reason}}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% Get a document
do_get(Id, #state{db = Db, cf_vectors = CfV, cf_metadata = CfM, cf_text = CfT,
                  docstore = Docstore}) ->
    case rocksdb:get(Db, CfV, Id, []) of
        {ok, VectorBin} ->
            case docstore_get(Docstore, Db, CfM, CfT, Id) of
                {ok, Metadata, Text} ->
                    {ok, #{
                        key => Id,
                        vector => decode_vector(VectorBin),
                        metadata => Metadata,
                        text => Text
                    }};
                not_found ->
                    {error, incomplete_data};
                {error, Reason} ->
                    {error, Reason}
            end;
        not_found ->
            not_found;
        {error, Reason} ->
            {error, {db_error, Reason}}
    end.

%% Delete a document
do_delete(Id, #state{db = Db, cf_vectors = CfV, cf_metadata = CfM,
                     cf_text = CfT, cf_hnsw = CfH, index = Index, index_module = Mod,
                     bm25_index = BM25Index, docstore = Docstore} = State) ->
    {ok, Batch} = rocksdb:batch(),
    ok = rocksdb:batch_delete(Batch, CfV, Id),
    ok = delete_docdata_from_batch(Docstore, Batch, CfM, CfT, Id),
    ok = rocksdb:batch_delete(Batch, CfH, Id),

    case rocksdb:write_batch(Db, Batch, []) of
        ok ->
            case docstore_delete(Docstore, Id) of
                ok ->
                    case Mod:delete(Index, Id) of
                        {ok, NewIndex} ->
                            %% Also remove from BM25 index if enabled
                            {ok, NewBM25Index} = bm25_remove(BM25Index, Id),
                            {ok, State#state{index = NewIndex, bm25_index = NewBM25Index}};
                        {error, Reason} -> {error, Reason}
                    end;
                {error, DsReason} ->
                    {error, {docstore_error, DsReason}}
            end;
        {error, Reason} ->
            {error, {db_error, Reason}}
    end.

%% Peek at documents (sample without search)
%% Optimized: collect keys from iterator, then batch fetch metadata/text
do_peek(Limit, #state{db = Db, cf_vectors = CfV, cf_metadata = CfM, cf_text = CfT,
                      docstore = Docstore}) ->
    case rocksdb:iterator(Db, CfV, []) of
        {ok, Iter} ->
            try
                %% Phase 1: Collect keys and vectors from iterator
                KeyVectors = collect_keys(Iter, rocksdb:iterator_move(Iter, first), Limit, []),
                case KeyVectors of
                    [] ->
                        {ok, []};
                    _ ->
                        %% Phase 2: Batch fetch metadata and text (from CFs or docstore)
                        Keys = [K || {K, _} <- KeyVectors],
                        {MetaResults, TextResults} =
                            docstore_multi_fetch(Docstore, Db, CfM, CfT, Keys),
                        %% Phase 3: Combine results
                        Docs = combine_peek_results(KeyVectors, MetaResults, TextResults, []),
                        {ok, Docs}
                end
            after
                rocksdb:iterator_close(Iter)
            end;
        {error, _} ->
            {ok, []}
    end.

%% Collect keys and vector binaries from iterator
collect_keys(_Iter, _, 0, Acc) ->
    lists:reverse(Acc);
collect_keys(_Iter, {error, _}, _Limit, Acc) ->
    lists:reverse(Acc);
collect_keys(Iter, {ok, Key, VectorBin}, Limit, Acc) ->
    collect_keys(Iter, rocksdb:iterator_move(Iter, next), Limit - 1, [{Key, VectorBin} | Acc]).

%% Combine key-vectors with batched metadata/text results
combine_peek_results([], [], [], Acc) ->
    lists:reverse(Acc);
combine_peek_results([{Key, VectorBin} | KVs], [MetaResult | Ms], [TextResult | Ts], Acc) ->
    case {MetaResult, TextResult} of
        {{ok, MetadataBin}, {ok, Text}} ->
            Doc = #{
                key => Key,
                vector => decode_vector(VectorBin),
                metadata => binary_to_term(MetadataBin),
                text => Text
            },
            combine_peek_results(KVs, Ms, Ts, [Doc | Acc]);
        _ ->
            %% Skip documents with missing metadata or text
            combine_peek_results(KVs, Ms, Ts, Acc)
    end.

%% Search for similar documents
%% Options:
%%   - k: number of results (default 5)
%%   - filter: function to filter by metadata
%%   - include_text: include text in results (default true)
%%   - include_metadata: include metadata in results (default true)
do_search(QueryVector, Opts, #state{db = Db, index = Index, index_module = Mod,
                                     cf_metadata = CfM, cf_text = CfT,
                                     docstore = Docstore}) ->
    K = maps:get(k, Opts, 5),
    IndexResults = Mod:search(Index, QueryVector, K, Opts),

    case IndexResults of
        [] ->
            {ok, []};
        _ ->
            %% Extract IDs and distances
            {Ids, Distances} = lists:unzip(IndexResults),

            %% Check what data to include
            IncludeText = maps:get(include_text, Opts, true),
            IncludeMeta = maps:get(include_metadata, Opts, true),
            Filter = maps:get(filter, Opts, fun(_) -> true end),

            %% Only fetch what's needed (skip lookups if not needed and no filter)
            NeedMeta = IncludeMeta orelse Filter =/= fun(_) -> true end,

            {MetaResults, TextResults} =
                docstore_search_fetch(Docstore, Db, CfM, CfT, Ids, NeedMeta, IncludeText),

            %% Combine results
            Results = combine_search_results(Ids, Distances, MetaResults, TextResults,
                                             Filter, IncludeText, IncludeMeta),
            {ok, Results}
    end.

%% Combine HNSW results with fetched metadata/text
combine_search_results(Ids, Distances, MetaResults, TextResults, Filter, IncludeText, IncludeMeta) ->
    combine_search_results(Ids, Distances, MetaResults, TextResults,
                           Filter, IncludeText, IncludeMeta, []).

combine_search_results([], [], [], [], _Filter, _IncludeText, _IncludeMeta, Acc) ->
    lists:reverse(Acc);
combine_search_results([Id | Ids], [Distance | Distances],
                       [MetaRes | MetaResults], [TextRes | TextResults],
                       Filter, IncludeText, IncludeMeta, Acc) ->
    %% Parse metadata if available. A missing row is NOT an error: index-only
    %% entries store no metadata (their doc data lives elsewhere), so they
    %% filter and return as empty metadata. Only fetch errors drop the hit.
    Metadata = case MetaRes of
        {ok, MetadataBin} -> binary_to_term(MetadataBin);
        not_needed -> #{};
        not_found -> #{};
        _ -> undefined
    end,

    %% Parse text if available
    Text = case TextRes of
        {ok, TextBin} -> TextBin;
        not_needed -> undefined;
        _ -> undefined
    end,

    %% Check filter (skip only when metadata could not be fetched)
    PassesFilter = case Metadata of
        undefined -> false;
        _ -> Filter(Metadata)
    end,

    case PassesFilter of
        true ->
            %% Build result map with only requested fields
            Result0 = #{key => Id, score => 1.0 - Distance},
            Result1 = case IncludeText of
                true when Text =/= undefined -> Result0#{text => Text};
                _ -> Result0
            end,
            Result2 = case IncludeMeta of
                true when Metadata =/= undefined -> Result1#{metadata => Metadata};
                _ -> Result1
            end,
            combine_search_results(Ids, Distances, MetaResults, TextResults,
                                   Filter, IncludeText, IncludeMeta, [Result2 | Acc]);
        false ->
            combine_search_results(Ids, Distances, MetaResults, TextResults,
                                   Filter, IncludeText, IncludeMeta, Acc)
    end.

%%====================================================================
%% Internal Functions - Encoding
%%====================================================================

%% Use 32-bit floats for 50% storage reduction (standard for similarity search)
encode_vector(Vector) when is_list(Vector) ->
    << <<F:32/float-little>> || F <- Vector >>.

decode_vector(Binary) ->
    [F || <<F:32/float-little>> <= Binary].

%%====================================================================
%% Internal Functions - BM25
%%====================================================================

%% Initialize BM25 index based on backend type
init_bm25(_DbPath, none, _Config) ->
    {ok, undefined};
init_bm25(_DbPath, memory, Config) ->
    BM25Config = maps:get(bm25, Config, #{}),
    Index = barrel_vectordb_bm25:new(BM25Config),
    {ok, Index};
init_bm25(DbPath, disk, Config) ->
    BM25Config = maps:get(bm25_disk, Config, #{}),
    BasePath = maps:get(base_path, BM25Config, filename:join(DbPath, "bm25")),
    FinalConfig = BM25Config#{base_path => BasePath},
    %% Try to open existing index or create new
    case filelib:is_dir(BasePath) of
        true ->
            barrel_vectordb_bm25_disk:open(BasePath);
        false ->
            barrel_vectordb_bm25_disk:new(FinalConfig)
    end.

%% Add document to BM25 index
%% Extract the {Id, Text} pairs a write contributes to the BM25 index. Writes
%% reaching the atomic path are always add_vector / add_vector_batch (auto-embed
%% adds are rewritten to these in prepare_writes).
bm25_pairs_of({add_vector, Id, Text, _Metadata, _Vector}) ->
    [{Id, Text}];
bm25_pairs_of({add_vector_batch, Docs}) ->
    [{Id, Text} || {Id, Text, _Metadata, _Vector} <- Docs];
bm25_pairs_of({index_only, Id, Text, _Vector}) when Text =/= <<>> ->
    %% Text is used transiently for BM25 and then dropped (not stored).
    [{Id, Text}];
bm25_pairs_of({index_only_batch, Entries}) ->
    [{Id, Text} || {Id, Text, _Vector} <- Entries, Text =/= <<>>];
bm25_pairs_of(_) ->
    [].

%% Fold bm25_add over a list of {Id, Text} pairs, threading the index. A no-op
%% (returns the unchanged index) when BM25 is disabled.
bm25_add_all(Index, []) ->
    {ok, Index};
bm25_add_all(Index, [{Id, Text} | Rest]) ->
    case bm25_add(Index, Id, Text) of
        {ok, NewIndex} -> bm25_add_all(NewIndex, Rest);
        {error, _} = Err -> Err
    end.

bm25_add(undefined, _Id, _Text) ->
    {ok, undefined};
bm25_add(Index, Id, Text) when is_map(Index) ->
    %% In-memory BM25 (returns new index, not {ok, Index})
    NewIndex = barrel_vectordb_bm25:add(Index, Id, Text),
    {ok, NewIndex};
bm25_add(Index, Id, Text) ->
    %% Disk BM25 (returns {ok, NewIndex})
    barrel_vectordb_bm25_disk:add(Index, Id, Text).

%% Remove document from BM25 index
bm25_remove(undefined, _Id) ->
    {ok, undefined};
bm25_remove(Index, Id) when is_map(Index) ->
    %% In-memory BM25
    NewIndex = barrel_vectordb_bm25:remove(Index, Id),
    {ok, NewIndex};
bm25_remove(Index, Id) ->
    %% Disk BM25
    case barrel_vectordb_bm25_disk:remove(Index, Id) of
        {ok, NewIndex} -> {ok, NewIndex};
        {error, not_found} -> {ok, Index}
    end.

%% Search BM25 index
do_search_bm25(#state{bm25_index = undefined}, _Query, _Opts) ->
    {error, bm25_not_enabled};
do_search_bm25(#state{bm25_index = Index, bm25_backend = memory}, Query, Opts) ->
    K = maps:get(k, Opts, 10),
    Results = barrel_vectordb_bm25:search(Index, Query, K),
    {ok, Results};
do_search_bm25(#state{bm25_index = Index, bm25_backend = disk}, Query, Opts) ->
    K = maps:get(k, Opts, 10),
    Results = barrel_vectordb_bm25_disk:search(Index, Query, K),
    {ok, Results}.

%% Hybrid search combining BM25 and vector results
do_search_hybrid(#state{bm25_index = undefined}, _Query, _Opts, _State) ->
    {error, bm25_not_enabled};
do_search_hybrid(#state{} = State, Query, Opts, Vector) ->
    K = maps:get(k, Opts, 10),
    BM25Weight = maps:get(bm25_weight, Opts, 0.5),
    VectorWeight = maps:get(vector_weight, Opts, 0.5),
    Fusion = maps:get(fusion, Opts, rrf),
    RRFk = maps:get(rrf_k, Opts, 60),  %% Configurable RRF constant

    %% Get BM25 results
    {ok, BM25Results} = do_search_bm25(State, Query, Opts#{k => K * 2}),

    %% Get vector results
    {ok, VectorResults} = do_search(Vector, Opts#{k => K * 2}, State),

    %% Merge results using RRF or linear combination
    Merged = case Fusion of
        rrf ->
            rrf_merge(BM25Results, VectorResults, K, BM25Weight, VectorWeight, RRFk);
        linear ->
            linear_merge(BM25Results, VectorResults, K, BM25Weight, VectorWeight)
    end,

    {ok, Merged}.

%% Reciprocal Rank Fusion
%% RRFk is the ranking constant (default 60, higher values = less emphasis on top ranks)
rrf_merge(BM25Results, VectorResults, K, BM25Weight, VectorWeight, RRFk) ->

    %% Build rank maps
    BM25Ranks = build_rank_map([Id || {Id, _} <- BM25Results]),
    VectorRanks = build_rank_map([maps:get(key, R) || R <- VectorResults]),

    %% Get all unique IDs
    AllIds = sets:to_list(sets:union(
        sets:from_list(maps:keys(BM25Ranks)),
        sets:from_list(maps:keys(VectorRanks))
    )),

    %% Compute RRF scores
    Scores = lists:map(fun(Id) ->
        BM25Rank = maps:get(Id, BM25Ranks, 1000),
        VectorRank = maps:get(Id, VectorRanks, 1000),
        BM25RRF = BM25Weight / (RRFk + BM25Rank),
        VectorRRF = VectorWeight / (RRFk + VectorRank),
        {Id, BM25RRF + VectorRRF}
    end, AllIds),

    %% Sort and take top K
    Sorted = lists:sort(fun({_, S1}, {_, S2}) -> S1 > S2 end, Scores),
    TopK = lists:sublist(Sorted, K),

    %% Build result maps (without metadata for now)
    [#{key => Id, score => Score} || {Id, Score} <- TopK].

%% Linear combination of scores
linear_merge(BM25Results, VectorResults, K, BM25Weight, VectorWeight) ->
    %% Normalize BM25 scores
    BM25Max = case BM25Results of
        [] -> 1.0;
        _ -> lists:max([S || {_, S} <- BM25Results])
    end,
    BM25Normalized = [{Id, S / max(BM25Max, 0.001)} || {Id, S} <- BM25Results],

    %% Get vector scores (already normalized 0-1)
    VectorScores = [{maps:get(key, R), maps:get(score, R)} || R <- VectorResults],

    %% Build score maps
    BM25Map = maps:from_list(BM25Normalized),
    VectorMap = maps:from_list(VectorScores),

    %% Get all unique IDs
    AllIds = sets:to_list(sets:union(
        sets:from_list(maps:keys(BM25Map)),
        sets:from_list(maps:keys(VectorMap))
    )),

    %% Compute combined scores
    Scores = lists:map(fun(Id) ->
        BM25Score = maps:get(Id, BM25Map, 0.0),
        VectorScore = maps:get(Id, VectorMap, 0.0),
        {Id, BM25Weight * BM25Score + VectorWeight * VectorScore}
    end, AllIds),

    %% Sort and take top K
    Sorted = lists:sort(fun({_, S1}, {_, S2}) -> S1 > S2 end, Scores),
    TopK = lists:sublist(Sorted, K),

    [#{key => Id, score => Score} || {Id, Score} <- TopK].

%% Build rank map from ordered list
build_rank_map(Ids) ->
    {Map, _} = lists:foldl(fun(Id, {Acc, Rank}) ->
        {Acc#{Id => Rank}, Rank + 1}
    end, {#{}, 1}, Ids),
    Map.

%% Get BM25 info
do_bm25_info(#state{bm25_index = undefined}) ->
    {error, bm25_not_enabled};
do_bm25_info(#state{bm25_index = Index, bm25_backend = memory}) ->
    Stats = barrel_vectordb_bm25:stats(Index),
    {ok, Stats#{backend => memory}};
do_bm25_info(#state{bm25_index = Index, bm25_backend = disk}) ->
    Info = barrel_vectordb_bm25_disk:info(Index),
    {ok, Info#{backend => disk}}.

%% Compact BM25 index (disk backend only)
do_bm25_compact(#state{bm25_index = undefined}) ->
    {error, bm25_not_enabled};
do_bm25_compact(#state{bm25_backend = memory}) ->
    {error, memory_backend_no_compaction};
do_bm25_compact(#state{bm25_index = Index, bm25_backend = disk} = State) ->
    case barrel_vectordb_bm25_disk:compact(Index) of
        {ok, NewIndex} ->
            {ok, State#state{bm25_index = NewIndex}};
        {error, _} = Error ->
            Error
    end.
