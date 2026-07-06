%%%-------------------------------------------------------------------
%%% @doc DiskANN Vamana Graph Implementation
%%%
%%% Implements the Vamana graph algorithm from the DiskANN paper with:
%%% - Two-pass construction (alpha=1.0 then alpha>1.0)
%%% - RobustPrune for alpha-RNG pruning
%%% - GreedySearch for graph traversal
%%% - FreshVamana insert/delete for streaming updates
%%% - Consolidate deletes for batch cleanup
%%% - BeamSearch with PQ for SSD-resident search
%%%
%%% The alpha parameter (>1) is critical for maintaining graph quality
%%% under streaming updates. It keeps more long-range edges which
%%% are essential for fast convergence during search.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_diskann).

-include("barrel_vectordb.hrl").

%% API
-export([
    new/1,
    build/2,
    insert/3,
    insert_batch/3,
    delete/2,
    search/3,
    search/4,
    size/1,
    info/1,
    get_vector/2,
    consolidate_deletes/1,
    needs_consolidation/1,
    %% Hot layer API
    compact/1,
    %% Persistence API
    open/1,
    open/2,
    close/1,
    sync/1,
    %% Serialization (for barrel_vectordb_index behaviour)
    serialize/1,
    deserialize/1
]).

%% Internal exports for testing
-export([
    greedy_search/5,
    robust_prune/5,
    find_medoid/2,
    close_all_standalone_dbs/0
]).

-record(diskann_config, {
    r = 64 :: pos_integer(),              %% Max out-degree
    l_build = 100 :: pos_integer(),       %% Build search width
    l_search = 100 :: pos_integer(),      %% Query search width
    alpha = 1.2 :: float(),               %% Pruning factor (>1 for long-range)
    dimension :: pos_integer(),
    distance_fn = cosine :: cosine | euclidean,
    assume_normalized = false :: boolean() %% Skip norm computation for normalized vectors
}).

-record(diskann_index, {
    config :: #diskann_config{},
    size = 0 :: non_neg_integer(),
    medoid_id :: binary() | undefined,    %% Entry point (string ID, for memory mode/API)
    medoid_int_id :: non_neg_integer() | undefined,  %% Entry point (integer ID, for disk mode)

    %% In RAM (small footprint) - graph structure (memory mode only)
    %% In disk mode, nodes are lazily loaded from disk into graph_cache_table
    nodes = #{} :: #{binary() => diskann_node()},

    %% Integer ID graph structure (disk mode) - maps IntId -> [NeighborIntIds]
    %% Only populated in memory mode; disk mode uses lazy loading
    nodes_int = #{} :: #{non_neg_integer() => [non_neg_integer()]},

    %% ID <-> disk index mapping (for disk mode, fallback when no RocksDB)
    id_to_idx = #{} :: #{binary() => non_neg_integer()},
    idx_to_id = #{} :: #{non_neg_integer() => binary()},

    %% RocksDB for ID mapping (disk mode only) - replaces maps for large indexes
    id_db :: rocksdb:db_handle() | undefined,
    id_cf_fwd :: rocksdb:cf_handle() | undefined,  %% string -> int
    id_cf_rev :: rocksdb:cf_handle() | undefined,  %% int -> string
    id_db_standalone = false :: boolean(),  %% true if we opened standalone DB (not from store)
    %% EncryptedEnv used by the ids RocksDB; kept referenced here (the
    %% NIF frees the env when the handle is garbage collected)
    id_db_env :: term() | undefined,
    next_int_id = 0 :: non_neg_integer(),

    %% Quantization settings
    quantization_method = none :: pq | turboquant | subspace_turboquant | none,

    %% PQ compression (always in RAM)
    pq_state :: term() | undefined,       %% Trained PQ for compression
    pq_codes = #{} :: #{binary() => binary()},  %% Id -> PQ code (M bytes)
    pq_codes_int = #{} :: #{non_neg_integer() => binary()},  %% IntId -> PQ code
    use_pq = false :: boolean(),          %% Whether to use PQ for search (legacy, use quantization_method)

    %% TurboQuant compression (always in RAM, no training required)
    tq_state :: term() | undefined,       %% TurboQuant config
    tq_codes = #{} :: #{binary() => binary()},  %% Id -> TQ code
    tq_codes_int = #{} :: #{non_neg_integer() => binary()},  %% IntId -> TQ code

    %% Deletion tracking
    deleted_set = sets:new() :: sets:set(binary()),
    deleted_int_set = sets:new() :: sets:set(non_neg_integer()),

    %% Storage mode: memory | disk
    storage_mode = memory :: memory | disk,

    %% In-memory vectors (used only in memory mode)
    vectors = #{} :: #{binary() => [float()]},

    %% Disk file handle and path (used only in disk mode)
    file_handle :: term() | undefined,
    base_path :: binary() | undefined,

    %% ETS-based vector cache for disk mode (persists across calls)
    cache_table :: ets:tid() | undefined,
    cache_max_size = 10000 :: pos_integer(),

    %% ETS-based graph cache for disk mode (lazy loading of neighbor lists)
    graph_cache_table :: ets:tid() | undefined,
    graph_cache_max_size = 100000 :: pos_integer(),

    %% Hot layer configuration (for absorbing writes with sub-ms latency)
    hot_enabled = false :: boolean(),
    hot_max_size = 10000 :: non_neg_integer(),
    hot_compaction_threshold = 0.8 :: float(),

    %% Hot layer data (pure in-memory, survives no crash)
    hot_vectors = #{} :: #{non_neg_integer() => [float()]},
    hot_graph = #{} :: #{non_neg_integer() => [non_neg_integer()]},
    hot_id_to_int = #{} :: #{binary() => non_neg_integer()},
    hot_int_to_id = #{} :: #{non_neg_integer() => binary()},
    hot_next_int_id = 0 :: non_neg_integer(),
    hot_deleted = sets:new() :: sets:set(non_neg_integer()),
    hot_pq_codes = #{} :: #{non_neg_integer() => binary()},
    hot_tq_codes = #{} :: #{non_neg_integer() => binary()},  %% Hot layer TQ codes
    hot_size = 0 :: non_neg_integer(),
    compaction_in_progress = false :: boolean()
}).

-record(diskann_node, {
    id :: binary(),
    neighbors = [] :: [binary()]
}).

-type diskann_index() :: #diskann_index{}.
-type diskann_node() :: #diskann_node{}.
-type diskann_config() :: #diskann_config{}.

-export_type([diskann_index/0, diskann_config/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Create a new empty DiskANN index
%% Options:
%%   - dimension: Vector dimension (required)
%%   - r: Max out-degree (default: 64)
%%   - l_build: Build search width (default: 100)
%%   - l_search: Query search width (default: 100)
%%   - alpha: Pruning factor (default: 1.2)
%%   - distance_fn: cosine | euclidean (default: cosine)
%%   - assume_normalized: Skip norm computation for cosine (default: false)
%%   - storage_mode: memory | disk (default: memory)
%%   - base_path: Path for disk storage (required if storage_mode=disk)
%%   - cache_max_size: Max vectors in LRU cache (default: 10000)
%%   - hot_layer: Enable hot memory layer for fast writes (default: false)
%%   - hot_max_size: Maximum vectors in hot layer before compaction (default: 10000)
%%   - hot_compaction_threshold: Trigger compaction at this % of max (default: 0.8)
%%   - quantization: none | pq | turboquant | subspace_turboquant (default: none)
%%   - tq_bits: TurboQuant bits per component (default: 3, when quantization=turboquant or subspace_turboquant)
%%   - tq_seed: TurboQuant random seed (default: 42)
%%   - tq_m: Number of subspaces for subspace_turboquant (default: auto, based on dimension)
-spec new(map()) -> {ok, diskann_index()} | {error, term()}.
new(Options) ->
    R = maps:get(r, Options, 64),
    LBuild = maps:get(l_build, Options, 100),
    LSearch = maps:get(l_search, Options, 100),
    Alpha = maps:get(alpha, Options, 1.2),
    Dimension = maps:get(dimension, Options, undefined),
    DistanceFn = maps:get(distance_fn, Options, cosine),
    AssumeNormalized = maps:get(assume_normalized, Options, false),
    StorageMode = maps:get(storage_mode, Options, memory),
    BasePath = maps:get(base_path, Options, undefined),
    CacheMaxSize = maps:get(cache_max_size, Options, 10000),
    %% Hot layer options
    HotEnabled = maps:get(hot_layer, Options, false),
    HotMaxSize = maps:get(hot_max_size, Options, 10000),
    HotCompactionThreshold = maps:get(hot_compaction_threshold, Options, 0.8),
    %% Quantization options
    QuantizationMethod = maps:get(quantization, Options, none),

    case Dimension of
        undefined ->
            {error, dimension_required};
        _ when Dimension > 0 ->
            case validate_storage_options(StorageMode, BasePath) of
                ok ->
                    %% Initialize TurboQuant or Subspace-TurboQuant if requested
                    TQResult = case QuantizationMethod of
                        turboquant ->
                            TQBits = maps:get(tq_bits, Options, 3),
                            TQSeed = maps:get(tq_seed, Options, 42),
                            barrel_vectordb_turboquant:new(#{
                                bits => TQBits,
                                dimension => Dimension,
                                seed => TQSeed
                            });
                        subspace_turboquant ->
                            TQBits = maps:get(tq_bits, Options, 3),
                            TQSeed = maps:get(tq_seed, Options, 42),
                            TQM = maps:get(tq_m, Options, barrel_vectordb_turboquant_subspace:select_m(Dimension)),
                            barrel_vectordb_turboquant_subspace:new(#{
                                bits => TQBits,
                                dimension => Dimension,
                                seed => TQSeed,
                                m => TQM
                            });
                        _ ->
                            {ok, undefined}
                    end,
                    case TQResult of
                        {ok, TQState} ->
                            Config = #diskann_config{
                                r = R,
                                l_build = LBuild,
                                l_search = LSearch,
                                alpha = Alpha,
                                dimension = Dimension,
                                distance_fn = DistanceFn,
                                assume_normalized = AssumeNormalized
                            },
                            HotOpts = #{
                                enabled => HotEnabled,
                                max_size => HotMaxSize,
                                compaction_threshold => HotCompactionThreshold
                            },
                            case StorageMode of
                                memory ->
                                    {ok, #diskann_index{
                                        config = Config,
                                        storage_mode = memory,
                                        base_path = undefined,
                                        cache_max_size = CacheMaxSize,
                                        cache_table = undefined,
                                        quantization_method = QuantizationMethod,
                                        tq_state = TQState
                                    }};
                                disk ->
                                    %% V2 format: Set up RocksDB and disk files
                                    new_disk_mode(Config, BasePath, CacheMaxSize, HotOpts, QuantizationMethod, TQState,
                                                  maps:get(crypto, Options, none))
                            end;
                        {error, TQError} ->
                            {error, {quantization_init_failed, QuantizationMethod, TQError}}
                    end;
                {error, _} = Error ->
                    Error
            end;
        _ ->
            {error, {invalid_dimension, Dimension}}
    end.

validate_storage_options(disk, undefined) ->
    {error, {disk_mode_requires_base_path}};
validate_storage_options(_, _) ->
    ok.

%% Create new disk mode index (V2 format with RocksDB)
new_disk_mode(Config, BasePath, CacheMaxSize, HotOpts, QuantizationMethod, TQState, Crypto) ->
    BasePathBin = to_binary_or_undefined(BasePath),
    %% Extract hot layer options
    HotEnabled = maps:get(enabled, HotOpts, false),
    HotMaxSize = maps:get(max_size, HotOpts, 10000),
    HotCompactionThreshold = maps:get(compaction_threshold, HotOpts, 0.8),
    %% Open RocksDB for ID mapping
    case open_id_db(BasePathBin, Crypto) of
        {ok, IdDb, CfFwd, CfRev, Standalone, IdEnv} ->
            %% Create disk files
            FileConfig = #{
                dimension => Config#diskann_config.dimension,
                r => Config#diskann_config.r,
                distance_fn => Config#diskann_config.distance_fn,
                crypto => file_crypto(Crypto)
            },
            case barrel_vectordb_diskann_file:create(BasePathBin, FileConfig) of
                {ok, FileHandle} ->
                    %% Create ETS caches
                    CacheTable = create_cache_table(),
                    GraphCacheTable = create_graph_cache_table(),
                    {ok, #diskann_index{
                        config = Config,
                        storage_mode = disk,
                        base_path = BasePathBin,
                        id_db_standalone = Standalone,
                        id_db_env = IdEnv,
                        cache_max_size = CacheMaxSize,
                        cache_table = CacheTable,
                        graph_cache_table = GraphCacheTable,
                        file_handle = FileHandle,
                        id_db = IdDb,
                        id_cf_fwd = CfFwd,
                        id_cf_rev = CfRev,
                        next_int_id = 0,
                        %% Hot layer configuration
                        hot_enabled = HotEnabled,
                        hot_max_size = HotMaxSize,
                        hot_compaction_threshold = HotCompactionThreshold,
                        %% Quantization configuration
                        quantization_method = QuantizationMethod,
                        tq_state = TQState
                    }};
                {error, Reason} ->
                    close_id_db(IdDb, Standalone),
                    {error, {disk_file_create_failed, Reason}}
            end;
        {error, Reason} ->
            {error, {id_db_open_failed, Reason}}
    end.

to_binary_or_undefined(undefined) -> undefined;
to_binary_or_undefined(Path) when is_list(Path) -> list_to_binary(Path);
to_binary_or_undefined(Path) when is_binary(Path) -> Path.

%% The flat-file layer needs only the key (nonces live in its meta
%% superblock); the ids RocksDB additionally uses the env.
file_crypto(none) -> none;
file_crypto(#{key := Key}) -> #{key => Key}.

file_handle_crypto(undefined) -> none;
file_handle_crypto(FileHandle) ->
    barrel_vectordb_diskann_file:get_crypto(FileHandle).

%% @doc Build index from a list of {Id, Vector} pairs using two-pass Vamana
-spec build(map(), [{binary(), [float()]}]) -> {ok, diskann_index()} | {error, term()}.
build(Options, Vectors) when length(Vectors) > 0 ->
    case new(Options) of
        {ok, Index0} ->
            case Index0#diskann_index.storage_mode of
                memory ->
                    build_memory_mode(Index0, Options, Vectors);
                disk ->
                    build_disk_mode(Index0, Options, Vectors)
            end;
        {error, _} = Error ->
            Error
    end;
build(_, []) ->
    {error, empty_vectors}.

%% Build index in memory mode (original behavior)
build_memory_mode(Index0, Options, Vectors) ->
    %% Store all vectors in memory
    VectorMap = maps:from_list(Vectors),
    Index1 = Index0#diskann_index{
        vectors = VectorMap,
        size = length(Vectors)
    },

    %% Build id to idx mappings (for consistency with disk mode)
    {IdToIdx, IdxToId} = build_id_mappings(Vectors),
    Index1b = Index1#diskann_index{
        id_to_idx = IdToIdx,
        idx_to_id = IdxToId
    },

    %% Find medoid (centroid) as entry point
    MedoidId = find_medoid(Vectors, Index1b#diskann_index.config),
    Index2 = Index1b#diskann_index{medoid_id = MedoidId},

    %% Initialize random graph
    Index3 = init_random_graph(Index2, maps:keys(VectorMap)),

    %% Two-pass Vamana construction
    Config = Index3#diskann_index.config,
    R = Config#diskann_config.r,
    L = Config#diskann_config.l_build,
    Alpha = Config#diskann_config.alpha,

    %% Pass 1: alpha = 1.0 (finds good short edges)
    Index4 = vamana_pass(Index3, 1.0, L, R),

    %% Pass 2: alpha > 1.0 (adds long-range edges for fast convergence)
    Index5 = vamana_pass(Index4, Alpha, L, R),

    %% Train and apply PQ if enabled and enough vectors
    UsePQ = maps:get(use_pq, Options, false),
    PQK = maps:get(pq_k, Options, 256),
    Index6 = case UsePQ andalso length(Vectors) >= PQK of
        true ->
            train_and_apply_pq(Index5, Options, Vectors);
        false ->
            Index5
    end,

    {ok, Index6}.

%% Build index in disk mode (vectors stored on SSD)
%% Uses integer IDs internally and RocksDB for ID mapping
%% Note: Index0 already has RocksDB handles and file_handle from new_disk_mode
build_disk_mode(Index0, Options, Vectors) ->
    %% RocksDB handles are already in Index0 from new_disk_mode
    %% file_handle is also already created from new_disk_mode
    FileHandle0 = Index0#diskann_index.file_handle,
    build_disk_mode_internal(Index0, Options, Vectors, FileHandle0).

build_disk_mode_internal(Index0, Options, Vectors, FileHandle0) ->
    Config = Index0#diskann_index.config,
    Dimension = Config#diskann_config.dimension,

    %% Assign integer IDs to all vectors and write vectors to disk
    {Index1, FileHandle1, IntIdVectors} = lists:foldl(
        fun({StringId, Vec}, {AccIndex, AccFH, AccIntVecs}) ->
            {IntId, NewIndex} = get_or_create_int_id(AccIndex, StringId),
            ok = barrel_vectordb_diskann_file:write_vector(AccFH, IntId, Vec),
            {NewIndex, AccFH, [{IntId, Vec, StringId} | AccIntVecs]}
        end,
        {Index0, FileHandle0, []},
        Vectors
    ),
    IntIdVectors2 = lists:reverse(IntIdVectors),

    %% Create graph cache table
    GraphCacheTable = create_graph_cache_table(),

    %% Update header with initial info
    {ok, FileHandle2} = barrel_vectordb_diskann_file:write_header(
        FileHandle1,
        #{
            dimension => Dimension,
            r => Config#diskann_config.r,
            node_count => length(Vectors),
            distance_fn => Config#diskann_config.distance_fn,
            entry_point => undefined,
            entry_point_int => 0,
            next_int_id => Index1#diskann_index.next_int_id
        }
    ),

    %% During build, we need vectors in cache for distance calculations
    %% For disk mode build, use the vector cache keyed by integer ID
    CacheTable = Index0#diskann_index.cache_table,
    populate_cache_for_build_int(CacheTable, IntIdVectors2),

    Index2 = Index1#diskann_index{
        file_handle = FileHandle2,
        graph_cache_table = GraphCacheTable,
        size = length(Vectors)
    },

    %% Find medoid using integer IDs
    VectorsForMedoid = [{StringId, Vec} || {_IntId, Vec, StringId} <- IntIdVectors2],
    MedoidStringId = find_medoid(VectorsForMedoid, Config),
    {ok, MedoidIntId} = string_id_to_int(Index2, MedoidStringId),
    Index3 = Index2#diskann_index{medoid_id = MedoidStringId, medoid_int_id = MedoidIntId},

    %% Initialize random graph using integer IDs
    IntIds = [IntId || {IntId, _, _} <- IntIdVectors2],
    Index4 = init_random_graph_int(Index3, IntIds),

    %% Two-pass Vamana construction using integer IDs
    R = Config#diskann_config.r,
    L = Config#diskann_config.l_build,
    Alpha = Config#diskann_config.alpha,

    Index5 = vamana_pass_int(Index4, 1.0, L, R),
    Index6 = vamana_pass_int(Index5, Alpha, L, R),

    %% Write graph nodes to disk
    Index7 = write_graph_nodes_to_disk(Index6, IntIds),

    %% Train and apply PQ (required for efficient disk-based search)
    UsePQ = maps:get(use_pq, Options, true),
    PQK = maps:get(pq_k, Options, 256),
    Index8 = case UsePQ andalso length(Vectors) >= min(PQK, 16) of
        true ->
            train_and_apply_pq_int(Index7, Options, IntIdVectors2);
        false ->
            Index7
    end,

    %% Update header with medoid
    {ok, FileHandle3} = barrel_vectordb_diskann_file:write_header(
        Index8#diskann_index.file_handle,
        #{
            dimension => Dimension,
            r => Config#diskann_config.r,
            node_count => length(Vectors),
            distance_fn => Config#diskann_config.distance_fn,
            entry_point => MedoidStringId,
            entry_point_int => MedoidIntId,
            next_int_id => Index8#diskann_index.next_int_id
        }
    ),

    %% Clear the build cache, keep only limited cache for search
    CacheMaxSize = Index8#diskann_index.cache_max_size,
    clear_cache_table(Index8#diskann_index.cache_table),
    %% Clear in-memory graph (it's now on disk)
    clear_graph_cache(Index8#diskann_index.graph_cache_table),

    Index9 = Index8#diskann_index{
        file_handle = FileHandle3,
        vectors = #{},  %% Clear vectors from memory
        nodes = #{},    %% Clear string ID graph
        nodes_int = #{} %% Clear in-memory integer graph (now on disk)
    },

    %% Pre-warm caches with medoid neighbors
    prewarm_caches(Index9, MedoidIntId, CacheMaxSize),

    {ok, Index9}.

%% Build id <-> idx mappings from vector list (for memory mode)
build_id_mappings(Vectors) ->
    {IdToIdx, IdxToId, _} = lists:foldl(
        fun({Id, _Vec}, {AccIdToIdx, AccIdxToId, Idx}) ->
            {AccIdToIdx#{Id => Idx}, AccIdxToId#{Idx => Id}, Idx + 1}
        end,
        {#{}, #{}, 0},
        Vectors
    ),
    {IdToIdx, IdxToId}.

%% @doc Insert a new vector (FreshVamana algorithm)
-spec insert(diskann_index(), binary(), [float()]) -> {ok, diskann_index()} | {error, term()}.
insert(#diskann_index{medoid_id = undefined, config = Config,
                      storage_mode = memory} = Index, Id, Vector) ->
    %% First insertion - memory mode
    Dim = Config#diskann_config.dimension,
    case length(Vector) of
        Dim ->
            NewIndex = Index#diskann_index{
                medoid_id = Id,
                nodes = #{Id => #diskann_node{id = Id, neighbors = []}},
                vectors = #{Id => Vector},
                id_to_idx = #{Id => 0},
                idx_to_id = #{0 => Id},
                size = 1
            },
            {ok, NewIndex};
        Other ->
            {error, {dimension_mismatch, Dim, Other}}
    end;
insert(#diskann_index{hot_enabled = true, storage_mode = disk} = Index, Id, Vector) ->
    %% Hot layer enabled - insert to hot layer for sub-millisecond writes
    %% This clause handles both first and subsequent insertions for hot layer
    case hot_insert(Index, Id, Vector) of
        {ok, NewIndex} ->
            {ok, NewIndex};
        {compact_needed, NewIndex} ->
            %% Spawn background compaction
            Self = self(),
            spawn(fun() ->
                {ok, CompactedIndex} = compact_hot_to_disk(NewIndex),
                Self ! {hot_compaction_done, CompactedIndex}
            end),
            {ok, NewIndex};
        {error, _} = Error ->
            Error
    end;
insert(#diskann_index{medoid_id = undefined, medoid_int_id = undefined, config = Config,
                      storage_mode = disk} = Index, Id, Vector) ->
    %% First insertion - disk mode V2 (with RocksDB) without hot layer
    Dim = Config#diskann_config.dimension,
    R = Config#diskann_config.r,
    case length(Vector) of
        Dim ->
            %% Get integer ID for this string ID
            {IntId, Index1} = get_or_create_int_id(Index, Id),
            %% Write vector to disk
            ok = barrel_vectordb_diskann_file:write_vector(
                Index1#diskann_index.file_handle, IntId, Vector),
            %% Write node to disk (empty neighbors)
            ok = barrel_vectordb_diskann_file:write_node_int(
                Index1#diskann_index.file_handle, IntId, [], R),
            %% Add to ETS cache (keyed by integer ID)
            cache_put_with_eviction_by_key(Index1#diskann_index.cache_table, IntId, Vector,
                                           Index1#diskann_index.cache_max_size),
            %% Update graph cache
            graph_cache_put_with_eviction(Index1#diskann_index.graph_cache_table, IntId, [],
                                          Index1#diskann_index.graph_cache_max_size),
            NewIndex = Index1#diskann_index{
                medoid_id = Id,
                medoid_int_id = IntId,
                nodes_int = #{IntId => []},
                size = 1
            },
            {ok, NewIndex};
        Other ->
            {error, {dimension_mismatch, Dim, Other}}
    end;
insert(#diskann_index{config = Config, medoid_id = S, use_pq = UsePQ,
                      pq_state = PQState, pq_codes = PQCodes,
                      storage_mode = memory} = Index, Id, Vector) ->
    %% Subsequent insertions - memory mode
    Dim = Config#diskann_config.dimension,
    case length(Vector) of
        Dim ->
            #diskann_config{l_build = L, alpha = Alpha, r = R} = Config,

            %% Encode with PQ if enabled
            NewPQCodes = case UsePQ andalso PQState =/= undefined of
                true ->
                    Code = barrel_vectordb_pq:encode(PQState, Vector),
                    maps:put(Id, Code, PQCodes);
                false ->
                    PQCodes
            end,

            Index1 = Index#diskann_index{
                vectors = maps:put(Id, Vector, Index#diskann_index.vectors),
                nodes = maps:put(Id, #diskann_node{id = Id, neighbors = []},
                                 Index#diskann_index.nodes),
                id_to_idx = maps:put(Id, Index#diskann_index.size,
                                     Index#diskann_index.id_to_idx),
                idx_to_id = maps:put(Index#diskann_index.size, Id,
                                     Index#diskann_index.idx_to_id),
                pq_codes = NewPQCodes,
                size = Index#diskann_index.size + 1
            },

            %% Search to find candidate neighbors
            {_Results, Visited} = greedy_search(Index1, S, Vector, 1, L),

            %% Prune to select R out-neighbors
            Index2 = robust_prune(Index1, Id, sets:to_list(Visited), Alpha, R),
            Neighbors = get_neighbors(Index2, Id),

            %% Add backward edges
            Index3 = lists:foldl(
                fun(J, AccIndex) ->
                    JNeighbors = get_neighbors(AccIndex, J),
                    case length(JNeighbors) + 1 > R of
                        true ->
                            robust_prune(AccIndex, J, [Id | JNeighbors], Alpha, R);
                        false ->
                            add_neighbor(AccIndex, J, Id)
                    end
                end,
                Index2,
                Neighbors
            ),
            {ok, Index3};
        Other ->
            {error, {dimension_mismatch, Dim, Other}}
    end;
insert(#diskann_index{config = Config, medoid_int_id = MedoidIntId,
                      use_pq = UsePQ, pq_state = PQState, pq_codes_int = PQCodesInt,
                      storage_mode = disk} = Index, Id, Vector) ->
    %% Subsequent insertions - disk mode V2 (with integer IDs) without hot layer
    Dim = Config#diskann_config.dimension,
    case length(Vector) of
        Dim ->
            #diskann_config{l_build = L, alpha = Alpha, r = R} = Config,

            %% Get integer ID for this string ID
            {IntId, Index0} = get_or_create_int_id(Index, Id),

            %% Encode with PQ if enabled
            NewPQCodesInt = case UsePQ andalso PQState =/= undefined of
                true ->
                    Code = barrel_vectordb_pq:encode(PQState, Vector),
                    maps:put(IntId, Code, PQCodesInt);
                false ->
                    PQCodesInt
            end,

            %% Write vector to disk
            ok = barrel_vectordb_diskann_file:write_vector(
                Index0#diskann_index.file_handle, IntId, Vector),

            %% Add to ETS cache (keyed by integer ID)
            cache_put_with_eviction_by_key(Index0#diskann_index.cache_table, IntId, Vector,
                                           Index0#diskann_index.cache_max_size),

            %% Initialize node with empty neighbors in cache
            graph_cache_put_with_eviction(Index0#diskann_index.graph_cache_table, IntId, [],
                                          Index0#diskann_index.graph_cache_max_size),

            Index1 = Index0#diskann_index{
                nodes_int = maps:put(IntId, [], Index0#diskann_index.nodes_int),
                pq_codes_int = NewPQCodesInt,
                size = Index0#diskann_index.size + 1
            },

            %% Search to find candidate neighbors using integer IDs
            {_Results, Visited} = greedy_search_int(Index1, MedoidIntId, Vector, 1, L),

            %% Prune to select R out-neighbors
            Index2 = robust_prune_int(Index1, IntId, sets:to_list(Visited), Alpha, R),
            Neighbors = get_neighbors_int(Index2, IntId),

            %% Add backward edges
            Index3 = lists:foldl(
                fun(J, AccIndex) ->
                    JNeighbors = get_neighbors_int(AccIndex, J),
                    case length(JNeighbors) + 1 > R of
                        true ->
                            robust_prune_int(AccIndex, J, [IntId | JNeighbors], Alpha, R);
                        false ->
                            add_neighbor_int(AccIndex, J, IntId)
                    end
                end,
                Index2,
                Neighbors
            ),

            %% Write updated node to disk
            ok = barrel_vectordb_diskann_file:write_node_int(
                Index3#diskann_index.file_handle, IntId, get_neighbors_int(Index3, IntId), R),

            {ok, Index3};
        Other ->
            {error, {dimension_mismatch, Dim, Other}}
    end.

%% @doc Batch insert multiple vectors efficiently
%% Inserts all vectors first, then builds graph edges in one pass.
%% Much more efficient than calling insert/3 repeatedly.
-spec insert_batch(diskann_index(), [{binary(), [float()]}], map()) ->
    {ok, diskann_index()} | {error, term()}.
insert_batch(Index, [], _Opts) ->
    {ok, Index};
insert_batch(Index, Vectors, Opts) ->
    %% Validate dimensions first
    Dim = (Index#diskann_index.config)#diskann_config.dimension,
    case validate_batch_dimensions(Vectors, Dim) of
        ok ->
            insert_batch_validated(Index, Vectors, Opts);
        {error, _} = Err ->
            Err
    end.

validate_batch_dimensions([], _Dim) -> ok;
validate_batch_dimensions([{_Id, Vec} | Rest], Dim) ->
    case length(Vec) of
        Dim -> validate_batch_dimensions(Rest, Dim);
        Other -> {error, {dimension_mismatch, Dim, Other}}
    end.

insert_batch_validated(#diskann_index{storage_mode = disk} = Index, Vectors, Opts) ->
    insert_batch_disk(Index, Vectors, Opts);
insert_batch_validated(#diskann_index{storage_mode = memory} = Index, Vectors, Opts) ->
    insert_batch_memory(Index, Vectors, Opts).

%% Batch insert for disk mode - optimized for throughput
%% Uses Vamana-style construction for efficient bulk insertion
insert_batch_disk(Index0, Vectors, _Opts) ->
    Config = Index0#diskann_index.config,
    #diskann_config{l_build = L, alpha = Alpha, r = R} = Config,

    %% Phase 1: Assign integer IDs and write vectors to disk
    %% Also build an in-memory vector map for fast access during graph building
    {IntIdVecs, VectorMap, Index1} = lists:foldl(
        fun({Id, Vec}, {AccIntIdVecs, AccVecMap, AccIdx}) ->
            {IntId, Idx1} = get_or_create_int_id(AccIdx, Id),
            %% Write vector to disk
            ok = barrel_vectordb_diskann_file:write_vector(
                Idx1#diskann_index.file_handle, IntId, Vec),
            %% Build in-memory map for fast access
            NewVecMap = maps:put(IntId, Vec, AccVecMap),
            %% Cache in ETS too
            cache_put_with_eviction_by_key(Idx1#diskann_index.cache_table, IntId, Vec,
                                           Idx1#diskann_index.cache_max_size),
            {[{IntId, Vec} | AccIntIdVecs], NewVecMap, Idx1}
        end,
        {[], #{}, Index0},
        Vectors
    ),
    IntIdVecsRev = lists:reverse(IntIdVecs),
    NewIntIds = [IntId || {IntId, _} <- IntIdVecsRev],

    %% Handle empty index - set first as medoid
    {Index2, MedoidIntId} = case Index1#diskann_index.medoid_int_id of
        undefined ->
            FirstIntId = hd(NewIntIds),
            {ok, FirstStringId} = int_id_to_string(Index1, FirstIntId),
            {Index1#diskann_index{
                medoid_id = FirstStringId,
                medoid_int_id = FirstIntId
            }, FirstIntId};
        ExistingMedoid ->
            {Index1, ExistingMedoid}
    end,

    %% Phase 2: Initialize random graph for new nodes
    %% This gives each new node R random neighbors from ALL nodes (existing + new)
    AllIntIds = maps:keys(Index2#diskann_index.nodes_int) ++ NewIntIds,
    AllIntIdsUnique = lists:usort(AllIntIds),
    Index3 = init_random_edges_for_new_nodes(Index2, NewIntIds, AllIntIdsUnique, R),

    %% Write initial random edges to disk
    lists:foreach(fun(IntId) ->
        Neighbors = maps:get(IntId, Index3#diskann_index.nodes_int, []),
        ok = barrel_vectordb_diskann_file:write_node_int(
            Index3#diskann_index.file_handle, IntId, Neighbors, R)
    end, NewIntIds),

    %% Phase 2.5: Pre-warm cache with existing nodes' vectors before Vamana pass
    %% This reduces disk I/O during the hot path by pre-loading likely-accessed vectors
    VectorMap2 = case Index2#diskann_index.size of
        0 -> VectorMap;  %% Empty index, nothing to pre-warm
        _ -> prewarm_cache_for_batch(Index3, MedoidIntId, VectorMap)
    end,

    %% Phase 3: Refine graph using Vamana pass on new nodes only
    %% Use the in-memory vector map for fast distance computation
    %% Use deferred backward edges for better performance (FreshDiskANN technique)
    %% Returns updated index and list of existing nodes whose neighbor lists were modified
    {Index4, ModifiedExisting} = vamana_pass_batch_deferred(Index3, NewIntIds, VectorMap2, MedoidIntId, Alpha, L, R),

    %% Write final edges to disk for new nodes
    lists:foreach(fun(IntId) ->
        Neighbors = maps:get(IntId, Index4#diskann_index.nodes_int, []),
        ok = barrel_vectordb_diskann_file:write_node_int(
            Index4#diskann_index.file_handle, IntId, Neighbors, R),
        %% Update graph cache
        graph_cache_put_with_eviction(Index4#diskann_index.graph_cache_table, IntId, Neighbors,
                                      Index4#diskann_index.graph_cache_max_size)
    end, NewIntIds),

    %% Also write updated edges for existing nodes that received backward edges
    lists:foreach(fun(IntId) ->
        Neighbors = maps:get(IntId, Index4#diskann_index.nodes_int, []),
        ok = barrel_vectordb_diskann_file:write_node_int(
            Index4#diskann_index.file_handle, IntId, Neighbors, R),
        %% Update graph cache
        graph_cache_put_with_eviction(Index4#diskann_index.graph_cache_table, IntId, Neighbors,
                                      Index4#diskann_index.graph_cache_max_size)
    end, ModifiedExisting),

    %% Update size
    NewSize = Index4#diskann_index.size + length(NewIntIds),
    {ok, Index4#diskann_index{size = NewSize}}.

%% Initialize random edges for new nodes
init_random_edges_for_new_nodes(Index, NewIntIds, AllIntIds, R) ->
    N = length(AllIntIds),
    IdsArray = list_to_tuple(AllIntIds),

    NewNodesInt = lists:foldl(
        fun(IntId, Acc) ->
            %% Pick R random neighbors from all nodes (excluding self)
            Neighbors = random_neighbors_int(IntId, IdsArray, N, R),
            maps:put(IntId, Neighbors, Acc)
        end,
        Index#diskann_index.nodes_int,
        NewIntIds
    ),
    Index#diskann_index{nodes_int = NewNodesInt}.

%% Optimized Vamana pass with deferred backward edge updates (FreshDiskANN technique)
%% Collects backward edges in a Δ (delta) map, applies them in batch at the end.
%% This reduces O(n × R) individual graph updates to O(n) batch updates.
%% Returns {UpdatedIndex, ModifiedExistingNodeIds} where ModifiedExistingNodeIds
%% are existing nodes whose neighbor lists were modified by backward edges.
vamana_pass_batch_deferred(Index, NewIntIds, VectorMap, MedoidIntId, Alpha, L, R) ->
    %% Shuffle for randomness
    Sigma = shuffle(NewIntIds),
    NewIntIdSet = sets:from_list(NewIntIds),

    %% Phase 1: Process all new nodes, collect backward edges in Delta map
    %% Delta = #{J => [list of new nodes that want J as neighbor]}
    {Index2, Delta} = lists:foldl(
        fun(IntId, {AccIndex, AccDelta}) ->
            %% Get vector from in-memory map (fast!)
            Vec = maps:get(IntId, VectorMap),

            %% Search using in-memory vectors where possible
            {_Results, Visited} = greedy_search_int_with_cache(AccIndex, MedoidIntId, Vec, 1, L, VectorMap),

            %% Prune to select R out-neighbors for this new node
            AccIndex2 = robust_prune_int_with_cache(AccIndex, IntId, sets:to_list(Visited), Alpha, R, VectorMap),
            Neighbors = maps:get(IntId, AccIndex2#diskann_index.nodes_int, []),

            %% Collect backward edges in Delta instead of updating immediately
            %% For each neighbor J, record that IntId wants to be J's neighbor
            NewDelta = lists:foldl(
                fun(J, DeltaAcc) ->
                    Existing = maps:get(J, DeltaAcc, []),
                    maps:put(J, [IntId | Existing], DeltaAcc)
                end,
                AccDelta,
                Neighbors
            ),
            {AccIndex2, NewDelta}
        end,
        {Index, #{}},
        Sigma
    ),

    %% Phase 2: Apply all backward edges in batch
    Index3 = apply_delta_edges(Index2, Delta, Alpha, R, VectorMap),

    %% Return the modified existing node IDs (Delta keys that are not new nodes)
    ModifiedExisting = [J || J <- maps:keys(Delta), not sets:is_element(J, NewIntIdSet)],
    {Index3, ModifiedExisting}.

%% Apply backward edges collected in Delta map
%% For each node J in Delta, add all requesting nodes as neighbors, pruning if needed
apply_delta_edges(Index, Delta, Alpha, R, VectorMap) ->
    maps:fold(
        fun(J, NewNeighbors, AccIndex) ->
            %% Get J's current neighbors
            JNeighbors = maps:get(J, AccIndex#diskann_index.nodes_int, []),

            %% Merge new neighbors (avoiding duplicates)
            AllCandidates = lists:usort(NewNeighbors ++ JNeighbors),

            %% If total exceeds R, prune; otherwise just add
            case length(AllCandidates) > R of
                true ->
                    %% Need to prune: use alpha-RNG to select best R neighbors
                    robust_prune_int_with_cache(AccIndex, J, AllCandidates, Alpha, R, VectorMap);
                false ->
                    %% Just update J's neighbors directly
                    AccIndex#diskann_index{
                        nodes_int = maps:put(J, AllCandidates, AccIndex#diskann_index.nodes_int)
                    }
            end
        end,
        Index,
        Delta
    ).

%% Greedy search using in-memory vector cache for batch operations
greedy_search_int_with_cache(Index, StartIntId, Query, K, L, VectorMap) ->
    StartDist = distance_with_cache(Index, StartIntId, Query, VectorMap),
    Candidates = gb_trees:from_orddict([{{StartDist, StartIntId}, true}]),
    Results = gb_trees:from_orddict([{{StartDist, StartIntId}, true}]),
    Visited = sets:from_list([StartIntId]),
    FurthestDist = StartDist,
    greedy_search_loop_int_cached(Index, Query, K, L, Candidates, Results, Visited, FurthestDist, VectorMap).

greedy_search_loop_int_cached(Index, Query, K, L, Candidates, Results, Visited, FurthestDist, VectorMap) ->
    case gb_trees:is_empty(Candidates) of
        true ->
            FinalResults = gb_trees:to_list(Results),
            TopK = lists:sublist(FinalResults, K),
            {[{IntId, Dist} || {{Dist, IntId}, _} <- TopK], Visited};
        false ->
            {{BestDist, BestIntId}, _, Candidates2} = gb_trees:take_smallest(Candidates),
            case BestDist > FurthestDist of
                true ->
                    FinalResults = gb_trees:to_list(Results),
                    TopK = lists:sublist(FinalResults, K),
                    {[{IntId, Dist} || {{Dist, IntId}, _} <- TopK], Visited};
                false ->
                    %% Use get_neighbors_int to load from disk/cache for existing nodes
                    Neighbors = get_neighbors_int_with_cache(Index, BestIntId),
                    {Candidates3, Results3, Visited3, FurthestDist3} =
                        process_neighbors_int_cached(Neighbors, Index, Query, L,
                                                     Candidates2, Results, Visited, FurthestDist, VectorMap),
                    greedy_search_loop_int_cached(Index, Query, K, L, Candidates3, Results3, Visited3, FurthestDist3, VectorMap)
            end
    end.

%% Get neighbors using in-memory map first, falling back to disk/cache
get_neighbors_int_with_cache(Index, IntId) ->
    case maps:find(IntId, Index#diskann_index.nodes_int) of
        {ok, Neighbors} -> Neighbors;
        error -> get_neighbors_int(Index, IntId)
    end.

process_neighbors_int_cached([], _Index, _Query, _L, Candidates, Results, Visited, FurthestDist, _VectorMap) ->
    {Candidates, Results, Visited, FurthestDist};
process_neighbors_int_cached([N | Rest], Index, Query, L, Candidates, Results, Visited, FurthestDist, VectorMap) ->
    case sets:is_element(N, Visited) of
        true ->
            process_neighbors_int_cached(Rest, Index, Query, L, Candidates, Results, Visited, FurthestDist, VectorMap);
        false ->
            Visited2 = sets:add_element(N, Visited),
            Dist = distance_with_cache(Index, N, Query, VectorMap),
            Candidates2 = gb_trees:enter({Dist, N}, true, Candidates),
            ResultSize = gb_trees:size(Results),
            {Results2, FurthestDist2} = case ResultSize < L of
                true ->
                    R2 = gb_trees:enter({Dist, N}, true, Results),
                    F2 = case gb_trees:is_empty(R2) of
                        true -> Dist;
                        false -> element(1, element(1, gb_trees:largest(R2)))
                    end,
                    {R2, F2};
                false when Dist < FurthestDist ->
                    {_, _, R2} = gb_trees:take_largest(Results),
                    R3 = gb_trees:enter({Dist, N}, true, R2),
                    F2 = element(1, element(1, gb_trees:largest(R3))),
                    {R3, F2};
                false ->
                    {Results, FurthestDist}
            end,
            process_neighbors_int_cached(Rest, Index, Query, L, Candidates2, Results2, Visited2, FurthestDist2, VectorMap)
    end.

%% Robust prune using in-memory vector cache
robust_prune_int_with_cache(Index, IntId, Candidates, Alpha, R, VectorMap) ->
    Vec = get_vector_with_cache(Index, IntId, VectorMap),

    %% Score all candidates by distance
    Scored = lists:filtermap(
        fun(CandId) when CandId =/= IntId ->
            CandVec = get_vector_with_cache(Index, CandId, VectorMap),
            case CandVec of
                undefined -> false;
                _ -> {true, {distance_vecs(Index, Vec, CandVec), CandId}}
            end;
           (_) -> false
        end,
        Candidates
    ),
    Sorted = lists:sort(Scored),

    %% Alpha-RNG pruning
    Selected = alpha_rng_prune_cached(Sorted, [], Alpha, R, Index, VectorMap),

    %% Update neighbors
    Index#diskann_index{
        nodes_int = maps:put(IntId, Selected, Index#diskann_index.nodes_int)
    }.

alpha_rng_prune_cached([], Acc, _Alpha, _R, _Index, _VectorMap) ->
    lists:reverse(Acc);
alpha_rng_prune_cached(_Remaining, Acc, _Alpha, R, _Index, _VectorMap) when length(Acc) >= R ->
    lists:reverse(Acc);
alpha_rng_prune_cached([{Dist, CandId} | Rest], Acc, Alpha, R, Index, VectorMap) ->
    CandVec = get_vector_with_cache(Index, CandId, VectorMap),
    %% Check alpha-RNG condition: keep if not dominated by existing neighbors
    IsDominated = lists:any(
        fun(SelectedId) ->
            SelectedVec = get_vector_with_cache(Index, SelectedId, VectorMap),
            DistToSelected = distance_vecs(Index, CandVec, SelectedVec),
            DistToSelected * Alpha < Dist
        end,
        Acc
    ),
    case IsDominated of
        true ->
            alpha_rng_prune_cached(Rest, Acc, Alpha, R, Index, VectorMap);
        false ->
            alpha_rng_prune_cached(Rest, [CandId | Acc], Alpha, R, Index, VectorMap)
    end.

%% Get vector from cache or disk
get_vector_with_cache(Index, IntId, VectorMap) ->
    case maps:find(IntId, VectorMap) of
        {ok, Vec} -> Vec;
        error -> get_vector_by_int_id(Index, IntId)
    end.

%% Distance computation using cache
distance_with_cache(Index, IntId, Query, VectorMap) ->
    Vec = get_vector_with_cache(Index, IntId, VectorMap),
    case Vec of
        undefined -> infinity;
        _ -> distance_vecs(Index, Vec, Query)
    end.

%% Helper to compute distance between two vectors
distance_vecs(#diskann_index{config = Config}, Vec1, Vec2) ->
    case Config#diskann_config.distance_fn of
        cosine ->
            case Config#diskann_config.assume_normalized of
                true -> 1.0 - dot_product(Vec1, Vec2);
                false -> cosine_distance(Vec1, Vec2)
            end;
        euclidean ->
            euclidean_distance(Vec1, Vec2)
    end.

%% Batch insert for memory mode
insert_batch_memory(Index0, Vectors, _Opts) ->
    Config = Index0#diskann_index.config,
    #diskann_config{l_build = L, alpha = Alpha, r = R} = Config,

    %% Handle empty index - set first as medoid
    Index1 = case Index0#diskann_index.medoid_id of
        undefined ->
            {FirstId, FirstVec} = hd(Vectors),
            Index0#diskann_index{
                medoid_id = FirstId,
                nodes = #{FirstId => #diskann_node{id = FirstId, neighbors = []}},
                vectors = #{FirstId => FirstVec},
                size = 1
            };
        _ ->
            Index0
    end,

    %% Get vectors to insert (skip first if it was medoid)
    ToInsert = case Index0#diskann_index.medoid_id of
        undefined -> tl(Vectors);
        _ -> Vectors
    end,

    %% Phase 1: Add all vectors to storage
    Index2 = lists:foldl(
        fun({Id, Vec}, AccIdx) ->
            AccIdx#diskann_index{
                vectors = maps:put(Id, Vec, AccIdx#diskann_index.vectors),
                nodes = maps:put(Id, #diskann_node{id = Id, neighbors = []},
                                AccIdx#diskann_index.nodes)
            }
        end,
        Index1,
        ToInsert
    ),

    %% Phase 2: Build graph edges
    Index3 = lists:foldl(
        fun({Id, Vec}, AccIdx) ->
            MedoidId = AccIdx#diskann_index.medoid_id,
            %% greedy_search returns {Results, Visited} where Visited is a set
            {_Results, Visited} = greedy_search(AccIdx, MedoidId, Vec, 1, L),

            %% Robust prune uses the visited set (list of Ids)
            AccIdx1 = robust_prune(AccIdx, Id, sets:to_list(Visited), Alpha, R),

            %% Add backward edges
            Neighbors = get_neighbors(AccIdx1, Id),
            lists:foldl(
                fun(J, AccIdx2) ->
                    JNeighbors = get_neighbors(AccIdx2, J),
                    case length(JNeighbors) + 1 > R of
                        true ->
                            robust_prune(AccIdx2, J, [Id | JNeighbors], Alpha, R);
                        false ->
                            add_neighbor(AccIdx2, J, Id)
                    end
                end,
                AccIdx1,
                Neighbors
            )
        end,
        Index2,
        ToInsert
    ),

    NewSize = Index3#diskann_index.size + length(ToInsert),
    {ok, Index3#diskann_index{size = NewSize}}.

%% @doc Lazy delete - marks node as deleted
-spec delete(diskann_index(), binary()) -> {ok, diskann_index()}.
delete(#diskann_index{hot_enabled = true, hot_id_to_int = HotIdToInt,
                      storage_mode = disk} = Index, Id) ->
    %% Hot layer enabled - check if ID is in hot layer first
    case maps:is_key(Id, HotIdToInt) of
        true ->
            %% Delete from hot layer
            case hot_delete(Index, Id) of
                {ok, NewIndex} -> {ok, NewIndex};
                not_found ->
                    %% Fallback to disk delete
                    delete_from_disk(Index, Id)
            end;
        false ->
            %% Delete from disk layer
            delete_from_disk(Index, Id)
    end;
delete(#diskann_index{storage_mode = disk} = Index, Id) ->
    %% Disk mode: need to update both string and int deleted sets
    delete_from_disk(Index, Id);
delete(Index, Id) ->
    %% Memory mode: only string set needed
    NewDeleted = sets:add_element(Id, Index#diskann_index.deleted_set),
    {ok, Index#diskann_index{deleted_set = NewDeleted}}.

%% Delete from disk layer
delete_from_disk(Index, Id) ->
    NewDeletedStr = sets:add_element(Id, Index#diskann_index.deleted_set),
    case string_id_to_int(Index, Id) of
        {ok, IntId} ->
            NewDeletedInt = sets:add_element(IntId, Index#diskann_index.deleted_int_set),
            {ok, Index#diskann_index{deleted_set = NewDeletedStr, deleted_int_set = NewDeletedInt}};
        not_found ->
            %% ID not found, just update string set
            {ok, Index#diskann_index{deleted_set = NewDeletedStr}}
    end.

%% @doc Search for K nearest neighbors
-spec search(diskann_index(), [float()], pos_integer()) -> [{binary(), float()}].
search(Index, Query, K) ->
    search(Index, Query, K, #{}).

%% @doc Search with options
%% Options:
%%   - l_search: Search beam width (default: from config)
%%   - rerank_factor: Multiplier for rerank candidates (default: 4)
-spec search(diskann_index(), [float()], pos_integer(), map()) -> [{binary(), float()}].
search(#diskann_index{medoid_id = undefined, medoid_int_id = undefined,
                      hot_size = 0}, _Query, _K, _Opts) ->
    [];
search(#diskann_index{medoid_id = undefined, medoid_int_id = undefined,
                      hot_enabled = true, hot_size = HotSize} = Index,
       Query, K, Opts) when HotSize > 0 ->
    %% Only hot layer has data - search hot layer only
    search_hot_layer(Index, Query, K, Opts);
search(#diskann_index{medoid_id = undefined, medoid_int_id = 0,
                      hot_size = 0}, _Query, _K, _Opts) ->
    [];
search(#diskann_index{medoid_id = undefined, medoid_int_id = 0,
                      hot_enabled = true, hot_size = HotSize} = Index,
       Query, K, Opts) when HotSize > 0 ->
    %% Only hot layer has data - search hot layer only
    search_hot_layer(Index, Query, K, Opts);
search(#diskann_index{hot_enabled = true, hot_size = HotSize, storage_mode = disk} = Index,
       Query, K, Opts) when HotSize > 0 ->
    %% Hot layer enabled with data - search both layers and merge
    search_combined(Index, Query, K, Opts);
search(#diskann_index{storage_mode = disk} = Index, Query, K, Opts) ->
    %% Disk mode: use integer IDs internally
    search_disk_mode(Index, Query, K, Opts);
search(#diskann_index{storage_mode = memory} = Index, Query, K, Opts) ->
    %% Memory mode: use string IDs
    search_memory_mode(Index, Query, K, Opts).

%% Search using integer IDs for disk mode (lazy graph loading)
search_disk_mode(#diskann_index{medoid_int_id = MedoidIntId, config = Config,
                                 deleted_int_set = DeletedIntSet,
                                 use_pq = UsePQ, pq_state = PQState,
                                 pq_codes_int = PQCodesInt} = Index,
                  Query, K, Opts) ->
    L = maps:get(l_search, Opts, Config#diskann_config.l_search),
    RerankFactor = maps:get(rerank_factor, Opts, 4),
    RerankK = K * RerankFactor,

    %% Use PQ-based beam search if PQ is available
    IntResults = case UsePQ andalso PQState =/= undefined of
        true ->
            %% Precompute distance tables for this query
            DistTables = barrel_vectordb_pq:precompute_tables(PQState, Query),
            beam_search_pq_int(Index, MedoidIntId, Query, DistTables, PQCodesInt, RerankK, L);
        false ->
            %% Standard greedy search with integer IDs
            {Res, _Visited} = greedy_search_int(Index, MedoidIntId, Query, RerankK, L),
            Res
    end,

    %% Filter deleted nodes and convert integer IDs to string IDs
    Filtered = lists:filtermap(
        fun({D, IntId}) ->
            case sets:is_element(IntId, DeletedIntSet) of
                true -> false;
                false ->
                    case int_id_to_string(Index, IntId) of
                        {ok, StringId} -> {true, {D, StringId}};
                        not_found -> false
                    end
            end
        end,
        IntResults
    ),

    %% Return top K
    TopK = lists:sublist(Filtered, K),
    [{Id, D} || {D, Id} <- TopK].

%% Search using string IDs for memory mode
search_memory_mode(#diskann_index{medoid_id = S, config = Config,
                                   deleted_set = DeletedSet,
                                   use_pq = UsePQ, pq_state = PQState} = Index,
                    Query, K, Opts) ->
    L = maps:get(l_search, Opts, Config#diskann_config.l_search),
    RerankFactor = maps:get(rerank_factor, Opts, 4),
    RerankK = K * RerankFactor,

    %% Use PQ-based beam search if PQ is available
    Results = case UsePQ andalso PQState =/= undefined of
        true ->
            %% Precompute distance tables for this query
            DistTables = barrel_vectordb_pq:precompute_tables(PQState, Query),
            beam_search_pq(Index, S, Query, DistTables, RerankK, L);
        false ->
            %% Standard greedy search
            {Res, _Visited} = greedy_search(Index, S, Query, RerankK, L),
            Res
    end,

    %% Filter deleted nodes
    Filtered = [{D, Id} || {D, Id} <- Results,
                           not sets:is_element(Id, DeletedSet)],

    %% Return top K
    TopK = lists:sublist(Filtered, K),
    [{Id, D} || {D, Id} <- TopK].

%% Beam search with PQ using integer IDs (for disk mode)
beam_search_pq_int(Index, StartIntId, Query, DistTables, PQCodesInt, K, L) ->
    %% Get PQ distance to start node
    StartDist = case maps:find(StartIntId, PQCodesInt) of
        {ok, Code} -> barrel_vectordb_pq:distance(DistTables, Code);
        error -> infinity
    end,

    Candidates = gb_trees:from_orddict([{{StartDist, StartIntId}, true}]),
    Results = gb_trees:from_orddict([{{StartDist, StartIntId}, true}]),
    Visited = sets:from_list([StartIntId]),
    FurthestDist = StartDist,

    %% Run beam search with PQ distances
    PQResults = beam_search_pq_loop_int(Index, DistTables, PQCodesInt, Candidates,
                                         Results, Visited, FurthestDist, K, L),

    %% Rerank top results with full vectors for accuracy
    rerank_with_full_vectors_int(PQResults, Query, Index, K).

beam_search_pq_loop_int(_Index, _DistTables, _PQCodesInt, {0, nil}, Results, _Visited, _FurthestDist, K, _L) ->
    ResultList = [Item || {Item, _} <- gb_trees:to_list(Results)],
    lists:sublist(ResultList, K);
beam_search_pq_loop_int(Index, DistTables, PQCodesInt, Candidates, Results, Visited, FurthestDist, K, L) ->
    {{CurrentDist, CurrentIntId}, _, RestCandidates} = gb_trees:take_smallest(Candidates),

    case CurrentDist > FurthestDist of
        true ->
            ResultList = [Item || {Item, _} <- gb_trees:to_list(Results)],
            lists:sublist(ResultList, K);
        false ->
            Neighbors = get_neighbors_int(Index, CurrentIntId),
            {NewCandidates, NewResults, NewVisited, NewFurthestDist} = lists:foldl(
                fun(N, {CandAcc, ResAcc, VisAcc, FurthAcc}) ->
                    case sets:is_element(N, VisAcc) of
                        true ->
                            {CandAcc, ResAcc, VisAcc, FurthAcc};
                        false ->
                            NewVisAcc = sets:add_element(N, VisAcc),
                            D = case maps:find(N, PQCodesInt) of
                                {ok, Code} -> barrel_vectordb_pq:distance(DistTables, Code);
                                error -> infinity
                            end,
                            ResSize = gb_trees:size(ResAcc),
                            ShouldAdd = D < FurthAcc orelse ResSize < L,
                            case ShouldAdd of
                                true ->
                                    NewCandAcc = gb_trees:insert({D, N}, true, CandAcc),
                                    NewResAcc0 = gb_trees:insert({D, N}, true, ResAcc),
                                    NewResSize = gb_trees:size(NewResAcc0),
                                    {NewResAcc, NewFurthAcc} = case NewResSize > L of
                                        true ->
                                            {_, _, Trimmed} = gb_trees:take_largest(NewResAcc0),
                                            {{LastD, _}, _} = gb_trees:largest(Trimmed),
                                            {Trimmed, LastD};
                                        false ->
                                            {{MaxD, _}, _} = gb_trees:largest(NewResAcc0),
                                            {NewResAcc0, MaxD}
                                    end,
                                    {NewCandAcc, NewResAcc, NewVisAcc, NewFurthAcc};
                                false ->
                                    {CandAcc, ResAcc, NewVisAcc, FurthAcc}
                            end
                    end
                end,
                {RestCandidates, Results, Visited, FurthestDist},
                Neighbors
            ),

            beam_search_pq_loop_int(Index, DistTables, PQCodesInt, NewCandidates,
                                     NewResults, NewVisited, NewFurthestDist, K, L)
    end.

%% Rerank top candidates using full vectors (integer IDs)
rerank_with_full_vectors_int(PQResults, Query, Index, K) ->
    Config = Index#diskann_index.config,
    Reranked = lists:map(
        fun({_PQDist, IntId}) ->
            Vec = get_vector_by_int_id(Index, IntId),
            case Vec of
                undefined ->
                    {infinity, IntId};
                _ ->
                    ExactDist = distance_vec(Config, Query, Vec),
                    {ExactDist, IntId}
            end
        end,
        PQResults
    ),
    Sorted = lists:sort(Reranked),
    lists:sublist(Sorted, K).

%% @doc Get index size (excluding deleted)
-spec size(diskann_index()) -> non_neg_integer().
size(#diskann_index{size = Size, deleted_set = Deleted,
                    hot_size = HotSize, hot_deleted = HotDeleted}) ->
    TotalSize = Size + HotSize,
    TotalDeleted = sets:size(Deleted) + sets:size(HotDeleted),
    TotalSize - TotalDeleted.

%% @doc Get index info
-spec info(diskann_index()) -> map().
info(#diskann_index{config = Config, size = Size, medoid_id = Medoid,
                    deleted_set = Deleted, nodes = Nodes, use_pq = UsePQ,
                    pq_state = PQState, pq_codes = PQCodes,
                    quantization_method = QuantMethod, tq_state = TQState, tq_codes = TQCodes,
                    storage_mode = StorageMode, base_path = BasePath,
                    cache_table = CacheTable, cache_max_size = CacheMaxSize,
                    hot_size = HotSize} = Index) ->
    AvgDegree = case maps:size(Nodes) of
        0 -> 0.0;
        N ->
            TotalDegree = maps:fold(
                fun(_, #diskann_node{neighbors = Ns}, Acc) ->
                    Acc + length(Ns)
                end,
                0,
                Nodes
            ),
            TotalDegree / N
    end,
    PQInfo = case UsePQ andalso PQState =/= undefined of
        true ->
            #{
                enabled => true,
                m => barrel_vectordb_pq:info(PQState),
                num_codes => maps:size(PQCodes)
            };
        false ->
            #{enabled => false}
    end,
    %% Quantization info
    QuantizationInfo = case QuantMethod of
        turboquant when TQState =/= undefined ->
            TQInfo = barrel_vectordb_turboquant:info(TQState),
            #{
                method => turboquant,
                bits => maps:get(bits, TQInfo),
                bytes_per_vector => maps:get(bytes_per_vector, TQInfo),
                num_codes => maps:size(TQCodes)
            };
        subspace_turboquant when TQState =/= undefined ->
            TQInfo = barrel_vectordb_turboquant_subspace:info(TQState),
            #{
                method => subspace_turboquant,
                bits => maps:get(bits, TQInfo),
                m => maps:get(m, TQInfo),
                bytes_per_vector => maps:get(bytes_per_vector, TQInfo),
                num_codes => maps:size(TQCodes)
            };
        pq when UsePQ, PQState =/= undefined ->
            #{method => pq, enabled => true};
        _ ->
            #{method => none}
    end,
    CacheSize = case CacheTable of
        undefined -> 0;
        _ -> ets:info(CacheTable, size)
    end,
    StorageInfo = #{
        mode => StorageMode,
        base_path => BasePath,
        cache_size => CacheSize,
        cache_max_size => CacheMaxSize
    },
    %% Include hot layer in total size calculation
    TotalSize = Size + HotSize,
    #{
        size => TotalSize,
        active_size => TotalSize - sets:size(Deleted),
        deleted_count => sets:size(Deleted),
        medoid => Medoid,
        avg_degree => AvgDegree,
        pq => PQInfo,
        quantization => QuantizationInfo,
        storage => StorageInfo,
        hot_layer => hot_layer_info(Index),
        config => #{
            r => Config#diskann_config.r,
            l_build => Config#diskann_config.l_build,
            l_search => Config#diskann_config.l_search,
            alpha => Config#diskann_config.alpha,
            dimension => Config#diskann_config.dimension,
            distance_fn => Config#diskann_config.distance_fn,
            assume_normalized => Config#diskann_config.assume_normalized
        }
    }.

%% @doc Get vector by ID (uses cache for disk mode)
-spec get_vector(diskann_index(), binary()) -> {ok, [float()]} | not_found.
get_vector(#diskann_index{storage_mode = memory} = Index, Id) ->
    %% Memory mode: use string ID directly
    {Vec, _UpdatedIndex} = get_vector_or_cached(Index, Id),
    case Vec of
        undefined -> not_found;
        _ -> {ok, Vec}
    end;
get_vector(#diskann_index{hot_enabled = true, hot_id_to_int = HotIdToInt,
                          hot_vectors = HotVecs, storage_mode = disk} = Index, StringId) ->
    %% Hot layer enabled - check hot layer first
    case maps:find(StringId, HotIdToInt) of
        {ok, HotIntId} ->
            case maps:find(HotIntId, HotVecs) of
                {ok, Vec} -> {ok, Vec};
                error -> get_vector_from_disk(Index, StringId)
            end;
        error ->
            get_vector_from_disk(Index, StringId)
    end;
get_vector(#diskann_index{storage_mode = disk, id_db = undefined} = Index, Id) ->
    %% Disk mode without RocksDB (legacy V1 fallback)
    {Vec, _UpdatedIndex} = get_vector_or_cached(Index, Id),
    case Vec of
        undefined -> not_found;
        _ -> {ok, Vec}
    end;
get_vector(#diskann_index{storage_mode = disk} = Index, StringId) ->
    %% Disk mode with RocksDB (V2 format): convert string ID to integer ID
    get_vector_from_disk(Index, StringId).

%% Get vector from disk layer
get_vector_from_disk(#diskann_index{id_db = undefined} = Index, StringId) ->
    {Vec, _UpdatedIndex} = get_vector_or_cached(Index, StringId),
    case Vec of
        undefined -> not_found;
        _ -> {ok, Vec}
    end;
get_vector_from_disk(Index, StringId) ->
    case string_id_to_int(Index, StringId) of
        {ok, IntId} ->
            Vec = get_vector_by_int_id(Index, IntId),
            case Vec of
                undefined -> not_found;
                _ -> {ok, Vec}
            end;
        not_found ->
            not_found
    end.

%% @doc Check if consolidation is needed (threshold-based)
%% Returns true if deleted count exceeds 10% of active size
-spec needs_consolidation(diskann_index()) -> boolean().
needs_consolidation(#diskann_index{storage_mode = memory, deleted_set = DeletedSet, size = Size}) ->
    DeletedCount = sets:size(DeletedSet),
    ActiveSize = Size - DeletedCount,
    ActiveSize > 0 andalso DeletedCount > ActiveSize div 10;
needs_consolidation(#diskann_index{storage_mode = disk, deleted_int_set = DeletedIntSet, size = Size}) ->
    DeletedCount = sets:size(DeletedIntSet),
    ActiveSize = Size - DeletedCount,
    ActiveSize > 0 andalso DeletedCount > ActiveSize div 10.

%% @doc Consolidate deleted nodes (batch cleanup)
%% This repairs the graph by removing edges to deleted nodes
%% and adding new edges to maintain navigability
-spec consolidate_deletes(diskann_index()) -> {ok, diskann_index()}.
consolidate_deletes(#diskann_index{storage_mode = disk} = Index) ->
    consolidate_deletes_disk(Index);
consolidate_deletes(#diskann_index{deleted_set = DeletedSet, config = Config,
                                   nodes = Nodes, vectors = Vectors} = Index) ->
    case sets:size(DeletedSet) of
        0 ->
            {ok, Index};
        _ ->
            consolidate_deletes_impl(Index, DeletedSet, Config, Nodes, Vectors)
    end.

consolidate_deletes_impl(Index, DeletedSet, Config, Nodes, Vectors) ->
    #diskann_config{alpha = Alpha, r = R} = Config,

    %% For each node with edges to deleted nodes, repair neighborhood
    UpdatedNodes = maps:fold(
        fun(P, #diskann_node{neighbors = Neighbors} = Node, AccNodes) ->
            case sets:is_element(P, DeletedSet) of
                true ->
                    %% Skip deleted nodes
                    AccNodes;
                false ->
                    DeletedNeighbors = [N || N <- Neighbors,
                                             sets:is_element(N, DeletedSet)],
                    case DeletedNeighbors of
                        [] ->
                            AccNodes#{P => Node};
                        _ ->
                            %% Repair: find new candidates from deleted nodes' neighbors
                            SurvivingNeighbors = Neighbors -- DeletedNeighbors,
                            DeletedOutNeighbors = lists:flatmap(
                                fun(V) ->
                                    case maps:find(V, Nodes) of
                                        {ok, #diskann_node{neighbors = VNs}} ->
                                            [N || N <- VNs,
                                                  not sets:is_element(N, DeletedSet)];
                                        error -> []
                                    end
                                end,
                                DeletedNeighbors
                            ),
                            Candidates = lists:usort(SurvivingNeighbors ++ DeletedOutNeighbors) -- [P],

                            %% Re-prune with alpha
                            NewNeighbors = prune_neighbors(
                                Index, P, Candidates, Alpha, R
                            ),
                            AccNodes#{P => Node#diskann_node{neighbors = NewNeighbors}}
                    end
            end
        end,
        #{},
        Nodes
    ),

    %% Remove deleted nodes from index
    NewVectors = maps:without(sets:to_list(DeletedSet), Vectors),
    NewSize = maps:size(UpdatedNodes),

    %% Update medoid if it was deleted
    NewMedoid = case sets:is_element(Index#diskann_index.medoid_id, DeletedSet) of
        true ->
            %% Pick new medoid from remaining nodes
            case maps:keys(UpdatedNodes) of
                [] -> undefined;
                [First | _] -> First
            end;
        false ->
            Index#diskann_index.medoid_id
    end,

    {ok, Index#diskann_index{
        nodes = UpdatedNodes,
        vectors = NewVectors,
        deleted_set = sets:new(),
        size = NewSize,
        medoid_id = NewMedoid
    }}.

%% Disk mode consolidation - repairs graph on disk
consolidate_deletes_disk(#diskann_index{deleted_int_set = DeletedIntSet} = Index) ->
    case sets:size(DeletedIntSet) of
        0 ->
            {ok, Index};
        _ ->
            consolidate_deletes_disk_impl(Index)
    end.

consolidate_deletes_disk_impl(#diskann_index{deleted_int_set = DeletedIntSet,
                                              config = Config,
                                              file_handle = FH} = Index) ->
    #diskann_config{alpha = Alpha, r = R} = Config,

    %% Get all non-deleted node IDs
    AllIntIds = get_all_int_ids(Index),
    ActiveIntIds = [Id || Id <- AllIntIds, not sets:is_element(Id, DeletedIntSet)],

    %% For each active node, check if it has edges to deleted nodes
    UpdatedNodesInt = lists:foldl(
        fun(IntId, AccNodes) ->
            Neighbors = get_neighbors_int(Index, IntId),
            DeletedNeighbors = [N || N <- Neighbors, sets:is_element(N, DeletedIntSet)],
            case DeletedNeighbors of
                [] ->
                    AccNodes;
                _ ->
                    %% Repair: find new candidates from deleted nodes' neighbors
                    SurvivingNeighbors = Neighbors -- DeletedNeighbors,
                    DeletedOutNeighbors = lists:flatmap(
                        fun(V) ->
                            VNs = get_neighbors_int(Index, V),
                            [N || N <- VNs, not sets:is_element(N, DeletedIntSet)]
                        end,
                        DeletedNeighbors
                    ),
                    Candidates = lists:usort(SurvivingNeighbors ++ DeletedOutNeighbors) -- [IntId],

                    %% Re-prune with alpha using robust_prune_int
                    Index2 = Index#diskann_index{nodes_int = AccNodes},
                    NewNeighbors = prune_candidates_int(Index2, IntId, Candidates, Alpha, R),

                    %% Write updated neighbors to disk
                    ok = barrel_vectordb_diskann_file:write_node_int(FH, IntId, NewNeighbors, R),

                    maps:put(IntId, NewNeighbors, AccNodes)
            end
        end,
        Index#diskann_index.nodes_int,
        ActiveIntIds
    ),

    %% Update medoid if it was deleted
    NewMedoidIntId = case sets:is_element(Index#diskann_index.medoid_int_id, DeletedIntSet) of
        true ->
            case ActiveIntIds of
                [] -> undefined;
                [First | _] -> First
            end;
        false ->
            Index#diskann_index.medoid_int_id
    end,

    NewMedoidId = case NewMedoidIntId of
        undefined -> undefined;
        _ ->
            case int_id_to_string(Index, NewMedoidIntId) of
                {ok, StrId} -> StrId;
                not_found -> undefined
            end
    end,

    NewSize = length(ActiveIntIds),

    {ok, Index#diskann_index{
        nodes_int = UpdatedNodesInt,
        deleted_int_set = sets:new(),
        deleted_set = sets:new(),
        size = NewSize,
        medoid_int_id = NewMedoidIntId,
        medoid_id = NewMedoidId
    }}.

%% Helper to prune candidates using robust prune logic with integer IDs
prune_candidates_int(Index, IntId, Candidates, Alpha, R) ->
    Vec = get_vector_by_int_id(Index, IntId),
    case Vec of
        undefined -> [];
        _ ->
            %% Score all candidates by distance
            Scored = lists:filtermap(
                fun(CandId) when CandId =/= IntId ->
                    CandVec = get_vector_by_int_id(Index, CandId),
                    case CandVec of
                        undefined -> false;
                        _ -> {true, {distance_vec(Index#diskann_index.config, Vec, CandVec), CandId}}
                    end;
                   (_) -> false
                end,
                Candidates
            ),
            Sorted = lists:sort(Scored),
            %% Alpha-RNG pruning
            alpha_rng_prune_int(Sorted, [], Alpha, R, Index)
    end.

alpha_rng_prune_int([], Acc, _Alpha, _R, _Index) ->
    lists:reverse(Acc);
alpha_rng_prune_int(_Remaining, Acc, _Alpha, R, _Index) when length(Acc) >= R ->
    lists:reverse(Acc);
alpha_rng_prune_int([{Dist, CandId} | Rest], Acc, Alpha, R, Index) ->
    CandVec = get_vector_by_int_id(Index, CandId),
    %% Check alpha-RNG condition: keep if not dominated by existing neighbors
    IsDominated = lists:any(
        fun(SelectedId) ->
            SelectedVec = get_vector_by_int_id(Index, SelectedId),
            case SelectedVec of
                undefined -> false;
                _ ->
                    DistToSelected = distance_vec(Index#diskann_index.config, CandVec, SelectedVec),
                    DistToSelected * Alpha < Dist
            end
        end,
        Acc
    ),
    case IsDominated of
        true ->
            alpha_rng_prune_int(Rest, Acc, Alpha, R, Index);
        false ->
            alpha_rng_prune_int(Rest, [CandId | Acc], Alpha, R, Index)
    end.

%% Get all integer IDs from the index
%% Note: memory clause kept for completeness, currently only disk path is used
-dialyzer({nowarn_function, get_all_int_ids/1}).
get_all_int_ids(#diskann_index{storage_mode = memory, idx_to_id = IdxToId}) ->
    maps:keys(IdxToId);
get_all_int_ids(#diskann_index{storage_mode = disk, next_int_id = NextId}) ->
    %% For disk mode, IDs are 0 to next_int_id - 1
    lists:seq(0, NextId - 1).

%%====================================================================
%% Persistence API
%%====================================================================

%% @doc Open an existing DiskANN index from disk
%% For V2 format: O(1) startup with lazy graph loading
%% For V1 format: Migrates to V2 or loads from diskann.index
-spec open(binary() | string()) -> {ok, diskann_index()} | {error, term()}.
open(BasePath) ->
    open(BasePath, #{}).

%% @doc Open with options: `crypto => none | #{key := <<_:256>>,
%% env => rocksdb env}'. The key encrypts the flat files; the env (or
%% one minted from the key) covers the diskann_ids RocksDB.
-spec open(binary() | string(), map()) ->
    {ok, diskann_index()} | {error, term()}.
open(BasePath, Opts) ->
    BasePathBin = to_binary_or_undefined(BasePath),
    Crypto = maps:get(crypto, Opts, none),
    IdDbPath = filename:join(BasePathBin, "diskann_ids"),
    case filelib:is_dir(IdDbPath) of
        true ->
            %% V2 format with RocksDB ID mapping - fast O(1) open
            open_v2(BasePathBin, Crypto);
        false ->
            %% V1 format - try to migrate or use legacy open. With
            %% crypto requested the meta decode fails closed
            %% (cannot_encrypt_legacy_index) before anything opens.
            open_v1_with_migration(BasePathBin, Crypto)
    end.

%% Open V2 format index (O(1) startup)
open_v2(BasePath, Crypto) ->
    %% 1. Open binary files (O(1))
    case barrel_vectordb_diskann_file:open(BasePath, #{crypto => file_crypto(Crypto)}) of
        {ok, FileHandle} ->
            %% 2. Read header from binary file
            case barrel_vectordb_diskann_file:read_header_from_file(FileHandle) of
                {ok, Header} ->
                    open_v2_with_header(BasePath, FileHandle, Header, Crypto);
                {error, _} ->
                    %% Fallback to meta file header
                    Header = barrel_vectordb_diskann_file:read_header(FileHandle),
                    open_v2_with_header(BasePath, FileHandle, Header, Crypto)
            end;
        {error, _} = Error ->
            Error
    end.

open_v2_with_header(BasePath, FileHandle, Header, Crypto) ->
    %% 3. Open RocksDB for ID mapping (O(1))
    case open_id_db(BasePath, Crypto) of
        {ok, IdDb, CfFwd, CfRev, Standalone, IdEnv} ->
            Dimension = maps:get(dimension, Header, 128),
            R = maps:get(r, Header, 64),
            NodeCount = maps:get(node_count, Header, 0),
            DistFn = maps:get(distance_fn, Header, cosine),

            %% Get integer medoid ID (V2) or string medoid (V1)
            MedoidIntId = maps:get(entry_point_int, Header, 0),
            MedoidId = maps:get(entry_point, Header, undefined),
            NextIntId = maps:get(next_int_id, Header, NodeCount),

            Config = #diskann_config{
                dimension = Dimension,
                r = R,
                distance_fn = DistFn
            },

            %% 4. Create ETS caches (empty - lazy load)
            CacheTable = create_cache_table(),
            GraphCacheTable = create_graph_cache_table(),

            %% 5. Load PQ state if available
            {PQState, PQCodesInt} = load_pq_state(
                BasePath, barrel_vectordb_diskann_file:get_crypto(FileHandle)),

            Index = #diskann_index{
                config = Config,
                file_handle = FileHandle,
                base_path = BasePath,
                storage_mode = disk,
                medoid_id = MedoidId,
                medoid_int_id = MedoidIntId,
                size = NodeCount,
                nodes = #{},  %% Empty - lazy load from disk
                nodes_int = #{},
                id_db = IdDb,
                id_cf_fwd = CfFwd,
                id_cf_rev = CfRev,
                id_db_standalone = Standalone,
                id_db_env = IdEnv,
                next_int_id = NextIntId,
                cache_table = CacheTable,
                graph_cache_table = GraphCacheTable,
                pq_state = PQState,
                pq_codes_int = PQCodesInt,
                use_pq = PQState =/= undefined
            },

            %% 6. Pre-warm caches with medoid + neighbors (optional, for better first-query latency)
            prewarm_caches(Index, MedoidIntId, min(100, Index#diskann_index.cache_max_size)),

            {ok, Index};

        {error, Reason} ->
            barrel_vectordb_diskann_file:close(FileHandle),
            {error, {id_db_open_failed, Reason}}
    end.

%% Open V1 format or migrate to V2
open_v1_with_migration(BasePath, Crypto) ->
    case barrel_vectordb_diskann_file:open(BasePath, #{crypto => file_crypto(Crypto)}) of
        {ok, FileHandle} ->
            Header = barrel_vectordb_diskann_file:read_header(FileHandle),
            %% Try to load from legacy diskann.index file
            MetaPath = filename:join(BasePath, "diskann.index"),
            case file:read_file(MetaPath) of
                {ok, MetaBin} ->
                    try binary_to_term(MetaBin) of
                        SerializedIndex ->
                            %% Legacy V1 index loaded - migrate to V2
                            migrate_v1_to_v2(SerializedIndex, FileHandle, BasePath)
                    catch
                        _:_ ->
                            %% Invalid binary, create empty V2 index
                            create_empty_v2_index(FileHandle, Header, BasePath)
                    end;
                {error, _} ->
                    %% No legacy file, create empty V2 index
                    create_empty_v2_index(FileHandle, Header, BasePath)
            end;
        {error, _} = Error ->
            Error
    end.

%% Migrate V1 index to V2 format (plaintext only: an encrypted open of
%% a V1 file set fails closed before reaching this)
migrate_v1_to_v2(V1Index, FileHandle, BasePath) ->
    %% Open RocksDB for new ID mapping
    case open_id_db(BasePath, none) of
        {ok, IdDb, CfFwd, CfRev, Standalone, _IdEnv} ->
            Config = V1Index#diskann_index.config,
            Nodes = V1Index#diskann_index.nodes,
            MedoidId = V1Index#diskann_index.medoid_id,

            %% Assign integer IDs to all existing nodes
            {Index1, IntIdMap} = maps:fold(
                fun(StringId, _Node, {AccIndex, AccMap}) ->
                    {IntId, NewIndex} = get_or_create_int_id(AccIndex, StringId),
                    {NewIndex, AccMap#{StringId => IntId}}
                end,
                {#diskann_index{
                    config = Config,
                    id_db = IdDb,
                    id_cf_fwd = CfFwd,
                    id_cf_rev = CfRev,
                    id_db_standalone = Standalone,
                    next_int_id = 0,
                    storage_mode = disk
                }, #{}},
                Nodes
            ),

            %% Get medoid integer ID
            MedoidIntId = maps:get(MedoidId, IntIdMap, 0),

            %% Convert graph to integer IDs and write to disk
            R = Config#diskann_config.r,
            maps:foreach(
                fun(StringId, #diskann_node{neighbors = Neighbors}) ->
                    IntId = maps:get(StringId, IntIdMap),
                    NeighborIntIds = [maps:get(N, IntIdMap, 0) || N <- Neighbors],
                    ok = barrel_vectordb_diskann_file:write_node_int(FileHandle, IntId, NeighborIntIds, R)
                end,
                Nodes
            ),

            %% Update header to V2 format
            NodeCount = maps:size(Nodes),
            {ok, FileHandle2} = barrel_vectordb_diskann_file:write_header(
                FileHandle,
                #{
                    dimension => Config#diskann_config.dimension,
                    r => R,
                    node_count => NodeCount,
                    distance_fn => Config#diskann_config.distance_fn,
                    entry_point => MedoidId,
                    entry_point_int => MedoidIntId,
                    next_int_id => Index1#diskann_index.next_int_id
                }
            ),

            %% Create caches
            CacheTable = create_cache_table(),
            GraphCacheTable = create_graph_cache_table(),

            %% Convert PQ codes to integer IDs
            PQCodesInt = case V1Index#diskann_index.use_pq of
                true ->
                    maps:fold(
                        fun(StringId, Code, Acc) ->
                            case maps:find(StringId, IntIdMap) of
                                {ok, IntId} -> Acc#{IntId => Code};
                                error -> Acc
                            end
                        end,
                        #{},
                        V1Index#diskann_index.pq_codes
                    );
                false ->
                    #{}
            end,

            Index2 = #diskann_index{
                config = Config,
                file_handle = FileHandle2,
                base_path = BasePath,
                storage_mode = disk,
                medoid_id = MedoidId,
                medoid_int_id = MedoidIntId,
                size = NodeCount,
                nodes = #{},  %% Clear - now on disk
                nodes_int = #{},
                id_db = IdDb,
                id_cf_fwd = CfFwd,
                id_cf_rev = CfRev,
                id_db_standalone = Standalone,
                next_int_id = Index1#diskann_index.next_int_id,
                cache_table = CacheTable,
                graph_cache_table = GraphCacheTable,
                pq_state = V1Index#diskann_index.pq_state,
                pq_codes_int = PQCodesInt,
                use_pq = V1Index#diskann_index.use_pq,
                deleted_set = V1Index#diskann_index.deleted_set
            },

            %% Delete legacy diskann.index file
            MetaPath = filename:join(BasePath, "diskann.index"),
            _ = file:delete(MetaPath),

            %% Pre-warm caches
            prewarm_caches(Index2, MedoidIntId, min(100, Index2#diskann_index.cache_max_size)),

            {ok, Index2};

        {error, Reason} ->
            barrel_vectordb_diskann_file:close(FileHandle),
            {error, {migration_failed, Reason}}
    end.

%% Create empty V2 index
create_empty_v2_index(FileHandle, Header, BasePath) ->
    case open_id_db(BasePath,
                    barrel_vectordb_diskann_file:get_crypto(FileHandle)) of
        {ok, IdDb, CfFwd, CfRev, Standalone, IdEnv} ->
            Dimension = maps:get(dimension, Header, 128),
            R = maps:get(r, Header, 64),
            NodeCount = maps:get(node_count, Header, 0),
            MedoidId = maps:get(entry_point, Header, undefined),
            DistFn = maps:get(distance_fn, Header, cosine),

            Config = #diskann_config{
                dimension = Dimension,
                r = R,
                distance_fn = DistFn
            },

            CacheTable = create_cache_table(),
            GraphCacheTable = create_graph_cache_table(),

            {ok, #diskann_index{
                config = Config,
                file_handle = FileHandle,
                base_path = BasePath,
                storage_mode = disk,
                medoid_id = MedoidId,
                medoid_int_id = 0,
                size = NodeCount,
                nodes = #{},
                nodes_int = #{},
                id_db = IdDb,
                id_cf_fwd = CfFwd,
                id_cf_rev = CfRev,
                id_db_standalone = Standalone,
                id_db_env = IdEnv,
                next_int_id = NodeCount,
                cache_table = CacheTable,
                graph_cache_table = GraphCacheTable
            }};
        {error, Reason} ->
            barrel_vectordb_diskann_file:close(FileHandle),
            {error, {id_db_open_failed, Reason}}
    end.

%% Load PQ state from disk. A small whole-file term: GCM envelope when
%% the index is encrypted (the meta superblock already gated the key).
load_pq_state(BasePath, Crypto) ->
    PqStatePath = filename:join(BasePath, "diskann.pq_state"),
    case file:read_file(PqStatePath) of
        {ok, Bin0} ->
            case pq_state_open(Crypto, Bin0) of
                {ok, Bin} ->
                    try binary_to_term(Bin) of
                        {PQState, PQCodesInt} -> {PQState, PQCodesInt};
                        PQState -> {PQState, #{}}  %% Legacy format
                    catch
                        _:_ -> {undefined, #{}}
                    end;
                error ->
                    {undefined, #{}}
            end;
        {error, _} ->
            {undefined, #{}}
    end.

pq_state_open(none, Bin) ->
    {ok, Bin};
pq_state_open(#{key := Key}, Bin) ->
    case barrel_crypto:decrypt(Bin, Key) of
        {error, _} -> error;
        Plain -> {ok, Plain}
    end.

%% Save PQ state to disk
save_pq_state(_BasePath, undefined, _PQCodesInt, _Crypto) -> ok;
save_pq_state(BasePath, PQState, PQCodesInt, Crypto) ->
    PqStatePath = filename:join(BasePath, "diskann.pq_state"),
    Bin = term_to_binary({PQState, PQCodesInt}),
    Out = case Crypto of
        none -> Bin;
        #{key := Key} -> barrel_crypto:encrypt(Bin, Key)
    end,
    file:write_file(PqStatePath, Out).

%% @doc Close the index and flush to disk
%% For V2 format: writes header and PQ state, closes RocksDB
%% Does NOT use term_to_binary for the entire index
-spec close(diskann_index()) -> ok.
close(#diskann_index{file_handle = undefined, cache_table = undefined,
                     graph_cache_table = undefined, id_db = undefined}) ->
    ok;
close(#diskann_index{storage_mode = disk} = Index) ->
    #diskann_index{
        file_handle = FileHandle,
        base_path = BasePath,
        cache_table = CacheTable,
        graph_cache_table = GraphCacheTable,
        id_db = IdDb,
        id_db_standalone = Standalone,
        config = Config,
        size = Size,
        medoid_id = MedoidId,
        medoid_int_id = MedoidIntId,
        next_int_id = NextIntId,
        pq_state = PQState,
        pq_codes_int = PQCodesInt
    } = Index,

    %% 1. Write final header to binary file (V2 format)
    _ = case FileHandle of
        undefined -> ok;
        _ ->
            {ok, _} = barrel_vectordb_diskann_file:write_header(
                FileHandle,
                #{
                    dimension => Config#diskann_config.dimension,
                    r => Config#diskann_config.r,
                    node_count => Size,
                    distance_fn => Config#diskann_config.distance_fn,
                    entry_point => MedoidId,
                    entry_point_int => MedoidIntId,
                    next_int_id => NextIntId
                }
            )
    end,

    %% 2. Save PQ state separately (small, uses term_to_binary only for PQ)
    _ = save_pq_state(BasePath, PQState, PQCodesInt,
                      file_handle_crypto(FileHandle)),

    %% 3. Sync binary files
    case FileHandle of
        undefined -> ok;
        _ ->
            barrel_vectordb_diskann_file:sync(FileHandle),
            barrel_vectordb_diskann_file:close(FileHandle)
    end,

    %% 4. Close RocksDB (only if standalone)
    close_id_db(IdDb, Standalone),

    %% 5. Delete ETS tables
    case CacheTable of
        undefined -> ok;
        _ -> ets:delete(CacheTable)
    end,
    case GraphCacheTable of
        undefined -> ok;
        _ -> ets:delete(GraphCacheTable)
    end,

    ok;
close(#diskann_index{storage_mode = memory, cache_table = CacheTable,
                     graph_cache_table = GraphCacheTable}) ->
    %% Memory mode - just clean up ETS tables if any
    case CacheTable of
        undefined -> ok;
        _ -> ets:delete(CacheTable)
    end,
    case GraphCacheTable of
        undefined -> ok;
        _ -> ets:delete(GraphCacheTable)
    end,
    ok.

%% @doc Force sync to disk
-spec sync(diskann_index()) -> ok.
sync(#diskann_index{file_handle = undefined}) ->
    ok;
sync(#diskann_index{storage_mode = disk, file_handle = FileHandle,
                    base_path = BasePath, config = Config,
                    size = Size, medoid_id = MedoidId,
                    medoid_int_id = MedoidIntId, next_int_id = NextIntId,
                    pq_state = PQState, pq_codes_int = PQCodesInt}) ->
    %% Write header
    {ok, _} = barrel_vectordb_diskann_file:write_header(
        FileHandle,
        #{
            dimension => Config#diskann_config.dimension,
            r => Config#diskann_config.r,
            node_count => Size,
            distance_fn => Config#diskann_config.distance_fn,
            entry_point => MedoidId,
            entry_point_int => MedoidIntId,
            next_int_id => NextIntId
        }
    ),
    %% Save PQ state
    _ = save_pq_state(BasePath, PQState, PQCodesInt,
                      file_handle_crypto(FileHandle)),
    %% Sync files
    _ = barrel_vectordb_diskann_file:sync(FileHandle),
    ok;
sync(#diskann_index{storage_mode = memory}) ->
    %% Nothing to sync for memory mode
    ok.

%% @doc Serialize index to binary (for barrel_vectordb_index behaviour)
-spec serialize(diskann_index()) -> binary().
serialize(#diskann_index{storage_mode = memory} = Index) ->
    %% Memory mode: serialize entire index including vectors
    %% Don't serialize ETS table reference
    term_to_binary(Index#diskann_index{cache_table = undefined});
serialize(#diskann_index{storage_mode = disk} = Index) ->
    %% Disk mode: serialize only in-memory structures (no vectors, no cache)
    IndexToSave = Index#diskann_index{
        file_handle = undefined,
        cache_table = undefined,  %% ETS table not serializable
        vectors = #{}
    },
    term_to_binary(IndexToSave).

%% @doc Deserialize index from binary
-spec deserialize(binary()) -> {ok, diskann_index()} | {error, term()}.
deserialize(Binary) ->
    try
        case binary_to_term(Binary) of
            #diskann_index{} = Index ->
                %% Re-open disk files and create ETS table if in disk mode
                case Index#diskann_index.storage_mode of
                    disk ->
                        BasePath = Index#diskann_index.base_path,
                        %% Create new ETS cache table
                        CacheTable = create_cache_table(),
                        Index1 = Index#diskann_index{cache_table = CacheTable},
                        case BasePath of
                            undefined ->
                                {ok, Index1};
                            _ ->
                                case barrel_vectordb_diskann_file:open(BasePath) of
                                    {ok, FileHandle} ->
                                        {ok, Index1#diskann_index{file_handle = FileHandle}};
                                    {error, _} ->
                                        %% Files might not exist yet
                                        {ok, Index1}
                                end
                        end;
                    memory ->
                        {ok, Index}
                end;
            _ ->
                {error, invalid_format}
        end
    catch
        _:_ ->
            {error, invalid_binary}
    end.

%%====================================================================
%% Internal: PQ Training and Encoding
%%====================================================================

%% Train PQ on vectors and encode all vectors to PQ codes
train_and_apply_pq(Index, Options, Vectors) ->
    Dim = (Index#diskann_index.config)#diskann_config.dimension,
    M = maps:get(pq_m, Options, 8),
    K = maps:get(pq_k, Options, 256),

    %% Dimension must be divisible by M
    case Dim rem M of
        0 ->
            %% Create and train PQ
            {ok, PQConfig} = barrel_vectordb_pq:new(#{
                m => M,
                k => K,
                dimension => Dim
            }),
            VecList = [V || {_, V} <- Vectors],
            {ok, TrainedPQ} = barrel_vectordb_pq:train(PQConfig, VecList),

            %% Encode all vectors to PQ codes
            PQCodes = maps:from_list([
                {Id, barrel_vectordb_pq:encode(TrainedPQ, Vec)}
                || {Id, Vec} <- Vectors
            ]),

            Index#diskann_index{
                pq_state = TrainedPQ,
                pq_codes = PQCodes,
                use_pq = true
            };
        _ ->
            %% Can't use PQ with this dimension
            Index
    end.

%%====================================================================
%% Internal: Vamana Build
%%====================================================================

%% Find medoid (vector closest to centroid)
%% Uses the configured distance function for consistency
find_medoid(Vectors, #diskann_config{dimension = Dim} = Config) ->
    %% Compute centroid
    N = length(Vectors),
    Centroid = lists:foldl(
        fun({_Id, Vec}, Acc) ->
            [A + V || {A, V} <- lists:zip(Acc, Vec)]
        end,
        [0.0 || _ <- lists:seq(1, Dim)],
        Vectors
    ),
    NormCentroid = [C / N || C <- Centroid],

    %% Find closest to centroid using configured distance function
    {MedoidId, _MinDist} = lists:foldl(
        fun({Id, Vec}, {BestId, BestDist}) ->
            Dist = distance_vec(Config, Vec, NormCentroid),
            case Dist < BestDist of
                true -> {Id, Dist};
                false -> {BestId, BestDist}
            end
        end,
        {undefined, infinity},
        Vectors
    ),
    MedoidId.

%% Initialize random R-regular graph
init_random_graph(#diskann_index{config = Config} = Index, Ids) ->
    R = Config#diskann_config.r,
    N = length(Ids),
    IdsArray = list_to_tuple(Ids),

    Nodes = lists:foldl(
        fun(Id, Acc) ->
            %% Pick R random neighbors (excluding self)
            Neighbors = random_neighbors(Id, IdsArray, N, R),
            Acc#{Id => #diskann_node{id = Id, neighbors = Neighbors}}
        end,
        #{},
        Ids
    ),
    Index#diskann_index{nodes = Nodes}.

random_neighbors(Id, IdsArray, N, R) ->
    NumNeighbors = min(R, N - 1),
    random_neighbors(Id, IdsArray, N, NumNeighbors, []).

random_neighbors(_Id, _IdsArray, _N, 0, Acc) ->
    Acc;
random_neighbors(Id, IdsArray, N, Remaining, Acc) ->
    Idx = rand:uniform(N),
    Neighbor = element(Idx, IdsArray),
    case Neighbor =:= Id orelse lists:member(Neighbor, Acc) of
        true ->
            random_neighbors(Id, IdsArray, N, Remaining, Acc);
        false ->
            random_neighbors(Id, IdsArray, N, Remaining - 1, [Neighbor | Acc])
    end.

%% Single pass of Vamana construction
vamana_pass(#diskann_index{medoid_id = S, nodes = Nodes} = Index,
            Alpha, L, R) ->
    %% Random permutation of all node IDs
    Ids = maps:keys(Nodes),
    Sigma = shuffle(Ids),

    lists:foldl(
        fun(Id, AccIndex) ->
            %% Use get_vector_or_cached to support both memory and disk modes
            {Vec, _} = get_vector_or_cached(AccIndex, Id),

            %% Search to find candidates
            {_Results, Visited} = greedy_search(AccIndex, S, Vec, 1, L),

            %% Prune to select R out-neighbors
            AccIndex2 = robust_prune(AccIndex, Id, sets:to_list(Visited), Alpha, R),
            Neighbors = get_neighbors(AccIndex2, Id),

            %% Add backward edges (bidirectional)
            lists:foldl(
                fun(J, AccInner) ->
                    JNeighbors = get_neighbors(AccInner, J),
                    case length(JNeighbors) + 1 > R of
                        true ->
                            robust_prune(AccInner, J, [Id | JNeighbors], Alpha, R);
                        false ->
                            add_neighbor(AccInner, J, Id)
                    end
                end,
                AccIndex2,
                Neighbors
            )
        end,
        Index,
        Sigma
    ).

%%====================================================================
%% Internal: Vamana Build with Integer IDs (for disk mode)
%%====================================================================

%% Initialize random R-regular graph using integer IDs
init_random_graph_int(#diskann_index{config = Config} = Index, IntIds) ->
    R = Config#diskann_config.r,
    N = length(IntIds),
    IdsArray = list_to_tuple(IntIds),

    NodesInt = lists:foldl(
        fun(IntId, Acc) ->
            %% Pick R random neighbors (excluding self)
            Neighbors = random_neighbors_int(IntId, IdsArray, N, R),
            Acc#{IntId => Neighbors}
        end,
        #{},
        IntIds
    ),
    Index#diskann_index{nodes_int = NodesInt}.

random_neighbors_int(IntId, IdsArray, N, R) ->
    NumNeighbors = min(R, N - 1),
    random_neighbors_int(IntId, IdsArray, N, NumNeighbors, []).

random_neighbors_int(_IntId, _IdsArray, _N, 0, Acc) ->
    Acc;
random_neighbors_int(IntId, IdsArray, N, Remaining, Acc) ->
    Idx = rand:uniform(N),
    Neighbor = element(Idx, IdsArray),
    case Neighbor =:= IntId orelse lists:member(Neighbor, Acc) of
        true ->
            random_neighbors_int(IntId, IdsArray, N, Remaining, Acc);
        false ->
            random_neighbors_int(IntId, IdsArray, N, Remaining - 1, [Neighbor | Acc])
    end.

%% Single pass of Vamana construction using integer IDs
vamana_pass_int(#diskann_index{medoid_int_id = S, nodes_int = NodesInt} = Index,
                Alpha, L, R) ->
    %% Random permutation of all node integer IDs
    IntIds = maps:keys(NodesInt),
    Sigma = shuffle(IntIds),

    lists:foldl(
        fun(IntId, AccIndex) ->
            %% Get vector by integer ID
            Vec = get_vector_by_int_id(AccIndex, IntId),

            %% Search to find candidates
            {_Results, Visited} = greedy_search_int(AccIndex, S, Vec, 1, L),

            %% Prune to select R out-neighbors
            AccIndex2 = robust_prune_int(AccIndex, IntId, sets:to_list(Visited), Alpha, R),
            Neighbors = get_neighbors_int(AccIndex2, IntId),

            %% Add backward edges (bidirectional)
            lists:foldl(
                fun(J, AccInner) ->
                    JNeighbors = get_neighbors_int(AccInner, J),
                    case length(JNeighbors) + 1 > R of
                        true ->
                            robust_prune_int(AccInner, J, [IntId | JNeighbors], Alpha, R);
                        false ->
                            add_neighbor_int(AccInner, J, IntId)
                    end
                end,
                AccIndex2,
                Neighbors
            )
        end,
        Index,
        Sigma
    ).

%% Greedy search using integer IDs
greedy_search_int(Index, StartIntId, Query, K, L) ->
    StartDist = distance_int(Index, StartIntId, Query),
    Candidates = gb_trees:from_orddict([{{StartDist, StartIntId}, true}]),
    Results = gb_trees:from_orddict([{{StartDist, StartIntId}, true}]),
    Visited = sets:from_list([StartIntId]),
    FurthestDist = StartDist,

    greedy_loop_int(Index, Query, Candidates, Results, Visited, FurthestDist, K, L).

greedy_loop_int(_Index, _Query, {0, nil}, Results, Visited, _FurthestDist, K, _L) ->
    ResultList = [Item || {Item, _} <- gb_trees:to_list(Results)],
    TopK = lists:sublist(ResultList, K),
    {TopK, Visited};
greedy_loop_int(Index, Query, Candidates, Results, Visited, FurthestDist, K, L) ->
    {{CurrentDist, CurrentIntId}, _, RestCandidates} = gb_trees:take_smallest(Candidates),

    case CurrentDist > FurthestDist of
        true ->
            ResultList = [Item || {Item, _} <- gb_trees:to_list(Results)],
            TopK = lists:sublist(ResultList, K),
            {TopK, Visited};
        false ->
            Neighbors = get_neighbors_int(Index, CurrentIntId),
            {NewCandidates, NewResults, NewVisited, NewFurthestDist} = lists:foldl(
                fun(N, {CandAcc, ResAcc, VisAcc, FurthAcc}) ->
                    case sets:is_element(N, VisAcc) of
                        true ->
                            {CandAcc, ResAcc, VisAcc, FurthAcc};
                        false ->
                            NewVisAcc = sets:add_element(N, VisAcc),
                            D = distance_int(Index, N, Query),
                            ResSize = gb_trees:size(ResAcc),
                            ShouldAdd = D < FurthAcc orelse ResSize < L,
                            case ShouldAdd of
                                true ->
                                    NewCandAcc = gb_trees:insert({D, N}, true, CandAcc),
                                    NewResAcc0 = gb_trees:insert({D, N}, true, ResAcc),
                                    NewResSize = gb_trees:size(NewResAcc0),
                                    {NewResAcc, NewFurthAcc} = case NewResSize > L of
                                        true ->
                                            {_, _, Trimmed} = gb_trees:take_largest(NewResAcc0),
                                            {{LastD, _}, _} = gb_trees:largest(Trimmed),
                                            {Trimmed, LastD};
                                        false ->
                                            {{MaxD, _}, _} = gb_trees:largest(NewResAcc0),
                                            {NewResAcc0, MaxD}
                                    end,
                                    {NewCandAcc, NewResAcc, NewVisAcc, NewFurthAcc};
                                false ->
                                    {CandAcc, ResAcc, NewVisAcc, FurthAcc}
                            end
                    end
                end,
                {RestCandidates, Results, Visited, FurthestDist},
                Neighbors
            ),

            greedy_loop_int(Index, Query, NewCandidates, NewResults, NewVisited, NewFurthestDist, K, L)
    end.

%% RobustPrune using integer IDs
robust_prune_int(Index, P, V, Alpha, R) ->
    CurrentNeighbors = get_neighbors_int(Index, P),
    Candidates = lists:usort(V ++ CurrentNeighbors) -- [P],

    PVec = get_vector_by_int_id(Index, P),
    Config = Index#diskann_index.config,

    %% Build list with cached distances: [{Dist, IntId, Vec}]
    CandidatesWithDist = lists:filtermap(
        fun(C) ->
            CVec = get_vector_by_int_id(Index, C),
            case CVec of
                undefined -> false;
                _ -> {true, {distance_vec(Config, PVec, CVec), C, CVec}}
            end
        end,
        Candidates
    ),

    %% Sort by distance to P
    SortedCandidates = lists:sort(fun({D1, _, _}, {D2, _, _}) -> D1 =< D2 end, CandidatesWithDist),

    %% Prune with cached data
    NewNeighbors = prune_loop_cached_int(Config, PVec, SortedCandidates, Alpha, R, []),

    %% Update node
    set_neighbors_int(Index, P, NewNeighbors).

prune_loop_cached_int(_Config, _PVec, [], _Alpha, _R, Acc) ->
    lists:reverse(Acc);
prune_loop_cached_int(_Config, _PVec, _Candidates, _Alpha, R, Acc) when length(Acc) >= R ->
    lists:reverse(Acc);
prune_loop_cached_int(Config, PVec, [{_Dist, PStar, PStarVec} | Rest], Alpha, R, Acc) ->
    NewAcc = [PStar | Acc],
    FilteredRest = lists:filter(
        fun({DistP_PPrime, _PPrime, PPrimeVec}) ->
            DistPStar_PPrime = distance_vec(Config, PStarVec, PPrimeVec),
            Alpha * DistPStar_PPrime > DistP_PPrime
        end,
        Rest
    ),
    prune_loop_cached_int(Config, PVec, FilteredRest, Alpha, R, NewAcc).

%% Write all graph nodes to disk
write_graph_nodes_to_disk(#diskann_index{nodes_int = NodesInt, file_handle = FH,
                                          config = Config} = Index, IntIds) ->
    R = Config#diskann_config.r,
    lists:foreach(
        fun(IntId) ->
            Neighbors = maps:get(IntId, NodesInt, []),
            ok = barrel_vectordb_diskann_file:write_node_int(FH, IntId, Neighbors, R)
        end,
        IntIds
    ),
    Index.

%% Populate cache for build with integer IDs
populate_cache_for_build_int(CacheTable, IntIdVectors) when CacheTable =/= undefined ->
    Now = erlang:monotonic_time(),
    lists:foreach(
        fun({IntId, Vec, _StringId}) ->
            ets:insert(CacheTable, {IntId, Vec, Now})
        end,
        IntIdVectors
    );
populate_cache_for_build_int(_, _) ->
    ok.

%% Clear graph cache
clear_graph_cache(undefined) -> ok;
clear_graph_cache(CacheTable) ->
    ets:delete_all_objects(CacheTable).

%% Pre-warm both vector and graph caches with medoid neighbors
prewarm_caches(Index, MedoidIntId, MaxSize) ->
    %% Get medoid neighbors
    Neighbors = get_neighbors_int(Index, MedoidIntId),
    %% Load medoid and its neighbors into caches
    ToLoad = lists:sublist([MedoidIntId | Neighbors], MaxSize),
    lists:foreach(
        fun(IntId) ->
            %% Load vector into cache
            _ = get_vector_by_int_id(Index, IntId),
            %% Neighbors are loaded on-demand, but we can pre-load them
            _ = get_neighbors_int(Index, IntId)
        end,
        ToLoad
    ),
    ok.

%% Pre-warm cache for batch insert by loading 2-hop neighborhood vectors
%% Returns updated VectorMap with pre-loaded vectors for faster distance computation
prewarm_cache_for_batch(Index, MedoidIntId, VectorMap) ->
    %% Get 1-hop neighbors (medoid's neighbors)
    OneHop = get_neighbors_int(Index, MedoidIntId),

    %% Get 2-hop neighbors (neighbors of medoid's neighbors)
    TwoHop = lists:foldl(
        fun(N, Acc) ->
            NNeighbors = get_neighbors_int(Index, N),
            lists:usort(NNeighbors ++ Acc)
        end,
        [],
        OneHop
    ),

    %% Combine all nodes to pre-warm (medoid + 1-hop + 2-hop), remove duplicates
    AllNodes = lists:usort([MedoidIntId | OneHop] ++ TwoHop),

    %% Load vectors into VectorMap for fast access during Vamana pass
    %% Also populates ETS cache as a side effect
    lists:foldl(
        fun(IntId, AccMap) ->
            case maps:is_key(IntId, AccMap) of
                true -> AccMap;  %% Already in map (new vector)
                false ->
                    case get_vector_by_int_id(Index, IntId) of
                        undefined -> AccMap;
                        Vec -> maps:put(IntId, Vec, AccMap)
                    end
            end
        end,
        VectorMap,
        AllNodes
    ).

%% Train and apply PQ with integer IDs
train_and_apply_pq_int(Index, Options, IntIdVectors) ->
    Dim = (Index#diskann_index.config)#diskann_config.dimension,
    M = maps:get(pq_m, Options, 8),
    K = maps:get(pq_k, Options, 256),

    case Dim rem M of
        0 ->
            {ok, PQConfig} = barrel_vectordb_pq:new(#{
                m => M,
                k => K,
                dimension => Dim
            }),
            VecList = [Vec || {_, Vec, _} <- IntIdVectors],
            {ok, TrainedPQ} = barrel_vectordb_pq:train(PQConfig, VecList),

            %% Encode all vectors to PQ codes with integer IDs
            PQCodesInt = maps:from_list([
                {IntId, barrel_vectordb_pq:encode(TrainedPQ, Vec)}
                || {IntId, Vec, _} <- IntIdVectors
            ]),
            %% Also keep string ID version for compatibility
            PQCodes = maps:from_list([
                {StringId, barrel_vectordb_pq:encode(TrainedPQ, Vec)}
                || {_, Vec, StringId} <- IntIdVectors
            ]),

            Index#diskann_index{
                pq_state = TrainedPQ,
                pq_codes = PQCodes,
                pq_codes_int = PQCodesInt,
                use_pq = true
            };
        _ ->
            Index
    end.

shuffle(List) ->
    [X || {_, X} <- lists:sort([{rand:uniform(), N} || N <- List])].

%%====================================================================
%% Internal: GreedySearch (Algorithm 1 from DiskANN paper)
%%====================================================================

%% @doc Core search algorithm - optimized version
%% Uses separate candidate queue and result set for efficiency
%% Returns ({SortedResults, VisitedSet})
greedy_search(Index, StartId, Query, K, L) ->
    StartDist = distance(Index, StartId, Query),
    %% Candidates: min-heap of nodes to explore
    Candidates = gb_trees:from_orddict([{{StartDist, StartId}, true}]),
    %% Results: best L nodes found so far
    Results = gb_trees:from_orddict([{{StartDist, StartId}, true}]),
    %% Visited: nodes we've already expanded
    Visited = sets:from_list([StartId]),
    %% Track furthest result distance for pruning
    FurthestDist = StartDist,

    greedy_loop(Index, Query, Candidates, Results, Visited, FurthestDist, K, L).

greedy_loop(_Index, _Query, {0, nil}, Results, Visited, _FurthestDist, K, _L) ->
    %% No more candidates
    ResultList = [Item || {Item, _} <- gb_trees:to_list(Results)],
    TopK = lists:sublist(ResultList, K),
    {TopK, Visited};
greedy_loop(Index, Query, Candidates, Results, Visited, FurthestDist, K, L) ->
    %% Get closest candidate
    {{CurrentDist, CurrentId}, _, RestCandidates} = gb_trees:take_smallest(Candidates),

    %% If closest candidate is further than our furthest result, we're done
    case CurrentDist > FurthestDist of
        true ->
            ResultList = [Item || {Item, _} <- gb_trees:to_list(Results)],
            TopK = lists:sublist(ResultList, K),
            {TopK, Visited};
        false ->
            %% Expand neighbors
            Neighbors = get_neighbors(Index, CurrentId),
            {NewCandidates, NewResults, NewVisited, NewFurthestDist} = lists:foldl(
                fun(N, {CandAcc, ResAcc, VisAcc, FurthAcc}) ->
                    case sets:is_element(N, VisAcc) of
                        true ->
                            {CandAcc, ResAcc, VisAcc, FurthAcc};
                        false ->
                            NewVisAcc = sets:add_element(N, VisAcc),
                            D = distance(Index, N, Query),
                            ResSize = gb_trees:size(ResAcc),
                            ShouldAdd = D < FurthAcc orelse ResSize < L,
                            case ShouldAdd of
                                true ->
                                    NewCandAcc = gb_trees:insert({D, N}, true, CandAcc),
                                    NewResAcc0 = gb_trees:insert({D, N}, true, ResAcc),
                                    %% Trim results if too many
                                    NewResSize = gb_trees:size(NewResAcc0),
                                    {NewResAcc, NewFurthAcc} = case NewResSize > L of
                                        true ->
                                            {_, _, Trimmed} = gb_trees:take_largest(NewResAcc0),
                                            {{LastD, _}, _} = gb_trees:largest(Trimmed),
                                            {Trimmed, LastD};
                                        false ->
                                            {{MaxD, _}, _} = gb_trees:largest(NewResAcc0),
                                            {NewResAcc0, MaxD}
                                    end,
                                    {NewCandAcc, NewResAcc, NewVisAcc, NewFurthAcc};
                                false ->
                                    {CandAcc, ResAcc, NewVisAcc, FurthAcc}
                            end
                    end
                end,
                {RestCandidates, Results, Visited, FurthestDist},
                Neighbors
            ),

            greedy_loop(Index, Query, NewCandidates, NewResults, NewVisited, NewFurthestDist, K, L)
    end.

%%====================================================================
%% Internal: BeamSearch with PQ (Optimized for DiskANN)
%%====================================================================

%% Beam search using PQ distance tables for fast approximate distance
%% Returns sorted results [{Distance, Id}]
beam_search_pq(Index, StartId, Query, DistTables, K, L) ->
    #diskann_index{pq_codes = PQCodes} = Index,

    %% Get PQ distance to start node
    StartDist = case maps:find(StartId, PQCodes) of
        {ok, Code} -> barrel_vectordb_pq:distance(DistTables, Code);
        error -> infinity
    end,

    %% Candidates: min-heap of nodes to explore
    Candidates = gb_trees:from_orddict([{{StartDist, StartId}, true}]),
    %% Results: best L nodes found so far
    Results = gb_trees:from_orddict([{{StartDist, StartId}, true}]),
    %% Visited: nodes we've already expanded
    Visited = sets:from_list([StartId]),
    FurthestDist = StartDist,

    %% Run beam search with PQ distances
    PQResults = beam_search_pq_loop(Index, DistTables, PQCodes, Candidates,
                                     Results, Visited, FurthestDist, K, L),

    %% Rerank top results with full vectors for accuracy (using lazy disk loading)
    rerank_with_full_vectors(PQResults, Query, Index, K).

beam_search_pq_loop(_Index, _DistTables, _PQCodes, {0, nil}, Results, _Visited, _FurthestDist, K, _L) ->
    %% No more candidates
    ResultList = [Item || {Item, _} <- gb_trees:to_list(Results)],
    lists:sublist(ResultList, K);
beam_search_pq_loop(Index, DistTables, PQCodes, Candidates, Results, Visited, FurthestDist, K, L) ->
    %% Get closest candidate
    {{CurrentDist, CurrentId}, _, RestCandidates} = gb_trees:take_smallest(Candidates),

    %% If closest candidate is further than our furthest result, we're done
    case CurrentDist > FurthestDist of
        true ->
            ResultList = [Item || {Item, _} <- gb_trees:to_list(Results)],
            lists:sublist(ResultList, K);
        false ->
            %% Expand neighbors
            Neighbors = get_neighbors(Index, CurrentId),
            {NewCandidates, NewResults, NewVisited, NewFurthestDist} = lists:foldl(
                fun(N, {CandAcc, ResAcc, VisAcc, FurthAcc}) ->
                    case sets:is_element(N, VisAcc) of
                        true ->
                            {CandAcc, ResAcc, VisAcc, FurthAcc};
                        false ->
                            NewVisAcc = sets:add_element(N, VisAcc),
                            %% Use PQ distance (fast O(M) lookup)
                            D = case maps:find(N, PQCodes) of
                                {ok, Code} -> barrel_vectordb_pq:distance(DistTables, Code);
                                error -> infinity
                            end,
                            ResSize = gb_trees:size(ResAcc),
                            ShouldAdd = D < FurthAcc orelse ResSize < L,
                            case ShouldAdd of
                                true ->
                                    NewCandAcc = gb_trees:insert({D, N}, true, CandAcc),
                                    NewResAcc0 = gb_trees:insert({D, N}, true, ResAcc),
                                    %% Trim results if too many
                                    NewResSize = gb_trees:size(NewResAcc0),
                                    {NewResAcc, NewFurthAcc} = case NewResSize > L of
                                        true ->
                                            {_, _, Trimmed} = gb_trees:take_largest(NewResAcc0),
                                            {{LastD, _}, _} = gb_trees:largest(Trimmed),
                                            {Trimmed, LastD};
                                        false ->
                                            {{MaxD, _}, _} = gb_trees:largest(NewResAcc0),
                                            {NewResAcc0, MaxD}
                                    end,
                                    {NewCandAcc, NewResAcc, NewVisAcc, NewFurthAcc};
                                false ->
                                    {CandAcc, ResAcc, NewVisAcc, FurthAcc}
                            end
                    end
                end,
                {RestCandidates, Results, Visited, FurthestDist},
                Neighbors
            ),

            beam_search_pq_loop(Index, DistTables, PQCodes, NewCandidates,
                                NewResults, NewVisited, NewFurthestDist, K, L)
    end.

%% Rerank top candidates using full vectors for accuracy
%% Now uses Index for lazy vector loading from disk
rerank_with_full_vectors(PQResults, Query, Index, K) ->
    Config = Index#diskann_index.config,
    %% Compute exact distances for top candidates
    Reranked = lists:map(
        fun({_PQDist, Id}) ->
            {Vec, _} = get_vector_or_cached(Index, Id),
            case Vec of
                undefined ->
                    {infinity, Id};
                _ ->
                    ExactDist = distance_vec(Config, Query, Vec),
                    {ExactDist, Id}
            end
        end,
        PQResults
    ),
    %% Sort by exact distance and take K
    Sorted = lists:sort(Reranked),
    lists:sublist(Sorted, K).

%%====================================================================
%% Internal: RobustPrune (Algorithm 2 from DiskANN paper)
%%====================================================================

%% @doc RobustPrune: Select R neighbors for node P using alpha-RNG pruning
%% V = candidate neighbors
%% Alpha > 1 keeps more long-range edges
%% Optimized: cache distance computations
robust_prune(Index, P, V, Alpha, R) ->
    %% V <- (V ∪ N_out(P)) \ {P}
    CurrentNeighbors = get_neighbors(Index, P),
    Candidates = lists:usort(V ++ CurrentNeighbors) -- [P],

    %% Pre-compute distances from P to all candidates (cache)
    {PVec, _} = get_vector_or_cached(Index, P),
    Config = Index#diskann_index.config,

    %% Build list with cached distances: [{Dist, Id, Vec}]
    CandidatesWithDist = lists:filtermap(
        fun(C) ->
            {CVec, _} = get_vector_or_cached(Index, C),
            case CVec of
                undefined -> false;
                _ -> {true, {distance_vec(Config, PVec, CVec), C, CVec}}
            end
        end,
        Candidates
    ),

    %% Sort by distance to P
    SortedCandidates = lists:sort(fun({D1, _, _}, {D2, _, _}) -> D1 =< D2 end, CandidatesWithDist),

    %% Prune with cached data
    NewNeighbors = prune_loop_cached(Config, PVec, SortedCandidates, Alpha, R, []),

    %% Update node
    set_neighbors(Index, P, NewNeighbors).

%% Alpha-RNG pruning with cached vectors - avoids repeated map lookups
prune_loop_cached(_Config, _PVec, [], _Alpha, _R, Acc) ->
    lists:reverse(Acc);
prune_loop_cached(_Config, _PVec, _Candidates, _Alpha, R, Acc) when length(Acc) >= R ->
    lists:reverse(Acc);
prune_loop_cached(Config, PVec, [{_Dist, PStar, PStarVec} | Rest], Alpha, R, Acc) ->
    %% Add p* to neighbors
    NewAcc = [PStar | Acc],

    %% Filter out candidates that are closer to p* than to P (with alpha factor)
    FilteredRest = lists:filter(
        fun({DistP_PPrime, _PPrime, PPrimeVec}) ->
            DistPStar_PPrime = distance_vec(Config, PStarVec, PPrimeVec),
            %% Keep p' only if alpha * d(p*, p') > d(p, p')
            Alpha * DistPStar_PPrime > DistP_PPrime
        end,
        Rest
    ),

    prune_loop_cached(Config, PVec, FilteredRest, Alpha, R, NewAcc).

%% Original prune_neighbors kept for compatibility with consolidate_deletes
prune_neighbors(Index, P, Candidates, Alpha, R) ->
    {PVec, _} = get_vector_or_cached(Index, P),
    Config = Index#diskann_index.config,
    CandidatesWithDist = lists:filtermap(
        fun(C) ->
            {CVec, _} = get_vector_or_cached(Index, C),
            case CVec of
                undefined -> false;
                _ -> {true, {distance_vec(Config, PVec, CVec), C, CVec}}
            end
        end,
        Candidates
    ),
    SortedCandidates = lists:sort(fun({D1, _, _}, {D2, _, _}) -> D1 =< D2 end, CandidatesWithDist),
    prune_loop_cached(Config, PVec, SortedCandidates, Alpha, R, []).

%%====================================================================
%% Internal: ETS-based Vector Cache (LRU) - for disk mode
%%====================================================================

%% Create a new ETS table for caching vectors
create_cache_table() ->
    ets:new(diskann_vector_cache, [set, public, {read_concurrency, true}]).

%% Clear all entries from cache table
clear_cache_table(undefined) -> ok;
clear_cache_table(CacheTable) ->
    ets:delete_all_objects(CacheTable).

%% Get vector: check cache/memory first, load from disk if needed
get_vector_cached(#diskann_index{storage_mode = memory, vectors = Vectors}, Id) ->
    %% Memory mode: vectors are in the vectors map
    case maps:find(Id, Vectors) of
        {ok, Vec} -> {Vec, ok};
        error -> {undefined, ok}
    end;
get_vector_cached(#diskann_index{storage_mode = disk, cache_table = CacheTable} = Index, Id) ->
    %% Disk mode: check ETS cache first
    case cache_get(CacheTable, Id) of
        {ok, Vec} ->
            %% Cache hit - touch to update LRU timestamp
            cache_touch(CacheTable, Id),
            {Vec, ok};
        not_found ->
            %% Cache miss - load from disk
            load_and_cache_vector(Index, Id)
    end.

%% Load vector from disk and add to ETS cache
load_and_cache_vector(#diskann_index{file_handle = undefined}, _Id) ->
    %% No file handle - can't load
    {undefined, ok};
load_and_cache_vector(Index, Id) ->
    case maps:find(Id, Index#diskann_index.id_to_idx) of
        {ok, Idx} ->
            case barrel_vectordb_diskann_file:read_vector(
                    Index#diskann_index.file_handle, Idx) of
                {ok, Vec} ->
                    %% Add to ETS cache with LRU eviction
                    cache_put_with_eviction(Index, Id, Vec),
                    {Vec, ok};
                {error, _} ->
                    {undefined, ok}
            end;
        error ->
            {undefined, ok}
    end.

%% Get from ETS cache
cache_get(undefined, _Id) -> not_found;
cache_get(CacheTable, Id) ->
    case ets:lookup(CacheTable, Id) of
        [{Id, Vec, _Timestamp}] -> {ok, Vec};
        [] -> not_found
    end.

%% Touch cache entry (update timestamp for LRU)
cache_touch(undefined, _Id) -> ok;
cache_touch(CacheTable, Id) ->
    Now = erlang:monotonic_time(),
    ets:update_element(CacheTable, Id, {3, Now}),
    ok.

%% Put into cache with LRU eviction if full
cache_put_with_eviction(#diskann_index{cache_table = CacheTable,
                                        cache_max_size = MaxSize}, Id, Vec) ->
    case CacheTable of
        undefined -> ok;
        _ ->
            Now = erlang:monotonic_time(),
            Size = ets:info(CacheTable, size),
            case Size >= MaxSize of
                true ->
                    %% Evict oldest entries (batch evict ~10%)
                    evict_oldest(CacheTable, max(1, MaxSize div 10));
                false ->
                    ok
            end,
            ets:insert(CacheTable, {Id, Vec, Now}),
            ok
    end.

%% Evict N oldest entries from cache
evict_oldest(CacheTable, N) ->
    %% Get all entries with timestamps
    Entries = ets:tab2list(CacheTable),
    %% Sort by timestamp (oldest first)
    Sorted = lists:sort(fun({_, _, T1}, {_, _, T2}) -> T1 < T2 end, Entries),
    %% Delete oldest N
    ToDelete = lists:sublist(Sorted, N),
    lists:foreach(
        fun({Id, _, _}) ->
            ets:delete(CacheTable, Id)
        end,
        ToDelete
    ).

%% Get vector with fallback (for internal use during build/pruning)
get_vector_or_cached(Index, Id) ->
    case Index#diskann_index.storage_mode of
        memory ->
            case maps:find(Id, Index#diskann_index.vectors) of
                {ok, Vec} -> {Vec, ok};
                error ->
                    %% Fallback to ETS cache (during transition)
                    case cache_get(Index#diskann_index.cache_table, Id) of
                        {ok, Vec} -> {Vec, ok};
                        not_found -> {undefined, ok}
                    end
            end;
        disk ->
            get_vector_cached(Index, Id)
    end.

%%====================================================================
%% Internal: Distance Functions
%%====================================================================

%% Distance using lazy vector loading
distance(Index, Id, QueryVec) ->
    {NodeVec, _Index2} = get_vector_or_cached(Index, Id),
    case NodeVec of
        undefined -> infinity;
        _ -> distance_vec(Index#diskann_index.config, QueryVec, NodeVec)
    end.

%% Fast path for pre-normalized vectors (common in embedding models)
%% For ||v|| = 1: cosine_distance = 1 - dot(v1, v2)
%% Eliminates 2 sqrt() calls and 6N multiplications/additions per distance
distance_vec(#diskann_config{distance_fn = cosine, assume_normalized = true}, Vec1, Vec2) ->
    1.0 - dot_product(Vec1, Vec2);
distance_vec(#diskann_config{distance_fn = cosine}, Vec1, Vec2) ->
    cosine_distance(Vec1, Vec2);
distance_vec(#diskann_config{distance_fn = euclidean}, Vec1, Vec2) ->
    euclidean_distance(Vec1, Vec2).

%% Fast dot product for normalized vectors
dot_product(Vec1, Vec2) ->
    dot_product(Vec1, Vec2, 0.0).

dot_product([], [], Acc) -> Acc;
dot_product([A | R1], [B | R2], Acc) ->
    dot_product(R1, R2, Acc + A * B).

%% Optimized cosine distance - avoids redundant norm calculations
%% For normalized vectors, this simplifies to 1 - dot(v1, v2)
cosine_distance(Vec1, Vec2) ->
    {Dot, SumSq1, SumSq2} = dot_and_norms(Vec1, Vec2),
    Denom = math:sqrt(SumSq1 * SumSq2),
    case Denom < 1.0e-10 of
        true -> 1.0;
        false -> 1.0 - (Dot / Denom)
    end.

%% Optimized: compute dot product and squared norms in single pass
dot_and_norms(Vec1, Vec2) ->
    dot_and_norms(Vec1, Vec2, 0.0, 0.0, 0.0).

dot_and_norms([], [], Dot, SumSq1, SumSq2) ->
    {Dot, SumSq1, SumSq2};
dot_and_norms([A | Rest1], [B | Rest2], Dot, SumSq1, SumSq2) ->
    dot_and_norms(Rest1, Rest2, Dot + A * B, SumSq1 + A * A, SumSq2 + B * B).

%% Optimized euclidean distance - avoid math:pow
euclidean_distance(Vec1, Vec2) ->
    SumSq = sum_sq_diff(Vec1, Vec2, 0.0),
    math:sqrt(SumSq).

sum_sq_diff([], [], Acc) -> Acc;
sum_sq_diff([A | Rest1], [B | Rest2], Acc) ->
    Diff = A - B,
    sum_sq_diff(Rest1, Rest2, Acc + Diff * Diff).


%%====================================================================
%% Internal: Graph Operations
%%====================================================================

get_neighbors(#diskann_index{nodes = Nodes}, Id) ->
    case maps:find(Id, Nodes) of
        {ok, #diskann_node{neighbors = Ns}} -> Ns;
        error -> []
    end.

set_neighbors(#diskann_index{nodes = Nodes} = Index, Id, Neighbors) ->
    case maps:find(Id, Nodes) of
        {ok, Node} ->
            NewNode = Node#diskann_node{neighbors = Neighbors},
            Index#diskann_index{nodes = Nodes#{Id => NewNode}};
        error ->
            Index
    end.

add_neighbor(#diskann_index{nodes = Nodes} = Index, NodeId, NewNeighborId) ->
    case maps:find(NodeId, Nodes) of
        {ok, #diskann_node{neighbors = Ns} = Node} ->
            case lists:member(NewNeighborId, Ns) of
                true -> Index;
                false ->
                    NewNode = Node#diskann_node{neighbors = [NewNeighborId | Ns]},
                    Index#diskann_index{nodes = Nodes#{NodeId => NewNode}}
            end;
        error ->
            Index
    end.

%%====================================================================
%% Internal: RocksDB ID Mapping (for billion-scale disk mode)
%%====================================================================

%% Open or get RocksDB for ID mapping
%% First tries to get handles from barrel_vectordb_store (shared RocksDB)
%% Falls back to opening a standalone database if store is not running
%% Returns {ok, Db, CfFwd, CfRev, Standalone} where Standalone is true if we opened our own DB
-spec open_id_db(binary(), none | map()) ->
    {ok, rocksdb:db_handle(), rocksdb:cf_handle(), rocksdb:cf_handle(),
     boolean(), term()} | {error, term()}.
open_id_db(BasePath, none) ->
    %% Try to get handles from the shared barrel_vectordb_store first
    case catch barrel_vectordb_store:get_diskann_db() of
        {ok, {Db, CfFwd, CfRev}} ->
            {ok, Db, CfFwd, CfRev, false, undefined};  %% Not standalone - managed by store
        _ ->
            %% Store not running, open standalone database for testing/standalone use
            case open_standalone_id_db(BasePath, undefined) of
                {ok, Db, CfFwd, CfRev} ->
                    {ok, Db, CfFwd, CfRev, true, undefined};  %% Standalone - we manage it
                {error, _} = Err ->
                    Err
            end
    end;
open_id_db(BasePath, #{key := Key} = Crypto) ->
    %% An encrypted index never shares the global store's plaintext db:
    %% always standalone under the caller's env (or one minted here)
    Env = case Crypto of
        #{env := E} ->
            E;
        _ ->
            {ok, E} = rocksdb:new_env({encrypted, Key}),
            E
    end,
    case open_standalone_id_db(BasePath, Env) of
        {ok, Db, CfFwd, CfRev} ->
            {ok, Db, CfFwd, CfRev, true, Env};
        {error, _} = Err ->
            Err
    end.

%% Open standalone RocksDB for ID mapping (for tests or standalone mode)
%% Also tracks open databases in process dictionary for cleanup
open_standalone_id_db(BasePath, Env) ->
    DbPath = filename:join(BasePath, "diskann_ids"),
    ok = filelib:ensure_dir(filename:join(DbPath, "dummy")),
    CfNames = ["default", "ids_fwd", "ids_rev"],
    DbOpts0 = [{create_if_missing, true}, {create_missing_column_families, true}],
    DbOpts = case Env of
        undefined -> DbOpts0;
        _ -> [{env, Env} | DbOpts0]
    end,
    CfOpts = [],
    case rocksdb:open(binary_to_list(DbPath), DbOpts,
                      [{Name, CfOpts} || Name <- CfNames]) of
        {ok, Db, [_Default, CfFwd, CfRev]} ->
            %% Track open database for cleanup
            register_standalone_db(Db),
            {ok, Db, CfFwd, CfRev};
        {error, _} = Err ->
            Err
    end.

%% Track open standalone databases in process dictionary
register_standalone_db(Db) ->
    OpenDbs = case get(diskann_open_standalone_dbs) of
        undefined -> [];
        List -> List
    end,
    put(diskann_open_standalone_dbs, [Db | OpenDbs]).

unregister_standalone_db(Db) ->
    case get(diskann_open_standalone_dbs) of
        undefined -> ok;
        List -> put(diskann_open_standalone_dbs, lists:delete(Db, List))
    end.

%% Close all open standalone databases (for test cleanup)
-spec close_all_standalone_dbs() -> ok.
close_all_standalone_dbs() ->
    case get(diskann_open_standalone_dbs) of
        undefined -> ok;
        List ->
            lists:foreach(fun(Db) ->
                catch rocksdb:close(Db)
            end, List),
            put(diskann_open_standalone_dbs, [])
    end,
    ok.

%% Get or create integer ID for string ID
%% Returns {IntId, UpdatedIndex}
-spec get_or_create_int_id(diskann_index(), binary()) -> {non_neg_integer(), diskann_index()}.
get_or_create_int_id(#diskann_index{id_db = undefined} = Index, StringId) ->
    %% Fallback to in-memory maps when RocksDB not available
    case maps:find(StringId, Index#diskann_index.id_to_idx) of
        {ok, IntId} ->
            {IntId, Index};
        error ->
            IntId = Index#diskann_index.next_int_id,
            NewIdToIdx = maps:put(StringId, IntId, Index#diskann_index.id_to_idx),
            NewIdxToId = maps:put(IntId, StringId, Index#diskann_index.idx_to_id),
            {IntId, Index#diskann_index{
                id_to_idx = NewIdToIdx,
                idx_to_id = NewIdxToId,
                next_int_id = IntId + 1
            }}
    end;
get_or_create_int_id(#diskann_index{id_db = Db, id_cf_fwd = CfFwd, id_cf_rev = CfRev,
                                     next_int_id = NextId} = Index, StringId) ->
    case rocksdb:get(Db, CfFwd, StringId, []) of
        {ok, <<IntId:64/little>>} ->
            {IntId, Index};
        not_found ->
            IntId = NextId,
            ok = rocksdb:put(Db, CfFwd, StringId, <<IntId:64/little>>, []),
            ok = rocksdb:put(Db, CfRev, <<IntId:64/little>>, StringId, []),
            {IntId, Index#diskann_index{next_int_id = NextId + 1}};
        {error, Reason} ->
            error({rocksdb_error, Reason})
    end.

%% Lookup string ID from integer ID
-spec int_id_to_string(diskann_index(), non_neg_integer()) -> {ok, binary()} | not_found.
int_id_to_string(#diskann_index{id_db = undefined} = Index, IntId) ->
    %% Fallback to in-memory maps
    case maps:find(IntId, Index#diskann_index.idx_to_id) of
        {ok, StringId} -> {ok, StringId};
        error -> not_found
    end;
int_id_to_string(#diskann_index{id_db = Db, id_cf_rev = CfRev}, IntId) ->
    case rocksdb:get(Db, CfRev, <<IntId:64/little>>, []) of
        {ok, StringId} -> {ok, StringId};
        not_found -> not_found;
        {error, Reason} -> error({rocksdb_error, Reason})
    end.

%% Lookup integer ID from string ID
-spec string_id_to_int(diskann_index(), binary()) -> {ok, non_neg_integer()} | not_found.
string_id_to_int(#diskann_index{id_db = undefined} = Index, StringId) ->
    %% Fallback to in-memory maps
    case maps:find(StringId, Index#diskann_index.id_to_idx) of
        {ok, IntId} -> {ok, IntId};
        error -> not_found
    end;
string_id_to_int(#diskann_index{id_db = Db, id_cf_fwd = CfFwd}, StringId) ->
    case rocksdb:get(Db, CfFwd, StringId, []) of
        {ok, <<IntId:64/little>>} -> {ok, IntId};
        not_found -> not_found;
        {error, Reason} -> error({rocksdb_error, Reason})
    end.

%% Close RocksDB ID mapping (only close if standalone)
close_id_db(undefined, _) -> ok;
close_id_db(_, false) -> ok;  %% Not standalone - shared with barrel_vectordb_store
close_id_db(Db, true) ->
    unregister_standalone_db(Db),
    case rocksdb:close(Db) of
        ok -> ok;
        {error, Reason} ->
            error_logger:warning_msg("Failed to close RocksDB: ~p~n", [Reason]),
            ok
    end.

%%====================================================================
%% Internal: Graph Cache for Lazy Loading (disk mode)
%%====================================================================

%% Create ETS table for graph cache (IntId -> [NeighborIntIds])
create_graph_cache_table() ->
    ets:new(diskann_graph_cache, [set, public, {read_concurrency, true}]).

%% Get neighbors from graph cache
graph_cache_get(undefined, _IntId) -> not_found;
graph_cache_get(CacheTable, IntId) ->
    case ets:lookup(CacheTable, IntId) of
        [{IntId, Neighbors, _Timestamp}] -> {ok, Neighbors};
        [] -> not_found
    end.

%% Touch graph cache entry (update LRU timestamp)
graph_cache_touch(undefined, _IntId) -> ok;
graph_cache_touch(CacheTable, IntId) ->
    Now = erlang:monotonic_time(),
    ets:update_element(CacheTable, IntId, {3, Now}),
    ok.

%% Put into graph cache with LRU eviction
graph_cache_put_with_eviction(CacheTable, IntId, Neighbors, MaxSize) ->
    case CacheTable of
        undefined -> ok;
        _ ->
            Now = erlang:monotonic_time(),
            Size = ets:info(CacheTable, size),
            case Size >= MaxSize of
                true ->
                    %% Evict oldest entries (batch evict ~10%)
                    evict_oldest(CacheTable, max(1, MaxSize div 10));
                false ->
                    ok
            end,
            ets:insert(CacheTable, {IntId, Neighbors, Now}),
            ok
    end.

%% Get neighbors using integer IDs (lazy loading from disk)
get_neighbors_int(#diskann_index{storage_mode = memory, nodes_int = NodesInt}, IntId) ->
    maps:get(IntId, NodesInt, []);
get_neighbors_int(#diskann_index{storage_mode = disk, graph_cache_table = Cache,
                                  file_handle = FH, graph_cache_max_size = MaxSize,
                                  config = Config} = _Index, IntId) ->
    case graph_cache_get(Cache, IntId) of
        {ok, Neighbors} ->
            graph_cache_touch(Cache, IntId),
            Neighbors;
        not_found ->
            %% Load from disk: Node offset = header + IntId * sector_size
            R = Config#diskann_config.r,
            case barrel_vectordb_diskann_file:read_node_by_int_id(FH, IntId, R) of
                {ok, {_IntId, Neighbors}} ->
                    graph_cache_put_with_eviction(Cache, IntId, Neighbors, MaxSize),
                    Neighbors;
                {error, _} ->
                    []
            end
    end.

%% Set neighbors using integer IDs
set_neighbors_int(#diskann_index{storage_mode = memory, nodes_int = NodesInt} = Index, IntId, Neighbors) ->
    Index#diskann_index{nodes_int = NodesInt#{IntId => Neighbors}};
set_neighbors_int(#diskann_index{storage_mode = disk, graph_cache_table = Cache,
                                  file_handle = FH, config = Config} = Index, IntId, Neighbors) ->
    %% Write to disk and update cache
    R = Config#diskann_config.r,
    ok = barrel_vectordb_diskann_file:write_node_int(FH, IntId, Neighbors, R),
    %% Update cache
    Now = erlang:monotonic_time(),
    case Cache of
        undefined -> ok;
        _ -> ets:insert(Cache, {IntId, Neighbors, Now})
    end,
    Index.

%% Add neighbor using integer IDs
add_neighbor_int(Index, NodeIntId, NewNeighborIntId) ->
    Neighbors = get_neighbors_int(Index, NodeIntId),
    case lists:member(NewNeighborIntId, Neighbors) of
        true -> Index;
        false -> set_neighbors_int(Index, NodeIntId, [NewNeighborIntId | Neighbors])
    end.

%%====================================================================
%% Internal: Distance with Integer IDs
%%====================================================================

%% Distance using integer ID for node lookup
distance_int(Index, IntId, QueryVec) ->
    Vec = get_vector_by_int_id(Index, IntId),
    case Vec of
        undefined -> infinity;
        _ -> distance_vec(Index#diskann_index.config, QueryVec, Vec)
    end.

%% Get vector by integer ID (uses ETS cache for disk mode)
get_vector_by_int_id(#diskann_index{storage_mode = memory, idx_to_id = IdxToId} = Index, IntId) ->
    case maps:find(IntId, IdxToId) of
        {ok, StringId} ->
            case maps:find(StringId, Index#diskann_index.vectors) of
                {ok, Vec} -> Vec;
                error -> undefined
            end;
        error -> undefined
    end;
get_vector_by_int_id(#diskann_index{storage_mode = disk, cache_table = CacheTable,
                                     file_handle = FH, cache_max_size = MaxSize} = _Index, IntId) ->
    %% Check ETS cache first (keyed by IntId for disk mode)
    case cache_get(CacheTable, IntId) of
        {ok, Vec} ->
            cache_touch(CacheTable, IntId),
            Vec;
        not_found ->
            %% Load from disk using mmap (zero-copy) if available
            case barrel_vectordb_diskann_file:read_vector_mmap(FH, IntId) of
                {ok, Vec} ->
                    cache_put_with_eviction_by_key(CacheTable, IntId, Vec, MaxSize),
                    Vec;
                {error, _} ->
                    undefined
            end
    end.

%% Put into cache with LRU eviction by key (for integer IDs)
cache_put_with_eviction_by_key(undefined, _Key, _Vec, _MaxSize) -> ok;
cache_put_with_eviction_by_key(CacheTable, Key, Vec, MaxSize) ->
    Now = erlang:monotonic_time(),
    Size = ets:info(CacheTable, size),
    case Size >= MaxSize of
        true ->
            evict_oldest(CacheTable, max(1, MaxSize div 10));
        false ->
            ok
    end,
    ets:insert(CacheTable, {Key, Vec, Now}),
    ok.

%%====================================================================
%% Hot Layer Functions
%%====================================================================

%% @doc Insert a vector into the hot layer (in-memory, sub-millisecond)
%% Returns {ok, NewIndex} or {compact_needed, NewIndex} when threshold reached
-spec hot_insert(diskann_index(), binary(), [float()]) ->
    {ok, diskann_index()} | {compact_needed, diskann_index()} | {error, term()}.
hot_insert(#diskann_index{config = Config} = Index, Id, Vector) ->
    Dim = Config#diskann_config.dimension,
    case length(Vector) of
        Dim ->
            hot_insert_validated(Index, Id, Vector);
        Other ->
            {error, {dimension_mismatch, Dim, Other}}
    end.

hot_insert_validated(#diskann_index{hot_size = 0} = Index, Id, Vector) ->
    %% First insertion into hot layer - set as entry point
    HotIntId = 0,
    NewIndex = Index#diskann_index{
        hot_vectors = #{HotIntId => Vector},
        hot_graph = #{HotIntId => []},
        hot_id_to_int = #{Id => HotIntId},
        hot_int_to_id = #{HotIntId => Id},
        hot_next_int_id = 1,
        hot_size = 1
    },
    {ok, NewIndex};
hot_insert_validated(#diskann_index{config = Config,
                                     hot_vectors = HotVecs,
                                     hot_graph = HotGraph,
                                     hot_id_to_int = HotIdToInt,
                                     hot_int_to_id = HotIntToId,
                                     hot_next_int_id = NextIntId,
                                     hot_size = HotSize,
                                     hot_max_size = HotMaxSize,
                                     hot_compaction_threshold = Threshold,
                                     use_pq = UsePQ,
                                     pq_state = PQState,
                                     hot_pq_codes = HotPQCodes} = Index,
                     Id, Vector) ->
    %% Assign integer ID
    HotIntId = NextIntId,
    #diskann_config{l_build = L, alpha = Alpha, r = R} = Config,

    %% Update PQ codes if enabled
    NewHotPQCodes = case UsePQ andalso PQState =/= undefined of
        true ->
            Code = barrel_vectordb_pq:encode(PQState, Vector),
            maps:put(HotIntId, Code, HotPQCodes);
        false ->
            HotPQCodes
    end,

    %% Add vector and empty neighbor list
    HotVecs1 = maps:put(HotIntId, Vector, HotVecs),
    HotGraph1 = maps:put(HotIntId, [], HotGraph),
    HotIdToInt1 = maps:put(Id, HotIntId, HotIdToInt),
    HotIntToId1 = maps:put(HotIntId, Id, HotIntToId),

    %% Build temporary index state for search
    TmpIndex = Index#diskann_index{
        hot_vectors = HotVecs1,
        hot_graph = HotGraph1,
        hot_id_to_int = HotIdToInt1,
        hot_int_to_id = HotIntToId1,
        hot_next_int_id = NextIntId + 1,
        hot_pq_codes = NewHotPQCodes,
        hot_size = HotSize + 1
    },

    %% Find neighbors using greedy search in hot layer
    {_Results, Visited} = greedy_search_hot(TmpIndex, 0, Vector, 1, L),

    %% Prune to select R out-neighbors
    TmpIndex2 = robust_prune_hot(TmpIndex, HotIntId, sets:to_list(Visited), Alpha, R),
    Neighbors = maps:get(HotIntId, TmpIndex2#diskann_index.hot_graph, []),

    %% Add backward edges
    FinalIndex = lists:foldl(
        fun(J, AccIndex) ->
            JNeighbors = maps:get(J, AccIndex#diskann_index.hot_graph, []),
            case length(JNeighbors) + 1 > R of
                true ->
                    robust_prune_hot(AccIndex, J, [HotIntId | JNeighbors], Alpha, R);
                false ->
                    add_neighbor_hot(AccIndex, J, HotIntId)
            end
        end,
        TmpIndex2,
        Neighbors
    ),

    %% Check if compaction is needed
    NewHotSize = FinalIndex#diskann_index.hot_size,
    CompactionNeeded = NewHotSize >= trunc(HotMaxSize * Threshold),
    case CompactionNeeded of
        true -> {compact_needed, FinalIndex};
        false -> {ok, FinalIndex}
    end.

%% Greedy search in hot layer only
%% Note: hot_size = 0 is valid when hot layer is enabled but empty
-dialyzer({nowarn_function, greedy_search_hot/5}).
greedy_search_hot(#diskann_index{hot_size = 0}, _StartIntId, _Query, _K, _L) ->
    {[], sets:new()};
greedy_search_hot(Index, StartIntId, Query, K, L) ->
    StartDist = distance_hot(Index, StartIntId, Query),
    Candidates = gb_trees:from_orddict([{{StartDist, StartIntId}, true}]),
    Results = gb_trees:from_orddict([{{StartDist, StartIntId}, true}]),
    Visited = sets:from_list([StartIntId]),
    FurthestDist = StartDist,
    greedy_loop_hot(Index, Query, Candidates, Results, Visited, FurthestDist, K, L).

greedy_loop_hot(_Index, _Query, {0, nil}, Results, Visited, _FurthestDist, K, _L) ->
    ResultList = [Item || {Item, _} <- gb_trees:to_list(Results)],
    TopK = lists:sublist(ResultList, K),
    {TopK, Visited};
greedy_loop_hot(Index, Query, Candidates, Results, Visited, FurthestDist, K, L) ->
    {{CurrentDist, CurrentIntId}, _, RestCandidates} = gb_trees:take_smallest(Candidates),

    case CurrentDist > FurthestDist of
        true ->
            ResultList = [Item || {Item, _} <- gb_trees:to_list(Results)],
            TopK = lists:sublist(ResultList, K),
            {TopK, Visited};
        false ->
            Neighbors = maps:get(CurrentIntId, Index#diskann_index.hot_graph, []),
            {NewCandidates, NewResults, NewVisited, NewFurthestDist} = lists:foldl(
                fun(N, {CandAcc, ResAcc, VisAcc, FurthAcc}) ->
                    case sets:is_element(N, VisAcc) of
                        true ->
                            {CandAcc, ResAcc, VisAcc, FurthAcc};
                        false ->
                            NewVisAcc = sets:add_element(N, VisAcc),
                            D = distance_hot(Index, N, Query),
                            ResSize = gb_trees:size(ResAcc),
                            ShouldAdd = D < FurthAcc orelse ResSize < L,
                            case ShouldAdd of
                                true ->
                                    NewCandAcc = gb_trees:insert({D, N}, true, CandAcc),
                                    NewResAcc0 = gb_trees:insert({D, N}, true, ResAcc),
                                    NewResSize = gb_trees:size(NewResAcc0),
                                    {NewResAcc, NewFurthAcc} = case NewResSize > L of
                                        true ->
                                            {_, _, Trimmed} = gb_trees:take_largest(NewResAcc0),
                                            {{LastD, _}, _} = gb_trees:largest(Trimmed),
                                            {Trimmed, LastD};
                                        false ->
                                            {{MaxD, _}, _} = gb_trees:largest(NewResAcc0),
                                            {NewResAcc0, MaxD}
                                    end,
                                    {NewCandAcc, NewResAcc, NewVisAcc, NewFurthAcc};
                                false ->
                                    {CandAcc, ResAcc, NewVisAcc, FurthAcc}
                            end
                    end
                end,
                {RestCandidates, Results, Visited, FurthestDist},
                Neighbors
            ),
            greedy_loop_hot(Index, Query, NewCandidates, NewResults, NewVisited, NewFurthestDist, K, L)
    end.

%% Distance computation for hot layer vectors
distance_hot(#diskann_index{hot_vectors = HotVecs, config = Config}, IntId, QueryVec) ->
    case maps:find(IntId, HotVecs) of
        {ok, Vec} -> distance_vec(Config, QueryVec, Vec);
        error -> infinity
    end.

%% RobustPrune for hot layer
robust_prune_hot(Index, P, Candidates, Alpha, R) ->
    #diskann_index{config = Config, hot_vectors = HotVecs} = Index,
    PVec = maps:get(P, HotVecs),

    %% Sort candidates by distance to P
    WithDist = lists:filtermap(
        fun(C) ->
            case C =:= P of
                true -> false;
                false ->
                    case maps:find(C, HotVecs) of
                        {ok, CVec} ->
                            D = distance_vec(Config, PVec, CVec),
                            {true, {D, C}};
                        error -> false
                    end
            end
        end,
        Candidates
    ),
    Sorted = lists:sort(WithDist),

    %% Alpha-RNG pruning
    Neighbors = prune_by_alpha_hot(Index, PVec, Sorted, Alpha, R, []),

    %% Update graph
    Index#diskann_index{
        hot_graph = maps:put(P, Neighbors, Index#diskann_index.hot_graph)
    }.

prune_by_alpha_hot(_Index, _PVec, [], _Alpha, _R, Acc) ->
    lists:reverse(Acc);
prune_by_alpha_hot(_Index, _PVec, _Candidates, _Alpha, R, Acc) when length(Acc) >= R ->
    lists:reverse(Acc);
prune_by_alpha_hot(Index, PVec, [{D_pv, V} | Rest], Alpha, R, Acc) ->
    #diskann_index{config = Config, hot_vectors = HotVecs} = Index,
    VVec = maps:get(V, HotVecs),

    %% Check if any neighbor in Acc is closer to V than Alpha * D(p,v)
    IsPruned = lists:any(
        fun(U) ->
            UVec = maps:get(U, HotVecs),
            D_uv = distance_vec(Config, UVec, VVec),
            D_uv < Alpha * D_pv
        end,
        Acc
    ),

    case IsPruned of
        true -> prune_by_alpha_hot(Index, PVec, Rest, Alpha, R, Acc);
        false -> prune_by_alpha_hot(Index, PVec, Rest, Alpha, R, [V | Acc])
    end.

%% Add a neighbor in hot layer graph
add_neighbor_hot(#diskann_index{hot_graph = HotGraph} = Index, Node, Neighbor) ->
    Neighbors = maps:get(Node, HotGraph, []),
    case lists:member(Neighbor, Neighbors) of
        true -> Index;
        false -> Index#diskann_index{hot_graph = maps:put(Node, [Neighbor | Neighbors], HotGraph)}
    end.

%% @doc Search in hot layer only
-spec search_hot_layer(diskann_index(), [float()], pos_integer(), map()) -> [{binary(), float()}].
search_hot_layer(#diskann_index{hot_size = 0}, _Query, _K, _Opts) ->
    [];
search_hot_layer(#diskann_index{hot_size = HotSize, config = Config,
                                 hot_deleted = HotDeleted,
                                 hot_int_to_id = HotIntToId} = Index,
                  Query, K, Opts) when HotSize > 0 ->
    L = maps:get(l_search, Opts, Config#diskann_config.l_search),

    %% Search from entry point 0
    {IntResults, _Visited} = greedy_search_hot(Index, 0, Query, K * 2, L),

    %% Filter deleted and convert to string IDs
    Filtered = lists:filtermap(
        fun({D, IntId}) ->
            case sets:is_element(IntId, HotDeleted) of
                true -> false;
                false ->
                    case maps:find(IntId, HotIntToId) of
                        {ok, StringId} -> {true, {StringId, D}};
                        error -> false
                    end
            end
        end,
        IntResults
    ),
    lists:sublist(Filtered, K).

%% @doc Search both hot layer and disk layer, merge results
-spec search_combined(diskann_index(), [float()], pos_integer(), map()) -> [{binary(), float()}].
search_combined(#diskann_index{hot_size = 0} = Index, Query, K, Opts) ->
    %% No hot layer data, just search disk
    search_disk_mode(Index, Query, K, Opts);
search_combined(Index, Query, K, Opts) ->
    %% Search both layers in parallel
    HotResults = search_hot_layer(Index, Query, K, Opts),
    DiskResults = search_disk_mode(Index, Query, K, Opts),
    merge_search_results(HotResults, DiskResults, K).

%% @doc Merge and deduplicate search results from hot and disk layers
-spec merge_search_results([{binary(), float()}], [{binary(), float()}], pos_integer()) ->
    [{binary(), float()}].
merge_search_results(HotResults, DiskResults, K) ->
    %% Combine results, hot layer takes precedence for duplicates
    HotIds = sets:from_list([Id || {Id, _} <- HotResults]),
    FilteredDisk = [{Id, D} || {Id, D} <- DiskResults, not sets:is_element(Id, HotIds)],
    Combined = HotResults ++ FilteredDisk,
    %% Sort by distance and take top K
    Sorted = lists:sort(fun({_, D1}, {_, D2}) -> D1 =< D2 end, Combined),
    lists:sublist(Sorted, K).

%% @doc Compact hot layer to disk
%% Moves all vectors from hot layer to disk via batch insert
-spec compact_hot_to_disk(diskann_index()) -> {ok, diskann_index()}.
compact_hot_to_disk(#diskann_index{hot_size = 0} = Index) ->
    {ok, Index};
compact_hot_to_disk(#diskann_index{compaction_in_progress = true} = Index) ->
    %% Already compacting
    {ok, Index};
compact_hot_to_disk(#diskann_index{hot_vectors = HotVecs,
                                    hot_int_to_id = HotIntToId,
                                    hot_deleted = HotDeleted} = Index) ->
    %% Mark compaction in progress
    Index1 = Index#diskann_index{compaction_in_progress = true},

    %% Build list of vectors to transfer (excluding deleted)
    Vectors = maps:fold(
        fun(IntId, Vec, Acc) ->
            case sets:is_element(IntId, HotDeleted) of
                true -> Acc;
                false ->
                    case maps:find(IntId, HotIntToId) of
                        {ok, StringId} -> [{StringId, Vec} | Acc];
                        error -> Acc
                    end
            end
        end,
        [],
        HotVecs
    ),

    case Vectors of
        [] ->
            %% All vectors were deleted, just clear hot layer
            {ok, clear_hot_layer(Index1)};
        _ ->
            %% Batch insert to disk (always succeeds)
            {ok, Index2} = insert_batch_disk(Index1, Vectors, #{}),
            %% Clear hot layer after successful compaction
            {ok, clear_hot_layer(Index2)}
    end.

%% @doc Clear hot layer state after compaction
-spec clear_hot_layer(diskann_index()) -> diskann_index().
clear_hot_layer(Index) ->
    Index#diskann_index{
        hot_vectors = #{},
        hot_graph = #{},
        hot_id_to_int = #{},
        hot_int_to_id = #{},
        hot_next_int_id = 0,
        hot_deleted = sets:new(),
        hot_pq_codes = #{},
        hot_tq_codes = #{},
        hot_size = 0,
        compaction_in_progress = false
    }.

%% @doc Delete from hot layer (tombstone)
-spec hot_delete(diskann_index(), binary()) -> {ok, diskann_index()} | not_found.
hot_delete(#diskann_index{hot_id_to_int = HotIdToInt,
                           hot_deleted = HotDeleted} = Index, Id) ->
    case maps:find(Id, HotIdToInt) of
        {ok, IntId} ->
            NewDeleted = sets:add_element(IntId, HotDeleted),
            {ok, Index#diskann_index{hot_deleted = NewDeleted}};
        error ->
            not_found
    end.

%% @doc Get hot layer statistics
-spec hot_layer_info(diskann_index()) -> map().
hot_layer_info(#diskann_index{hot_enabled = false}) ->
    #{enabled => false};
hot_layer_info(#diskann_index{hot_enabled = true,
                               hot_size = HotSize,
                               hot_max_size = HotMaxSize,
                               hot_compaction_threshold = Threshold,
                               hot_deleted = HotDeleted,
                               compaction_in_progress = Compacting}) ->
    #{
        enabled => true,
        size => HotSize,
        max_size => HotMaxSize,
        compaction_threshold => Threshold,
        deleted_count => sets:size(HotDeleted),
        compaction_in_progress => Compacting,
        fill_ratio => case HotMaxSize of
            0 -> 0.0;
            _ -> HotSize / HotMaxSize
        end
    }.

%% @doc Trigger manual compaction of hot layer to disk
-spec compact(diskann_index()) -> {ok, diskann_index()} | {error, term()}.
compact(#diskann_index{hot_enabled = false} = Index) ->
    {ok, Index};
compact(Index) ->
    compact_hot_to_disk(Index).
