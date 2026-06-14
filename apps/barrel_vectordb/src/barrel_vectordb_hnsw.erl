%%%-------------------------------------------------------------------
%%% @doc HNSW (Hierarchical Navigable Small World) Implementation.
%%%
%%% Based on the paper by Yu. A. Malkov, D. A. Yashunin:
%%% "Efficient and robust approximate nearest neighbor search using
%%% Hierarchical Navigable Small World graphs" (2016).
%%%
%%% Many implementation details not covered in the original paper were
%%% derived from antirez's practical optimizations for Redis vector-sets.
%%% Reference: https://antirez.com/news/156
%%%
%%% == Algorithm Overview ==
%%%
%%% HNSW builds a multi-layer graph where each layer is a navigable
%%% small-world graph. Higher layers have fewer nodes and act as "express
%%% lanes" for fast navigation, while layer 0 contains all nodes.
%%%
%%% Search proceeds top-down: starting from the entry point at the highest
%%% layer, we greedily descend until reaching layer 0, then perform a more
%%% thorough beam search to find the k nearest neighbors.
%%%
%%% Insert assigns each new node to a random layer L (exponentially
%%% distributed), then connects it to neighbors at layers 0..L using the
%%% same search mechanism.
%%%
%%% == Implementation Features ==
%%%
%%% 1. Bidirectional links only. When a new node is inserted, we add
%%%    connections from the new node to its neighbors AND from each
%%%    neighbor back to the new node. This maintains graph navigability
%%%    and enables proper deletion.
%%%
%%% 2. 8-bit scalar quantization. Vectors are quantized to signed int8
%%%    values using per-vector scaling:
%%%
%%%        quantized[i] = round(vector[i] * 127 / max_abs_value)
%%%
%%%    The scale factor (max_abs_value) is stored with each quantized
%%%    vector. This provides 8x memory reduction compared to float64
%%%    with minimal recall loss (~1-2% for typical embeddings).
%%%
%%%    Storage format: `<<Scale:32/float-little, Components/binary>>'
%%%    where each component is a signed 8-bit integer.
%%%
%%% 3. Norm caching. The L2 norm of each vector is computed once at
%%%    insertion time and stored in the node record. This eliminates
%%%    redundant norm calculations during cosine distance computation:
%%%
%%%        cosine_distance = 1 - (A·B) / (||A|| * ||B||)
%%%
%%%    Without caching: 3 dot products per comparison (A·B, A·A, B·B)
%%%    With caching: 1 dot product + 2 lookups (A·B, cached norms)
%%%
%%% 4. Integer domain dot product. Distance calculations between
%%%    quantized vectors use integer arithmetic:
%%%
%%%        int_dot = sum(a[i] * b[i])  -- integer multiplication
%%%        real_dot = int_dot * scale_a * scale_b / (127 * 127)
%%%
%%%    This is faster than floating-point operations and SIMD-friendly.
%%%
%%% 5. Priority queue using gb_trees. The search algorithm maintains
%%%    candidates in a balanced tree (Erlang's gb_trees) providing
%%%    O(log N) insert and extract-min operations, versus O(N log N)
%%%    for repeated lists:sort calls.
%%%
%%% 6. Direct graph serialization. The full graph structure (nodes,
%%%    vectors, neighbors, metadata) is serialized to binary format
%%%    for persistence. On startup, the index is deserialized directly
%%%    without rebuilding the graph, providing ~100x faster loading
%%%    compared to re-inserting all vectors.
%%%
%%%    Serialization format (version 2):
%%%    `<<Version:8, Size:32, MaxLayer:8, Dim:16, EntryPoint/binary,
%%%      Config/binary, Nodes/binary>>'
%%%
%%% 7. True deletion. Nodes are actually removed from the graph, not
%%%    just marked as deleted. The deleted node is removed from all
%%%    its neighbors' adjacency lists. A new entry point is selected
%%%    if the deleted node was the entry point.
%%%
%%% == Configuration Parameters ==
%%%
%%% - M: Maximum number of connections per node per layer (default: 16).
%%%   Higher values improve recall but increase memory and build time.
%%%
%%% - M_max0: Maximum connections at layer 0 (default: 2*M).
%%%   Layer 0 typically needs more connections for good recall.
%%%
%%% - ef_construction: Beam width during index construction (default: 200).
%%%   Higher values improve index quality but slow down insertion.
%%%
%%% - ef_search: Beam width during search (default: max(K, 50)).
%%%   Higher values improve recall at the cost of search latency.
%%%
%%% - ml: Level multiplier = 1/ln(M). Controls the probability
%%%   distribution for assigning nodes to layers.
%%%
%%% == Complexity ==
%%%
%%% - Insert: O(log N) average, O(ef_construction * M * log N) work
%%% - Search: O(log N) average for finding entry, O(ef_search * M) at layer 0
%%% - Delete: O(M * layers) to update neighbor lists
%%% - Memory: O(N * M * layers) for graph + O(N * dim) for vectors
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_hnsw).

-include("barrel_vectordb.hrl").

%% API
-export([
    new/0,
    new/1,
    insert/3,
    search/3,
    search/4,
    delete/2,
    size/1,
    info/1,
    get_node/2
]).

%% Serialization
-export([
    serialize/1,
    deserialize/1,
    serialize_node/1,
    deserialize_node/1
]).

%% Quantization (exported for server/store modules)
-export([
    quantize/1,
    quantize/2,
    dequantize/1,
    dequantize/2,
    scalar_quantize/1,
    compute_norm/1
]).

%% Distance functions (exported for testing)
-export([
    cosine_distance/2,
    euclidean_distance/2,
    cosine_similarity/2
]).

%%====================================================================
%% API
%%====================================================================

%% @doc Create a new empty HNSW index with default configuration
-spec new() -> hnsw_index().
new() ->
    new(#{}).

%% @doc Create a new empty HNSW index with custom configuration
%% Options:
%%   - m: Max connections per layer (default: 16)
%%   - m_max0: Max connections at layer 0 (default: 2*M)
%%   - ef_construction: Build-time ef (default: 200)
%%   - distance_fn: cosine | euclidean (default: cosine)
%%   - dimension: Vector dimension (default: 768)
%%   - quantization: scalar | turboquant | subspace_turboquant | none (default: scalar)
%%   - tq_bits: TurboQuant bits per component (default: 3, when quantization=turboquant or subspace_turboquant)
%%   - tq_seed: TurboQuant random seed (default: 42)
%%   - tq_m: Number of subspaces for subspace_turboquant (default: auto, based on dimension)
-spec new(map()) -> hnsw_index().
new(Options) ->
    M = maps:get(m, Options, 16),
    MMax0 = maps:get(m_max0, Options, M * 2),
    EfConstruction = maps:get(ef_construction, Options, 200),
    DistanceFn = maps:get(distance_fn, Options, cosine),
    Dimension = maps:get(dimension, Options, ?DEFAULT_DIMENSION),
    Quantization = maps:get(quantization, Options, scalar),

    Ml = 1.0 / math:log(M),

    %% Initialize TurboQuant or Subspace-TurboQuant if requested
    TQState = case Quantization of
        turboquant ->
            TQBits = maps:get(tq_bits, Options, 3),
            TQSeed = maps:get(tq_seed, Options, 42),
            case barrel_vectordb_turboquant:new(#{
                bits => TQBits,
                dimension => Dimension,
                seed => TQSeed
            }) of
                {ok, TQ} -> TQ;
                {error, Reason} -> error({turboquant_init_failed, Reason})
            end;
        subspace_turboquant ->
            TQBits = maps:get(tq_bits, Options, 3),
            TQSeed = maps:get(tq_seed, Options, 42),
            TQM = maps:get(tq_m, Options, barrel_vectordb_turboquant_subspace:select_m(Dimension)),
            case barrel_vectordb_turboquant_subspace:new(#{
                bits => TQBits,
                dimension => Dimension,
                seed => TQSeed,
                m => TQM
            }) of
                {ok, TQ} -> TQ;
                {error, Reason} -> error({subspace_turboquant_init_failed, Reason})
            end;
        _ ->
            undefined
    end,

    Config = #hnsw_config{
        m = M,
        m_max0 = MMax0,
        ef_construction = EfConstruction,
        ml = Ml,
        distance_fn = DistanceFn,
        quantization = Quantization,
        tq_state = TQState
    },

    #hnsw_index{
        entry_point = undefined,
        max_layer = 0,
        nodes = #{},
        config = Config,
        size = 0,
        dimension = Dimension
    }.

%% @doc Insert a vector with given ID into the index
%% Vector is quantized to 8-bit and norm is cached
-spec insert(hnsw_index(), binary(), [float()]) -> hnsw_index().
insert(#hnsw_index{entry_point = undefined, config = Config, dimension = Dim} = Index,
       Id, Vector) when length(Vector) =:= Dim ->
    NodeLayer = random_layer(Config#hnsw_config.ml),
    QuantizedVec = quantize(Vector),
    Norm = compute_norm(Vector),
    Node = #hnsw_node{
        id = Id,
        vector = QuantizedVec,
        norm = Norm,
        layer = NodeLayer,
        neighbors = init_neighbors(NodeLayer)
    },
    Index#hnsw_index{
        entry_point = Id,
        max_layer = NodeLayer,
        nodes = #{Id => Node},
        size = 1
    };
insert(#hnsw_index{dimension = Dim, nodes = Nodes} = Index, Id, Vector) when length(Vector) =:= Dim ->
    CleanIndex = case maps:is_key(Id, Nodes) of
        true -> delete(Index, Id);
        false -> Index
    end,
    case CleanIndex#hnsw_index.entry_point of
        undefined ->
            insert(CleanIndex, Id, Vector);
        _ ->
            insert_node(CleanIndex, Id, Vector)
    end;
insert(#hnsw_index{dimension = Dim}, _Id, Vector) ->
    error({invalid_dimension, Dim, length(Vector)}).

%% @doc Search for k nearest neighbors
-spec search(hnsw_index(), [float()], pos_integer()) -> [{binary(), float()}].
search(Index, Query, K) ->
    search(Index, Query, K, #{}).

%% @doc Search for k nearest neighbors with options
-spec search(hnsw_index(), [float()], pos_integer(), map()) -> [{binary(), float()}].
search(#hnsw_index{entry_point = undefined}, _Query, _K, _Options) ->
    [];
search(#hnsw_index{entry_point = EP, max_layer = MaxLayer, nodes = Nodes,
                   config = Config} = Index, Query, K, Options) ->
    EfSearch = maps:get(ef_search, Options, max(K, 50)),
    QueryQuantized = quantize(Query),
    QueryNorm = compute_norm(Query),

    EPNode = maps:get(EP, Nodes),
    EPDist = distance_quantized(QueryQuantized, QueryNorm, EPNode, Config#hnsw_config.distance_fn),

    {CurrentBest, _} = lists:foldl(
        fun(Layer, {BestId, _BestDist}) ->
            search_layer_greedy(Index, QueryQuantized, QueryNorm, BestId, Layer)
        end,
        {EP, EPDist},
        lists:seq(MaxLayer, 1, -1)
    ),

    Candidates = search_layer(Index, QueryQuantized, QueryNorm, CurrentBest, 0, EfSearch),
    TopK = take_n_smallest(Candidates, K),
    [{Id, Dist} || {Dist, Id} <- TopK].

%% @doc Delete a node from the index
-spec delete(hnsw_index(), binary()) -> hnsw_index().
delete(#hnsw_index{nodes = Nodes, size = Size} = Index, Id) ->
    case maps:is_key(Id, Nodes) of
        true ->
            Node = maps:get(Id, Nodes),
            NewNodes = remove_from_neighbors(maps:remove(Id, Nodes), Id, Node#hnsw_node.neighbors),
            NewIndex = case Index#hnsw_index.entry_point of
                Id ->
                    case maps:keys(NewNodes) of
                        [] -> Index#hnsw_index{entry_point = undefined, max_layer = 0};
                        [NewEP | _] ->
                            NewEPNode = maps:get(NewEP, NewNodes),
                            Index#hnsw_index{
                                entry_point = NewEP,
                                max_layer = NewEPNode#hnsw_node.layer
                            }
                    end;
                _ -> Index
            end,
            NewIndex#hnsw_index{nodes = NewNodes, size = Size - 1};
        false ->
            Index
    end.

%% @doc Get index size
-spec size(hnsw_index()) -> non_neg_integer().
size(#hnsw_index{size = Size}) -> Size.

%% @doc Get index info
-spec info(hnsw_index()) -> map().
info(#hnsw_index{entry_point = EP, max_layer = MaxLayer, size = Size,
                  config = Config, dimension = Dim}) ->
    QuantInfo = case Config#hnsw_config.quantization of
        turboquant ->
            TQInfo = barrel_vectordb_turboquant:info(Config#hnsw_config.tq_state),
            #{method => turboquant,
              bits => maps:get(bits, TQInfo),
              bytes_per_vector => maps:get(bytes_per_vector, TQInfo)};
        Other ->
            #{method => Other}
    end,
    #{
        entry_point => EP,
        max_layer => MaxLayer,
        size => Size,
        dimension => Dim,
        config => #{
            m => Config#hnsw_config.m,
            m_max0 => Config#hnsw_config.m_max0,
            ef_construction => Config#hnsw_config.ef_construction,
            distance_fn => Config#hnsw_config.distance_fn
        },
        quantization => QuantInfo
    }.

%% @doc Get a node by ID
-spec get_node(hnsw_index(), binary()) -> {ok, hnsw_node()} | not_found.
get_node(#hnsw_index{nodes = Nodes}, Id) ->
    case maps:find(Id, Nodes) of
        {ok, Node} -> {ok, Node};
        error -> not_found
    end.

%%====================================================================
%% Quantization Functions
%%====================================================================

%% @doc Quantize a float vector to 8-bit signed integers (scalar quantization).
%% Format: `<<Scale:32/float-little, Components/binary>>'
%% This is the default/legacy function for backward compatibility.
-spec quantize([float()]) -> binary().
quantize(Vector) ->
    scalar_quantize(Vector).

%% @doc Quantize a vector using the method specified in config
-spec quantize([float()], #hnsw_config{}) -> binary().
quantize(Vector, #hnsw_config{quantization = scalar}) ->
    scalar_quantize(Vector);
quantize(Vector, #hnsw_config{quantization = turboquant, tq_state = TQ}) ->
    barrel_vectordb_turboquant:encode(TQ, Vector);
quantize(Vector, #hnsw_config{quantization = none}) ->
    %% Store as float32 binary (no quantization)
    << <<V:32/float-little>> || V <- Vector >>.

%% @doc 8-bit scalar quantization
-spec scalar_quantize([float()]) -> binary().
scalar_quantize(Vector) ->
    MaxAbs = lists:max([abs(V) || V <- Vector]),
    Scale = case MaxAbs < 1.0e-10 of
        true -> 1.0;
        false -> 127.0 / MaxAbs
    end,
    Components = << <<(clamp(round(V * Scale), -127, 127)):8/signed>> || V <- Vector >>,
    <<MaxAbs:32/float-little, Components/binary>>.

%% @doc Dequantize back to float vector (scalar quantization)
-spec dequantize(binary()) -> [float()].
dequantize(<<MaxAbs:32/float-little, Components/binary>>) ->
    Scale = case MaxAbs < 1.0e-10 of
        true -> 1.0;
        false -> MaxAbs / 127.0
    end,
    [V * Scale || <<V:8/signed>> <= Components].

%% @doc Dequantize using the method specified in config
-spec dequantize(binary(), #hnsw_config{}) -> [float()].
dequantize(Quantized, #hnsw_config{quantization = scalar}) ->
    dequantize(Quantized);
dequantize(Quantized, #hnsw_config{quantization = turboquant, tq_state = TQ}) ->
    barrel_vectordb_turboquant:decode(TQ, Quantized);
dequantize(Quantized, #hnsw_config{quantization = none}) ->
    %% Stored as float32 binary
    [V || <<V:32/float-little>> <= Quantized].

%% @doc Compute L2 norm of a float vector
-spec compute_norm([float()]) -> float().
compute_norm(Vector) ->
    math:sqrt(lists:sum([V * V || V <- Vector])).

%% @private Clamp integer to range
clamp(V, Min, _Max) when V < Min -> Min;
clamp(V, _Min, Max) when V > Max -> Max;
clamp(V, _Min, _Max) -> V.

%%====================================================================
%% Serialization
%%====================================================================

%% @doc Serialize entire index to binary (includes full graph structure)
-spec serialize(hnsw_index()) -> binary().
serialize(#hnsw_index{entry_point = EP, max_layer = MaxLayer, nodes = Nodes,
                       config = Config, size = Size, dimension = Dim}) ->
    %% Version 2: includes full graph structure for fast loading
    Version = 2,
    EPBin = case EP of
        undefined -> <<0:16>>;
        _ -> <<(byte_size(EP)):16, EP/binary>>
    end,
    ConfigBin = <<
        (Config#hnsw_config.m):16,
        (Config#hnsw_config.m_max0):16,
        (Config#hnsw_config.ef_construction):16,
        (distance_fn_to_int(Config#hnsw_config.distance_fn)):8
    >>,
    NodesList = maps:to_list(Nodes),
    NodesBin = << <<(serialize_node_full(Id, Node))/binary>> || {Id, Node} <- NodesList >>,
    <<Version:8, Size:32, MaxLayer:8, Dim:16, EPBin/binary, ConfigBin/binary, NodesBin/binary>>.

%% @doc Deserialize index from binary
-spec deserialize(binary()) -> {ok, hnsw_index()} | {error, term()}.
deserialize(<<2:8, Size:32, MaxLayer:8, Dim:16, Rest/binary>>) ->
    %% Version 2: full graph structure
    try
        {EP, Rest1} = deserialize_entry_point(Rest),
        {Config, Rest2} = deserialize_config(Rest1),
        {Nodes, <<>>} = deserialize_nodes(Rest2, Size, #{}),
        {ok, #hnsw_index{
            entry_point = EP,
            max_layer = MaxLayer,
            nodes = Nodes,
            config = Config,
            size = Size,
            dimension = Dim
        }}
    catch
        _:Reason -> {error, {deserialization_failed, Reason}}
    end;
deserialize(<<1:8, _/binary>> = Binary) ->
    %% Version 1: legacy format (term_to_binary)
    try
        Index = binary_to_term(Binary),
        case is_record(Index, hnsw_index) of
            true -> {ok, Index};
            false -> {error, invalid_index_format}
        end
    catch
        _:Reason -> {error, {deserialization_failed, Reason}}
    end;
deserialize(_) ->
    {error, invalid_format}.

%% @doc Serialize a single node (for incremental persistence)
-spec serialize_node(hnsw_node()) -> binary().
serialize_node(#hnsw_node{layer = Layer, neighbors = Neighbors}) ->
    NeighborBin = serialize_neighbors(Neighbors),
    NumLayers = maps:size(Neighbors),
    <<?HNSW_NODE_VERSION:8, Layer:8, NumLayers:8, NeighborBin/binary>>.

%% @doc Deserialize a single node
-spec deserialize_node(binary()) -> {ok, map()} | {error, term()}.
deserialize_node(<<?HNSW_NODE_VERSION:8, Layer:8, NumLayers:8, Rest/binary>>) ->
    case deserialize_neighbors(Rest, NumLayers, #{}) of
        {Neighbors, <<>>} ->
            {ok, #{layer => Layer, neighbors => Neighbors}};
        {_, _} ->
            {error, trailing_data}
    end;
deserialize_node(_) ->
    {error, invalid_node_format}.

%%====================================================================
%% Distance Functions (using quantized vectors)
%%====================================================================

%% @doc Cosine distance between two float vectors
-spec cosine_distance([float()], [float()]) -> float().
cosine_distance(Vec1, Vec2) ->
    1.0 - cosine_similarity(Vec1, Vec2).

%% @doc Cosine similarity between two float vectors
-spec cosine_similarity([float()], [float()]) -> float().
cosine_similarity(Vec1, Vec2) ->
    Dot = dot_product(Vec1, Vec2),
    Norm1 = math:sqrt(dot_product(Vec1, Vec1)),
    Norm2 = math:sqrt(dot_product(Vec2, Vec2)),
    Denom = Norm1 * Norm2,
    case Denom < 1.0e-10 of
        true -> 0.0;
        false -> Dot / Denom
    end.

%% @doc Euclidean distance between two float vectors
-spec euclidean_distance([float()], [float()]) -> float().
euclidean_distance(Vec1, Vec2) ->
    SumSq = lists:foldl(
        fun({A, B}, Acc) ->
            Diff = A - B,
            Acc + Diff * Diff
        end,
        0.0,
        lists:zip(Vec1, Vec2)
    ),
    math:sqrt(SumSq).

%%====================================================================
%% Internal: Quantized Distance Functions
%%====================================================================

%% Distance using quantized vectors and cached norms
distance_quantized(QueryQuantized, QueryNorm, #hnsw_node{vector = NodeVec, norm = NodeNorm}, cosine) ->
    cosine_distance_quantized(QueryQuantized, QueryNorm, NodeVec, NodeNorm);
distance_quantized(QueryQuantized, _QueryNorm, #hnsw_node{vector = NodeVec}, euclidean) ->
    euclidean_distance_quantized(QueryQuantized, NodeVec).

%% Cosine distance using quantized vectors and cached norms
cosine_distance_quantized(<<QScale:32/float-little, QComps/binary>>,
                          QueryNorm,
                          <<NScale:32/float-little, NComps/binary>>,
                          NodeNorm) ->
    %% Integer domain dot product
    IntDot = dot_product_int8(QComps, NComps),
    %% Scale back to float domain
    RealDot = IntDot * QScale * NScale / (127.0 * 127.0),
    Denom = QueryNorm * NodeNorm,
    Similarity = case Denom < 1.0e-10 of
        true -> 0.0;
        false -> RealDot / Denom
    end,
    1.0 - Similarity.

%% Euclidean distance using quantized vectors
euclidean_distance_quantized(<<QScale:32/float-little, QComps/binary>>,
                              <<NScale:32/float-little, NComps/binary>>) ->
    %% Approximate using quantized values
    SumSq = euclidean_sum_sq_int8(QComps, NComps, QScale, NScale, 0.0),
    math:sqrt(SumSq).

%% Integer domain dot product for int8 vectors - optimized 8 bytes at a time
dot_product_int8(A, B) ->
    dot_product_int8_acc(A, B, 0).

dot_product_int8_acc(<<A1:8/signed, A2:8/signed, A3:8/signed, A4:8/signed,
                       A5:8/signed, A6:8/signed, A7:8/signed, A8:8/signed, RestA/binary>>,
                     <<B1:8/signed, B2:8/signed, B3:8/signed, B4:8/signed,
                       B5:8/signed, B6:8/signed, B7:8/signed, B8:8/signed, RestB/binary>>, Acc) ->
    Sum = A1*B1 + A2*B2 + A3*B3 + A4*B4 + A5*B5 + A6*B6 + A7*B7 + A8*B8,
    dot_product_int8_acc(RestA, RestB, Acc + Sum);
dot_product_int8_acc(<<A:8/signed, RestA/binary>>, <<B:8/signed, RestB/binary>>, Acc) ->
    dot_product_int8_acc(RestA, RestB, Acc + A * B);
dot_product_int8_acc(<<>>, <<>>, Acc) -> Acc;
%% Handle mismatched vector lengths gracefully (return accumulated value)
dot_product_int8_acc(<<>>, _, Acc) -> Acc;
dot_product_int8_acc(_, <<>>, Acc) -> Acc.

%% Euclidean sum of squares for int8 vectors - optimized 4 bytes at a time
euclidean_sum_sq_int8(A, B, QS, NS, Acc) ->
    ScaleQ = QS / 127.0,
    ScaleN = NS / 127.0,
    euclidean_sum_sq_int8_acc(A, B, ScaleQ, ScaleN, Acc).

euclidean_sum_sq_int8_acc(<<A1:8/signed, A2:8/signed, A3:8/signed, A4:8/signed, RestA/binary>>,
                          <<B1:8/signed, B2:8/signed, B3:8/signed, B4:8/signed, RestB/binary>>,
                          ScaleQ, ScaleN, Acc) ->
    D1 = A1 * ScaleQ - B1 * ScaleN,
    D2 = A2 * ScaleQ - B2 * ScaleN,
    D3 = A3 * ScaleQ - B3 * ScaleN,
    D4 = A4 * ScaleQ - B4 * ScaleN,
    SumSq = D1*D1 + D2*D2 + D3*D3 + D4*D4,
    euclidean_sum_sq_int8_acc(RestA, RestB, ScaleQ, ScaleN, Acc + SumSq);
euclidean_sum_sq_int8_acc(<<A:8/signed, RestA/binary>>, <<B:8/signed, RestB/binary>>,
                          ScaleQ, ScaleN, Acc) ->
    Diff = A * ScaleQ - B * ScaleN,
    euclidean_sum_sq_int8_acc(RestA, RestB, ScaleQ, ScaleN, Acc + Diff * Diff);
euclidean_sum_sq_int8_acc(<<>>, <<>>, _ScaleQ, _ScaleN, Acc) -> Acc;
%% Handle mismatched vector lengths gracefully
euclidean_sum_sq_int8_acc(<<>>, _, _ScaleQ, _ScaleN, Acc) -> Acc;
euclidean_sum_sq_int8_acc(_, <<>>, _ScaleQ, _ScaleN, Acc) -> Acc.

%%====================================================================
%% Internal: Search with Priority Queue (gb_trees)
%%====================================================================

%% Insert a new node
insert_node(#hnsw_index{entry_point = EP, max_layer = MaxLayer, nodes = Nodes,
                        config = Config, size = Size} = Index, Id, Vector) ->
    NodeLayer = random_layer(Config#hnsw_config.ml),
    QuantizedVec = quantize(Vector),
    Norm = compute_norm(Vector),

    EPNode = maps:get(EP, Nodes),
    EPDist = distance_quantized(QuantizedVec, Norm, EPNode, Config#hnsw_config.distance_fn),

    TopLayer = max(MaxLayer, NodeLayer),
    {CurrentBest, _} = lists:foldl(
        fun(Layer, {BestId, _BestDist}) ->
            search_layer_greedy(Index, QuantizedVec, Norm, BestId, Layer)
        end,
        {EP, EPDist},
        lists:seq(TopLayer, NodeLayer + 1, -1)
    ),

    InsertLayers = lists:seq(min(MaxLayer, NodeLayer), 0, -1),

    {NewNode, UpdatedNodes} = lists:foldl(
        fun(Layer, {AccNode, AccNodes}) ->
            EfC = Config#hnsw_config.ef_construction,
            Candidates = search_layer(Index#hnsw_index{nodes = AccNodes},
                                      QuantizedVec, Norm, CurrentBest, Layer, EfC),
            M = case Layer of
                0 -> Config#hnsw_config.m_max0;
                _ -> Config#hnsw_config.m
            end,
            SelectedNeighbors = select_neighbors(Candidates, M),
            NewNeighbors = (AccNode#hnsw_node.neighbors)#{Layer => SelectedNeighbors},
            NewAccNode = AccNode#hnsw_node{neighbors = NewNeighbors},
            UpdatedAccNodes = lists:foldl(
                fun(NeighborId, NodesAcc) ->
                    add_connection(NodesAcc, NeighborId, Id, Layer, M)
                end,
                AccNodes,
                SelectedNeighbors
            ),
            {NewAccNode, UpdatedAccNodes}
        end,
        {#hnsw_node{id = Id, vector = QuantizedVec, norm = Norm, layer = NodeLayer,
                    neighbors = init_neighbors(NodeLayer)}, Nodes},
        InsertLayers
    ),

    FinalNodes = UpdatedNodes#{Id => NewNode},
    NewMaxLayer = max(MaxLayer, NodeLayer),
    NewEP = if NodeLayer > MaxLayer -> Id; true -> EP end,

    Index#hnsw_index{
        entry_point = NewEP,
        max_layer = NewMaxLayer,
        nodes = FinalNodes,
        size = Size + 1
    }.

%% Greedy search at a single layer
search_layer_greedy(#hnsw_index{nodes = Nodes, config = Config}, QueryQ, QueryNorm, StartId, Layer) ->
    StartNode = maps:get(StartId, Nodes),
    StartDist = distance_quantized(QueryQ, QueryNorm, StartNode, Config#hnsw_config.distance_fn),
    search_layer_greedy_loop(Nodes, QueryQ, QueryNorm, StartId, StartDist, Layer, Config).

search_layer_greedy_loop(Nodes, QueryQ, QueryNorm, BestId, BestDist, Layer, Config) ->
    Node = maps:get(BestId, Nodes),
    Neighbors = maps:get(Layer, Node#hnsw_node.neighbors, []),

    {NewBestId, NewBestDist} = lists:foldl(
        fun(NeighborId, {AccBestId, AccBestDist}) ->
            case maps:find(NeighborId, Nodes) of
                {ok, NeighborNode} ->
                    Dist = distance_quantized(QueryQ, QueryNorm, NeighborNode,
                                             Config#hnsw_config.distance_fn),
                    if Dist < AccBestDist -> {NeighborId, Dist};
                       true -> {AccBestId, AccBestDist}
                    end;
                error ->
                    {AccBestId, AccBestDist}
            end
        end,
        {BestId, BestDist},
        Neighbors
    ),

    if NewBestId =:= BestId ->
        {BestId, BestDist};
    true ->
        search_layer_greedy_loop(Nodes, QueryQ, QueryNorm, NewBestId, NewBestDist, Layer, Config)
    end.

%% Full ef-search at a layer using gb_trees as priority queue
%% Keys are {Distance, Id} tuples to handle duplicate distances
search_layer(#hnsw_index{nodes = Nodes, config = Config}, QueryQ, QueryNorm, StartId, Layer, Ef) ->
    StartNode = maps:get(StartId, Nodes),
    StartDist = distance_quantized(QueryQ, QueryNorm, StartNode, Config#hnsw_config.distance_fn),

    Visited = sets:from_list([StartId]),
    %% Candidates: min-heap with {Distance, Id} as key to handle duplicates
    Candidates = gb_trees:from_orddict([{{StartDist, StartId}, true}]),
    %% Results: also gb_trees for O(log N) insert/trim instead of O(N log N) sort
    Results = gb_trees:from_orddict([{{StartDist, StartId}, true}]),
    FurthestDist = StartDist,

    FinalResults = search_layer_loop(Nodes, QueryQ, QueryNorm, Layer, Config, Ef, Visited, Candidates, Results, FurthestDist),
    %% Convert gb_trees results back to list
    [{Dist, Id} || {{Dist, Id}, _} <- gb_trees:to_list(FinalResults)].

search_layer_loop(_Nodes, _QueryQ, _QueryNorm, _Layer, _Config, _Ef, _Visited, {0, nil}, Results, _FurthestDist) ->
    %% Empty candidates gb_trees
    Results;
search_layer_loop(Nodes, QueryQ, QueryNorm, Layer, Config, Ef, Visited, Candidates, Results, FurthestDist) ->
    %% Get smallest distance candidate
    {{CurrentDist, CurrentId}, _, RestCandidates} = gb_trees:take_smallest(Candidates),

    %% If current candidate is further than furthest result, we're done
    if CurrentDist > FurthestDist ->
        Results;
    true ->
        Node = maps:get(CurrentId, Nodes),
        Neighbors = maps:get(Layer, Node#hnsw_node.neighbors, []),

        {NewVisited, NewCandidates, NewResults, NewFurthestDist} = lists:foldl(
            fun(NeighborId, {VisAcc, CandAcc, ResAcc, FurthAcc}) ->
                case sets:is_element(NeighborId, VisAcc) of
                    true -> {VisAcc, CandAcc, ResAcc, FurthAcc};
                    false ->
                        NewVisAcc = sets:add_element(NeighborId, VisAcc),
                        case maps:find(NeighborId, Nodes) of
                            {ok, NeighborNode} ->
                                Dist = distance_quantized(QueryQ, QueryNorm, NeighborNode,
                                                         Config#hnsw_config.distance_fn),
                                ResSize = gb_trees:size(ResAcc),
                                ShouldAdd = Dist < FurthAcc orelse ResSize < Ef,
                                case ShouldAdd of
                                    true ->
                                        %% Add to candidates with {Dist, Id} key
                                        NewCandAcc = gb_trees:insert({Dist, NeighborId}, true, CandAcc),
                                        %% Add to results (gb_trees, O(log N))
                                        NewResAcc0 = gb_trees:insert({Dist, NeighborId}, true, ResAcc),
                                        %% Trim if too many, update furthest (O(log N) operations)
                                        NewResSize = gb_trees:size(NewResAcc0),
                                        {NewResAcc, NewFurthAcc} = case NewResSize > Ef of
                                            true ->
                                                %% Remove largest (furthest) - O(log N)
                                                {_, _, Trimmed} = gb_trees:take_largest(NewResAcc0),
                                                %% Get new furthest - O(log N)
                                                {{LastDist, _}, _} = gb_trees:largest(Trimmed),
                                                {Trimmed, LastDist};
                                            false ->
                                                %% Get current max distance - O(log N)
                                                {{MaxDist, _}, _} = gb_trees:largest(NewResAcc0),
                                                {NewResAcc0, MaxDist}
                                        end,
                                        {NewVisAcc, NewCandAcc, NewResAcc, NewFurthAcc};
                                    false ->
                                        {NewVisAcc, CandAcc, ResAcc, FurthAcc}
                                end;
                            error ->
                                {NewVisAcc, CandAcc, ResAcc, FurthAcc}
                        end
                end
            end,
            {Visited, RestCandidates, Results, FurthestDist},
            Neighbors
        ),

        search_layer_loop(Nodes, QueryQ, QueryNorm, Layer, Config, Ef, NewVisited, NewCandidates, NewResults, NewFurthestDist)
    end.

%% Take N smallest from list of {Distance, Id}
take_n_smallest(List, N) ->
    Sorted = lists:sort(List),
    lists:sublist(Sorted, N).

%% Select M neighbors from candidates
select_neighbors(Candidates, M) ->
    Selected = lists:sublist(Candidates, M),
    [Id || {_Dist, Id} <- Selected].

%% Add bidirectional connection
add_connection(Nodes, NodeId, NewNeighborId, Layer, MaxM) ->
    case maps:find(NodeId, Nodes) of
        {ok, Node} ->
            CurrentNeighbors = maps:get(Layer, Node#hnsw_node.neighbors, []),
            case lists:member(NewNeighborId, CurrentNeighbors) of
                true -> Nodes;
                false ->
                    NewNeighbors = [NewNeighborId | CurrentNeighbors],
                    PrunedNeighbors = if length(NewNeighbors) > MaxM ->
                        lists:sublist(NewNeighbors, MaxM);
                    true ->
                        NewNeighbors
                    end,
                    UpdatedNode = Node#hnsw_node{
                        neighbors = (Node#hnsw_node.neighbors)#{Layer => PrunedNeighbors}
                    },
                    Nodes#{NodeId => UpdatedNode}
            end;
        error ->
            Nodes
    end.

%% Remove node from all its neighbors' neighbor lists
remove_from_neighbors(Nodes, RemovedId, NeighborsByLayer) ->
    maps:fold(
        fun(_Layer, NeighborIds, AccNodes) ->
            lists:foldl(
                fun(NeighborId, NodesAcc) ->
                    case maps:find(NeighborId, NodesAcc) of
                        {ok, NeighborNode} ->
                            UpdatedNeighbors = maps:map(
                                fun(_L, Ns) ->
                                    lists:delete(RemovedId, Ns)
                                end,
                                NeighborNode#hnsw_node.neighbors
                            ),
                            NodesAcc#{NeighborId => NeighborNode#hnsw_node{neighbors = UpdatedNeighbors}};
                        error ->
                            NodesAcc
                    end
                end,
                AccNodes,
                NeighborIds
            )
        end,
        Nodes,
        NeighborsByLayer
    ).

%% Initialize empty neighbors map
init_neighbors(MaxLayer) ->
    maps:from_list([{L, []} || L <- lists:seq(0, MaxLayer)]).

%% Random layer generation
random_layer(Ml) ->
    U = rand:uniform(),
    floor(-math:log(U) * Ml).

%% Float dot product
dot_product(Vec1, Vec2) ->
    lists:sum([A * B || {A, B} <- lists:zip(Vec1, Vec2)]).

%%====================================================================
%% Serialization Helpers
%%====================================================================

serialize_node_full(Id, #hnsw_node{vector = Vec, norm = Norm, layer = Layer, neighbors = Neighbors}) ->
    IdLen = byte_size(Id),
    VecLen = byte_size(Vec),
    NeighborBin = serialize_neighbors(Neighbors),
    <<IdLen:16, Id/binary, VecLen:16, Vec/binary, Norm:64/float-little, Layer:8,
      (maps:size(Neighbors)):8, NeighborBin/binary>>.

serialize_neighbors(Neighbors) ->
    Sorted = lists:sort(maps:to_list(Neighbors)),
    << <<(serialize_layer_neighbors(L, Ns))/binary>> || {L, Ns} <- Sorted >>.

serialize_layer_neighbors(Layer, NeighborIds) ->
    NumNeighbors = length(NeighborIds),
    NeighborsBin = << <<(byte_size(Id)):16, Id/binary>> || Id <- NeighborIds >>,
    <<Layer:8, NumNeighbors:16, NeighborsBin/binary>>.

deserialize_entry_point(<<0:16, Rest/binary>>) ->
    {undefined, Rest};
deserialize_entry_point(<<Len:16, EP:Len/binary, Rest/binary>>) ->
    {EP, Rest}.

deserialize_config(<<M:16, MMax0:16, EfC:16, DistFnInt:8, Rest/binary>>) ->
    Config = #hnsw_config{
        m = M,
        m_max0 = MMax0,
        ef_construction = EfC,
        ml = 1.0 / math:log(M),
        distance_fn = int_to_distance_fn(DistFnInt)
    },
    {Config, Rest}.

deserialize_nodes(Rest, 0, Acc) ->
    {Acc, Rest};
deserialize_nodes(<<IdLen:16, Id:IdLen/binary, VecLen:16, Vec:VecLen/binary,
                    Norm:64/float-little, Layer:8, NumLayers:8, Rest/binary>>, N, Acc) ->
    {Neighbors, Rest2} = deserialize_neighbors(Rest, NumLayers, #{}),
    Node = #hnsw_node{
        id = Id,
        vector = Vec,
        norm = Norm,
        layer = Layer,
        neighbors = Neighbors
    },
    deserialize_nodes(Rest2, N - 1, Acc#{Id => Node}).

deserialize_neighbors(Rest, 0, Acc) ->
    {Acc, Rest};
deserialize_neighbors(<<L:8, NumNeighbors:16, Rest/binary>>, N, Acc) ->
    {NeighborIds, Rest2} = deserialize_neighbor_ids(Rest, NumNeighbors, []),
    deserialize_neighbors(Rest2, N - 1, Acc#{L => NeighborIds}).

deserialize_neighbor_ids(Rest, 0, Acc) ->
    {lists:reverse(Acc), Rest};
deserialize_neighbor_ids(<<Len:16, Id:Len/binary, Rest/binary>>, N, Acc) ->
    deserialize_neighbor_ids(Rest, N - 1, [Id | Acc]).

distance_fn_to_int(cosine) -> 0;
distance_fn_to_int(euclidean) -> 1.

int_to_distance_fn(0) -> cosine;
int_to_distance_fn(1) -> euclidean.
