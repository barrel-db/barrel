%%%-------------------------------------------------------------------
%%% @doc Product Quantization for DiskANN
%%%
%%% Product Quantization (PQ) compresses vectors by:
%%% 1. Splitting D-dimensional vector into M subspaces
%%% 2. Training K centroids per subspace (k-means)
%%% 3. Encoding vector as M bytes (indices into codebooks)
%%%
%%% Distance computation uses precomputed lookup tables:
%%% - For query q, precompute distance from q's subvectors to all centroids
%%% - Distance to any encoded vector = sum of M table lookups
%%%
%%% Memory: M bytes per vector (vs D*4 for float32)
%%% With M=8, K=256: ~32x compression for 256-dim vectors
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_pq).

-include("barrel_vectordb.hrl").

%% API
-export([
    new/1,
    train/2,
    encode/2,
    decode/2,
    precompute_tables/2,
    distance/2,
    batch_encode/2,
    is_trained/1,
    info/1
]).

%% Internal exports for testing
-export([
    split_subvectors/3,
    kmeans/3
]).

-record(pq_config, {
    m :: pos_integer(),              %% Number of subquantizers
    k :: pos_integer(),              %% Centroids per subquantizer (typically 256)
    dimension :: pos_integer(),      %% Original vector dimension
    subvector_dim :: pos_integer(),  %% Dimension per subspace (dim/m)
    codebooks :: [binary()] | undefined,  %% M codebooks, each K*subvector_dim floats
    trained = false :: boolean()
}).

-type pq_config() :: #pq_config{}.
-type pq_code() :: binary().  %% M bytes
-type distance_tables() :: binary().  %% M*K float32 values

-export_type([pq_config/0, pq_code/0, distance_tables/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Create a new PQ configuration
%% Options:
%%   m - number of subquantizers (default: 8)
%%   k - centroids per subquantizer (default: 256, max: 256)
%%   dimension - vector dimension (required, must be divisible by m)
-spec new(map()) -> {ok, pq_config()} | {error, term()}.
new(Options) ->
    M = maps:get(m, Options, 8),
    K = maps:get(k, Options, 256),
    Dimension = maps:get(dimension, Options, undefined),

    case validate_config(M, K, Dimension) of
        ok ->
            SubvectorDim = Dimension div M,
            {ok, #pq_config{
                m = M,
                k = K,
                dimension = Dimension,
                subvector_dim = SubvectorDim,
                codebooks = undefined,
                trained = false
            }};
        {error, _} = Error ->
            Error
    end.

%% @doc Train codebooks using k-means clustering
%% Vectors should be a list of float lists, all same dimension
-spec train(pq_config(), [[float()]]) -> {ok, pq_config()} | {error, term()}.
train(#pq_config{m = M, k = K, dimension = Dim, subvector_dim = SubDim} = Config, Vectors)
  when length(Vectors) >= K ->
    %% Split each vector into M subvectors
    SubvectorGroups = lists:foldl(
        fun(Vec, Acc) when length(Vec) =:= Dim ->
            Subvecs = split_subvectors(Vec, M, SubDim),
            merge_subvector_groups(Subvecs, Acc);
        (_, _) ->
            throw({error, {invalid_dimension, Dim}})
        end,
        [[] || _ <- lists:seq(1, M)],
        Vectors
    ),

    %% Train K centroids for each subspace
    Codebooks = lists:map(
        fun(SubvectorList) ->
            Centroids = kmeans(SubvectorList, K, 20),
            encode_codebook(Centroids, SubDim)
        end,
        SubvectorGroups
    ),

    {ok, Config#pq_config{codebooks = Codebooks, trained = true}};
train(#pq_config{k = K}, Vectors) ->
    {error, {not_enough_vectors, length(Vectors), K}}.

%% @doc Encode a vector to M bytes using trained codebooks
-spec encode(pq_config(), [float()]) -> pq_code().
encode(#pq_config{trained = false}, _Vec) ->
    error(not_trained);
encode(#pq_config{m = M, k = _K, dimension = Dim, subvector_dim = SubDim,
                  codebooks = Codebooks}, Vec) when length(Vec) =:= Dim ->
    Subvecs = split_subvectors(Vec, M, SubDim),
    Codes = lists:zipwith(
        fun(Subvec, Codebook) ->
            find_nearest_centroid(Subvec, Codebook, SubDim)
        end,
        Subvecs,
        Codebooks
    ),
    list_to_binary(Codes).

%% @doc Batch encode multiple vectors
-spec batch_encode(pq_config(), [[float()]]) -> [pq_code()].
batch_encode(Config, Vectors) ->
    [encode(Config, V) || V <- Vectors].

%% @doc Decode PQ code back to approximate vector
-spec decode(pq_config(), pq_code()) -> [float()].
decode(#pq_config{trained = false}, _Code) ->
    error(not_trained);
decode(#pq_config{m = M, subvector_dim = SubDim, codebooks = Codebooks}, Code)
  when byte_size(Code) =:= M ->
    CodeList = binary_to_list(Code),
    Subvecs = lists:zipwith(
        fun(Idx, Codebook) ->
            get_centroid(Idx, Codebook, SubDim)
        end,
        CodeList,
        Codebooks
    ),
    lists:flatten(Subvecs).

%% @doc Precompute distance tables for a query vector
%% Returns M*K float32 values: distance from each query subvector to each centroid
-spec precompute_tables(pq_config(), [float()]) -> distance_tables().
precompute_tables(#pq_config{trained = false}, _Query) ->
    error(not_trained);
precompute_tables(#pq_config{m = M, k = K, dimension = Dim, subvector_dim = SubDim,
                             codebooks = Codebooks}, Query) when length(Query) =:= Dim ->
    Subvecs = split_subvectors(Query, M, SubDim),
    Tables = lists:zipwith(
        fun(QuerySub, Codebook) ->
            compute_distances_to_centroids(QuerySub, Codebook, K, SubDim)
        end,
        Subvecs,
        Codebooks
    ),
    iolist_to_binary(Tables).

%% @doc Compute asymmetric distance using precomputed tables
%% This is the fast O(M) lookup vs O(D) for full distance
%% Tables size must be M*K*4 where M = byte_size(Code)
-spec distance(distance_tables(), pq_code()) -> float().
distance(Tables, Code) ->
    M = byte_size(Code),
    %% Infer K from table size: Tables = M * K * 4 bytes
    TableSize = byte_size(Tables),
    K = TableSize div (M * 4),
    distance_acc(Tables, Code, 0, M, K, 0.0).

%% @doc Check if PQ is trained
-spec is_trained(pq_config()) -> boolean().
is_trained(#pq_config{trained = Trained}) -> Trained.

%% @doc Get PQ configuration info
-spec info(pq_config()) -> map().
info(#pq_config{m = M, k = K, dimension = Dim, subvector_dim = SubDim, trained = Trained}) ->
    #{
        m => M,
        k => K,
        dimension => Dim,
        subvector_dim => SubDim,
        trained => Trained,
        bytes_per_vector => M,
        compression_ratio => (Dim * 4) / M  %% vs float32
    }.

%%====================================================================
%% Internal Functions
%%====================================================================

validate_config(_, _, undefined) ->
    {error, dimension_required};
validate_config(_, K, _) when K > 256 ->
    {error, {k_too_large, K, 256}};
validate_config(M, K, Dimension) when M > 0, K > 0, K =< 256, Dimension > 0 ->
    case Dimension rem M of
        0 -> ok;
        _ -> {error, {dimension_not_divisible, Dimension, M}}
    end;
validate_config(M, K, Dim) ->
    {error, {invalid_config, M, K, Dim}}.

%% Split vector into M subvectors of SubDim each
split_subvectors(Vec, M, SubDim) ->
    split_subvectors(Vec, M, SubDim, []).

split_subvectors([], 0, _SubDim, Acc) ->
    lists:reverse(Acc);
split_subvectors(Vec, M, SubDim, Acc) when M > 0 ->
    {Subvec, Rest} = lists:split(SubDim, Vec),
    split_subvectors(Rest, M - 1, SubDim, [Subvec | Acc]).

%% Merge subvectors into groups for training
merge_subvector_groups(Subvecs, Groups) ->
    lists:zipwith(fun(S, G) -> [S | G] end, Subvecs, Groups).

%% K-means clustering with adaptive initialization
kmeans(Vectors, K, MaxIter) ->
    N = length(Vectors),
    %% Use simple random init for small datasets (faster), k-means++ for large
    InitCentroids = case N < 100000 of
        true -> random_init(Vectors, K);
        false -> kmeans_plus_plus_init(Vectors, K)
    end,
    %% Adaptive iterations: fewer for small datasets
    %% n < 10k: 5 iters, n >= 10k: 15 iters
    Iterations = case N < 10000 of
        true -> min(5, MaxIter);
        false -> min(15, MaxIter)
    end,
    kmeans_iterate_early_stop(Vectors, InitCentroids, Iterations, infinity).

%% Simple random initialization (O(k) - very fast)
random_init(Vectors, K) ->
    N = length(Vectors),
    VecArray = list_to_tuple(Vectors),
    Indices = random_sample_indices(N, K, []),
    [element(I, VecArray) || I <- Indices].

random_sample_indices(_N, 0, Acc) ->
    Acc;
random_sample_indices(N, K, Acc) ->
    I = rand:uniform(N),
    case lists:member(I, Acc) of
        true -> random_sample_indices(N, K, Acc);
        false -> random_sample_indices(N, K - 1, [I | Acc])
    end.

%% K-means++ initialization for better convergence (O(n*k) - slower but better quality)
kmeans_plus_plus_init(Vectors, K) ->
    %% Pick first centroid randomly
    N = length(Vectors),
    FirstIdx = rand:uniform(N),
    FirstCentroid = lists:nth(FirstIdx, Vectors),
    kmeans_plus_plus_init(Vectors, K - 1, [FirstCentroid]).

kmeans_plus_plus_init(_Vectors, 0, Centroids) ->
    lists:reverse(Centroids);
kmeans_plus_plus_init(Vectors, Remaining, Centroids) ->
    %% Compute distances to nearest centroid
    Distances = [min_distance_to_centroids(V, Centroids) || V <- Vectors],
    TotalDist = lists:sum(Distances),

    %% Sample proportional to D^2
    Target = rand:uniform() * TotalDist,
    NewCentroid = sample_by_distance(Vectors, Distances, Target, 0.0),
    kmeans_plus_plus_init(Vectors, Remaining - 1, [NewCentroid | Centroids]).

min_distance_to_centroids(Vec, Centroids) ->
    Dists = [squared_euclidean(Vec, C) || C <- Centroids],
    lists:min(Dists).

sample_by_distance([V | _], [D | _], Target, Acc) when Acc + D >= Target ->
    V;
sample_by_distance([_ | Vs], [D | Ds], Target, Acc) ->
    sample_by_distance(Vs, Ds, Target, Acc + D);
sample_by_distance([V], _, _, _) ->
    V.

kmeans_iterate_early_stop(_, Centroids, 0, _LastChange) ->
    Centroids;
kmeans_iterate_early_stop(Vectors, Centroids, Iter, LastChange) ->
    %% Assign each vector to nearest centroid
    Assignments = [assign_to_cluster(V, Centroids) || V <- Vectors],

    %% Recompute centroids
    K = length(Centroids),
    Clusters = group_by_cluster(lists:zip(Vectors, Assignments), K),
    NewCentroids = [compute_centroid(C, lists:nth(I, Centroids))
                   || {I, C} <- lists:zip(lists:seq(1, K), Clusters)],

    %% Compute change (total centroid movement)
    Change = centroid_change(Centroids, NewCentroids),

    %% Early stop if converged or change is increasing (local minimum passed)
    case Change < 1.0e-6 orelse (LastChange =/= infinity andalso Change > LastChange * 0.99) of
        true -> NewCentroids;
        false -> kmeans_iterate_early_stop(Vectors, NewCentroids, Iter - 1, Change)
    end.

centroid_change(Old, New) ->
    lists:sum([squared_euclidean(O, N) || {O, N} <- lists:zip(Old, New)]).

assign_to_cluster(Vec, Centroids) ->
    Distances = [{I, squared_euclidean(Vec, C)}
                 || {I, C} <- lists:zip(lists:seq(1, length(Centroids)), Centroids)],
    {MinIdx, _} = lists:foldl(
        fun({I, D}, {_BestI, BestD}) when D < BestD -> {I, D};
           (_, Acc) -> Acc
        end,
        {1, infinity},
        Distances
    ),
    MinIdx.

group_by_cluster(Assignments, K) ->
    Empty = [[] || _ <- lists:seq(1, K)],
    lists:foldl(
        fun({Vec, ClusterIdx}, Acc) ->
            update_cluster(Acc, ClusterIdx, Vec)
        end,
        Empty,
        Assignments
    ).

update_cluster(Clusters, Idx, Vec) ->
    {Before, [Cluster | After]} = lists:split(Idx - 1, Clusters),
    Before ++ [[Vec | Cluster] | After].

compute_centroid([], OldCentroid) ->
    OldCentroid;  %% Keep old centroid if cluster is empty
compute_centroid(Vectors, _) ->
    N = length(Vectors),
    Dim = length(hd(Vectors)),
    Sums = lists:foldl(
        fun(Vec, Acc) ->
            [A + V || {A, V} <- lists:zip(Acc, Vec)]
        end,
        [0.0 || _ <- lists:seq(1, Dim)],
        Vectors
    ),
    [S / N || S <- Sums].

squared_euclidean(Vec1, Vec2) ->
    lists:sum([math:pow(A - B, 2) || {A, B} <- lists:zip(Vec1, Vec2)]).

%% Encode codebook as binary (K centroids * SubDim floats)
encode_codebook(Centroids, SubDim) ->
    << <<F:32/float-little>> || C <- Centroids, F <- pad_centroid(C, SubDim) >>.

pad_centroid(Centroid, SubDim) when length(Centroid) =:= SubDim ->
    Centroid;
pad_centroid(Centroid, SubDim) when length(Centroid) < SubDim ->
    Centroid ++ [0.0 || _ <- lists:seq(1, SubDim - length(Centroid))].

%% Find nearest centroid index (0-255)
find_nearest_centroid(Subvec, Codebook, SubDim) ->
    find_nearest_centroid(Subvec, Codebook, SubDim, 0, 0, infinity).

find_nearest_centroid(_Subvec, <<>>, _SubDim, BestIdx, _CurIdx, _BestDist) ->
    BestIdx;
find_nearest_centroid(Subvec, Codebook, SubDim, BestIdx, CurIdx, BestDist) ->
    ByteSize = SubDim * 4,
    <<CentroidBin:ByteSize/binary, Rest/binary>> = Codebook,
    Centroid = [F || <<F:32/float-little>> <= CentroidBin],
    Dist = squared_euclidean(Subvec, Centroid),
    {NewBestIdx, NewBestDist} = case Dist < BestDist of
        true -> {CurIdx, Dist};
        false -> {BestIdx, BestDist}
    end,
    find_nearest_centroid(Subvec, Rest, SubDim, NewBestIdx, CurIdx + 1, NewBestDist).

%% Get centroid from codebook by index
get_centroid(Idx, Codebook, SubDim) ->
    ByteOffset = Idx * SubDim * 4,
    ByteSize = SubDim * 4,
    <<_:ByteOffset/binary, CentroidBin:ByteSize/binary, _/binary>> = Codebook,
    [F || <<F:32/float-little>> <= CentroidBin].

%% Compute distances from query subvector to all K centroids
compute_distances_to_centroids(QuerySub, Codebook, K, SubDim) ->
    compute_distances_to_centroids(QuerySub, Codebook, K, SubDim, []).

compute_distances_to_centroids(_QuerySub, <<>>, 0, _SubDim, Acc) ->
    lists:reverse(Acc);
compute_distances_to_centroids(QuerySub, Codebook, Remaining, SubDim, Acc) ->
    ByteSize = SubDim * 4,
    <<CentroidBin:ByteSize/binary, Rest/binary>> = Codebook,
    Centroid = [F || <<F:32/float-little>> <= CentroidBin],
    Dist = squared_euclidean(QuerySub, Centroid),
    DistBin = <<Dist:32/float-little>>,
    compute_distances_to_centroids(QuerySub, Rest, Remaining - 1, SubDim, [DistBin | Acc]).

%% Fast distance using precomputed tables
distance_acc(_Tables, <<>>, _Offset, 0, _K, Acc) ->
    math:sqrt(Acc);  %% Return Euclidean distance
distance_acc(Tables, <<Code:8, RestCodes/binary>>, Offset, M, K, Acc) ->
    %% Each table is K*4 bytes (K float32 values)
    TableOffset = Offset * K * 4,
    EntryOffset = Code * 4,
    <<_:TableOffset/binary, Table:(K*4)/binary, _/binary>> = Tables,
    <<_:EntryOffset/binary, Dist:32/float-little, _/binary>> = Table,
    distance_acc(Tables, RestCodes, Offset + 1, M - 1, K, Acc + Dist).
