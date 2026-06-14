-module(barrel_faiss_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0, groups/0, init_per_suite/1, end_per_suite/1]).
-export([
    %% Basic tests
    new_l2_test/1,
    new_inner_product_test/1,
    dimension_test/1,

    %% Factory tests
    factory_flat_test/1,
    factory_hnsw_test/1,
    factory_ivf_test/1,
    factory_pq_test/1,
    factory_sq_test/1,
    factory_ivf_pq_test/1,

    %% Search tests
    search_exact_test/1,
    search_multi_query_test/1,
    search_k_larger_than_n_test/1,
    search_empty_index_test/1,
    search_inner_product_test/1,

    %% Add tests
    add_incremental_test/1,
    add_batch_test/1,

    %% Training tests
    ivf_training_test/1,

    %% Accuracy tests
    l2_distance_positive_test/1,
    search_order_test/1,

    %% Serialization tests
    serialize_flat_test/1,
    serialize_hnsw_test/1,
    serialize_trained_ivf_test/1,

    %% File I/O tests
    file_io_flat_test/1,
    file_io_hnsw_test/1,

    %% Deletion tests
    remove_ids_basic_test/1,
    remove_ids_batch_test/1,
    remove_ids_not_found_test/1,
    remove_ids_hnsw_error_test/1,
    add_with_ids_ivf_test/1
]).

all() ->
    [
        {group, basic},
        {group, factory},
        {group, search},
        {group, add},
        {group, training},
        {group, accuracy},
        {group, serialization},
        {group, file_io},
        {group, deletion}
    ].

groups() ->
    [
        {basic, [sequence], [
            new_l2_test,
            new_inner_product_test,
            dimension_test
        ]},
        {factory, [sequence], [
            factory_flat_test,
            factory_hnsw_test,
            factory_ivf_test,
            factory_pq_test,
            factory_sq_test,
            factory_ivf_pq_test
        ]},
        {search, [sequence], [
            search_exact_test,
            search_multi_query_test,
            search_k_larger_than_n_test,
            search_empty_index_test,
            search_inner_product_test
        ]},
        {add, [sequence], [
            add_incremental_test,
            add_batch_test
        ]},
        {training, [sequence], [
            ivf_training_test
        ]},
        {accuracy, [sequence], [
            l2_distance_positive_test,
            search_order_test
        ]},
        {serialization, [sequence], [
            serialize_flat_test,
            serialize_hnsw_test,
            serialize_trained_ivf_test
        ]},
        {file_io, [sequence], [
            file_io_flat_test,
            file_io_hnsw_test
        ]},
        {deletion, [sequence], [
            remove_ids_basic_test,
            remove_ids_batch_test,
            remove_ids_not_found_test,
            remove_ids_hnsw_error_test,
            add_with_ids_ivf_test
        ]}
    ].

init_per_suite(Config) ->
    rand:seed(exsplus, {1, 2, 3}),
    Config.

end_per_suite(_Config) ->
    ok.

%%====================================================================
%% Basic tests
%%====================================================================

new_l2_test(_Config) ->
    Dim = 128,
    {ok, Index} = barrel_faiss:new(Dim),
    128 = barrel_faiss:dimension(Index),
    true = barrel_faiss:is_trained(Index),
    0 = barrel_faiss:ntotal(Index),
    ok = barrel_faiss:close(Index).

new_inner_product_test(_Config) ->
    Dim = 64,
    {ok, Index} = barrel_faiss:new(Dim, inner_product),
    64 = barrel_faiss:dimension(Index),
    true = barrel_faiss:is_trained(Index),
    0 = barrel_faiss:ntotal(Index),
    ok = barrel_faiss:close(Index).

dimension_test(_Config) ->
    lists:foreach(fun(Dim) ->
        {ok, Index} = barrel_faiss:new(Dim),
        Dim = barrel_faiss:dimension(Index),
        ok = barrel_faiss:close(Index)
    end, [1, 8, 32, 128, 256, 512]).

%%====================================================================
%% Factory tests
%%====================================================================

factory_flat_test(_Config) ->
    Dim = 64,
    {ok, Index} = barrel_faiss:index_factory(Dim, <<"Flat">>),
    64 = barrel_faiss:dimension(Index),
    true = barrel_faiss:is_trained(Index),
    ok = barrel_faiss:close(Index),

    {ok, IndexIP} = barrel_faiss:index_factory(Dim, <<"Flat">>, inner_product),
    64 = barrel_faiss:dimension(IndexIP),
    true = barrel_faiss:is_trained(IndexIP),
    ok = barrel_faiss:close(IndexIP).

factory_hnsw_test(_Config) ->
    Dim = 32,
    {ok, Index} = barrel_faiss:index_factory(Dim, <<"HNSW32">>),
    32 = barrel_faiss:dimension(Index),
    true = barrel_faiss:is_trained(Index),
    ok = barrel_faiss:close(Index),

    %% HNSW with different M values
    {ok, Index16} = barrel_faiss:index_factory(Dim, <<"HNSW16">>),
    ok = barrel_faiss:close(Index16).

factory_ivf_test(_Config) ->
    Dim = 32,
    {ok, Index} = barrel_faiss:index_factory(Dim, <<"IVF16,Flat">>),
    32 = barrel_faiss:dimension(Index),
    false = barrel_faiss:is_trained(Index),
    ok = barrel_faiss:close(Index).

factory_pq_test(_Config) ->
    Dim = 32,
    {ok, Index} = barrel_faiss:index_factory(Dim, <<"PQ8">>),
    32 = barrel_faiss:dimension(Index),
    false = barrel_faiss:is_trained(Index),
    ok = barrel_faiss:close(Index).

factory_sq_test(_Config) ->
    Dim = 32,
    {ok, Index} = barrel_faiss:index_factory(Dim, <<"SQ8">>),
    32 = barrel_faiss:dimension(Index),
    %% SQ8 may need training in some FAISS versions
    case barrel_faiss:is_trained(Index) of
        true -> ok;
        false ->
            ok = barrel_faiss:train(Index, random_vectors(100, Dim)),
            true = barrel_faiss:is_trained(Index)
    end,
    ok = barrel_faiss:close(Index).

factory_ivf_pq_test(_Config) ->
    Dim = 32,
    {ok, Index} = barrel_faiss:index_factory(Dim, <<"IVF16,PQ8">>),
    32 = barrel_faiss:dimension(Index),
    false = barrel_faiss:is_trained(Index),
    ok = barrel_faiss:close(Index).

%%====================================================================
%% Search tests
%%====================================================================

search_exact_test(_Config) ->
    Dim = 4,
    {ok, Index} = barrel_faiss:new(Dim),

    %% Add vector [0,0,0,0]
    V0 = <<0.0:32/float-native, 0.0:32/float-native, 0.0:32/float-native, 0.0:32/float-native>>,
    ok = barrel_faiss:add(Index, V0),

    %% Search for exact same vector - distance should be 0
    {ok, DistBin, LabelBin} = barrel_faiss:search(Index, V0, 1),
    [Dist] = [D || <<D:32/float-native>> <= DistBin],
    [Label] = [L || <<L:64/signed-native>> <= LabelBin],

    true = (Dist == 0.0),
    0 = Label,

    ok = barrel_faiss:close(Index).

search_multi_query_test(_Config) ->
    Dim = 8,
    {ok, Index} = barrel_faiss:new(Dim),

    %% Add 10 vectors
    Vectors = random_vectors(10, Dim),
    ok = barrel_faiss:add(Index, Vectors),

    %% Search with 3 queries at once
    Queries = random_vectors(3, Dim),
    {ok, DistBin, LabelBin} = barrel_faiss:search(Index, Queries, 5),

    %% Should get 3 * 5 = 15 results
    Distances = [D || <<D:32/float-native>> <= DistBin],
    Labels = [L || <<L:64/signed-native>> <= LabelBin],

    15 = length(Distances),
    15 = length(Labels),

    ok = barrel_faiss:close(Index).

search_k_larger_than_n_test(_Config) ->
    Dim = 4,
    {ok, Index} = barrel_faiss:new(Dim),

    %% Add only 3 vectors
    Vectors = random_vectors(3, Dim),
    ok = barrel_faiss:add(Index, Vectors),

    %% Search for K=10 (larger than ntotal=3)
    Query = random_vectors(1, Dim),
    {ok, DistBin, LabelBin} = barrel_faiss:search(Index, Query, 10),

    Distances = [D || <<D:32/float-native>> <= DistBin],
    Labels = [L || <<L:64/signed-native>> <= LabelBin],

    %% Should get 10 results, but last 7 will have label -1
    10 = length(Distances),
    10 = length(Labels),

    %% First 3 should be valid labels (0, 1, 2)
    ValidLabels = lists:sublist(Labels, 3),
    true = lists:all(fun(L) -> L >= 0 andalso L < 3 end, ValidLabels),

    %% Last 7 should be -1
    InvalidLabels = lists:nthtail(3, Labels),
    true = lists:all(fun(L) -> L == -1 end, InvalidLabels),

    ok = barrel_faiss:close(Index).

search_empty_index_test(_Config) ->
    Dim = 4,
    {ok, Index} = barrel_faiss:new(Dim),
    0 = barrel_faiss:ntotal(Index),

    %% Search empty index
    Query = random_vectors(1, Dim),
    {ok, _DistBin, LabelBin} = barrel_faiss:search(Index, Query, 5),

    Labels = [L || <<L:64/signed-native>> <= LabelBin],

    %% All labels should be -1
    5 = length(Labels),
    true = lists:all(fun(L) -> L == -1 end, Labels),

    ok = barrel_faiss:close(Index).

search_inner_product_test(_Config) ->
    Dim = 4,
    {ok, Index} = barrel_faiss:new(Dim, inner_product),

    %% Add normalized vectors
    V1 = <<1.0:32/float-native, 0.0:32/float-native, 0.0:32/float-native, 0.0:32/float-native>>,
    V2 = <<0.0:32/float-native, 1.0:32/float-native, 0.0:32/float-native, 0.0:32/float-native>>,
    ok = barrel_faiss:add(Index, <<V1/binary, V2/binary>>),

    %% Query with V1 - should match V1 best (highest inner product)
    {ok, DistBin, LabelBin} = barrel_faiss:search(Index, V1, 2),
    [D1, D2] = [D || <<D:32/float-native>> <= DistBin],
    [L1, L2] = [L || <<L:64/signed-native>> <= LabelBin],

    ct:pal("IP distances: ~p, labels: ~p~n", [[D1, D2], [L1, L2]]),

    %% First result should be label 0 (V1 matches itself)
    0 = L1,
    %% Second result should be label 1 (V2)
    1 = L2,

    ok = barrel_faiss:close(Index).

%%====================================================================
%% Add tests
%%====================================================================

add_incremental_test(_Config) ->
    Dim = 8,
    {ok, Index} = barrel_faiss:new(Dim),

    %% Add vectors incrementally
    ok = barrel_faiss:add(Index, random_vectors(5, Dim)),
    5 = barrel_faiss:ntotal(Index),

    ok = barrel_faiss:add(Index, random_vectors(10, Dim)),
    15 = barrel_faiss:ntotal(Index),

    ok = barrel_faiss:add(Index, random_vectors(25, Dim)),
    40 = barrel_faiss:ntotal(Index),

    ok = barrel_faiss:close(Index).

add_batch_test(_Config) ->
    Dim = 64,
    N = 1000,
    {ok, Index} = barrel_faiss:new(Dim),

    %% Add 1000 vectors at once
    Vectors = random_vectors(N, Dim),
    ok = barrel_faiss:add(Index, Vectors),
    N = barrel_faiss:ntotal(Index),

    %% Search should work
    Query = random_vectors(1, Dim),
    {ok, _Dist, Labels} = barrel_faiss:search(Index, Query, 10),
    LabelList = [L || <<L:64/signed-native>> <= Labels],
    true = lists:all(fun(L) -> L >= 0 andalso L < N end, LabelList),

    ok = barrel_faiss:close(Index).

%%====================================================================
%% Training tests
%%====================================================================

ivf_training_test(_Config) ->
    Dim = 32,
    Nlist = 16,
    {ok, Index} = barrel_faiss:index_factory(Dim, <<"IVF16,Flat">>),

    %% Should not be trained initially
    false = barrel_faiss:is_trained(Index),

    %% Train with enough vectors (need at least nlist * 39 for good clustering)
    TrainingVectors = random_vectors(Nlist * 50, Dim),
    ok = barrel_faiss:train(Index, TrainingVectors),

    %% Should be trained now
    true = barrel_faiss:is_trained(Index),

    %% Now we can add vectors
    ok = barrel_faiss:add(Index, random_vectors(100, Dim)),
    100 = barrel_faiss:ntotal(Index),

    %% And search
    Query = random_vectors(1, Dim),
    {ok, _Dist, _Labels} = barrel_faiss:search(Index, Query, 5),

    ok = barrel_faiss:close(Index).

%%====================================================================
%% Accuracy tests
%%====================================================================

l2_distance_positive_test(_Config) ->
    Dim = 16,
    {ok, Index} = barrel_faiss:new(Dim),

    %% Add random vectors
    ok = barrel_faiss:add(Index, random_vectors(100, Dim)),

    %% Search with multiple queries
    Queries = random_vectors(10, Dim),
    {ok, DistBin, _Labels} = barrel_faiss:search(Index, Queries, 10),

    %% All L2 distances should be non-negative
    Distances = [D || <<D:32/float-native>> <= DistBin],
    true = lists:all(fun(D) -> D >= 0.0 end, Distances),

    ok = barrel_faiss:close(Index).

search_order_test(_Config) ->
    Dim = 8,
    {ok, Index} = barrel_faiss:new(Dim),

    %% Add 50 random vectors
    ok = barrel_faiss:add(Index, random_vectors(50, Dim)),

    %% Search
    Query = random_vectors(1, Dim),
    {ok, DistBin, _Labels} = barrel_faiss:search(Index, Query, 10),

    %% Distances should be in ascending order (closest first)
    Distances = [D || <<D:32/float-native>> <= DistBin],
    Sorted = lists:sort(Distances),
    Distances = Sorted,

    ok = barrel_faiss:close(Index).

%%====================================================================
%% Serialization tests
%%====================================================================

serialize_flat_test(_Config) ->
    Dim = 16,
    {ok, Index1} = barrel_faiss:new(Dim),

    Vectors = random_vectors(20, Dim),
    ok = barrel_faiss:add(Index1, Vectors),

    {ok, Binary} = barrel_faiss:serialize(Index1),
    {ok, Index2} = barrel_faiss:deserialize(Binary),

    %% Properties should match
    Dim = barrel_faiss:dimension(Index1),
    Dim = barrel_faiss:dimension(Index2),
    NTotal = barrel_faiss:ntotal(Index1),
    NTotal = barrel_faiss:ntotal(Index2),

    %% Search results should match
    Query = random_vectors(1, Dim),
    {ok, D1, L1} = barrel_faiss:search(Index1, Query, 5),
    {ok, D2, L2} = barrel_faiss:search(Index2, Query, 5),
    D1 = D2,
    L1 = L2,

    ok = barrel_faiss:close(Index1),
    ok = barrel_faiss:close(Index2).

serialize_hnsw_test(_Config) ->
    Dim = 32,
    {ok, Index1} = barrel_faiss:index_factory(Dim, <<"HNSW16">>),

    ok = barrel_faiss:add(Index1, random_vectors(100, Dim)),

    {ok, Binary} = barrel_faiss:serialize(Index1),
    true = byte_size(Binary) > 0,

    {ok, Index2} = barrel_faiss:deserialize(Binary),

    Dim = barrel_faiss:dimension(Index2),
    100 = barrel_faiss:ntotal(Index2),

    ok = barrel_faiss:close(Index1),
    ok = barrel_faiss:close(Index2).

serialize_trained_ivf_test(_Config) ->
    Dim = 16,
    {ok, Index1} = barrel_faiss:index_factory(Dim, <<"IVF8,Flat">>),

    %% Train
    ok = barrel_faiss:train(Index1, random_vectors(400, Dim)),
    true = barrel_faiss:is_trained(Index1),

    %% Add
    ok = barrel_faiss:add(Index1, random_vectors(50, Dim)),

    %% Serialize/deserialize
    {ok, Binary} = barrel_faiss:serialize(Index1),
    {ok, Index2} = barrel_faiss:deserialize(Binary),

    %% Trained state should be preserved
    true = barrel_faiss:is_trained(Index2),
    50 = barrel_faiss:ntotal(Index2),

    ok = barrel_faiss:close(Index1),
    ok = barrel_faiss:close(Index2).

%%====================================================================
%% File I/O tests
%%====================================================================

file_io_flat_test(Config) ->
    Dim = 16,
    PrivDir = ?config(priv_dir, Config),
    Path = list_to_binary(filename:join(PrivDir, "flat_index.faiss")),

    {ok, Index1} = barrel_faiss:new(Dim),
    Vectors = random_vectors(50, Dim),
    ok = barrel_faiss:add(Index1, Vectors),

    ok = barrel_faiss:write_index(Index1, Path),

    {ok, Index2} = barrel_faiss:read_index(Path),
    Dim = barrel_faiss:dimension(Index2),
    50 = barrel_faiss:ntotal(Index2),

    %% Search results should match
    Query = random_vectors(1, Dim),
    {ok, D1, L1} = barrel_faiss:search(Index1, Query, 5),
    {ok, D2, L2} = barrel_faiss:search(Index2, Query, 5),
    D1 = D2,
    L1 = L2,

    ok = barrel_faiss:close(Index1),
    ok = barrel_faiss:close(Index2).

file_io_hnsw_test(Config) ->
    Dim = 32,
    PrivDir = ?config(priv_dir, Config),
    Path = list_to_binary(filename:join(PrivDir, "hnsw_index.faiss")),

    {ok, Index1} = barrel_faiss:index_factory(Dim, <<"HNSW16">>),
    ok = barrel_faiss:add(Index1, random_vectors(200, Dim)),

    ok = barrel_faiss:write_index(Index1, Path),

    {ok, Index2} = barrel_faiss:read_index(Path),
    Dim = barrel_faiss:dimension(Index2),
    200 = barrel_faiss:ntotal(Index2),

    ok = barrel_faiss:close(Index1),
    ok = barrel_faiss:close(Index2).

%%====================================================================
%% Deletion tests
%%====================================================================

remove_ids_basic_test(_Config) ->
    Dim = 4,
    {ok, Index} = barrel_faiss:new(Dim),

    %% Add 5 vectors
    Vectors = random_vectors(5, Dim),
    ok = barrel_faiss:add(Index, Vectors),
    5 = barrel_faiss:ntotal(Index),

    %% Remove vector with ID 2
    IDs = <<2:64/signed-native>>,
    {ok, Removed} = barrel_faiss:remove_ids(Index, IDs),
    1 = Removed,
    4 = barrel_faiss:ntotal(Index),

    ok = barrel_faiss:close(Index).

remove_ids_batch_test(_Config) ->
    Dim = 8,
    {ok, Index} = barrel_faiss:new(Dim),

    %% Add 10 vectors
    ok = barrel_faiss:add(Index, random_vectors(10, Dim)),
    10 = barrel_faiss:ntotal(Index),

    %% Remove vectors with IDs 1, 3, 5, 7
    IDs = <<1:64/signed-native, 3:64/signed-native, 5:64/signed-native, 7:64/signed-native>>,
    {ok, Removed} = barrel_faiss:remove_ids(Index, IDs),
    4 = Removed,
    6 = barrel_faiss:ntotal(Index),

    ok = barrel_faiss:close(Index).

remove_ids_not_found_test(_Config) ->
    Dim = 4,
    {ok, Index} = barrel_faiss:new(Dim),

    %% Add 3 vectors (IDs 0, 1, 2)
    ok = barrel_faiss:add(Index, random_vectors(3, Dim)),
    3 = barrel_faiss:ntotal(Index),

    %% Try to remove non-existent ID 999
    IDs = <<999:64/signed-native>>,
    {ok, Removed} = barrel_faiss:remove_ids(Index, IDs),
    0 = Removed,
    3 = barrel_faiss:ntotal(Index),

    ok = barrel_faiss:close(Index).

remove_ids_hnsw_error_test(_Config) ->
    Dim = 32,
    {ok, Index} = barrel_faiss:index_factory(Dim, <<"HNSW16">>),

    %% Add some vectors
    ok = barrel_faiss:add(Index, random_vectors(10, Dim)),
    10 = barrel_faiss:ntotal(Index),

    %% Try to remove - HNSW doesn't support removal
    IDs = <<0:64/signed-native>>,
    {error, _Reason} = barrel_faiss:remove_ids(Index, IDs),

    %% ntotal should still be 10 (unchanged)
    10 = barrel_faiss:ntotal(Index),

    ok = barrel_faiss:close(Index).

add_with_ids_ivf_test(_Config) ->
    Dim = 32,
    {ok, Index} = barrel_faiss:index_factory(Dim, <<"IVF16,Flat">>),

    %% Train first
    ok = barrel_faiss:train(Index, random_vectors(16 * 50, Dim)),
    true = barrel_faiss:is_trained(Index),

    %% Add vectors with explicit IDs: 100, 200, 300
    V1 = random_vectors(1, Dim),
    V2 = random_vectors(1, Dim),
    V3 = random_vectors(1, Dim),
    Vectors = <<V1/binary, V2/binary, V3/binary>>,
    IDs = <<100:64/signed-native, 200:64/signed-native, 300:64/signed-native>>,

    ok = barrel_faiss:add_with_ids(Index, Vectors, IDs),
    3 = barrel_faiss:ntotal(Index),

    %% Search for V1 - should return ID 100
    {ok, _DistBin, LabelBin} = barrel_faiss:search(Index, V1, 1),
    [Label] = [L || <<L:64/signed-native>> <= LabelBin],
    100 = Label,

    %% Remove ID 200
    RemoveIDs = <<200:64/signed-native>>,
    {ok, 1} = barrel_faiss:remove_ids(Index, RemoveIDs),
    2 = barrel_faiss:ntotal(Index),

    ok = barrel_faiss:close(Index).

%%====================================================================
%% Helpers
%%====================================================================

random_vectors(N, Dim) ->
    << <<(rand:uniform()):32/float-native>> || _ <- lists:seq(1, N * Dim) >>.
