%% @doc Erlang bindings for FAISS vector similarity search.
%%
%% FAISS (Facebook AI Similarity Search) is a library for efficient
%% similarity search and clustering of dense vectors.
%%
%% == Quick Start ==
%%
%% ```
%% %% Create a flat index for 128-dimensional vectors
%% {ok, Index} = barrel_faiss:new(128),
%%
%% %% Add vectors (binary of packed float32)
%% Vectors = <<1.0:32/float-native, 2.0:32/float-native, ...>>,
%% ok = barrel_faiss:add(Index, Vectors),
%%
%% %% Search for nearest neighbors
%% {ok, Distances, Labels} = barrel_faiss:search(Index, Query, 10),
%%
%% %% Clean up
%% ok = barrel_faiss:close(Index).
%% '''
%%
%% == Vector Format ==
%%
%% Vectors are passed as binaries of packed 32-bit floats in native endianness.
%% For n vectors of dimension d, the binary size is `n * d * 4' bytes.
%%
%% == Index Types ==
%%
%% Use {@link index_factory/2} to create different index types:
%% <ul>
%%   <li>`<<"Flat">>' - Exact search, no training needed</li>
%%   <li>`<<"HNSW32">>' - Fast approximate search, no training</li>
%%   <li>`<<"IVF100,Flat">>' - Inverted file index, requires training</li>
%%   <li>`<<"IVF100,PQ8">>' - IVF with product quantization</li>
%% </ul>
%%
%% @end
-module(barrel_faiss).

%% Index creation
-export([new/1, new/2, index_factory/2, index_factory/3, close/1]).

%% Index properties
-export([dimension/1, is_trained/1, ntotal/1]).

%% Core operations
-export([add/2, add_with_ids/3, search/3, train/2]).

%% Deletion
-export([remove_ids/2]).

%% Serialization
-export([serialize/1, deserialize/1]).

%% File I/O
-export([write_index/2, read_index/1]).

%% Types
-export_type([index/0, metric_type/0]).

-opaque index() :: reference().
-type metric_type() :: l2 | inner_product.

-on_load(init/0).

-define(nif_stub, erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]})).

init() ->
    PrivDir = code:priv_dir(barrel_faiss),
    ok = erlang:load_nif(filename:join(PrivDir, "barrel_faiss"), 0).

%% @doc Create a new flat L2 index.
%%
%% Equivalent to `new(Dimension, l2)'.
%%
%% @see new/2
-spec new(Dimension :: pos_integer()) -> {ok, index()} | {error, term()}.
new(Dimension) ->
    new(Dimension, l2).

%% @doc Create a new flat index with specified metric.
%%
%% Creates an `IndexFlat' which performs exact (brute-force) search.
%% No training is required.
%%
%% == Metrics ==
%% <ul>
%%   <li>`l2' - Euclidean distance (smaller = more similar)</li>
%%   <li>`inner_product' - Inner product (larger = more similar)</li>
%% </ul>
%%
%% == Example ==
%% ```
%% {ok, Index} = barrel_faiss:new(128, l2),
%% true = barrel_faiss:is_trained(Index).
%% '''
-spec new(Dimension :: pos_integer(), Metric :: metric_type()) ->
    {ok, index()} | {error, term()}.
new(_Dimension, _Metric) ->
    ?nif_stub.

%% @doc Create an index using a factory string.
%%
%% Equivalent to `index_factory(Dimension, Description, l2)'.
%%
%% @see index_factory/3
-spec index_factory(Dimension :: pos_integer(), Description :: binary()) ->
    {ok, index()} | {error, term()}.
index_factory(Dimension, Description) ->
    index_factory(Dimension, Description, l2).

%% @doc Create an index using a factory string with specified metric.
%%
%% The factory string describes the index structure. Common options:
%%
%% <ul>
%%   <li>`<<"Flat">>' - Exact search</li>
%%   <li>`<<"HNSW32">>' - Hierarchical NSW graph (32 neighbors)</li>
%%   <li>`<<"IVF100,Flat">>' - IVF with 100 centroids</li>
%%   <li>`<<"IVF100,PQ8">>' - IVF + product quantization (8 bytes/vector)</li>
%%   <li>`<<"PQ16">>' - Product quantization only</li>
%% </ul>
%%
%% IVF indexes require training before adding vectors.
%%
%% == Example ==
%% ```
%% {ok, Index} = barrel_faiss:index_factory(128, <<"HNSW32">>),
%% ok = barrel_faiss:add(Index, Vectors).
%% '''
-spec index_factory(Dimension :: pos_integer(), Description :: binary(),
                    Metric :: metric_type()) ->
    {ok, index()} | {error, term()}.
index_factory(_Dimension, _Description, _Metric) ->
    ?nif_stub.

%% @doc Close and release index resources.
%%
%% After closing, the index reference becomes invalid.
-spec close(Index :: index()) -> ok.
close(_Index) ->
    ?nif_stub.

%% @doc Get the dimension of vectors in the index.
-spec dimension(Index :: index()) -> pos_integer() | {error, term()}.
dimension(_Index) ->
    ?nif_stub.

%% @doc Check if the index is trained.
%%
%% Flat and HNSW indexes are always trained.
%% IVF indexes require explicit training with {@link train/2}.
-spec is_trained(Index :: index()) -> boolean() | {error, term()}.
is_trained(_Index) ->
    ?nif_stub.

%% @doc Get the number of vectors in the index.
-spec ntotal(Index :: index()) -> non_neg_integer() | {error, term()}.
ntotal(_Index) ->
    ?nif_stub.

%% @doc Add vectors to the index.
%%
%% Vectors must be a binary of packed float32 values in native endianness.
%% The binary size must be `n * dimension * 4' bytes.
%%
%% This function runs on a dirty CPU scheduler.
%%
%% == Example ==
%% ```
%% %% Add 3 vectors of dimension 4
%% Vectors = <<1.0:32/float-native, 2.0:32/float-native, 3.0:32/float-native, 4.0:32/float-native,
%%             5.0:32/float-native, 6.0:32/float-native, 7.0:32/float-native, 8.0:32/float-native,
%%             9.0:32/float-native, 10.0:32/float-native, 11.0:32/float-native, 12.0:32/float-native>>,
%% ok = barrel_faiss:add(Index, Vectors).
%% '''
-spec add(Index :: index(), Vectors :: binary()) -> ok | {error, term()}.
add(_Index, _Vectors) ->
    ?nif_stub.

%% @doc Add vectors with explicit IDs.
%%
%% Vectors must be a binary of packed float32 values in native endianness.
%% IDs must be a binary of packed int64 values in native endianness.
%% The number of vectors must match the number of IDs.
%%
%% Note: Not all index types support add_with_ids. Use IDMap wrapper if needed.
%%
%% This function runs on a dirty CPU scheduler.
%%
%% == Example ==
%% ```
%% Vectors = <<1.0:32/float-native, 2.0:32/float-native, 3.0:32/float-native, 4.0:32/float-native>>,
%% IDs = <<100:64/signed-native>>,
%% ok = barrel_faiss:add_with_ids(Index, Vectors, IDs).
%% '''
-spec add_with_ids(Index :: index(), Vectors :: binary(), IDs :: binary()) ->
    ok | {error, term()}.
add_with_ids(_Index, _Vectors, _IDs) ->
    ?nif_stub.

%% @doc Search for k nearest neighbors.
%%
%% Queries must be a binary of packed float32 values.
%% Returns distances and labels as binaries.
%%
%% This function runs on a dirty CPU scheduler.
%%
%% == Return Format ==
%% <ul>
%%   <li>`Distances' - `nq * k' float32 values</li>
%%   <li>`Labels' - `nq * k' int64 values (-1 if fewer than k results)</li>
%% </ul>
%%
%% == Example ==
%% ```
%% Query = <<1.5:32/float-native, 2.5:32/float-native, 3.5:32/float-native, 4.5:32/float-native>>,
%% {ok, Distances, Labels} = barrel_faiss:search(Index, Query, 5),
%% DistList = [D || <<D:32/float-native>> <= Distances],
%% LabelList = [L || <<L:64/signed-native>> <= Labels].
%% '''
-spec search(Index :: index(), Queries :: binary(), K :: pos_integer()) ->
    {ok, Distances :: binary(), Labels :: binary()} | {error, term()}.
search(_Index, _Queries, _K) ->
    ?nif_stub.

%% @doc Train the index with sample vectors.
%%
%% Required for IVF indexes before adding vectors.
%% The number of training vectors should be at least `nlist * 39' where
%% `nlist' is the number of centroids (e.g., 100 for `IVF100').
%%
%% This function runs on a dirty CPU scheduler.
%%
%% == Example ==
%% ```
%% {ok, Index} = barrel_faiss:index_factory(128, <<"IVF100,Flat">>),
%% false = barrel_faiss:is_trained(Index),
%% ok = barrel_faiss:train(Index, TrainingVectors),
%% true = barrel_faiss:is_trained(Index).
%% '''
-spec train(Index :: index(), Vectors :: binary()) -> ok | {error, term()}.
train(_Index, _Vectors) ->
    ?nif_stub.

%% @doc Remove vectors by ID.
%%
%% IDs must be a binary of packed int64 values in native endianness.
%% Returns the number of vectors actually removed.
%%
%% Note: Not all index types support removal. HNSW indexes will return an error.
%%
%% This function runs on a dirty CPU scheduler.
%%
%% == Example ==
%% ```
%% %% Remove vectors with IDs 0, 1, 2
%% IDs = <<0:64/signed-native, 1:64/signed-native, 2:64/signed-native>>,
%% {ok, Removed} = barrel_faiss:remove_ids(Index, IDs),
%% 3 = Removed.
%% '''
-spec remove_ids(Index :: index(), IDs :: binary()) ->
    {ok, Removed :: non_neg_integer()} | {error, term()}.
remove_ids(_Index, _IDs) ->
    ?nif_stub.

%% @doc Serialize index to binary.
%%
%% The returned binary can be stored in any K/V database and later
%% restored with {@link deserialize/1}.
%%
%% This function runs on a dirty CPU scheduler.
%%
%% == Example ==
%% ```
%% {ok, Binary} = barrel_faiss:serialize(Index),
%% ok = rocksdb:put(Db, <<"my_index">>, Binary).
%% '''
%%
%% @see deserialize/1
-spec serialize(Index :: index()) -> {ok, binary()} | {error, term()}.
serialize(_Index) ->
    ?nif_stub.

%% @doc Deserialize index from binary.
%%
%% Restores an index previously serialized with {@link serialize/1}.
%%
%% This function runs on a dirty CPU scheduler.
%%
%% == Example ==
%% ```
%% {ok, Binary} = rocksdb:get(Db, <<"my_index">>),
%% {ok, Index} = barrel_faiss:deserialize(Binary).
%% '''
%%
%% @see serialize/1
-spec deserialize(Binary :: binary()) -> {ok, index()} | {error, term()}.
deserialize(_Binary) ->
    ?nif_stub.

%% @doc Write index to file.
%%
%% This function runs on a dirty IO scheduler.
-spec write_index(Index :: index(), Path :: binary()) -> ok | {error, term()}.
write_index(_Index, _Path) ->
    ?nif_stub.

%% @doc Read index from file.
%%
%% This function runs on a dirty IO scheduler.
-spec read_index(Path :: binary()) -> {ok, index()} | {error, term()}.
read_index(_Path) ->
    ?nif_stub.
