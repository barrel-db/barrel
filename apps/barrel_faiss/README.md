# barrel_faiss

Erlang NIF bindings for [FAISS](https://github.com/facebookresearch/faiss) (Facebook AI Similarity Search).

FAISS is a library for efficient similarity search and clustering of dense vectors.

## Requirements

- Erlang/OTP 24+
- CMake 3.14+
- C++17 compiler
- FAISS library

### Installing FAISS

**macOS (Homebrew):**
```bash
brew install faiss libomp
```

**macOS (MacPorts):**
```bash
sudo port install libfaiss
```

**Linux (Debian/Ubuntu):**
```bash
apt install libfaiss-dev libomp-dev libopenblas-dev
```

**FreeBSD:**
```bash
pkg install faiss openblas
```

## Build

```bash
rebar3 compile
```

## Usage

### Creating an Index

```erlang
%% Create a flat L2 index with dimension 128
{ok, Index} = barrel_faiss:new(128).

%% Create with inner product metric
{ok, Index} = barrel_faiss:new(128, inner_product).

%% Create using factory string (supports all FAISS index types)
{ok, FlatIndex} = barrel_faiss:index_factory(128, <<"Flat">>).
{ok, HnswIndex} = barrel_faiss:index_factory(128, <<"HNSW32">>).
{ok, IvfIndex} = barrel_faiss:index_factory(128, <<"IVF100,Flat">>).
```

### Adding Vectors

Vectors are passed as binaries of packed 32-bit floats in native endianness:

```erlang
%% Create 3 vectors of dimension 4
Vectors = <<
    1.0:32/float-native, 2.0:32/float-native, 3.0:32/float-native, 4.0:32/float-native,
    5.0:32/float-native, 6.0:32/float-native, 7.0:32/float-native, 8.0:32/float-native,
    9.0:32/float-native, 10.0:32/float-native, 11.0:32/float-native, 12.0:32/float-native
>>,

{ok, Index} = barrel_faiss:new(4),
ok = barrel_faiss:add(Index, Vectors),
3 = barrel_faiss:ntotal(Index).
```

### Searching

```erlang
%% Search for 5 nearest neighbors
Query = <<1.5:32/float-native, 2.5:32/float-native, 3.5:32/float-native, 4.5:32/float-native>>,
{ok, Distances, Labels} = barrel_faiss:search(Index, Query, 5),

%% Parse results
DistanceList = [D || <<D:32/float-native>> <= Distances],
LabelList = [L || <<L:64/signed-native>> <= Labels].
```

### Adding Vectors with IDs

For indexes that support ID assignment (IVF indexes):

```erlang
%% Add vectors with explicit IDs
Ids = <<100:64/signed-native, 200:64/signed-native, 300:64/signed-native>>,
ok = barrel_faiss:add_with_ids(Index, Vectors, Ids).
```

### Removing Vectors

Remove vectors by ID (supported by Flat and IVF indexes, not HNSW):

```erlang
%% Remove vectors by their IDs
IdsToRemove = <<100:64/signed-native, 200:64/signed-native>>,
{ok, NumRemoved} = barrel_faiss:remove_ids(Index, IdsToRemove).
```

### Training IVF Indexes

IVF indexes require training before adding vectors:

```erlang
{ok, Index} = barrel_faiss:index_factory(128, <<"IVF100,Flat">>),
false = barrel_faiss:is_trained(Index),

%% Train with sample vectors (need enough for 100 centroids)
TrainingData = generate_random_vectors(10000, 128),
ok = barrel_faiss:train(Index, TrainingData),
true = barrel_faiss:is_trained(Index),

%% Now you can add vectors
ok = barrel_faiss:add(Index, Vectors).
```

### Serialization (for K/V Storage)

Serialize indexes to binary for storage in RocksDB, ETS, or any K/V store:

```erlang
%% Serialize to binary
{ok, Binary} = barrel_faiss:serialize(Index),

%% Store in your K/V store
ok = rocksdb:put(Db, <<"my_index">>, Binary),

%% Later, deserialize
{ok, Binary2} = rocksdb:get(Db, <<"my_index">>),
{ok, Index2} = barrel_faiss:deserialize(Binary2).
```

### File I/O

```erlang
%% Save to file
ok = barrel_faiss:write_index(Index, <<"/path/to/index.faiss">>),

%% Load from file
{ok, Index2} = barrel_faiss:read_index(<<"/path/to/index.faiss">>).
```

### Index Properties

```erlang
Dim = barrel_faiss:dimension(Index),      %% Vector dimension
N = barrel_faiss:ntotal(Index),           %% Number of vectors
Trained = barrel_faiss:is_trained(Index). %% Training status
```

### Cleanup

```erlang
ok = barrel_faiss:close(Index).
```

## Index Types

The `index_factory/2,3` function supports all FAISS index types:

| Description | Factory String | Notes |
|-------------|----------------|-------|
| Flat (exact search) | `<<"Flat">>` | No training needed |
| HNSW | `<<"HNSW32">>` | No training needed, fast |
| IVF | `<<"IVF100,Flat">>` | Requires training |
| IVF + PQ | `<<"IVF100,PQ8">>` | Requires training, compressed |
| PQ | `<<"PQ8">>` | Requires training |

## Performance Notes

- `add/2`, `search/3`, `train/2`, `serialize/1`, `deserialize/1` run on dirty CPU schedulers
- `write_index/2`, `read_index/1` run on dirty IO schedulers
- Use HNSW for fast approximate search without training
- Use IVF indexes for large datasets (millions of vectors)

## License

Apache-2.0
