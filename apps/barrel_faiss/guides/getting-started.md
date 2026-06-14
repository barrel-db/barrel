# Getting Started

## Installation

### Prerequisites

Install FAISS library on your system:

**macOS (Homebrew)**
```bash
brew install faiss libomp
```

**macOS (MacPorts)**
```bash
sudo port install libfaiss
```

**Linux (Debian/Ubuntu)**
```bash
apt install libfaiss-dev libomp-dev
```

**FreeBSD**
```bash
pkg install faiss
```

### Add to Your Project

Add `faiss` as a dependency in `rebar.config`:

```erlang
{deps, [
    {barrel_faiss, "0.2.0"}
]}.
```

## Basic Usage

### Creating an Index

```erlang
%% Create a flat L2 index for 128-dimensional vectors
{ok, Index} = barrel_faiss:new(128).

%% Or specify the metric type
{ok, IndexIP} = barrel_faiss:new(128, inner_product).
```

### Adding Vectors

Vectors are packed as 32-bit floats in native endianness:

```erlang
%% Create 3 vectors of dimension 4
Vectors = <<
    1.0:32/float-native, 2.0:32/float-native, 3.0:32/float-native, 4.0:32/float-native,
    5.0:32/float-native, 6.0:32/float-native, 7.0:32/float-native, 8.0:32/float-native,
    9.0:32/float-native, 10.0:32/float-native, 11.0:32/float-native, 12.0:32/float-native
>>,
ok = barrel_faiss:add(Index, Vectors).
```

### Searching

```erlang
%% Search for 5 nearest neighbors
Query = <<1.5:32/float-native, 2.5:32/float-native, 3.5:32/float-native, 4.5:32/float-native>>,
{ok, Distances, Labels} = barrel_faiss:search(Index, Query, 5).

%% Parse results
DistList = [D || <<D:32/float-native>> <= Distances],
LabelList = [L || <<L:64/signed-native>> <= Labels].
```

### Cleanup

```erlang
ok = barrel_faiss:close(Index).
```

## Index Types

Use `index_factory/2` for different index types:

```erlang
%% Flat index (exact search)
{ok, Flat} = barrel_faiss:index_factory(128, <<"Flat">>).

%% HNSW (fast approximate search)
{ok, HNSW} = barrel_faiss:index_factory(128, <<"HNSW32">>).

%% IVF (requires training)
{ok, IVF} = barrel_faiss:index_factory(128, <<"IVF100,Flat">>).
```

## Training IVF Indexes

IVF indexes require training before use:

```erlang
{ok, Index} = barrel_faiss:index_factory(128, <<"IVF100,Flat">>),
false = barrel_faiss:is_trained(Index),

%% Train with at least nlist * 39 vectors (100 * 39 = 3900)
ok = barrel_faiss:train(Index, TrainingVectors),
true = barrel_faiss:is_trained(Index),

%% Now you can add vectors
ok = barrel_faiss:add(Index, Vectors).
```

## Next Steps

- See [K/V Database Integration](kv-integration.md) for persistence patterns
- Check the [API Reference](faiss.html) for complete function documentation
