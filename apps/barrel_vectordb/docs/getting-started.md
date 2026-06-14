# Quick Start

Get started with Barrel VectorDB in a few minutes.

## Installation

Add to your `rebar.config`:

```erlang
{deps, [
    {barrel_vectordb, {git, "https://github.com/barrel-db/barrel_vectordb.git", {branch, "main"}}}
]}.
```

## Basic Usage

### With Embeddings

If you want automatic text embedding, configure an embedder:

```erlang
%% Start a store with local Python embeddings
{ok, _} = barrel_vectordb:start_link(#{
    name => my_store,
    path => "/tmp/vectors",
    embedder => {local, #{}}  %% requires Python + sentence-transformers
}).

%% Add documents (text is embedded automatically)
ok = barrel_vectordb:add(my_store, <<"doc1">>, <<"Hello world">>, #{}).
ok = barrel_vectordb:add(my_store, <<"doc2">>, <<"Goodbye world">>, #{}).

%% Search with text query
{ok, Results} = barrel_vectordb:search(my_store, <<"hi there">>, #{k => 5}).
%% => [#{key => <<"doc1">>, text => <<"Hello world">>, score => 0.89, ...}, ...]
```

### Vector-Only Mode

If you have pre-computed vectors, skip the embedder:

```erlang
%% Start a store without embedder
{ok, _} = barrel_vectordb:start_link(#{
    name => my_store,
    path => "/tmp/vectors",
    dimensions => 768
}).

%% Add with pre-computed vectors
ok = barrel_vectordb:add_vector(my_store, <<"doc1">>, <<"Hello">>, #{}, Vector).

%% Search with vector query
{ok, Results} = barrel_vectordb:search_vector(my_store, QueryVector, #{k => 5}).
```

## Core API

### Add Documents

```erlang
%% Add with text (requires embedder)
ok = barrel_vectordb:add(Store, Id, Text, Metadata).

%% Add with explicit vector (no embedder required)
ok = barrel_vectordb:add_vector(Store, Id, Text, Metadata, Vector).

%% Add batch (requires embedder)
{ok, #{inserted := N}} = barrel_vectordb:add_batch(Store, [
    {<<"id1">>, <<"text 1">>, #{type => a}},
    {<<"id2">>, <<"text 2">>, #{type => b}}
]).
```

### Search

```erlang
%% Search with text query (requires embedder)
{ok, Results} = barrel_vectordb:search(Store, <<"query text">>, #{k => 10}).

%% Search with vector (no embedder required)
{ok, Results} = barrel_vectordb:search_vector(Store, Vector, #{k => 10}).

%% Search with metadata filter
{ok, Results} = barrel_vectordb:search(Store, <<"query">>, #{
    k => 10,
    filter => fun(Meta) -> maps:get(type, Meta) =:= important end
}).

%% Search with optimized options (skip text/metadata for faster results)
{ok, Results} = barrel_vectordb:search_vector(Store, Vector, #{
    k => 50,
    include_text => false,
    include_metadata => false
}).
```

### Document Operations

```erlang
%% Get document by ID
{ok, Doc} = barrel_vectordb:get(Store, <<"doc1">>).

%% Update document (requires embedder)
ok = barrel_vectordb:update(Store, <<"doc1">>, <<"New text">>, #{}).

%% Upsert (requires embedder)
ok = barrel_vectordb:upsert(Store, <<"doc1">>, <<"Text">>, #{}).

%% Delete
ok = barrel_vectordb:delete(Store, <<"doc1">>).

%% Count
N = barrel_vectordb:count(Store).

%% Checkpoint HNSW index (speeds up restart)
ok = barrel_vectordb:checkpoint(Store).
```

## Configuration

```erlang
barrel_vectordb:start_link(#{
    name => my_store,              %% Store name (required)
    path => "/var/data/vectors",   %% RocksDB path
    dimensions => 768,             %% Vector dimensions (default: 768)
    backend => hnsw,               %% Index backend: hnsw (default), faiss, or diskann
    embedder => EmbedderConfig,    %% Embedding provider (optional)
    hnsw => #{                     %% HNSW index parameters
        m => 16,
        ef_construction => 200
    },
    batch => #{                    %% Write batching options
        min_batch_size => 4,
        max_batch_size => 256
    }
}).
```

## Search Options

| Option | Default | Description |
|--------|---------|-------------|
| `k` | 5 | Number of results to return |
| `filter` | - | Function `fun(Metadata) -> boolean()` to filter results |
| `include_text` | true | Include text in results |
| `include_metadata` | true | Include metadata in results |
| `ef_search` | max(k, 50) | Search width (higher = better recall, slower) |

## HNSW Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `m` | 16 | Max connections per node |
| `ef_construction` | 200 | Build-time search width |
| `ef_search` | 50 | Default query-time search width |
| `distance_fn` | cosine | `cosine` or `euclidean` |

## Index Backends

### HNSW (Default)

Pure Erlang HNSW implementation. No external dependencies.

```erlang
{ok, _} = barrel_vectordb:start_link(#{
    name => my_store,
    path => "/tmp/vectors",
    backend => hnsw
}).
```

### FAISS

High-performance FAISS backend via [barrel_faiss](https://gitlab.enki.io/barrel-db/barrel_faiss) NIF.

```erlang
{ok, _} = barrel_vectordb:start_link(#{
    name => my_store,
    path => "/tmp/vectors",
    backend => faiss,
    faiss => #{
        index_type => <<"HNSW32">>,
        distance_fn => cosine
    }
}).
```

| Feature | HNSW | FAISS |
|---------|------|-------|
| Dependencies | None | barrel_faiss NIF |
| Insert speed | Baseline | 1.6-3x faster |
| Search speed | Baseline | 2x faster |
| Delete speed | Fast (native) | Slower (soft delete) |

### DiskANN

SSD-optimized Vamana graph for billion-scale vector search with minimal memory.

```erlang
{ok, _} = barrel_vectordb:start_link(#{
    name => my_store,
    path => "/tmp/vectors",
    backend => diskann,
    diskann => #{
        r => 64,                    %% Max out-degree
        l_build => 100,             %% Build search width
        l_search => 100,            %% Query search width
        storage_mode => disk,       %% memory | disk
        hot_enabled => true         %% Enable hot layer for fast writes
    }
}).
```

| Feature | HNSW | FAISS | DiskANN |
|---------|------|-------|---------|
| Dependencies | None | barrel_faiss NIF | None |
| Memory usage | ~4KB/vector | Medium | ~530B/vector (disk mode) |
| Insert speed | Fast | 1.6-3x faster | Slower (batch optimized) |
| Search speed | ~0.25ms | 2x faster | ~1ms |
| Best for | Small-medium | GPU, batch queries | Large-scale, memory-constrained |

See [DiskANN documentation](features.md#diskann-index) for detailed configuration.

## Next Steps

- [Embedding Models](embeddings.md) - Configure embedding providers
- [BM25 Text Search](bm25-disk.md) - Lexical and hybrid search
- [Erlang API](api/erlang.md) - Full API reference
