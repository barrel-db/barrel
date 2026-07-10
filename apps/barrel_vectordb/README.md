<div align="center">

# barrel_vectordb

**High-performance vector database for Erlang with HNSW, FAISS, DiskANN, and BM25 backends**

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

[Documentation](https://barrel-db.eu/docs/lib/vectordb/) |
[Examples](./examples) |
[barrel-db.eu](https://barrel-db.eu)

</div>

---

## Quick Start

### With Embeddings

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

### Vector-Only (no embedder)

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

## Installation

Add to your `rebar.config`:

```erlang
{deps, [
    {barrel_vectordb, "2.1.0"}
]}.
```

This includes `barrel_embed` for embedding support. To use text-based operations (`add/4`, `search/3`), configure an embedder provider. Without an embedder configured, use `add_vector/5` and `search_vector/3` with pre-computed vectors.

### Optional: Reranking

For cross-encoder reranking, add barrel_rerank:

```erlang
{deps, [
    {barrel_vectordb, "2.1.0"},
    {barrel_rerank, "1.0.0"}
]}.
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
    include_text => false,      %% Skip text lookup
    include_metadata => false   %% Skip metadata lookup
}).

%% Search with custom ef_search (higher = better recall, slower)
{ok, Results} = barrel_vectordb:search_vector(Store, Vector, #{
    k => 10,
    ef_search => 200   %% Default is max(k, 50)
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

%% Peek (sample documents)
{ok, Docs} = barrel_vectordb:peek(Store, 10).

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
    backend => hnsw,               %% Index backend: hnsw (default) or faiss
    embedder => EmbedderConfig,    %% Embedding provider (optional)
    hnsw => #{                     %% HNSW index parameters
        m => 16,
        ef_construction => 200
    },
    batch => #{                    %% Write batching options
        min_batch_size => 4,       %% Min requests before batching
        max_batch_size => 256      %% Max batch size
    }
}).
```

## Index Backends

barrel_vectordb supports two vector index backends:

### HNSW (Default)

Pure Erlang HNSW implementation. No external dependencies.

```erlang
{ok, _} = barrel_vectordb:start_link(#{
    name => my_store,
    path => "/tmp/vectors",
    backend => hnsw  %% default, can be omitted
}).
```

### FAISS

High-performance FAISS backend via [barrel_faiss](https://github.com/barrel-db/barrel) NIF.
Typically 2-6x faster than pure Erlang HNSW for insert and search operations.

**Installation:**

Add to your `rebar.config`:

```erlang
{profiles, [
    {faiss, [
        {deps, [
            {barrel_faiss, {git, "https://github.com/barrel-db/barrel.git", {branch, "main"}}}
        ]}
    ]}
]}.
```

Requires FAISS library installed on your system. See [barrel_faiss README](https://github.com/barrel-db/barrel) for installation instructions.

**Usage:**

```erlang
{ok, _} = barrel_vectordb:start_link(#{
    name => my_store,
    path => "/tmp/vectors",
    backend => faiss,
    faiss => #{
        index_type => <<"HNSW32">>,  %% default
        distance_fn => cosine        %% cosine (default) or euclidean
    }
}).
```

**Backend Comparison:**

| Feature | HNSW | FAISS |
|---------|------|-------|
| Dependencies | None | barrel_faiss NIF |
| Insert speed | Baseline | 1.6-3x faster |
| Search speed | Baseline | 2x faster |
| Index build | Baseline | 6x faster |
| Delete speed | Fast (native) | Slower (soft delete) |
| Memory | Higher | Lower |

**When to use FAISS:**
- Large indexes (>100K vectors)
- High insert throughput requirements
- Search latency is critical

**When to use HNSW:**
- Simpler deployment (no NIF)
- Frequent deletions
- Smaller indexes

## Vector Quantization

Reduce memory usage with TurboQuant compression:

```erlang
%% Create quantizer (no training needed)
{ok, TQ} = barrel_vectordb_turboquant:new(#{
    dimension => 768,
    bits => 3
}).

%% Encode vector (768 floats -> ~388 bytes)
Code = barrel_vectordb_turboquant:encode(TQ, Vector).

%% Fast distance computation
Tables = barrel_vectordb_turboquant:precompute_tables(TQ, Query),
Distance = barrel_vectordb_turboquant:distance_nif(Tables, Code).
```

For large dimensions, use Subspace-TurboQuant:

```erlang
{ok, TQS} = barrel_vectordb_turboquant_subspace:new(#{
    dimension => 1536,
    m => 16  %% 16 subspaces
}).
```

See [TurboQuant Documentation](https://barrel-db.eu/docs/lib/vectordb/turboquant.html) for details.

## Embedding Providers

Embedder is **explicit** - if not configured, only `add_vector/5` and `search_vector/3` work.
Text-based operations return `{error, embedder_not_configured}`.

### Local

Local Python with sentence-transformers. CPU-based, no external API calls.

```erlang
embedder => {local, #{
    python => "python3",                %% Python executable (default)
    model => "BAAI/bge-base-en-v1.5",   %% Model name (default, 768 dims)
    timeout => 120000                   %% Timeout in ms (default)
}}
```

**Setup with virtual environment (recommended):**

```bash
# Create virtual environment
python3 -m venv ~/.venv/barrel_embed
source ~/.venv/barrel_embed/bin/activate

# Install dependencies
pip install sentence-transformers

# Verify installation
python -c "from sentence_transformers import SentenceTransformer; print('OK')"
```

Then start the store with the virtual environment's Python path:

```erlang
{ok, _} = barrel_vectordb:start_link(#{
    name => my_store,
    path => "/tmp/vectors",
    embedder => {local, #{
        python => "/home/user/.venv/barrel_embed/bin/python"
    }}
}).
```

Or activate the venv before starting your Erlang application:

```bash
source ~/.venv/barrel_embed/bin/activate
rebar3 shell
```

```erlang
%% Now python3 will use the venv automatically
{ok, _} = barrel_vectordb:start_link(#{
    name => my_store,
    path => "/tmp/vectors",
    embedder => {local, #{}}  %% uses default python3
}).
```

**Supported Models:**

Any sentence-transformers or HuggingFace model works. Popular choices:

| Model | Dimensions | Notes |
|-------|------------|-------|
| `BAAI/bge-base-en-v1.5` | 768 | Default, good quality/speed |
| `BAAI/bge-small-en-v1.5` | 384 | Faster, smaller |
| `BAAI/bge-large-en-v1.5` | 1024 | Best quality, slower |
| `sentence-transformers/all-MiniLM-L6-v2` | 384 | Fast, general purpose |
| `sentence-transformers/all-mpnet-base-v2` | 768 | High quality |
| `nomic-ai/nomic-embed-text-v1.5` | 768 | Long context (8192 tokens) |

The dimension is auto-detected from the model.

### Ollama

Local Ollama server. Requires [Ollama](https://ollama.ai) to be running.

```erlang
embedder => {ollama, #{
    url => <<"http://localhost:11434">>,   %% Ollama API URL (default)
    model => <<"nomic-embed-text">>,       %% Model name (default, 768 dims)
    timeout => 30000                       %% Timeout in ms (default)
}}
```

```bash
# Pull embedding models:
ollama pull nomic-embed-text
```

**Supported Models:**

| Model | Dimensions | Notes |
|-------|------------|-------|
| `nomic-embed-text` | 768 | Default, general purpose |
| `mxbai-embed-large` | 1024 | High quality |
| `all-minilm` | 384 | Fast |
| `snowflake-arctic-embed` | 1024 | Multilingual |

### OpenAI

OpenAI Embeddings API. Requires an API key.

```erlang
embedder => {openai, #{
    api_key => <<"sk-...">>,               %% API key (or set OPENAI_API_KEY env var)
    model => <<"text-embedding-3-small">>, %% Model name (default, 1536 dims)
    timeout => 30000                       %% Timeout in ms (default)
}}
```

```bash
# Set API key as environment variable (alternative to config)
export OPENAI_API_KEY=sk-...
```

**Supported Models:**

| Model | Dimensions | Notes |
|-------|------------|-------|
| `text-embedding-3-small` | 1536 | Default, fast and cheap |
| `text-embedding-3-large` | 3072 | Higher quality |
| `text-embedding-ada-002` | 1536 | Legacy model |

### FastEmbed

Lightweight ONNX-based embeddings. Faster than sentence-transformers for many models.

```erlang
embedder => {fastembed, #{
    python => "python3",                    %% Python executable (default)
    model => "BAAI/bge-small-en-v1.5",      %% Model name (default, 384 dims)
    timeout => 120000                       %% Timeout in ms (default)
}}
```

**Setup:**

```bash
pip install fastembed
```

**Supported Models:**

| Model | Dimensions | Notes |
|-------|------------|-------|
| `BAAI/bge-small-en-v1.5` | 384 | Default, fast |
| `BAAI/bge-base-en-v1.5` | 768 | Good balance |
| `sentence-transformers/all-MiniLM-L6-v2` | 384 | General purpose |

### Provider Chain

Try providers in order until one succeeds.

```erlang
embedder => [
    {openai, #{api_key => <<"sk-...">>}},  %% Try OpenAI first
    {ollama, #{url => <<"http://localhost:11434">>}},
    {local, #{}}  %% Fallback to CPU
]
```

## Advanced Embedding Types

### SPLADE (Sparse Embeddings)

Neural sparse embeddings with term expansion. Produces sparse vectors for hybrid search.

```erlang
%% Initialize SPLADE provider
{ok, State} = barrel_embed:init(#{
    embedder => {splade, #{
        model => "prithivida/Splade_PP_en_v1"
    }}
}).

%% Get sparse vectors directly
{ok, SparseVec} = barrel_embed_splade:embed_sparse(<<"query text">>, Config).
%% => #{indices => [1, 42, 156], values => [0.5, 0.3, 0.8]}
```

**Setup:**

```bash
pip install transformers torch
```

### ColBERT (Late Interaction)

Multi-vector embeddings for fine-grained token-level matching.

```erlang
%% Initialize ColBERT provider
{ok, State} = barrel_embed:init(#{
    embedder => {colbert, #{
        model => "colbert-ir/colbertv2.0"
    }}
}).

%% Get multi-vector embeddings
{ok, MultiVec} = barrel_embed_colbert:embed_multi(<<"query text">>, Config).
%% => [[0.1, 0.2, ...], [0.3, 0.4, ...], ...]  %% One vector per token

%% MaxSim scoring between query and document
Score = barrel_embed_colbert:maxsim_score(QueryVecs, DocVecs).
```

**Setup:**

```bash
pip install transformers torch
```

### CLIP (Image Embeddings)

Cross-modal embeddings for image-text search. Images and text share the same vector space.

```erlang
%% Initialize CLIP provider
{ok, State} = barrel_embed:init(#{
    embedder => {clip, #{
        model => "openai/clip-vit-base-patch32"
    }}
}).

%% Embed text (for cross-modal search)
{ok, TextVec} = barrel_embed_clip:embed(<<"a photo of a cat">>, Config).

%% Embed image (base64 encoded)
{ok, ImageVec} = barrel_embed_clip:embed_image(Base64Image, Config).

%% TextVec and ImageVec are in the same space - compare with cosine similarity!
```

**Setup:**

```bash
pip install transformers torch pillow
```

**Supported Models:**

| Model | Dimensions | Notes |
|-------|------------|-------|
| `openai/clip-vit-base-patch32` | 512 | Default, fast |
| `openai/clip-vit-base-patch16` | 512 | Higher quality |
| `openai/clip-vit-large-patch14` | 768 | Best quality |

## Reranking

> **Requires**: `barrel_rerank` dependency

Cross-encoder reranking for improved search relevance. Use after initial vector search.

```erlang
%% Add barrel_rerank to your deps
{deps, [
    {barrel_vectordb, "2.1.0"},
    {barrel_embed, "2.3.0"},
    {barrel_rerank, "1.0.0"}
]}.
```

```erlang
%% Start the reranker
{ok, Reranker} = barrel_rerank:start_link(#{
    model => "cross-encoder/ms-marco-MiniLM-L-6-v2"
}).

%% Two-stage retrieval
%% Stage 1: Fast vector search (top 100)
{ok, Candidates} = barrel_vectordb:search(Store, Query, #{k => 100}).

%% Stage 2: Rerank candidates
Docs = [maps:get(text, C) || C <- Candidates],
{ok, Ranked} = barrel_rerank:rerank(Reranker, Query, Docs).
%% => [{0, 0.95}, {2, 0.82}, {1, 0.45}, ...]  %% {Index, Score}

%% Get top 10 after reranking
Top10 = [lists:nth(Idx + 1, Candidates) || {Idx, _} <- lists:sublist(Ranked, 10)].

%% Cleanup
ok = barrel_rerank:stop(Reranker).
```

**Setup:**

The venv with dependencies is auto-created on first use, or manually:

```erlang
{ok, _} = barrel_rerank_venv:ensure_venv().
```

**Supported Models:**

| Model | Notes |
|-------|-------|
| `cross-encoder/ms-marco-MiniLM-L-6-v2` | Default, fast |
| `cross-encoder/ms-marco-MiniLM-L-12-v2` | Better quality |
| `BAAI/bge-reranker-base` | Good quality |
| `BAAI/bge-reranker-large` | Best quality |

## BM25 Sparse Retrieval

Pure Erlang BM25 implementation for lexical search. In-memory index.

```erlang
%% Create index
Index = barrel_vectordb_bm25:new().

%% Add documents
Index1 = barrel_vectordb_bm25:add(Index, <<"doc1">>, <<"The quick brown fox">>).
Index2 = barrel_vectordb_bm25:add(Index1, <<"doc2">>, <<"The lazy dog">>).

%% Search
Results = barrel_vectordb_bm25:search(Index2, <<"quick fox">>, 10).
%% => [{<<"doc1">>, 2.45}, ...]

%% Get sparse vector for a document
{ok, SparseVec} = barrel_vectordb_bm25:get_vector(Index2, <<"doc1">>).
%% => #{indices => [hash1, hash2, ...], values => [1.2, 0.8, ...]}

%% Index stats
Stats = barrel_vectordb_bm25:stats(Index2).
%% => #{doc_count => 2, avg_doc_len => 3.5, ...}
```

**Note:** BM25 index is in-memory and not persisted. Rebuild from documents on startup.

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

## Testing

### Unit Tests

Unit tests use mocking and don't require external dependencies:

```bash
rebar3 eunit
```

### With Optional Dependencies

To run tests that exercise barrel_embed:

```bash
rebar3 as test_embed eunit
```

To run tests with all optional dependencies:

```bash
rebar3 as test_full eunit
```

## Performance

### Search Latency

| Metric | Typical Value |
|--------|---------------|
| P50 | ~1ms |
| P99 | ~5ms |

### Optimizations

- **Batch writes**: Concurrent writes are automatically batched via gen_batch_server
- **Batch lookups**: Search uses `rocksdb:multi_get` for efficient result fetching
- **Skip options**: Use `include_text => false` to skip unnecessary RocksDB reads
- **HNSW optimization**: O(log N) candidate management with balanced trees

### Benchmarking

Run the benchmark suite:

```bash
rebar3 as bench compile && rebar3 as bench eunit --module=barrel_vectordb_bench
```

### Backend Comparison Benchmarks

Compare HNSW vs FAISS performance:

```bash
# Quick comparison
./scripts/run_backend_bench.sh --quick

# Default comparison
./scripts/run_backend_bench.sh

# Full benchmark suite
./scripts/run_backend_bench.sh --full
```

Or programmatically:

```erlang
rebar3 as bench_faiss shell
barrel_vectordb_backend_bench:run_all().
```

## Architecture

- **Storage**: RocksDB with column families
- **Index**: HNSW for approximate nearest neighbor search
- **Vectors**: 8-bit quantization with norm caching
- **Embeddings**: Pluggable providers with fallback
- **Batching**: gen_batch_server for automatic write coalescing

See the API documentation for detailed architecture information.

## Support

| Channel | For |
|---------|-----|
| [GitHub Issues](https://github.com/barrel-db/barrel/issues) | Bug reports, feature requests |
| [Email](mailto:support@barrel-db.eu) | Commercial inquiries |

## License

Apache-2.0. See [LICENSE](LICENSE) for details.

---

Built by [Enki Multimedia](https://enki-multimedia.eu) | [barrel-db.eu](https://barrel-db.eu)
