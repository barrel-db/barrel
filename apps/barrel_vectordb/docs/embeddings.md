# Embedding Models

Barrel VectorDB supports multiple embedding providers through the `barrel_embed` library (included as a dependency).

The embedder is **explicit** - if not configured, only `add_vector/5` and `search_vector/3` work. Text-based operations return `{error, embedder_not_configured}`.

## Providers

### Local (sentence-transformers)

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

Then configure the store:

```erlang
{ok, _} = barrel_vectordb:start_link(#{
    name => my_store,
    path => "/tmp/vectors",
    embedder => {local, #{
        python => "/home/user/.venv/barrel_embed/bin/python"
    }}
}).
```

**Supported Models:**

| Model | Dimensions | Notes |
|-------|------------|-------|
| `BAAI/bge-base-en-v1.5` | 768 | Default, good quality/speed |
| `BAAI/bge-small-en-v1.5` | 384 | Faster, smaller |
| `BAAI/bge-large-en-v1.5` | 1024 | Best quality, slower |
| `sentence-transformers/all-MiniLM-L6-v2` | 384 | Fast, general purpose |
| `sentence-transformers/all-mpnet-base-v2` | 768 | High quality |
| `nomic-ai/nomic-embed-text-v1.5` | 768 | Long context (8192 tokens) |

### Ollama

Local Ollama server. Requires [Ollama](https://ollama.ai) to be running.

```erlang
embedder => {ollama, #{
    url => <<"http://localhost:11434">>,   %% Ollama API URL (default)
    model => <<"nomic-embed-text">>,       %% Model name (default, 768 dims)
    timeout => 30000                       %% Timeout in ms (default)
}}
```

**Setup:**

```bash
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
    python => "python3",
    model => "BAAI/bge-small-en-v1.5",
    timeout => 120000
}}
```

**Setup:**

```bash
pip install fastembed
```

### Provider Chain

Try providers in order until one succeeds:

```erlang
embedder => [
    {openai, #{api_key => <<"sk-...">>}},
    {ollama, #{url => <<"http://localhost:11434">>}},
    {local, #{}}
]
```

## Advanced Embeddings

### SPLADE (Sparse Embeddings)

Neural sparse embeddings with term expansion for hybrid search.

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {splade, #{
        model => "prithivida/Splade_PP_en_v1"
    }}
}).

{ok, SparseVec} = barrel_vectordb_embed_splade:embed_sparse(<<"query text">>, Config).
%% => #{indices => [1, 42, 156], values => [0.5, 0.3, 0.8]}
```

**Setup:**

```bash
pip install transformers torch
```

### ColBERT (Late Interaction)

Multi-vector embeddings for fine-grained token-level matching.

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {colbert, #{
        model => "colbert-ir/colbertv2.0"
    }}
}).

{ok, MultiVec} = barrel_vectordb_embed_colbert:embed_multi(<<"query text">>, Config).
%% => [[0.1, 0.2, ...], [0.3, 0.4, ...], ...]

Score = barrel_vectordb_embed_colbert:maxsim_score(QueryVecs, DocVecs).
```

### CLIP (Image Embeddings)

Cross-modal embeddings for image-text search.

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {clip, #{
        model => "openai/clip-vit-base-patch32"
    }}
}).

{ok, TextVec} = barrel_vectordb_embed_clip:embed(<<"a photo of a cat">>, Config).
{ok, ImageVec} = barrel_vectordb_embed_clip:embed_image(Base64Image, Config).
```

**Setup:**

```bash
pip install transformers torch pillow
```

| Model | Dimensions | Notes |
|-------|------------|-------|
| `openai/clip-vit-base-patch32` | 512 | Default, fast |
| `openai/clip-vit-base-patch16` | 512 | Higher quality |
| `openai/clip-vit-large-patch14` | 768 | Best quality |

## Reranking

Cross-encoder reranking for improved search relevance using the optional `barrel_rerank` package.

### Installation

Add `barrel_rerank` to your dependencies:

```erlang
%% rebar.config
{deps, [
    {barrel_vectordb, "1.4.0"},
    {barrel_rerank, "0.1.1"}
]}.
```

### Usage

```erlang
%% Start the reranker
{ok, Reranker} = barrel_rerank:start_link(#{
    model => "cross-encoder/ms-marco-MiniLM-L-6-v2"
}).

%% Two-stage retrieval
{ok, Candidates} = barrel_vectordb:search(Store, Query, #{k => 100}).

Docs = [maps:get(text, C) || C <- Candidates],
{ok, Ranked} = barrel_rerank:rerank(Reranker, Query, Docs).
%% => [{0, 0.95}, {2, 0.82}, {1, 0.45}, ...]

%% Stop when done
ok = barrel_rerank:stop(Reranker).
```

**Supported Reranker Models:**

| Model | Notes |
|-------|-------|
| `cross-encoder/ms-marco-MiniLM-L-6-v2` | Default, fast |
| `cross-encoder/ms-marco-MiniLM-L-12-v2` | Better quality |
| `BAAI/bge-reranker-base` | Good quality |
| `BAAI/bge-reranker-large` | Best quality |

## BM25 Sparse Retrieval

Pure Erlang BM25 implementation for lexical search:

```erlang
Index = barrel_vectordb_bm25:new().
Index1 = barrel_vectordb_bm25:add(Index, <<"doc1">>, <<"The quick brown fox">>).
Index2 = barrel_vectordb_bm25:add(Index1, <<"doc2">>, <<"The lazy dog">>).

{ok, Results} = barrel_vectordb_bm25:search(Index2, <<"quick fox">>, 10).
%% => [{<<"doc1">>, 2.45}, ...]
```

!!! note
    BM25 index is in-memory and not persisted. Rebuild from documents on startup.
