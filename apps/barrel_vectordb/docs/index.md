# Barrel VectorDB

**Embeddable vector database for Erlang with HNSW indexing and semantic search.**

Build AI-powered search into your Erlang applications. Barrel VectorDB provides a production-ready vector store with pluggable embedding providers, automatic batching, and sub-millisecond search latency.

<div class="grid cards" markdown>

-   :material-rocket-launch: __Quick Start__

    ---

    Get up and running in 5 minutes with vector search

    [:octicons-arrow-right-24: Quick Start](getting-started.md)

-   :material-brain: __Embedding Models__

    ---

    Local, Ollama, OpenAI, and more - choose your embedder

    [:octicons-arrow-right-24: Embeddings](embeddings.md)

-   :material-text-search: __BM25 Text Search__

    ---

    Lexical and hybrid search alongside vectors

    [:octicons-arrow-right-24: BM25](bm25-disk.md)

-   :material-api: __API Reference__

    ---

    Embedded Erlang API for full control

    [:octicons-arrow-right-24: Erlang API](api/erlang.md)

</div>

## What is Barrel VectorDB?

Barrel VectorDB is an embeddable vector database designed for Erlang/OTP applications:

- **HNSW Indexing**: Pure Erlang HNSW with O(log N) search, or optional FAISS backend for high throughput
- **Pluggable Embeddings**: Local (sentence-transformers), Ollama, OpenAI, FastEmbed, with fallback chains
- **Sub-millisecond Search**: P50 ~1ms, P99 ~5ms typical latency
- **Production Ready**: Built on RocksDB for persistence, gen_batch_server for write coalescing

## Quick Example

```erlang
%% Start a store with local embeddings
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

## Core Features

### Vector Indexing

| Feature | HNSW (Default) | FAISS (Optional) |
|---------|---------------|------------------|
| Dependencies | None | barrel_faiss NIF |
| Insert speed | Baseline | 1.6-3x faster |
| Search speed | Baseline | 2x faster |
| Delete speed | Fast (native) | Slower (soft delete) |
| Memory | Higher | Lower |

### Embedding Providers

| Provider | Description |
|----------|-------------|
| `local` | Python + sentence-transformers (CPU) |
| `ollama` | Local Ollama server |
| `openai` | OpenAI Embeddings API |
| `fastembed` | ONNX-based embeddings |
| Provider chain | Try providers in order |

### Advanced Features

- **SPLADE**: Neural sparse embeddings for hybrid search
- **ColBERT**: Multi-vector late interaction
- **CLIP**: Cross-modal image-text search
- **Reranking**: Cross-encoder for improved relevance
- **BM25**: Pure Erlang lexical search

## Get Started

<div class="grid cards" markdown>

-   :material-clock-fast: __5 minutes__

    ---

    Install and start searching

    ```erlang
    {deps, [
        {barrel_vectordb, {git,
          "https://github.com/barrel-db/barrel_vectordb.git",
          {branch, "main"}}}
    ]}.
    ```

-   :material-code-tags: __Vector-only mode__

    ---

    Bring your own embeddings

    ```erlang
    {ok, _} = barrel_vectordb:start_link(#{
        name => store,
        path => "/tmp/vectors",
        dimensions => 768
    }).

    barrel_vectordb:add_vector(store,
        <<"id">>, <<"text">>, #{}, Vector).
    ```

</div>

## Community & Support

- [:fontawesome-brands-github: GitHub Repository](https://github.com/barrel-db/barrel_vectordb)
- [:material-file-document: Erlang API](api/erlang.md)
- [:material-bug: Report Issues](https://github.com/barrel-db/barrel_vectordb/issues)
