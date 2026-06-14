# barrel_embed

Lightweight embedding generation for Erlang.

A standalone library for generating text and image embeddings with multiple provider backends and automatic fallback support.

## Features

- **Multiple Providers** - Ollama, OpenAI, local Python, FastEmbed, and more
- **Automatic Fallback** - Provider chain with seamless failover
- **Batch Processing** - Efficient batch embedding with configurable chunk size
- **Specialized Embeddings** - Sparse (SPLADE), multi-vector (ColBERT), cross-modal (CLIP)
- **Resource Management** - Python execution rate limiting

## Quick Example

```erlang
%% Initialize with Ollama
{ok, State} = barrel_embed:init(#{
    embedder => {ollama, #{
        url => <<"http://localhost:11434">>,
        model => <<"nomic-embed-text">>
    }}
}).

%% Generate embedding
{ok, Vector} = barrel_embed:embed(<<"Hello world">>, State).

%% Batch embedding
{ok, Vectors} = barrel_embed:embed_batch([<<"text1">>, <<"text2">>], State).
```

## Provider Overview

### Cloud Providers

| Provider | Dimensions | Best For |
|----------|------------|----------|
| [OpenAI](providers/openai.md) | 256-3072 | Production, general purpose |
| [Cohere](providers/cohere.md) | 384-1024 | Input type optimization |
| [Voyage AI](providers/voyage.md) | 512-1536 | RAG, domain-specific (code, law, finance) |
| [Jina AI](providers/jina.md) | 768-1024 | Long context (8K), free tier |
| [Mistral](providers/mistral.md) | 1024 | EU data residency |
| [Azure OpenAI](providers/azure.md) | 1536-3072 | Enterprise, Azure ecosystem |
| [AWS Bedrock](providers/bedrock.md) | 1024-1536 | Enterprise, AWS ecosystem |
| [Google Vertex AI](providers/vertex.md) | 768 | Enterprise, GCP ecosystem |

### Local Providers

| Provider | Type | Best For |
|----------|------|----------|
| [Ollama](providers/ollama.md) | Dense | Local deployment, no Python needed |
| [Local](providers/local.md) | Dense | Offline, full control |
| [FastEmbed](providers/fastembed.md) | Dense | Lightweight local inference |

### Specialized Providers

| Provider | Type | Best For |
|----------|------|----------|
| [SPLADE](providers/splade.md) | Sparse | Hybrid search, keyword expansion |
| [ColBERT](providers/colbert.md) | Multi-vector | Fine-grained semantic matching |
| [CLIP](providers/clip.md) | Cross-modal | Image-text search |

## Installation

Add to your `rebar.config`:

```erlang
{deps, [
    {barrel_embed, {git, "https://github.com/barrel-db/barrel_embed.git", {tag, "v1.0.0"}}}
]}.
```

## Next Steps

- [Getting Started](getting-started.md) - Installation and first steps
- [Providers](providers/index.md) - Detailed provider documentation
- [API Reference](api.md) - Complete API documentation
