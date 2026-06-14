<div align="center">

# barrel_embed

**Lightweight embedding generation for Erlang with 15+ provider backends**

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Version](https://img.shields.io/badge/Version-2.0.1-green.svg)]()

[Documentation](https://docs.barrel-db.eu/embed) |
[Examples](./examples) |
[barrel-db.eu](https://barrel-db.eu)

</div>

---

A standalone library for generating text and image embeddings with multiple provider backends and automatic fallback support.

## Features

- Multiple embedding providers with automatic fallback
- Provider chain configuration for high availability
- Batch embedding with configurable chunk size
- Python integration via async port server (no NIF dependency)
- Sparse, multi-vector, and cross-modal embeddings

## Providers

| Provider | Type | Requirements | Description |
|----------|------|--------------|-------------|
| `local` | Dense | Python + sentence-transformers | Local CPU inference |
| `ollama` | Dense | Ollama server | Local Ollama API |
| `openai` | Dense | API key | OpenAI Embeddings API |
| `fastembed` | Dense | Python + fastembed | ONNX-based, lighter than sentence-transformers |
| `splade` | Sparse | Python + transformers + torch | Neural sparse embeddings for hybrid search |
| `colbert` | Multi-vector | Python + transformers + torch | Token-level embeddings for fine-grained matching |
| `clip` | Cross-modal | Python + transformers + torch + pillow | Image/text embeddings in same space |

## Installation

Add to your `rebar.config`:

```erlang
{deps, [
    {barrel_embed, "2.0.1"}
]}.
```

**Note**: Local Python providers require Python 3.9+ installed on your system.

## Quick Start

```erlang
%% Initialize with a single provider
{ok, State} = barrel_embed:init(#{
    embedder => {ollama, #{
        url => <<"http://localhost:11434">>,
        model => <<"nomic-embed-text">>
    }}
}).

%% Generate embedding
{ok, Vector} = barrel_embed:embed(<<"Hello world">>, State).

%% Batch embedding
{ok, Vectors} = barrel_embed:embed_batch([<<"text1">>, <<" text2">>], State).
```

## Configuration

### Single Provider

```erlang
%% Ollama (recommended for local deployment)
#{embedder => {ollama, #{
    url => <<"http://localhost:11434">>,
    model => <<"nomic-embed-text">>
}}}

%% Local Python (sentence-transformers)
#{embedder => {local, #{
    python => "python3",
    model => "BAAI/bge-base-en-v1.5"
}}}

%% OpenAI
#{embedder => {openai, #{
    api_key => <<"sk-...">>,  %% or set OPENAI_API_KEY env var
    model => <<"text-embedding-3-small">>
}}}

%% FastEmbed (ONNX, lighter)
#{embedder => {fastembed, #{
    model => "BAAI/bge-small-en-v1.5"
}}}
```

### Provider Chain (Fallback)

```erlang
#{embedder => [
    {ollama, #{url => <<"http://localhost:11434">>}},
    {openai, #{api_key => <<"sk-...">>}},
    {local, #{}}  %% fallback to CPU
]}
```

### Custom Dimensions and Batch Size

```erlang
#{
    embedder => {local, #{}},
    dimensions => 768,
    batch_size => 64
}
```

## Ollama Example

First, install Ollama and pull an embedding model:

```bash
# Install Ollama (macOS)
brew install ollama

# Start Ollama server
ollama serve

# Pull embedding model
ollama pull nomic-embed-text
```

Then use in Erlang:

```erlang
%% Start the application
application:ensure_all_started(barrel_embed).

%% Initialize with Ollama
{ok, State} = barrel_embed:init(#{
    embedder => {ollama, #{
        url => <<"http://localhost:11434">>,
        model => <<"nomic-embed-text">>
    }},
    dimensions => 768
}).

%% Generate embeddings
{ok, Vec1} = barrel_embed:embed(<<"The quick brown fox">>, State).
{ok, Vec2} = barrel_embed:embed(<<"A fast auburn canine">>, State).

%% Calculate cosine similarity
Dot = lists:sum(lists:zipwith(fun(A, B) -> A * B end, Vec1, Vec2)).
Norm1 = math:sqrt(lists:sum([X * X || X <- Vec1])).
Norm2 = math:sqrt(lists:sum([X * X || X <- Vec2])).
Similarity = Dot / (Norm1 * Norm2).
%% => ~0.85 (semantically similar)
```

## Specialized APIs

### SPLADE Sparse Embeddings

```erlang
{ok, State} = barrel_embed:init(#{embedder => {splade, #{}}}).

%% Get sparse vector (indices + values)
{ok, #{indices := Indices, values := Values}} =
    barrel_embed_splade:embed_sparse(<<"query text">>, Config).
```

### ColBERT Multi-Vector Embeddings

```erlang
{ok, State} = barrel_embed:init(#{embedder => {colbert, #{}}}).

%% Get token-level vectors
{ok, TokenVectors} = barrel_embed_colbert:embed_multi(<<"document">>, Config).

%% Calculate MaxSim score
Score = barrel_embed_colbert:maxsim_score(QueryVecs, DocVecs).
```

### CLIP Image Embeddings

```erlang
{ok, State} = barrel_embed:init(#{embedder => {clip, #{}}}).

%% Embed image (base64-encoded)
{ok, ImageVec} = barrel_embed_clip:embed_image(ImageBase64, Config).

%% Embed text (same vector space as images)
{ok, TextVec} = barrel_embed_clip:embed(<<"a photo of a cat">>, Config).

%% Now ImageVec and TextVec can be compared with cosine similarity
```

## API Reference

### barrel_embed

| Function | Description |
|----------|-------------|
| `init(Config)` | Initialize embedding state |
| `embed(Text, State)` | Generate embedding for single text |
| `embed_batch(Texts, State)` | Generate embeddings for multiple texts |
| `embed_batch(Texts, Opts, State)` | Batch embed with options |
| `dimension(State)` | Get embedding dimension |
| `info(State)` | Get provider information |

## Application Configuration

Configure the managed venv path in `sys.config`:

```erlang
%% sys.config
[
    {barrel_embed, [
        {managed_venv_path, "/path/to/.venv"}  %% Optional: custom venv path
    ]}
].
```

## Python Setup

### Using Virtual Environment (Recommended)

```bash
# Quick setup with uv (fast)
./scripts/setup_venv.sh

# Or manually
uv venv .venv
uv pip install -r priv/requirements.txt --python .venv/bin/python
```

Then use the venv in your Erlang code:

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {local, #{
        venv => "/absolute/path/to/.venv"
    }}
}).
```

### Manual Installation

Install based on providers used:

```bash
# For local provider
pip install sentence-transformers

# For fastembed provider
pip install fastembed

# For splade/colbert providers
pip install transformers torch

# For clip provider
pip install transformers torch pillow
```

See [venv-setup](https://docs.barrel-db.eu/embed/venv-setup/) for detailed virtualenv instructions.

## Support

| Channel | For |
|---------|-----|
| [GitHub Issues](https://github.com/barrel-db/barrel_embed/issues) | Bug reports, feature requests |
| [Email](mailto:support@barrel-db.eu) | Commercial inquiries |

## License

Apache-2.0. See [LICENSE](LICENSE) for details.

---

Built by [Enki Multimedia](https://enki-multimedia.eu) | [barrel-db.eu](https://barrel-db.eu)
