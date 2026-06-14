# Getting Started

## Installation

Add barrel_embed to your `rebar.config`:

```erlang
{deps, [
    {barrel_embed, {git, "https://github.com/barrel-db/barrel_embed.git", {tag, "v2.2.0"}}}
]}.
```

Then fetch dependencies:

```bash
rebar3 get-deps
```

## Choose Your Provider

### Option 1: Ollama (Recommended for Local)

Ollama is the easiest way to run embeddings locally without Python dependencies.

```bash
# Install Ollama
brew install ollama  # macOS
# or see https://ollama.ai for other platforms

# Start server and pull model
ollama serve &
ollama pull nomic-embed-text
```

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {ollama, #{
        url => <<"http://localhost:11434">>,
        model => <<"nomic-embed-text">>
    }}
}).
```

### Option 2: OpenAI (Production)

Best quality embeddings with minimal setup.

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {openai, #{
        api_key => <<"sk-...">>,  % or set OPENAI_API_KEY env var
        model => <<"text-embedding-3-small">>
    }}
}).
```

### Option 3: FastEmbed (Lightweight Local)

ONNX-based embeddings without PyTorch (~100MB vs ~2GB).

```erlang
%% Venv created automatically, deps installed on first use
{ok, State} = barrel_embed:init(#{
    embedder => {fastembed, #{
        model => "BAAI/bge-small-en-v1.5"
    }}
}).
```

### Option 4: Local Python (sentence-transformers)

Full control with sentence-transformers (requires ~2GB for PyTorch).

```erlang
%% Venv created automatically, deps installed on first use
{ok, State} = barrel_embed:init(#{
    embedder => {local, #{
        model => "BAAI/bge-base-en-v1.5"
    }}
}).
```

## Basic Usage

### Single Text

```erlang
{ok, Vector} = barrel_embed:embed(<<"The quick brown fox">>, State).
%% Vector is a list of floats, e.g., [0.123, -0.456, ...]
```

### Batch Embedding

```erlang
Texts = [<<"Document 1">>, <<"Document 2">>, <<"Document 3">>],
{ok, Vectors} = barrel_embed:embed_batch(Texts, State).
%% Vectors is a list of embedding vectors
```

### Check Configuration

```erlang
%% Get embedding dimension
Dim = barrel_embed:dimension(State).
%% => 768

%% Get provider info
Info = barrel_embed:info(State).
%% => #{configured => true, providers => [...], dimension => 768}
```

## Provider Chain (Fallback)

Configure multiple providers for high availability:

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => [
        {ollama, #{url => <<"http://localhost:11434">>}},
        {openai, #{api_key => <<"sk-...">>}},
        {local, #{}}  % fallback to CPU
    ]
}).
```

If the first provider fails, barrel_embed automatically tries the next one.

## Cosine Similarity

Calculate similarity between embeddings:

```erlang
cosine_similarity(V1, V2) ->
    Dot = lists:sum(lists:zipwith(fun(A, B) -> A * B end, V1, V2)),
    Norm1 = math:sqrt(lists:sum([X * X || X <- V1])),
    Norm2 = math:sqrt(lists:sum([X * X || X <- V2])),
    Dot / (Norm1 * Norm2).
```

## Next Steps

- Explore [provider-specific features](providers/index.md)
- See the complete [API reference](api.md)
