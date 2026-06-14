# Ollama Provider

Local embedding generation using Ollama server.

## Requirements

- [Ollama](https://ollama.ai) installed and running

## Setup

```bash
# Install Ollama
brew install ollama  # macOS
# or download from https://ollama.ai

# Start server
ollama serve

# Pull an embedding model
ollama pull nomic-embed-text
```

## Configuration

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {ollama, #{
        url => <<"http://localhost:11434">>,  % default
        model => <<"nomic-embed-text">>       % required
    }}
}).
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `url` | binary | `<<"http://localhost:11434">>` | Ollama server URL |
| `model` | binary | required | Model name |

## Supported Models

Popular embedding models available in Ollama:

| Model | Dimensions | Notes |
|-------|-----------|-------|
| `nomic-embed-text` | 768 | Good general-purpose |
| `mxbai-embed-large` | 1024 | High quality |
| `all-minilm` | 384 | Lightweight |
| `snowflake-arctic-embed` | 1024 | Multilingual |

List available models:

```bash
ollama list
```

## Example

```erlang
%% Start application
application:ensure_all_started(barrel_embed).

%% Initialize
{ok, State} = barrel_embed:init(#{
    embedder => {ollama, #{
        url => <<"http://localhost:11434">>,
        model => <<"nomic-embed-text">>
    }}
}).

%% Single embedding
{ok, Vec} = barrel_embed:embed(<<"Hello world">>, State).
768 = length(Vec).

%% Batch embedding
{ok, Vecs} = barrel_embed:embed_batch([
    <<"First document">>,
    <<"Second document">>,
    <<"Third document">>
], State).
3 = length(Vecs).
```

## API Compatibility

The Ollama provider supports both API versions:

- `/api/embed` (newer, preferred)
- `/api/embeddings` (legacy fallback)

It automatically detects and uses the appropriate endpoint.

## Troubleshooting

### Connection refused

Ensure Ollama is running:

```bash
ollama serve
```

### Model not found

Pull the model first:

```bash
ollama pull nomic-embed-text
```

### Slow first request

The first request may be slow as Ollama loads the model into memory. Subsequent requests will be fast.
