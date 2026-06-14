# FastEmbed Provider

Lightweight local embedding using FastEmbed (ONNX-based).

## Requirements

```bash
# Using virtualenv with uv (recommended)
./scripts/setup_venv.sh
uv pip install fastembed --python .venv/bin/python

# Or install manually
pip install fastembed
```

## Why FastEmbed?

FastEmbed is a lighter alternative to sentence-transformers:

| | FastEmbed | sentence-transformers |
|---|-----------|----------------------|
| Install size | ~100MB | ~2GB+ |
| Dependencies | ONNX Runtime | PyTorch |
| Performance | Optimized inference | Standard |
| Quality | Similar | Similar |

## Configuration

```erlang
%% Using virtualenv (recommended)
{ok, State} = barrel_embed:init(#{
    embedder => {fastembed, #{
        venv => "/absolute/path/to/.venv",
        model => "BAAI/bge-small-en-v1.5",     % default
        timeout => 120000                       % default, ms
    }}
}).

%% Using system Python
{ok, State} = barrel_embed:init(#{
    embedder => {fastembed, #{
        python => "python3",                    % default
        model => "BAAI/bge-small-en-v1.5",     % default
        timeout => 120000                       % default, ms
    }}
}).
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `venv` | string | `undefined` | Path to virtualenv (recommended) |
| `python` | string | `"python3"` | Python executable (if no venv) |
| `model` | string | `"BAAI/bge-small-en-v1.5"` | Model name |
| `timeout` | integer | `120000` | Timeout in milliseconds |

## Supported Models

| Model | Dimensions | Notes |
|-------|-----------|-------|
| `BAAI/bge-small-en-v1.5` | 384 | Default, fast |
| `BAAI/bge-base-en-v1.5` | 768 | Good balance |
| `BAAI/bge-large-en-v1.5` | 1024 | Highest quality |
| `sentence-transformers/all-MiniLM-L6-v2` | 384 | Popular lightweight |
| `nomic-ai/nomic-embed-text-v1.5` | 768 | Good general-purpose |

## Example

```erlang
%% Initialize
{ok, State} = barrel_embed:init(#{
    embedder => {fastembed, #{
        model => "BAAI/bge-small-en-v1.5"
    }}
}).

%% Generate embedding
{ok, Vec} = barrel_embed:embed(<<"Fast and lightweight">>, State).
384 = length(Vec).

%% Batch embedding
{ok, Vecs} = barrel_embed:embed_batch([
    <<"Document 1">>,
    <<"Document 2">>
], State).
```

## When to Use FastEmbed

Choose FastEmbed when:

- Disk space is limited
- PyTorch is not desired
- You need lightweight local inference
- Quality requirements are standard (not maximum)

Choose sentence-transformers (local provider) when:

- You need access to all HuggingFace models
- You're already using PyTorch
- You need maximum compatibility
