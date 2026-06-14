# Local Provider

Local embedding generation using Python and sentence-transformers.

## Requirements

```bash
# Using virtualenv with uv (recommended)
./scripts/setup_venv.sh

# Or install manually
pip install sentence-transformers
```

## Configuration

```erlang
%% Using virtualenv (recommended)
{ok, State} = barrel_embed:init(#{
    embedder => {local, #{
        venv => "/absolute/path/to/.venv",
        model => "BAAI/bge-base-en-v1.5",      % default
        timeout => 120000                       % default, ms
    }}
}).

%% Using system Python
{ok, State} = barrel_embed:init(#{
    embedder => {local, #{
        python => "python3",                    % default
        model => "BAAI/bge-base-en-v1.5",      % default
        timeout => 120000                       % default, ms
    }}
}).
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `venv` | string | `undefined` | Path to virtualenv (recommended) |
| `python` | string | `"python3"` | Python executable (if no venv) |
| `model` | string | `"BAAI/bge-base-en-v1.5"` | Model name |
| `timeout` | integer | `120000` | Timeout in milliseconds |

When `venv` is specified, the provider properly activates the virtualenv by setting `VIRTUAL_ENV`, `PATH`, and `PYTHONPATH` environment variables.

## Supported Models

Any model from HuggingFace that works with sentence-transformers:

| Model | Dimensions | Notes |
|-------|-----------|-------|
| `BAAI/bge-base-en-v1.5` | 768 | Good general-purpose (default) |
| `BAAI/bge-large-en-v1.5` | 1024 | Higher quality |
| `BAAI/bge-small-en-v1.5` | 384 | Faster, smaller |
| `sentence-transformers/all-MiniLM-L6-v2` | 384 | Lightweight |
| `sentence-transformers/all-mpnet-base-v2` | 768 | High quality |

## Example

```erlang
%% Initialize
{ok, State} = barrel_embed:init(#{
    embedder => {local, #{
        model => "sentence-transformers/all-MiniLM-L6-v2"
    }}
}).

%% Generate embedding
{ok, Vec} = barrel_embed:embed(<<"Hello world">>, State).
384 = length(Vec).

%% Batch embedding
{ok, Vecs} = barrel_embed:embed_batch([
    <<"First text">>,
    <<"Second text">>
], State).
```

## Python Environment

### Using Virtual Environment (Recommended)

The `venv` option properly activates the virtualenv environment:

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {local, #{
        venv => "/absolute/path/to/.venv"
    }}
}).
```

This sets the correct environment variables (`VIRTUAL_ENV`, `PATH`) so Python packages are discovered correctly.

### Quick Setup

```bash
# Using uv (fast)
./scripts/setup_venv.sh

# Or manually with uv
uv venv .venv
uv pip install -r priv/requirements.txt --python .venv/bin/python
```

### Using Conda

For Conda environments, use the `python` option:

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {local, #{
        python => "/path/to/conda/envs/myenv/bin/python"
    }}
}).
```

## Resource Management

The local provider uses a Python subprocess. barrel_embed includes rate limiting to prevent resource exhaustion:

```erlang
%% sys.config
{barrel_embed, [
    {python_max_concurrent, 4}  % default: schedulers * 2 + 1
]}
```

## First Request Latency

The first request may be slow as it:

1. Starts the Python subprocess
2. Loads the model into memory

Subsequent requests reuse the loaded model and are much faster.

## Troubleshooting

### ModuleNotFoundError: sentence_transformers

Install the dependency:

```bash
pip install sentence-transformers
```

### Model download slow

Models are downloaded from HuggingFace on first use. Set `HF_HOME` to control cache location:

```bash
export HF_HOME=/path/to/cache
```
