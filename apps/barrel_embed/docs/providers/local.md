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
{ok, State} = barrel_embed:init(#{
    embedder => {local, #{
        python => "python3",                    % default, fallback only
        model => "BAAI/bge-base-en-v1.5",      % default
        timeout => 120000                       % default, ms
    }}
}).
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `python` | string | `"python3"` | Python executable, used only if the managed venv could not be created |
| `model` | string | `"BAAI/bge-base-en-v1.5"` | Model name |
| `timeout` | integer | `120000` | Timeout in milliseconds |

barrel_embed manages its own Python virtualenv automatically and installs
`sentence-transformers` into it on first use. See
[Python Virtualenv Setup](venv-setup.md) for the managed venv API and how to
change its location.

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

### Managed Virtualenv (Default)

barrel_embed creates a venv under its `priv` directory on application
startup and installs `sentence-transformers` there automatically:

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {local, #{
        model => "BAAI/bge-base-en-v1.5"
    }}
}).
```

Set a custom venv location via the `venv_dir` application env (see
[Python Virtualenv Setup](venv-setup.md)).

### Using a Different Python Interpreter

If the managed venv could not be created, barrel_embed falls back to the
`python` option. This is also useful for Conda environments:

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {local, #{
        python => "/path/to/conda/envs/myenv/bin/python"
    }}
}).
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
