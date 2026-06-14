# Python Virtualenv Setup

barrel_embed automatically manages a Python virtualenv with required dependencies.

## Automatic Setup

barrel_embed automatically:

1. Creates a venv at `priv/.venv` on application startup
2. Installs uvloop for async performance (required on Unix)
3. Installs provider-specific dependencies when providers are initialized

**No manual setup required.**

```erlang
%% Just use it - venv is created and configured automatically
{ok, State} = barrel_embed:init(#{
    embedder => {fastembed, #{}}
}).
```

## Venv Management API

```erlang
%% Get venv path
Path = barrel_embed:venv_path().

%% Check if uvloop is installed
barrel_embed:has_uvloop().
%% => true

%% Manually install provider deps
barrel_embed:install_provider(fastembed).

%% Recreate venv from scratch
barrel_embed:refresh_venv().
```

## Custom Python Executable

By default, barrel_embed uses `python3`. To use a different Python:

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {fastembed, #{
        python => "/usr/local/bin/python3.11"
    }}
}).
```

This is useful when you have multiple Python versions installed or need a specific interpreter.

## Custom Venv Location

Set a custom venv path via application config:

```erlang
%% In sys.config
{barrel_embed, [
    {venv_dir, "/opt/barrel_embed/venv"}
]}.
```

## Provider Dependencies

| Provider | Packages | Size |
|----------|----------|------|
| `fastembed` | fastembed | ~100MB |
| `local` | sentence-transformers | ~2GB |
| `splade` | transformers, torch | ~2GB |
| `colbert` | transformers, torch | ~2GB |
| `clip` | transformers, torch, pillow | ~2GB |

Dependencies are installed on-demand when a provider is first initialized.

## uvloop

uvloop is required on Unix systems and installed automatically. It provides significant performance improvements for the async embedding server.

Check uvloop status:
```erlang
barrel_embed:has_uvloop().
%% => true (Unix) or false (Windows)
```

## Troubleshooting

### Venv Creation Failed

Check:
1. Python 3 is installed: `python3 --version`
2. venv module available: `python3 -m venv --help`
3. Write permissions to priv directory

### uvloop Installation Failed

uvloop requires a C compiler. On Debian/Ubuntu:
```bash
apt-get install build-essential python3-dev
```

On macOS:
```bash
xcode-select --install
```
