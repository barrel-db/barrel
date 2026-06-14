# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.2.1] - 2026-04-02

### Changed

- Removed erlang_python dependency from documentation
- Updated README to reflect port-based Python integration
- Regenerated rebar.lock without erlang_python

## [2.2.0] - 2026-04-02

### Added

#### Managed Virtual Environment

- Auto-create Python venv at `priv/.venv` on application startup
- Auto-install provider dependencies when providers are initialized
- uvloop is now required on Unix systems for optimal async performance
- New `barrel_embed_venv` module for venv lifecycle management

#### New API Functions

- `barrel_embed:refresh_venv/0` - Recreate managed venv from scratch
- `barrel_embed:install_provider/1` - Install dependencies for a specific provider
- `barrel_embed:venv_path/0` - Get the managed venv path
- `barrel_embed:has_uvloop/0` - Check if uvloop is installed

#### Configuration

- `{barrel_embed, [{venv_dir, "/custom/path"}]}` - Custom venv location
- Providers auto-use managed venv when no explicit `venv` option is set

### Changed

#### Restored Port-Based Backend

- **Breaking**: Reverted from erlang_python NIF to port-based JSON communication
- Removed erlang_python dependency (no more NIF compilation required)
- Restored `barrel_embed_port_server` for Python process management
- Async Python server with request multiplexing via newline-delimited JSON
- Simpler deployment: no NIF compilation, works on any platform with Python

- Providers (local, fastembed, splade, colbert, clip) now default to managed venv
- uvloop installation failures now cause venv creation to fail (was silent warning)

## [2.1.1] - 2026-03-31

### Added

- Ollama integration tests (`barrel_embed_ollama_tests`)
  - Tests against real Ollama instance when available
  - Covers init, embed, embed_batch, dimension, and availability
  - Error handling tests for unavailable endpoints

## [2.0.1] - 2026-03-07

### Added

- Apache-2.0 LICENSE file
- `examples/basic_usage.erl` with ollama, openai, and local provider examples
- ex_doc configuration in rebar.config for HexDocs generation
- Hex.pm links in app.src (GitHub, Documentation)

### Changed

- README now uses Hex.pm dependency format
- Documentation links point to docs.barrel-db.eu
- Support links changed from GitLab to GitHub Issues

## [2.0.0] - 2026-02-20

### Changed

#### New erlang_python NIF Backend

- **Breaking**: Replaced port-based stdio JSON communication with erlang_python 1.5.0 NIF integration
- All Python providers now use direct `py:call` instead of subprocess communication
- 2.7x improvement in batch throughput (936 vs 348 texts/sec with bge-small-en-v1.5)
- Requires erlang_python 1.5.0+ as a dependency

#### New Modules

- `barrel_embed_py` - Erlang wrapper for py:call with timeout support
- `priv/barrel_embed/nif_api.py` - Thread-safe Python API for model caching

### Removed

- `barrel_embed_port_server` - Port-based Python server (replaced by NIF)
- `barrel_embed_python_queue` - Rate limiting queue (erlang_python handles this)
- `priv/barrel_embed/server.py` - Async stdio server
- `priv/barrel_embed/__main__.py` - CLI entry point
- `priv/barrel_embed/providers/` - Provider classes (now in nif_api.py)

### Migration

If you were using custom provider configurations, the API remains the same.
The `venv` option is still supported and recommended:

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {local, #{
        venv => "/path/to/.venv"
    }}
}).
```

## [1.0.0] - 2026-01-27

### Added

#### Virtual Environment Support

- Added `venv` configuration option for all Python providers (local, fastembed, splade, colbert, clip)
- Proper venv activation in port environment (sets `VIRTUAL_ENV`, `PATH`, `PYTHONPATH`)
- New `scripts/setup_venv.sh` for fast venv setup using `uv`
- Requirements files for different installation profiles:
  - `priv/requirements.txt` - Default (sentence-transformers + uvloop)
  - `priv/requirements-minimal.txt` - Minimal (no ML libs)
  - `priv/requirements-full.txt` - All providers
- Documentation: `docs/venv-setup.md`

#### CI Improvements

- Added `integration:venv` job to test Erlang-Python venv communication
- Python tests now use `uv` for faster dependency installation

### Changed

- `setup_python_venv.sh` now uses `uv` when available (falls back to pip)
- Python queue default limit changed from `schedulers/2 + 1` to `schedulers * 2 + 1`
- Updated all Python provider documentation with venv examples

## [0.2.0] - 2026-01-27

### Added

#### Cloud Providers

- `cohere` - Cohere Embed API with input type optimization
- `voyage` - Voyage AI for RAG and domain-specific embeddings (code, law, finance)
- `jina` - Jina AI with 8K context and free tier
- `mistral` - Mistral AI with EU data residency
- `azure` - Azure OpenAI for enterprise compliance
- `bedrock` - AWS Bedrock (Titan, Cohere models) with IAM and API key auth
- `vertex` - Google Vertex AI for GCP ecosystem

#### Documentation

- Provider comparison guide (`docs/choosing-provider.md`)
- Developer guide for adding cloud providers (`docs/dev/adding-provider.md`)
- Individual documentation pages for all cloud providers

#### Tooling

- `scripts/setup_python_venv.sh` for one-command Python venv setup
  - Added `--dev` option for installing dev dependencies (test + uvloop)
  - Added `--uvloop` option to install uvloop via pyproject.toml extra
  - Added `--test` option to run Python tests after setup
  - Added `-h/--help` option for usage information
- GitLab CI configuration (`.gitlab-ci.yml`)
  - Erlang tests with rebar3 eunit
  - Python tests with pytest
  - Dialyzer type checking
  - Xref cross-reference checks

#### Testing

- Python test suite for uvloop integration (`priv/tests/test_server.py`)
  - uvloop detection and event loop policy tests
  - AsyncEmbedServer dispatch and handler tests
  - Concurrent task execution tests

#### Python Engine

- Async request multiplexing for concurrent embeddings
- Improved error handling and logging

### Changed

- Updated hackney dependency to 2.0.1 for HTTP/2 support
- Provider init now properly loads modules before checking exports
- Removed redundant `application:ensure_all_started(hackney)` from providers (hackney starts via app.src)

## [0.1.0] - 2026-01-14

### Added

- Initial release extracted from barrel_vectordb
- Core embedding coordinator (`barrel_embed`) with provider chain and fallback support
- Provider behaviour (`barrel_embed_provider`) for implementing custom providers
- Python execution rate limiter (`barrel_embed_python_queue`)

#### Providers

- `local` - Local Python with sentence-transformers
- `ollama` - Ollama server API (supports both `/api/embed` and `/api/embeddings`)
- `openai` - OpenAI Embeddings API
- `fastembed` - FastEmbed ONNX-based embeddings (lighter than sentence-transformers)
- `splade` - SPLADE sparse embeddings for hybrid search
    - `embed_sparse/2`, `embed_batch_sparse/2` for native sparse vectors
    - Automatic sparse-to-dense conversion for compatibility
- `colbert` - ColBERT multi-vector embeddings for fine-grained matching
    - `embed_multi/2`, `embed_batch_multi/2` for token-level vectors
    - `maxsim_score/2` for late interaction scoring
- `clip` - CLIP image/text cross-modal embeddings
    - `embed_image/2`, `embed_image_batch/2` for image embeddings
    - Text embeddings in same vector space for cross-modal search

#### Features

- Batch embedding with configurable chunk size
- Provider chain with automatic fallback on failure
- Application supervision tree with ETS-based rate limiting
- Comprehensive EUnit test suite

[2.2.1]: https://github.com/barrel-db/barrel_embed/releases/tag/v2.2.1
[2.2.0]: https://github.com/barrel-db/barrel_embed/releases/tag/v2.2.0
[2.1.1]: https://github.com/barrel-db/barrel_embed/releases/tag/v2.1.1
[2.0.1]: https://github.com/barrel-db/barrel_embed/releases/tag/v2.0.1
[2.0.0]: https://github.com/barrel-db/barrel_embed/releases/tag/v2.0.0
[1.0.0]: https://github.com/barrel-db/barrel_embed/releases/tag/v1.0.0
[0.2.0]: https://github.com/barrel-db/barrel_embed/releases/tag/v0.2.0
[0.1.0]: https://github.com/barrel-db/barrel_embed/releases/tag/v0.1.0
