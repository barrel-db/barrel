# Contributing

Guidelines for contributing to Barrel VectorDB.

## Development Setup

### Prerequisites

- Erlang/OTP 26 or later
- rebar3
- Python 3.8+ (for embedding providers)

### Clone and Build

```bash
git clone https://gitlab.enki.io/barrel-db/barrel_vectordb.git
cd barrel_vectordb
rebar3 compile
```

### Run Tests

```bash
# Unit tests (no external dependencies)
rebar3 eunit

# All tests including integration
rebar3 ct
```

## Testing

### Unit Tests

Unit tests use mocking and don't require external dependencies:

```bash
rebar3 eunit
```

### Integration Tests

Integration tests verify real embedding providers. Setup required backends first:

```bash
# Setup for local provider
pip install sentence-transformers

# Setup for Ollama provider
ollama serve &
ollama pull nomic-embed-text

# Setup for OpenAI provider
export OPENAI_API_KEY=sk-...

# Run integration tests
rebar3 eunit --module=barrel_vectordb_integration_tests
```

Tests automatically skip if their required backend is unavailable.

### Benchmarks

```bash
# Run benchmark suite
rebar3 as bench compile && rebar3 as bench eunit --module=barrel_vectordb_bench

# Backend comparison
./scripts/run_backend_bench.sh --quick
```

## Code Style

- Follow standard Erlang conventions
- Use edoc for documentation
- Keep functions focused and small
- Add tests for new functionality

### Module Structure

```erlang
-module(barrel_vectordb_example).

%% API
-export([public_function/1]).

%% Internal exports
-export_type([my_type/0]).

-type my_type() :: term().

%% @doc Public function documentation.
-spec public_function(Arg) -> Result when
    Arg :: term(),
    Result :: ok | {error, term()}.
public_function(Arg) ->
    internal_function(Arg).

%% Internal functions

internal_function(Arg) ->
    ok.
```

## Pull Requests

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Run the test suite
6. Submit a pull request

### Commit Messages

Use clear, descriptive commit messages:

```
Add support for new embedding provider

- Add barrel_vectordb_embed_newprovider.erl
- Update embed module to support new provider
- Add unit tests
```

## Architecture

### Core Components

- **barrel_vectordb.erl**: Main API module
- **barrel_vectordb_store.erl**: Store process (gen_batch_server)
- **barrel_vectordb_hnsw.erl**: HNSW index implementation
- **barrel_vectordb_embed.erl**: Embedding provider abstraction

### Storage

- RocksDB with column families
- Vectors stored with 8-bit quantization
- HNSW index checkpoints for fast restart

## Reporting Issues

Use the GitLab issue tracker:

- [:material-bug: Report Issues](https://gitlab.enki.io/barrel-db/barrel_vectordb/-/issues)

Include:

- Erlang/OTP version
- barrel_vectordb version
- Steps to reproduce
- Expected vs actual behavior
- Relevant logs

## License

Contributions are licensed under Apache-2.0.
