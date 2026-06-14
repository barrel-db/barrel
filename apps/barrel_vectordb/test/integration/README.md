# Integration Tests

Integration tests verify the embedding providers work with real backends.
These tests are **not** run automatically by `rebar3 eunit`.

## Prerequisites

### Local Provider (Python)

**Option 1: Virtual environment (recommended)**

```bash
# Create virtual environment
python3 -m venv ~/.venv/barrel_embed
source ~/.venv/barrel_embed/bin/activate

# Install dependencies
pip install sentence-transformers

# Verify installation
python -c "from sentence_transformers import SentenceTransformer; print('OK')"
```

Run tests with either:

```bash
# Option A: Activate venv first
source ~/.venv/barrel_embed/bin/activate
rebar3 eunit --module=barrel_vectordb_integration_tests

# Option B: Use BARREL_PYTHON environment variable
BARREL_PYTHON=~/.venv/barrel_embed/bin/python rebar3 eunit --module=barrel_vectordb_integration_tests
```

**Option 2: System-wide installation**

```bash
pip install sentence-transformers
rebar3 eunit --module=barrel_vectordb_integration_tests
```

**Note:** The first run will download the model (~400MB for the default model).

### Ollama Provider

```bash
# Install Ollama from https://ollama.ai
# Start Ollama server
ollama serve

# Pull embedding model
ollama pull nomic-embed-text

# Verify it's running
curl http://localhost:11434/api/tags
```

### OpenAI Provider

```bash
# Set your API key as an environment variable
export OPENAI_API_KEY=sk-...

# Verify it works
curl https://api.openai.com/v1/models \
  -H "Authorization: Bearer $OPENAI_API_KEY"
```

**Note:** OpenAI API calls incur costs. The integration tests use minimal text to reduce usage.

## Running Integration Tests

```bash
# Run all integration tests
rebar3 eunit --module=barrel_vectordb_integration_tests

# Run specific test
rebar3 eunit --module=barrel_vectordb_integration_tests --test=test_local_embed
```

## Test Coverage

| Test | Provider | What it tests |
|------|----------|---------------|
| `test_local_embed` | local | Single text embedding |
| `test_local_batch` | local | Batch embedding |
| `test_local_custom_model` | local | Custom model loading |
| `test_ollama_embed` | ollama | Single text embedding |
| `test_ollama_batch` | ollama | Batch embedding |
| `test_ollama_custom_model` | ollama | Custom model selection |
| `test_openai_embed` | openai | Single text embedding |
| `test_openai_batch` | openai | Batch embedding |
| `test_openai_custom_model` | openai | text-embedding-3-large model |
| `test_provider_chain` | multiple | Fallback between providers |

## Skipping Tests

Tests automatically skip if the required backend is not available:

```
test_ollama_embed: skipped (ollama not available)
```

## Adding New Integration Tests

Integration tests should:
1. Check if the backend is available before running
2. Use `?_test` or similar to allow skipping
3. Clean up any resources after the test
4. Use small test data to keep tests fast
