# SPLADE Provider

Sparse embedding generation using SPLADE models.

## What is SPLADE?

SPLADE (Sparse Lexical and Expansion) produces **sparse vectors** instead of dense vectors. Each dimension corresponds to a vocabulary token, enabling:

- Lexical matching (like BM25)
- Semantic expansion (synonyms, related terms)
- Efficient inverted index storage

## Requirements

```bash
# Using virtualenv with uv (recommended)
./scripts/setup_venv.sh
uv pip install transformers torch --python .venv/bin/python

# Or install manually
pip install transformers torch
```

## Configuration

```erlang
%% Using virtualenv (recommended)
{ok, State} = barrel_embed:init(#{
    embedder => {splade, #{
        venv => "/absolute/path/to/.venv",
        model => "prithivida/Splade_PP_en_v1",     % default
        timeout => 120000                           % default, ms
    }}
}).

%% Using system Python
{ok, State} = barrel_embed:init(#{
    embedder => {splade, #{
        python => "python3",                        % default
        model => "prithivida/Splade_PP_en_v1",     % default
        timeout => 120000                           % default, ms
    }}
}).
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `venv` | string | `undefined` | Path to virtualenv (recommended) |
| `python` | string | `"python3"` | Python executable (if no venv) |
| `model` | string | `"prithivida/Splade_PP_en_v1"` | Model name |
| `timeout` | integer | `120000` | Timeout in milliseconds |

## Supported Models

| Model | Notes |
|-------|-------|
| `prithivida/Splade_PP_en_v1` | Default, SPLADE++ |
| `naver/splade-cocondenser-ensembledistil` | NAVER's SPLADE |

## Sparse Vector Format

Unlike dense embeddings, SPLADE returns sparse vectors:

```erlang
#{
    indices => [1, 5, 10, 42, ...],   % vocabulary token IDs
    values => [0.5, 0.3, 0.8, 0.2, ...]  % weights
}
```

## API

### Native Sparse API

```erlang
%% Single text
{ok, SparseVec} = barrel_embed_splade:embed_sparse(<<"query text">>, Config).
#{indices := Indices, values := Values} = SparseVec.

%% Batch
{ok, SparseVecs} = barrel_embed_splade:embed_batch_sparse(Texts, Config).
```

### Dense API (Compatibility)

For compatibility with dense search, SPLADE can convert to dense vectors:

```erlang
%% Returns dense vector (sparse converted to dense)
{ok, DenseVec} = barrel_embed:embed(<<"text">>, State).
```

!!! warning
    Dense conversion is memory-intensive for large vocabularies (~30k dimensions). Use native sparse API when possible.

## Example: Hybrid Search

Combine SPLADE with dense embeddings for hybrid search:

```erlang
%% SPLADE for lexical matching
{ok, SpladeState} = barrel_embed:init(#{embedder => {splade, #{}}}).
{ok, SparseVec} = barrel_embed_splade:embed_sparse(Query, SpladeConfig).

%% Dense for semantic matching
{ok, DenseState} = barrel_embed:init(#{embedder => {ollama, #{...}}}).
{ok, DenseVec} = barrel_embed:embed(Query, DenseState).

%% Combine scores (application-specific)
FinalScore = Alpha * SparseScore + (1 - Alpha) * DenseScore.
```

## Use Cases

- **Keyword search with semantic expansion**: SPLADE expands queries with related terms
- **Hybrid retrieval**: Combine with dense embeddings
- **Efficient storage**: Sparse vectors compress well
