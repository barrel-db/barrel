# ColBERT Provider

Multi-vector embedding generation using ColBERT models.

## What is ColBERT?

ColBERT (Contextualized Late Interaction over BERT) produces **multiple vectors per text** - one for each token. This enables fine-grained, token-level matching.

**Late Interaction**: Query and document are encoded independently, then compared token-by-token using MaxSim scoring.

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
    embedder => {colbert, #{
        venv => "/absolute/path/to/.venv",
        model => "colbert-ir/colbertv2.0",     % default
        timeout => 120000                       % default, ms
    }}
}).

%% Using system Python
{ok, State} = barrel_embed:init(#{
    embedder => {colbert, #{
        python => "python3",                    % default
        model => "colbert-ir/colbertv2.0",     % default
        timeout => 120000                       % default, ms
    }}
}).
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `venv` | string | `undefined` | Path to virtualenv (recommended) |
| `python` | string | `"python3"` | Python executable (if no venv) |
| `model` | string | `"colbert-ir/colbertv2.0"` | Model name |
| `timeout` | integer | `120000` | Timeout in milliseconds |

## Supported Models

| Model | Dimensions | Notes |
|-------|-----------|-------|
| `colbert-ir/colbertv2.0` | 128 | Default, standard ColBERT |
| `answerdotai/answerai-colbert-small-v1` | 96 | Smaller, faster |
| `jinaai/jina-colbert-v2` | 128 | Long context (8192 tokens) |

## Multi-Vector Format

ColBERT returns a list of token vectors:

```erlang
[
    [0.1, 0.2, ...],  % vector for token 1
    [0.3, 0.4, ...],  % vector for token 2
    [0.5, 0.6, ...],  % vector for token 3
    ...
]
```

## API

### Native Multi-Vector API

```erlang
%% Single text - returns list of token vectors
{ok, TokenVecs} = barrel_embed_colbert:embed_multi(<<"document text">>, Config).

%% Batch
{ok, MultiVecs} = barrel_embed_colbert:embed_batch_multi(Texts, Config).
```

### MaxSim Scoring

ColBERT uses MaxSim for scoring:

```
Score(Q, D) = Σ max(qi · dj) for all qi in Q, dj in D
```

```erlang
%% Calculate MaxSim score between query and document
QueryVecs = ...,  % from embed_multi
DocVecs = ...,    % from embed_multi
Score = barrel_embed_colbert:maxsim_score(QueryVecs, DocVecs).
```

### Dense API (Mean Pooling)

For compatibility, ColBERT can return a single vector via mean pooling:

```erlang
%% Returns mean-pooled single vector
{ok, Vec} = barrel_embed:embed(<<"text">>, State).
```

!!! note
    Mean pooling loses the fine-grained matching capability. Use `embed_multi/2` for best results.

## Example: Passage Retrieval

```erlang
%% Initialize
{ok, State} = barrel_embed:init(#{embedder => {colbert, #{}}}).
{_, Config} = hd(maps:get(providers, State)).

%% Embed query and documents
{ok, QueryVecs} = barrel_embed_colbert:embed_multi(<<"What is machine learning?">>, Config).
{ok, Doc1Vecs} = barrel_embed_colbert:embed_multi(Doc1, Config).
{ok, Doc2Vecs} = barrel_embed_colbert:embed_multi(Doc2, Config).

%% Score and rank
Score1 = barrel_embed_colbert:maxsim_score(QueryVecs, Doc1Vecs).
Score2 = barrel_embed_colbert:maxsim_score(QueryVecs, Doc2Vecs).
```

## Use Cases

- **Passage retrieval**: Fine-grained matching for QA systems
- **Semantic search**: When single-vector similarity isn't precise enough
- **Long documents**: Token-level matching captures local relevance
