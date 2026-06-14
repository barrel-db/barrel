# Cohere Provider

Embedding generation using Cohere's Embed API.

## Requirements

- Cohere API key from [dashboard.cohere.com/api-keys](https://dashboard.cohere.com/api-keys)

## Configuration

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {cohere, #{
        api_key => <<"...">>,                    % or use env var
        model => <<"embed-english-v3.0">>        % default
    }}
}).
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `api_key` | binary | `COHERE_API_KEY` env var | API key |
| `model` | binary | `<<"embed-english-v3.0">>` | Model name |
| `input_type` | binary | `<<"search_document">>` | Input type for optimization |
| `url` | binary | `<<"https://api.cohere.com/v1">>` | API endpoint |

## Using Environment Variable

Set `COHERE_API_KEY` instead of passing in config:

```bash
export COHERE_API_KEY=your-api-key
```

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {cohere, #{}}  % uses env var
}).
```

## Supported Models

| Model | Dimensions | Notes |
|-------|-----------|-------|
| `embed-english-v3.0` | 1024 | Default, English |
| `embed-multilingual-v3.0` | 1024 | 100+ languages |
| `embed-english-light-v3.0` | 384 | Faster, cheaper |
| `embed-multilingual-light-v3.0` | 384 | Multilingual light |

## Input Types

Cohere supports different input types for optimization:

| Input Type | Use Case |
|------------|----------|
| `search_document` | Documents to be searched (default) |
| `search_query` | Search queries |
| `classification` | Classification tasks |
| `clustering` | Clustering tasks |

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {cohere, #{
        input_type => <<"search_query">>  % for queries
    }}
}).
```

## Example

```erlang
%% Initialize
{ok, State} = barrel_embed:init(#{
    embedder => {cohere, #{
        api_key => <<"your-api-key">>,
        model => <<"embed-multilingual-v3.0">>
    }}
}).

%% Generate embeddings
{ok, Vec} = barrel_embed:embed(<<"Machine learning is fascinating">>, State).

%% Batch (more efficient for multiple texts)
{ok, Vecs} = barrel_embed:embed_batch([
    <<"Document about AI">>,
    <<"Document about databases">>,
    <<"Document about networking">>
], State).
```

## Pricing

Cohere charges per token. See [cohere.com/pricing](https://cohere.com/pricing) for current rates.

For high-volume usage:

- Use batch embedding instead of single calls
- Consider `embed-english-light-v3.0` for cost savings
- Cache embeddings when possible
