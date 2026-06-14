# Voyage AI Provider

Embedding generation using Voyage AI's Embeddings API. Voyage provides state-of-the-art embeddings optimized for retrieval and RAG applications.

## Requirements

- Voyage AI API key from [dash.voyageai.com/api-keys](https://dash.voyageai.com/api-keys)

## Configuration

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {voyage, #{
        api_key => <<"...">>,                    % or use env var
        model => <<"voyage-3">>                  % default
    }}
}).
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `api_key` | binary | `VOYAGE_API_KEY` env var | API key |
| `model` | binary | `<<"voyage-3">>` | Model name |
| `url` | binary | `<<"https://api.voyageai.com/v1">>` | API endpoint |

## Using Environment Variable

Set `VOYAGE_API_KEY` instead of passing in config:

```bash
export VOYAGE_API_KEY=pa-...
```

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {voyage, #{}}  % uses env var
}).
```

## Supported Models

| Model | Dimensions | Notes |
|-------|-----------|-------|
| `voyage-3` | 1024 | Default, best quality |
| `voyage-3-lite` | 512 | Faster, cheaper |
| `voyage-code-3` | 1024 | Optimized for code |
| `voyage-finance-2` | 1024 | Financial domain |
| `voyage-law-2` | 1024 | Legal domain |
| `voyage-large-2` | 1536 | Legacy, high quality |

## Domain-Specific Models

Voyage offers specialized models for specific domains:

```erlang
%% For code search
{ok, State} = barrel_embed:init(#{
    embedder => {voyage, #{model => <<"voyage-code-3">>}}
}).

%% For legal documents
{ok, State} = barrel_embed:init(#{
    embedder => {voyage, #{model => <<"voyage-law-2">>}}
}).

%% For financial documents
{ok, State} = barrel_embed:init(#{
    embedder => {voyage, #{model => <<"voyage-finance-2">>}}
}).
```

## Example

```erlang
%% Initialize
{ok, State} = barrel_embed:init(#{
    embedder => {voyage, #{
        api_key => <<"pa-...">>,
        model => <<"voyage-3">>
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

## Why Voyage AI?

- Top-ranked on MTEB benchmark for retrieval
- Optimized specifically for RAG and search
- Domain-specific models for code, law, finance
- OpenAI-compatible API format

## Pricing

See [voyageai.com/pricing](https://www.voyageai.com/pricing) for current rates.
