# Mistral Provider

Embedding generation using Mistral AI's Embeddings API. Mistral provides high-quality embeddings with EU data residency.

## Requirements

- Mistral API key from [console.mistral.ai/api-keys](https://console.mistral.ai/api-keys)

## Configuration

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {mistral, #{
        api_key => <<"...">>,                    % or use env var
        model => <<"mistral-embed">>             % default
    }}
}).
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `api_key` | binary | `MISTRAL_API_KEY` env var | API key |
| `model` | binary | `<<"mistral-embed">>` | Model name |
| `url` | binary | `<<"https://api.mistral.ai/v1">>` | API endpoint |

## Using Environment Variable

Set `MISTRAL_API_KEY` instead of passing in config:

```bash
export MISTRAL_API_KEY=your-api-key
```

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {mistral, #{}}  % uses env var
}).
```

## Supported Models

| Model | Dimensions | Notes |
|-------|-----------|-------|
| `mistral-embed` | 1024 | Default, multilingual |

## Example

```erlang
%% Initialize
{ok, State} = barrel_embed:init(#{
    embedder => {mistral, #{
        api_key => <<"your-api-key">>,
        model => <<"mistral-embed">>
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

## Why Mistral?

- EU-based company (data residency compliance)
- OpenAI-compatible API format
- Competitive pricing
- Strong multilingual support

## Pricing

See [mistral.ai/pricing](https://mistral.ai/pricing) for current rates.
