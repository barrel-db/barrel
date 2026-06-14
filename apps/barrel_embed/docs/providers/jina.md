# Jina AI Provider

Embedding generation using Jina AI's Embeddings API. Jina provides multilingual embeddings with 8K context length.

## Requirements

- Jina AI API key from [jina.ai/api](https://jina.ai/api/) (free tier available)

## Configuration

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {jina, #{
        api_key => <<"...">>,                    % or use env var
        model => <<"jina-embeddings-v3">>        % default
    }}
}).
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `api_key` | binary | `JINA_API_KEY` env var | API key |
| `model` | binary | `<<"jina-embeddings-v3">>` | Model name |
| `url` | binary | `<<"https://api.jina.ai/v1">>` | API endpoint |

## Using Environment Variable

Set `JINA_API_KEY` instead of passing in config:

```bash
export JINA_API_KEY=jina_...
```

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {jina, #{}}  % uses env var
}).
```

## Supported Models

| Model | Dimensions | Notes |
|-------|-----------|-------|
| `jina-embeddings-v3` | 1024 | Default, 8K context, multilingual |
| `jina-embeddings-v2-base-en` | 768 | English |
| `jina-embeddings-v2-base-de` | 768 | German |
| `jina-embeddings-v2-base-es` | 768 | Spanish |
| `jina-embeddings-v2-base-zh` | 768 | Chinese |
| `jina-colbert-v2` | 128 | Late interaction (per token) |
| `jina-clip-v1` | 768 | Multimodal (text + images) |

## Language-Specific Models

For single-language use cases, language-specific models may perform better:

```erlang
%% German documents
{ok, State} = barrel_embed:init(#{
    embedder => {jina, #{model => <<"jina-embeddings-v2-base-de">>}}
}).

%% Chinese documents
{ok, State} = barrel_embed:init(#{
    embedder => {jina, #{model => <<"jina-embeddings-v2-base-zh">>}}
}).
```

## Example

```erlang
%% Initialize
{ok, State} = barrel_embed:init(#{
    embedder => {jina, #{
        api_key => <<"jina_...">>,
        model => <<"jina-embeddings-v3">>
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

## Why Jina AI?

- 8K context length (longest among embedding providers)
- Free tier available (1M tokens/month)
- Multilingual support (100+ languages)
- Language-specific models available
- OpenAI-compatible API format

## Pricing

Jina offers a generous free tier. See [jina.ai/api](https://jina.ai/api/) for pricing details.
