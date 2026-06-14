# OpenAI Provider

Embedding generation using OpenAI's Embeddings API.

## Requirements

- OpenAI API key

## Configuration

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {openai, #{
        api_key => <<"sk-...">>,                    % or use env var
        model => <<"text-embedding-3-small">>       % default
    }}
}).
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `api_key` | binary | `OPENAI_API_KEY` env var | API key |
| `model` | binary | `<<"text-embedding-3-small">>` | Model name |
| `url` | binary | `<<"https://api.openai.com/v1/embeddings">>` | API endpoint |

## Using Environment Variable

Set `OPENAI_API_KEY` instead of passing in config:

```bash
export OPENAI_API_KEY=sk-...
```

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {openai, #{}}  % uses env var
}).
```

## Supported Models

| Model | Dimensions | Max Tokens | Notes |
|-------|-----------|------------|-------|
| `text-embedding-3-small` | 1536 | 8191 | Fast, cost-effective |
| `text-embedding-3-large` | 3072 | 8191 | Highest quality |
| `text-embedding-ada-002` | 1536 | 8191 | Legacy |

## Example

```erlang
%% Initialize
{ok, State} = barrel_embed:init(#{
    embedder => {openai, #{
        api_key => <<"sk-proj-...">>,
        model => <<"text-embedding-3-small">>
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

## Rate Limiting

OpenAI has rate limits. For high-volume usage, consider:

1. Using batch embedding instead of single calls
2. Implementing retry logic at the application level
3. Using the provider chain with fallback

```erlang
#{embedder => [
    {openai, #{}},
    {ollama, #{url => <<"http://localhost:11434">>}}  % fallback
]}
```

## Cost Considerations

OpenAI charges per token. To optimize costs:

- Use `text-embedding-3-small` for most use cases
- Batch multiple texts in single API calls
- Cache embeddings when possible
