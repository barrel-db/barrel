# Azure OpenAI Provider

Embedding generation using Azure OpenAI's Embeddings API. Provides enterprise-grade embeddings with Azure security, compliance, and regional deployment.

## Requirements

- Azure subscription with OpenAI resource
- Deployed embedding model in Azure OpenAI Studio
- API key from Azure Portal

## Configuration

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {azure, #{
        api_key => <<"...">>,                              % or use env var
        endpoint => <<"https://your-resource.cognitiveservices.azure.com">>,
        deployment => <<"text-embedding-3-small">>         % your deployment name
    }}
}).
```

!!! note "Endpoint Format"
    Your endpoint may use either `*.openai.azure.com` or `*.cognitiveservices.azure.com`
    depending on your Azure configuration. Check Azure Portal for the exact URL.

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `api_key` | binary | `AZURE_OPENAI_API_KEY` env var | API key |
| `endpoint` | binary | `AZURE_OPENAI_ENDPOINT` env var | Azure OpenAI endpoint |
| `deployment` | binary | required | Deployment name |
| `api_version` | binary | `<<"2024-02-01">>` | API version |

## Using Environment Variables

Set environment variables instead of passing in config:

```bash
export AZURE_OPENAI_API_KEY=your-api-key
export AZURE_OPENAI_ENDPOINT=https://your-resource.cognitiveservices.azure.com
```

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {azure, #{
        deployment => <<"text-embedding-3-small">>
    }}
}).
```

## Supported Models

Deploy these models in Azure OpenAI Studio:

| Model | Dimensions | Notes |
|-------|-----------|-------|
| `text-embedding-3-small` | 1536 | Fast, cost-effective |
| `text-embedding-3-large` | 3072 | Highest quality |
| `text-embedding-ada-002` | 1536 | Legacy |

## Setup Steps

1. **Create Azure OpenAI Resource**
   - Go to Azure Portal
   - Create an "Azure OpenAI" resource
   - Note the endpoint URL

2. **Deploy a Model**
   - Go to Azure OpenAI Studio
   - Click "Deployments" -> "Create"
   - Select an embedding model
   - Note the deployment name

3. **Get API Key**
   - Go to Azure Portal -> Your Resource -> Keys and Endpoint
   - Copy Key 1 or Key 2

## Example

```erlang
%% Initialize
{ok, State} = barrel_embed:init(#{
    embedder => {azure, #{
        api_key => <<"your-api-key">>,
        endpoint => <<"https://your-resource.cognitiveservices.azure.com">>,
        deployment => <<"text-embedding-3-small">>,
        api_version => <<"2024-02-01">>
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

## Why Azure OpenAI?

- Enterprise security and compliance (SOC 2, HIPAA, etc.)
- Regional deployment (data residency)
- Azure Active Directory integration
- Private endpoints (VNet integration)
- Same models as OpenAI with enterprise features

## Pricing

Azure OpenAI pricing is similar to OpenAI. See [azure.microsoft.com/pricing/details/cognitive-services/openai-service/](https://azure.microsoft.com/pricing/details/cognitive-services/openai-service/) for current rates.
