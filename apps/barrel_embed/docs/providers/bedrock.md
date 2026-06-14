# AWS Bedrock Provider

Embedding generation using AWS Bedrock's embedding API. Provides access to Amazon Titan and Cohere embedding models through AWS infrastructure.

## Requirements

- AWS account with Bedrock access
- Model access enabled in Bedrock console
- Authentication via API key (ABSK) or IAM credentials

## Configuration

### Option 1: API Key (Recommended)

Uses AWS Bedrock API Key (ABSK format, available since July 2025):

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {bedrock, #{
        api_key => <<"ABSK...">>,                % or use env var
        region => <<"us-east-1">>,
        model => <<"amazon.titan-embed-text-v2:0">>
    }}
}).
```

### Option 2: IAM Credentials

Uses AWS access keys with SigV4 signing:

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {bedrock, #{
        access_key_id => <<"AKIA...">>,
        secret_access_key => <<"...">>,
        session_token => <<"...">>,              % optional, for temporary credentials
        region => <<"us-east-1">>,
        model => <<"amazon.titan-embed-text-v2:0">>
    }}
}).
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `api_key` | binary | `BEDROCK_API_KEY` env var | API key (ABSK format) |
| `access_key_id` | binary | `AWS_ACCESS_KEY_ID` env var | IAM access key |
| `secret_access_key` | binary | `AWS_SECRET_ACCESS_KEY` env var | IAM secret key |
| `session_token` | binary | `AWS_SESSION_TOKEN` env var | Session token (optional) |
| `region` | binary | `<<"us-east-1">>` | AWS region |
| `model` | binary | `<<"amazon.titan-embed-text-v2:0">>` | Model ID |

## Using Environment Variables

```bash
# Option 1: API key
export BEDROCK_API_KEY=ABSK...

# Option 2: IAM credentials
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-1
```

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {bedrock, #{
        model => <<"amazon.titan-embed-text-v2:0">>
    }}
}).
```

## Supported Models

Enable model access in AWS Bedrock console first:

| Model | Dimensions | Notes |
|-------|-----------|-------|
| `amazon.titan-embed-text-v2:0` | 1024 | Default, Amazon Titan |
| `amazon.titan-embed-text-v1` | 1536 | Legacy Titan |
| `cohere.embed-english-v3` | 1024 | Cohere English |
| `cohere.embed-multilingual-v3` | 1024 | Cohere multilingual |

## Setup Steps

1. **Enable Model Access**
   - Go to AWS Console -> Bedrock
   - Click "Model access"
   - Request access for embedding models

2. **Get Credentials**

   For API key:
   - Go to Bedrock -> API keys
   - Create a new API key (ABSK format)

   For IAM:
   - Create IAM user with Bedrock permissions
   - Generate access keys

## Example

```erlang
%% Initialize with API key
{ok, State} = barrel_embed:init(#{
    embedder => {bedrock, #{
        api_key => <<"ABSK...">>,
        region => <<"us-east-1">>,
        model => <<"amazon.titan-embed-text-v2:0">>
    }}
}).

%% Generate embeddings
{ok, Vec} = barrel_embed:embed(<<"Machine learning is fascinating">>, State).

%% Batch (processed sequentially - Bedrock doesn't support native batch)
{ok, Vecs} = barrel_embed:embed_batch([
    <<"Document about AI">>,
    <<"Document about databases">>,
    <<"Document about networking">>
], State).
```

## Regions

Bedrock is available in these regions:

- `us-east-1` (N. Virginia) - Most models
- `us-west-2` (Oregon)
- `eu-central-1` (Frankfurt)
- `eu-west-1` (Ireland)
- `ap-northeast-1` (Tokyo)

## Why AWS Bedrock?

- AWS infrastructure (VPC, IAM, CloudWatch)
- Multiple model providers (Titan, Cohere) through one API
- Pay-per-use pricing
- Enterprise compliance (SOC, HIPAA, FedRAMP)

## Pricing

See [aws.amazon.com/bedrock/pricing](https://aws.amazon.com/bedrock/pricing) for current rates.
