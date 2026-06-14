# Google Vertex AI Provider

Embedding generation using Google Vertex AI's embedding API. Provides access to Google's embedding models including the Gecko family.

## Requirements

- Google Cloud project with Vertex AI API enabled
- Authentication via access token or API key

## Configuration

### Option 1: Access Token (Recommended for development)

Get a token with `gcloud auth print-access-token`:

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {vertex, #{
        access_token => <<"ya29...">>,           % or use env var
        project => <<"my-project">>,
        region => <<"us-central1">>,
        model => <<"text-embedding-004">>
    }}
}).
```

### Option 2: API Key

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {vertex, #{
        api_key => <<"...">>,                    % or use env var
        project => <<"my-project">>,
        region => <<"us-central1">>,
        model => <<"text-embedding-004">>
    }}
}).
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `access_token` | binary | `GOOGLE_ACCESS_TOKEN` env var | OAuth access token |
| `api_key` | binary | `GOOGLE_API_KEY` env var | API key |
| `project` | binary | `GOOGLE_CLOUD_PROJECT` env var | GCP project ID |
| `region` | binary | `<<"us-central1">>` | GCP region |
| `model` | binary | `<<"text-embedding-004">>` | Model name |

## Using Environment Variables

```bash
# Get access token
export GOOGLE_ACCESS_TOKEN=$(gcloud auth print-access-token)
export GOOGLE_CLOUD_PROJECT=my-project
```

```erlang
{ok, State} = barrel_embed:init(#{
    embedder => {vertex, #{}}
}).
```

## Supported Models

| Model | Dimensions | Notes |
|-------|-----------|-------|
| `text-embedding-004` | 768 | Default, latest |
| `text-embedding-005` | 768 | Newest |
| `textembedding-gecko@001` | 768 | Legacy |
| `textembedding-gecko@003` | 768 | Legacy |
| `textembedding-gecko-multilingual@001` | 768 | Multilingual legacy |
| `text-multilingual-embedding-002` | 768 | Multilingual |

## Setup Steps

1. **Enable Vertex AI API**
   - Go to GCP Console
   - Enable "Vertex AI API"

2. **Get Authentication**

   For development (access token):
   ```bash
   gcloud auth login
   gcloud auth print-access-token
   ```

   For production (service account):
   - Create service account with Vertex AI User role
   - Download JSON key
   - Use `GOOGLE_APPLICATION_CREDENTIALS`

3. **Note Project ID**
   ```bash
   gcloud config get-value project
   ```

## Example

```erlang
%% Initialize
{ok, State} = barrel_embed:init(#{
    embedder => {vertex, #{
        access_token => <<"ya29...">>,
        project => <<"my-project">>,
        region => <<"us-central1">>,
        model => <<"text-embedding-004">>
    }}
}).

%% Generate embeddings
{ok, Vec} = barrel_embed:embed(<<"Machine learning is fascinating">>, State).

%% Batch
{ok, Vecs} = barrel_embed:embed_batch([
    <<"Document about AI">>,
    <<"Document about databases">>,
    <<"Document about networking">>
], State).
```

## Regions

Vertex AI is available in many regions. Common choices:

- `us-central1` (Iowa) - Default
- `us-east1` (South Carolina)
- `europe-west1` (Belgium)
- `asia-northeast1` (Tokyo)

## Access Token Refresh

Access tokens expire after 1 hour. For long-running applications:

1. Use service account authentication
2. Or refresh token periodically:

```erlang
%% Refresh token
NewToken = os:cmd("gcloud auth print-access-token"),
{ok, NewState} = barrel_embed:init(#{
    embedder => {vertex, #{
        access_token => list_to_binary(string:trim(NewToken)),
        project => <<"my-project">>
    }}
}).
```

## Why Vertex AI?

- Google's latest embedding models
- Tight integration with GCP services
- BigQuery, Cloud Storage integration
- Enterprise security (VPC-SC, CMEK)

## Pricing

See [cloud.google.com/vertex-ai/pricing](https://cloud.google.com/vertex-ai/pricing) for current rates.
