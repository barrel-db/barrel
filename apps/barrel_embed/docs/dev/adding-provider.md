# Adding a Provider

This guide explains how to add a new embedding provider to barrel_embed.

## Provider Types

- **Cloud providers** - Use HTTP APIs (OpenAI, Cohere, etc.)
- **Local providers** - Use Python subprocess (sentence-transformers, SPLADE, etc.)

---

# Adding a Cloud Provider

Cloud providers are simpler - they make HTTP requests to external APIs.

## Step 1: Create Erlang Module

Create `src/barrel_embed_myprovider.erl`:

```erlang
-module(barrel_embed_myprovider).
-behaviour(barrel_embed_provider).

-export([embed/2, embed_batch/2, dimension/1, name/0, init/1, available/1]).

-define(DEFAULT_URL, <<"https://api.myprovider.com/v1">>).
-define(DEFAULT_MODEL, <<"embed-model">>).
-define(DEFAULT_TIMEOUT, 30000).
-define(DEFAULT_DIMENSION, 1024).

name() -> myprovider.

dimension(Config) ->
    maps:get(dimension, Config, ?DEFAULT_DIMENSION).

init(Config) ->
    case get_api_key(Config) of
        undefined ->
            {error, api_key_not_configured};
        ApiKey ->
            {ok, maps:merge(#{
                url => ?DEFAULT_URL,
                model => ?DEFAULT_MODEL,
                timeout => ?DEFAULT_TIMEOUT,
                dimension => ?DEFAULT_DIMENSION
            }, Config#{api_key => ApiKey})}
    end.

available(Config) ->
    maps:get(api_key, Config, undefined) =/= undefined.

embed(Text, Config) ->
    case embed_batch([Text], Config) of
        {ok, [Vector]} -> {ok, Vector};
        {error, _} = E -> E
    end.

embed_batch(Texts, Config) ->
    Url = maps:get(url, Config),
    ApiKey = maps:get(api_key, Config),
    Model = maps:get(model, Config),
    Timeout = maps:get(timeout, Config),

    ApiUrl = <<Url/binary, "/embeddings">>,
    Body = json:encode(#{
        <<"input">> => Texts,
        <<"model">> => Model
    }),
    Headers = [
        {<<"Authorization">>, <<"Bearer ", ApiKey/binary>>},
        {<<"Content-Type">>, <<"application/json">>}
    ],

    case hackney:request(post, ApiUrl, Headers, Body,
                         [{recv_timeout, Timeout}, {with_body, true}]) of
        {ok, 200, _RespHeaders, RespBody} ->
            parse_response(RespBody);
        {ok, StatusCode, _RespHeaders, RespBody} ->
            {error, {http_error, StatusCode, RespBody}};
        {error, Reason} ->
            {error, {request_failed, Reason}}
    end.

%% Internal functions
get_api_key(Config) ->
    case maps:get(api_key, Config, undefined) of
        undefined ->
            case os:getenv("MYPROVIDER_API_KEY") of
                false -> undefined;
                Key -> list_to_binary(Key)
            end;
        Key when is_binary(Key) -> Key;
        Key when is_list(Key) -> list_to_binary(Key)
    end.

parse_response(Body) ->
    Response = json:decode(Body),
    case maps:find(<<"data">>, Response) of
        {ok, Data} ->
            Sorted = lists:sort(
                fun(A, B) ->
                    maps:get(<<"index">>, A, 0) < maps:get(<<"index">>, B, 0)
                end, Data),
            {ok, [maps:get(<<"embedding">>, Item) || Item <- Sorted]};
        _ ->
            {error, {invalid_response, no_data_field}}
    end.
```

## Step 2: Register Provider

In `src/barrel_embed.erl`, add to `provider_module/1`:

```erlang
provider_module(myprovider) -> barrel_embed_myprovider;
```

## Step 3: Test

```erlang
rebar3 compile
rebar3 shell

{ok, S} = barrel_embed:init(#{embedder => {myprovider, #{}}}).
barrel_embed:embed(<<"test">>, S).
```

## Authentication Patterns

Different APIs use different auth headers:

| Style | Header | Example Providers |
|-------|--------|-------------------|
| Bearer token | `Authorization: Bearer <key>` | OpenAI, Cohere, Voyage, Jina, Mistral |
| API key header | `api-key: <key>` | Azure OpenAI |
| Custom header | `x-goog-api-key: <key>` | Google Vertex AI |

```erlang
%% Bearer token (most common)
{<<"Authorization">>, <<"Bearer ", ApiKey/binary>>}

%% Azure style
{<<"api-key">>, ApiKey}

%% Google style
{<<"x-goog-api-key">>, ApiKey}
```

## Response Formats

### OpenAI-compatible (most providers)

```json
{"data": [{"embedding": [...], "index": 0}, {"embedding": [...], "index": 1}]}
```

```erlang
parse_response(Body) ->
    Response = json:decode(Body),
    case maps:find(<<"data">>, Response) of
        {ok, Data} ->
            Sorted = lists:sort(
                fun(A, B) ->
                    maps:get(<<"index">>, A, 0) < maps:get(<<"index">>, B, 0)
                end, Data),
            {ok, [maps:get(<<"embedding">>, Item) || Item <- Sorted]};
        _ ->
            {error, {invalid_response, no_data_field}}
    end.
```

### Cohere-style

```json
{"embeddings": [[...], [...]]}
```

```erlang
parse_response(Body) ->
    Response = json:decode(Body),
    case maps:find(<<"embeddings">>, Response) of
        {ok, Embeddings} -> {ok, Embeddings};
        _ -> {error, {invalid_response, no_embeddings_field}}
    end.
```

### Vertex AI-style

```json
{"predictions": [{"embeddings": {"values": [...]}}]}
```

```erlang
parse_response(Body) ->
    Response = json:decode(Body),
    case maps:find(<<"predictions">>, Response) of
        {ok, Predictions} ->
            Embeddings = lists:map(
                fun(Pred) ->
                    EmbedObj = maps:get(<<"embeddings">>, Pred),
                    maps:get(<<"values">>, EmbedObj)
                end, Predictions),
            {ok, Embeddings};
        _ ->
            {error, {invalid_response, no_predictions_field}}
    end.
```

## AWS SigV4 Signing (Bedrock)

For AWS Bedrock, you need AWS Signature Version 4. See `barrel_embed_bedrock.erl` for a complete implementation including:

- `sign_request/11` - Create SigV4 signature
- `canonical_headers/1` - Format headers for signing
- `hmac_sha256/2` - HMAC-SHA256 helper

---

# Adding a Python Provider

## Overview

Adding a provider requires:

1. **Python**: Create provider class extending `AsyncEmbedServer`
2. **Python**: Register in `__main__.py`
3. **Erlang**: Create provider module implementing `barrel_embed_provider`

## Step 1: Create Python Provider

Create `priv/barrel_embed/providers/mymodel.py`:

```python
"""MyModel embedding provider."""

from ..server import AsyncEmbedServer, logger

DEFAULT_MODEL = "org/model-name"


class MyModelServer(AsyncEmbedServer):
    """Embedding server using MyModel."""

    def __init__(self, model_name: str = None, max_workers: int = 4):
        super().__init__(max_workers=max_workers)
        self.model_name = model_name or DEFAULT_MODEL
        self.model = None
        self.dimension = None

    def load_model(self) -> bool:
        """Load the embedding model. Return True on success."""
        try:
            # Import your model library here (lazy import)
            from mylib import MyModel

            logger.info(f"Loading model: {self.model_name}")
            self.model = MyModel(self.model_name)
            self.dimension = self.model.get_dimension()
            logger.info(f"Model loaded: {self.dimension} dimensions")
            return True
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            return False

    def handle_info(self) -> dict:
        """Return model metadata."""
        return {
            "ok": True,
            "dimensions": self.dimension,
            "model": self.model_name,
            "backend": "mymodel",
        }

    def embed_sync(self, texts: list) -> dict:
        """Generate embeddings. Called in thread pool."""
        try:
            embeddings = self.model.encode(texts)
            return {"ok": True, "embeddings": embeddings.tolist()}
        except Exception as e:
            logger.error(f"Embedding failed: {e}")
            return {"ok": False, "error": str(e)}
```

### Key Points

- **Lazy imports**: Import heavy libraries in `load_model()`, not at module level
- **Logging**: Use the provided `logger` for debugging
- **Error handling**: Always catch exceptions and return `{"ok": False, "error": ...}`
- **Thread safety**: `embed_sync` runs in a thread pool, ensure your model is thread-safe

## Step 2: Register Provider

### Update `priv/barrel_embed/__main__.py`

Add your provider to the argument choices:

```python
parser.add_argument(
    "--provider",
    choices=["sentence_transformers", "fastembed", "splade", "colbert", "clip", "mymodel"],
    default="sentence_transformers",
    help="Embedding provider to use"
)
```

Add the import and instantiation:

```python
elif args.provider == "mymodel":
    from .providers.mymodel import MyModelServer
    server = MyModelServer(
        model_name=args.model,
        max_workers=args.max_workers
    )
```

### Update `priv/barrel_embed/providers/__init__.py`

```python
from .mymodel import MyModelServer

__all__ = [
    "SentenceTransformerServer",
    "FastEmbedServer",
    "SpladeServer",
    "ColBERTServer",
    "CLIPServer",
    "MyModelServer",
]
```

## Step 3: Create Erlang Provider

Create `src/barrel_embed_mymodel.erl`:

```erlang
%%%-------------------------------------------------------------------
%%% @doc MyModel embedding provider
%%%
%%% Uses MyModel for embeddings.
%%%
%%% == Requirements ==
%%% ```
%%% pip install mylib
%%% '''
%%%
%%% == Configuration ==
%%% ```
%%% Config = #{
%%%     python => "python3",           %% Python executable
%%%     model => "org/model-name",     %% Model name
%%%     timeout => 120000              %% Timeout in ms
%%% }.
%%% '''
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_embed_mymodel).
-behaviour(barrel_embed_provider).

-export([embed/2, embed_batch/2, dimension/1, name/0, init/1, available/1]).

-define(DEFAULT_PYTHON, "python3").
-define(DEFAULT_MODEL, "org/model-name").
-define(DEFAULT_TIMEOUT, 120000).
-define(DEFAULT_DIMENSION, 768).

%%====================================================================
%% Behaviour Callbacks
%%====================================================================

%% @doc Provider name.
name() -> mymodel.

%% @doc Get dimension for this provider.
dimension(Config) ->
    maps:get(dimension, Config, ?DEFAULT_DIMENSION).

%% @doc Initialize the provider.
init(Config) ->
    Python = maps:get(python, Config, ?DEFAULT_PYTHON),
    Model = maps:get(model, Config, ?DEFAULT_MODEL),
    Timeout = maps:get(timeout, Config, ?DEFAULT_TIMEOUT),

    Args = ["-m", "barrel_embed",
            "--provider", "mymodel",
            "--model", Model],

    Opts = [{timeout, Timeout}, {priv_dir, get_priv_dir()}],

    case barrel_embed_port_server:start_link(Python, Args, Opts) of
        {ok, Server} ->
            case barrel_embed_port_server:info(Server, Timeout) of
                {ok, #{dimensions := Dims}} ->
                    {ok, Config#{
                        server => Server,
                        dimension => Dims,
                        timeout => Timeout
                    }};
                {error, Reason} ->
                    barrel_embed_port_server:stop(Server),
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Check if provider is available.
available(#{server := Server}) ->
    is_process_alive(Server);
available(_) ->
    false.

%% @doc Generate embedding for a single text.
embed(Text, Config) ->
    case embed_batch([Text], Config) of
        {ok, [Vector]} -> {ok, Vector};
        {error, _} = E -> E
    end.

%% @doc Generate embeddings for multiple texts.
embed_batch(Texts, #{server := Server, timeout := Timeout}) ->
    barrel_embed_port_server:embed_batch(Server, Texts, Timeout);
embed_batch(_, _) ->
    {error, server_not_initialized}.

%%====================================================================
%% Internal Functions
%%====================================================================

get_priv_dir() ->
    case code:priv_dir(barrel_embed) of
        {error, bad_name} -> "priv";
        Dir -> Dir
    end.
```

## Step 4: Add Dependencies

Update `priv/pyproject.toml`:

```toml
[project.optional-dependencies]
mymodel = ["mylib>=1.0.0"]
all = [
    # ... existing deps ...
    "mylib>=1.0.0",
]
```

## Step 5: Test

### Test Python Directly

```bash
cd priv
python -m barrel_embed --provider mymodel --help
python -m barrel_embed --provider mymodel --model org/model-name
```

Then send JSON to stdin:

```json
{"id": 1, "action": "info"}
{"id": 2, "action": "embed", "texts": ["hello world"]}
```

### Test in Erlang

```erlang
rebar3 shell

%% Initialize
{ok, State} = barrel_embed:init(#{embedder => {mymodel, #{}}}).

%% Get info
barrel_embed:info(State).

%% Generate embedding
{ok, Vector} = barrel_embed:embed(<<"test">>, State).
length(Vector).  %% Should match dimensions

%% Batch embedding
{ok, Vectors} = barrel_embed:embed_batch([<<"hello">>, <<"world">>], State).
```

## Specialized Providers

### Sparse Embeddings (SPLADE-style)

For providers that return sparse vectors, override the `dispatch` method:

```python
async def dispatch(self, request: dict) -> dict:
    action = request.get("action")

    if action == "info":
        return self.handle_info()
    elif action == "embed":
        texts = request.get("texts", [])
        return await self.handle_embed_sparse(texts)
    else:
        return {"ok": False, "error": f"Unknown action: {action}"}

async def handle_embed_sparse(self, texts: list) -> dict:
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(self.executor, self.embed_sparse_sync, texts)
    return result

def embed_sparse_sync(self, texts: list) -> dict:
    """Return sparse vectors with indices and values."""
    try:
        results = []
        for text in texts:
            sparse = self.model.encode_sparse(text)
            results.append({
                "indices": sparse.indices.tolist(),
                "values": sparse.values.tolist()
            })
        return {"ok": True, "embeddings": results}
    except Exception as e:
        return {"ok": False, "error": str(e)}
```

See `priv/barrel_embed/providers/splade.py` for a complete example.

### Image Embeddings (CLIP-style)

For multi-modal providers, add image handling:

```python
async def dispatch(self, request: dict) -> dict:
    action = request.get("action")

    if action == "info":
        return self.handle_info()
    elif action == "embed":
        texts = request.get("texts", [])
        return await self.handle_embed(texts)
    elif action == "embed_image":
        images = request.get("images", [])
        return await self.handle_embed_image(images)
    else:
        return {"ok": False, "error": f"Unknown action: {action}"}

async def handle_embed_image(self, images: list) -> dict:
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(self.executor, self.embed_image_sync, images)
    return result

def embed_image_sync(self, images: list) -> dict:
    """Embed base64-encoded images."""
    import base64
    from PIL import Image
    import io

    try:
        pil_images = []
        for img_b64 in images:
            img_bytes = base64.b64decode(img_b64)
            pil_images.append(Image.open(io.BytesIO(img_bytes)))

        embeddings = self.model.encode_images(pil_images)
        return {"ok": True, "embeddings": embeddings.tolist()}
    except Exception as e:
        return {"ok": False, "error": str(e)}
```

On the Erlang side, add image embedding functions:

```erlang
%% In your provider module
-export([embed_image/2, embed_image_batch/2]).

embed_image(ImageBase64, Config) ->
    case embed_image_batch([ImageBase64], Config) of
        {ok, [Vector]} -> {ok, Vector};
        {error, _} = E -> E
    end.

embed_image_batch(Images, #{server := Server, timeout := Timeout}) ->
    barrel_embed_port_server:embed_image_batch(Server, Images, Timeout);
embed_image_batch(_, _) ->
    {error, server_not_initialized}.
```

See `priv/barrel_embed/providers/clip.py` for a complete example.

### Multi-vector Embeddings (ColBERT-style)

For token-level embeddings:

```python
def embed_sync(self, texts: list) -> dict:
    """Return multiple vectors per text (one per token)."""
    try:
        results = []
        for text in texts:
            # Returns [num_tokens, dimension] matrix
            token_embeddings = self.model.encode_tokens(text)
            results.append(token_embeddings.tolist())
        return {"ok": True, "embeddings": results}
    except Exception as e:
        return {"ok": False, "error": str(e)}
```

See `priv/barrel_embed/providers/colbert.py` for a complete example.

## Best Practices

1. **Error messages**: Include enough context to debug issues
2. **Model validation**: Warn for unknown models but don't fail
3. **Timeout configuration**: Allow users to adjust for slow networks/large models
4. **Memory**: Be aware of model memory requirements
5. **Thread safety**: Most embedding models are thread-safe for inference, but verify
6. **Logging**: Log model loading, but not every request (too verbose)
