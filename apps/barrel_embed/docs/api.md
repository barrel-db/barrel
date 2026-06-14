# API Reference

## barrel_embed

Main embedding coordinator module.

### init/1

Initialize embedding state from configuration.

```erlang
-spec init(Config :: map()) -> {ok, State} | {ok, undefined} | {error, term()}.
```

**Config options:**

| Key | Type | Description |
|-----|------|-------------|
| `embedder` | `provider() \| [provider()]` | Provider or provider chain |
| `dimensions` | `pos_integer()` | Embedding dimension (default: 768) |
| `batch_size` | `pos_integer()` | Batch chunk size (default: 32) |

**Returns:**

- `{ok, State}` - Initialized state
- `{ok, undefined}` - No embedder configured
- `{error, Reason}` - Initialization failed

### embed/2

Generate embedding for a single text.

```erlang
-spec embed(Text :: binary(), State) -> {ok, [float()]} | {error, term()}.
```

### embed_batch/2

Generate embeddings for multiple texts.

```erlang
-spec embed_batch(Texts :: [binary()], State) -> {ok, [[float()]]} | {error, term()}.
```

### embed_batch/3

Generate embeddings with options.

```erlang
-spec embed_batch(Texts :: [binary()], Options :: map(), State) ->
    {ok, [[float()]]} | {error, term()}.
```

**Options:**

| Key | Type | Description |
|-----|------|-------------|
| `batch_size` | `pos_integer()` | Override batch chunk size |

### dimension/1

Get embedding dimension.

```erlang
-spec dimension(State) -> pos_integer() | undefined.
```

### info/1

Get provider information.

```erlang
-spec info(State) -> map().
```

**Returns:**

```erlang
#{
    configured => boolean(),
    providers => [#{module => atom(), name => atom()}],
    dimension => pos_integer()
}
```

---

## barrel_embed_provider

Provider behaviour and utilities.

### Behaviour Callbacks

```erlang
-callback embed(Text :: binary(), Config :: map()) ->
    {ok, [float()]} | {error, term()}.

-callback embed_batch(Texts :: [binary()], Config :: map()) ->
    {ok, [[float()]]} | {error, term()}.

-callback dimension(Config :: map()) -> pos_integer().

-callback name() -> atom().

%% Optional
-callback init(Config :: map()) -> {ok, map()} | {error, term()}.
-callback available(Config :: map()) -> boolean().
```

### call_embed/3

Call provider's embed function.

```erlang
-spec call_embed(Module :: atom(), Text :: binary(), Config :: map()) ->
    {ok, [float()]} | {error, term()}.
```

### call_embed_batch/3

Call provider's embed_batch function.

```erlang
-spec call_embed_batch(Module :: atom(), Texts :: [binary()], Config :: map()) ->
    {ok, [[float()]]} | {error, term()}.
```

### check_available/2

Check if provider is available.

```erlang
-spec check_available(Module :: atom(), Config :: map()) -> boolean().
```

---

## barrel_embed_splade

SPLADE sparse embedding provider.

### embed_sparse/2

Generate sparse embedding.

```erlang
-spec embed_sparse(Text :: binary(), Config :: map()) ->
    {ok, sparse_vector()} | {error, term()}.
```

**Returns:**

```erlang
#{indices => [non_neg_integer()], values => [float()]}
```

### embed_batch_sparse/2

Generate sparse embeddings for multiple texts.

```erlang
-spec embed_batch_sparse(Texts :: [binary()], Config :: map()) ->
    {ok, [sparse_vector()]} | {error, term()}.
```

---

## barrel_embed_colbert

ColBERT multi-vector embedding provider.

### embed_multi/2

Generate multi-vector embedding (one vector per token).

```erlang
-spec embed_multi(Text :: binary(), Config :: map()) ->
    {ok, [[float()]]} | {error, term()}.
```

### embed_batch_multi/2

Generate multi-vector embeddings for multiple texts.

```erlang
-spec embed_batch_multi(Texts :: [binary()], Config :: map()) ->
    {ok, [[[float()]]]} | {error, term()}.
```

### maxsim_score/2

Calculate MaxSim score between query and document.

```erlang
-spec maxsim_score(QueryVecs :: [[float()]], DocVecs :: [[float()]]) -> float().
```

---

## barrel_embed_clip

CLIP image/text embedding provider.

### embed_image/2

Generate embedding for a base64-encoded image.

```erlang
-spec embed_image(ImageBase64 :: binary(), Config :: map()) ->
    {ok, [float()]} | {error, term()}.
```

### embed_image_batch/2

Generate embeddings for multiple images.

```erlang
-spec embed_image_batch(Images :: [binary()], Config :: map()) ->
    {ok, [[float()]]} | {error, term()}.
```

---

## barrel_embed_port_server

Low-level gen_server for Python port communication with request multiplexing.

This module manages the Erlang-Python communication for embedding providers that use local Python models. It handles concurrent requests efficiently by assigning unique IDs and routing responses to the correct callers.

### start_link/3

Start the port server with a Python executable.

```erlang
-spec start_link(Python :: string(), Args :: [string()], Opts :: proplists:proplist()) ->
    {ok, pid()} | {error, term()}.
```

**Args** are passed to `python -m barrel_embed`.

**Opts:**

| Key | Type | Description |
|-----|------|-------------|
| `timeout` | `timeout()` | Default timeout in ms (default: 120000) |
| `priv_dir` | `string()` | Path to priv directory |

### embed_batch/3

Generate embeddings for texts.

```erlang
-spec embed_batch(Server :: pid(), Texts :: [binary()], Timeout :: timeout()) ->
    {ok, [[float()]]} | {error, term()}.
```

### embed_image_batch/3

Generate embeddings for base64-encoded images.

```erlang
-spec embed_image_batch(Server :: pid(), Images :: [binary()], Timeout :: timeout()) ->
    {ok, [[float()]]} | {error, term()}.
```

### embed_sparse_batch/3

Generate sparse embeddings (for SPLADE-style providers).

```erlang
-spec embed_sparse_batch(Server :: pid(), Texts :: [binary()], Timeout :: timeout()) ->
    {ok, [map()]} | {error, term()}.
```

Returns maps with `indices` and `values` keys.

### embed_multi_batch/3

Generate multi-vector embeddings (for ColBERT-style providers).

```erlang
-spec embed_multi_batch(Server :: pid(), Texts :: [binary()], Timeout :: timeout()) ->
    {ok, [[[float()]]]} | {error, term()}.
```

### info/2

Get model information.

```erlang
-spec info(Server :: pid(), Timeout :: timeout()) -> {ok, map()} | {error, term()}.
```

**Returns** a map containing:

| Key | Type | Description |
|-----|------|-------------|
| `dimensions` | `integer()` | Embedding dimensions |
| `model` | `binary()` | Model name |
| `backend` | `binary()` | Backend type |

### stop/1

Stop the server and close the port.

```erlang
-spec stop(Server :: pid()) -> ok.
```

