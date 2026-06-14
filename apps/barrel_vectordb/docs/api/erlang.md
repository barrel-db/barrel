# Erlang API

Complete reference for the Barrel VectorDB Erlang API.

## Store Management

### start_link/1

Start a vector store.

```erlang
-spec start_link(Config) -> {ok, pid()} | {error, Reason} when
    Config :: #{
        name := atom(),
        path => string(),
        dimensions => pos_integer(),
        backend => hnsw | faiss,
        embedder => embedder_config(),
        hnsw => hnsw_opts(),
        batch => batch_opts()
    },
    Reason :: term().
```

**Example:**

```erlang
{ok, _} = barrel_vectordb:start_link(#{
    name => my_store,
    path => "/var/data/vectors",
    dimensions => 768,
    embedder => {local, #{}}
}).
```

### stop/1

Stop a vector store.

```erlang
-spec stop(Store) -> ok when Store :: atom().
```

### stats/1

Get store statistics.

```erlang
-spec stats(Store) -> {ok, Stats} when
    Store :: atom(),
    Stats :: #{
        count := non_neg_integer(),
        dimensions := pos_integer(),
        backend := atom()
    }.
```

## Document Operations

### add/4

Add a document with text (requires embedder).

```erlang
-spec add(Store, Id, Text, Metadata) -> ok | {error, Reason} when
    Store :: atom(),
    Id :: binary(),
    Text :: binary(),
    Metadata :: map(),
    Reason :: embedder_not_configured | term().
```

### add_vector/5

Add a document with pre-computed vector.

```erlang
-spec add_vector(Store, Id, Text, Metadata, Vector) -> ok | {error, Reason} when
    Store :: atom(),
    Id :: binary(),
    Text :: binary(),
    Metadata :: map(),
    Vector :: [float()],
    Reason :: term().
```

### add_batch/2

Add multiple documents in a batch.

```erlang
-spec add_batch(Store, Docs) -> {ok, Result} | {error, Reason} when
    Store :: atom(),
    Docs :: [{Id, Text, Metadata}],
    Id :: binary(),
    Text :: binary(),
    Metadata :: map(),
    Result :: #{inserted := non_neg_integer()},
    Reason :: term().
```

### get/2

Get a document by ID.

```erlang
-spec get(Store, Id) -> {ok, Doc} | not_found | {error, Reason} when
    Store :: atom(),
    Id :: binary(),
    Doc :: #{
        id := binary(),
        text := binary(),
        metadata := map()
    },
    Reason :: term().
```

### update/4

Update a document (requires embedder).

```erlang
-spec update(Store, Id, Text, Metadata) -> ok | {error, Reason} when
    Store :: atom(),
    Id :: binary(),
    Text :: binary(),
    Metadata :: map(),
    Reason :: not_found | embedder_not_configured | term().
```

### upsert/4

Insert or update a document (requires embedder).

```erlang
-spec upsert(Store, Id, Text, Metadata) -> ok | {error, Reason} when
    Store :: atom(),
    Id :: binary(),
    Text :: binary(),
    Metadata :: map(),
    Reason :: embedder_not_configured | term().
```

### delete/2

Delete a document.

```erlang
-spec delete(Store, Id) -> ok | {error, Reason} when
    Store :: atom(),
    Id :: binary(),
    Reason :: term().
```

### count/1

Count documents in store.

```erlang
-spec count(Store) -> non_neg_integer() when Store :: atom().
```

### peek/2

Sample documents from store.

```erlang
-spec peek(Store, N) -> {ok, Docs} when
    Store :: atom(),
    N :: pos_integer(),
    Docs :: [map()].
```

### checkpoint/1

Checkpoint HNSW index to disk.

```erlang
-spec checkpoint(Store) -> ok | {error, Reason} when
    Store :: atom(),
    Reason :: term().
```

## Search

### search/3

Search with text query (requires embedder).

```erlang
-spec search(Store, Query, Opts) -> {ok, Results} | {error, Reason} when
    Store :: atom(),
    Query :: binary(),
    Opts :: search_opts(),
    Results :: [search_result()],
    Reason :: embedder_not_configured | term().
```

### search_vector/3

Search with vector query.

```erlang
-spec search_vector(Store, Vector, Opts) -> {ok, Results} | {error, Reason} when
    Store :: atom(),
    Vector :: [float()],
    Opts :: search_opts(),
    Results :: [search_result()],
    Reason :: term().
```

### Search Options

```erlang
-type search_opts() :: #{
    k => pos_integer(),           %% Number of results (default: 5)
    filter => filter_fun(),       %% Metadata filter function
    include_text => boolean(),    %% Include text in results (default: true)
    include_metadata => boolean(),%% Include metadata (default: true)
    ef_search => pos_integer()    %% HNSW search width (default: max(k, 50))
}.

-type filter_fun() :: fun((Metadata :: map()) -> boolean()).

-type search_result() :: #{
    key := binary(),
    score := float(),
    text => binary(),
    metadata => map()
}.
```

## Embedding

### barrel_vectordb_embed:init/1

Initialize embedder.

```erlang
-spec init(Config) -> {ok, State} | {error, Reason} when
    Config :: #{embedder := embedder_config()}.
```

### barrel_vectordb_embed:embed/2

Embed text.

```erlang
-spec embed(Text, State) -> {ok, Vector} | {error, Reason} when
    Text :: binary(),
    Vector :: [float()].
```

### barrel_vectordb_embed:embed_batch/2

Embed multiple texts.

```erlang
-spec embed_batch(Texts, State) -> {ok, Vectors} | {error, Reason} when
    Texts :: [binary()],
    Vectors :: [[float()]].
```

## BM25

### barrel_vectordb_bm25:new/0

Create BM25 index.

```erlang
-spec new() -> index().
```

### barrel_vectordb_bm25:add/3

Add document to BM25 index.

```erlang
-spec add(Index, Id, Text) -> Index when
    Index :: index(),
    Id :: binary(),
    Text :: binary().
```

### barrel_vectordb_bm25:search/3

Search BM25 index.

```erlang
-spec search(Index, Query, K) -> {ok, Results} when
    Index :: index(),
    Query :: binary(),
    K :: pos_integer(),
    Results :: [{Id :: binary(), Score :: float()}].
```

## Reranking

### barrel_vectordb_rerank:init/1

Initialize reranker.

```erlang
-spec init(Config) -> {ok, Reranker} | {error, Reason} when
    Config :: #{model := binary()}.
```

### barrel_vectordb_rerank:rerank/3

Rerank documents.

```erlang
-spec rerank(Query, Docs, Reranker) -> {ok, Ranked} | {error, Reason} when
    Query :: binary(),
    Docs :: [binary()],
    Ranked :: [{Index :: non_neg_integer(), Score :: float()}].
```

### barrel_vectordb_rerank:stop/1

Stop reranker.

```erlang
-spec stop(Reranker) -> ok.
```
