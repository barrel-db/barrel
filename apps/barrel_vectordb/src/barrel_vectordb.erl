%%%-------------------------------------------------------------------
%%% @doc barrel_vectordb - Erlang Vector Database
%%%
%%% An Erlang library for storing and searching vectors. Supports optional
%%% embedding providers for text-to-vector conversion.
%%%
%%% == Quick Start (with embeddings) ==
%%% ```
%%% %% Start a store with local Python embeddings
%%% {ok, _} = barrel_vectordb:start_link(#{
%%%     name => my_store,
%%%     path => "/tmp/vectors",
%%%     embedder => {local, #{}}  %% requires Python + sentence-transformers
%%% }).
%%%
%%% %% Add a document (embeds the text automatically)
%%% ok = barrel_vectordb:add(my_store, <<"doc-1">>, <<"Hello world">>, #{}).
%%%
%%% %% Search with text query
%%% {ok, Results} = barrel_vectordb:search(my_store, <<"greetings">>, #{k => 5}).
%%% '''
%%%
%%% == Quick Start (vector-only, no embedder) ==
%%% ```
%%% %% Start a store without embedder
%%% {ok, _} = barrel_vectordb:start_link(#{
%%%     name => my_store,
%%%     path => "/tmp/vectors",
%%%     dimensions => 768
%%% }).
%%%
%%% %% Add with pre-computed vector
%%% ok = barrel_vectordb:add_vector(my_store, <<"doc-1">>, <<"Hello">>, #{}, Vector).
%%%
%%% %% Search with vector query
%%% {ok, Results} = barrel_vectordb:search_vector(my_store, QueryVector, #{k => 5}).
%%% '''
%%%
%%% == Configuration ==
%%% ```
%%% #{
%%%     name => atom(),              %% Store name (required)
%%%     path => string(),            %% RocksDB path
%%%     dimensions => pos_integer(), %% Vector dimensions (default: 768)
%%%     embedder => EmbedderConfig,  %% Embedding provider (optional)
%%%     hnsw => HnswConfig           %% HNSW index parameters
%%% }
%%% '''
%%%
%%% == Embedding Providers ==
%%%
%%% Embedder is **explicit** - if not configured, only `add_vector/5' and
%%% `search_vector/3' work. Text-based operations return `{error, embedder_not_configured}'.
%%%
%%% ```
%%% %% Local Python with sentence-transformers (CPU, no external calls)
%%% embedder => {local, #{
%%%     python => "python3",
%%%     model => "BAAI/bge-base-en-v1.5"
%%% }}
%%%
%%% %% Ollama (local LLM server)
%%% embedder => {ollama, #{
%%%     url => <<"http://localhost:11434">>,
%%%     model => <<"nomic-embed-text">>
%%% }}
%%%
%%% %% Provider chain with fallback
%%% embedder => [
%%%     {ollama, #{url => <<"http://localhost:11434">>}},
%%%     {local, #{}}  %% Fallback to CPU
%%% ]
%%% '''
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb).

%% API - Lifecycle
-export([
    start_link/1,
    stop/1
]).

%% API - Document Operations
-export([
    add/4,
    add/5,
    add_vector/5,
    add_batch/2,
    add_vector_batch/2,
    add_index_only/4,
    add_index_only_batch/2,
    get/2,
    update/4,
    upsert/4,
    delete/2,
    peek/2
]).

%% API - Search
-export([
    search/3,
    search_vector/3,
    search_bm25/3,
    search_hybrid/3
]).

%% API - Embedding
-export([
    embed/2,
    embed_batch/2
]).

%% API - Info
-export([
    stats/1,
    count/1,
    embedder_info/1
]).

%% API - Maintenance
-export([
    checkpoint/1,
    destroy/1
]).

%% Types
-type store() :: atom() | binary() | pid().
%% A store reference - a registered name (binary preferred; atoms are
%% normalized, so dynamic names should be binaries) or a pid.

-type id() :: binary().
%% Unique document identifier.

-type text() :: binary().
%% Document text content.

-type vector() :: [float()].
%% Embedding vector (list of floats).

-type metadata() :: #{atom() => term()}.
%% Arbitrary metadata map associated with a document.

-type search_opts() :: #{
    k => pos_integer(),
    filter => fun((metadata()) -> boolean()),
    include_text => boolean(),
    include_metadata => boolean(),
    ef_search => pos_integer(),
    %% Hybrid search options
    bm25_weight => float(),
    vector_weight => float(),
    fusion => rrf | linear
}.
%% Options for search operations.
%% - `k': Number of results to return (default: 5)
%% - `filter': Function to filter results by metadata
%% - `include_text': Include text in results (default: true)
%% - `include_metadata': Include metadata in results (default: true)
%% - `ef_search': Search width, higher = better recall (default: max(k, 50))

-type search_result() :: #{
    key := id(),
    text := text(),
    metadata := metadata(),
    score := float(),
    vector => vector()
}.
%% A single search result.

-type store_config() :: #{
    name := atom(),
    path => string() | binary(),
    db_path => string() | binary(),
    dimension => pos_integer(),
    dimensions => pos_integer(),
    embedder => embedder_config(),
    backend => hnsw | faiss | diskann,
    hnsw => hnsw_config(),
    faiss => faiss_config(),
    diskann => diskann_config(),
    %% BM25 options
    bm25_backend => memory | disk | none,
    bm25 => map(),
    bm25_disk => map(),
    %% Read-through document backend (record mode)
    docstore => {module(), map()},
    _ => _
}.
%% Store configuration options.

-type embedder_config() ::
    {local, map()} |
    {ollama, map()} |
    {openai, map()} |
    {anthropic, map()} |
    [{atom(), map()}].
%% Embedding provider configuration.

-type hnsw_config() :: #{
    m => pos_integer(),
    ef_construction => pos_integer(),
    distance_fn => cosine | euclidean
}.
%% HNSW index parameters.

-type faiss_config() :: #{
    index_type => binary(),
    nprobe => pos_integer()
}.
%% FAISS index parameters.

-type diskann_config() :: #{
    base_path => string() | binary(),
    l => pos_integer(),
    r => pos_integer()
}.
%% DiskANN index parameters.

-export_type([
    store/0, id/0, text/0, vector/0, metadata/0,
    search_opts/0, search_result/0,
    store_config/0, embedder_config/0, hnsw_config/0, faiss_config/0, diskann_config/0
]).

%%====================================================================
%% Lifecycle API
%%====================================================================

%% @doc Start a new vector store.
%%
%% Creates a new vector store with the given configuration. The store
%% is registered under the name provided in the config.
%%
%% == Example ==
%% ```
%% {ok, Pid} = barrel_vectordb:start_link(#{
%%     name => my_store,
%%     path => "/var/lib/myapp/vectors",
%%     dimensions => 768,
%%     embedder => {local, #{}}
%% }).
%% '''
%%
%% @param Config Store configuration map
%% @returns `{ok, Pid}' on success, `{error, Reason}' on failure
-spec start_link(store_config()) -> {ok, pid()} | {error, term()}.
start_link(Config) ->
    Name = maps:get(name, Config, barrel_vectordb_store),
    %% Normalize dimension/dimensions key
    Dimension = case {maps:get(dimension, Config, undefined), maps:get(dimensions, Config, undefined)} of
        {undefined, undefined} -> 768;
        {undefined, D} -> D;
        {D, _} -> D
    end,
    %% Normalize path/db_path key
    DbPath = case {maps:get(path, Config, undefined), maps:get(db_path, Config, undefined)} of
        {undefined, undefined} -> default_path(Name);
        {undefined, P} -> P;
        {P, _} -> P
    end,
    StoreConfig = maps:merge(#{
        db_path => DbPath,
        dimension => Dimension
    }, maps:without([name, dimensions, path], Config)),
    barrel_vectordb_server:start_link(Name, StoreConfig).

%% @doc Stop a vector store.
%%
%% Gracefully shuts down the store, persisting any pending data.
%%
%% @param Store Store name or pid
%% @returns `ok'
%% @doc Destroy a store: close every handle (RocksDB, index, disk
%% BM25) and remove the storage directory. The store stops.
-spec destroy(store()) -> ok | {error, term()}.
destroy(Store) ->
    {ok, DbPath} = barrel_vectordb_server:get_db_path(Store),
    ok = stop(Store),
    case file:del_dir_r(DbPath) of
        ok -> ok;
        {error, enoent} -> ok;
        {error, _} = Err -> Err
    end.

-spec stop(store()) -> ok.
stop(Store) ->
    barrel_vectordb_server:stop(Store).

%%====================================================================
%% Document Operations API
%%====================================================================

%% @doc Add a document with automatic embedding.
%%
%% Embeds the text using the configured provider and stores it in the
%% vector database along with its metadata.
%%
%% == Example ==
%% ```
%% ok = barrel_vectordb:add(my_store, <<"doc-1">>, <<"Hello world">>, #{
%%     type => greeting,
%%     language => english
%% }).
%% '''
%%
%% @param Store Store name or pid
%% @param Id Unique document identifier
%% @param Text The text to embed and store
%% @param Metadata Arbitrary metadata map
%% @returns `ok' on success, `{error, Reason}' on failure
-spec add(store(), id(), text(), metadata()) -> ok | {error, term()}.
add(Store, Id, Text, Metadata) ->
    barrel_vectordb_server:add(Store, Id, Text, Metadata).

%% @doc Add a document with explicit vector.
%%
%% Stores the document with a pre-computed embedding vector instead of
%% generating one automatically.
%%
%% == Example ==
%% ```
%% Vector = [0.1, 0.2, ...],  %% 768 dimensions
%% ok = barrel_vectordb:add(my_store, <<"doc-1">>, <<"Hello">>, #{}, Vector).
%% '''
%%
%% @param Store Store name or pid
%% @param Id Unique document identifier
%% @param Text The text content
%% @param Metadata Arbitrary metadata map
%% @param Vector Pre-computed embedding vector
%% @returns `ok' on success, `{error, Reason}' on failure
-spec add(store(), id(), text(), metadata(), vector()) -> ok | {error, term()}.
add(Store, Id, Text, Metadata, Vector) ->
    barrel_vectordb_server:add_vector(Store, Id, Text, Metadata, Vector).

%% @doc Add a document with pre-computed vector (alias).
%%
%% Same as `add/5', provided for API clarity.
%%
%% @see add/5
-spec add_vector(store(), id(), text(), metadata(), vector()) -> ok | {error, term()}.
add_vector(Store, Id, Text, Metadata, Vector) ->
    barrel_vectordb_server:add_vector(Store, Id, Text, Metadata, Vector).

%% @doc Add multiple documents in batch.
%%
%% Efficiently adds multiple documents with automatic embedding.
%% More efficient than calling `add/4' multiple times.
%%
%% == Example ==
%% ```
%% Docs = [
%%     {<<"id-1">>, <<"text 1">>, #{type => a}},
%%     {<<"id-2">>, <<"text 2">>, #{type => b}}
%% ],
%% {ok, #{inserted := 2}} = barrel_vectordb:add_batch(my_store, Docs).
%% '''
%%
%% @param Store Store name or pid
%% @param Docs List of `{Id, Text, Metadata}' tuples
%% @returns `{ok, Stats}' on success, `{error, Reason}' on failure
-spec add_batch(store(), [{id(), text(), metadata()}]) ->
    {ok, #{inserted := non_neg_integer()}} | {error, term()}.
add_batch(Store, Docs) ->
    barrel_vectordb_server:add_batch(Store, Docs).

%% @doc Add multiple documents with pre-computed vectors in batch.
%%
%% Efficiently adds multiple documents with their vectors in a single
%% atomic RocksDB write. Much faster than calling `add_vector/5' multiple times.
%%
%% == Example ==
%% ```
%% Docs = [
%%     {<<"id-1">>, <<"text 1">>, #{type => a}, Vector1},
%%     {<<"id-2">>, <<"text 2">>, #{type => b}, Vector2}
%% ],
%% {ok, #{inserted := 2}} = barrel_vectordb:add_vector_batch(my_store, Docs).
%% '''
%%
%% @param Store Store name or pid
%% @param Docs List of `{Id, Text, Metadata, Vector}' tuples
%% @returns `{ok, Stats}' on success, `{error, Reason}' on failure
-spec add_vector_batch(store(), [{id(), text(), metadata(), vector()}]) ->
    {ok, #{inserted := non_neg_integer()}} | {error, term()}.
add_vector_batch(Store, Docs) ->
    barrel_vectordb_server:add_vector_batch(Store, Docs).

%% @doc Index a vector without storing text or metadata.
%%
%% For callers that own document storage elsewhere (for example a
%% document database fronted by a `barrel_vectordb_docstore' adapter):
%% the vector is written to the vector column family in the atomic batch
%% (it stays authoritative for index rebuild on restart), `Text' feeds
%% the BM25 index transiently and is then dropped, and no text/metadata
%% is stored anywhere. Any stale text/metadata rows from a previous full
%% `add' are cleared in the same batch.
%%
%% Idempotent upsert by id: re-adding replaces the vector and the BM25
%% entry, so a crashed producer can safely re-drive the same entries.
%% Use {@link delete/2} to remove an entry.
%%
%% @param Store Store name or pid
%% @param Id Unique document identifier
%% @param Text Text for BM25 indexing only (`<<>>' skips BM25)
%% @param Vector Pre-computed embedding vector
%% @returns `ok' on success, `{error, Reason}' on failure
-spec add_index_only(store(), id(), text(), vector()) -> ok | {error, term()}.
add_index_only(Store, Id, Text, Vector) ->
    barrel_vectordb_server:add_index_only(Store, Id, Text, Vector).

%% @doc Index multiple vectors without storing text or metadata.
%%
%% Batch form of {@link add_index_only/4}: one atomic RocksDB write for
%% all vectors.
%%
%% @param Store Store name or pid
%% @param Entries List of `{Id, Text, Vector}' tuples
%% @returns `{ok, Stats}' on success, `{error, Reason}' on failure
-spec add_index_only_batch(store(), [{id(), text(), vector()}]) ->
    {ok, #{inserted := non_neg_integer()}} | {error, term()}.
add_index_only_batch(Store, Entries) ->
    barrel_vectordb_server:add_index_only_batch(Store, Entries).

%% @doc Get a document by ID.
%%
%% Retrieves a document with its vector, text, and metadata.
%%
%% == Example ==
%% ```
%% {ok, Doc} = barrel_vectordb:get(my_store, <<"doc-1">>),
%% Text = maps:get(text, Doc),
%% Meta = maps:get(metadata, Doc).
%% '''
%%
%% @param Store Store name or pid
%% @param Id Document identifier
%% @returns `{ok, Document}' if found, `not_found', or `{error, Reason}'
-spec get(store(), id()) -> {ok, map()} | not_found | {error, term()}.
get(Store, Id) ->
    barrel_vectordb_server:get(Store, Id).

%% @doc Delete a document.
%%
%% Removes a document from the store, including its vector and metadata.
%%
%% == Example ==
%% ```
%% ok = barrel_vectordb:delete(my_store, <<"doc-1">>).
%% '''
%%
%% @param Store Store name or pid
%% @param Id Document identifier
%% @returns `ok' on success, `{error, Reason}' on failure
-spec delete(store(), id()) -> ok | {error, term()}.
delete(Store, Id) ->
    barrel_vectordb_server:delete(Store, Id).

%% @doc Update a document.
%%
%% Updates an existing document by re-embedding the text and storing
%% the new text and metadata. Returns `not_found' if the document
%% does not exist.
%%
%% == Example ==
%% ```
%% ok = barrel_vectordb:update(my_store, <<"doc-1">>, <<"New text">>, #{updated => true}).
%% '''
%%
%% @param Store Store name or pid
%% @param Id Document identifier
%% @param Text New text to embed and store
%% @param Metadata New metadata map
%% @returns `ok' on success, `not_found', or `{error, Reason}'
-spec update(store(), id(), text(), metadata()) -> ok | not_found | {error, term()}.
update(Store, Id, Text, Metadata) ->
    barrel_vectordb_server:update(Store, Id, Text, Metadata).

%% @doc Insert or update a document.
%%
%% If the document exists, updates it. If not, inserts it.
%% Always succeeds (unless there's an error).
%%
%% == Example ==
%% ```
%% ok = barrel_vectordb:upsert(my_store, <<"doc-1">>, <<"Text">>, #{}).
%% '''
%%
%% @param Store Store name or pid
%% @param Id Document identifier
%% @param Text Text to embed and store
%% @param Metadata Metadata map
%% @returns `ok' on success, `{error, Reason}' on failure
-spec upsert(store(), id(), text(), metadata()) -> ok | {error, term()}.
upsert(Store, Id, Text, Metadata) ->
    barrel_vectordb_server:upsert(Store, Id, Text, Metadata).

%% @doc Peek at documents.
%%
%% Returns a sample of documents without performing a search.
%% Useful for inspecting the store contents.
%%
%% == Example ==
%% ```
%% {ok, Docs} = barrel_vectordb:peek(my_store, 10),
%% [#{key := K, text := T, metadata := M} | _] = Docs.
%% '''
%%
%% @param Store Store name or pid
%% @param Limit Maximum number of documents to return
%% @returns `{ok, Docs}' list of documents
-spec peek(store(), pos_integer()) -> {ok, [map()]}.
peek(Store, Limit) ->
    barrel_vectordb_server:peek(Store, Limit).

%%====================================================================
%% Search API
%%====================================================================

%% @doc Search for similar documents using text query.
%%
%% Embeds the query text and finds the most similar documents using
%% approximate nearest neighbor search (HNSW).
%%
%% == Example ==
%% ```
%% {ok, Results} = barrel_vectordb:search(my_store, <<"hello">>, #{
%%     k => 5,
%%     filter => fun(Meta) -> maps:get(type, Meta, undefined) =:= greeting end
%% }),
%% [#{key := Key, text := Text, score := Score} | _] = Results.
%% '''
%%
%% @param Store Store name or pid
%% @param Query Text to search for
%% @param Opts Search options
%% @returns `{ok, Results}' list of matching documents sorted by similarity
-spec search(store(), text(), search_opts()) ->
    {ok, [search_result()]} | {error, term()}.
search(Store, Query, Opts) when is_binary(Query) ->
    barrel_vectordb_server:search(Store, Query, Opts).

%% @doc Search for similar documents using a vector query.
%%
%% Finds the most similar documents to the given vector without
%% needing to embed a text query first.
%%
%% == Example ==
%% ```
%% QueryVector = [0.1, 0.2, ...],  %% 768 dimensions
%% {ok, Results} = barrel_vectordb:search_vector(my_store, QueryVector, #{k => 5}).
%% '''
%%
%% @param Store Store name or pid
%% @param Vector Query vector
%% @param Opts Search options
%% @returns `{ok, Results}' list of matching documents sorted by similarity
-spec search_vector(store(), vector(), search_opts()) ->
    {ok, [search_result()]} | {error, term()}.
search_vector(Store, Vector, Opts) when is_list(Vector) ->
    barrel_vectordb_server:search_vector(Store, Vector, Opts).

%% @doc Search for similar documents using BM25 text search.
%%
%% Performs keyword-based BM25 text search. Requires BM25 backend to be
%% enabled in the store configuration.
%%
%% == Example ==
%% ```
%% {ok, Results} = barrel_vectordb:search_bm25(my_store, <<"erlang programming">>, #{
%%     k => 10
%% }),
%% [{DocId, Score} | _] = Results.
%% '''
%%
%% @param Store Store name or pid
%% @param Query Text query for BM25 search
%% @param Opts Search options (k)
%% @returns `{ok, Results}' list of `{DocId, Score}' tuples sorted by BM25 score
-spec search_bm25(store(), text(), search_opts()) ->
    {ok, [{id(), float()}]} | {error, term()}.
search_bm25(Store, Query, Opts) when is_binary(Query) ->
    barrel_vectordb_server:search_bm25(Store, Query, Opts).

%% @doc Hybrid search combining BM25 and vector search.
%%
%% Performs both BM25 text search and vector similarity search,
%% then combines the results using a fusion algorithm (RRF or linear).
%%
%% == Example ==
%% ```
%% {ok, Results} = barrel_vectordb:search_hybrid(my_store, <<"erlang">>, #{
%%     k => 10,
%%     bm25_weight => 0.5,
%%     vector_weight => 0.5,
%%     fusion => rrf  %% or 'linear'
%% }).
%% '''
%%
%% Results carry `text' and `metadata' like vector search (disable with
%% `include_text => false' / `include_metadata => false'). Passing
%% `query_vector => Vector' skips the internal embedder for the vector
%% leg, so stores without an embedder can run hybrid search when the
%% caller embeds the query itself.
%%
%% @param Store Store name or pid
%% @param Query Text query (used for the BM25 leg)
%% @param Opts Search options (k, bm25_weight, vector_weight, fusion,
%%        rrf_k, query_vector, include_text, include_metadata)
%% @returns `{ok, Results}' list of result maps sorted by combined score
-spec search_hybrid(store(), text(), search_opts()) ->
    {ok, [search_result()]} | {error, term()}.
search_hybrid(Store, Query, Opts) when is_binary(Query) ->
    barrel_vectordb_server:search_hybrid(Store, Query, Opts).

%%====================================================================
%% Embedding API
%%====================================================================

%% @doc Generate embedding for a single text.
%%
%% Uses the store's configured embedding provider to generate a vector.
%% Useful when you need the vector for other purposes.
%%
%% == Example ==
%% ```
%% {ok, Vector} = barrel_vectordb:embed(my_store, <<"hello world">>),
%% 768 = length(Vector).
%% '''
%%
%% @param Store Store name or pid
%% @param Text Text to embed
%% @returns `{ok, Vector}' or `{error, Reason}'
-spec embed(store(), text()) -> {ok, vector()} | {error, term()}.
embed(Store, Text) ->
    barrel_vectordb_server:embed(Store, Text).

%% @doc Generate embeddings for multiple texts.
%%
%% More efficient than calling `embed/2' multiple times as it batches
%% the requests to the embedding provider.
%%
%% == Example ==
%% ```
%% {ok, Vectors} = barrel_vectordb:embed_batch(my_store, [<<"text 1">>, <<"text 2">>]),
%% 2 = length(Vectors).
%% '''
%%
%% @param Store Store name or pid
%% @param Texts List of texts to embed
%% @returns `{ok, Vectors}' or `{error, Reason}'
-spec embed_batch(store(), [text()]) -> {ok, [vector()]} | {error, term()}.
embed_batch(Store, Texts) ->
    barrel_vectordb_server:embed_batch(Store, Texts).

%%====================================================================
%% Info API
%%====================================================================

%% @doc Get store statistics.
%%
%% Returns information about the store including document count,
%% dimensions, and HNSW index statistics.
%%
%% == Example ==
%% ```
%% {ok, Stats} = barrel_vectordb:stats(my_store),
%% Count = maps:get(count, Stats),
%% Dims = maps:get(dimension, Stats).
%% '''
%%
%% @param Store Store name or pid
%% @returns `{ok, Stats}' map with store statistics
-spec stats(store()) -> {ok, map()}.
stats(Store) ->
    barrel_vectordb_server:stats(Store).

%% @doc Get document count.
%%
%% Returns the number of documents in the store.
%%
%% @param Store Store name or pid
%% @returns Document count
-spec count(store()) -> non_neg_integer().
count(Store) ->
    barrel_vectordb_server:count(Store).

%% @doc Get embedding provider information.
%%
%% Returns information about the configured embedding providers.
%%
%% == Example ==
%% ```
%% {ok, Info} = barrel_vectordb:embedder_info(my_store),
%% Dimension = maps:get(dimension, Info),
%% Providers = maps:get(providers, Info).
%% '''
%%
%% @param Store Store name or pid
%% @returns `{ok, Info}' map with embedder information
-spec embedder_info(store()) -> {ok, map()}.
embedder_info(Store) ->
    barrel_vectordb_server:embedder_info(Store).

%% @doc Checkpoint the HNSW index to disk.
%%
%% Persists the current in-memory HNSW index metadata to RocksDB.
%% This can speed up restart by avoiding a full index rebuild.
%%
%% Note: The index is automatically rebuilt from vectors on startup
%% if no checkpoint exists, so this is optional but improves restart time.
%%
%% == Example ==
%% ```
%% ok = barrel_vectordb:checkpoint(my_store).
%% '''
%%
%% @param Store Store name or pid
%% @returns `ok' on success
-spec checkpoint(store()) -> ok.
checkpoint(Store) ->
    barrel_vectordb_server:checkpoint(Store).

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private
%% Generate default database path for a store name
default_path(Name) when is_atom(Name) ->
    "priv/barrel_vectordb_" ++ atom_to_list(Name);
default_path(Name) when is_binary(Name) ->
    "priv/barrel_vectordb_" ++ binary_to_list(Name).
