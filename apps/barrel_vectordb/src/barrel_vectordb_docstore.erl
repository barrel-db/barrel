%% @doc Document backend behaviour for barrel_vectordb.
%%
%% A vector store keeps each vector's source text and metadata somewhere. By
%% default (`docstore => undefined') they live in the store's own RocksDB column
%% families, written in the same atomic batch as the vector. Configuring an
%% external docstore (for example {@link barrel_vectordb_docdb_backend}) routes
%% text and metadata elsewhere; the vector itself always stays local so the ANN
%% index can be rebuilt from it.
%%
%% External backends are written after the vector batch commits (vector-first,
%% doc-second), so a crash between the two can leave a vector without its text;
%% the vector column family stays authoritative for rebuild.
%% @end
-module(barrel_vectordb_docstore).

-callback init(Name :: atom(), Config :: map()) ->
    {ok, Ctx :: term()} | {error, term()}.

-callback put(Ctx :: term(), Id :: binary(), Text :: binary(), Metadata :: map()) ->
    ok | {error, term()}.

-callback multi_put(Ctx :: term(), [{binary(), binary(), map()}]) ->
    ok | {error, term()}.

-callback get(Ctx :: term(), Id :: binary()) ->
    {ok, Text :: binary(), Metadata :: map()} | not_found | {error, term()}.

-callback multi_get(Ctx :: term(), [binary()]) ->
    [{ok, binary(), map()} | not_found | {error, term()}].

-callback delete(Ctx :: term(), Id :: binary()) ->
    ok | {error, term()}.

-callback terminate(Ctx :: term()) -> ok.
