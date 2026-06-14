%%%-------------------------------------------------------------------
%%% @doc Vector index backend behaviour
%%%
%%% Defines the interface that vector index backends must implement.
%%% Supports both pure Erlang HNSW and FAISS (via NIF) backends.
%%%
%%% == Available Backends ==
%%% <ul>
%%%   <li>`hnsw' - Pure Erlang HNSW implementation (default)</li>
%%%   <li>`faiss' - Facebook FAISS via NIF binding (optional)</li>
%%% </ul>
%%%
%%% == Usage ==
%%% ```
%%% %% Get the module for a backend
%%% Mod = barrel_vectordb_index:backend_module(hnsw),
%%%
%%% %% Create an index
%%% {ok, Index} = Mod:new(#{dimension => 128}),
%%%
%%% %% Insert vectors
%%% {ok, Index2} = Mod:insert(Index, <<"doc1">>, [0.1, 0.2, ...]),
%%%
%%% %% Search
%%% Results = Mod:search(Index2, [0.1, 0.2, ...], 10).
%%% '''
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_index).

%% API
-export([
    backend_module/1,
    is_available/1
]).

%% Type exports
-export_type([index/0, backend/0]).

%%====================================================================
%% Types
%%====================================================================

-type index() :: term().
%% Opaque index type. Implementation depends on the backend.

-type backend() :: hnsw | faiss | diskann.
%% Available backend types.
%% Note: hybrid backend has been removed in favor of pure DiskANN

%%====================================================================
%% Behaviour Callbacks
%%====================================================================

%% @doc Create a new empty index.
%% Config may include:
%% - dimension: Vector dimension (required)
%% - distance_fn: cosine | euclidean (default: cosine)
%% - m: HNSW max connections per layer (default: 16)
%% - ef_construction: HNSW build-time beam width (default: 200)
-callback new(Config :: map()) -> {ok, index()} | {error, term()}.

%% @doc Insert a vector with the given ID.
%% Returns updated index on success.
-callback insert(Index :: index(), Id :: binary(), Vector :: [float()]) ->
    {ok, index()} | {error, term()}.

%% @doc Search for K nearest neighbors.
%% Returns list of {Id, Distance} tuples sorted by distance (ascending).
-callback search(Index :: index(), Query :: [float()], K :: pos_integer()) ->
    [{binary(), float()}].

%% @doc Search with options.
%% Options may include:
%% - ef_search: Search beam width (HNSW)
-callback search(Index :: index(), Query :: [float()], K :: pos_integer(), Opts :: map()) ->
    [{binary(), float()}].

%% @doc Delete a vector by ID.
%% Returns updated index on success.
-callback delete(Index :: index(), Id :: binary()) ->
    {ok, index()} | {error, term()}.

%% @doc Get the number of vectors in the index.
-callback size(Index :: index()) -> non_neg_integer().

%% @doc Get index information and statistics.
-callback info(Index :: index()) -> map().

%% @doc Serialize index to binary for persistence.
-callback serialize(Index :: index()) -> binary().

%% @doc Deserialize index from binary.
-callback deserialize(Binary :: binary()) -> {ok, index()} | {error, term()}.

%% @doc Close and release index resources.
%% Optional callback - only needed for backends with external resources (e.g., NIF).
-callback close(Index :: index()) -> ok.

-optional_callbacks([close/1]).

%%====================================================================
%% API Functions
%%====================================================================

%% @doc Get the implementation module for a backend.
%% Returns the module that implements the barrel_vectordb_index behaviour.
-spec backend_module(backend()) -> module().
backend_module(hnsw) -> barrel_vectordb_index_hnsw;
backend_module(faiss) -> barrel_vectordb_index_faiss;
backend_module(diskann) -> barrel_vectordb_diskann.

%% @doc Check if a backend is available.
%% HNSW and diskann are always available. FAISS requires barrel_faiss NIF.
-spec is_available(backend()) -> boolean().
is_available(hnsw) ->
    true;
is_available(diskann) ->
    true;
is_available(faiss) ->
    %% Check if barrel_faiss module is loaded/available
    case code:ensure_loaded(barrel_faiss) of
        {module, barrel_faiss} -> true;
        {error, _} -> false
    end.
