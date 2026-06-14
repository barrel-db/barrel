%%%-------------------------------------------------------------------
%%% @doc HNSW backend wrapper for barrel_vectordb_index behaviour
%%%
%%% Thin wrapper around barrel_vectordb_hnsw that adapts its API to
%%% the barrel_vectordb_index behaviour specification.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_index_hnsw).
-behaviour(barrel_vectordb_index).

-include("barrel_vectordb.hrl").

%% barrel_vectordb_index callbacks
-export([
    new/1,
    insert/3,
    search/3,
    search/4,
    delete/2,
    size/1,
    info/1,
    serialize/1,
    deserialize/1
]).

%%====================================================================
%% barrel_vectordb_index callbacks
%%====================================================================

%% @doc Create a new HNSW index.
-spec new(map()) -> {ok, hnsw_index()} | {error, term()}.
new(Config) ->
    try
        Index = barrel_vectordb_hnsw:new(Config),
        {ok, Index}
    catch
        error:Reason -> {error, Reason}
    end.

%% @doc Insert a vector with the given ID.
-spec insert(hnsw_index(), binary(), [float()]) -> {ok, hnsw_index()} | {error, term()}.
insert(Index, Id, Vector) ->
    try
        NewIndex = barrel_vectordb_hnsw:insert(Index, Id, Vector),
        {ok, NewIndex}
    catch
        error:{invalid_dimension, Expected, Got} ->
            {error, {dimension_mismatch, Expected, Got}};
        error:Reason ->
            {error, Reason}
    end.

%% @doc Search for K nearest neighbors.
-spec search(hnsw_index(), [float()], pos_integer()) -> [{binary(), float()}].
search(Index, Query, K) ->
    barrel_vectordb_hnsw:search(Index, Query, K).

%% @doc Search for K nearest neighbors with options.
-spec search(hnsw_index(), [float()], pos_integer(), map()) -> [{binary(), float()}].
search(Index, Query, K, Opts) ->
    barrel_vectordb_hnsw:search(Index, Query, K, Opts).

%% @doc Delete a vector by ID.
-spec delete(hnsw_index(), binary()) -> {ok, hnsw_index()} | {error, term()}.
delete(Index, Id) ->
    try
        NewIndex = barrel_vectordb_hnsw:delete(Index, Id),
        {ok, NewIndex}
    catch
        error:Reason -> {error, Reason}
    end.

%% @doc Get the number of vectors in the index.
-spec size(hnsw_index()) -> non_neg_integer().
size(Index) ->
    barrel_vectordb_hnsw:size(Index).

%% @doc Get index information and statistics.
-spec info(hnsw_index()) -> map().
info(Index) ->
    BaseInfo = barrel_vectordb_hnsw:info(Index),
    BaseInfo#{backend => hnsw}.

%% @doc Serialize index to binary.
-spec serialize(hnsw_index()) -> binary().
serialize(Index) ->
    barrel_vectordb_hnsw:serialize(Index).

%% @doc Deserialize index from binary.
-spec deserialize(binary()) -> {ok, hnsw_index()} | {error, term()}.
deserialize(Binary) ->
    barrel_vectordb_hnsw:deserialize(Binary).
