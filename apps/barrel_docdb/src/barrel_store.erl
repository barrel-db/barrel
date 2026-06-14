%%%-------------------------------------------------------------------
%%% @doc Storage behaviour for barrel_docdb
%%%
%%% Defines the interface for storage backends. The default implementation
%%% uses RocksDB (barrel_store_rocksdb).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_store).

%% Behaviour callbacks
-callback open(Path :: string(), Options :: map()) ->
    {ok, DbRef :: term()} | {error, term()}.

-callback close(DbRef :: term()) -> ok.

-callback put(DbRef :: term(), Key :: binary(), Value :: binary()) ->
    ok | {error, term()}.

-callback put(DbRef :: term(), Key :: binary(), Value :: binary(), Options :: list()) ->
    ok | {error, term()}.

-callback get(DbRef :: term(), Key :: binary()) ->
    {ok, binary()} | not_found | {error, term()}.

-callback delete(DbRef :: term(), Key :: binary()) ->
    ok | {error, term()}.

-callback write_batch(DbRef :: term(), Operations :: list()) ->
    ok | {error, term()}.

-callback fold(DbRef :: term(), Prefix :: binary(), Fun :: fun(), Acc :: term()) ->
    term().

-callback fold_range(DbRef :: term(), StartKey :: binary(), EndKey :: binary(),
                     Fun :: fun(), Acc :: term()) ->
    term().

%% Optional callbacks
-optional_callbacks([]).

%% API exports
-export([get_backend/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Get the configured storage backend module
-spec get_backend() -> module().
get_backend() ->
    application:get_env(barrel_docdb, default_store, barrel_store_rocksdb).
