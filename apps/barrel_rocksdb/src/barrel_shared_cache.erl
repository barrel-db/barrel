%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. Oct 2018 13:19
%%%-------------------------------------------------------------------
-module(barrel_shared_cache).
-author("benoitc").
-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([get_cache/0]).

-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  terminate/2
]).

-include("barrel_rocksdb.hrl").

-define(SERVER, ?MODULE).

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

get_cache() ->
  gen_server:call(?SERVER, get_cache).

init([]) ->
  {ok, Cache} = init_cache(),
  {ok, Cache}.


handle_call(get_cache, _From, Cache) ->
  {reply, Cache, Cache};
handle_call(_Msg, _From, Cache) ->
  ?LOG_WARNING("unkown call msg=~p~n", [_Msg, Cache]),
  {reply, ok, Cache}.

handle_cast(_Msg, Cache) ->
  {noreply, Cache}.

terminate(_Reason, Cache) ->
  rocksdb:release_cache(Cache).

cache_size() ->
  {ok, CacheSize} = barrel_lib:parse_information_unit(
    application:get_env(barrel, cache_size, ?CACHE)
  ),
  CacheSize.

init_cache() ->
  CacheSize = cache_size(),
  {ok, Cache} = rocksdb:new_lru_cache(CacheSize),
  ok = rocksdb:set_strict_capacity_limit(Cache, true),
  {ok, Cache}.
  
