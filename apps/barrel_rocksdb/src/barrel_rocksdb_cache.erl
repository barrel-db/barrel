%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 26. Nov 2018 05:35
%%%-------------------------------------------------------------------
-module(barrel_rocksdb_cache).
-author("benoitc").
-behaviour(gen_server).

-include("barrel_rocksdb.hrl").


%% API
-export([
  set_capacity/2,
  get_stats/1,
  get_cache/1,
  run_locked/2,
  start_link/2
]).


-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  terminate/2
]).


set_capacity(Cache, Capacity) when is_integer(Capacity) ->
  {ok, Sz}  = cache_size(Capacity),
  gen_server:call({via, gproc, ?rdb_cache(Cache)}, {set_capacity, Sz}).

get_stats(Cache) ->
  gen_server:call({via, gproc, ?rdb_cache(Cache)}, get_stats).

get_cache(Cache) ->
  gen_server:call({via, gproc, ?rdb_cache(Cache)}, get_cache).


run_locked(Cache, Fun) ->
  gen_server:call({via, gproc, ?rdb_cache(Cache)}, {run_locked, Fun}).


start_link(Name, CacheSize0) ->
  {ok, CacheSize} = cache_size(CacheSize0),
  gen_server:start_link({via, gproc, ?rdb_cache(Name)}, ?MODULE, [CacheSize], []).

%% gen_server callbacks

init([CacheSize]) ->
  {ok, Cache} = rocksdb:new_lru_cache(CacheSize),
  ok = rocksdb:set_strict_capacity_limit(Cache, true),
  {ok, Cache}.

handle_call({set_capacity, Capacity}, _From, Cache) ->
  ok = rocksdb:set_capacity(Cache, Capacity),
  {reply, ok, Cache};

handle_call(get_stats, _From, Cache) ->
  Stats = #{capacity => rocksdb:get_capacity(Cache),
            usage => rocksdb:get_usage(Cache),
            pinned_usage => rocksdb:get_pinned_usage(Cache)},
  {reply, Stats, Cache};

handle_call(get_cache, _From, Cache) ->
  {reply, {ok, Cache}, Cache};

handle_call({run_locked, Fun}, _From, Cache) ->
  {reply, run_locked_fun(Fun, Cache), Cache}.


handle_cast(_Msg, Cache) ->
  {noreply, Cache}.

terminate(_Reason, Cache) ->
  _ = rocksdb:release_cache(Cache),
  ok.


%% internal
cache_size(Sz) ->
  barrel_lib:parse_size_unit(Sz).


run_locked_fun(Fun, Cache) ->
  try
    Res = Fun(Cache),
    {ok, Res}
  catch
    _:Reason -> {error, Reason}
  end.