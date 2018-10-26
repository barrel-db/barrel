%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 27. Sep 2018 22:03
%%%-------------------------------------------------------------------
-module(barrel_store_rocksdb).
-author("benoitc").

-export([
  init_barrel/2,
  close_barrel/1
]).


-include("barrel_store_rocksdb.hrl").

-define(CACHE, 128 * 16#100000). % 128 Mib
-define(DB_OPEN_RETRIES, 30).
-define(DB_OPEN_RETRY_DELAY, 2000).
-define(TAB, barrel_rocksdb).
-define(SERVER, barrel_rocksdb).

init_barrel(Name, Config) ->
  {Shared, Cache} = maybe_init_cache(Config),
  Path = db_path(Name, Config),
  case barrel_store_rocksdb_base:open(Path, Cache) of
    {ok, Ref} ->
      #{ engine => ?MODULE, ref => Ref, cache => Cache, shared_cache => Shared };
    Error ->
      Error
  end.

close_barrel(#{ ref := Ref, cache := Cache, shared_cache := false }) ->
  rocksdb:release_cache(Cache),
  rocksdb:close(Ref);
close_barrel(#{ ref := Ref }) ->
  rocksdb:close(Ref).


db_path(Name, Config) ->
  filename:join(path(Config), db_name(Name)).


path(#{ path := Path }) ->
  filelib:ensure_dir(filename:join(Path, "dummy")),
  Path;
path(_) ->
  Dir = filename:join([barrel_config:get(data_dir), "#rocksdb"]),
  filelib:ensure_dir(filename:join(Dir, "dummy")),
  Dir.



%% we hash the db name on the filesystem to not have any encoding issue.
db_name(Name) ->
  binary_to_list(
    barrel_lib:to_hex(crypto:hash(sha256, barrel_lib:to_binary(Name)))
  ).

maybe_init_cache(#{ cache := Cache }) -> {true, Cache};
maybe_init_cache(Config) ->
  {ok, CacheSize} = cache_size(Config),
  {ok, Cache} = rocksdb:new_lru_cache(CacheSize),
  ok = rocksdb:set_strict_capacity_limit(Cache, true),
  {false, Cache}.


cache_size(#{ cache_size := Sz }) when is_integer(Sz) ->
  barrel_resource_monitor_misc:parse_information_unit(Sz);
cache_size(_) ->
  ?CACHE.