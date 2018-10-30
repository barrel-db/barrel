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


-include("barrel_rocksdb.hrl").

-define(DB_OPEN_RETRIES, 30).
-define(DB_OPEN_RETRY_DELAY, 2000).
-define(TAB, barrel_rocksdb).
-define(SERVER, barrel_rocksdb).

init_barrel(Id, Config) ->
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

hash_name(Name) ->
  Hash = crypto:hash(sha256, barrel_lib:to_binary(Name)),
  binary_to_list(barrel_lib:to_hex(Hash)).
  

local_name(Name) ->
  
  lists:flatten(["#rocksdb#", hash_name(Name)]).

path(Name, #{ path := Path }) ->
  case filelib:is_dir(Path) of
    true ->
      ?LOG_INFO("storage ~p is using directory ~p~n", [Name, Path]),
      Path;
    false ->
      ?LOG_INFO("error initializing storage ~p: directory ~p doesn't exist.~n", [Name, Path]),
      erlang:exit({storage_error, {directory_not_found, Path}})
  end;

path(Name, _) ->
  Dir = filename:join([barrel_config:get(data_dir), local_name(Name)]),
  ?LOG_INFO("storage ~p is using directory ~p~n", [Name, Dir]),
  filelib:ensure_dir(filename:join(Dir, "dummy")),
  Dir.



%% we hash the db name on the filesystem to not have any encoding issue.
db_name(Name) ->
  binary_to_list(
    barrel_lib:to_hex(crypto:hash(sha256, barrel_lib:to_binary(Name)))
  ).


%% TODO: make the cache configurable
init([Name, Config]) ->
  Cache = barrel_shared_cache:get_cache(),
  Path  = path(Name, Config),
  
  {ok, #{ path => Path, cache => Cache}}.


handle_call(get_barrel, )



