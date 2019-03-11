%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. Oct 2018 09:31
%%%-------------------------------------------------------------------
-module(barrel_config).
-author("benoitc").

%% API
-export([
  init/0,
  set/2,
  get/1, get/2
]).

-export([storage/0]).

-export([data_dir/0]).

-include("barrel.hrl").


storage() ->
  persistent_term:get({?MODULE, storage}).


get(Key) ->
  persistent_term:get({?MODULE, Key}).

get(Key, Default) ->
  try
    persistent_term:get({?MODULE, Key})
  catch
      error:badarg  -> Default
  end.

set(Key, Value) ->
  application:set_env(?APP, Key, Value),
  persistent_term:put({?MODULE, Key}, Value).

init() ->
  %% configure the logger module from the application config
  Logger = application:get_env(barrel, logger_module, logger),
  barrel_config:set('$barrel_logger', Logger),

  % Configure data dir
  DataDir = data_dir(),
  barrel_config:set(data_dir, DataDir),

  [env_or_default(Key, Default) ||
   {Key, Default} <- [
                      {fold_timeout, 5000},
                      {index_worker_threads, 128},
                      %% storage backend
                      {storage, barrel_rocksdb},
                      %% docs storage
                      {rocksdb_root_dir, DataDir},
                      {rocksdb_cache_size, 128 bsl 20}, %% 128 MB,
                      {rocksdb_write_buffer_size, 64 bsl 20} %% 64 MB
                     ]],

  ok.

data_dir() ->
  Default = filename:join([?DATA_DIR, node(), "barrel"]),
  Dir = application:get_env(barrel, data_dir, Default),
  _ = filelib:ensure_dir(filename:join([".", Dir, "dummy"])),
  Dir.

env_or_default(Key, Default) ->
  case application:get_env(barrel, Key) of
    {ok, Value} ->
      set(Key, Value);
    undefined ->
      set(Key, Default)
  end.
