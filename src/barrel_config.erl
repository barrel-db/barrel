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
                      {barrel_timestamp_file, default_timestamp_file()},

                      {fold_timeout, 5000},
                      {index_worker_threads, 128},

                      {server_max_memory, 8 bsl 30}, %% 8GB.

                      %% rate keeper
                      {metrics_update_rate, 100},

                      {transactions_per_bytes, 1000},

                      %% we recycle attachments every 60000 if they are not used.
                      {attachment_timeout, 60000},
                      %% number of attachments to keep open
                      {keep_attachment_file_num, 1000},

                      %% storage backend
                      {storage, barrel_rocksdb},
                      %% rocksdb storage
                      {rocksdb_root_dir, DataDir},
                      {rocksdb_cache_size, default_memory()},
                      {rocksdb_write_buffer_size, 64 bsl 20}, %% 64 MB
                      {rocksdb_ioq, index_worker_threads()},
                      {rocksdb_log_stats, false}
                     ]],

  ok.


default_memory() ->
  MaxSize = barrel_memory:get_total_memory(),
  %% reserve 1GB for system and binaries, and use 30% of the rest
  Mem = (MaxSize - 1024 * 1024) * 0.3,
  erlang:trunc(Mem).

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

index_worker_threads() ->
  erlang:max(10, erlang:system_info(dirty_io_schedulers)).

default_timestamp_file() ->
  FullPath = filename:join(barrel_config:get(data_dir), ?TS_FILE),
      ok = filelib:ensure_dir(FullPath),
  FullPath.
