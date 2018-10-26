%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. Oct 2018 11:31
%%%-------------------------------------------------------------------
-module(barrel_store_rocksdb_base).
-author("benoitc").

%% API
-export([
  open/2,
  close/1
]).

-include("barrel_store_rocksdb.hrl").

-define(DB_OPEN_RETRIES, 30).
-define(DB_OPEN_RETRY_DELAY, 2000).

open(Path, Cache) ->
  Retries = application:get_env(barrel, rocksdb_open_retries, ?DB_OPEN_RETRIES),
  CFOptions = init_cfs(Path, cf_options(Cache)),
  DbOptions = db_options(),
  open_db(Path, DbOptions, CFOptions, Retries, false).

close(Db) ->
  rocksdb:close(Db).


open_db(_Path, _DbOpts, _CfOpts, 0, LastError) ->
  {error, LastError};
open_db(Path, DbOpts, CfOpts, RetriesLeft, _LastError) ->
  case rocksdb:open_with_cf(Path, DbOpts, CfOpts) of
    OKResult = {ok, _, _} ->
      OKResult;
    %% Check specifically for lock error, this can be caused if
    %% a crashed instance takes some time to flush leveldb information
    %% out to disk.  The process is gone, but the NIF resource cleanup
    %% may not have completed.
    {error, {db_open, OpenErr}=Reason} ->
      case lists:prefix("IO error: lock ", OpenErr) of
        true ->
          SleepFor = application:get_env(barrel, db_open_retry_delay, ?DB_OPEN_RETRY_DELAY),
          _ = ?LOG_WARNING(
            "~s: barrel rocksdb backend retrying ~p in ~p ms after error ~s\n",
            [?MODULE, Path, SleepFor, OpenErr]
          ),
          timer:sleep(SleepFor),
          open_db(Path, DbOpts, CfOpts, RetriesLeft - 1, Reason);
        false ->
          {error, Reason}
      end;
    {error, _} = Error ->
      Error
  end.

db_options() ->
  [
    {create_if_missing, true},
    {create_missing_column_families, true},
    {max_open_files, 10000},
    {allow_concurrent_memtable_write, true},
    {enable_write_thread_adaptive_yield, true},
    {bytes_per_sync, 4 bsl 20}
  ].

all_cfs(Path) ->
  case rocksdb:list_column_families(Path, []) of
    {ok, Cfs} -> Cfs;
    _ -> ["default"]
  end.

cf_options(Cache) ->
  BlockOptions = [{block_cache, Cache}],
  [
    {block_based_table_options, BlockOptions},
    {write_buffer_size, 64 bsl 20}, %% 64MB
    {max_write_buffer_number, 4},
    {min_write_buffer_number_to_merge, 1},
    {level0_file_num_compaction_trigger, 2},
    {level0_slowdown_writes_trigger, 20},
    {level0_stop_writes_trigger, 32},

    {min_write_buffer_number_to_merge, 1},

    %%       level-size  file-size  max-files
    %% L1:      64 MB       4 MB         16
    %% L2:     640 MB       8 MB         80
    %% L3:    6.25 GB      16 MB        400
    %% L4:    62.5 GB      32 MB       2000
    %% L5:     625 GB      64 MB      10000
    %% L6:     6.1 TB     128 MB      50000
    %%
    {max_bytes_for_level_base, 64 bsl 20},

    {max_bytes_for_level_multiplier, 10},
    {target_file_size_base, 4 bsl 20}, %% 4MB
    {target_file_size_multiplier, 2},
    {compression, snappy},
    {prefix_extractor, {fixed_prefix_transform, 10}},
    {merge_operator, {bitset_merge_operator, 16000}}
  ].

init_cfs(Path, CfOptions) ->
  [{Name, cf_options(CfOptions)} || Name <- all_cfs(Path)].