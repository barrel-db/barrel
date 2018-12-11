%%% Copyright (c) 2018. Benoit Chesneau
%%
%% Licensed under the Apache License, Version 2.0 (the "License"); you may not
%% use this file except in compliance with the License. You may obtain a copy of
%% the License at
%%
%%    http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
%% License for the specific language governing permissions and limitations under
%% the License.

%% @doc handle rocksdb options
-module(barrel_rocksdb_options).
-author("benoitc").

%% API
-export([
  db_options/1
]).


db_options(Options) ->
  default_db_options() ++ cf_options(Options).




default_db_options() ->
  [
    {create_if_missing, true},
    {create_missing_column_families, true},
    {max_open_files, 10000},
    {allow_concurrent_memtable_write, true},
    {enable_write_thread_adaptive_yield, true},

    %% Periodically sync both the WAL and SST writes to smooth out disk
    %% usage. Not performing such syncs can be faster but can cause
    %% performance blips when the OS decides it needs to flush data.
    {wal_bytes_per_sync, 512 bsl 10},  %% 512 KB
    {bytes_per_sync, 512 bsl 10}, %% 512 KB,

    %% Because we open a long running rocksdb instance, we do not want the
    %% manifest file to grow unbounded. Assuming each manifest entry is about 1
    %% KB, this allows for 128 K entries. This could account for several hours to
    %% few months of runtime without rolling based on the workload.
    {max_manifest_file_size, 128 bsl 20} %% 128 MB

  ].

cf_options(Options) ->
  WriteBufferSize =  64 bsl 20, %% 64 MB
  CfOpts = [
    {write_buffer_size, WriteBufferSize}, %% 64MB
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
    {merge_operator, counter_merge_operator}
  ],

  case maps:get(cache, Options, false) of
    false -> CfOpts;
    Cache ->
      %% Reserve 1 memtable worth of memory from the cache. Under high
      %% load situations we'll be using somewhat more than 1 memtable
      %% but usually not significantly more unless there is an I/O
      %% throughput problem.
      %%
      %% We ensure that at least 1MB is allocated for the block cache.
      %% Some unit tests expect to see a non-zero block cache hit rate,
      %% but they use a cache that is small enough that all of it would
      %% otherwise be reserved for the memtable.
      {ok, CacheRef} = barrel_rocksdb_cache:run_locked(
        Cache,
        fun(CacheRef) ->
          Capacity = rocksdb:get_capacity(CacheRef),
          NewCapacity = erlang:max(1 bsl 20, Capacity - WriteBufferSize),
          ok = rocksdb:set_capacity(CacheRef, NewCapacity),
          Cache
        end
      ),
      BlockOptions = [{block_cache, CacheRef}],
      CfOpts ++ [{block_based_table_options, BlockOptions}]
  end.