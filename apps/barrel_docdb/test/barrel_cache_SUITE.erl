%%%-------------------------------------------------------------------
%%% @doc Test suite for barrel_cache module
%%%
%%% Tests shared RocksDB block cache functionality including cache
%%% lifecycle, block options generation, and fallback behavior.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_cache_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% CT callbacks
-export([all/0, groups/0, init_per_suite/1, end_per_suite/1]).
-export([init_per_group/2, end_per_group/2]).
-export([init_per_testcase/2, end_per_testcase/2]).

%% Test cases - lifecycle
-export([
    start_stop/1,
    get_cache_handle/1
]).

%% Test cases - block options
-export([
    block_opts_defaults/1,
    block_opts_custom_bloom/1,
    block_opts_custom_block_size/1,
    block_opts_custom_cache_index/1,
    block_opts_includes_cache/1
]).

%% Test cases - fallback
-export([
    fallback_when_not_running/1,
    fallback_opts_structure/1
]).

%% Test cases - integration
-export([
    rocksdb_accepts_opts/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, lifecycle}, {group, block_opts}, {group, fallback}, {group, integration}].

groups() ->
    [
        {lifecycle, [sequence], [
            start_stop,
            get_cache_handle
        ]},
        {block_opts, [sequence], [
            block_opts_defaults,
            block_opts_custom_bloom,
            block_opts_custom_block_size,
            block_opts_custom_cache_index,
            block_opts_includes_cache
        ]},
        {fallback, [sequence], [
            fallback_when_not_running,
            fallback_opts_structure
        ]},
        {integration, [sequence], [
            rocksdb_accepts_opts
        ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(lifecycle, Config) ->
    %% Ensure clean state - stop application to prevent supervisor restart
    application:stop(barrel_docdb),
    _ = try gen_server:stop(barrel_cache) catch _:_ -> ok end,
    timer:sleep(100),
    Config;
init_per_group(block_opts, Config) ->
    %% Start full application to get supervised cache
    {ok, Apps} = application:ensure_all_started(barrel_docdb),
    [{started_apps, Apps} | Config];
init_per_group(fallback, Config) ->
    %% Ensure cache is NOT running for fallback tests
    application:stop(barrel_docdb),
    _ = try gen_server:stop(barrel_cache) catch _:_ -> ok end,
    timer:sleep(50),
    Config;
init_per_group(integration, Config) ->
    %% Start full application to get supervised cache
    {ok, Apps} = application:ensure_all_started(barrel_docdb),
    [{started_apps, Apps} | Config].

end_per_group(lifecycle, _Config) ->
    _ = try gen_server:stop(barrel_cache) catch _:_ -> ok end,
    ok;
end_per_group(block_opts, _Config) ->
    application:stop(barrel_docdb),
    ok;
end_per_group(fallback, _Config) ->
    ok;
end_per_group(integration, _Config) ->
    application:stop(barrel_docdb),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Test Cases - Lifecycle
%%====================================================================

start_stop(_Config) ->
    %% Start cache
    {ok, Pid} = barrel_cache:start_link(),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),

    %% Registered as barrel_cache
    ?assertEqual(Pid, whereis(barrel_cache)),

    %% Stop cache
    ok = gen_server:stop(Pid),
    ?assertNot(is_process_alive(Pid)),
    ?assertEqual(undefined, whereis(barrel_cache)),

    ok.

get_cache_handle(_Config) ->
    %% Start cache
    {ok, Pid} = barrel_cache:start_link(),

    %% Get cache handle
    {ok, Cache} = barrel_cache:get_cache(),
    ?assertNotEqual(undefined, Cache),

    %% Stop and cleanup
    ok = gen_server:stop(Pid),
    ok.

%%====================================================================
%% Test Cases - Block Options
%%====================================================================

block_opts_defaults(_Config) ->
    %% Get block options with defaults
    Opts = barrel_cache:get_block_opts(),

    %% Check default values
    ?assertEqual({bloom_filter, 10}, proplists:get_value(filter_policy, Opts)),
    ?assertEqual(false, proplists:get_value(whole_key_filtering, Opts)),
    ?assertEqual(4096, proplists:get_value(block_size, Opts)),
    ?assertEqual(true, proplists:get_value(cache_index_and_filter_blocks, Opts)),

    ok.

block_opts_custom_bloom(_Config) ->
    %% Custom bloom filter bits
    Opts = barrel_cache:get_block_opts(#{bloom_bits => 15}),

    ?assertEqual({bloom_filter, 15}, proplists:get_value(filter_policy, Opts)),
    %% Other defaults unchanged
    ?assertEqual(4096, proplists:get_value(block_size, Opts)),

    ok.

block_opts_custom_block_size(_Config) ->
    %% Custom block size
    Opts = barrel_cache:get_block_opts(#{block_size => 8192}),

    ?assertEqual(8192, proplists:get_value(block_size, Opts)),
    %% Other defaults unchanged
    ?assertEqual({bloom_filter, 10}, proplists:get_value(filter_policy, Opts)),

    ok.

block_opts_custom_cache_index(_Config) ->
    %% Disable cache index and filter blocks
    Opts = barrel_cache:get_block_opts(#{cache_index_and_filter => false}),

    ?assertEqual(false, proplists:get_value(cache_index_and_filter_blocks, Opts)),

    ok.

block_opts_includes_cache(_Config) ->
    %% Verify cache is running
    CachePid = whereis(barrel_cache),
    ct:pal("Cache PID: ~p", [CachePid]),
    ?assertNotEqual(undefined, CachePid),

    %% Get cache handle directly
    CacheResult = barrel_cache:get_cache(),
    ct:pal("Cache result: ~p", [CacheResult]),

    %% When cache is running, block_cache should be included
    Opts = barrel_cache:get_block_opts(),
    ct:pal("Block opts: ~p", [Opts]),

    %% Should have block_cache option
    BlockCache = proplists:get_value(block_cache, Opts),
    ct:pal("Block cache from opts: ~p", [BlockCache]),

    %% Check if cache was successfully created
    case CacheResult of
        {ok, ExpectedCache} ->
            ?assertNotEqual(undefined, BlockCache),
            ?assertEqual(ExpectedCache, BlockCache);
        {error, not_started} ->
            %% Cache creation failed (e.g., rocksdb not available)
            %% block_cache should be undefined in this case
            ?assertEqual(undefined, BlockCache)
    end,

    ok.

%%====================================================================
%% Test Cases - Fallback
%%====================================================================

fallback_when_not_running(_Config) ->
    %% Verify cache is not running
    ?assertEqual(undefined, whereis(barrel_cache)),

    %% get_block_opts should still work (fallback)
    Opts = barrel_cache:get_block_opts(),
    ?assert(is_list(Opts)),

    %% Should have bloom filter
    ?assertEqual({bloom_filter, 10}, proplists:get_value(filter_policy, Opts)),

    ok.

fallback_opts_structure(_Config) ->
    %% Verify cache is not running
    ?assertEqual(undefined, whereis(barrel_cache)),

    %% Fallback should return all expected options except block_cache
    Opts = barrel_cache:get_block_opts(#{bloom_bits => 12, block_size => 16384}),

    ?assertEqual({bloom_filter, 12}, proplists:get_value(filter_policy, Opts)),
    ?assertEqual(false, proplists:get_value(whole_key_filtering, Opts)),
    ?assertEqual(16384, proplists:get_value(block_size, Opts)),
    ?assertEqual(true, proplists:get_value(cache_index_and_filter_blocks, Opts)),

    %% block_cache should NOT be present in fallback mode
    ?assertEqual(undefined, proplists:get_value(block_cache, Opts)),

    ok.

%%====================================================================
%% Test Cases - Integration
%%====================================================================

rocksdb_accepts_opts(_Config) ->
    %% Test that RocksDB actually accepts the block options
    BlockOpts = barrel_cache:get_block_opts(),

    %% Create a temporary RocksDB database with these options
    TmpDir = filename:join(["/tmp", "barrel_cache_test_" ++ integer_to_list(erlang:system_time())]),
    ok = filelib:ensure_dir(TmpDir ++ "/"),

    DbOpts = [
        {create_if_missing, true},
        {block_based_table_options, BlockOpts}
    ],

    %% Should open successfully
    {ok, Db} = rocksdb:open(TmpDir, DbOpts),
    ?assertNotEqual(undefined, Db),

    %% Write and read a value to verify it works
    ok = rocksdb:put(Db, <<"key">>, <<"value">>, []),
    {ok, <<"value">>} = rocksdb:get(Db, <<"key">>, []),

    %% Cleanup
    ok = rocksdb:close(Db),
    ok = rocksdb:destroy(TmpDir, []),

    ok.
