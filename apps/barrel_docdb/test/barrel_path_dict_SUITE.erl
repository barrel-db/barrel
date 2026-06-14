%%%-------------------------------------------------------------------
%%% @doc Test suite for barrel_path_dict module
%%%
%%% Tests path ID interning functionality including ID generation,
%%% caching, persistence, and concurrent access.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_path_dict_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% CT callbacks
-export([all/0, groups/0, init_per_suite/1, end_per_suite/1]).
-export([init_per_group/2, end_per_group/2]).
-export([init_per_testcase/2, end_per_testcase/2]).

%% Test cases - lifecycle
-export([
    start_stop/1
]).

%% Test cases - path ID generation
-export([
    get_or_create_simple/1,
    consistent_ids/1,
    different_paths_different_ids/1,
    different_dbs_independent_ids/1,
    nested_paths/1,
    paths_with_values/1
]).

%% Test cases - caching
-export([
    cache_hit/1,
    get_id_from_cache/1,
    get_path_reverse_lookup/1,
    clear_cache/1
]).

%% Test cases - persistence
-export([
    load_from_store/1,
    persistence_across_cache_clear/1
]).

%% Test cases - concurrent access
-export([
    concurrent_get_or_create/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, lifecycle}, {group, path_id}, {group, caching},
     {group, persistence}, {group, concurrent}].

groups() ->
    [
        {lifecycle, [sequence], [
            start_stop
        ]},
        {path_id, [sequence], [
            get_or_create_simple,
            consistent_ids,
            different_paths_different_ids,
            different_dbs_independent_ids,
            nested_paths,
            paths_with_values
        ]},
        {caching, [sequence], [
            cache_hit,
            get_id_from_cache,
            get_path_reverse_lookup,
            clear_cache
        ]},
        {persistence, [sequence], [
            load_from_store,
            persistence_across_cache_clear
        ]},
        {concurrent, [sequence], [
            concurrent_get_or_create
        ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(lifecycle, Config) ->
    %% Ensure clean state - stop app if running
    application:stop(barrel_docdb),
    timer:sleep(50),
    Config;
init_per_group(_Group, Config) ->
    %% Start full application
    application:stop(barrel_docdb),
    timer:sleep(50),
    {ok, Apps} = application:ensure_all_started(barrel_docdb),
    %% Create a test database
    DbName = <<"path_dict_test_db">>,
    barrel_docdb:delete_db(DbName),
    {ok, Db} = barrel_docdb:create_db(DbName, #{}),
    {ok, StoreRef} = barrel_db_server:get_store_ref(Db),
    %% Reset path dict for clean state
    barrel_path_dict:reset(),
    [{started_apps, Apps}, {db_name, DbName}, {db, Db}, {store_ref, StoreRef} | Config].

end_per_group(lifecycle, _Config) ->
    ok;
end_per_group(_Group, Config) ->
    %% Cleanup test database
    DbName = ?config(db_name, Config),
    barrel_docdb:delete_db(DbName),
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
    %% Start path dict
    {ok, Pid} = barrel_path_dict:start_link(),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),

    %% Registered as barrel_path_dict
    ?assertEqual(Pid, whereis(barrel_path_dict)),

    %% Stop
    ok = gen_server:stop(Pid),
    ?assertNot(is_process_alive(Pid)),
    ?assertEqual(undefined, whereis(barrel_path_dict)),

    ok.

%%====================================================================
%% Test Cases - Path ID Generation
%%====================================================================

get_or_create_simple(Config) ->
    StoreRef = ?config(store_ref, Config),
    DbName = ?config(db_name, Config),

    %% Create a simple path ID
    Path = [<<"type">>],
    PathId = barrel_path_dict:get_or_create_id(StoreRef, DbName, Path),

    %% Should be a positive integer
    ?assert(is_integer(PathId)),
    ?assert(PathId > 0),

    ok.

consistent_ids(Config) ->
    StoreRef = ?config(store_ref, Config),
    DbName = ?config(db_name, Config),

    %% Same path should always return same ID
    Path = [<<"status">>],
    Id1 = barrel_path_dict:get_or_create_id(StoreRef, DbName, Path),
    Id2 = barrel_path_dict:get_or_create_id(StoreRef, DbName, Path),
    Id3 = barrel_path_dict:get_or_create_id(StoreRef, DbName, Path),

    ?assertEqual(Id1, Id2),
    ?assertEqual(Id2, Id3),

    ok.

different_paths_different_ids(Config) ->
    StoreRef = ?config(store_ref, Config),
    DbName = ?config(db_name, Config),

    %% Different paths should get different IDs
    Path1 = [<<"field1">>],
    Path2 = [<<"field2">>],
    Path3 = [<<"field3">>],

    Id1 = barrel_path_dict:get_or_create_id(StoreRef, DbName, Path1),
    Id2 = barrel_path_dict:get_or_create_id(StoreRef, DbName, Path2),
    Id3 = barrel_path_dict:get_or_create_id(StoreRef, DbName, Path3),

    ?assertNotEqual(Id1, Id2),
    ?assertNotEqual(Id2, Id3),
    ?assertNotEqual(Id1, Id3),

    ok.

different_dbs_independent_ids(Config) ->
    StoreRef = ?config(store_ref, Config),
    DbName = ?config(db_name, Config),

    %% Create another test database
    DbName2 = <<"path_dict_test_db2">>,
    barrel_docdb:delete_db(DbName2),
    {ok, Db2} = barrel_docdb:create_db(DbName2, #{}),
    {ok, StoreRef2} = barrel_db_server:get_store_ref(Db2),

    try
        %% Same path in different databases should get independent IDs
        Path = [<<"shared_field">>],
        Id1 = barrel_path_dict:get_or_create_id(StoreRef, DbName, Path),
        Id2 = barrel_path_dict:get_or_create_id(StoreRef2, DbName2, Path),

        %% Both should be valid IDs (though they might happen to be same)
        ?assert(is_integer(Id1)),
        ?assert(is_integer(Id2)),
        ?assert(Id1 > 0),
        ?assert(Id2 > 0)
    after
        barrel_docdb:delete_db(DbName2)
    end,

    ok.

nested_paths(Config) ->
    StoreRef = ?config(store_ref, Config),
    DbName = ?config(db_name, Config),

    %% Nested paths should work correctly
    Path1 = [<<"user">>, <<"name">>],
    Path2 = [<<"user">>, <<"email">>],
    Path3 = [<<"user">>, <<"address">>, <<"city">>],

    Id1 = barrel_path_dict:get_or_create_id(StoreRef, DbName, Path1),
    Id2 = barrel_path_dict:get_or_create_id(StoreRef, DbName, Path2),
    Id3 = barrel_path_dict:get_or_create_id(StoreRef, DbName, Path3),

    %% All should be different
    ?assertNotEqual(Id1, Id2),
    ?assertNotEqual(Id2, Id3),
    ?assertNotEqual(Id1, Id3),

    %% But consistent
    ?assertEqual(Id1, barrel_path_dict:get_or_create_id(StoreRef, DbName, Path1)),
    ?assertEqual(Id2, barrel_path_dict:get_or_create_id(StoreRef, DbName, Path2)),
    ?assertEqual(Id3, barrel_path_dict:get_or_create_id(StoreRef, DbName, Path3)),

    ok.

paths_with_values(Config) ->
    StoreRef = ?config(store_ref, Config),
    DbName = ?config(db_name, Config),

    %% Paths can include values at the end (for posting lists)
    Path1 = [<<"type">>, <<"user">>],
    Path2 = [<<"type">>, <<"admin">>],
    Path3 = [<<"status">>, <<"active">>],

    Id1 = barrel_path_dict:get_or_create_id(StoreRef, DbName, Path1),
    Id2 = barrel_path_dict:get_or_create_id(StoreRef, DbName, Path2),
    Id3 = barrel_path_dict:get_or_create_id(StoreRef, DbName, Path3),

    %% All should be different
    ?assertNotEqual(Id1, Id2),
    ?assertNotEqual(Id2, Id3),
    ?assertNotEqual(Id1, Id3),

    ok.

%%====================================================================
%% Test Cases - Caching
%%====================================================================

cache_hit(Config) ->
    StoreRef = ?config(store_ref, Config),
    DbName = ?config(db_name, Config),

    %% Create a path
    Path = [<<"cached_field">>],
    Id1 = barrel_path_dict:get_or_create_id(StoreRef, DbName, Path),

    %% Second call should hit cache (we can't directly test this,
    %% but we can verify it still works)
    Id2 = barrel_path_dict:get_or_create_id(StoreRef, DbName, Path),
    ?assertEqual(Id1, Id2),

    %% get_id should also work from cache
    {ok, CachedId} = barrel_path_dict:get_id(DbName, Path),
    ?assertEqual(Id1, CachedId),

    ok.

get_id_from_cache(Config) ->
    StoreRef = ?config(store_ref, Config),
    DbName = ?config(db_name, Config),

    %% Unknown path should return not_found
    ?assertEqual(not_found, barrel_path_dict:get_id(DbName, [<<"unknown_field">>])),

    %% Create a path
    Path = [<<"known_field">>],
    Id = barrel_path_dict:get_or_create_id(StoreRef, DbName, Path),

    %% Now it should be in cache
    ?assertEqual({ok, Id}, barrel_path_dict:get_id(DbName, Path)),

    ok.

get_path_reverse_lookup(Config) ->
    StoreRef = ?config(store_ref, Config),
    DbName = ?config(db_name, Config),

    %% Create a path
    Path = [<<"reverse_lookup_field">>],
    Id = barrel_path_dict:get_or_create_id(StoreRef, DbName, Path),

    %% Reverse lookup should work
    ?assertEqual(Path, barrel_path_dict:get_path(StoreRef, DbName, Id)),

    %% Unknown ID should return undefined
    ?assertEqual(undefined, barrel_path_dict:get_path(StoreRef, DbName, 999999)),

    ok.

clear_cache(Config) ->
    StoreRef = ?config(store_ref, Config),
    DbName = ?config(db_name, Config),

    %% Create some paths
    Path1 = [<<"clear_test_1">>],
    Path2 = [<<"clear_test_2">>],
    Id1 = barrel_path_dict:get_or_create_id(StoreRef, DbName, Path1),
    Id2 = barrel_path_dict:get_or_create_id(StoreRef, DbName, Path2),

    %% Verify they're in cache
    ?assertEqual({ok, Id1}, barrel_path_dict:get_id(DbName, Path1)),
    ?assertEqual({ok, Id2}, barrel_path_dict:get_id(DbName, Path2)),

    %% Clear cache for this database
    ok = barrel_path_dict:clear_cache(DbName),

    %% Cache should be empty
    ?assertEqual(not_found, barrel_path_dict:get_id(DbName, Path1)),
    ?assertEqual(not_found, barrel_path_dict:get_id(DbName, Path2)),

    %% But can still get IDs (from store)
    ?assertEqual(Id1, barrel_path_dict:get_or_create_id(StoreRef, DbName, Path1)),
    ?assertEqual(Id2, barrel_path_dict:get_or_create_id(StoreRef, DbName, Path2)),

    ok.

%%====================================================================
%% Test Cases - Persistence
%%====================================================================

load_from_store(Config) ->
    StoreRef = ?config(store_ref, Config),
    DbName = ?config(db_name, Config),

    %% Create some paths
    Path1 = [<<"persist_test_1">>],
    Path2 = [<<"persist_test_2">>],
    Id1 = barrel_path_dict:get_or_create_id(StoreRef, DbName, Path1),
    Id2 = barrel_path_dict:get_or_create_id(StoreRef, DbName, Path2),

    %% Clear cache
    ok = barrel_path_dict:clear_cache(DbName),

    %% Load from store
    ok = barrel_path_dict:load_from_store(StoreRef, DbName),

    %% Should be in cache again with same IDs
    ?assertEqual({ok, Id1}, barrel_path_dict:get_id(DbName, Path1)),
    ?assertEqual({ok, Id2}, barrel_path_dict:get_id(DbName, Path2)),

    ok.

persistence_across_cache_clear(Config) ->
    StoreRef = ?config(store_ref, Config),
    DbName = ?config(db_name, Config),

    %% Create a path
    Path = [<<"persistent_field">>],
    OriginalId = barrel_path_dict:get_or_create_id(StoreRef, DbName, Path),

    %% Clear cache and get again
    ok = barrel_path_dict:clear_cache(DbName),
    NewId = barrel_path_dict:get_or_create_id(StoreRef, DbName, Path),

    %% Should get the same ID (loaded from store)
    ?assertEqual(OriginalId, NewId),

    ok.

%%====================================================================
%% Test Cases - Concurrent Access
%%====================================================================

concurrent_get_or_create(Config) ->
    StoreRef = ?config(store_ref, Config),
    DbName = ?config(db_name, Config),

    %% Clear cache and reset for clean state
    barrel_path_dict:reset(),

    %% Same path accessed concurrently should get same ID
    Path = [<<"concurrent_field">>],
    NumProcs = 10,

    Parent = self(),
    Pids = [spawn_link(fun() ->
        Id = barrel_path_dict:get_or_create_id(StoreRef, DbName, Path),
        Parent ! {self(), Id}
    end) || _ <- lists:seq(1, NumProcs)],

    %% Collect results
    Results = [receive {Pid, Id} -> Id end || Pid <- Pids],

    %% All should be the same ID
    [FirstId | Rest] = Results,
    lists:foreach(fun(Id) ->
        ?assertEqual(FirstId, Id)
    end, Rest),

    %% Different paths accessed concurrently should get different IDs
    Paths = [[<<"concurrent_", (integer_to_binary(N))/binary>>] || N <- lists:seq(1, 10)],

    Pids2 = [spawn_link(fun() ->
        Id = barrel_path_dict:get_or_create_id(StoreRef, DbName, P),
        Parent ! {self(), {P, Id}}
    end) || P <- Paths],

    Results2 = [receive {Pid, Result} -> Result end || Pid <- Pids2],
    Ids = [Id || {_, Id} <- Results2],

    %% All IDs should be unique
    ?assertEqual(length(Ids), length(lists:usort(Ids))),

    ok.
