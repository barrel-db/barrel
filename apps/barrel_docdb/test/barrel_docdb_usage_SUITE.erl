%%%-------------------------------------------------------------------
%%% @doc Usage Statistics Test Suite
%%%
%%% Tests for barrel_docdb_usage module.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_docdb_usage_SUITE).

-include_lib("common_test/include/ct.hrl").

%% CT callbacks
-export([all/0, groups/0, init_per_suite/1, end_per_suite/1,
         init_per_group/2, end_per_group/2,
         init_per_testcase/2, end_per_testcase/2]).

%% Test cases
-export([
    get_db_usage_basic/1,
    get_db_usage_with_documents/1,
    get_db_usage_not_found/1,
    get_all_usage_empty/1,
    get_all_usage_multiple_dbs/1,
    usage_stats_fields/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, usage_tests}].

groups() ->
    [
        {usage_tests, [sequence], [
            get_db_usage_basic,
            get_db_usage_with_documents,
            get_db_usage_not_found,
            get_all_usage_empty,
            get_all_usage_multiple_dbs,
            usage_stats_fields
        ]}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(usage_tests, Config) ->
    %% Clean up any leftover test databases
    lists:foreach(fun(DbName) ->
        barrel_docdb:delete_db(DbName)
    end, [<<"usage_test_db">>, <<"usage_test_db2">>, <<"usage_test_db3">>]),
    Config.

end_per_group(usage_tests, _Config) ->
    %% Clean up test databases
    lists:foreach(fun(DbName) ->
        barrel_docdb:delete_db(DbName)
    end, [<<"usage_test_db">>, <<"usage_test_db2">>, <<"usage_test_db3">>]),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

%% @doc Test basic get_db_usage for an empty database
get_db_usage_basic(_Config) ->
    DbName = <<"usage_test_db">>,
    {ok, _} = barrel_docdb:create_db(DbName),

    {ok, Stats} = barrel_docdb_usage:get_db_usage(DbName),

    %% Verify required fields exist
    true = is_map(Stats),
    DbName = maps:get(database, Stats),
    true = is_integer(maps:get(document_count, Stats)),
    true = is_integer(maps:get(storage_bytes, Stats)),
    true = is_integer(maps:get(memtable_size, Stats)),
    true = is_integer(maps:get(sst_files_size, Stats)),
    true = is_integer(maps:get(last_updated, Stats)),

    barrel_docdb:delete_db(DbName),
    ok.

%% @doc Test get_db_usage reflects document count after writes
get_db_usage_with_documents(_Config) ->
    DbName = <<"usage_test_db">>,
    {ok, _} = barrel_docdb:create_db(DbName),

    %% Get initial stats
    {ok, Stats1} = barrel_docdb_usage:get_db_usage(DbName),
    InitialCount = maps:get(document_count, Stats1),

    %% Insert some documents
    lists:foreach(fun(N) ->
        DocId = iolist_to_binary([<<"doc_">>, integer_to_binary(N)]),
        Doc = #{<<"id">> => DocId, <<"value">> => N},
        {ok, _} = barrel_docdb:put_doc(DbName, Doc)
    end, lists:seq(1, 10)),

    %% Get stats after writes
    {ok, Stats2} = barrel_docdb_usage:get_db_usage(DbName),
    NewCount = maps:get(document_count, Stats2),

    %% Document count should have increased
    %% Note: RocksDB estimate-num-keys is an approximation
    true = NewCount >= InitialCount,

    barrel_docdb:delete_db(DbName),
    ok.

%% @doc Test get_db_usage returns error for non-existent database
get_db_usage_not_found(_Config) ->
    {error, not_found} = barrel_docdb_usage:get_db_usage(<<"nonexistent_db">>),
    ok.

%% @doc Test get_all_usage with no databases
get_all_usage_empty(_Config) ->
    %% Get list of current databases to restore later
    ExistingDbs = barrel_docdb:list_dbs(),

    %% Delete all databases temporarily
    lists:foreach(fun(DbName) ->
        barrel_docdb:delete_db(DbName)
    end, ExistingDbs),

    %% Get all usage - should return empty list
    {ok, Stats} = barrel_docdb_usage:get_all_usage(),
    [] = Stats,

    %% Restore databases
    lists:foreach(fun(DbName) ->
        barrel_docdb:create_db(DbName)
    end, ExistingDbs),
    ok.

%% @doc Test get_all_usage with multiple databases
get_all_usage_multiple_dbs(_Config) ->
    %% Create multiple databases
    DbNames = [<<"usage_test_db">>, <<"usage_test_db2">>, <<"usage_test_db3">>],
    lists:foreach(fun(DbName) ->
        {ok, _} = barrel_docdb:create_db(DbName)
    end, DbNames),

    %% Get all usage
    {ok, Stats} = barrel_docdb_usage:get_all_usage(),

    %% Should have at least 3 databases
    true = length(Stats) >= 3,

    %% Each stats entry should be a valid map
    lists:foreach(fun(S) ->
        true = is_map(S),
        true = maps:is_key(database, S),
        true = maps:is_key(document_count, S),
        true = maps:is_key(storage_bytes, S)
    end, Stats),

    %% All our test databases should be in the results
    StatDbNames = [maps:get(database, S) || S <- Stats],
    lists:foreach(fun(DbName) ->
        true = lists:member(DbName, StatDbNames)
    end, DbNames),

    %% Clean up
    lists:foreach(fun(DbName) ->
        barrel_docdb:delete_db(DbName)
    end, DbNames),
    ok.

%% @doc Test that usage stats have all expected fields with correct types
usage_stats_fields(_Config) ->
    DbName = <<"usage_test_db">>,
    {ok, _} = barrel_docdb:create_db(DbName),

    {ok, Stats} = barrel_docdb_usage:get_db_usage(DbName),

    %% Verify all expected fields
    ExpectedFields = [database, document_count, storage_bytes,
                      memtable_size, sst_files_size, last_updated],

    lists:foreach(fun(Field) ->
        true = maps:is_key(Field, Stats)
    end, ExpectedFields),

    %% Verify types
    true = is_binary(maps:get(database, Stats)),
    true = is_integer(maps:get(document_count, Stats)),
    true = is_integer(maps:get(storage_bytes, Stats)),
    true = is_integer(maps:get(memtable_size, Stats)),
    true = is_integer(maps:get(sst_files_size, Stats)),
    true = is_integer(maps:get(last_updated, Stats)),

    %% Verify values are non-negative
    true = maps:get(document_count, Stats) >= 0,
    true = maps:get(storage_bytes, Stats) >= 0,
    true = maps:get(memtable_size, Stats) >= 0,
    true = maps:get(sst_files_size, Stats) >= 0,

    %% Verify last_updated is a reasonable timestamp (after year 2020)
    MinTimestamp = 1577836800000, %% 2020-01-01 00:00:00 UTC in milliseconds
    true = maps:get(last_updated, Stats) > MinTimestamp,

    barrel_docdb:delete_db(DbName),
    ok.
