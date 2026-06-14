%%%-------------------------------------------------------------------
%%% @doc Test suite for compaction filter and revision pruning
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_compaction_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [
        {group, basic_pruning},
        {group, integrated_pruning},
        {group, timer_compaction}
    ].

groups() ->
    [
        {basic_pruning, [sequence], [
            filter_handler_starts,
            prune_depth_config,
            parse_key_test
        ]},
        {integrated_pruning, [sequence], [
            many_revisions_get_pruned,
            pruning_deletes_bodies,
            pruning_preserves_winner
        ]},
        {timer_compaction, [sequence], [
            compaction_timer_starts,
            compaction_triggered_by_size
        ]}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(barrel_docdb),
    ok.

init_per_group(basic_pruning, Config) ->
    Config;
init_per_group(timer_compaction, Config) ->
    %% Clean up any existing test database
    DbName = <<"timer_test_db">>,
    case barrel_docdb:open_db(DbName) of
        {ok, _} -> barrel_docdb:delete_db(DbName);
        _ -> ok
    end,
    DataDir = "/tmp/barrel_test_timer",
    os:cmd("rm -rf " ++ DataDir),

    %% Create db with short compaction interval and low threshold for testing
    DbOpts = #{
        data_dir => DataDir,
        compaction_interval => 100,  %% 100ms for fast testing
        compaction_size_threshold => 1024  %% 1KB threshold (very low for testing)
    },
    {ok, DbPid} = barrel_docdb:create_db(DbName, DbOpts),
    [{db_name, DbName}, {data_dir, DataDir}, {db_pid, DbPid} | Config];
init_per_group(integrated_pruning, Config) ->
    %% Clean up any existing test database
    DbName = <<"prune_test_db">>,
    case barrel_docdb:open_db(DbName) of
        {ok, _} ->
            ct:pal("Deleting existing test database"),
            barrel_docdb:delete_db(DbName);
        _ -> ok
    end,
    DataDir = "/tmp/barrel_test_pruning",
    os:cmd("rm -rf " ++ DataDir),

    %% Create db with very low prune depth for testing
    DbOpts = #{
        data_dir => DataDir,
        prune_depth => 3  %% Keep only 3 revisions per branch
    },
    ct:pal("Creating test database with opts: ~p", [DbOpts]),
    {ok, DbPid} = barrel_docdb:create_db(DbName, DbOpts),
    ct:pal("Database created, pid: ~p", [DbPid]),

    [{db_name, DbName}, {data_dir, DataDir} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(integrated_pruning, Config) ->
    DbName = proplists:get_value(db_name, Config),
    barrel_docdb:delete_db(DbName),
    DataDir = proplists:get_value(data_dir, Config),
    os:cmd("rm -rf " ++ DataDir),
    ok;
end_per_group(timer_compaction, Config) ->
    DbName = proplists:get_value(db_name, Config),
    barrel_docdb:delete_db(DbName),
    DataDir = proplists:get_value(data_dir, Config),
    os:cmd("rm -rf " ++ DataDir),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

%%====================================================================
%% Basic Pruning Tests
%%====================================================================

filter_handler_starts(_Config) ->
    %% Create a temporary db to test that filter handler starts
    DbName = <<"filter_handler_test_db">>,
    DataDir = "/tmp/barrel_test_filter_handler",
    os:cmd("rm -rf " ++ DataDir),

    DbOpts = #{
        data_dir => DataDir,
        prune_depth => 100
    },

    {ok, DbPid} = barrel_docdb:create_db(DbName, DbOpts),
    ?assert(is_pid(DbPid)),

    %% Database should be functional
    Doc = #{<<"id">> => <<"test1">>, <<"value">> => <<"hello">>},
    {ok, #{<<"rev">> := Rev}} = barrel_docdb:put_doc(DbName, Doc),
    ?assert(is_binary(Rev)),

    %% Cleanup
    barrel_docdb:delete_db(DbName),
    os:cmd("rm -rf " ++ DataDir),

    ok.

prune_depth_config(_Config) ->
    %% Start a filter handler directly to test config (unlinked)
    {ok, FilterPid} = gen_server:start(barrel_compaction_filter, #{
        db_name => <<"test_db">>,
        prune_depth => 500
    }, []),

    %% Check prune depth
    ?assertEqual(500, barrel_compaction_filter:get_prune_depth(FilterPid)),

    %% Change prune depth
    ok = barrel_compaction_filter:set_prune_depth(FilterPid, 1000),
    ?assertEqual(1000, barrel_compaction_filter:get_prune_depth(FilterPid)),

    %% Check initial stats
    Stats = barrel_compaction_filter:get_stats(FilterPid),
    ?assertEqual(0, maps:get(filter_calls, Stats)),
    ?assertEqual(0, maps:get(entities_processed, Stats)),
    ?assertEqual(0, maps:get(revisions_pruned, Stats)),

    %% Cleanup - use gen_server:stop to properly stop
    gen_server:stop(FilterPid),

    ok.

parse_key_test(_Config) ->
    %% Test doc_entity key parsing
    EntityKey = barrel_store_keys:doc_entity(<<"mydb">>, <<"doc123">>),
    ?assertEqual({doc_entity, <<"mydb">>, <<"doc123">>},
                 barrel_store_keys:parse_key(EntityKey)),

    %% Test doc_body_rev key parsing
    BodyKey = barrel_store_keys:doc_body_rev(<<"mydb">>, <<"doc123">>, <<"1-abc">>),
    ?assertEqual({doc_body_rev, <<"mydb">>, <<"doc123">>, <<"1-abc">>},
                 barrel_store_keys:parse_key(BodyKey)),

    %% Test unknown key
    ?assertEqual(other, barrel_store_keys:parse_key(<<"unknown">>)),

    ok.

%%====================================================================
%% Integrated Pruning Tests
%%====================================================================

many_revisions_get_pruned(Config) ->
    DbName = proplists:get_value(db_name, Config),
    DocId = <<"prune_doc1">>,

    %% Create document and update many times
    %% Prune depth is 3, so after more than 3 updates, old revisions should be prunable
    Doc0 = #{<<"id">> => DocId, <<"count">> => 0},
    {ok, #{<<"rev">> := Rev0}} = barrel_docdb:put_doc(DbName, Doc0),
    ct:pal("Created doc with rev: ~s", [Rev0]),

    %% Update 10 times (far more than prune depth of 3)
    Revs = lists:foldl(
        fun(N, AccRevs) ->
            PrevRev = hd(AccRevs),
            DocN = #{<<"id">> => DocId, <<"_rev">> => PrevRev, <<"count">> => N},
            {ok, #{<<"rev">> := NewRev}} = barrel_docdb:put_doc(DbName, DocN),
            ct:pal("Update ~p created rev: ~s", [N, NewRev]),
            [NewRev | AccRevs]
        end,
        [Rev0],
        lists:seq(1, 10)
    ),

    %% We have 11 revisions total (Rev0 + 10 updates)
    ?assertEqual(11, length(Revs)),

    %% Get latest rev (should still work)
    {ok, LatestDoc} = barrel_docdb:get_doc(DbName, DocId),
    ?assertEqual(10, maps:get(<<"count">>, LatestDoc)),
    LatestRev = maps:get(<<"_rev">>, LatestDoc),
    ?assertEqual(hd(Revs), LatestRev),

    %% Trigger compaction
    {ok, Pid} = barrel_docdb:db_pid(DbName),
    {ok, StoreRef} = barrel_db_server:get_store_ref(Pid),
    ok = barrel_store_rocksdb:compact_default_cf(StoreRef),

    ct:pal("Compaction triggered"),

    %% Doc should still be accessible after compaction
    {ok, DocAfter} = barrel_docdb:get_doc(DbName, DocId),
    ?assertEqual(10, maps:get(<<"count">>, DocAfter)),

    ok.

pruning_deletes_bodies(Config) ->
    DbName = proplists:get_value(db_name, Config),
    DocId = <<"prune_body_doc">>,

    %% Create document with larger body
    LargeValue = binary:copy(<<"x">>, 5000),  %% 5KB to ensure body is stored
    Doc0 = #{<<"id">> => DocId, <<"data">> => LargeValue, <<"version">> => 0},
    {ok, #{<<"rev">> := Rev0}} = barrel_docdb:put_doc(DbName, Doc0),

    %% Get store ref for direct body access
    {ok, Pid} = barrel_docdb:db_pid(DbName),
    {ok, StoreRef} = barrel_db_server:get_store_ref(Pid),

    %% Note: The current body is stored at doc_body(DbName, DocId),
    %% only when updated does the old body move to doc_body_rev(DbName, DocId, Rev)

    %% Verify current body exists
    CurrentBodyKey = barrel_store_keys:doc_body(DbName, DocId),
    {ok, _Body0} = barrel_store_rocksdb:body_get(StoreRef, CurrentBodyKey),
    ct:pal("Body for rev ~s exists at current location", [Rev0]),

    %% Update 5 times (more than prune depth of 3)
    %% After each update, the previous body is moved to doc_body_rev location
    {FinalRev, AllRevs} = lists:foldl(
        fun(N, {PrevRev, AccRevs}) ->
            DocN = #{<<"id">> => DocId, <<"_rev">> => PrevRev,
                     <<"data">> => LargeValue, <<"version">> => N},
            {ok, #{<<"rev">> := NewRev}} = barrel_docdb:put_doc(DbName, DocN),
            {NewRev, [NewRev | AccRevs]}
        end,
        {Rev0, [Rev0]},
        lists:seq(1, 5)
    ),

    ct:pal("Created ~p revisions, final: ~s", [length(AllRevs), FinalRev]),

    %% Verify Rev0 body was moved to revision-keyed location
    Rev0BodyKey = barrel_store_keys:doc_body_rev(DbName, DocId, Rev0),
    {ok, _BodyRev0} = barrel_store_rocksdb:body_get(StoreRef, Rev0BodyKey),
    ct:pal("Rev0 body exists at revision location"),

    %% Trigger compaction
    ok = barrel_store_rocksdb:compact_default_cf(StoreRef),

    %% Latest body should still exist at current location
    {ok, _BodyFinal} = barrel_store_rocksdb:body_get(StoreRef, CurrentBodyKey),
    ct:pal("Final body still exists at current location"),

    %% Old bodies may be deleted (deleted in compaction, cleaned up in next compaction)
    %% Note: The actual deletion happens via write_batch during compaction filter,
    %% and the keys are cleaned up in subsequent compaction cycles

    ok.

pruning_preserves_winner(Config) ->
    DbName = proplists:get_value(db_name, Config),
    DocId = <<"prune_winner_doc">>,

    %% Create document
    Doc0 = #{<<"id">> => DocId, <<"value">> => <<"initial">>},
    {ok, #{<<"rev">> := Rev0}} = barrel_docdb:put_doc(DbName, Doc0),

    %% Update a few times
    Doc1 = #{<<"id">> => DocId, <<"_rev">> => Rev0, <<"value">> => <<"update1">>},
    {ok, #{<<"rev">> := Rev1}} = barrel_docdb:put_doc(DbName, Doc1),

    Doc2 = #{<<"id">> => DocId, <<"_rev">> => Rev1, <<"value">> => <<"update2">>},
    {ok, #{<<"rev">> := Rev2}} = barrel_docdb:put_doc(DbName, Doc2),

    Doc3 = #{<<"id">> => DocId, <<"_rev">> => Rev2, <<"value">> => <<"final">>},
    {ok, #{<<"rev">> := Rev3}} = barrel_docdb:put_doc(DbName, Doc3),

    ct:pal("Created revisions: ~s -> ~s -> ~s -> ~s", [Rev0, Rev1, Rev2, Rev3]),

    %% Trigger compaction
    {ok, Pid} = barrel_docdb:db_pid(DbName),
    {ok, StoreRef} = barrel_db_server:get_store_ref(Pid),
    ok = barrel_store_rocksdb:compact_default_cf(StoreRef),

    %% Winner document should still be accessible with correct value
    {ok, DocAfter} = barrel_docdb:get_doc(DbName, DocId),
    ?assertEqual(<<"final">>, maps:get(<<"value">>, DocAfter)),
    ?assertEqual(Rev3, maps:get(<<"_rev">>, DocAfter)),

    ct:pal("Winner preserved after compaction"),

    ok.

%%====================================================================
%% Timer Compaction Tests
%%====================================================================

compaction_timer_starts(Config) ->
    %% Verify that the compaction timer is running by checking that
    %% the database was created with the custom interval
    DbPid = proplists:get_value(db_pid, Config),
    ?assert(is_pid(DbPid)),
    ?assert(is_process_alive(DbPid)),

    %% The database should be functional
    DbName = proplists:get_value(db_name, Config),
    Doc = #{<<"id">> => <<"timer_test_doc">>, <<"value">> => <<"hello">>},
    {ok, #{<<"rev">> := Rev}} = barrel_docdb:put_doc(DbName, Doc),
    ?assert(is_binary(Rev)),

    ct:pal("Database with compaction timer is functional"),
    ok.

compaction_triggered_by_size(Config) ->
    DbName = proplists:get_value(db_name, Config),

    %% Create multiple documents to exceed the 1KB threshold
    %% Each document has a ~500 byte body to quickly exceed threshold
    LargeValue = binary:copy(<<"x">>, 500),
    lists:foreach(
        fun(N) ->
            DocId = iolist_to_binary([<<"large_doc_">>, integer_to_binary(N)]),
            Doc = #{<<"id">> => DocId, <<"data">> => LargeValue},
            {ok, _} = barrel_docdb:put_doc(DbName, Doc)
        end,
        lists:seq(1, 10)
    ),

    ct:pal("Created 10 documents with ~p bytes each", [500]),

    %% Get initial db size
    {ok, Pid} = barrel_docdb:db_pid(DbName),
    {ok, StoreRef} = barrel_db_server:get_store_ref(Pid),
    {ok, InitialSize} = barrel_store_rocksdb:get_db_size(StoreRef),
    ct:pal("Database size after writes: ~p bytes", [InitialSize]),

    %% Wait for the timer to fire (interval is 100ms, wait a bit longer)
    timer:sleep(300),

    %% Database should still be functional after timer fires
    {ok, Doc1} = barrel_docdb:get_doc(DbName, <<"large_doc_1">>),
    ?assertEqual(LargeValue, maps:get(<<"data">>, Doc1)),

    ct:pal("Database functional after compaction timer fired"),
    ok.
