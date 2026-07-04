%%%-------------------------------------------------------------------
%%% @doc Test suite for conflict detection and resolution
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_conflict_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [
        {group, conflict_detection},
        {group, conflict_resolution},
        {group, mvcc}
    ].

groups() ->
    [
        {conflict_detection, [sequence], [
            no_conflict_single_rev,
            detect_conflict_after_put_rev,
            get_doc_with_conflicts_option,
            get_conflicts_api
        ]},
        {conflict_resolution, [sequence], [
            resolve_conflict_choose_winner,
            resolve_conflict_choose_loser,
            resolve_conflict_merge,
            resolve_conflict_no_conflicts_error,
            resolve_conflict_invalid_rev_error
        ]},
        {mvcc, [sequence], [
            mvcc_first_write_no_rev,
            mvcc_update_without_rev_rejected,
            mvcc_update_with_stale_rev_rejected,
            mvcc_update_with_current_rev_accepted
        ]}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(barrel_docdb),
    ok.

init_per_group(_Group, Config) ->
    %% Clean up any existing test database
    case barrel_docdb:open_db(<<"conflict_test_db">>) of
        {ok, _} -> barrel_docdb:delete_db(<<"conflict_test_db">>);
        _ -> ok
    end,
    DataDir = "/tmp/barrel_test_conflict",
    os:cmd("rm -rf " ++ DataDir),
    {ok, _} = barrel_docdb:create_db(<<"conflict_test_db">>, #{data_dir => DataDir}),
    Config.

end_per_group(_Group, _Config) ->
    barrel_docdb:delete_db(<<"conflict_test_db">>),
    ok.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

%%====================================================================
%% Conflict Detection Tests
%%====================================================================

no_conflict_single_rev(_Config) ->
    DbName = <<"conflict_test_db">>,
    DocId = <<"doc_no_conflict">>,

    %% Create a document
    Doc = #{<<"id">> => DocId, <<"value">> => 1},
    {ok, #{<<"rev">> := Rev1}} = barrel_docdb:put_doc(DbName, Doc),

    %% Get conflicts - should be empty
    {ok, Conflicts} = barrel_docdb:get_conflicts(DbName, DocId),
    ?assertEqual([], Conflicts),

    %% Get doc with conflicts option - should not have _conflicts field
    {ok, DocWithOpts} = barrel_docdb:get_doc(DbName, DocId, #{conflicts => true}),
    ?assertEqual(false, maps:is_key(<<"_conflicts">>, DocWithOpts)),

    %% Update the doc
    Doc2 = #{<<"id">> => DocId, <<"_rev">> => Rev1, <<"value">> => 2},
    {ok, _} = barrel_docdb:put_doc(DbName, Doc2),

    %% Still no conflicts
    {ok, Conflicts2} = barrel_docdb:get_conflicts(DbName, DocId),
    ?assertEqual([], Conflicts2),

    ok.

detect_conflict_after_put_rev(_Config) ->
    %% Conflicts can only be created through replication; the VV
    %% protocol and a rewritten test land with phase 3 step 3.
    {skip, revtree_removed}.

get_doc_with_conflicts_option(_Config) ->
    %% Needs replication-created conflicts; rewritten with the VV
    %% protocol in phase 3 step 3.
    {skip, revtree_removed}.

get_conflicts_api(_Config) ->
    %% Needs replication-created conflicts; rewritten with the VV
    %% protocol in phase 3 step 3.
    {skip, revtree_removed}.

%%====================================================================
%% Conflict Resolution Tests
%%====================================================================

resolve_conflict_choose_winner(_Config) ->
    %% Needs replication-created conflicts; rewritten with the VV
    %% protocol in phase 3 step 3.
    {skip, revtree_removed}.

resolve_conflict_choose_loser(_Config) ->
    %% Needs replication-created conflicts; rewritten with the VV
    %% protocol in phase 3 step 3.
    {skip, revtree_removed}.

resolve_conflict_merge(_Config) ->
    %% Needs replication-created conflicts; rewritten with the VV
    %% protocol in phase 3 step 3.
    {skip, revtree_removed}.

resolve_conflict_no_conflicts_error(_Config) ->
    DbName = <<"conflict_test_db">>,
    DocId = <<"doc_no_conflicts_to_resolve">>,

    %% Create a document without conflicts
    Doc = #{<<"id">> => DocId, <<"test">> => true},
    {ok, #{<<"rev">> := Rev}} = barrel_docdb:put_doc(DbName, Doc),

    %% Try to resolve - should fail with no_conflicts
    {error, no_conflicts} = barrel_docdb:resolve_conflict(DbName, DocId, Rev, {choose, Rev}),

    ok.

resolve_conflict_invalid_rev_error(_Config) ->
    %% Needs replication-created conflicts; rewritten with the VV
    %% protocol in phase 3 step 3.
    {skip, revtree_removed}.

%%====================================================================
%% MVCC strict _rev tests
%%====================================================================

mvcc_first_write_no_rev(_Config) ->
    DbName = <<"conflict_test_db">>,
    DocId = <<"mvcc_first">>,
    %% First write without _rev: accepted (no existing doc).
    {ok, _} = barrel_docdb:put_doc(DbName, #{<<"id">> => DocId, <<"v">> => 1}),
    ok.

mvcc_update_without_rev_rejected(_Config) ->
    DbName = <<"conflict_test_db">>,
    DocId = <<"mvcc_no_rev">>,
    {ok, _} = barrel_docdb:put_doc(DbName, #{<<"id">> => DocId, <<"v">> => 1}),
    %% Second write without _rev: must be rejected as conflict.
    ?assertEqual({error, conflict},
                 barrel_docdb:put_doc(DbName, #{<<"id">> => DocId, <<"v">> => 2})),
    ok.

mvcc_update_with_stale_rev_rejected(_Config) ->
    DbName = <<"conflict_test_db">>,
    DocId = <<"mvcc_stale">>,
    {ok, #{<<"rev">> := Rev1}} = barrel_docdb:put_doc(
        DbName, #{<<"id">> => DocId, <<"v">> => 1}),
    {ok, _} = barrel_docdb:put_doc(
        DbName, #{<<"id">> => DocId, <<"_rev">> => Rev1, <<"v">> => 2}),
    %% Using the old Rev1 now is stale.
    ?assertEqual({error, conflict},
                 barrel_docdb:put_doc(
                   DbName,
                   #{<<"id">> => DocId, <<"_rev">> => Rev1, <<"v">> => 3})),
    ok.

mvcc_update_with_current_rev_accepted(_Config) ->
    DbName = <<"conflict_test_db">>,
    DocId = <<"mvcc_current">>,
    {ok, #{<<"rev">> := Rev1}} = barrel_docdb:put_doc(
        DbName, #{<<"id">> => DocId, <<"v">> => 1}),
    {ok, #{<<"rev">> := Rev2}} = barrel_docdb:put_doc(
        DbName, #{<<"id">> => DocId, <<"_rev">> => Rev1, <<"v">> => 2}),
    true = Rev2 =/= Rev1,
    ok.
