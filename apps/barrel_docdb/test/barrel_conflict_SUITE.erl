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
    DbName = <<"conflict_test_db">>,
    DocId = <<"doc_with_conflict">>,

    %% Create initial document
    Doc = #{<<"id">> => DocId, <<"value">> => <<"initial">>},
    {ok, #{<<"rev">> := Rev1}} = barrel_docdb:put_doc(DbName, Doc),

    %% Update the document to create branch 1 (2-xxx from 1-xxx)
    Doc2 = #{<<"id">> => DocId, <<"_rev">> => Rev1, <<"value">> => <<"updated">>},
    {ok, #{<<"rev">> := _Rev2}} = barrel_docdb:put_doc(DbName, Doc2),

    %% Simulate a conflict by using put_rev with a DIFFERENT branch from same parent
    %% This creates: 1-xxx -> 2-original AND 1-xxx -> 2-conflict (two leaves)
    ConflictDoc = #{<<"id">> => DocId, <<"value">> => <<"conflict">>},
    ConflictRev = <<"2-conflictabc123456789">>,
    History = [ConflictRev, Rev1],

    {ok, Pid} = barrel_docdb:db_pid(DbName),
    {ok, _, _} = barrel_db_server:put_rev(Pid, ConflictDoc, History, false),

    %% Now there should be a conflict (two branches from same root)
    {ok, Conflicts} = barrel_docdb:get_conflicts(DbName, DocId),
    ?assertEqual(1, length(Conflicts)),

    ok.

get_doc_with_conflicts_option(_Config) ->
    DbName = <<"conflict_test_db">>,
    DocId = <<"doc_conflicts_opt">>,

    %% Create initial document
    Doc = #{<<"id">> => DocId, <<"value">> => <<"initial">>},
    {ok, #{<<"rev">> := Rev1}} = barrel_docdb:put_doc(DbName, Doc),

    %% Update to create branch 1
    Doc2 = #{<<"id">> => DocId, <<"_rev">> => Rev1, <<"value">> => <<"updated">>},
    {ok, #{<<"rev">> := _Rev2}} = barrel_docdb:put_doc(DbName, Doc2),

    %% Create a conflict (branch 2 from same parent)
    ConflictDoc = #{<<"id">> => DocId, <<"value">> => <<"conflict">>},
    ConflictRev = <<"2-conflict123456789abc">>,
    History = [ConflictRev, Rev1],

    {ok, Pid} = barrel_docdb:db_pid(DbName),
    {ok, _, _} = barrel_db_server:put_rev(Pid, ConflictDoc, History, false),

    %% Get doc without conflicts option - no _conflicts field
    {ok, DocNoOpt} = barrel_docdb:get_doc(DbName, DocId),
    ?assertEqual(false, maps:is_key(<<"_conflicts">>, DocNoOpt)),

    %% Get doc with conflicts option - should have _conflicts field
    {ok, DocWithOpt} = barrel_docdb:get_doc(DbName, DocId, #{conflicts => true}),
    ?assertEqual(true, maps:is_key(<<"_conflicts">>, DocWithOpt)),

    ConflictRevs = maps:get(<<"_conflicts">>, DocWithOpt),
    ?assertEqual(1, length(ConflictRevs)),

    ok.

get_conflicts_api(_Config) ->
    DbName = <<"conflict_test_db">>,
    DocId = <<"doc_get_conflicts_api">>,

    %% Create initial document
    Doc = #{<<"id">> => DocId, <<"data">> => <<"test">>},
    {ok, #{<<"rev">> := Rev1}} = barrel_docdb:put_doc(DbName, Doc),

    %% Update to create branch 1 (main branch)
    Doc2 = #{<<"id">> => DocId, <<"_rev">> => Rev1, <<"data">> => <<"main">>},
    {ok, #{<<"rev">> := _Rev2}} = barrel_docdb:put_doc(DbName, Doc2),

    %% Create two additional conflicts (branch 2 and 3 from same root)
    Conflict1Doc = #{<<"id">> => DocId, <<"data">> => <<"conflict1">>},
    Conflict1Rev = <<"2-aaaaaaaaaaaaaaaaaa">>,
    History1 = [Conflict1Rev, Rev1],

    Conflict2Doc = #{<<"id">> => DocId, <<"data">> => <<"conflict2">>},
    Conflict2Rev = <<"2-bbbbbbbbbbbbbbbbbb">>,
    History2 = [Conflict2Rev, Rev1],

    {ok, Pid} = barrel_docdb:db_pid(DbName),
    {ok, _, _} = barrel_db_server:put_rev(Pid, Conflict1Doc, History1, false),
    {ok, _, _} = barrel_db_server:put_rev(Pid, Conflict2Doc, History2, false),

    %% Get conflicts - should have 2 (the two put_rev branches; main branch is the winner)
    {ok, Conflicts} = barrel_docdb:get_conflicts(DbName, DocId),
    ?assertEqual(2, length(Conflicts)),

    %% Non-existent doc returns error
    {error, not_found} = barrel_docdb:get_conflicts(DbName, <<"nonexistent">>),

    ok.

%%====================================================================
%% Conflict Resolution Tests
%%====================================================================

resolve_conflict_choose_winner(_Config) ->
    DbName = <<"conflict_test_db">>,
    DocId = <<"doc_resolve_winner">>,

    %% Create initial document
    Doc = #{<<"id">> => DocId, <<"name">> => <<"original">>},
    {ok, #{<<"rev">> := Rev1}} = barrel_docdb:put_doc(DbName, Doc),

    %% Update to create branch 1
    Doc2 = #{<<"id">> => DocId, <<"_rev">> => Rev1, <<"name">> => <<"updated">>},
    {ok, #{<<"rev">> := _Rev2}} = barrel_docdb:put_doc(DbName, Doc2),

    %% Create a conflict (branch 2)
    ConflictDoc = #{<<"id">> => DocId, <<"name">> => <<"conflict">>},
    ConflictRev = <<"2-zzzzzzzzzzzzzzzzzzz">>,
    History = [ConflictRev, Rev1],

    {ok, Pid} = barrel_docdb:db_pid(DbName),
    {ok, _, _} = barrel_db_server:put_rev(Pid, ConflictDoc, History, false),

    %% Verify conflict exists
    {ok, Conflicts} = barrel_docdb:get_conflicts(DbName, DocId),
    ?assert(length(Conflicts) > 0),

    %% Get the current winner
    {ok, CurrentDoc} = barrel_docdb:get_doc(DbName, DocId),
    Winner = maps:get(<<"_rev">>, CurrentDoc),

    %% Resolve by choosing the winner (keeps current state, marks conflicts as deleted)
    {ok, Result} = barrel_docdb:resolve_conflict(DbName, DocId, Winner, {choose, Winner}),
    ?assertEqual(DocId, maps:get(id, Result)),
    ?assert(maps:get(conflicts_resolved, Result) > 0),

    %% Conflicts should now be empty
    {ok, []} = barrel_docdb:get_conflicts(DbName, DocId),

    ok.

resolve_conflict_choose_loser(_Config) ->
    DbName = <<"conflict_test_db">>,
    DocId = <<"doc_resolve_loser">>,

    %% Create initial document
    Doc = #{<<"id">> => DocId, <<"choice">> => <<"original">>},
    {ok, #{<<"rev">> := Rev1}} = barrel_docdb:put_doc(DbName, Doc),

    %% Update to create a branch with high lexicographic rev (will be winner)
    Doc2 = #{<<"id">> => DocId, <<"_rev">> => Rev1, <<"choice">> => <<"updated">>},
    {ok, #{<<"rev">> := _Rev2}} = barrel_docdb:put_doc(DbName, Doc2),

    %% Create a conflict with lower lexicographic rev (will be the loser)
    ConflictDoc = #{<<"id">> => DocId, <<"choice">> => <<"conflict">>},
    ConflictRev = <<"2-00000000000000000000">>,
    History = [ConflictRev, Rev1],

    {ok, Pid} = barrel_docdb:db_pid(DbName),
    {ok, _, _} = barrel_db_server:put_rev(Pid, ConflictDoc, History, false),

    %% Get conflicts and the winner
    {ok, Conflicts} = barrel_docdb:get_conflicts(DbName, DocId),
    {ok, CurrentDoc} = barrel_docdb:get_doc(DbName, DocId),
    Winner = maps:get(<<"_rev">>, CurrentDoc),

    %% The loser should be in the conflicts list
    [Loser] = Conflicts,
    ?assertNotEqual(Winner, Loser),

    %% Resolve by choosing the loser (creates new rev as child of loser)
    {ok, Result} = barrel_docdb:resolve_conflict(DbName, DocId, Winner, {choose, Loser}),
    NewRev = maps:get(rev, Result),
    %% New rev should be gen 3 (child of the gen 2 loser)
    ?assertMatch(<<"3-", _/binary>>, NewRev),
    ?assert(maps:get(conflicts_resolved, Result) >= 1),

    %% Conflicts should now be empty
    {ok, []} = barrel_docdb:get_conflicts(DbName, DocId),

    ok.

resolve_conflict_merge(_Config) ->
    DbName = <<"conflict_test_db">>,
    DocId = <<"doc_resolve_merge">>,

    %% Create initial document
    Doc = #{<<"id">> => DocId, <<"field1">> => <<"value1">>},
    {ok, #{<<"rev">> := Rev1}} = barrel_docdb:put_doc(DbName, Doc),

    %% Update to create branch 1
    Doc2 = #{<<"id">> => DocId, <<"_rev">> => Rev1, <<"field1">> => <<"updated">>},
    {ok, #{<<"rev">> := _Rev2}} = barrel_docdb:put_doc(DbName, Doc2),

    %% Create a conflict (branch 2)
    ConflictDoc = #{<<"id">> => DocId, <<"field2">> => <<"value2">>},
    ConflictRev = <<"2-mergeconflict123456">>,
    History = [ConflictRev, Rev1],

    {ok, Pid} = barrel_docdb:db_pid(DbName),
    {ok, _, _} = barrel_db_server:put_rev(Pid, ConflictDoc, History, false),

    %% Get current winner
    {ok, CurrentDoc} = barrel_docdb:get_doc(DbName, DocId),
    Winner = maps:get(<<"_rev">>, CurrentDoc),

    %% Resolve by merging - combine both fields
    MergedDoc = #{<<"field1">> => <<"value1">>, <<"field2">> => <<"value2">>, <<"merged">> => true},
    {ok, Result} = barrel_docdb:resolve_conflict(DbName, DocId, Winner, {merge, MergedDoc}),

    %% Should have a new revision
    NewRev = maps:get(rev, Result),
    ?assertNotEqual(Winner, NewRev),
    ?assert(maps:get(conflicts_resolved, Result) > 0),

    %% Conflicts should be empty
    {ok, []} = barrel_docdb:get_conflicts(DbName, DocId),

    %% Get the merged document
    {ok, FinalDoc} = barrel_docdb:get_doc(DbName, DocId),
    ?assertEqual(true, maps:get(<<"merged">>, FinalDoc)),

    ok.

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
    DbName = <<"conflict_test_db">>,
    DocId = <<"doc_invalid_rev">>,

    %% Create initial document
    Doc = #{<<"id">> => DocId, <<"data">> => 1},
    {ok, #{<<"rev">> := Rev1}} = barrel_docdb:put_doc(DbName, Doc),

    %% Update to create branch 1
    Doc2 = #{<<"id">> => DocId, <<"_rev">> => Rev1, <<"data">> => 2},
    {ok, #{<<"rev">> := _Rev2}} = barrel_docdb:put_doc(DbName, Doc2),

    %% Create a conflict (branch 2)
    ConflictDoc = #{<<"id">> => DocId, <<"data">> => 3},
    ConflictRev = <<"2-invalidrevtest00000">>,
    History = [ConflictRev, Rev1],

    {ok, Pid} = barrel_docdb:db_pid(DbName),
    {ok, _, _} = barrel_db_server:put_rev(Pid, ConflictDoc, History, false),

    %% Get winner
    {ok, CurrentDoc} = barrel_docdb:get_doc(DbName, DocId),
    Winner = maps:get(<<"_rev">>, CurrentDoc),

    %% Try to choose a non-existent revision
    {error, {invalid_rev, _}} = barrel_docdb:resolve_conflict(DbName, DocId, Winner,
                                                              {choose, <<"2-doesnotexist">>}),

    ok.


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
