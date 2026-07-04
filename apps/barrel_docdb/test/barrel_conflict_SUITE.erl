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
        {group, merger_and_bounds},
        {group, mvcc}
    ].

groups() ->
    [
        {conflict_detection, [sequence], [
            no_conflict_single_rev,
            detect_conflict_via_replication,
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
        {merger_and_bounds, [sequence], [
            merger_resolves_concurrent_writes,
            merger_declining_falls_back_to_conflict,
            merger_crash_falls_back_to_conflict,
            merger_skips_tombstones,
            conflict_sibling_bound
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

%% A version authored by a fake remote peer, with the version vector
%% such a peer would ship for a fresh write.
remote_version(Author) ->
    V = barrel_version:new(barrel_hlc:new_hlc(), Author),
    VV = barrel_vv:bump(barrel_vv:new(), V),
    {barrel_version:to_token(V), barrel_vv:encode(VV)}.

%% Create a doc with one conflict sibling: the remote version is issued
%% before the local write, so it is concurrent but loses LWW and lands
%% as a live conflict. Returns {LocalRev, RemoteTok}.
make_conflict(Db, DocId) ->
    {RemoteTok, RemoteVV} = remote_version(<<"peer_a">>),
    {ok, #{<<"rev">> := LocalRev}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => DocId, <<"v">> => <<"local">>}),
    {ok, DocId, LocalRev} = barrel_docdb:put_version(
        Db, #{<<"id">> => DocId, <<"v">> => <<"remote">>},
        RemoteTok, RemoteVV, false),
    {LocalRev, RemoteTok}.

detect_conflict_via_replication(_Config) ->
    DbName = <<"conflict_test_db">>,
    DocId = <<"doc_detect_conflict">>,

    {LocalRev, RemoteTok} = make_conflict(DbName, DocId),

    %% The local winner stays current, the remote is a conflict sibling
    {ok, Doc} = barrel_docdb:get_doc(DbName, DocId),
    ?assertEqual(LocalRev, maps:get(<<"_rev">>, Doc)),
    ?assertEqual(<<"local">>, maps:get(<<"v">>, Doc)),
    {ok, Conflicts} = barrel_docdb:get_conflicts(DbName, DocId),
    ?assertEqual([RemoteTok], Conflicts),

    ok.

get_doc_with_conflicts_option(_Config) ->
    DbName = <<"conflict_test_db">>,
    DocId = <<"doc_conflicts_option">>,

    {_LocalRev, RemoteTok} = make_conflict(DbName, DocId),

    {ok, Doc} = barrel_docdb:get_doc(DbName, DocId, #{conflicts => true}),
    ?assertEqual([RemoteTok], maps:get(<<"_conflicts">>, Doc)),

    %% Without the option the doc carries no _conflicts field
    {ok, Doc2} = barrel_docdb:get_doc(DbName, DocId),
    ?assertNot(maps:is_key(<<"_conflicts">>, Doc2)),

    ok.

get_conflicts_api(_Config) ->
    DbName = <<"conflict_test_db">>,
    DocId = <<"doc_conflicts_api">>,

    %% Two concurrent remote versions from distinct peers, both losing
    {TokA, VVA} = remote_version(<<"peer_a">>),
    {TokB, VVB} = remote_version(<<"peer_b">>),
    {ok, #{<<"rev">> := LocalRev}} =
        barrel_docdb:put_doc(DbName, #{<<"id">> => DocId, <<"v">> => <<"local">>}),
    {ok, DocId, LocalRev} = barrel_docdb:put_version(
        DbName, #{<<"id">> => DocId, <<"v">> => <<"a">>}, TokA, VVA, false),
    {ok, DocId, LocalRev} = barrel_docdb:put_version(
        DbName, #{<<"id">> => DocId, <<"v">> => <<"b">>}, TokB, VVB, false),

    {ok, Conflicts} = barrel_docdb:get_conflicts(DbName, DocId),
    ?assertEqual(lists:sort([TokA, TokB]), lists:sort(Conflicts)),

    ok.

%%====================================================================
%% Conflict Resolution Tests
%%====================================================================

resolve_conflict_choose_winner(_Config) ->
    DbName = <<"conflict_test_db">>,
    DocId = <<"doc_choose_winner">>,

    {LocalRev, _RemoteTok} = make_conflict(DbName, DocId),

    %% Keep the current winner: still a resolving write (new rev),
    %% conflicts cleared
    {ok, #{rev := NewRev, conflicts_resolved := 1}} =
        barrel_docdb:resolve_conflict(DbName, DocId, LocalRev,
                                      {choose, LocalRev}),
    ?assertNotEqual(LocalRev, NewRev),

    {ok, Doc} = barrel_docdb:get_doc(DbName, DocId),
    ?assertEqual(<<"local">>, maps:get(<<"v">>, Doc)),
    ?assertEqual(NewRev, maps:get(<<"_rev">>, Doc)),
    {ok, Conflicts} = barrel_docdb:get_conflicts(DbName, DocId),
    ?assertEqual([], Conflicts),

    ok.

resolve_conflict_choose_loser(_Config) ->
    DbName = <<"conflict_test_db">>,
    DocId = <<"doc_choose_loser">>,

    {LocalRev, RemoteTok} = make_conflict(DbName, DocId),

    %% Choose the conflict sibling: its archived body becomes current
    {ok, #{rev := NewRev, conflicts_resolved := 1}} =
        barrel_docdb:resolve_conflict(DbName, DocId, LocalRev,
                                      {choose, RemoteTok}),

    {ok, Doc} = barrel_docdb:get_doc(DbName, DocId),
    ?assertEqual(<<"remote">>, maps:get(<<"v">>, Doc)),
    ?assertEqual(NewRev, maps:get(<<"_rev">>, Doc)),
    {ok, Conflicts} = barrel_docdb:get_conflicts(DbName, DocId),
    ?assertEqual([], Conflicts),

    ok.

resolve_conflict_merge(_Config) ->
    DbName = <<"conflict_test_db">>,
    DocId = <<"doc_merge">>,

    {LocalRev, _RemoteTok} = make_conflict(DbName, DocId),

    {ok, #{rev := NewRev, conflicts_resolved := 1}} =
        barrel_docdb:resolve_conflict(DbName, DocId, LocalRev,
                                      {merge, #{<<"v">> => <<"merged">>}}),

    {ok, Doc} = barrel_docdb:get_doc(DbName, DocId),
    ?assertEqual(<<"merged">>, maps:get(<<"v">>, Doc)),
    ?assertEqual(NewRev, maps:get(<<"_rev">>, Doc)),
    {ok, Conflicts} = barrel_docdb:get_conflicts(DbName, DocId),
    ?assertEqual([], Conflicts),

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

    {LocalRev, RemoteTok} = make_conflict(DbName, DocId),

    %% Base rev must be the current winner
    {error, {conflict, LocalRev}} =
        barrel_docdb:resolve_conflict(DbName, DocId, RemoteTok,
                                      {choose, RemoteTok}),

    %% Choosing a version that is not a sibling fails
    {FakeTok, _} = remote_version(<<"peer_z">>),
    {error, {unknown_version, FakeTok}} =
        barrel_docdb:resolve_conflict(DbName, DocId, LocalRev,
                                      {choose, FakeTok}),

    %% The conflict is untouched
    {ok, Conflicts} = barrel_docdb:get_conflicts(DbName, DocId),
    ?assertEqual([RemoteTok], Conflicts),

    ok.

%%====================================================================
%% Merger Hook and Sibling Bound Tests
%%====================================================================

%% Deterministic body merge used as the conflict_merger hook ({M, F}
%% form; module-qualified so it survives config round-trips).
merge_bodies(_DocId, LocalBody, RemoteBody) ->
    Merged = maps:merge(LocalBody, RemoteBody),
    {merge, Merged#{<<"merged">> => true}}.

decline_merge(_DocId, _LocalBody, _RemoteBody) ->
    keep.

crash_merge(_DocId, _LocalBody, _RemoteBody) ->
    error(merger_boom).

with_db(Name, Opts, Fun) ->
    DataDir = "/tmp/barrel_test_conflict_" ++ binary_to_list(Name),
    os:cmd("rm -rf " ++ DataDir),
    {ok, _} = barrel_docdb:create_db(Name, Opts#{data_dir => DataDir}),
    try
        Fun(Name)
    after
        barrel_docdb:delete_db(Name),
        os:cmd("rm -rf " ++ DataDir)
    end.

merger_resolves_concurrent_writes(_Config) ->
    with_db(<<"merger_db">>,
            #{conflict_merger => {?MODULE, merge_bodies}},
            fun(Db) ->
        DocId = <<"merged_doc">>,
        {RemoteTok, RemoteVV} = remote_version(<<"peer_a">>),
        {ok, _} = barrel_docdb:put_doc(
            Db, #{<<"id">> => DocId, <<"local">> => 1}),
        {ok, DocId, NewTok} = barrel_docdb:put_version(
            Db, #{<<"id">> => DocId, <<"remote">> => 2},
            RemoteTok, RemoteVV, false),

        %% No conflict recorded: the merged body is the new current
        {ok, Doc} = barrel_docdb:get_doc(Db, DocId),
        ?assertEqual(NewTok, maps:get(<<"_rev">>, Doc)),
        ?assertEqual(1, maps:get(<<"local">>, Doc)),
        ?assertEqual(2, maps:get(<<"remote">>, Doc)),
        ?assertEqual(true, maps:get(<<"merged">>, Doc)),
        {ok, []} = barrel_docdb:get_conflicts(Db, DocId),

        %% The merged write's vector covers the remote version:
        %% redelivery is a no-op
        {ok, DocId, NewTok} = barrel_docdb:put_version(
            Db, #{<<"id">> => DocId, <<"remote">> => 2},
            RemoteTok, RemoteVV, false),

        %% History tells the full story: local write, remote arrival,
        %% resolving merge
        {ok, Entries0} = barrel_docdb:fold_history(
            Db, fun(E, Acc) -> {ok, [E | Acc]} end, []),
        Entries = [E || E <- lists:reverse(Entries0),
                        maps:get(id, E) =:= DocId],
        ?assertEqual([local, replicated, resolve],
                     [maps:get(cause, E) || E <- Entries]),

        %% The remote body stays queryable as a superseded version
        {ok, RemoteBody} = barrel_docdb:get_version_body(Db, DocId, RemoteTok),
        ?assertEqual(2, maps:get(<<"remote">>, RemoteBody))
    end).

merger_declining_falls_back_to_conflict(_Config) ->
    with_db(<<"merger_decline_db">>,
            #{conflict_merger => {?MODULE, decline_merge}},
            fun(Db) ->
        DocId = <<"declined_doc">>,
        {RemoteTok, RemoteVV} = remote_version(<<"peer_a">>),
        {ok, #{<<"rev">> := LocalRev}} = barrel_docdb:put_doc(
            Db, #{<<"id">> => DocId, <<"v">> => <<"local">>}),
        {ok, DocId, LocalRev} = barrel_docdb:put_version(
            Db, #{<<"id">> => DocId, <<"v">> => <<"remote">>},
            RemoteTok, RemoteVV, false),
        {ok, Conflicts} = barrel_docdb:get_conflicts(Db, DocId),
        ?assertEqual([RemoteTok], Conflicts)
    end).

merger_crash_falls_back_to_conflict(_Config) ->
    with_db(<<"merger_crash_db">>,
            #{conflict_merger => {?MODULE, crash_merge}},
            fun(Db) ->
        DocId = <<"crashed_doc">>,
        {RemoteTok, RemoteVV} = remote_version(<<"peer_a">>),
        {ok, #{<<"rev">> := LocalRev}} = barrel_docdb:put_doc(
            Db, #{<<"id">> => DocId, <<"v">> => <<"local">>}),
        {ok, DocId, LocalRev} = barrel_docdb:put_version(
            Db, #{<<"id">> => DocId, <<"v">> => <<"remote">>},
            RemoteTok, RemoteVV, false),
        {ok, Conflicts} = barrel_docdb:get_conflicts(Db, DocId),
        ?assertEqual([RemoteTok], Conflicts)
    end).

merger_skips_tombstones(_Config) ->
    with_db(<<"merger_tombstone_db">>,
            #{conflict_merger => {?MODULE, merge_bodies}},
            fun(Db) ->
        DocId = <<"tombstone_doc">>,
        %% A concurrent remote DELETE: body merge is meaningless, the
        %% conflict path applies
        {RemoteTok, RemoteVV} = remote_version(<<"peer_a">>),
        {ok, #{<<"rev">> := LocalRev}} = barrel_docdb:put_doc(
            Db, #{<<"id">> => DocId, <<"v">> => <<"local">>}),
        {ok, DocId, LocalRev} = barrel_docdb:put_version(
            Db, #{<<"id">> => DocId}, RemoteTok, RemoteVV, true),
        {ok, Conflicts} = barrel_docdb:get_conflicts(Db, DocId),
        ?assertEqual([RemoteTok], Conflicts),
        %% The winner is still the live local doc
        {ok, Doc} = barrel_docdb:get_doc(Db, DocId),
        ?assertEqual(<<"local">>, maps:get(<<"v">>, Doc))
    end).

conflict_sibling_bound(_Config) ->
    with_db(<<"bound_db">>,
            #{max_conflict_versions => 2},
            fun(Db) ->
        DocId = <<"bounded_doc">>,
        %% Three concurrent losing siblings against one local winner;
        %% versions order peer HLCs by issue time (a < b < c)
        {TokA, VVA} = remote_version(<<"peer_a">>),
        {TokB, VVB} = remote_version(<<"peer_b">>),
        {TokC, VVC} = remote_version(<<"peer_c">>),
        {ok, #{<<"rev">> := LocalRev}} = barrel_docdb:put_doc(
            Db, #{<<"id">> => DocId, <<"v">> => <<"local">>}),
        {ok, DocId, LocalRev} = barrel_docdb:put_version(
            Db, #{<<"id">> => DocId, <<"v">> => <<"a">>}, TokA, VVA, false),
        {ok, DocId, LocalRev} = barrel_docdb:put_version(
            Db, #{<<"id">> => DocId, <<"v">> => <<"b">>}, TokB, VVB, false),
        {ok, DocId, LocalRev} = barrel_docdb:put_version(
            Db, #{<<"id">> => DocId, <<"v">> => <<"c">>}, TokC, VVC, false),

        %% Bound 2: the lowest version (peer_a) was dropped
        {ok, Conflicts} = barrel_docdb:get_conflicts(Db, DocId),
        ?assertEqual(lists:sort([TokB, TokC]), lists:sort(Conflicts)),
        ?assertEqual({error, not_found},
                     barrel_docdb:get_version_body(Db, DocId, TokA)),
        {ok, _} = barrel_docdb:get_version_body(Db, DocId, TokB),

        %% Resolution clears the bounded set
        {ok, #{conflicts_resolved := 2}} = barrel_docdb:resolve_conflict(
            Db, DocId, LocalRev, {merge, #{<<"v">> => <<"done">>}}),
        {ok, []} = barrel_docdb:get_conflicts(Db, DocId)
    end).

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
