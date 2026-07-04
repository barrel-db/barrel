%%%-------------------------------------------------------------------
%%% @doc Test suite for the retention sweeper
%%%
%%% Databases here use a 1-second retention window and manual sweeps
%%% (the periodic timer fires every 10 minutes by default, far outside
%%% test time).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_retention_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [
        {group, sweep}
    ].

groups() ->
    [
        {sweep, [sequence], [
            sweep_prunes_history_and_superseded,
            sweep_forgets_expired_tombstones,
            sweep_is_idempotent,
            sweep_keeps_live_conflicts,
            sweep_respects_window,
            info_exposes_retention,
            checkpoint_below_floor_forces_resync
        ]}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    DataDir = "/tmp/barrel_test_retention",
    os:cmd("rm -rf " ++ DataDir),
    [{data_dir, DataDir} | Config].

end_per_suite(Config) ->
    ok = application:stop(barrel_docdb),
    os:cmd("rm -rf " ++ ?config(data_dir, Config)),
    ok.

init_per_testcase(Case, Config) ->
    %% One fresh short-retention db per case
    Db = atom_to_binary(Case, utf8),
    DataDir = ?config(data_dir, Config),
    {ok, _} = barrel_docdb:create_db(Db, #{
        data_dir => DataDir,
        retention_period => 1
    }),
    [{db, Db} | Config].

end_per_testcase(_Case, Config) ->
    barrel_docdb:delete_db(?config(db, Config)),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

all_history(Db) ->
    {ok, Entries} = barrel_docdb:fold_history(
        Db, fun(E, Acc) -> {ok, [E | Acc]} end, []),
    lists:reverse(Entries).

%% Let every write fall behind the 1-second retention window
age_past_window() ->
    timer:sleep(1200).

remote_version(Author) ->
    V = barrel_version:new(barrel_hlc:new_hlc(), Author),
    VV = barrel_vv:bump(barrel_vv:new(), V),
    {barrel_version:to_token(V), barrel_vv:encode(VV)}.

%%====================================================================
%% Tests
%%====================================================================

sweep_prunes_history_and_superseded(Config) ->
    Db = ?config(db, Config),
    DocId = <<"doc1">>,

    {ok, #{<<"rev">> := Rev1}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => DocId, <<"v">> => 1}),
    {ok, #{<<"rev">> := Rev2}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => DocId, <<"_rev">> => Rev1,
                                   <<"v">> => 2}),
    ?assertEqual(2, length(all_history(Db))),
    {ok, _} = barrel_docdb:get_version_body(Db, DocId, Rev1),

    age_past_window(),
    {ok, Stats} = barrel_docdb:sweep_retention(Db),
    ?assertEqual(1, maps:get(superseded_removed, Stats)),

    %% History pruned, floor advanced
    ?assertEqual([], all_history(Db)),
    ?assertNotEqual(undefined, barrel_docdb:history_floor(Db)),

    %% Current state untouched, superseded version gone
    {ok, Doc} = barrel_docdb:get_doc(Db, DocId),
    ?assertEqual(2, maps:get(<<"v">>, Doc)),
    ?assertEqual(Rev2, maps:get(<<"_rev">>, Doc)),
    ?assertEqual({error, not_found},
                 barrel_docdb:get_version_body(Db, DocId, Rev1)),
    {ok, Versions} = barrel_docdb:get_doc_versions(Db, DocId),
    ?assertEqual([{Rev2, current}],
                 [{maps:get(version, V), maps:get(status, V)} || V <- Versions]),
    ok.

sweep_forgets_expired_tombstones(Config) ->
    Db = ?config(db, Config),
    DocId = <<"doc_gone">>,

    {ok, #{<<"rev">> := Rev1}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => DocId, <<"v">> => 1}),
    {ok, _} = barrel_docdb:delete_doc(Db, DocId, #{rev => Rev1}),

    age_past_window(),
    {ok, Stats} = barrel_docdb:sweep_retention(Db),
    ?assertEqual(1, maps:get(docs_forgotten, Stats)),

    %% The database has fully forgotten the document
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(Db, DocId)),
    ?assertEqual({error, not_found}, barrel_docdb:get_doc_versions(Db, DocId)),
    ?assertEqual([], all_history(Db)),
    {ok, Changes, _} = barrel_docdb:get_changes(Db, first),
    ?assertEqual([], [C || C <- Changes, maps:get(id, C) =:= DocId]),

    %% The id is writable again as a brand new doc
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => DocId, <<"v">> => <<"new">>}),
    ok.

sweep_is_idempotent(Config) ->
    Db = ?config(db, Config),
    DocId = <<"doc_idem">>,

    {ok, #{<<"rev">> := Rev1}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => DocId, <<"v">> => 1}),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => DocId, <<"_rev">> => Rev1,
                                         <<"v">> => 2}),

    age_past_window(),
    {ok, _} = barrel_docdb:sweep_retention(Db),
    {ok, Doc} = barrel_docdb:get_doc(Db, DocId),

    %% Re-sweeping finds nothing and changes nothing
    {ok, Stats2} = barrel_docdb:sweep_retention(Db),
    ?assertEqual(0, maps:get(history_swept, Stats2)),
    ?assertEqual(0, maps:get(superseded_removed, Stats2)),
    ?assertEqual(0, maps:get(docs_forgotten, Stats2)),
    ?assertEqual({ok, Doc}, barrel_docdb:get_doc(Db, DocId)),
    ok.

sweep_keeps_live_conflicts(Config) ->
    Db = ?config(db, Config),
    DocId = <<"doc_conflict">>,

    %% Concurrent remote loser: a live conflict sibling
    {RemoteTok, RemoteVV} = remote_version(<<"peer_a">>),
    {ok, #{<<"rev">> := LocalRev}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => DocId, <<"v">> => <<"local">>}),
    {ok, DocId, LocalRev} = barrel_docdb:put_version(
        Db, #{<<"id">> => DocId, <<"v">> => <<"remote">>},
        RemoteTok, RemoteVV, false),

    age_past_window(),
    {ok, _} = barrel_docdb:sweep_retention(Db),

    %% History gone, but the conflict is live state: sibling and its
    %% body survive until resolved
    ?assertEqual([], all_history(Db)),
    {ok, Conflicts} = barrel_docdb:get_conflicts(Db, DocId),
    ?assertEqual([RemoteTok], Conflicts),
    {ok, SiblingBody} = barrel_docdb:get_version_body(Db, DocId, RemoteTok),
    ?assertEqual(<<"remote">>, maps:get(<<"v">>, SiblingBody)),

    %% Resolution still works after the sweep
    {ok, _} = barrel_docdb:resolve_conflict(Db, DocId, LocalRev,
                                            {choose, RemoteTok}),
    {ok, Resolved} = barrel_docdb:get_doc(Db, DocId),
    ?assertEqual(<<"remote">>, maps:get(<<"v">>, Resolved)),
    ok.

sweep_respects_window(Config) ->
    %% A separate db with a 1-hour window: fresh writes never swept
    DataDir = ?config(data_dir, Config),
    Db = <<"retention_window_db">>,
    {ok, _} = barrel_docdb:create_db(Db, #{
        data_dir => DataDir,
        retention_period => 3600
    }),
    try
        DocId = <<"doc_fresh">>,
        {ok, #{<<"rev">> := Rev1}} =
            barrel_docdb:put_doc(Db, #{<<"id">> => DocId, <<"v">> => 1}),
        {ok, _} = barrel_docdb:delete_doc(Db, DocId, #{rev => Rev1}),

        {ok, Stats} = barrel_docdb:sweep_retention(Db),
        ?assertEqual(0, maps:get(history_swept, Stats)),
        ?assertEqual(0, maps:get(docs_forgotten, Stats)),
        ?assertEqual(2, length(all_history(Db)))
    after
        barrel_docdb:delete_db(Db)
    end,
    ok.

info_exposes_retention(Config) ->
    Db = ?config(db, Config),
    {ok, Info} = barrel_docdb:db_info(Db),
    ?assertEqual(1, maps:get(retention_period, Info)),
    ?assertEqual(undefined, maps:get(history_floor, Info)),

    age_past_window(),
    {ok, _} = barrel_docdb:sweep_retention(Db),
    {ok, Info2} = barrel_docdb:db_info(Db),
    ?assertNotEqual(undefined, maps:get(history_floor, Info2)),
    ok.

checkpoint_below_floor_forces_resync(Config) ->
    DataDir = ?config(data_dir, Config),
    Source = ?config(db, Config),
    Target = <<"retention_rep_target">>,
    {ok, _} = barrel_docdb:create_db(Target, #{data_dir => DataDir}),
    try
        %% First replication establishes a checkpoint
        {ok, _} = barrel_docdb:put_doc(Source, #{<<"id">> => <<"r1">>,
                                                 <<"v">> => 1}),
        {ok, R1} = barrel_rep:replicate(Source, Target),
        ?assertNotEqual(first, maps:get(last_seq, R1)),

        %% The source floor moves past the checkpoint
        {ok, _} = barrel_docdb:put_doc(Source, #{<<"id">> => <<"r2">>,
                                                 <<"v">> => 2}),
        age_past_window(),
        {ok, _} = barrel_docdb:sweep_retention(Source),

        %% The stale checkpoint is discarded: full resync from first
        {ok, R2} = barrel_rep:replicate(Source, Target),
        ?assertEqual(first, maps:get(start_seq, R2)),
        {ok, _} = barrel_docdb:get_doc(Target, <<"r2">>)
    after
        barrel_docdb:delete_db(Target)
    end,
    ok.
