%%%-------------------------------------------------------------------
%%% @doc PITR: branch a database at a past instant. Docs changed after
%%% T rewind to their latest retained state at or before T; docs
%%% created after T are forgotten; feed, query index, channels, and
%%% outbox all reflect the rewound state; guards fire when T predates
%%% retention.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_timeline_pitr_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([
    rewind_updates/1,
    rewind_forgets_created_after_t/1,
    rewind_restores_deleted_after_t/1,
    rewind_restores_tombstone/1,
    rewind_feed_replay/1,
    rewind_query_index/1,
    rewind_channels/1,
    rewind_outbox_swept/1,
    rewind_floor_guard/1,
    rewind_then_write/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [rewind_updates, rewind_forgets_created_after_t,
     rewind_restores_deleted_after_t, rewind_restores_tombstone,
     rewind_feed_replay, rewind_query_index, rewind_channels,
     rewind_outbox_swept, rewind_floor_guard, rewind_then_write].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    DataDir = "/tmp/barrel_test_timeline_pitr",
    os:cmd("rm -rf " ++ DataDir),
    [{data_dir, DataDir} | Config].

end_per_suite(Config) ->
    ok = application:stop(barrel_docdb),
    os:cmd("rm -rf " ++ ?config(data_dir, Config)),
    ok.

init_per_testcase(Case, Config) ->
    Db = <<"pitr_", (atom_to_binary(Case, utf8))/binary>>,
    {ok, _} = barrel_docdb:create_db(Db, #{
        data_dir => ?config(data_dir, Config)
    }),
    [{db, Db}, {branch, <<Db/binary, "_b">>} | Config].

end_per_testcase(_Case, Config) ->
    _ = barrel_docdb:delete_db(?config(branch, Config),
                               #{data_dir => ?config(data_dir, Config)}),
    _ = barrel_docdb:delete_db(?config(db, Config)),
    ok.

%% The cursor for "now": the HLC of the last applied write, taken
%% from the changes feed (what a client would hold).
cursor(Db) ->
    {ok, Changes, Last} = barrel_docdb:get_changes(Db, first),
    ?assert(length(Changes) >= 0),
    Last.

%%====================================================================
%% Cases
%%====================================================================

rewind_updates(Config) ->
    Db = ?config(db, Config),
    Branch = ?config(branch, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"d">>,
                                         <<"v">> => 1}),
    T = cursor(Db),
    {ok, #{<<"_rev">> := R1}} = barrel_docdb:get_doc(Db, <<"d">>),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"d">>,
                                         <<"v">> => 2,
                                         <<"_rev">> => R1}),
    {ok, _} = barrel_docdb:branch_db(Db, Branch, #{at => T}),
    %% the branch reads v1 with its original rev; the parent keeps v2
    {ok, #{<<"v">> := 1, <<"_rev">> := R1}} =
        barrel_docdb:get_doc(Branch, <<"d">>),
    {ok, #{<<"v">> := 2}} = barrel_docdb:get_doc(Db, <<"d">>),
    %% history on the branch ends at T
    {ok, N} = barrel_docdb:fold_history(
        Branch, fun(_, Acc) -> {ok, Acc + 1} end, 0),
    ?assertEqual(1, N),
    ok.

rewind_forgets_created_after_t(Config) ->
    Db = ?config(db, Config),
    Branch = ?config(branch, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"before">>,
                                         <<"k">> => <<"x">>}),
    T = cursor(Db),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"after">>,
                                         <<"k">> => <<"x">>}),
    {ok, _} = barrel_docdb:branch_db(Db, Branch, #{at => T}),
    {error, not_found} = barrel_docdb:get_doc(Branch, <<"after">>),
    {error, not_found} =
        barrel_docdb:get_doc_for_replication(Branch, <<"after">>),
    %% not in the feed, the index, or history
    {ok, Changes, _} = barrel_docdb:get_changes(Branch, first),
    ?assertEqual([<<"before">>], [maps:get(id, C) || C <- Changes]),
    {ok, Rows, _} = barrel_docdb:find(Branch, #{
        where => [{path, [<<"k">>], <<"x">>}]}),
    ?assertEqual(1, length(Rows)),
    {ok, 1} = barrel_docdb:fold_history(
        Branch, fun(_, Acc) -> {ok, Acc + 1} end, 0),
    ok.

rewind_restores_deleted_after_t(Config) ->
    Db = ?config(db, Config),
    Branch = ?config(branch, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"d">>,
                                         <<"v">> => 1}),
    T = cursor(Db),
    {ok, #{<<"_rev">> := Rev}} = barrel_docdb:get_doc(Db, <<"d">>),
    {ok, _} = barrel_docdb:delete_doc(Db, <<"d">>, #{rev => Rev}),
    {ok, _} = barrel_docdb:branch_db(Db, Branch, #{at => T}),
    %% alive again on the branch, still deleted on the parent
    {ok, #{<<"v">> := 1}} = barrel_docdb:get_doc(Branch, <<"d">>),
    {error, not_found} = barrel_docdb:get_doc(Db, <<"d">>),
    ok.

rewind_restores_tombstone(Config) ->
    Db = ?config(db, Config),
    Branch = ?config(branch, Config),
    {ok, #{<<"rev">> := R}} = barrel_docdb:put_doc(
        Db, #{<<"id">> => <<"d">>, <<"v">> => 1}),
    {ok, _} = barrel_docdb:delete_doc(Db, <<"d">>, #{rev => R}),
    T = cursor(Db),
    %% recreate after T
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"d">>,
                                         <<"v">> => 2}),
    {ok, _} = barrel_docdb:branch_db(Db, Branch, #{at => T}),
    %% at T the doc was a tombstone
    {error, not_found} = barrel_docdb:get_doc(Branch, <<"d">>),
    {ok, #{deleted := true}} =
        barrel_docdb:get_doc_for_replication(Branch, <<"d">>),
    {ok, #{<<"v">> := 2}} = barrel_docdb:get_doc(Db, <<"d">>),
    ok.

rewind_feed_replay(Config) ->
    Db = ?config(db, Config),
    Branch = ?config(branch, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"a">>}),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"b">>}),
    {ok, PreChanges, T} = barrel_docdb:get_changes(Db, first),
    PreRows = [{maps:get(id, C), maps:get(hlc, C)} || C <- PreChanges],
    {ok, #{<<"_rev">> := RevA}} = barrel_docdb:get_doc(Db, <<"a">>),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"a">>,
                                         <<"v">> => 2,
                                         <<"_rev">> => RevA}),
    {ok, _} = barrel_docdb:branch_db(Db, Branch, #{at => T}),
    %% the branch feed replays the docs at their ORIGINAL HLCs
    {ok, Changes, Last} = barrel_docdb:get_changes(Branch, first),
    ?assertEqual(PreRows,
                 [{maps:get(id, C), maps:get(hlc, C)} || C <- Changes]),
    %% and nothing after T remains
    ?assertNot(barrel_hlc:less(T, Last)),
    ok.

rewind_query_index(Config) ->
    Db = ?config(db, Config),
    Branch = ?config(branch, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"d">>,
                                         <<"color">> => <<"red">>}),
    T = cursor(Db),
    {ok, #{<<"_rev">> := R}} = barrel_docdb:get_doc(Db, <<"d">>),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"d">>,
                                         <<"color">> => <<"blue">>,
                                         <<"_rev">> => R}),
    {ok, _} = barrel_docdb:branch_db(Db, Branch, #{at => T}),
    %% the restored value is queryable; the post-T value is gone
    {ok, Red, _} = barrel_docdb:find(Branch, #{
        where => [{path, [<<"color">>], <<"red">>}]}),
    ?assertEqual(1, length(Red)),
    {ok, Blue, _} = barrel_docdb:find(Branch, #{
        where => [{path, [<<"color">>], <<"blue">>}]}),
    ?assertEqual(0, length(Blue)),
    %% the path-filtered changes feed agrees
    {ok, PathChanges, _} = barrel_docdb:get_changes(
        Branch, first, #{paths => [<<"color/red">>]}),
    ?assertEqual(1, length(PathChanges)),
    ok.

rewind_channels(Config) ->
    Db0 = ?config(db, Config),
    DataDir = ?config(data_dir, Config),
    Branch = ?config(branch, Config),
    Channels = #{<<"reds">> => [<<"color/red">>]},
    ok = barrel_docdb:close_db(Db0),
    {ok, _} = barrel_docdb:create_db(Db0, #{data_dir => DataDir,
                                            channels => Channels}),
    {ok, _} = barrel_docdb:put_doc(Db0, #{<<"id">> => <<"d">>,
                                          <<"color">> => <<"red">>}),
    T = cursor(Db0),
    {ok, #{<<"_rev">> := R}} = barrel_docdb:get_doc(Db0, <<"d">>),
    %% departs the channel after T
    {ok, _} = barrel_docdb:put_doc(Db0, #{<<"id">> => <<"d">>,
                                          <<"color">> => <<"blue">>,
                                          <<"_rev">> => R}),
    {ok, _} = barrel_docdb:branch_db(Db0, Branch, #{at => T}),
    {ok, ChanChanges, _} = barrel_docdb:get_changes(
        Branch, first, #{channel => <<"reds">>}),
    ?assertEqual([<<"d">>], [maps:get(id, C) || C <- ChanChanges]),
    ok.

rewind_outbox_swept(Config) ->
    Db = ?config(db, Config),
    Branch = ?config(branch, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"pre">>},
                                   #{outbox => [<<"t">>]}),
    T = cursor(Db),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"post">>},
                                   #{outbox => [<<"t">>]}),
    {ok, _} = barrel_docdb:branch_db(Db, Branch, #{at => T}),
    Pending = barrel_docdb:outbox_fold(
        Branch, <<"t">>,
        fun(E, Acc) -> {ok, [maps:get(id, E) | Acc]} end, []),
    ?assertEqual([<<"pre">>], Pending),
    ok.

rewind_floor_guard(Config) ->
    Db = ?config(db, Config),
    DataDir = ?config(data_dir, Config),
    Branch = ?config(branch, Config),
    %% a tight retention window, then sweep past T
    ok = barrel_docdb:close_db(Db),
    {ok, _} = barrel_docdb:create_db(Db, #{data_dir => DataDir,
                                           retention_period => 1}),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"d">>,
                                         <<"v">> => 1}),
    T = cursor(Db),
    {ok, #{<<"_rev">> := R}} = barrel_docdb:get_doc(Db, <<"d">>),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"d">>,
                                         <<"v">> => 2,
                                         <<"_rev">> => R}),
    timer:sleep(1100),
    {ok, _} = barrel_docdb:sweep_retention(Db),
    ?assertEqual({error, pitr_window_exceeded},
                 barrel_docdb:branch_db(Db, Branch, #{at => T})),
    %% the aborted fork left nothing behind
    ?assertNot(filelib:is_dir(
        filename:join(DataDir, binary_to_list(Branch)))),
    ok.

rewind_then_write(Config) ->
    Db = ?config(db, Config),
    Branch = ?config(branch, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"d">>,
                                         <<"v">> => 1}),
    T = cursor(Db),
    {ok, #{<<"_rev">> := R}} = barrel_docdb:get_doc(Db, <<"d">>),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"d">>,
                                         <<"v">> => 2,
                                         <<"_rev">> => R}),
    {ok, _} = barrel_docdb:branch_db(Db, Branch, #{at => T}),
    %% new writes on the rewound branch work and the feed stays ordered
    {ok, #{<<"_rev">> := BR}} = barrel_docdb:get_doc(Branch, <<"d">>),
    {ok, _} = barrel_docdb:put_doc(Branch, #{<<"id">> => <<"d">>,
                                             <<"v">> => 3,
                                             <<"_rev">> => BR}),
    {ok, _} = barrel_docdb:put_doc(Branch, #{<<"id">> => <<"e">>}),
    {ok, #{<<"v">> := 3}} = barrel_docdb:get_doc(Branch, <<"d">>),
    {ok, Changes, _} = barrel_docdb:get_changes(Branch, first),
    Hlcs = [maps:get(hlc, C) || C <- Changes],
    ?assertEqual(Hlcs, lists:sort(fun(A, B) ->
        barrel_hlc:less(A, B) orelse barrel_hlc:equal(A, B)
    end, Hlcs)),
    %% the branch's post-fork writes are strictly after T
    {ok, PostT, _} = barrel_docdb:get_changes(Branch, T),
    ?assertEqual(2, length(PostT)),
    ok.
