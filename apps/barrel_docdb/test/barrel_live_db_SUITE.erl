%%% @doc barrel_live on real databases: conflicts replicate whole, edits
%%% replace what they observed, old peers never remove anything, upgrade
%%% conversion, and automatic resolution.
-module(barrel_live_db_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([conflict_reaches_both_sides/1,
         loser_survives_many_edits/1,
         edit_winner_keeps_conflict/1,
         observed_replaces_all/1,
         recreate_over_deletes/1,
         read_returns_observed/1,
         old_peer_never_removes/1,
         relayed_version_keeps_sibling/1,
         converted_doc_keeps_history_seen/1,
         resolver_one_version_everywhere/1,
         resolver_three_writers/1,
         different_resolvers_terminate/1,
         filtered_peer_gets_delete/1,
         filtered_peer_gets_delete_by_query/1,
         filter_matches_live_conflict/1]).

all() ->
    [conflict_reaches_both_sides, loser_survives_many_edits,
     edit_winner_keeps_conflict, observed_replaces_all, recreate_over_deletes,
     read_returns_observed, old_peer_never_removes,
     relayed_version_keeps_sibling, converted_doc_keeps_history_seen,
     resolver_one_version_everywhere, resolver_three_writers,
     different_resolvers_terminate, filtered_peer_gets_delete,
     filtered_peer_gets_delete_by_query, filter_matches_live_conflict].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Dir = "/tmp/barrel_live_db_" ++ integer_to_list(erlang:system_time(millisecond)),
    [{dir, Dir} | Config].

end_per_suite(Config) ->
    os:cmd("rm -rf " ++ ?config(dir, Config)),
    ok.

init_per_testcase(TC, Config) ->
    Opts = case TC of
        resolver_one_version_everywhere -> #{conflict_resolver => fun max_v/1};
        resolver_three_writers -> #{conflict_resolver => fun max_v/1};
        _ -> #{}
    end,
    Dbs = [open(TC, N, Opts, Config) || N <- [a, b, c]],
    [{dbs, Dbs} | Config].

end_per_testcase(_TC, Config) ->
    [barrel_docdb:delete_db(D) || D <- ?config(dbs, Config)],
    ok.

open(TC, N, Opts, Config) ->
    Db = <<(atom_to_binary(TC))/binary, "_", (atom_to_binary(N))/binary>>,
    _ = barrel_docdb:delete_db(Db),
    {ok, _} = barrel_docdb:create_db(Db, Opts#{data_dir => ?config(dir, Config)}),
    Db.

%% Order-independent resolver: the highest "v" wins.
max_v(Versions) ->
    Live = [B || #{deleted := false, body := B} <- Versions],
    case Live of
        [] -> delete;
        _ -> {merge, #{<<"v">> => lists:max([maps:get(<<"v">>, B, 0) || B <- Live])}}
    end.

min_v(Versions) ->
    {merge, #{<<"v">> => lists:min([maps:get(<<"v">>, B, 0)
                                    || #{body := B} <- Versions])}}.

%%====================================================================

%% The original bug: a concurrent version stayed on the replica that
%% saw the conflict. Now both replicas hold both versions.
conflict_reaches_both_sides(Config) ->
    [A, B | _] = ?config(dbs, Config),
    put(A, <<"d">>, 1),
    sync(A, B),
    edit(A, <<"d">>, 2),
    edit(B, <<"d">>, 3),
    sync(A, B),
    {ok, CA} = barrel_docdb:get_conflicts(A, <<"d">>),
    {ok, CB} = barrel_docdb:get_conflicts(B, <<"d">>),
    ?assertMatch([_], CA),
    ?assertEqual(CA, CB),
    ?assertEqual(rev(A, <<"d">>), rev(B, <<"d">>)).

%% Nine edits of the winner on one side keep the other side's loser and
%% add no conflict.
loser_survives_many_edits(Config) ->
    [A, B | _] = ?config(dbs, Config),
    put(A, <<"d">>, 0),
    sync(A, B),
    edit(A, <<"d">>, 1),
    edit(B, <<"d">>, 2),
    sync(A, B),
    {ok, [Loser]} = barrel_docdb:get_conflicts(A, <<"d">>),
    [edit(A, <<"d">>, 10 + I) || I <- lists:seq(1, 9)],
    sync(A, B),
    ?assertEqual({ok, [Loser]}, barrel_docdb:get_conflicts(A, <<"d">>)),
    ?assertEqual({ok, [Loser]}, barrel_docdb:get_conflicts(B, <<"d">>)).

edit_winner_keeps_conflict(Config) ->
    [A, B | _] = ?config(dbs, Config),
    conflict(A, B),
    {ok, [Loser]} = barrel_docdb:get_conflicts(A, <<"d">>),
    Winner = rev(A, <<"d">>),
    {ok, _} = barrel_docdb:put_doc(A, #{<<"id">> => <<"d">>, <<"_rev">> => Winner,
                                        <<"v">> => 9}),
    ?assertEqual({ok, [Loser]}, barrel_docdb:get_conflicts(A, <<"d">>)),
    ?assertEqual({error, conflict},
                 barrel_docdb:put_doc(A, #{<<"id">> => <<"d">>, <<"_rev">> => Loser,
                                           <<"v">> => 1})).

observed_replaces_all(Config) ->
    [A, B | _] = ?config(dbs, Config),
    conflict(A, B),
    {ok, #{<<"_observed">> := Seen}} =
        barrel_docdb:get_doc(A, <<"d">>, #{conflicts => true}),
    {ok, _} = barrel_docdb:put_doc(A, #{<<"id">> => <<"d">>, <<"_observed">> => Seen,
                                        <<"v">> => 9}),
    ?assertEqual({ok, []}, barrel_docdb:get_conflicts(A, <<"d">>)),
    sync(A, B),
    ?assertEqual({ok, []}, barrel_docdb:get_conflicts(B, <<"d">>)),
    {ok, #{<<"v">> := 9}} = barrel_docdb:get_doc(B, <<"d">>).

%% Two concurrent deletes, then a write without a rev: it replaces both.
recreate_over_deletes(Config) ->
    [A, B | _] = ?config(dbs, Config),
    put(A, <<"d">>, 1),
    sync(A, B),
    {ok, _} = barrel_docdb:delete_doc(A, <<"d">>, #{rev => rev(A, <<"d">>)}),
    {ok, _} = barrel_docdb:delete_doc(B, <<"d">>, #{rev => rev(B, <<"d">>)}),
    sync(A, B),
    {ok, [_]} = barrel_docdb:get_conflicts(A, <<"d">>),
    {ok, _} = barrel_docdb:put_doc(A, #{<<"id">> => <<"d">>, <<"v">> => 2}),
    ?assertEqual({ok, []}, barrel_docdb:get_conflicts(A, <<"d">>)),
    sync(A, B),
    {ok, #{<<"v">> := 2}} = barrel_docdb:get_doc(B, <<"d">>).

read_returns_observed(Config) ->
    [A, B | _] = ?config(dbs, Config),
    conflict(A, B),
    {ok, #{<<"_rev">> := Rev, <<"_conflicts">> := [L], <<"_observed">> := Obs}} =
        barrel_docdb:get_doc(A, <<"d">>, #{conflicts => true}),
    ?assertEqual(lists:sort([Rev, L]), lists:sort(Obs)).

%% A version from an old peer that dominates the current one is kept as
%% a conflict, never replaces it; the exact same version again is
%% ignored.
old_peer_never_removes(Config) ->
    [A | _] = ?config(dbs, Config),
    put(A, <<"d">>, 1),
    Cur = barrel_version:from_token(rev(A, <<"d">>)),
    Old = barrel_version:new(barrel_hlc:new_hlc(), <<"oldpeer">>),
    VV = barrel_vv:encode(barrel_vv:bump(barrel_vv:bump(barrel_vv:new(), Cur), Old)),
    Tok = barrel_version:to_token(Old),
    {ok, _, _} = barrel_docdb:put_version(A, #{<<"id">> => <<"d">>, <<"v">> => 2},
                                          Tok, VV, false),
    {ok, Conflicts} = barrel_docdb:get_conflicts(A, <<"d">>),
    ?assertEqual(1, length(Conflicts)),
    {ok, _, _} = barrel_docdb:put_version(A, #{<<"id">> => <<"d">>, <<"v">> => 2},
                                          Tok, VV, false),
    ?assertEqual({ok, Conflicts}, barrel_docdb:get_conflicts(A, <<"d">>)).

%% A holds a1 and b; A edits the winner b into a2 and keeps a1. An old
%% peer relays a2 to C. When A and C merge, a1 survives.
relayed_version_keeps_sibling(Config) ->
    [A, B, C] = ?config(dbs, Config),
    put(A, <<"d">>, 0),
    sync(A, B),
    edit(A, <<"d">>, 1),
    edit(B, <<"d">>, 2),
    sync(A, B),
    {ok, [A1]} = barrel_docdb:get_conflicts(A, <<"d">>),
    edit(A, <<"d">>, 3),
    {ok, #{doc := Doc, version := Tok, vv := VV}} =
        barrel_docdb:get_doc_for_replication(A, <<"d">>),
    {ok, _, _} = barrel_docdb:put_version(C, Doc, Tok, VV, false),
    sync(A, C),
    ?assertEqual({ok, [A1]}, barrel_docdb:get_conflicts(C, <<"d">>)),
    ?assertEqual({ok, [A1]}, barrel_docdb:get_conflicts(A, <<"d">>)).

%% A document written before the context column existed: its stored
%% versions count as seen, so a stale old-peer copy of one is ignored.
converted_doc_keeps_history_seen(Config) ->
    [A | _] = ?config(dbs, Config),
    put(A, <<"d">>, 1),
    R1 = rev(A, <<"d">>),
    edit(A, <<"d">>, 2),
    strip_ctx(A, <<"d">>),
    V1 = barrel_version:from_token(R1),
    {ok, _, _} = barrel_docdb:put_version(
        A, #{<<"id">> => <<"d">>, <<"v">> => 1}, R1,
        barrel_vv:encode(barrel_vv:bump(barrel_vv:new(), V1)), false),
    ?assertEqual({ok, []}, barrel_docdb:get_conflicts(A, <<"d">>)).

%% Two replicas resolve the same conflict with the same function: the
%% same version, no new conflict.
resolver_one_version_everywhere(Config) ->
    [A, B | _] = ?config(dbs, Config),
    put(A, <<"d">>, 0),
    sync(A, B),
    edit(A, <<"d">>, 5),
    edit(B, <<"d">>, 7),
    sync(A, B),
    ?assertEqual({ok, []}, barrel_docdb:get_conflicts(A, <<"d">>)),
    ?assertEqual({ok, []}, barrel_docdb:get_conflicts(B, <<"d">>)),
    ?assertEqual(rev(A, <<"d">>), rev(B, <<"d">>)),
    {ok, #{<<"v">> := 7}} = barrel_docdb:get_doc(A, <<"d">>).

resolver_three_writers(Config) ->
    [A, B, C] = ?config(dbs, Config),
    put(A, <<"d">>, 0),
    sync(A, B), sync(A, C),
    edit(A, <<"d">>, 3),
    edit(B, <<"d">>, 9),
    edit(C, <<"d">>, 4),
    sync(A, B),
    sync(B, C),
    sync(A, C),
    sync(A, B),
    [?assertEqual({ok, []}, barrel_docdb:get_conflicts(D, <<"d">>)) || D <- [A, B, C]],
    ?assertEqual(rev(A, <<"d">>), rev(B, <<"d">>)),
    ?assertEqual(rev(B, <<"d">>), rev(C, <<"d">>)),
    {ok, #{<<"v">> := 9}} = barrel_docdb:get_doc(C, <<"d">>).

%% Replicas running different functions: it ends, nothing is lost or
%% emptied (the result depends on sync order).
different_resolvers_terminate(Config) ->
    Dir = ?config(dir, Config),
    [A0, B0 | _] = ?config(dbs, Config),
    ok = barrel_docdb:close_db(A0),
    ok = barrel_docdb:close_db(B0),
    {ok, _} = barrel_docdb:create_db(A0, #{data_dir => Dir,
                                           conflict_resolver => fun max_v/1}),
    {ok, _} = barrel_docdb:create_db(B0, #{data_dir => Dir,
                                           conflict_resolver => fun min_v/1}),
    put(A0, <<"d">>, 0),
    sync(A0, B0),
    edit(A0, <<"d">>, 2),
    edit(B0, <<"d">>, 8),
    [sync(A0, B0) || _ <- lists:seq(1, 5)],
    {ok, #{state := SA}} = barrel_docdb:get_doc_state(A0, <<"d">>),
    {ok, #{state := SB}} = barrel_docdb:get_doc_state(B0, <<"d">>),
    ?assertEqual(SA, SB),
    ?assertNotEqual([], barrel_live:tokens(barrel_live:decode_state(SA))).

%%====================================================================

put(Db, Id, V) ->
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => Id, <<"v">> => V}),
    ok.

edit(Db, Id, V) ->
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => Id, <<"_rev">> => rev(Db, Id),
                                         <<"v">> => V}),
    ok.

rev(Db, Id) ->
    {ok, #{<<"_rev">> := R}} = barrel_docdb:get_doc(Db, Id, #{include_deleted => true}),
    R.

conflict(A, B) ->
    put(A, <<"d">>, 0),
    sync(A, B),
    edit(A, <<"d">>, 1),
    edit(B, <<"d">>, 2),
    sync(A, B).

%% Sync both ways until nothing is written.
sync(A, B) -> sync(A, B, 6).
sync(_A, _B, 0) -> ct:fail(no_quiescence);
sync(A, B, N) ->
    {ok, R1} = barrel_rep:replicate(A, B, #{attachments => false}),
    {ok, R2} = barrel_rep:replicate(B, A, #{attachments => false}),
    case maps:get(docs_written, R1) + maps:get(docs_written, R2) of
        0 -> ok;
        _ -> sync(A, B, N - 1)
    end.

%% Rewrite the entity without its context column, as an older version
%% stored it.
strip_ctx(Db, Id) ->
    {ok, Pid} = barrel_docdb:open_db(Db),
    {ok, Store} = barrel_db_server:get_store_ref(Pid),
    Key = barrel_store_keys:doc_entity(Db, Id),
    {ok, Cols} = barrel_store_rocksdb:get_entity(Store, Key),
    ok = barrel_store_rocksdb:put_entity(Store, Key,
                                         lists:keydelete(<<"ctx">>, 1, Cols), []).

%% A peer that syncs only blue documents gets the delete of one it holds.
filtered_peer_gets_delete(Config) ->
    [A, _, C] = ?config(dbs, Config),
    Blue = #{paths => [<<"type/blue">>]},
    {ok, #{<<"rev">> := R}} = barrel_docdb:put_doc(A, #{<<"id">> => <<"d">>,
                                                        <<"type">> => <<"blue">>}),
    {ok, _} = barrel_rep:replicate(A, C, #{filter => Blue, attachments => false}),
    {ok, _} = barrel_docdb:get_doc(C, <<"d">>),
    {ok, _} = barrel_docdb:delete_doc(A, <<"d">>, #{rev => R}),
    {ok, _} = barrel_rep:replicate(A, C, #{filter => Blue, attachments => false}),
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(C, <<"d">>)).

filtered_peer_gets_delete_by_query(Config) ->
    [A, _, C] = ?config(dbs, Config),
    Q = #{query => #{where => [{path, [<<"type">>], <<"blue">>}]}},
    {ok, #{<<"rev">> := R}} = barrel_docdb:put_doc(A, #{<<"id">> => <<"d">>,
                                                        <<"type">> => <<"blue">>}),
    {ok, _} = barrel_rep:replicate(A, C, #{filter => Q, attachments => false}),
    {ok, _} = barrel_docdb:get_doc(C, <<"d">>),
    {ok, _} = barrel_docdb:delete_doc(A, <<"d">>, #{rev => R}),
    {ok, _} = barrel_rep:replicate(A, C, #{filter => Q, attachments => false}),
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(C, <<"d">>)).

%% The winner is red but a live conflict is blue: a blue-only peer gets
%% the whole state.
filter_matches_live_conflict(Config) ->
    [A, B, C] = ?config(dbs, Config),
    {ok, _} = barrel_docdb:put_doc(A, #{<<"id">> => <<"d">>, <<"type">> => <<"blue">>}),
    sync(A, B),
    {ok, _} = barrel_docdb:put_doc(A, #{<<"id">> => <<"d">>, <<"_rev">> => rev(A, <<"d">>),
                                        <<"type">> => <<"blue">>, <<"v">> => 1}),
    {ok, _} = barrel_docdb:put_doc(B, #{<<"id">> => <<"d">>, <<"_rev">> => rev(B, <<"d">>),
                                        <<"type">> => <<"red">>}),
    sync(A, B),
    {ok, [_]} = barrel_docdb:get_conflicts(A, <<"d">>),
    {ok, _} = barrel_rep:replicate(A, C, #{filter => #{paths => [<<"type/blue">>]},
                                           attachments => false}),
    ?assertEqual(barrel_docdb:get_conflicts(A, <<"d">>),
                 barrel_docdb:get_conflicts(C, <<"d">>)).
