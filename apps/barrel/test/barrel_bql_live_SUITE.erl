%%%-------------------------------------------------------------------
%%% @doc Live BQL queries: snapshot + ready, add/change deltas via
%%% barrel_query_sub, remove synthesis via the changes stream (unmatch
%%% and delete), id-bounded subscriptions, cleanup, and matcher-drift
%%% guards for the exotic operators.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_bql_live_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([snapshot_and_ready/1,
         add_and_change_events/1,
         unmatch_emits_remove/1,
         delete_emits_remove/1,
         nonmatching_writes_are_silent/1,
         projection_deltas/1,
         limit_caps_snapshot_only/1,
         id_prefix_subscription/1,
         matcher_drift_operators/1,
         owner_death_cleanup/1,
         unsubscribe_idempotent/1,
         missing_subscribe_rejected/1]).

all() ->
    [snapshot_and_ready,
     add_and_change_events,
     unmatch_emits_remove,
     delete_emits_remove,
     nonmatching_writes_are_silent,
     projection_deltas,
     limit_caps_snapshot_only,
     id_prefix_subscription,
     matcher_drift_operators,
     owner_death_cleanup,
     unsubscribe_idempotent,
     missing_subscribe_rejected].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel),
    Dir = "/tmp/barrel_bql_live_test_"
        ++ integer_to_list(erlang:system_time(millisecond)),
    [{dir, Dir} | Config].

end_per_suite(Config) ->
    os:cmd("rm -rf " ++ ?config(dir, Config)),
    ok.

init_per_testcase(TC, Config) ->
    Name = list_to_atom("bqll_" ++ atom_to_list(TC)),
    {ok, Db} = barrel:open(Name, #{
        docdb => #{data_dir => ?config(dir, Config)},
        vectordb => #{dimension => 3,
                      db_path => ?config(dir, Config) ++ "/"
                                 ++ atom_to_list(TC)}}),
    [{db, Db} | Config].

end_per_testcase(_TC, Config) ->
    Db = ?config(db, Config),
    try barrel:close(Db) catch _:_ -> ok end,
    try barrel_docdb:delete_db(maps:get(docdb, Db)) catch _:_ -> ok end,
    ok.

%%====================================================================
%% Helpers
%%====================================================================

put_user(Db, Id, Name, Status) ->
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => Id,
                                   <<"type">> => <<"user">>,
                                   <<"name">> => Name,
                                   <<"status">> => Status}).

recv_rows(Ref) ->
    receive {bql_rows, Ref, Rows} -> Rows
    after 2000 -> ct:fail(no_rows)
    end.

recv_ready(Ref) ->
    receive {bql_ready, Ref, Meta} -> Meta
    after 2000 -> ct:fail(no_ready)
    end.

recv_change(Ref) ->
    receive {bql_change, Ref, Change} -> Change
    after 2000 -> ct:fail(no_change)
    end.

assert_silence(Ref) ->
    receive
        {bql_change, Ref, Unexpected} -> ct:fail({unexpected, Unexpected})
    after 400 ->
        ok
    end.

drain_snapshot(Ref) ->
    receive
        {bql_rows, Ref, _} -> drain_snapshot(Ref)
    after 0 ->
        ok
    end.

subscribe_ready(Db, Bql) ->
    {ok, Sub} = barrel:subscribe_query(Db, Bql),
    Meta = recv_ready(maps:get(ref, Sub)),
    drain_snapshot(maps:get(ref, Sub)),
    {Sub, Meta}.

%%====================================================================
%% Cases
%%====================================================================

snapshot_and_ready(Config) ->
    Db = ?config(db, Config),
    put_user(Db, <<"u1">>, <<"Alice">>, <<"active">>),
    put_user(Db, <<"u2">>, <<"Bob">>, <<"active">>),
    put_user(Db, <<"u3">>, <<"Carol">>, <<"inactive">>),
    {ok, Sub} = barrel:subscribe_query(Db,
        "SELECT * FROM db WHERE status = 'active' SUBSCRIBE"),
    #{ref := Ref} = Sub,
    Rows = recv_rows(Ref),
    ?assertEqual([<<"u1">>, <<"u2">>],
                 lists:sort([maps:get(<<"id">>, R) || R <- Rows])),
    #{count := 2} = recv_ready(Ref),
    ok = barrel:unsubscribe_query(Sub).

add_and_change_events(Config) ->
    Db = ?config(db, Config),
    {Sub, #{count := 0}} = subscribe_ready(Db,
        "SELECT * FROM db WHERE status = 'active' SUBSCRIBE"),
    #{ref := Ref} = Sub,
    put_user(Db, <<"u1">>, <<"Alice">>, <<"active">>),
    #{action := add, id := <<"u1">>, row := Row} = recv_change(Ref),
    ?assertEqual(<<"Alice">>, maps:get(<<"name">>, Row)),
    %% update while still matching -> change
    {ok, Doc} = barrel:get_doc(Db, <<"u1">>),
    {ok, _} = barrel:put_doc(Db, Doc#{<<"name">> => <<"Alicia">>}),
    #{action := change, id := <<"u1">>, row := Row2} = recv_change(Ref),
    ?assertEqual(<<"Alicia">>, maps:get(<<"name">>, Row2)),
    ok = barrel:unsubscribe_query(Sub).

unmatch_emits_remove(Config) ->
    Db = ?config(db, Config),
    put_user(Db, <<"u1">>, <<"Alice">>, <<"active">>),
    {Sub, #{count := 1}} = subscribe_ready(Db,
        "SELECT * FROM db WHERE status = 'active' SUBSCRIBE"),
    #{ref := Ref} = Sub,
    %% the doc stops matching: query_sub is silent, the changes-stream
    %% leg must synthesize the remove
    {ok, Doc} = barrel:get_doc(Db, <<"u1">>),
    {ok, _} = barrel:put_doc(Db, Doc#{<<"status">> => <<"disabled">>}),
    #{action := remove, id := <<"u1">>} = recv_change(Ref),
    ok = barrel:unsubscribe_query(Sub).

delete_emits_remove(Config) ->
    Db = ?config(db, Config),
    put_user(Db, <<"u1">>, <<"Alice">>, <<"active">>),
    {Sub, #{count := 1}} = subscribe_ready(Db,
        "SELECT * FROM db WHERE status = 'active' SUBSCRIBE"),
    #{ref := Ref} = Sub,
    {ok, _} = barrel:delete_doc(Db, <<"u1">>),
    #{action := remove, id := <<"u1">>} = recv_change(Ref),
    ok = barrel:unsubscribe_query(Sub).

nonmatching_writes_are_silent(Config) ->
    Db = ?config(db, Config),
    {Sub, _} = subscribe_ready(Db,
        "SELECT * FROM db WHERE status = 'active' SUBSCRIBE"),
    #{ref := Ref} = Sub,
    put_user(Db, <<"u9">>, <<"Zed">>, <<"parked">>),
    assert_silence(Ref),
    ok = barrel:unsubscribe_query(Sub).

projection_deltas(Config) ->
    Db = ?config(db, Config),
    {Sub, _} = subscribe_ready(Db,
        "SELECT name FROM db WHERE status = 'active' SUBSCRIBE"),
    #{ref := Ref} = Sub,
    put_user(Db, <<"u1">>, <<"Alice">>, <<"active">>),
    #{action := add, row := Row} = recv_change(Ref),
    ?assertEqual(#{<<"id">> => <<"u1">>, <<"name">> => <<"Alice">>}, Row),
    ok = barrel:unsubscribe_query(Sub).

limit_caps_snapshot_only(Config) ->
    Db = ?config(db, Config),
    put_user(Db, <<"u1">>, <<"A">>, <<"active">>),
    put_user(Db, <<"u2">>, <<"B">>, <<"active">>),
    put_user(Db, <<"u3">>, <<"C">>, <<"active">>),
    {ok, Sub} = barrel:subscribe_query(Db,
        "SELECT * FROM db WHERE status = 'active' LIMIT 2 SUBSCRIBE"),
    #{ref := Ref} = Sub,
    Rows = recv_rows(Ref),
    ?assertEqual(2, length(Rows)),
    #{count := 2} = recv_ready(Ref),
    %% deltas are NOT limited
    put_user(Db, <<"u4">>, <<"D">>, <<"active">>),
    #{action := add, id := <<"u4">>} = recv_change(Ref),
    ok = barrel:unsubscribe_query(Sub).

id_prefix_subscription(Config) ->
    Db = ?config(db, Config),
    put_user(Db, <<"user:1">>, <<"Alice">>, <<"active">>),
    %% id-only subscription: no engine where, so the changes stream
    %% drives adds too
    {Sub, #{count := 1}} = subscribe_ready(Db,
        "SELECT * FROM db WHERE id LIKE 'user:%' SUBSCRIBE"),
    #{ref := Ref} = Sub,
    put_user(Db, <<"user:2">>, <<"Bob">>, <<"active">>),
    #{action := add, id := <<"user:2">>} = recv_change(Ref),
    put_user(Db, <<"other:1">>, <<"Zed">>, <<"active">>),
    assert_silence(Ref),
    {ok, _} = barrel:delete_doc(Db, <<"user:2">>),
    #{action := remove, id := <<"user:2">>} = recv_change(Ref),
    ok = barrel:unsubscribe_query(Sub).

%% the query_sub trie and its own matcher must not lose adds for the
%% non-equality operators (drift guard against barrel_query:match)
matcher_drift_operators(Config) ->
    Db = ?config(db, Config),
    Cases = [
        {"SELECT * FROM db WHERE name LIKE 'A%' SUBSCRIBE",
         #{<<"id">> => <<"m1">>, <<"name">> => <<"Alice">>}},
        {"SELECT * FROM db WHERE name LIKE 'B_b' SUBSCRIBE",
         #{<<"id">> => <<"m2">>, <<"name">> => <<"Bob">>}},
        {"SELECT * FROM db WHERE status IN ('a', 'b') SUBSCRIBE",
         #{<<"id">> => <<"m3">>, <<"status">> => <<"b">>}},
        {"SELECT * FROM db WHERE score > 5 SUBSCRIBE",
         #{<<"id">> => <<"m4">>, <<"score">> => 7}},
        {"SELECT * FROM db WHERE flag IS NOT MISSING SUBSCRIBE",
         #{<<"id">> => <<"m5">>, <<"flag">> => false}}
    ],
    lists:foreach(
        fun({Bql, Doc}) ->
            {Sub, _} = subscribe_ready(Db, Bql),
            #{ref := Ref} = Sub,
            {ok, _} = barrel:put_doc(Db, Doc),
            Id = maps:get(<<"id">>, Doc),
            ?assertMatch(#{action := add, id := Id},
                         recv_change(Ref)),
            ok = barrel:unsubscribe_query(Sub)
        end,
        Cases).

owner_death_cleanup(Config) ->
    Db = ?config(db, Config),
    Parent = self(),
    Owner = spawn(fun() ->
        {ok, Sub} = barrel:subscribe_query(Db,
            "SELECT * FROM db WHERE status = 'x' SUBSCRIBE"),
        Parent ! {sub, Sub},
        receive die -> ok end
    end),
    Sub = receive {sub, S} -> S after 2000 -> ct:fail(no_sub) end,
    #{pid := LivePid} = Sub,
    ?assert(is_process_alive(LivePid)),
    MRef = erlang:monitor(process, LivePid),
    Owner ! die,
    receive
        {'DOWN', MRef, process, LivePid, _} -> ok
    after 2000 ->
        ct:fail(live_query_survived_owner)
    end.

unsubscribe_idempotent(Config) ->
    Db = ?config(db, Config),
    {Sub, _} = subscribe_ready(Db,
        "SELECT * FROM db WHERE status = 'x' SUBSCRIBE"),
    ok = barrel:unsubscribe_query(Sub),
    ok = barrel:unsubscribe_query(Sub).

missing_subscribe_rejected(Config) ->
    Db = ?config(db, Config),
    ?assertEqual({error, missing_subscribe},
                 barrel:subscribe_query(Db,
                     "SELECT * FROM db WHERE a = 1")).
