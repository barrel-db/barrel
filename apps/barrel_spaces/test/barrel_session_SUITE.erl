%%%-------------------------------------------------------------------
%%% @doc Sessions: sliding TTL over the space's doc-TTL machinery,
%%% chronological messages, data/summary/pinned context, cascade
%%% deletes, and the janitor collecting orphaned messages after the
%%% sweeper tombstones an idle session.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_session_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([
    t_create_get_touch/1,
    t_sliding_ttl/1,
    t_messages_chronological/1,
    t_message_ranges/1,
    t_data_summary_pinned/1,
    t_delete_cascade/1,
    t_janitor_orphans/1,
    t_list_by_agent/1
]).

-export([t_no_ttl_durable/1, t_import_with_ids/1, t_list_match_indexed/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [t_create_get_touch, t_sliding_ttl, t_messages_chronological,
     t_message_ranges, t_data_summary_pinned, t_delete_cascade,
     t_janitor_orphans, t_list_by_agent, t_no_ttl_durable, t_import_with_ids, t_list_match_indexed].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_spaces),
    application:set_env(barrel_docdb, data_dir, ?config(priv_dir, Config)),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(Case, Config) ->
    Vec = #{dimension => 3, bm25_backend => memory,
            db_path => filename:join(?config(priv_dir, Config),
                                     atom_to_list(Case) ++ "_vec")},
    {ok, Space} = barrel_spaces:create_space(#{
        label => atom_to_binary(Case, utf8),
        session_ttl => 60,
        vectordb => Vec}),
    [{space, Space} | Config].

end_per_testcase(_Case, Config) ->
    #{id := Id} = ?config(space, Config),
    _ = barrel_spaces:drop_space(Id),
    ok.

%%====================================================================
%% Cases
%%====================================================================

t_create_get_touch(Config) ->
    Space = ?config(space, Config),
    {ok, Sid} = barrel_session:create(Space, #{agent => <<"a1">>}),
    ?assertMatch(<<"ses_", _/binary>>, Sid),
    {ok, Doc} = barrel_session:get(Space, Sid),
    ?assertEqual(<<"a1">>, maps:get(<<"agent">>, Doc)),
    %% ttl defaults from the space's session_ttl
    ?assertEqual(60, maps:get(<<"ttl">>, Doc)),
    {ok, ExpiresAt} = barrel_session:touch(Space, Sid),
    ?assert(ExpiresAt > barrel_spaces:now_ms()),
    ok.

t_sliding_ttl(Config) ->
    Space = ?config(space, Config),
    %% sub-second ttl: every touch slides the window
    {ok, Sid} = barrel_session:create(Space, #{ttl => 1}),
    timer:sleep(600),
    {ok, _} = barrel_session:touch(Space, Sid),
    timer:sleep(600),
    %% 1.2s after create but only 0.6s after the touch: still alive
    {ok, _} = barrel_session:get(Space, Sid),
    timer:sleep(1100),
    %% idle past the ttl: lazily gone
    ?assertEqual({error, not_found}, barrel_session:get(Space, Sid)),
    ?assertEqual({error, not_found}, barrel_session:touch(Space, Sid)),
    ok.

t_messages_chronological(Config) ->
    Space = ?config(space, Config),
    {ok, Sid} = barrel_session:create(Space, #{}),
    lists:foreach(
        fun(N) ->
            {ok, _} = barrel_session:add_message(Space, Sid, #{
                role => <<"user">>,
                content => <<"m", (integer_to_binary(N))/binary>>})
        end, lists:seq(1, 5)),
    {ok, Messages} = barrel_session:get_messages(Space, Sid),
    ?assertEqual([<<"m1">>, <<"m2">>, <<"m3">>, <<"m4">>, <<"m5">>],
                 [maps:get(<<"content">>, M) || M <- Messages]),
    ok.

t_message_ranges(Config) ->
    Space = ?config(space, Config),
    {ok, Sid} = barrel_session:create(Space, #{}),
    {ok, _} = barrel_session:add_message(Space, Sid,
                                         #{role => <<"user">>,
                                           content => <<"old">>}),
    timer:sleep(5),
    Mid = barrel_spaces:now_ms(),
    timer:sleep(5),
    {ok, _} = barrel_session:add_message(Space, Sid,
                                         #{role => <<"user">>,
                                           content => <<"new">>}),
    {ok, [OnlyNew]} = barrel_session:get_messages(Space, Sid,
                                                  #{since => Mid}),
    ?assertEqual(<<"new">>, maps:get(<<"content">>, OnlyNew)),
    {ok, [OnlyOld]} = barrel_session:get_messages(Space, Sid,
                                                  #{before => Mid}),
    ?assertEqual(<<"old">>, maps:get(<<"content">>, OnlyOld)),
    {ok, [Newest]} = barrel_session:get_messages(
        Space, Sid, #{order => desc, limit => 1}),
    ?assertEqual(<<"new">>, maps:get(<<"content">>, Newest)),
    ok.

t_data_summary_pinned(Config) ->
    Space = ?config(space, Config),
    {ok, Sid} = barrel_session:create(Space, #{}),
    {ok, _} = barrel_session:set_data(Space, Sid, <<"cursor">>, 42),
    ?assertEqual({ok, 42},
                 barrel_session:get_data(Space, Sid, <<"cursor">>)),
    ?assertEqual({error, not_found},
                 barrel_session:get_data(Space, Sid, <<"nope">>)),
    {ok, _} = barrel_session:set_summary(Space, Sid, <<"so far">>),
    {ok, Doc} = barrel_session:get(Space, Sid),
    ?assertEqual(<<"so far">>, maps:get(<<"summary">>, Doc)),
    %% pinned items sort by priority, 0 first
    {ok, P1} = barrel_session:pin_context(Space, Sid,
                                          #{content => <<"later">>,
                                            priority => 9}),
    {ok, _P2} = barrel_session:pin_context(Space, Sid,
                                           #{content => <<"first">>,
                                             priority => 0}),
    {ok, [A, B]} = barrel_session:list_pinned(Space, Sid),
    ?assertEqual(<<"first">>, maps:get(<<"content">>, A)),
    ?assertEqual(<<"later">>, maps:get(<<"content">>, B)),
    {ok, _} = barrel_session:unpin_context(Space, Sid, P1),
    {ok, [_]} = barrel_session:list_pinned(Space, Sid),
    ok.

t_delete_cascade(Config) ->
    #{db := #{docdb := DbBin}} = Space = ?config(space, Config),
    {ok, Sid} = barrel_session:create(Space, #{}),
    {ok, _} = barrel_session:add_message(Space, Sid,
                                         #{role => <<"user">>,
                                           content => <<"x">>}),
    ok = barrel_session:delete(Space, Sid),
    ?assertEqual({error, not_found}, barrel_session:get(Space, Sid)),
    %% no session-prefixed docs remain
    {ok, Left} = barrel_docdb:fold_docs(
        DbBin, fun(D, Acc) -> {ok, [D | Acc]} end, [],
        #{id_prefix => <<"session:", Sid/binary>>}),
    ?assertEqual([], Left),
    ok.

t_janitor_orphans(Config) ->
    #{id := SpaceId} = Space = ?config(space, Config),
    {ok, Sid} = barrel_session:create(Space, #{ttl => 1}),
    {ok, _} = barrel_session:add_message(Space, Sid,
                                         #{role => <<"user">>,
                                           content => <<"orphan">>}),
    %% let the session expire, then force the space's TTL sweep to
    %% tombstone the root
    timer:sleep(1100),
    {ok, SweptCount} = barrel_docdb:sweep_ttl(SpaceId),
    ?assert(SweptCount >= 1),
    %% the message doc is now an orphan; the janitor collects it
    {ok, Deleted} = barrel_spaces_janitor:sweep(),
    ?assert(Deleted >= 1),
    {ok, Messages} = barrel_session:get_messages(Space, Sid),
    ?assertEqual([], Messages),
    ok.

t_list_by_agent(Config) ->
    Space = ?config(space, Config),
    {ok, _} = barrel_session:create(Space, #{agent => <<"alice">>}),
    {ok, _} = barrel_session:create(Space, #{agent => <<"alice">>}),
    {ok, _} = barrel_session:create(Space, #{agent => <<"bob">>}),
    {ok, All} = barrel_session:list(Space),
    ?assertEqual(3, length(All)),
    {ok, Alice} = barrel_session:list(Space, #{agent => <<"alice">>}),
    ?assertEqual(2, length(Alice)),
    {ok, Bob} = barrel_session:list(Space, #{agent => <<"bob">>}),
    ?assertEqual(1, length(Bob)),
    ok.

t_no_ttl_durable(Config) ->
    #{id := SpaceId} = Space = ?config(space, Config),
    {ok, Sid} = barrel_session:create(Space, #{agent => <<"keeper">>,
                                               ttl => infinity}),
    {ok, Doc} = barrel_session:get(Space, Sid),
    ?assertEqual(0, maps:get(<<"ttl">>, Doc)),
    {ok, 0} = barrel_session:touch(Space, Sid),
    {ok, 0} = barrel_session:set_data(Space, Sid, <<"k">>, 1),
    %% the TTL sweeper never collects it
    {ok, _} = barrel_docdb:sweep_ttl(SpaceId),
    {ok, _} = barrel_session:get(Space, Sid),
    %% ttl => 0 means the same thing
    {ok, Sid2} = barrel_session:create(Space, #{ttl => 0}),
    {ok, 0} = barrel_session:touch(Space, Sid2),
    ok.

t_import_with_ids(Config) ->
    Space = ?config(space, Config),
    %% caller-supplied id on create; duplicates conflict, colons refused
    {ok, <<"legacy-1">>} =
        barrel_session:create(Space, #{id => <<"legacy-1">>}),
    ?assertMatch({error, _},
                 barrel_session:create(Space, #{id => <<"legacy-1">>})),
    ?assertEqual({error, invalid_session_id},
                 barrel_session:create(Space, #{id => <<"a:b">>})),
    %% import preserves timestamps; default ttl is never
    T0 = barrel_spaces:now_ms() - 86400000,
    {ok, <<"legacy-2">>} = barrel_session:import_session(Space, #{
        id => <<"legacy-2">>, agent => <<"old-bot">>,
        data => #{<<"user_id">> => <<"u1">>},
        created_at => T0, updated_at => T0}),
    {ok, Doc} = barrel_session:get(Space, <<"legacy-2">>),
    ?assertEqual(T0, maps:get(<<"created_at">>, Doc)),
    ?assertEqual(0, maps:get(<<"ttl">>, Doc)),
    ?assertEqual({error, id_required},
                 barrel_session:import_session(Space, #{agent => <<"x">>})),
    %% messages with their own timestamps sort chronologically and do
    %% not slide the session
    {ok, _} = barrel_session:import_message(Space, <<"legacy-2">>,
        #{role => <<"user">>, content => <<"first">>,
          ts => T0, seq => 1}),
    {ok, _} = barrel_session:import_message(Space, <<"legacy-2">>,
        #{role => <<"assistant">>, content => <<"second">>,
          ts => T0 + 1000, seq => 2}),
    {ok, [M1, M2]} = barrel_session:get_messages(Space, <<"legacy-2">>),
    ?assertEqual(<<"first">>, maps:get(<<"content">>, M1)),
    ?assertEqual(<<"second">>, maps:get(<<"content">>, M2)),
    ok.

t_list_match_indexed(Config) ->
    Space = ?config(space, Config),
    {ok, S1} = barrel_session:create(Space, #{agent => <<"a1">>,
        data => #{<<"user_id">> => <<"u1">>}}),
    {ok, _S2} = barrel_session:create(Space, #{agent => <<"a1">>,
        data => #{<<"user_id">> => <<"u2">>}}),
    {ok, S3} = barrel_session:create(Space, #{agent => <<"a2">>,
        data => #{<<"user_id">> => <<"u1">>}}),
    {ok, ByUser} = barrel_session:list(Space, #{
        match => #{<<"data.user_id">> => <<"u1">>}}),
    ?assertEqual(lists:sort([S1, S3]),
                 lists:sort([maps:get(<<"session">>, D) || D <- ByUser])),
    %% agent composes with match; paths also accept key lists
    {ok, Both} = barrel_session:list(Space, #{agent => <<"a1">>,
        match => #{[<<"data">>, <<"user_id">>] => <<"u1">>}}),
    ?assertEqual([S1], [maps:get(<<"session">>, D) || D <- Both]),
    {ok, Limited} = barrel_session:list(Space, #{agent => <<"a1">>,
                                                 limit => 1}),
    ?assertEqual(1, length(Limited)),
    ok.
