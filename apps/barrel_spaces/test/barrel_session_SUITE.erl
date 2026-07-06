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

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [t_create_get_touch, t_sliding_ttl, t_messages_chronological,
     t_message_ranges, t_data_summary_pinned, t_delete_cascade,
     t_janitor_orphans, t_list_by_agent].

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
