%%%-------------------------------------------------------------------
%%% @doc Documents written before 2.0 (edited in place) read the same,
%%% convert on their next write, and convert all at once with
%%% barrel_spaces:migrate/0.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_spaces_migrate_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([t_legacy_revoked_grant/1,
         t_legacy_session_reads/1,
         t_legacy_session_splits_on_write/1,
         t_legacy_handoff/1,
         t_migrate_all/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [t_legacy_revoked_grant, t_legacy_session_reads,
     t_legacy_session_splits_on_write, t_legacy_handoff, t_migrate_all].

init_per_suite(Config) ->
    application:set_env(barrel_docdb, data_dir, ?config(priv_dir, Config)),
    {ok, _} = application:ensure_all_started(barrel_spaces),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(Case, Config) ->
    Reg = <<"mig_", (atom_to_binary(Case))/binary>>,
    ok = application:set_env(barrel_spaces, registry_db, Reg),
    {ok, Space} = barrel_spaces:create_space(#{label => Reg,
                                               vectordb => none}),
    [{space, Space}, {registry, Reg} | Config].

end_per_testcase(_Case, Config) ->
    #{id := Id} = ?config(space, Config),
    _ = barrel_spaces:drop_space(Id),
    _ = barrel_docdb:delete_db(?config(registry, Config)),
    application:unset_env(barrel_spaces, registry_db).

%%====================================================================
%% Cases
%%====================================================================

t_legacy_revoked_grant(Config) ->
    #{id := SpaceId} = ?config(space, Config),
    {ok, Token, #{<<"token_id">> := Tid}} = barrel_caps:grant(SpaceId, #{}),
    At = revoke_in_place(Tid),
    ?assertEqual({error, revoked}, barrel_caps:verify(Token, SpaceId, read)),
    {ok, [G]} = barrel_caps:list(SpaceId),
    ?assertEqual(At, maps:get(<<"revoked_at">>, G)),
    %% the revoke document keeps the original time
    ?assertEqual({ok, 1}, barrel_caps:migrate()),
    ?assertEqual({ok, 0}, barrel_caps:migrate()),
    {ok, #{<<"revoked_at">> := At}} =
        barrel_docdb:get_doc(barrel_spaces:registry_db(),
                             <<"revoke:", Tid/binary>>),
    ok.

t_legacy_session_reads(Config) ->
    Space = ?config(space, Config),
    Sid = put_legacy_session(Space, <<"old-1">>),
    {ok, S} = barrel_session:get(Space, Sid),
    ?assertEqual(#{<<"user_id">> => <<"u1">>, <<"k">> => 1},
                 maps:get(<<"data">>, S)),
    ?assertEqual(<<"legacy summary">>, maps:get(<<"summary">>, S)),
    ?assertEqual([<<"p1">>], [maps:get(<<"id">>, P)
                              || P <- maps:get(<<"pinned">>, S)]),
    ?assertEqual({ok, 1}, barrel_session:get_data(Space, Sid, <<"k">>)),
    %% data in a pre-2.0 header still matches a listing
    {ok, [#{<<"session">> := Sid}]} =
        barrel_session:list(Space, #{match => #{<<"data.user_id">> =>
                                                    <<"u1">>}}),
    ok.

t_legacy_session_splits_on_write(Config) ->
    #{db := Db} = Space = ?config(space, Config),
    Sid = put_legacy_session(Space, <<"old-2">>),
    {ok, _} = barrel_session:delete_data(Space, Sid, <<"k">>),
    {ok, _} = barrel_session:unpin_context(Space, Sid, <<"p1">>),
    %% the header no longer holds the facts; removed ones stay removed
    {ok, Header} = barrel:get_doc(Db, <<"session:", Sid/binary>>),
    ?assertNot(maps:is_key(<<"data">>, Header)),
    ?assertNot(maps:is_key(<<"pinned">>, Header)),
    {ok, S} = barrel_session:get(Space, Sid),
    ?assertEqual(#{<<"user_id">> => <<"u1">>}, maps:get(<<"data">>, S)),
    ?assertEqual([], maps:get(<<"pinned">>, S)),
    ?assertEqual(<<"legacy summary">>, maps:get(<<"summary">>, S)),
    {ok, [#{<<"session">> := Sid}]} =
        barrel_session:list(Space, #{match => #{<<"data.user_id">> =>
                                                    <<"u1">>}}),
    ok.

t_legacy_handoff(Config) ->
    #{id := SpaceId} = ?config(space, Config),
    {ok, #{handoff_id := Hid, token := T}} =
        barrel_handoff:create(SpaceId, #{task_name => <<"old">>}),
    accept_in_place(Hid, <<"bob">>),
    {ok, #{<<"status">> := <<"accepted">>, <<"accepted_by">> := <<"bob">>}} =
        barrel_handoff:get(Hid),
    ?assertEqual({error, already_accepted},
                 barrel_handoff:accept(T, #{agent => <<"eve">>,
                                            session => false})),
    {ok, #{<<"status">> := <<"completed">>, <<"accepted_by">> := <<"bob">>}} =
        barrel_handoff:complete(T, #{}),
    %% completing wrote the accept step the old document held
    {ok, _} = barrel_docdb:get_doc(barrel_spaces:registry_db(),
                                   <<"handoff:", Hid/binary,
                                     ":accept:bob">>),
    ok.

t_migrate_all(Config) ->
    #{id := SpaceId} = Space = ?config(space, Config),
    {ok, _, #{<<"token_id">> := Tid}} = barrel_caps:grant(SpaceId, #{}),
    _ = revoke_in_place(Tid),
    {ok, #{handoff_id := Hid}} =
        barrel_handoff:create(SpaceId, #{task_name => <<"old">>}),
    accept_in_place(Hid, <<"bob">>),
    _ = put_legacy_session(Space, <<"old-3">>),
    {ok, #{grants := 1, handoffs := 1, sessions := 1, skipped := []}} =
        barrel_spaces:migrate(),
    {ok, #{grants := 0, handoffs := 0, sessions := 0}} =
        barrel_spaces:migrate(),
    {ok, S} = barrel_session:get(Space, <<"old-3">>),
    ?assertEqual(<<"legacy summary">>, maps:get(<<"summary">>, S)),
    ok.

%%====================================================================
%% Pre-2.0 writes
%%====================================================================

revoke_in_place(Tid) ->
    Reg = barrel_spaces:registry_db(),
    {ok, G} = barrel_docdb:get_doc(Reg, <<"grant:", Tid/binary>>),
    At = barrel_spaces:now_ms() - 1000,
    {ok, _} = barrel_docdb:put_doc(Reg, G#{<<"revoked_at">> => At}),
    At.

accept_in_place(Hid, Agent) ->
    Reg = barrel_spaces:registry_db(),
    {ok, H} = barrel_docdb:get_doc(Reg, <<"handoff:", Hid/binary>>),
    {ok, _} = barrel_docdb:put_doc(Reg, H#{<<"status">> => <<"accepted">>,
                                           <<"accepted_by">> => Agent,
                                           <<"accepted_at">> =>
                                               barrel_spaces:now_ms()}).

put_legacy_session(#{db := Db}, Sid) ->
    Now = barrel_spaces:now_ms(),
    {ok, _} = barrel:put_doc(Db, #{
        <<"id">> => <<"session:", Sid/binary>>,
        <<"type">> => <<"session">>,
        <<"session">> => Sid,
        <<"agent">> => <<"old-bot">>,
        <<"data">> => #{<<"user_id">> => <<"u1">>, <<"k">> => 1},
        <<"metadata">> => #{},
        <<"summary">> => <<"legacy summary">>,
        <<"pinned">> => [#{<<"id">> => <<"p1">>, <<"content">> => <<"x">>,
                           <<"priority">> => 5, <<"metadata">> => #{},
                           <<"pinned_at">> => Now}],
        <<"ttl">> => 0,
        <<"created_at">> => Now,
        <<"updated_at">> => Now}),
    Sid.
