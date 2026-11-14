%%%-------------------------------------------------------------------
%%% @doc Grants, sessions and handoffs written on replicas that cannot
%%% see each other, then replicated: no write is lost, a revocation and
%%% a removal stay, a handoff never goes back, and every replica gives
%%% the same answers.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_spaces_mesh_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([t_revoke_during_split/1,
         t_revoke_survives_grant_rewrite/1,
         t_revoke_before_grant_arrives/1,
         t_session_two_sides/1,
         t_same_key_later_write_wins/1,
         t_unpin_stays_removed/1,
         t_delete_data_stays_removed/1,
         t_handoff_split_accept/1,
         t_handoff_never_goes_back/1,
         t_counters_two_nodes/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-import(barrel_spaces_mesh, [on/2, sync/2, sync_all/1, space/1]).

all() ->
    [t_revoke_during_split, t_revoke_survives_grant_rewrite,
     t_revoke_before_grant_arrives,
     t_session_two_sides, t_same_key_later_write_wins,
     t_unpin_stays_removed, t_delete_data_stays_removed,
     t_handoff_split_accept, t_handoff_never_goes_back,
     t_counters_two_nodes].

init_per_suite(Config) ->
    application:set_env(barrel_docdb, data_dir, ?config(priv_dir, Config)),
    {ok, _} = application:ensure_all_started(barrel_spaces),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(Case, Config) ->
    Replicas = barrel_spaces_mesh:start(atom_to_binary(Case), 3),
    [{replicas, Replicas} | Config].

end_per_testcase(_Case, Config) ->
    barrel_spaces_mesh:stop(?config(replicas, Config)).

%%====================================================================
%% 1. A revocation reaches every node, even after a split
%%====================================================================

t_revoke_during_split(Config) ->
    [A, B, C] = Rs = ?config(replicas, Config),
    {ok, Token, _} = on(A, fun() ->
                               barrel_caps:grant(<<"sp_mesh">>,
                                                 #{rights => [read]})
                           end),
    ok = sync_all(Rs),
    {ok, _} = on(B, fun() -> verify(Token) end),
    %% A revokes while B is cut off; B and C revoke too, concurrently
    ok = on(A, fun() -> barrel_caps:revoke(Token, #{by => <<"ops">>}) end),
    ok = on(C, fun() -> barrel_caps:revoke(Token, #{by => <<"c">>}) end),
    {ok, _} = on(B, fun() -> verify(Token) end),
    ok = sync(A, B),
    ?assertEqual({error, revoked}, on(B, fun() -> verify(Token) end)),
    ok = sync_all(Rs),
    [?assertEqual({error, revoked}, on(R, fun() -> verify(Token) end))
     || R <- Rs],
    %% the record says who revoked it, the same on every node
    Bys = [on(R, fun() ->
                     {ok, [G]} = barrel_caps:list(<<"sp_mesh">>),
                     maps:get(<<"revoked_by">>, G)
                 end) || R <- Rs],
    ?assertMatch([By, By, By], Bys),
    ok.

%% An old node edits the grant document in place while another
%% revokes: the edit cannot undo the revocation.
t_revoke_survives_grant_rewrite(Config) ->
    [A, B, _C] = Rs = ?config(replicas, Config),
    {ok, Token, #{<<"token_id">> := Tid}} =
        on(A, fun() -> barrel_caps:grant(<<"sp_mesh">>, #{}) end),
    ok = sync_all(Rs),
    ok = on(A, fun() -> barrel_caps:revoke(Token) end),
    on(B, fun() ->
              Reg = barrel_spaces:registry_db(),
              {ok, G} = barrel_docdb:get_doc(Reg, <<"grant:", Tid/binary>>),
              {ok, _} = barrel_docdb:put_doc(
                          Reg, G#{<<"label">> => <<"renamed">>,
                                  <<"revoked_at">> => 0})
          end),
    ok = sync_all(Rs),
    [?assertEqual({error, revoked}, on(R, fun() -> verify(Token) end))
     || R <- Rs],
    ok.

%% The token id reaches an operator on a node the grant has not
%% replicated to yet: the revocation still holds once it arrives.
t_revoke_before_grant_arrives(Config) ->
    [A, B, _C] = Rs = ?config(replicas, Config),
    {ok, Token, #{<<"token_id">> := Tid}} =
        on(A, fun() -> barrel_caps:grant(<<"sp_mesh">>, #{}) end),
    ok = on(B, fun() -> barrel_caps:revoke(Tid) end),
    ok = sync_all(Rs),
    [?assertEqual({error, revoked}, on(R, fun() -> verify(Token) end))
     || R <- Rs],
    ok.

%%====================================================================
%% 2. A laptop and a server sharing one session
%%====================================================================

t_session_two_sides(Config) ->
    [Laptop, Server, _] = Rs = ?config(replicas, Config),
    {ok, Sid} = barrel_session:create(space(Laptop), #{agent => <<"a">>}),
    ok = sync_all(Rs),
    {ok, _} = barrel_session:set_data(space(Laptop), Sid,
                                      <<"current_file">>, <<"app.erl">>),
    {ok, _} = barrel_session:set_data(space(Server), Sid,
                                      <<"plan">>, <<"refactor the parser">>),
    {ok, PinId} = barrel_session:pin_context(space(Server), Sid,
                                             #{content => <<"use OTP 29">>}),
    {ok, _} = barrel_session:set_summary(space(Laptop), Sid, <<"so far">>),
    ok = sync_all(Rs),
    [begin
         {ok, S} = barrel_session:get(space(R), Sid),
         ?assertEqual(#{<<"current_file">> => <<"app.erl">>,
                        <<"plan">> => <<"refactor the parser">>},
                      maps:get(<<"data">>, S)),
         ?assertEqual([PinId], [maps:get(<<"id">>, P)
                                || P <- maps:get(<<"pinned">>, S)]),
         ?assertEqual(<<"so far">>, maps:get(<<"summary">>, S))
     end || R <- Rs],
    ok.

t_same_key_later_write_wins(Config) ->
    [A, B, C] = Rs = ?config(replicas, Config),
    {ok, Sid} = barrel_session:create(space(A), #{}),
    ok = sync_all(Rs),
    {ok, _} = barrel_session:set_data(space(A), Sid, <<"k">>, <<"first">>),
    {ok, _} = barrel_session:set_data(space(B), Sid, <<"k">>, <<"second">>),
    ok = sync(B, C),
    ok = sync(A, C),
    ok = sync_all(Rs),
    [?assertEqual({ok, <<"second">>},
                  barrel_session:get_data(space(R), Sid, <<"k">>))
     || R <- Rs],
    ok.

%%====================================================================
%% 3. Forgetting things for good
%%====================================================================

t_unpin_stays_removed(Config) ->
    [A, B, Stale] = Rs = ?config(replicas, Config),
    {ok, Sid} = barrel_session:create(space(A), #{}),
    {ok, PinId} = barrel_session:pin_context(space(A), Sid,
                                             #{content => <<"outdated">>}),
    ok = sync_all(Rs),
    {ok, _} = barrel_session:unpin_context(space(A), Sid, PinId),
    %% the stale device keeps working on its copy, then pushes it
    {ok, _} = barrel_session:touch(space(Stale), Sid),
    {ok, _} = barrel_session:set_summary(space(Stale), Sid, <<"later">>),
    ok = sync(Stale, A),
    ok = sync(Stale, B),
    ok = sync_all(Rs),
    [?assertEqual({ok, []}, barrel_session:list_pinned(space(R), Sid))
     || R <- Rs],
    ok.

t_delete_data_stays_removed(Config) ->
    [A, B, Stale] = Rs = ?config(replicas, Config),
    {ok, Sid} = barrel_session:create(space(A), #{}),
    {ok, _} = barrel_session:set_data(space(A), Sid, <<"secret">>, <<"s3">>),
    {ok, _} = barrel_session:set_data(space(A), Sid, <<"keep">>, 1),
    ok = sync_all(Rs),
    {ok, _} = barrel_session:delete_data(space(B), Sid, <<"secret">>),
    %% the stale copy is touched again, but not the removed key
    {ok, _} = barrel_session:set_data(space(Stale), Sid, <<"keep">>, 2),
    ok = sync(Stale, B),
    ok = sync_all(Rs),
    [begin
         ?assertEqual({error, not_found},
                      barrel_session:get_data(space(R), Sid, <<"secret">>)),
         {ok, S} = barrel_session:get(space(R), Sid),
         ?assertEqual(#{<<"keep">> => 2}, maps:get(<<"data">>, S))
     end || R <- Rs],
    ok.

%%====================================================================
%% 4. Handing work between agents on different machines
%%====================================================================

t_handoff_split_accept(Config) ->
    [A, B, C] = Rs = ?config(replicas, Config),
    {ok, #{handoff_id := Hid, token := T}} =
        on(A, fun() -> barrel_handoff:create(<<"sp_mesh">>,
                                             #{task_name => <<"review PR 42">>})
              end),
    ok = sync_all(Rs),
    {ok, _} = on(B, fun() -> accept(T, <<"reviewer">>) end),
    {ok, _} = on(C, fun() -> accept(T, <<"backup-reviewer">>) end),
    ok = sync_all(Rs),
    Views = [on(R, fun() -> barrel_handoff:get(Hid) end) || R <- Rs],
    [{ok, #{<<"status">> := <<"accepted">>, <<"accepted_by">> := Owner,
            <<"accepted_at">> := At}} | _] = Views,
    ?assert(lists:member(Owner, [<<"reviewer">>, <<"backup-reviewer">>])),
    [?assertMatch({ok, #{<<"status">> := <<"accepted">>,
                         <<"accepted_by">> := Owner,
                         <<"accepted_at">> := At}}, V) || V <- Views],
    %% the owner is the same in every listing
    [?assertMatch({ok, [#{<<"accepted_by">> := Owner}]},
                  on(R, fun() -> barrel_handoff:list(#{}) end)) || R <- Rs],
    ok.

t_handoff_never_goes_back(Config) ->
    [A, B, Stale] = Rs = ?config(replicas, Config),
    {ok, #{handoff_id := Hid, token := T}} =
        on(A, fun() -> barrel_handoff:create(<<"sp_mesh">>,
                                             #{task_name => <<"ship">>})
              end),
    ok = sync_all(Rs),
    {ok, _} = on(A, fun() -> accept(T, <<"worker">>) end),
    ok = sync(A, B),
    {ok, Done} = on(B, fun() ->
                           barrel_handoff:complete(T, #{result => <<"ok">>,
                                                        revoke => false})
                       end),
    ?assertEqual(<<"completed">>, maps:get(<<"status">>, Done)),
    %% the stale node still sees it pending, a late agent accepts it
    %% there, and that state is pushed everywhere
    {ok, #{<<"status">> := <<"pending">>}} =
        on(Stale, fun() -> barrel_handoff:get(Hid) end),
    {ok, _} = on(Stale, fun() -> accept(T, <<"late">>) end),
    ok = sync(Stale, B),
    ok = sync(Stale, A),
    ok = sync_all(Rs),
    [?assertMatch({ok, #{<<"status">> := <<"completed">>,
                         <<"result">> := <<"ok">>,
                         <<"accepted_by">> := <<"worker">>}},
                  on(R, fun() -> barrel_handoff:get(Hid) end)) || R <- Rs],
    ok.

%%====================================================================
%% 5. Two agents counting in one session
%%====================================================================

t_counters_two_nodes(Config) ->
    [A, B, _C] = Rs = ?config(replicas, Config),
    {ok, Sid} = barrel_session:create(space(A), #{}),
    ok = sync_all(Rs),
    Incr = fun(R, N) ->
               {ok, _} = barrel_session:incr_counter(space(R), Sid,
                                                     <<"messages">>, N)
           end,
    [Incr(A, 1) || _ <- lists:seq(1, 12)],
    [Incr(B, 1) || _ <- lists:seq(1, 9)],
    {ok, _} = barrel_session:incr_counter(space(B), Sid, <<"extractions">>, 1),
    ok = sync_all(Rs),
    [?assertEqual({ok, #{<<"messages">> => 21, <<"extractions">> => 1}},
                  barrel_session:get_counters(space(R), Sid)) || R <- Rs],
    {ok, S} = barrel_session:get(space(A), Sid),
    ?assertEqual(21, maps:get(<<"messages">>, maps:get(<<"counters">>, S))),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

verify(Token) ->
    barrel_caps:verify(Token, <<"sp_mesh">>, read).

accept(Token, Agent) ->
    barrel_handoff:accept(Token, #{agent => Agent, session => false}).
