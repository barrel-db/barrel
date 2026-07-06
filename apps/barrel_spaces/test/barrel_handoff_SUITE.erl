%%%-------------------------------------------------------------------
%%% @doc Handoffs: possession of the token is the right to accept,
%%% sharing is by reference (the acceptor reads the from-agent's
%%% context in place), double accepts lose the CAS, completion revokes
%%% the grant, chains carry lineage, and pending handoffs are
%%% discoverable through the registry.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_handoff_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([
    t_two_agent_by_reference/1,
    t_bad_tokens/1,
    t_double_accept/1,
    t_complete_revokes/1,
    t_chain/1,
    t_list_filters/1,
    t_discovery_via_changes/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [t_two_agent_by_reference, t_bad_tokens, t_double_accept,
     t_complete_revokes, t_chain, t_list_filters,
     t_discovery_via_changes].

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
        label => atom_to_binary(Case, utf8), vectordb => Vec}),
    [{space, Space} | Config].

end_per_testcase(_Case, Config) ->
    #{id := Id} = ?config(space, Config),
    _ = barrel_spaces:drop_space(Id),
    ok.

%%====================================================================
%% Cases
%%====================================================================

t_two_agent_by_reference(Config) ->
    #{id := SpaceId, db := Db} = Space = ?config(space, Config),
    %% agent A works in the space, then hands off
    {ok, ASid} = barrel_session:create(Space, #{agent => <<"alice">>}),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"draft">>,
                                   <<"state">> => <<"half-done">>}),
    {ok, #{handoff_id := Hid, token := Token}} = barrel_handoff:create(
        Space, #{task_name => <<"finish the draft">>,
                 from_agent => <<"alice">>, to_agent => <<"bob">>,
                 from_session => ASid,
                 pending => [<<"polish conclusion">>]}),
    %% agent B accepts with the token and reads A's context IN PLACE
    {ok, #{handoff := H, space := BSpace, session := BSid}} =
        barrel_handoff:accept(Token, #{agent => <<"bob">>}),
    ?assertEqual(Hid, maps:get(<<"handoff">>, H)),
    ?assertEqual(<<"accepted">>, maps:get(<<"status">>, H)),
    #{db := BDb} = BSpace,
    {ok, #{<<"state">> := <<"half-done">>}} =
        barrel:get_doc(BDb, <<"draft">>),
    %% and B's work is immediately visible to A (same database)
    {ok, _} = barrel:put_doc(BDb, #{<<"id">> => <<"review">>,
                                    <<"by">> => <<"bob">>}),
    {ok, _} = barrel:get_doc(Db, <<"review">>),
    %% B's session lives in the shared space, tagged with the handoff
    {ok, BSess} = barrel_session:get(BSpace, BSid),
    ?assertEqual(<<"bob">>, maps:get(<<"agent">>, BSess)),
    ?assertEqual(Hid, maps:get(<<"handoff_id">>,
                               maps:get(<<"data">>, BSess))),
    ?assertEqual(SpaceId, maps:get(id, BSpace)),
    ok.

t_bad_tokens(Config) ->
    Space = ?config(space, Config),
    {ok, #{token := Token}} = barrel_handoff:create(
        Space, #{task_name => <<"t">>}),
    ?assertEqual({error, invalid_token},
                 barrel_handoff:accept(<<"garbage">>, #{})),
    %% a valid capability that belongs to no handoff
    #{id := SpaceId} = Space,
    {ok, Loose, _} = barrel_caps:grant(SpaceId, #{rights => [read]}),
    ?assertEqual({error, not_found},
                 barrel_handoff:accept(Loose, #{})),
    %% a revoked handoff token cannot accept
    ok = barrel_caps:revoke(Token),
    ?assertEqual({error, revoked}, barrel_handoff:accept(Token, #{})),
    %% an expired grant cannot accept
    {ok, #{token := Expired}} = barrel_handoff:create(
        Space, #{task_name => <<"late">>,
                 expires_at => barrel_spaces:now_ms() - 1}),
    ?assertEqual({error, expired},
                 barrel_handoff:accept(Expired, #{})),
    ok.

t_double_accept(Config) ->
    Space = ?config(space, Config),
    {ok, #{token := Token}} = barrel_handoff:create(
        Space, #{task_name => <<"once">>}),
    {ok, _} = barrel_handoff:accept(Token, #{agent => <<"first">>}),
    ?assertEqual({error, already_accepted},
                 barrel_handoff:accept(Token, #{agent => <<"second">>})),
    ok.

t_complete_revokes(Config) ->
    Space = ?config(space, Config),
    {ok, #{token := Token}} = barrel_handoff:create(
        Space, #{task_name => <<"done soon">>}),
    {ok, _} = barrel_handoff:accept(Token, #{agent => <<"worker">>}),
    {ok, Done} = barrel_handoff:complete(Token,
                                         #{result => <<"shipped">>}),
    ?assertEqual(<<"completed">>, maps:get(<<"status">>, Done)),
    ?assertEqual(<<"shipped">>, maps:get(<<"result">>, Done)),
    %% the grant died with the completion
    ?assertEqual({error, revoked}, barrel_caps:auth_context(Token)),
    ?assertEqual({error, already_completed},
                 barrel_handoff:complete(Token, #{})),
    ok.

t_chain(Config) ->
    Space = ?config(space, Config),
    {ok, #{handoff_id := H1, token := T1}} = barrel_handoff:create(
        Space, #{task_name => <<"step 1">>, to_agent => <<"bob">>}),
    {ok, _} = barrel_handoff:accept(T1, #{agent => <<"bob">>}),
    {ok, #{handoff_id := H2, token := T2}} = barrel_handoff:chain(
        T1, #{task_name => <<"step 2">>, from_agent => <<"bob">>,
              to_agent => <<"carol">>}),
    {ok, Doc2} = barrel_handoff:get(H2),
    ?assertEqual(H1, maps:get(<<"parent_handoff">>, Doc2)),
    ?assertEqual(H1, maps:get(<<"chain_root">>, Doc2)),
    ?assertEqual(1, maps:get(<<"chain_depth">>, Doc2)),
    %% the first handoff completed; the new token accepts the new one
    {ok, Doc1} = barrel_handoff:get(H1),
    ?assertEqual(<<"completed">>, maps:get(<<"status">>, Doc1)),
    {ok, _} = barrel_handoff:accept(T2, #{agent => <<"carol">>}),
    %% chaining again extends the lineage from the same root
    {ok, #{handoff_id := H3}} = barrel_handoff:chain(
        T2, #{task_name => <<"step 3">>, to_agent => <<"dave">>}),
    {ok, Doc3} = barrel_handoff:get(H3),
    ?assertEqual(H2, maps:get(<<"parent_handoff">>, Doc3)),
    ?assertEqual(H1, maps:get(<<"chain_root">>, Doc3)),
    ?assertEqual(2, maps:get(<<"chain_depth">>, Doc3)),
    ok.

t_list_filters(Config) ->
    #{id := SpaceId} = Space = ?config(space, Config),
    {ok, _} = barrel_handoff:create(Space, #{task_name => <<"a">>,
                                             to_agent => <<"bob">>}),
    {ok, #{token := T}} = barrel_handoff:create(
        Space, #{task_name => <<"b">>, to_agent => <<"carol">>}),
    {ok, _} = barrel_handoff:accept(T, #{agent => <<"carol">>}),
    {ok, All} = barrel_handoff:list(#{space => SpaceId}),
    ?assertEqual(2, length(All)),
    {ok, Pending} = barrel_handoff:list(#{space => SpaceId,
                                          status => <<"pending">>}),
    ?assertEqual([<<"a">>],
                 [maps:get(<<"task_name">>, H) || H <- Pending]),
    {ok, ForCarol} = barrel_handoff:list(#{to_agent => <<"carol">>}),
    ?assert(lists:all(
        fun(H) -> maps:get(<<"to_agent">>, H) =:= <<"carol">> end,
        ForCarol)),
    ok.

t_discovery_via_changes(Config) ->
    Space = ?config(space, Config),
    Registry = barrel_spaces:registry_db(),
    {ok, _, Since} = barrel_docdb:get_changes(Registry, first),
    {ok, #{handoff_id := Hid}} = barrel_handoff:create(
        Space, #{task_name => <<"observable">>}),
    %% an observer of the registry's changes feed sees the new handoff
    {ok, Changes, _} = barrel_docdb:get_changes(Registry, Since),
    Ids = [maps:get(id, C) || C <- Changes],
    ?assert(lists:member(<<"handoff:", Hid/binary>>, Ids)),
    ok.
