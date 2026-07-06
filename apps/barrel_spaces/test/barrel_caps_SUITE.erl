%%%-------------------------------------------------------------------
%%% @doc Capability tokens: issuance, the verify matrix (wrong space,
%%% insufficient right, expiry, revocation, tamper), auth contexts,
%%% and drop_space revoking every grant.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_caps_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([
    t_grant_and_verify/1,
    t_rights_ladder/1,
    t_verify_matrix/1,
    t_revoke/1,
    t_auth_context/1,
    t_drop_space_revokes/1,
    t_list_strips_hashes/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [t_grant_and_verify, t_rights_ladder, t_verify_matrix, t_revoke,
     t_auth_context, t_drop_space_revokes, t_list_strips_hashes].

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
    {ok, #{id := Space}} = barrel_spaces:create_space(#{
        label => atom_to_binary(Case, utf8), vectordb => Vec}),
    [{space, Space} | Config].

end_per_testcase(_Case, Config) ->
    _ = barrel_spaces:drop_space(?config(space, Config)),
    ok.

%%====================================================================
%% Cases
%%====================================================================

t_grant_and_verify(Config) ->
    Space = ?config(space, Config),
    {ok, Token, Grant} = barrel_caps:grant(Space, #{
        rights => [read, write], subject => <<"agent-2">>,
        label => <<"worker">>}),
    ?assertMatch(<<"bsp_", _/binary>>, Token),
    ?assertEqual(61, byte_size(Token)),
    ?assertNot(maps:is_key(<<"token_hash">>, Grant)),
    {ok, G1} = barrel_caps:verify(Token, Space, read),
    ?assertEqual(<<"agent-2">>, maps:get(<<"subject">>, G1)),
    {ok, _} = barrel_caps:verify(Token, Space, write),
    ?assertEqual({error, forbidden},
                 barrel_caps:verify(Token, Space, admin)),
    ok.

t_rights_ladder(Config) ->
    Space = ?config(space, Config),
    {ok, Admin, _} = barrel_caps:grant(Space, #{rights => [admin]}),
    %% admin covers everything below it
    {ok, _} = barrel_caps:verify(Admin, Space, read),
    {ok, _} = barrel_caps:verify(Admin, Space, write),
    {ok, _} = barrel_caps:verify(Admin, Space, admin),
    {ok, ReadOnly, _} = barrel_caps:grant(Space, #{rights => [read]}),
    {ok, _} = barrel_caps:verify(ReadOnly, Space, read),
    ?assertEqual({error, forbidden},
                 barrel_caps:verify(ReadOnly, Space, write)),
    %% bad rights refused at issue time
    ?assertEqual({error, empty_rights},
                 barrel_caps:grant(Space, #{rights => []})),
    ?assertEqual({error, invalid_rights},
                 barrel_caps:grant(Space, #{rights => [root]})),
    ok.

t_verify_matrix(Config) ->
    Space = ?config(space, Config),
    {ok, Token, _} = barrel_caps:grant(Space, #{rights => [read]}),
    %% wrong space
    ?assertEqual({error, wrong_space},
                 barrel_caps:verify(Token, <<"sp_other">>, read)),
    %% tamper: flip a secret byte
    Size = byte_size(Token) - 1,
    <<Head:Size/binary, Last>> = Token,
    Tampered = <<Head/binary, (flip_b32(Last))>>,
    ?assertEqual({error, invalid_token},
                 barrel_caps:verify(Tampered, Space, read)),
    %% garbage shapes
    ?assertEqual({error, invalid_token},
                 barrel_caps:verify(<<"nonsense">>, Space, read)),
    ?assertEqual({error, not_found},
                 barrel_caps:verify(
                     <<"bsp_aaaaaaaaaaaaaaaa_",
                       (binary:copy(<<"a">>, 40))/binary>>,
                     Space, read)),
    %% expiry
    {ok, Expired, _} = barrel_caps:grant(Space, #{
        rights => [read],
        expires_at => barrel_spaces:now_ms() - 1}),
    ?assertEqual({error, expired},
                 barrel_caps:verify(Expired, Space, read)),
    ok.

t_revoke(Config) ->
    Space = ?config(space, Config),
    {ok, Token, Grant} = barrel_caps:grant(Space, #{rights => [write]}),
    {ok, _} = barrel_caps:verify(Token, Space, write),
    ok = barrel_caps:revoke(Token),
    ?assertEqual({error, revoked},
                 barrel_caps:verify(Token, Space, write)),
    %% idempotent, and by token id too
    ok = barrel_caps:revoke(Token),
    ok = barrel_caps:revoke(maps:get(<<"token_id">>, Grant)),
    ok.

t_auth_context(Config) ->
    Space = ?config(space, Config),
    {ok, Token, _} = barrel_caps:grant(Space, #{
        rights => [read, write], subject => <<"agent-3">>}),
    {ok, Ctx} = barrel_caps:auth_context(Token),
    ?assertEqual(Space, maps:get(space, Ctx)),
    ?assertEqual(<<"agent-3">>, maps:get(subject, Ctx)),
    ?assertEqual([read, write], lists:sort(maps:get(rights, Ctx))),
    ?assertEqual([<<"read">>, <<"write">>],
                 lists:sort(maps:get(scopes, Ctx))),
    ?assertEqual({error, invalid_token},
                 barrel_caps:auth_context(<<"bsp_short">>)),
    ok.

t_drop_space_revokes(Config) ->
    Space = ?config(space, Config),
    {ok, T1, _} = barrel_caps:grant(Space, #{rights => [read]}),
    {ok, T2, _} = barrel_caps:grant(Space, #{rights => [admin]}),
    ok = barrel_spaces:drop_space(Space),
    ?assertEqual({error, revoked}, barrel_caps:auth_context(T1)),
    ?assertEqual({error, revoked}, barrel_caps:auth_context(T2)),
    ok.

t_list_strips_hashes(Config) ->
    Space = ?config(space, Config),
    {ok, _, _} = barrel_caps:grant(Space, #{rights => [read],
                                            label => <<"a">>}),
    {ok, _, _} = barrel_caps:grant(Space, #{rights => [write],
                                            label => <<"b">>}),
    {ok, Grants} = barrel_caps:list(Space),
    ?assertEqual(2, length(Grants)),
    lists:foreach(
        fun(G) ->
            ?assertNot(maps:is_key(<<"token_hash">>, G)),
            ?assertEqual(Space, maps:get(<<"space">>, G))
        end, Grants),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

%% flip to a different char within the base32 alphabet so the token
%% shape stays valid but the hash cannot match
flip_b32($a) -> $b;
flip_b32(_) -> $a.
