%%%-------------------------------------------------------------------
%%% @doc Space lifecycle: registry docs, generated names, open/close/
%%% drop through the facade lifecycle manager, listing, and per-space
%%% encryption keys (agent isolation).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_spaces_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
    t_lifecycle/1,
    t_registry_doc/1,
    t_dropped_space_refuses_open/1,
    t_list_spaces/1,
    t_encrypted_space/1,
    t_space_is_a_barrel_db/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [t_lifecycle, t_registry_doc, t_dropped_space_refuses_open,
     t_list_spaces, t_encrypted_space, t_space_is_a_barrel_db].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_spaces),
    application:set_env(barrel_docdb, data_dir, ?config(priv_dir, Config)),
    application:set_env(barrel_spaces, test_encryption_master,
                        <<"spaces suite master">>),
    Config.

end_per_suite(_Config) ->
    application:unset_env(barrel_spaces, test_encryption_master),
    ok.

vec_opts(Config, Tag) ->
    #{dimension => 3, bm25_backend => memory,
      db_path => filename:join(?config(priv_dir, Config), Tag)}.

%%====================================================================
%% Cases
%%====================================================================

t_lifecycle(Config) ->
    {ok, #{id := Id, db := Db}} = barrel_spaces:create_space(#{
        label => <<"scratch">>,
        vectordb => vec_opts(Config, "lc_vec")}),
    ?assertMatch(<<"sp_", _/binary>>, Id),
    ?assertEqual(19, byte_size(Id)),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>, <<"v">> => 1}),
    ok = barrel_spaces:close_space(Id),
    %% reopen finds the data again
    {ok, #{db := Db2}} = barrel_spaces:open_space(Id),
    {ok, #{<<"v">> := 1}} = barrel:get_doc(Db2, <<"a">>),
    ok = barrel_spaces:drop_space(Id),
    ok.

t_registry_doc(Config) ->
    {ok, #{id := Id}} = barrel_spaces:create_space(#{
        label => <<"lab">>, purpose => <<"tests">>,
        owner => <<"agent-1">>, session_ttl => 120,
        vectordb => vec_opts(Config, "reg_vec")}),
    {ok, Info} = barrel_spaces:space_info(Id),
    ?assertEqual(<<"space">>, maps:get(<<"type">>, Info)),
    ?assertEqual(<<"lab">>, maps:get(<<"label">>, Info)),
    ?assertEqual(<<"tests">>, maps:get(<<"purpose">>, Info)),
    ?assertEqual(<<"agent-1">>, maps:get(<<"owner">>, Info)),
    ?assertEqual(<<"active">>, maps:get(<<"status">>, Info)),
    ?assertEqual(120, maps:get(<<"session_ttl">>, Info)),
    ?assertEqual(false, maps:get(<<"encrypted">>, Info)),
    ok = barrel_spaces:drop_space(Id),
    {ok, Info2} = barrel_spaces:space_info(Id),
    ?assertEqual(<<"dropped">>, maps:get(<<"status">>, Info2)),
    ok.

t_dropped_space_refuses_open(Config) ->
    {ok, #{id := Id}} = barrel_spaces:create_space(#{
        vectordb => vec_opts(Config, "drop_vec")}),
    ok = barrel_spaces:drop_space(Id),
    ?assertEqual({error, space_dropped}, barrel_spaces:open_space(Id)),
    %% dropping again is a no-op
    ok = barrel_spaces:drop_space(Id),
    ok.

t_list_spaces(Config) ->
    {ok, #{id := A}} = barrel_spaces:create_space(#{
        label => <<"list-a">>, vectordb => vec_opts(Config, "la_vec")}),
    {ok, #{id := B}} = barrel_spaces:create_space(#{
        label => <<"list-b">>, vectordb => vec_opts(Config, "lb_vec")}),
    ok = barrel_spaces:drop_space(B),
    {ok, Spaces} = barrel_spaces:list_spaces(),
    Ids = [maps:get(<<"space">>, S) || S <- Spaces],
    ?assert(lists:member(A, Ids)),
    ?assertNot(lists:member(B, Ids)),
    ok = barrel_spaces:drop_space(A),
    ok.

t_encrypted_space(Config) ->
    Enc = #{provider => barrel_spaces_test_keyprovider},
    {ok, #{id := Id, db := Db}} = barrel_spaces:create_space(#{
        label => <<"secret">>, encryption => Enc,
        vectordb => vec_opts(Config, "enc_vec")}),
    {ok, Info} = barrel_spaces:space_info(Id),
    ?assertEqual(true, maps:get(<<"encrypted">>, Info)),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>}),
    ok = barrel_spaces:close_space(Id),
    %% reopening needs the runtime spec, exactly like any barrel db
    ?assertMatch({error, db_is_encrypted}, barrel_spaces:open_space(Id)),
    {ok, #{db := Db2}} = barrel_spaces:open_space(Id,
                                                  #{encryption => Enc}),
    {ok, _} = barrel:get_doc(Db2, <<"a">>),
    %% a wrong master fails closed
    ok = barrel_spaces:close_space(Id),
    application:set_env(barrel_spaces, test_encryption_master,
                        <<"other">>),
    ?assertEqual({error, wrong_encryption_key},
                 barrel_spaces:open_space(Id, #{encryption => Enc})),
    application:set_env(barrel_spaces, test_encryption_master,
                        <<"spaces suite master">>),
    ok = barrel_spaces:drop_space(Id, #{encryption => Enc}),
    ok.

t_space_is_a_barrel_db(Config) ->
    %% every barrel feature works inside a space unchanged
    {ok, #{id := Id, db := Db}} = barrel_spaces:create_space(#{
        vectordb => vec_opts(Config, "full_vec")}),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>,
                                   <<"kind">> => <<"note">>}),
    ok = barrel:vector_add(Db, <<"a">>, <<"hello">>, #{},
                           [1.0, 0.0, 0.0]),
    {ok, [#{key := <<"a">>} | _]} =
        barrel:search_vector(Db, [1.0, 0.0, 0.0], #{k => 1}),
    {ok, Changes, _} = barrel:changes(Db, first),
    ?assert(length(Changes) >= 1),
    ok = barrel_spaces:drop_space(Id),
    ok.
