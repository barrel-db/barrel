%%%-------------------------------------------------------------------
%%% @doc Context catalog (B4): card round trips, id minting, credential
%%% rejection, name resolution.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ctx_catalog_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([round_trip/1,
         minted_ids/1,
         explicit_id/1,
         rejects_credentials/1,
         rejects_bad_locations/1,
         update_card/1,
         list_and_unlisted/1,
         resolve_name/1]).

all() ->
    [round_trip, minted_ids, explicit_id, rejects_credentials,
     rejects_bad_locations, update_card, list_and_unlisted, resolve_name].

init_per_suite(Config) ->
    application:load(barrel_docdb),
    application:set_env(barrel_docdb, data_dir, ?config(priv_dir, Config)),
    {ok, _} = application:ensure_all_started(barrel),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    Db = <<"_cat_", (atom_to_binary(TC))/binary>>,
    application:set_env(barrel, ctx_catalog_db, Db),
    [{catalog, Db} | Config].

end_per_testcase(_TC, Config) ->
    _ = barrel_docdb:delete_db(?config(catalog, Config)),
    application:unset_env(barrel, ctx_catalog_db),
    ok.

card(Name) ->
    #{<<"name">> => Name,
      <<"title">> => <<"t">>,
      <<"topics">> => [<<"erlang">>],
      <<"capabilities">> => #{<<"query">> => <<"token">>},
      <<"locations">> => [#{<<"kind">> => <<"remote">>,
                            <<"endpoint">> => <<"https://r1.example:8080">>,
                            <<"db">> => <<"src">>}]}.

round_trip(_Config) ->
    {ok, #{<<"id">> := Id} = Card} = barrel_ctx_catalog:register(card(<<"a/b">>)),
    ?assertMatch(#{<<"type">> := <<"context_card">>, <<"format">> := 1,
                   <<"discoverable">> := <<"listed">>,
                   <<"updated_at">> := _}, Card),
    {ok, Got} = barrel_ctx_catalog:get(Id),
    ?assertEqual(Card, Got),
    ?assertNot(maps:is_key(<<"_rev">>, Got)),
    ?assertEqual({error, not_found}, barrel_ctx_catalog:get(<<"ctx_nope">>)).

minted_ids(_Config) ->
    Ids = [begin
               {ok, #{<<"id">> := Id}} =
                   barrel_ctx_catalog:register(card(<<"n">>)),
               Id
           end || _ <- lists:seq(1, 5)],
    ?assertEqual(5, length(lists:usort(Ids))),
    lists:foreach(
        fun(<<"ctx_", Rest/binary>>) ->
            ?assertEqual(24, byte_size(Rest)),
            ?assertEqual(match, re:run(Rest, "^[a-z2-7]{24}$",
                                       [{capture, none}]))
        end, Ids).

explicit_id(_Config) ->
    Card = (card(<<"docs/http">>))#{<<"id">> => <<"ctx_local_docs">>},
    {ok, #{<<"id">> := <<"ctx_local_docs">>}} =
        barrel_ctx_catalog:register(Card),
    ?assertEqual({error, already_exists}, barrel_ctx_catalog:register(Card)),
    ?assertMatch({error, {invalid_card, id}},
                 barrel_ctx_catalog:register(Card#{<<"id">> => <<"x">>})),
    ?assertMatch({error, {invalid_card, id}},
                 barrel_ctx_catalog:register(
                   Card#{<<"id">> => <<"ctx_UPPER">>})).

rejects_credentials(_Config) ->
    Base = card(<<"c">>),
    [Loc] = maps:get(<<"locations">>, Base),
    Bad = [Base#{<<"token">> => <<"x">>},
           Base#{<<"meta">> => #{<<"nested">> => [#{<<"Password">> => 1}]}},
           Base#{<<"locations">> => [Loc#{<<"api_key">> => <<"k">>}]},
           Base#{<<"locations">> =>
                     [Loc#{<<"endpoint">> =>
                               <<"https://u:p@r1.example">>}]},
           Base#{<<"note">> => <<"bsp_abc_def">>}],
    lists:foreach(
        fun(Card) ->
            ?assertMatch({error, {invalid_card, _}},
                         barrel_ctx_catalog:register(Card))
        end, Bad),
    {ok, Cards} = barrel_ctx_catalog:list(#{include_unlisted => true}),
    ?assertEqual([], Cards).

rejects_bad_locations(_Config) ->
    Base = card(<<"l">>),
    Cases = [{#{<<"kind">> => <<"ftp">>, <<"db">> => <<"x">>},
              {unknown_location_kind, <<"ftp">>}},
             {#{<<"kind">> => <<"remote">>, <<"db">> => <<"x">>},
              {incomplete_location, <<"remote">>}},
             {#{<<"kind">> => <<"local">>, <<"db">> => <<"a/b">>},
              {db, <<"a/b">>}},
             {#{<<"kind">> => <<"remote">>, <<"db">> => <<"x">>,
                <<"endpoint">> => <<"file:///etc">>},
              {endpoint, <<"file:///etc">>}}],
    lists:foreach(
        fun({Loc, Reason}) ->
            ?assertEqual({error, {invalid_card, Reason}},
                         barrel_ctx_catalog:register(
                           Base#{<<"locations">> => [Loc]}))
        end, Cases),
    ?assertEqual({error, {invalid_card, locations}},
                 barrel_ctx_catalog:register(Base#{<<"locations">> => []})),
    ?assertEqual({error, {invalid_card, name}},
                 barrel_ctx_catalog:register(maps:remove(<<"name">>, Base))).

update_card(_Config) ->
    {ok, #{<<"id">> := Id}} = barrel_ctx_catalog:register(card(<<"u">>)),
    {ok, Updated} = barrel_ctx_catalog:update(
        Id, #{<<"title">> => <<"new">>, <<"id">> => <<"ctx_hijack">>}),
    ?assertMatch(#{<<"id">> := Id, <<"title">> := <<"new">>}, Updated),
    {ok, Got} = barrel_ctx_catalog:get(Id),
    ?assertEqual(<<"new">>, maps:get(<<"title">>, Got)),
    ?assertMatch({error, {invalid_card, {credential_field, _}}},
                 barrel_ctx_catalog:update(Id, #{<<"secret">> => <<"s">>})),
    ?assertEqual({error, not_found},
                 barrel_ctx_catalog:update(<<"ctx_nope">>, #{})).

list_and_unlisted(_Config) ->
    {ok, _} = barrel_ctx_catalog:register(card(<<"b/one">>)),
    {ok, _} = barrel_ctx_catalog:register(card(<<"a/two">>)),
    {ok, _} = barrel_ctx_catalog:register(
                (card(<<"a/hidden">>))#{<<"discoverable">> => <<"unlisted">>}),
    {ok, Listed} = barrel_ctx_catalog:list(#{}),
    ?assertEqual([<<"a/two">>, <<"b/one">>],
                 [maps:get(<<"name">>, C) || C <- Listed]),
    {ok, All} = barrel_ctx_catalog:list(#{include_unlisted => true}),
    ?assertEqual(3, length(All)),
    {ok, Prefixed} = barrel_ctx_catalog:list(#{name_prefix => <<"a/">>,
                                               include_unlisted => true}),
    ?assertEqual([<<"a/hidden">>, <<"a/two">>],
                 [maps:get(<<"name">>, C) || C <- Prefixed]).

resolve_name(_Config) ->
    {ok, #{<<"id">> := Id}} = barrel_ctx_catalog:register(card(<<"one">>)),
    {ok, _} = barrel_ctx_catalog:register(card(<<"dup">>)),
    {ok, _} = barrel_ctx_catalog:register(card(<<"dup">>)),
    ?assertEqual({ok, Id}, barrel_ctx_catalog:resolve_name(<<"one">>)),
    ?assertEqual({error, ambiguous},
                 barrel_ctx_catalog:resolve_name(<<"dup">>)),
    ?assertEqual({error, not_found},
                 barrel_ctx_catalog:resolve_name(<<"none">>)),
    ok = barrel_ctx_catalog:unregister(Id),
    ?assertEqual({error, not_found},
                 barrel_ctx_catalog:resolve_name(<<"one">>)).
