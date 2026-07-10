%%%-------------------------------------------------------------------
%%% @doc barrel_vectordb_docdb_backend is a shipped, public docstore seam:
%%% a user writes `docstore => {barrel_vectordb_docdb_backend, #{}}' in a
%%% store config. Nothing exercised it, so a break in it would surface only
%%% in a consumer's application.
%%%
%%% It calls barrel_docdb, which barrel_vectordb does not depend on: the
%%% backend is optional and resolved at runtime by config. These tests skip
%%% themselves when barrel_docdb is not on the path, so a standalone
%%% barrel_vectordb build stays green.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_docdb_backend_tests).

-include_lib("eunit/include/eunit.hrl").

-define(BACKEND, barrel_vectordb_docdb_backend).

docdb_available() ->
    code:ensure_loaded(barrel_docdb) =:= {module, barrel_docdb}.

backend_test_() ->
    case docdb_available() of
        false ->
            [];
        true ->
            {setup, fun setup/0, fun cleanup/1,
             fun(Ctx) ->
                 [?_test(roundtrip(Ctx)),
                  ?_test(missing_reads_not_found(Ctx)),
                  ?_test(multi_put_and_multi_get(Ctx)),
                  ?_test(multi_put_propagates_error(Ctx)),
                  ?_test(delete_is_idempotent(Ctx)),
                  ?_test(metadata_round_trips_arbitrary_terms(Ctx))]
             end}
    end.

setup() ->
    Dir = "/tmp/barrel_vectordb_docdb_backend_tests",
    os:cmd("rm -rf " ++ Dir),
    %% Load before set_env: loading an application resets its env from the
    %% .app file, so a set_env before ensure_all_started is discarded.
    _ = application:load(barrel_docdb),
    application:set_env(barrel_docdb, data_dir, Dir),
    {ok, _} = application:ensure_all_started(barrel_docdb),
    {ok, Ctx} = ?BACKEND:init(vdb_backend_test, #{}),
    Ctx.

%% Do not stop barrel_docdb here: other test modules in this run rely on it
%% staying up. terminate/1 closes the database this fixture opened.
cleanup(Ctx) ->
    ok = ?BACKEND:terminate(Ctx),
    ok.

roundtrip(Ctx) ->
    ok = ?BACKEND:put(Ctx, <<"a">>, <<"hello">>, #{lang => en}),
    ?assertEqual({ok, <<"hello">>, #{lang => en}}, ?BACKEND:get(Ctx, <<"a">>)).

missing_reads_not_found(Ctx) ->
    ?assertEqual(not_found, ?BACKEND:get(Ctx, <<"nope">>)).

multi_put_and_multi_get(Ctx) ->
    Pairs = [{<<"m1">>, <<"one">>, #{n => 1}},
             {<<"m2">>, <<"two">>, #{n => 2}}],
    ok = ?BACKEND:multi_put(Ctx, Pairs),
    ?assertEqual([{ok, <<"one">>, #{n => 1}}, {ok, <<"two">>, #{n => 2}}],
                 ?BACKEND:multi_get(Ctx, [<<"m1">>, <<"m2">>])),
    %% multi_get preserves order and reports gaps in place.
    ?assertEqual([{ok, <<"one">>, #{n => 1}}, not_found],
                 ?BACKEND:multi_get(Ctx, [<<"m1">>, <<"gone">>])).

multi_put_propagates_error(_Ctx) ->
    %% The fold carries an error forward instead of writing the rest. Point it
    %% at a database that was never opened: every put fails.
    Bad = #{db => <<"no-such-database">>},
    ?assertMatch({error, _},
                 ?BACKEND:multi_put(Bad, [{<<"x">>, <<"t">>, #{}},
                                          {<<"y">>, <<"t">>, #{}}])).

delete_is_idempotent(Ctx) ->
    ok = ?BACKEND:put(Ctx, <<"d">>, <<"x">>, #{}),
    ok = ?BACKEND:delete(Ctx, <<"d">>),
    ?assertEqual(not_found, ?BACKEND:get(Ctx, <<"d">>)),
    %% Deleting an absent id is not an error.
    ?assertEqual(ok, ?BACKEND:delete(Ctx, <<"d">>)),
    ?assertEqual(ok, ?BACKEND:delete(Ctx, <<"never-existed">>)).

%%====================================================================
%% Through the real path: a store configured with this docstore.
%%
%% The unit tests above call init/2 directly with an atom name. The store
%% normalises names to binaries before reaching the seam, so a direct call
%% cannot catch a crash on the name. Drive it the way a user does.
%%====================================================================

store_test_() ->
    case docdb_available() of
        false -> [];
        true -> {setup, fun store_setup/0, fun store_cleanup/1,
                 fun(_) -> [?_test(search_hydrates_from_docdb())] end}
    end.

store_setup() ->
    Dir = "/tmp/barrel_vectordb_docdb_backend_store_tests",
    os:cmd("rm -rf " ++ Dir),
    _ = application:load(barrel_docdb),
    application:set_env(barrel_docdb, data_dir, Dir ++ "/docdb"),
    %% Other modules in this run may already have started these, or started
    %% their processes standalone (a store runs without the application). Take
    %% whatever is up; the start_link below is the real assertion.
    _ = application:ensure_all_started(barrel_docdb),
    _ = application:ensure_all_started(barrel_vectordb),
    {ok, Pid} = barrel_vectordb:start_link(
                    #{name => ds_store,
                      path => Dir ++ "/vec",
                      dimension => 3,
                      docstore => {?BACKEND, #{db => <<"ds_docs">>}}}),
    Pid.

store_cleanup(_Pid) ->
    catch barrel_vectordb:stop(ds_store),
    ok.

search_hydrates_from_docdb() ->
    ok = barrel_vectordb:add_vector(ds_store, <<"d1">>, <<"hello there">>,
                                    #{tag => x}, [0.1, 0.2, 0.3]),
    {ok, [Hit]} = barrel_vectordb:search_vector(ds_store, [0.1, 0.2, 0.3],
                                                #{k => 1}),
    ?assertEqual(<<"d1">>, maps:get(key, Hit)),
    %% Text and metadata came back through the docstore, not the store's own
    %% column families.
    ?assertEqual(<<"hello there">>, maps:get(text, Hit)),
    ?assertEqual(#{tag => x}, maps:get(metadata, Hit)),
    %% And the document really lives in barrel_docdb.
    {ok, Doc} = barrel_docdb:get_doc(<<"ds_docs">>, <<"d1">>),
    ?assertEqual(<<"hello there">>, maps:get(<<"text">>, Doc)).

metadata_round_trips_arbitrary_terms(Ctx) ->
    %% Metadata is term_to_binary'd, so atom keys and nested terms survive
    %% the document codec exactly. That is the point of the encoding.
    Meta = #{atom_key => [1, 2, {nested, <<"tuple">>}], <<"bin">> => 3.5},
    ok = ?BACKEND:put(Ctx, <<"t">>, <<"body">>, Meta),
    ?assertEqual({ok, <<"body">>, Meta}, ?BACKEND:get(Ctx, <<"t">>)).
