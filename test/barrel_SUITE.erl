%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. Apr 2018 11:04
%%%-------------------------------------------------------------------
-module(barrel_SUITE).
-author("benoitc").

%% API
-export([
  all/0,
  init_per_suite/1,
  end_per_suite/1,
  init_per_testcase/2,
  end_per_testcase/2
]).

-export([
  save_doc/1,
  update_non_existing_doc/1,
  delete_doc/1
]).

all() ->
  [
    save_doc,
    update_non_existing_doc,
    delete_doc
  ].

init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(barrel),
  {ok, _} = barrel_store_sup:start_store(default, barrel_memory_storage, #{}),

  Config.

init_per_testcase(_, Config) ->
  ok = barrel:create_barrel(<<"test">>, #{}),
  Config.

end_per_testcase(_, _Config) ->
  ok = barrel:delete_barrel(<<"test">>),
  ok.

end_per_suite(Config) ->
  ok = application:stop(barrel),
  Config.


save_doc(_Config) ->
  Doc0 = #{ <<"id">> => <<"a">>, <<"v">> => 1},
  {ok, Doc1} = barrel:save_doc(<<"test">>, Doc0),
  #{ <<"id">> := <<"a">>, <<"v">> := 1, <<"_rev">> := _Rev} = Doc1,
  {ok, Doc1} = barrel:fetch_doc(<<"test">>, <<"a">>, #{}),
  {ok, #{ <<"v">> := 2} = Doc2} = barrel:save_doc(<<"test">>, Doc1#{ <<"v">> => 2 }),
  {ok, Doc2} = barrel:fetch_doc(<<"test">>, <<"a">>, #{}),
  {error, {{conflict, revision_conflict}, <<"a">>}} =  barrel:save_doc(<<"test">>, Doc1#{ <<"v">> => 2 }),
  {error, {{conflict, doc_exists}, <<"a">>}} = barrel:save_doc(<<"test">>, Doc0),
  ok.

update_non_existing_doc(_Config) ->
  Doc0 = #{ <<"id">> => <<"a">>, <<"v">> => 1, <<"_rev">> => <<"1-AAAAAAAAAAA">>},
  {error, {not_found, <<"a">>}} = barrel:save_doc(<<"test">>, Doc0).

delete_doc(_Config) ->
  Doc0 = #{ <<"id">> => <<"a">>, <<"v">> => 1},
  {ok, Doc1} = barrel:save_doc(<<"test">>, Doc0),
  {ok, Doc1} = barrel:fetch_doc(<<"test">>, <<"a">>, #{}),
  {ok, Doc2} = barrel:delete_doc(<<"test">>, <<"a">>, maps:get(<<"_rev">>, Doc1)),
  undefined = maps:get(<<"v">>, Doc2, undefined),
  {error, not_found} = barrel:fetch_doc(<<"test">>, <<"a">>, #{}),
  {ok, Doc2} = barrel:fetch_doc(<<"test">>, <<"a">>, #{ rev => maps:get(<<"_rev">>, Doc2)}),
  ok.
  
