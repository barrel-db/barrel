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
  save_doc/1
]).

all() ->
  [
    save_doc
  ].

init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(barrel),
  {ok, _} = barrel_store_sup:start_store(default, barrel_memory_storage, #{}),

  Config.

init_per_testcase(_, Config) ->
  ok = barrel:create_barrel(<<"test">>, #{}),
  Config.

end_per_testcase(_, _Config) ->
  ok = barrel:delete_barrel(<<"test">>, #{}),
  ok.

end_per_suite(Config) ->
  ok = application:stop(barrel),
  Config.


save_doc(_Config) ->
  Doc0 = #{ <<"id">> => <<"a">>, <<"v">> => 1},
  Doc1 = barrel:save_doc(<<"test">>, Doc0),
  #{ <<"id">> := <<"a">>, <<"v">> := 1, <<"_rev">> := _Rev} = Doc1,
  {ok, Doc1} = barrel:fetch_doc(<<"test">>, <<"a">>, #{}),
  #{ <<"v">> := 2} = Doc2 = barrel:save_doc(<<"test">>, Doc1#{ <<"v">> => 2 }),
  {ok, Doc2} = barrel:fetch_doc(<<"test">>, <<"a">>, #{}),
  {error, conflict} =  barrel:save_doc(<<"test">>, Doc1#{ <<"v">> => 2 }),
  ok.
