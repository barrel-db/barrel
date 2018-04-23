%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. Apr 2018 14:46
%%%-------------------------------------------------------------------
-module(barrel_query_SUITE).
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
  basic/1
]).

all() ->
  [
    basic
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


basic(_Suite) ->
  Docs = [
    #{ <<"id">> => <<"a">>, <<"v">> => 1, <<"o">> => #{ <<"o1">> => 1, << "o2">> => 1}}
  ],
  [{ok, #{ <<"id">> := <<"a">>}}] = barrel:save_docs(<<"test">>, Docs),
  timer:sleep(200),
  io:format("tab ~p~n", [ets:tab2list(default_test_data)]),
  Fun = fun(DocId, Acc) -> {ok, [DocId | Acc]} end,
  [<<"a">>] = barrel:query(<<"test">>, <<"/id">>, Fun, [], #{}),
  ok.
  
