%% Copyright 2017, Benoit Chesneau
%%
%% Licensed under the Apache License, Version 2.0 (the "License"); you may not
%% use this file except in compliance with the License. You may obtain a copy of
%% the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
%% License for the specific language governing permissions and limitations under
%% the License.


-module(barrel_changes_listener_SUITE).
-author("benoitc").


-export([
  collect_change/1,
  collect_changes/1,
  changes_feed_callback/1
]).

-export([
  all/0,
  init_per_suite/1,
  end_per_suite/1,
  init_per_testcase/2,
  end_per_testcase/2
]).

all() ->
  [
    collect_change,
    collect_changes,
    changes_feed_callback
  ].

init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(barrel),
  Config.

init_per_testcase(_, Config) ->
  _ = barrel_store:create_db(#{ <<"database_id">> => <<"testdb">> }),
  Config.

end_per_testcase(_, _Config) ->
  _ = barrel_store:delete_db(<<"testdb">>),
  ok.

end_per_suite(Config) ->
  ok = application:stop(barrel),
  Config.

collect_change(_Config) ->
  {ok, Pid} = barrel_changes_listener:start_link(<<"testdb">>, #{since => 0, mode => sse}),
  [] = barrel_changes_listener:changes(Pid),
  Doc = #{ <<"id">> => <<"aa">>, <<"v">> => 1},
  {ok, <<"aa">>, _RevId} = barrel:post(<<"testdb">>, Doc, []),
  timer:sleep(100),
  [#{ <<"seq">> := 1, <<"id">> := <<"aa">>}] = barrel_changes_listener:changes(Pid),
  [] = barrel_changes_listener:changes(Pid),
  ok = barrel_changes_listener:stop(Pid).

collect_changes(_Config) ->
  {ok, Pid} = barrel_changes_listener:start_link(<<"testdb">>, #{since => 0, mode => sse}),
  [] = barrel_changes_listener:changes(Pid),
  Doc = #{ <<"id">> => <<"aa">>, <<"v">> => 1},
  {ok, <<"aa">>, _} = barrel:post(<<"testdb">>, Doc, []),
  timer:sleep(100),
  [#{ <<"seq">> := 1, <<"id">> := <<"aa">>}] = barrel_changes_listener:changes(Pid),
  [] = barrel_changes_listener:changes(Pid),
  Doc2 = #{ <<"id">> => <<"bb">>, <<"v">> => 1},
  {ok, <<"bb">>, _} = barrel:post(<<"testdb">>, Doc2, []),
  {ok, _, _} = barrel:get(<<"testdb">>, <<"bb">>, []),
  timer:sleep(100),
  [#{ <<"seq">> := 2, <<"id">> := <<"bb">>}] = barrel_changes_listener:changes(Pid),
  [] = barrel_changes_listener:changes(Pid),
  Doc3 = #{ <<"id">> => <<"cc">>, <<"v">> => 1},
  Doc4 = #{ <<"id">> => <<"dd">>, <<"v">> => 1},
  {ok, <<"cc">>, _} = barrel:post(<<"testdb">>, Doc3, []),
  {ok, <<"dd">>, _} = barrel:post(<<"testdb">>, Doc4, []),
  {ok, _, _} = barrel:get(<<"testdb">>, <<"cc">>, []),
  {ok, _, _} = barrel:get(<<"testdb">>, <<"dd">>, []),
  timer:sleep(100),
  [
    #{ <<"seq">> := 3, <<"id">> := <<"cc">>},
    #{ <<"seq">> := 4, <<"id">> := <<"dd">>}
  ] = barrel_changes_listener:changes(Pid),
  
  ok = barrel_changes_listener:stop(Pid).

changes_feed_callback(_Config) ->
  Self = self(),
  Callback =
  fun(Change) ->
    Self ! {change, Change}
  end,
  Options = #{since => 0, mode => sse, changes_cb => Callback },
  {ok, Pid} = barrel_changes_listener:start_link(<<"testdb">>, Options),
  
  Doc1 = #{ <<"id">> => <<"aa">>, <<"v">> => 1},
  Doc2 = #{ <<"id">> => <<"bb">>, <<"v">> => 1},
  {ok, <<"aa">>, _} = barrel:post(<<"testdb">>, Doc1, []),
  {ok, <<"bb">>, _} = barrel:post(<<"testdb">>, Doc2, []),
  {ok, _, _} = barrel:get(<<"testdb">>, <<"aa">>, []),
  {ok, _, _} = barrel:get(<<"testdb">>, <<"bb">>, []),
  timer:sleep(100),
  [
    #{ <<"seq">> := 1, <<"id">> := <<"aa">>},
    #{ <<"seq">> := 2, <<"id">> := <<"bb">>}
  ] = collect_changes(2, queue:new()),
  ok = barrel_changes_listener:stop(Pid).


collect_changes(0, Q) ->
  queue:to_list(Q);
collect_changes(I, Q) ->
  receive
    {change, Change} ->
      collect_changes(I-1, queue:in(Change, Q))
  end.
