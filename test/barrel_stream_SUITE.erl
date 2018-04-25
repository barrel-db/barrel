%% Copyright (c) 2018. Benoit Chesneau
%%
%% Licensed under the Apache License, Version 2.0 (the "License"); you may not
%% use this file except in compliance with the License. You may obtain a copy of
%% the License at
%%
%%    http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
%% License for the specific language governing permissions and limitations under
%% the License.
-module(barrel_stream_SUITE).
-author("benoitc").

%% API
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
  Config.

end_per_testcase(_, _Config) ->
  ok.

end_per_suite(Config) ->
  ok = application:stop(barrel),
  Config.


basic(_Config) ->
  BarrelId = <<"testdb">>,
  Batch = [
    {create, #{ <<"id">> => <<"a">>, <<"k">> => <<"v">>}},
    {create, #{ <<"id">> => <<"b">>, <<"k">> => <<"v2">>}}
  ],
  ok = barrel:create_barrel(BarrelId, #{}),
  timer:sleep(100),
  Stream = #{ barrel => BarrelId, interval => 100},
  ok = barrel_db_stream_mgr:subscribe(Stream, self(), 0),
  [{ok, <<"a">>, RevA },
   {ok, <<"b">> ,RevB }] = barrel_db:write_changes(BarrelId, Batch),
  timer:sleep(200),
  receive
    {changes, Stream, Changes, LastSeq} ->
      2 = LastSeq,
      [#{ <<"id">> := <<"a">>, <<"rev">> := RevA },
       #{ <<"id">> := <<"b">>, <<"rev">> := RevB }] = Changes,
      ok
  after 5000 ->
    erlang:error(timeout)
  end,
  ok = barrel_db_stream_mgr:unsubscribe(Stream, self()),
  timer:sleep(200),
  ok = barrel:delete_barrel(BarrelId),
  ok.

  
  