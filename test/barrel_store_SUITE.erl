%% Copyright (c) 2017. Benoit Chesneau
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

-module(barrel_store_SUITE).
-author("benoitc").

%% API
-export([
  all/0,
  init_per_suite/1,
  end_per_suite/1,
  init_per_testcase/2,
  end_per_testcase/1
]).

-export([
  create_db/1,
  persist_db/1,
  persist_ephemeral_db/1,
  create_and_stop_db/1
]).

-include("barrel.hrl").

all() ->
  [
    create_db,
    persist_db,
    persist_ephemeral_db,
    create_and_stop_db
  ].

init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(barrel),
  Config.

end_per_suite(Config) ->
  ok = application:stop(barrel),
  Config.


init_per_testcase(_, Config) ->
  Config.

end_per_testcase(_Config) ->
  ok.

create_db(_Config) ->
  {ok, #{ <<"database_id">> := <<"testdb">>}} = barrel_store:create_db(#{ <<"database_id">> => <<"testdb">> }),
  [<<"testdb">>] = barrel_store:databases(),
  true = barrel_db:exists(<<"testdb">>),
  {error, db_exists} = barrel:create_database(#{ <<"database_id">> => <<"testdb">> }),
  {ok, #{ <<"database_id">> := <<"testdb1">>}} = barrel_store:create_db(#{ <<"database_id">> => <<"testdb1">> }),
  [<<"testdb">>, <<"testdb1">>] = barrel_store:databases(),
  ok = barrel_store:delete_db(<<"testdb">>),
  [] = ets:lookup(barrel_dbs, <<"testdb">>),
  [<<"testdb1">>] = barrel_store:databases(),
  ok = barrel_store:delete_db(<<"testdb1">>),
  [] = barrel_store:databases().


persist_db(_Config) ->
  {ok, #{ <<"database_id">> := <<"testdb">>}} = barrel_store:create_db(#{ <<"database_id">> => <<"testdb">> }),
  [<<"testdb">>] = barrel_store:databases(),
  ok = application:stop(barrel),
  timer:sleep(200),
  ok = application:start(barrel),
  [<<"testdb">>] = barrel_store:databases(),
  ok = barrel_store:delete_db(<<"testdb">>),
  [] = barrel_store:databases(),
  ok = application:stop(barrel),
  timer:sleep(100),
  {ok, _} = application:ensure_all_started(barrel),
  [] = barrel_store:databases().


persist_ephemeral_db(_Config) ->
  DbConfig = #{ <<"database_id">> => <<"testmemdb">>, <<"in_memory">> => true},
  {ok, #{ <<"database_id">> := <<"testmemdb">>}} = barrel_store:create_db(DbConfig),
  [<<"testmemdb">>] = barrel_store:databases(),
  ok = application:stop(barrel),
  timer:sleep(200),
  ok = application:start(barrel),
  [<<"testmemdb">>] = barrel_store:databases(),
  ok = barrel_store:delete_db(<<"testmemdb">>),
  [] = barrel_store:databases(),
  ok = application:stop(barrel),
  timer:sleep(100),
  {ok, _} = application:ensure_all_started(barrel),
  [] = barrel_store:databases().


create_and_stop_db(_Config) ->
  {ok, #{ <<"database_id">> := <<"testdb">>}} = barrel_store:create_db(#{ <<"database_id">> => <<"testdb">> }),
  [<<"testdb">>] = barrel_store:databases(),
  ok = application:stop(barrel),
  timer:sleep(400),
  ok = application:start(barrel),
  {error, db_exists} = barrel:create_database(#{ <<"database_id">> => <<"testdb">> }),
  ok = barrel_store:delete_db(<<"testdb">>),
  [] = barrel_store:databases().