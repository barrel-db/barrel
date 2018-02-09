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

-module(barrel_db_sup).
-author("benoitc").
-behaviour(supervisor).


%% API
-export([
  start_link/0,
  start_db/1,
  stop_db/1
]).

-export([init/1]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_db(DbRef) ->
  _ = lager:info("start db: ref=~p", [DbRef]),
  supervisor:start_child(?MODULE, [DbRef]).

stop_db(DbPid) ->
  supervisor:terminate_child(?MODULE, DbPid).

init([]) ->
  SupFlags = #{strategy=> simple_one_for_one,
               intensity => 0,
               period => 1},
  Spec = #{id => barrel_db,
           start => {barrel_db, start_link, []},
           restart => temporary,
           shutdown => 5000},
  {ok, {SupFlags, [Spec]}}.