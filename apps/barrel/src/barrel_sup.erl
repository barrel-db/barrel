%% Copyright 2016-2017, Benoit Chesneau
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

%%%-------------------------------------------------------------------
%% @doc barrel top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(barrel_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-include("barrel.hrl").

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
  supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
  ok = barrel_config:init(),
  Specs = [
    %% services management
    %% TODO: put services in their own supervisor?
    #{id => barrel_services_sup,
      start => {barrel_services_sup, start_link, []},
      type => supervisor},

    #{id => barrel_services,
      start => {barrel_services, start_link, []}},

    %% persist time server
    #{
      id => time_server,
      start => {barrel_time_server, start_link, []}
    },

    %% barrel event server
    #{id => barrel_event,
      start => {barrel_event, start_link, []},
      shutdown => infinity,
      type => worker
    },

    #{id => barrel_storage,
      start => {barrel_storage, start_link, []},
      shutdown => infinity,
      type => worker
    },

    %% safe supervisor
    #{id => barrel_safe_sup,
      start => {supervisor, start_link, [{local, barrel_safe_sup}, ?MODULE, safe]},
      type => supervisor,
      modules => [?MODULE]}
  ],

  {ok, { {one_for_one, 4, 2000}, Specs} };

init(safe) ->
  Specs =
    [
     %% transaction server
     #{id => barrel_tx,
       start => {barrel_tx, start_link, []}
      },
     %% barrel db supervisor
     #{id => barrel_server_sup,
       start => {barrel_server_sup, start_link, []}
      }
    ],

  {ok, { {one_for_one, 4, 2000}, Specs} }.