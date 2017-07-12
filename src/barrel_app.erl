%% Copyright 2016, Benoit Chesneau
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

-module(barrel_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
  {ok, Sup} = barrel_sup:start_link(),
  %% load default rpc services
  ok = load_services(),
  {ok, Sup}.


%%--------------------------------------------------------------------
stop(_State) ->
  ok.

services() ->
  #{
    'barrel.v1.Database' => barrel_db_service,
    'barrel.v1.DatabaseChanges' => barrel_changes_services,
    'barrel.v1.Replicate' => barrel_replicate_service
  }.

load_services() ->
  Services = services(),
  ok = barrel_rpc:load_services(Services),
  ok.