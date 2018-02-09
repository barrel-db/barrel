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

-module(barrel_jobs_sup).
-author("benoitc").
-behaviour(supervisor).


%% API
-export([start_link/0]).

-export([init/1]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
  Specs = [
    %% broker
    #{ id => broker,
       start => {barrel_jobs_broker, start_link, []},
       restart => permanent,
       shutdown => 5000,
       type => worker,
       modules => [barrel_jobs_broker]
    },
    %% pool manager
    #{ id => pool,
       start => {barrel_jobs_pool, start_link, []},
       restart => permanent,
       shutdown => 5000,
       type => worker,
       modules => [barrel_jobs_pool]
    }
  ],
  SupFlags = #{ strategy => one_for_one, intensity => 5, period => 10 },
  {ok, {SupFlags, Specs}}.