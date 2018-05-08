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

start_link() -> supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
  
  Specs = [%% extensions supervisor
           #{id => barrel_ext_sup,
             start => {barrel_ext_sup, start_link, []},
             restart => permanent,
             shutdown => infinity,
             type => supervisor,
             modules => [barrel_ext_sup]},

           #{id => barrel_monitor_sup,
             start => {barrel_monitor_sup, start_link, []},
             restart => permanent,
             shutdown => infinity,
             type => supervisor,
             modules => [barrel_monitor_sup]},

           %% persist time server
           #{
             id => persist_time_server,
             start => {barrel_ts, start_link, []},
             restart => permanent,
             shutdown => 5000,
             type => worker,
             modules => [barrel_flake_ts]
           },

           %% stats
           #{
             id => statistics,
             start => {barrel_statistics, start_link, []},
             restart => permanent,
             shutdown => infinity,
             type => worker,
             module => [barrel_statistics]
           },
           
           
           %% main jobs supervisor
           #{id => barrel_jobs_sup,
             start => {barrel_jobs_sup, start_link, []},
             restart => permanent,
             shutdown => infinity,
             type => supervisor,
             modules => [barrel_jobs_sup]},

           %% barrel event server
           #{id => barrel_event,
             start => {barrel_event, start_link, []},
             restart => permanent,
             shutdown => infinity,
             type => worker,
             modules => [barrel_event]
           },

           %% stores supervisor
           #{id => barrel_store_sup,
             start => {barrel_store_sup, start_link, []},
             restart => permanent,
             shutdown => infinity,
             type => supervisor,
             modules => [barrel_store_sup]},

           %% barrel containers supervisor
           #{id => barrel_index_actor_sup,
             start => {barrel_index_actor_sup, start_link, []},
             restart => permanent,
             shutdown => infinity,
             type => supervisor,
             modules => [barrel_index_actor_sup]
           },
           
           %% barrel containers supervisor
           #{id => barrel_db_sup,
             start => {barrel_db_sup, start_link, []},
             restart => permanent,
             shutdown => infinity,
             type => supervisor,
             modules => [barrel_db_sup]
           },

           %% changes streams supervisor
           #{id => barrel_db_stream_sup,
             start => {barrel_db_stream_sup, start_link, []},
             restart => permanent,
             shutdown => infinity,
             type => supervisor,
             modules => [barrel_db_stream_sup]
           }
  
    ],
  
  
  
  {ok, { {one_for_one, 4, 2000}, Specs} }.