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

-module(barrel_jobs_broker).
-author("benoitc").

%% API
-export([start_link/0]).
-export([init/1]).

-include("barrel.hrl").


start_link() ->
  sbroker:start_link({local, ?MODULE}, ?MODULE, [], [{read_time_after, 16}]).

%% TODO: add regulator
init([]) ->
  JobSQueueLimit = application:get_env(barrel, jobs_queue_limit, ?JOBS_QUEUE_LIMIT),
  JobsQueueTimeout = application:get_env(barrel, jobs_queue_timeout, ?JOBS_QUEUE_TIMEOUT),
  %%JobsIdleMaxLimit = application:get_env(barrel, jobs_idle_max_limit, ?JOBS_IDLE_MAX_LIMIT),
  QueueSpec = {sbroker_timeout_queue, #{out => out,
                                        timeout => JobsQueueTimeout,
                                        drop => drop_r,
                                        min => 0,
                                        max => JobSQueueLimit}}, %% TODO: optimize
  WorkerSpec = {sbroker_drop_queue, #{ max => infinity }},
  {ok,  {QueueSpec, WorkerSpec, []}}.



