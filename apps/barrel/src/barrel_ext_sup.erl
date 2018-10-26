%%% Copyright (c) 2018. Benoit Chesneau
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

-module(barrel_ext_sup).
-author("benoitc").

%% API
-export([start_proc/5, stop_proc/1]).

-export([start_link/0]).

%% supervisor callbacks
-export([init/1]).

-define(SHUTDOWN, 120000).  % 2 minutes

start_proc(Name, M, F, A, Opts) ->
  Spec =#{id => Name,
          start => {M, F, A},
          restart => maps:get(restart, Opts, transient),
          shutdown => maps:get(shutdown, Opts, ?SHUTDOWN),
          type => maps:get(type, Opts, worker),
          modules => maps:get(modules, Opts, [M]) },
  case supervisor:start_child(?MODULE, Spec) of
    {error, already_present} ->
      supervisor:restart_child(?MODULE, Name);
    Other ->
      Other
  end.

stop_proc(Name) ->
  supervisor:terminate_child(?MODULE, Name).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
  {ok, {#{strategy => one_for_all,
          intensity => 10,
          period => 60}, []}}.