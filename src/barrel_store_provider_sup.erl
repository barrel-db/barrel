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
-module(barrel_store_provider_sup).
-behaviour(supervisor).
-author("benoitc").

%% API
-export([
  start_store/3, start_store/4,
  stop_store/1,
  all_stores/0
]).

-export([start_link/0]).

%% supervisor callback
-export([init/1]).

start_store(Name, Mod, Args) ->
  start_store(Name, Mod, Mod, Args).

start_store(Name, Mod, IMod, Args) ->
  Spec = store_spec(Name, Mod, IMod, Args),
  supervisor:start_child(?MODULE, Spec).

stop_store(Name) ->
  case supervisor:terminate_child(?MODULE, Name) of
    ok ->
      supervisor:delete_child(?MODULE, Name);
    Error ->
      Error
  end.

all_stores() ->
  [Id || {Id, _Child, _Type, _Modules} <- supervisor:which_children(?MODULE)].


start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
  Stores = application:get_env(barrel, stores, []),
  _ = empty_stores_warning(Stores),
  Specs = lists:map(
    fun
      ({Name, Mod, Args}) -> store_spec(Name, Mod, Mod, Args);
      ({Name, Mod, IMod, Args}) -> store_spec(Name, Mod, IMod, Args)
    end,
    Stores
  ),
  SupFlags = #{strategy => one_for_one,
               intensity => 5,
               period => 10},
  {ok, {SupFlags, Specs}}.

store_spec(Name, Mod, IMod, Args) ->
  #{id => Name,
    start => {barrel_store_provider, start_link, [Name, Mod, IMod, Args]},
    restart => permanent,
    shutdown => 5000,
    type => worker,
    modules => [Mod]}.

empty_stores_warning([]) ->
  _ = lager:notice("no storage setup.~n", []),
  ok;
empty_stores_warning(_) ->
  ok.