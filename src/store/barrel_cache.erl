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

%% @doc module to maintain the shared cache between databases.
-module(barrel_cache).
-author("benoitc").

%% API
-export([
  get_cache/0,
  get_usage/0,
  get_pinned_usage/0,
  set_capacity/1,
  get_capacity/0
]).

-export([start_link/1]).

-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

-type cache() :: binary() | reference().

-export_types([cache/0]).

%% @doc return the cache resource
-spec get_cache() -> cache().
get_cache() ->
  gen_server:call(?MODULE, get_cache).

%% @doc returns the memory size for a specific entry in the cache.
-spec get_usage() -> non_neg_integer().
get_usage() ->
  gen_server:call(?MODULE, get_usage).

%% @doc  returns the memory size for the entries in use by the system
-spec get_pinned_usage() -> non_neg_integer().
get_pinned_usage() ->
  gen_server:call(?MODULE, get_pinned_usage).

%% @doc  returns the memory size for the entries in use by the system
-spec set_capacity(Capacity :: non_neg_integer()) -> pk.
set_capacity(Capacity) ->
  gen_server:call(?MODULE, {set_capacity, Capacity}).

%% @doc  returns the maximum configured capacity of the cache.
-spec get_capacity() -> non_neg_integer().
get_capacity() ->
  gen_server:call(?MODULE, get_capacity).

start_link(Size) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [Size], []).

init([Size]) ->
  process_flag(trap_exit, true),
  _ = lager:info("initialize cache with ~p bytes size.", [Size]),
  rocksdb:new_lru_cache(Size).

handle_call(get_cache, _From, Cache) ->
  {reply, Cache, Cache};

handle_call(get_usage, _From, Cache) ->
  {reply, rocksdb:get_usage(Cache), Cache};

handle_call(get_pinned_usage, _From, Cache) ->
  {reply, rocksdb:get_pinned_usage(Cache), Cache};

handle_call({set_capacity, Capacity}, _From, Cache) ->
  {reply, rocksdb:set_capacity(Cache, Capacity), Cache};

handle_call(get_capacity, _From, Cache) ->
  {reply, rocksdb:get_capacity(Cache), Cache};

handle_call(_Msg, _From, Cache) ->
  {reply, bad_call, Cache}.

handle_cast(_Msg, Cache) ->
  {noreply, Cache}.

handle_info(_Info, Cache) ->
  {noreply, Cache}.

terminate(_Reason, Cache) ->
  _ = (catch rocksdb:release_cache(Cache)),
  ok.

code_change(_OldVsn, Cache, _Extra) ->
  {ok, Cache}.