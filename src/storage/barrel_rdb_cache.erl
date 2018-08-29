%% Copyright 2018 Benoit Chesneau
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

-module(barrel_rdb_cache).
-author("benoitc").

%% API
-export([
  get_pinned_usage/0,
  get_usage/0,
  get_cache/0,
  get_capacity/0
]).

-export([start_link/0]).

-export([
  init/1,
  handle_call/3,
  handle_cast/2
]).

-define(CACHE, 128 * 16#100000). % 128 Mib


get_usage() ->
  rocksdb:get_usage(get_cache()).

get_pinned_usage() ->
  rocksdb:get_pinned_usage(get_cache()).

get_capacity() ->
  rocksdb:get_capacity(get_cache()).

get_cache() -> gen_server:call(?MODULE, get_cache).


start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
  {ok, CacheSize} = barrel_resource_monitor_misc:parse_information_unit(
    application:get_env(barrel, cache, ?CACHE)
  ),
  {ok, Cache} = rocksdb:new_lru_cache(CacheSize),
  {ok, Cache}.

handle_call(get_cache, _From, Cache) ->
  {reply, Cache, Cache}.

handle_cast(_Msg, Cache) -> {noreply, Cache}.