%% Copyright 2018, Benoit Chesneau
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

-module(barrel_rocksdb_provider).
-author("benoitc").

%% Provider API
-export([
  init/2,
  handle_call/3,
  handle_cast/2,
  terminate/2
]).

%% for tests
-export([get_cache/1]).

-define(CACHE, 128 * 16#100000). % 128 Mib


get_cache(Name) ->
  barrel_store_provider:call(Name, get_cache).


init(_Name, Options) ->
  {ok, CacheSize} = barrel_resource_monitor_misc:parse_information_unit(
    maps:get(cache, Options, ?CACHE)
  ),
  {ok, Cache} = rocksdb:new_lru_cache(CacheSize),
  {ok, #{ cache => Cache }}.

handle_call(get_cache, _From, #{ cache := Cache } = State) ->
  {reply, Cache, State};

handle_call(_Msg, _From, State) ->
  {reply, bad_call, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

terminate(_Reason, #{ cache := Cache }) ->
  _ = rocksdb:release_cache(Cache),
  ok.