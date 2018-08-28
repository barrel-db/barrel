%% Copyright 2017, Benoit Chesneau
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


%% TODO: replace this by "instrument" when ready
-module(barrel_statistics).
-author("benoitc").


-export([
  set_ticker_count/2,
  record_tick/2,
  get_ticker_count/1,
  reset_ticker_count/1,
  reset_tickers/0
]).


%% API
-export([start_link/0]).

%% gen server callbacks
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).


-include("barrel_statistics.hrl").

set_ticker_count(TickerType, Count) ->
  ets:update_element(?MODULE, TickerType, {2, Count}).

record_tick(TickerType, Count) ->
  ets:update_counter(?MODULE, TickerType, {2, Count}).

get_ticker_count(TickerType) ->
  ets:update_counter(?MODULE, TickerType, {2, 0}).

reset_ticker_count(TickerType) ->
  set_ticker_count(TickerType, 0).

reset_tickers() ->
  lists:foreach(
    fun(Entry) ->
      reset_ticker_count(Entry)
    end, ?TICKERS).


start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
  _  = ets:new(?MODULE, [ordered_set, public, named_table, {read_concurrency, true}, {write_concurrency, true}]),
  lists:foreach(
    fun(Entry) ->
      ets:insert(?MODULE, {Entry, 0})
    end,
    ?TICKERS
  ),
  {ok, #{}}.

handle_call(_Msg, _From, State) ->
  {reply, ok, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.
