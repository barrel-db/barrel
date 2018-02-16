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

-module(barrel_job_worker).
-author("benoitc").
-behaviour(gen_server).

%% API
-export([
  start_link/0,
  handle_work/3
]).

%% gen_server callbacks
-export([
  init/1,
  handle_call/3,
  handle_cast/2
]).

-include("barrel.hrl").

-dialyzer({nowarn_function, enqueue/0}).


handle_work(Worker, From, Job) ->
  gen_server:cast(Worker, {work, From, Job}).

start_link() ->
  gen_server:start_link(?MODULE, [], []).

init([]) ->
  {ok, #{}}.


handle_call(Msg, _From, State) ->
  _ = lager:debug("db worker received a synchronous event: ~p~n", [Msg]),
  {reply, ok, State}.

handle_cast({work, From, {M, F, A}},  St) ->
  Res = (catch erlang:apply(M, F, A)),
  reply(From, Res),
  _ = enqueue(),
  {noreply, St};
handle_cast(_Msg, St) ->
  {noreply, St}.

reply({To, Tag}, Reply)  ->
  _ = lager:debug("reply to=~p, reply=~p~n", [To, Reply]),
  Msg = {Tag, Reply},
  try To ! Msg of
    _ ->
      ok
  catch
    _:_ -> ok
  end.

enqueue() ->
  Pool = whereis(?jobs_pool),
  sbroker:async_ask_r(?jobs_broker, self(), {Pool, self()}).