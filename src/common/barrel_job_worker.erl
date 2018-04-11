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
  handle_work/3,
  handle_request/4
]).

%% gen_server callbacks
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2
]).

-include("barrel.hrl").

-dialyzer({nowarn_function, enqueue/0}).


handle_work(Worker, From, Job) ->
  gen_server:cast(Worker, {work, From, Job}).

handle_request(Worker, From, Cmd, DbPid) ->
  gen_server:cast(Worker, {request, From, Cmd, DbPid}).


start_link() ->
  gen_server:start_link(?MODULE, [], []).

init([]) ->
  {ok, #{}}.


handle_call(Msg, _From, State) ->
  _ = lager:debug("db worker received a synchronous event: ~p~n", [Msg]),
  {reply, ok, State}.

handle_cast({work, {Pid, _} = From, MFA},  St) ->
  MRef = erlang:monitor(process, Pid),
  Res = (catch exec(MFA)),
  erlang:demonitor(MRef, [flush]),
  case Res of
    stop -> {stop, normal, St};
    _ ->
      reply(From, Res),
      _ = enqueue(),
      {noreply, St}
  end;

handle_cast({request, From, Cmd, DbPid},  St) ->
  handle_command(Cmd, From, DbPid, St);

handle_cast(_Msg, St) ->
  {noreply, St}.

handle_info({'DOWN', _, _, _, _}, State) ->
  {stop, normal, State};
handle_info(_Info, State) ->
  {noreply, State}.

handle_command(Cmd, {Pid, _} = From, DbPid, State) ->
  {Mod, ModState} = barrel_db:get_state(DbPid),
  MFA = case Cmd of
          {fetch_doc, DocId, Options} ->
            {barrel_db, do_fetch_doc, [DocId, Options, {Mod, ModState}]};
          {put_local_doc, DocId, Doc} ->
            {Mod, put_local_doc, [DocId, Doc, ModState]};
          {get_local_doc, DocId} ->
            {Mod, get_local_doc, [DocId, ModState]};
          {delete_local_doc, DocId} ->
            {Mod, delete_local_doc, [DocId, ModState]}
        end,
  
  MRef = erlang:monitor(process, Pid),
  Res = (catch exec(MFA)),
  erlang:demonitor(MRef, [flush]),
  reply(From, Res),
  {stop, normal, State}.


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


exec({F, A}) ->
  erlang:apply(F, A);
exec({M, F, A}) ->
  erlang:apply(M, F, A);
exec(F) when is_function(F) ->
  F();
exec(_) ->
  erlang:error(badarg).