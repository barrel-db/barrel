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
-module(barrel_index_actor).
-author("benoitc").
-behaviour(gen_server).


%% API
-export([
  refresh/1
]).

-export([start_link/4]).

-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2
]).

-export([where/1]).

-include("barrel.hrl").

refresh(DbRef) ->
  case barrel_db:updated_seq(DbRef) of
    {ok, UpdatedSeq} ->
      call(DbRef, {refresh, UpdatedSeq});
    Error ->
      Error
  end.


call(DbRef, Msg) ->
  with_index_actor(
    DbRef,
    fun(IndexPid) ->
      call_1(IndexPid, Msg, infinity)
    end
  ).


call_1(IndexPid, Msg, Timeout) ->
  try gen_server:call(IndexPid, Msg, Timeout)
  catch
    exit:{noproc,_} -> {error, db_not_found};
    exit:noproc ->  {error, db_not_found};
    %% Handle the case where the monitor triggers
    exit:{normal, _} -> {error, db_not_found}
  end.



with_index_actor(DbRef, Fun) ->
  case where(DbRef) of
    IndexPid when is_pid(IndexPid) ->
      Fun(IndexPid);
    undefined ->
      {error, db_not_found}
  end.

where({DbName, Node}) ->
  rpc:call(Node, gproc, whereis_name, [?index(DbName)]);
where(DbName) ->
  gproc:whereis_name(?index(DbName)).



start_link(DbName, DbPid, Mod, State) ->
  gen_server:start_link({via, gproc, ?index(DbName)}, ?MODULE, [DbName, DbPid, Mod, State], []).

init([DbName, DbPid, Mod, ModState]) ->
  process_flag(trap_exit, true),
  StartSeq = Mod:get_indexed_seq(ModState),
  {ok, Updater} = barrel_index_updater:start_link(self(), DbName, Mod, ModState, StartSeq),
  State = #{ name => DbName,
             db => DbPid,
             mod => Mod,
             modstate => ModState,
             indexed_seq => StartSeq,
             updater => Updater,
             waiters => [] },
  {ok, State}.


handle_call({refresh, UpdatedSeq}, From, State = #{ indexed_seq := IndexedSeq }) when IndexedSeq < UpdatedSeq ->
  #{ waiters := Waiters } = State,
  Waiters2 = Waiters ++ [From],
  {noreply, State#{ waiters => Waiters2 }};
handle_call({refresh, _UpdateSeq}, _From, State) ->
  {reply, ok, State};
handle_call(_Msg, _From, State) ->
  {reply, bad_call, State}.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info({updated, IndexedSeq}, State) ->
  case maps:get(waiters, State) of
    []->
      set_indexed_seq(IndexedSeq, State),
      {noreply, State#{ indexed_seq => IndexedSeq }};
    Waiters ->
      set_indexed_seq(IndexedSeq, State),
      notify(Waiters, ok),
      {noreply, State#{ indexed_seq => IndexedSeq, waiters => []}}
  end;

handle_info({'EXIT', DbPid, Reason}, State = #{ db := DbPid }) ->
  _ = lager:info("closing index, db=~p down=~p~n", [maps:get(name, State), Reason]),
  notify(maps:get(waiters, State), {error, db_not_found}),
  {stop, normal, State};

handle_info({'EXIT', Updater, Reason}, State = #{ updater := Updater }) ->
  #{ db := DbPid, name := DbName, indexed_seq := StartSeq } = State,
  _ = lager:info("index updater for db=~p exited with reason=~p~n", [DbName, Reason]),
  {ok, NewUpdater} = barrel_index_updater:start_link(self(), DbName, DbPid, StartSeq),
  {noreply, #{ updater => NewUpdater }};

handle_info(_Info, State) ->
  {noreply, State}.


notify([], _Msg) -> ok;
notify([Waiter | Rest], Msg) ->
  gen_server:reply(Waiter, Msg),
  notify(Rest, Msg).


set_indexed_seq(Seq, #{ mod := Mod, modstate := ModState}) ->
  Mod:set_indexed_seq(Seq, ModState).