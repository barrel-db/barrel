                                                                                                                                                                                                                                                                                              %% Copyright 2016, Bernard Notarianni
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

-module(barrel_replicate_task).
-author("Bernard Notarianni").

-behaviour(gen_server).

%% specific API
-export([
  start_link/4,
  info/1
]).

%% gen_server API
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

%% internal api
-export([
  replication_key/1,
  delete/3
]).


-record(st, { id          ::binary()  % replication id
            , source
            , target
            , checkpoint              % checkpoint object
            , changes_since_pid
            , metrics
            , options
            }).



start_link(RepId, Source, Target, Options) ->
  gen_server:start_link(?MODULE, {RepId, Source, Target, Options}, []).


info(Pid) when is_pid(Pid)->
  gen_server:call(Pid, info).

delete(RepId, Source, Target) ->
  Checkpoint = barrel_replicate_checkpoint:new(RepId, Source, Target, []),
  ok = barrel_replicate_checkpoint:delete(Checkpoint),
  ok.

replication_key(RepId) -> {n, l, {barrel_replicate, RepId}}.

%% default is always a local db.
rep_resource({_Mod, _Uri}=Res) ->
  Res;
rep_resource(DbId) when is_binary(DbId) ->
  DefaultBackend = application:get_env(barrel_replicate, local_backend, barrel),
  {DefaultBackend, DbId};
rep_resource(_) ->
  erlang:error(bad_replication_uri).


%% gen_server callbacks

init({RepId, Source0, Target0, Options}) ->
  process_flag(trap_exit, true),
  {ok, Source} = maybe_connect(rep_resource(Source0)),
  {ok, Target} = maybe_connect(rep_resource(Target0)),
  Metrics = barrel_replicate_metrics:new(),
  Checkpoint = barrel_replicate_checkpoint:new(RepId, Source, Target, Options),
  StartSeq = barrel_replicate_checkpoint:get_start_seq(Checkpoint),
  case start_changes_feed_process(Source, StartSeq) of
    {ok, Pid} ->
      State = #st{id=RepId,
        source=Source,
        target=Target,
        checkpoint=Checkpoint,
        changes_since_pid=Pid,
        metrics=Metrics,
        options=Options},
      ok = barrel_replicate_metrics:create_task(Metrics, Options),
      barrel_replicate_metrics:update_task(Metrics),
      {ok, State};
    {error, Reason} ->
      {stop, Reason}
      
  end.
  
start_changes_feed_process(Source, StartSeq) ->
  Self = self(),
  Callback =
    fun(Change) ->
        Self ! {change, Change}
    end,
  SseOptions = #{since => StartSeq, mode => sse, changes_cb => Callback },
  {Backend, SourceUri} = Source,
  Backend:start_changes_listener(SourceUri, SseOptions).

handle_call(info, _From, State) ->
  RepId = State#st.id,
  Source = State#st.source,
  Checkpoint = State#st.checkpoint,
  History = case barrel_replicate_checkpoint:read_checkpoint_doc(Source, RepId) of
              {ok, Doc} ->
                maps:get(<<"history">>, Doc);
              _Other ->
                []
            end,
  Info = #{ id => State#st.id
          , source => State#st.source
          , target => State#st.target
          , last_seq => barrel_replicate_checkpoint:get_last_seq(Checkpoint)
          , metrics => State#st.metrics
          , checkpoints => History
          },

  {reply, Info, State};


handle_call(stop, _From, State) ->
  {stop, normal, stopped, State}.

handle_cast(shutdown, State) ->
  {stop, normal, State}.

handle_info({change, Change}, S) ->
  Source = S#st.source,
  Target = S#st.target,
  Checkpoint = S#st.checkpoint,
  Metrics = S#st.metrics,
  LastSeq = maps:get(<<"seq">>, Change),
  true = LastSeq > barrel_replicate_checkpoint:get_last_seq(Checkpoint),

  {ok, Metrics2} = barrel_replicate_alg:replicate(Source, Target, [Change], Metrics),
  Checkpoint2 = barrel_replicate_checkpoint:set_last_seq(LastSeq, Checkpoint),
  Checkpoint3 = barrel_replicate_checkpoint:maybe_write_checkpoint(Checkpoint2),

  S2 = S#st{checkpoint=Checkpoint3, metrics=Metrics2},
  barrel_replicate_metrics:update_task(Metrics2),
  {noreply, S2};



handle_info({'EXIT', Pid, Reason}, #st{changes_since_pid=Pid}=State) ->
  _ = lager:warning("[~s] changes process exited pid=~p reason=~p",[?MODULE_STRING, Pid, Reason]),
  Source = State#st.source,
  Checkpoint = State#st.checkpoint,
  
  case database_exist(Source) of
    true ->
      LastSeq = barrel_replicate_checkpoint:get_last_seq(Checkpoint),
      {ok, NewPid} = start_changes_feed_process(Source, LastSeq),
      _ = lager:warning("[~s] changes process restarted pid=~p",[?MODULE_STRING, NewPid]),
      {noreply, State};
    false ->
      _ = lager:warning("[~s] database ~p does not exist anymore. Stop task pid=~p",
                    [?MODULE_STRING, Source, self()]),
      {stop, normal, State}
  end;

handle_info({'EXIT', Pid, Reason}, State) ->
  _ = lager:error("[~s] exit from process pid=~p reason=~p",[?MODULE_STRING, Pid, Reason]),
  {stop, Reason, State}.

terminate(_Reason, State) ->
  #st{
    changes_since_pid = Pid,
    id = RepId,
    source = Source,
    target = Target
  } = State,
  
  barrel_replicate_metrics:update_task(State#st.metrics),
  _ = lager:debug(
    "barrel_replicate(~p} terminated: ~p",
    [RepId, _Reason]
  ),
  (catch barrel_replicate_checkpoint:write_checkpoint(State#st.checkpoint)),
  
  {Backend, _SourceUri} = Source,
  _ = (catch Backend:stop(Pid)),
  
  %% close the connections
  [maybe_close(Conn) || Conn <- [Source, Target]],
  ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.


maybe_connect({Backend, Uri}) ->
  {ok, Conn} = Backend:connect(Uri),
  {ok, {Backend, Conn}}.

%% TODO: really handle closing
%% maybe_close({Mod, ModState}) -> Mod:disconnect(ModState);
maybe_close(_) -> ok.

database_exist({Backend, Uri}) ->
  case catch Backend:database_infos(Uri) of
    {ok, _} -> true;
    _Else -> false
  end.
