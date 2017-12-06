%% Copyright 2017 Benoit Chesneau
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

%% specific API
-export([
  start_link/1,
  info/1
]).

%% states API
-export([
  init/2,
  wait_for_source/1,
  wait_for_target/1,
  wait_for_changes/1
]).

%% internal api
-export([
  replication_key/1,
  stream_worker/3,
  wait_for_resume/2,
  loop_changes/1
]).

-export([
  system_continue/3,
  system_code_change/4,
  system_terminate/4
]).



-record(st, {
  id          ::binary()  % replication id
  , source
  , target
  , parent
  , checkpoint              % checkpoint object
  , stream
  , metrics
  , options
  , last_seq
  , keepalive
  , config
}).


start_link(Config) ->
  proc_lib:start_link(?MODULE, init, [self(), Config]).

info(Pid) when is_pid(Pid)->
  MRef = erlang:monitor(process, Pid),
  Pid ! {get_state, {self(), MRef}},
  receive
    {replication_state, MRef, State} ->
      erlang:demonitor(MRef, [flush]),
      {ok, State};
    {'DOWN', MRef, process, _, Reason} ->
      erlang:error(Reason)
  end.

replication_key(RepId) -> {n, l, {barrel_replicate, RepId}}.

spawn_stream_worker(Source, StartSeq) ->
  spawn_link(?MODULE, stream_worker, [self(), Source, StartSeq]).

stream_worker(Parent, Source, StartSeq) ->
  Stream = barrel_replicate_api_wrapper:subscribe_changes(Source, StartSeq, #{}),
  stream_loop(Parent, Source, Stream).

stream_loop(Parent, Source, Stream) ->
  case  barrel_replicate_api_wrapper:await_change(Stream, infinity) of
    Change when is_map(Change) ->
      Parent ! {change, self(), Change},
      stream_loop(Parent, Source, Stream);
    {done, _LastSeq} ->
      exit(normal)
  end.

stop_stream_worker(Stream) when is_pid(Stream) ->
  unlink(Stream),
  MRef = erlang:monitor(process, Stream),
  exit(Stream, shutdown),
  receive
    {'DOWN', MRef, process, Stream, shutdown} ->
      ok
  end;
stop_stream_worker(_) ->
  ok.

nodes_to_watch(#{ source := {Source, _}, target := {Target, _} }) ->
  [Source, Target];
nodes_to_watch(#{ source := {Source, _} }) ->
  [Source];
nodes_to_watch(#{ target := {Target, _} }) ->
  [Target];
nodes_to_watch(_) ->
  [].

register_if_keepalive(#{ keepalive := true} = Config) ->
  barrel_replicate_monitor:register(self(), nodes_to_watch(Config));
register_if_keepalive(_) ->
  ok.

unregister_if_keepalive(#st{keepalive=true}) ->
  barrel_replicate_monitor:unregister(self());
unregister_if_keepalive(_) ->
  ok.

init(Parent, Config) ->
  process_flag(trap_exit, true),
  proc_lib:init_ack(Parent, {ok, self()}),
  barrel_statistics:record_tick(num_replications_started, 1),
  %% extract config
  #{ id := RepId,
     source := Source,
     target := Target} = Config,
  KeepAlive = maps:get(keepalive, Config, false),
  _ = gproc:reg(replication_key(RepId)),
  %% register if need to be kept alive
  ok = register_if_keepalive(Config),
  %% start loop
  State = #st{ id = RepId,
               source = Source,
               target = Target,
               parent = Parent,
               keepalive = KeepAlive,
               config = Config},
  wait_for_source(State).


wait_for_source(#st{ source = Db, keepalive = KeepAlive } = State) ->
  case barrel_replicate_api_wrapper:database_infos(Db) of
    {ok, _} ->
      wait_for_target(State);
    {bad_rpc, nodedown} ->
      receive
        Event -> handle_event(Event, wait_for_source, State)
      end;
    Error ->
      case KeepAlive of
        true ->
          B = backoff:init(200, 20000, self(), ping),
          B2 = backoff:type(B, jitter),
          TRef = backoff:fire(B2),
          wait_for_source(State, TRef, B2);
        false ->
          erlang:error({source_error, {Db, Error}})
      end
  end.

wait_for_source(#st{ source = RemoteDb } = State, TRef, B ) ->
  receive
    {timeout, TRef, ping} ->
      case barrel_replicate_api_wrapper:database_infos(RemoteDb) of
        {ok, _} ->
          wait_for_target(State);
        {bad_rpc, nodedown} ->
          receive
            Event -> handle_event(Event, wait_for_source, State)
          end;
        _Error ->
          {_, B2} = backoff:fail(B),
          NewTref = backoff:fire(B2),
          wait_for_source(State, NewTref, B2)
      end;
    Event ->
      handle_event(Event, wait_for_source, State)
  end.

wait_for_target(#st{ target = Db, keepalive = KeepAlive } = State) ->
  case barrel_replicate_api_wrapper:database_infos(Db) of
    {ok, _} ->
      wait_for_changes(State);
    {bad_rpc, nodedown} ->
      receive
        Event -> handle_event(Event, wait_for_source, State)
      end;
    Error ->
      case KeepAlive of
        true ->
          B = backoff:init(200, 20000, self(), ping),
          B2 = backoff:type(B, jitter),
          TRef = backoff:fire(B2),
          wait_for_target(State, TRef, B2);
        false ->
          erlang:error({source_error, {Db, Error}})
      end
  end.

wait_for_target(#st{ target = {_, _} = RemoteDb } = State, TRef, B ) ->
  receive
    {timeout, TRef, ping} ->
      case barrel_replicate_api_wrapper:database_infos(RemoteDb) of
        {ok, _} ->
          wait_for_changes(State);
        {bad_rpc, nodedown} ->
          receive
            Event -> handle_event(Event, wait_for_source, State)
          end;
        _Error ->
          {_, B2} = backoff:fail(B),
          NewTref = backoff:fire(B2),
          wait_for_target(State, NewTref, B2)
      end;
    Event ->
      handle_event(Event, wait_for_source, State)
  end.


wait_for_changes(#st{ source=Source, config=Config } = State0) ->
  Options = maps:get(options, Config, #{}),
  %% init_metrics
  Metrics = barrel_replicate_metrics:new(),
  ok = barrel_replicate_metrics:create_task(Metrics, Options),
  barrel_replicate_metrics:update_task(Metrics),
  %% initialize the changes feed
  Checkpoint = barrel_replicate_checkpoint:new(Config),
  StartSeq = barrel_replicate_checkpoint:get_start_seq(Checkpoint),
  Stream = spawn_stream_worker(Source, StartSeq),
  %% udpate the state
  State1 = State0#st{ checkpoint = Checkpoint,
                      stream = Stream,
                      metrics = Metrics,
                      last_seq = StartSeq},
  loop_changes(State1).

loop_changes(State = #st{stream=Stream, parent=Parent, keepalive=KeepAlive}) ->
  receive
    {change, Stream, Change} ->
      NewState = handle_change(Change, State),
      loop_changes(NewState);
    pause ->
      ok = stop_stream_worker(Stream),
      proc_lib:hibernate(?MODULE, loop_changes, [State#st{stream=nil}]);
    resume ->
      #st{ source = Source, last_seq = LastSeq} = State,
      Stream = spawn_stream_worker(Source, LastSeq),
      loop_changes(State#st{ stream = Stream });
    stop ->
      ok = unregister_if_keepalive(State),
      exit(normal);
    {get_state, From} ->
      ok = handle_get_state(From, State),
      loop_changes(State);
    {'EXIT', Stream, remote_down} ->
      case KeepAlive of
        true ->
          loop_changes(State#st{stream=nil});
        fakse ->
          barrel_statistics:record_tick(num_replications_errors, 1),
          NewState = handle_stream_exit(remote_down, State),
          loop_changes(NewState)
      end;
    {'EXIT', Stream, Reason} ->
      barrel_statistics:record_tick(num_replications_errors, 1),
      NewState = handle_stream_exit(Reason, State),
      loop_changes(NewState);
    {'EXIT', Parent, Reason} ->
      terminate(Reason, State);
    {system, From, Request} ->
      sys:handle_system_msg(
        Request, From, Parent, ?MODULE, [],
        {loop_changes, State});
    Other ->
      _ = lager:error("~s: got unexpected message: ~p~n", [?MODULE_STRING, Other]),
      exit({unexpected_message, Other})
  end.

handle_event(Event, StateFun, #st{parent=Parent}=State) ->
  case Event of
    pause ->
      proc_lib:hibernate(?MODULE, wait_for_resume, [StateFun, State#st{stream=nil}]);
    resume ->
      StateFun(State);
    stop ->
      ok = unregister_if_keepalive(State),
      exit(normal);
    {get_state, From} ->
      ok = handle_get_state(From, State),
      StateFun(State);
    {'EXIT', Parent, Reason} ->
      terminate(Reason, State);
    {system, From, Request} ->
      sys:handle_system_msg(
        Request, From, Parent, ?MODULE, [],
        {StateFun, State});
    Other ->
      _ = lager:error("~s: got unexpected message: ~p~n", [?MODULE_STRING, Other]),
      exit({unexpected_message, Other})
  end.

wait_for_resume(StateFun, State) ->
  receive
    Event ->
      handle_event(Event, StateFun, State)
  end.

handle_change(Change, State) ->
  #st{
    source=Source,
    target=Target,
    checkpoint=Checkpoint,
    metrics=Metrics
  } = State,
  LastSeq = maps:get(<<"seq">>, Change),
  %% TODO: better handling of edge case, asserting is quite bad there
  true = (LastSeq > barrel_replicate_checkpoint:get_last_seq(Checkpoint)),
  {ok, NewMetrics} = barrel_replicate_alg:replicate(Source, Target, [Change], Metrics),
  NewCheckpoint = barrel_replicate_checkpoint:maybe_write_checkpoint(
    barrel_replicate_checkpoint:set_last_seq(LastSeq, Checkpoint)
  ),
  %% notify metrics
  barrel_replicate_metrics:update_task(NewMetrics),
  State#st{ checkpoint=NewCheckpoint, metrics=NewMetrics, last_seq=LastSeq }.

handle_get_state({FromPid, FromTag}, State) ->
  #st{
    id = RepId,
    source = Source,
    target = Target,
    metrics = Metrics,
    checkpoint = Checkpoint
  } = State,
  LastSeq = barrel_replicate_checkpoint:get_last_seq(Checkpoint),
  Checkpoints = case barrel_replicate_checkpoint:read_checkpoint_doc(Source, RepId) of
                  {ok, Doc} ->  maps:get(<<"history">>, Doc);
                  _Other -> []
                end,
  Info = #{
    id => RepId,
    source => Source,
    target => Target,
    last_seq => LastSeq,
    metrics => Metrics,
    checkpoints => Checkpoints },
  _ = FromPid ! {replication_state, FromTag, Info},
  ok.

-spec handle_stream_exit(_, _) -> no_return().
handle_stream_exit({{error, {shutdown ,db_down}}, _}, _State) ->
  %% TODO: is this a normal condition ? We should probably retry there?
  _ = lager:debug("~s, db shutdown:~n~p~n~n", [?MODULE_STRING, _State]),
  exit(normal);
handle_stream_exit(Reason, #st{ id = RepId} = State) ->
  _ = lager:debug(
    "~s, ~p change stream exited:~p~n~p~n~n",
    [?MODULE_STRING, RepId, Reason, State]
  ),
  exit(normal).

system_continue(_, _, {StateFun, State}) ->
  StateFun(State).

-spec system_terminate(any(), _, _, _) -> no_return().
system_terminate(Reason, _, _, {_, State}) ->
  _ = lager:info("sytem terminate ~p~n", [State]),
  terminate(Reason, State).

system_code_change(Misc, _, _, _) ->
  {ok, Misc}.

-spec terminate(_, _) -> no_return().
terminate(Reason, State) ->
  barrel_statistics:record_tick(num_replications_stopped, 1),
  #st{
    id = RepId,
    metrics = Metrics,
    checkpoint = Checkpoint
  } = State,
  _ = lager:debug( "~s (~p} terminated: ~p", [?MODULE_STRING, RepId, Reason]),
  _ = barrel_replicate_metrics:update_task(Metrics),
  %% try to write the checkpoint if we can
  (catch barrel_replicate_checkpoint:write_checkpoint(Checkpoint)),
  erlang:exit(Reason).