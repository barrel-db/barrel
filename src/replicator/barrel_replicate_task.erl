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
  wait_for_resume/2,
  wait_for_source/3,
  wait_for_target/3,
  wait_registration/1,
  wait_for_changes/1
]).

%% internal api
-export([
  replication_key/1,
  stream_worker/3,
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
  , keepalive=false
  , status=active
  , config
}).


-type state() :: #st{}.

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
  _ = [barrel_node_checker:monitor_node(Node) || Node <- nodes_to_watch(Config)],
  ok;
register_if_keepalive(_) ->
  ok.

unregister_if_keepalive(#st{keepalive=true, config=Config}) ->
  _ = [barrel_node_checker:demonitor_node(Node) || Node <- nodes_to_watch(Config)],
  ok;
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
  wait_registration(State).


wait_registration(State) ->
  receive
    Event ->
      handle_event(Event, wait_for_source, State)
    after 0 ->
      wait_for_source(State)
  end.

wait_for_source(#st{ source = Db, keepalive = KeepAlive, id=Id } = State) ->
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
          alarm_handler:set_alarm({{barrel_replication_task, Id}, {source_not_found, Db}}),
          B = backoff:init(200, 20000, self(), ping),
          B2 = backoff:type(B, jitter),
          TRef = backoff:fire(B2),
          wait_for_source(State, TRef, B2);
        false ->
          erlang:error({source_error, {Db, Error}})
      end
  end.

wait_for_source(#st{ id=Id, source = RemoteDb } = State, TRef, B ) ->
  receive
    {timeout, TRef, ping} ->
      case barrel_replicate_api_wrapper:database_infos(RemoteDb) of
        {ok, _} ->
          alarm_handler:clear_alarm({barrel_replication_task, Id}),
          wait_for_target(State);
        {bad_rpc, nodedown} ->
          alarm_handler:clear_alarm({barrel_replication_task, Id}),
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

wait_for_target(#st{ id=Id, target = Db, keepalive = KeepAlive } = State) ->
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
          alarm_handler:set_alarm({{barrel_replication_task, Id}, {target_not_found, Db}}),
          B = backoff:init(200, 20000, self(), ping),
          B2 = backoff:type(B, jitter),
          TRef = backoff:fire(B2),
          wait_for_target(State, TRef, B2);
        false ->
          erlang:error({source_error, {Db, Error}})
      end
  end.

wait_for_target(#st{ id=Id, target = {_, _} = RemoteDb } = State, TRef, B ) ->
  receive
    {timeout, TRef, ping} ->
      case barrel_replicate_api_wrapper:database_infos(RemoteDb) of
        {ok, _} ->
          alarm_handler:clear_alarm({barrel_replication_task, Id}),
          wait_for_changes(State#{ status = active });
        {bad_rpc, nodedown} ->
          alarm_handler:clear_alarm({barrel_replication_task, Id}),
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

loop_changes(State = #st{id=Id, stream=Stream, parent=Parent, status=Status}) ->
  receive
    {change, Stream, Change} ->
      try handle_change(Change, State)
      catch
        _:Reason ->
          _ = lager:error(
            "~s: error while processing the change ~p, replication stopped: ~p .~n",
            [?MODULE_STRING, Change, Reason]
          ),
          _ = stop_stream_worker(Stream),
          _ = timer:sleep(1000),
          wait_for_source(State#st{ stream = nil})
      end;
    {nodedown, _Node}=Ev when Status =:= paused ->
      _ = lager:warning(
        "replication ~p (~p) received unexpected ~p event on state ~p~n.",
        [Id, Status, Ev, loop_changes]
      ),
      proc_lib:hibernate(?MODULE, loop_changes, [State]);
    {nodedown, _Node} ->
      alarm_handler:set_alarm({{barrel_replication_task, Id}, paused}),
      ok = stop_stream_worker(Stream),
      proc_lib:hibernate(?MODULE, loop_changes, [State#st{stream=nil, status=paused}]);
    {nodeup, _Node}=Ev when Status =:= active ->
      _ = lager:warning(
        "replication ~p (~p) received unexpected ~p event on state ~p~n.",
        [Id, Status, Ev, loop_changes]
      ),
      loop_changes(State);
    {nodeup, _Node} ->
      alarm_handler:clear_alarm({barrel_replication_task, Id}),
      #st{ source = Source, last_seq = LastSeq} = State,
      Stream = spawn_stream_worker(Source, LastSeq),
      loop_changes(State#st{ stream = Stream, status=active });
    stop ->
      cleanup_and_exit(State, normal);
    {get_state, From} ->
      ok = handle_get_state(From, State),
      loop_changes(State);
    {'EXIT', Stream, Reason} ->
      barrel_statistics:record_tick(num_replications_errors, 1),
      handle_stream_exit(Reason, State);
    {'EXIT', Parent, Reason} ->
      terminate(Reason, State);
    {system, From, Request} ->
      sys:handle_system_msg(
        Request, From, Parent, ?MODULE, [],
        {loop_changes, State});
    Other ->
      _ = lager:error("~s: got unexpected message: ~p~n", [?MODULE_STRING, Other]),
      cleanup_and_exit(State, {unexpected_message, Other})
  end.

handle_event(Event, StateFun, #st{id=Id, parent=Parent, status=Status}=State) ->
  case Event of
    {nodedown, _Node}=Ev when Status =:= paused ->
      _ = lager:warning(
        "replication ~p (~p) received ~p event on state ~p~n.",
        [Id, Status, Ev, StateFun]
      ),
      proc_lib:hibernate(?MODULE, wait_for_resume, [StateFun, State]);
    {nodedown, _Node} ->
      alarm_handler:set_alarm({{barrel_replication_task, Id}, paused}),
      proc_lib:hibernate(?MODULE, wait_for_resume, [StateFun, State#st{stream=nil, status=paused}]);
    {nodeup, _Node}=Ev when Status =:= active ->
      _ = lager:warning(
        "resumed replication ~p (~p) received ~p event. next state: ~p~n.",
        [Id, Status, Ev, StateFun]
      ),
      erlang:apply(?MODULE, StateFun, [State]);
    {nodeup, _Node} ->
      alarm_handler:clear_alarm({barrel_replication_task, Id}),
      erlang:apply(?MODULE, StateFun, [State#st{ status = active }]);
    stop ->
      cleanup_and_exit(State, normal);
    {get_state, From} ->
      ok = handle_get_state(From, State),
      erlang:apply(?MODULE, StateFun, [State]);
    {'EXIT', Parent, Reason} ->
      terminate(Reason, State);
    {system, From, Request} ->
      sys:handle_system_msg(
        Request, From, Parent, ?MODULE, [],
        {StateFun, State});
    Other ->
      _ = lager:error("~s: got unexpected message: ~p~n", [?MODULE_STRING, Other]),
      cleanup_and_exit(State, {unexpected_message, Other})
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
  loop_changes(
    State#st{checkpoint=NewCheckpoint, metrics=NewMetrics, last_seq=LastSeq}
  ).

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
handle_stream_exit(Reason, #st{ id = RepId, keepalive = KeepAlive } = State) ->
  _ = lager:info(
    "~s, ~p change stream exited:~p~n~p~n~n",
    [?MODULE_STRING, RepId, Reason, State]
  ),
  case KeepAlive of
    true ->
      wait_for_source(State#st{stream=nil});
    false ->
      barrel_statistics:record_tick(num_replications_errors, 1),
      cleanup_and_exit(State, Reason)
  end.

system_continue(_, _, {StateFun, State}) ->
  erlang:apply(?MODULE, StateFun, [State]).

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
  cleanup_and_exit(State, Reason).

-spec cleanup_and_exit(state(), any()) -> no_return().
cleanup_and_exit(State, Reason) ->
  _ = unregister_if_keepalive(State),
  alarm_handler:clear_alarm({barrel_replication_task, State#st.id}),
  erlang:exit(Reason).