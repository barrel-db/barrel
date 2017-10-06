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

%% gen_server API
-export([
  init/2,
  loop/1
]).

%% internal api
-export([
  replication_key/1,
  stream_worker/3
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

init(Parent, Config) ->
  process_flag(trap_exit, true),
  proc_lib:init_ack(Parent, {ok, self()}),

  barrel_statistics:record_tick(num_replications_started, 1),

  #{ id := RepId,
     source := Source0,
     target := Target0 } = Config,

  Options = maps:get(options, Config, #{}),

  %% source and target should be connected before we start a replication
  Source = barrel_replicate_api_wrapper:setup_channel(Source0),
  Target = barrel_replicate_api_wrapper:setup_channel(Target0),

  Config1 = Config#{ source => Source, target => Target },

  %% init_metrics
  Metrics = barrel_replicate_metrics:new(),
  ok = barrel_replicate_metrics:create_task(Metrics, Options),
  barrel_replicate_metrics:update_task(Metrics),
  %% initialize the changes feed
  Checkpoint = barrel_replicate_checkpoint:new(Config1),
  _ = lager:info("checkpoint is ~p~n", [Checkpoint]),
  StartSeq = barrel_replicate_checkpoint:get_start_seq(Checkpoint),
  Stream = spawn_stream_worker(Source, StartSeq),
  %% start loop
  State = #st{ id = RepId,
               source = Source,
               target = Target,
               parent = Parent,
               checkpoint=Checkpoint,
               stream = Stream,
               metrics = Metrics },
  loop(State).

spawn_stream_worker(Source, StartSeq) ->
  spawn_link(?MODULE, stream_worker, [self(), Source, StartSeq]).

stream_worker(Parent, Source, StartSeq) ->
  Stream = barrel_replicate_api_wrapper:subscribe_changes(Source, StartSeq, #{}),
  stream_loop(Parent, Source, Stream).

stream_loop(Parent, Source, Stream) ->
  case  barrel_replicate_api_wrapper:await_change(Source, Stream, infinity) of
    Change when is_map(Change) ->
      #{ <<"seq">> := Seq } = Change,
      _ = put(last_seq, Seq),
      Parent ! {change, self(), Change},
      stream_loop(Parent, Source, Stream);
    {end_stream, Error, LastSeq} ->
      exit({Error, LastSeq});
    Else ->
      exit({Else, get(last_seq)})
  end.

loop(State = #st{parent=Parent, stream=Stream}) ->
  receive
    {change, Stream, Change} ->
      NewState = handle_change(Change, State),
      loop(NewState);
    {get_state, From} ->
      _ = handle_get_state(From, State),
      loop(State);
    stop ->
      exit(normal);
    {'EXIT', Stream, Reason} ->
      barrel_statistics:record_tick(num_replications_errors, 1),
      NewState = handle_stream_exit(Reason, State),
      loop(NewState);
    {'EXIT', Parent, Reason} ->
      terminate(Reason, State);
    {system, From, Request} ->
      sys:handle_system_msg(
        Request, From, Parent, ?MODULE, [],
        {loop, State});
    Other ->
      _ = lager:error("~s: got unexpected message: ~p~n", [?MODULE_STRING, Other]),
      exit({unexpected_message, Other})
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
  State#st{ checkpoint=NewCheckpoint, metrics=NewMetrics }.

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
  exit(normal);
handle_stream_exit({normal, _}, _State) ->
  exit(normal);
handle_stream_exit(normal, _State) ->
  exit(normal);
handle_stream_exit({Reason, _LastSeq}, _State) ->
  %% we do not try to recover the changes stream. It should normally not hang.
  %% So let the task supervisor recover it eventually.
  exit({changes_stream_exit, Reason}).


system_continue(_, _, {loop, State}) ->
  loop(State).

-spec system_terminate(any(), _, _, _) -> no_return().
system_terminate(Reason, _, _, {loop, State}) ->
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
