-module(barrel_changes_stream).


-export([await/1, await/2,
         ack/2]).

-export([start_link/3]).

-export([init/1,
         callback_mode/0,
         terminate/3,
         handle_event/4]).

-export([push/3,
         wait_pending/3]).

-include("barrel.hrl").


-define(CHANGES_INTERVAL, 100).
-define(BATCH_SIZE, 100).
-define(BATCHES_ON_FLY, 5).

await(StreamPid) ->
 await(StreamPid, infinity).

await(StreamPid, Timeout) ->
  MRef = erlang:monitor(process, StreamPid),
  receive
    {changes, ReqId, Changes} ->
      erlang:demonitor(MRef, [flush]),
      {ReqId, Changes};
    {'DOWN', MRef, process, _, Reason} ->
      ?LOG_INFO("stream down reason=~p~n", [Reason]),
      []
  after
    Timeout ->
      erlang:demonitor(MRef, [flush]),
      erlang:exit(timeout)
  end.

ack(StreamPid, ReqId) ->
  gen_statem:cast(StreamPid, {ack, ReqId}).


start_link(Name, Owner, Options) ->
  gen_statem:start_link(?MODULE, [Name, Owner, Options], []).


init([Name, Owner, Options]) ->
  {ok, Barrel} = barrel_db:open_barrel(Name),
  Since = maps:get(since, Options, first),
  Interval = maps:get(interval, Options, ?CHANGES_INTERVAL),
  BatchSize = maps:get(batch_size, Options, ?BATCH_SIZE),

  InitState = #{ barrel => Barrel,
                 since => Since,
                 interval => Interval,
                 batch_size => BatchSize,
                 owner => Owner,
                 pending => [] },

  {ok, push, InitState, [{next_event, info, send_changes}]}.

callback_mode() -> state_functions.

terminate(_Reason, _StateType, _State) ->
  ok.


push(info, send_changes, #{ barrel := Barrel,
                            since := Since,
                            interval := Interval,
                            owner := Owner,
                            batch_size := BatchSize,
                            pending := Pending } = State) ->
  Cb = fun(Change, Acc) ->
           Acc1 = [ Change | Acc],
           case (length(Acc1) =:= BatchSize) of
             true ->
               {stop, lists:reverse(Acc1)};
             false ->
               {ok, Acc1}
           end
       end,
  {ok, Changes, LastSeq} =
    barrel:fold_changes(Barrel, Since, Cb, [], #{}),
  ReqId = erlang:make_ref(),
  Owner ! {changes, ReqId, Changes},

  Pending2 = [ReqId | Pending],
  NewState =  State#{ since => LastSeq,
                      pending => Pending2 },
  case (length(Pending2) < ?BATCHES_ON_FLY) of
    true ->
      erlang:send_after(Interval, self(), send_changes),
      {keep_state, NewState};
    false ->
      {next_state, wait_pending, NewState}
  end.

wait_pending(cast, {ack, ReqId}, #{ pending := Pending } = State) ->
  case (Pending -- [ReqId]) of
    [] ->
      {next_state, push, State#{ pending => [] },
       [{next_event, info, send_changes}]};
    Pending ->
      ?LOG_WARNING("~s, received unknown pending request id=~p", [?MODULE_STRING, ReqId]),
      {keep_state, State};
    Pending2 ->
      {keep_state, State#{ pending => Pending2 }}
  end.

handle_event(EventType, StateType, Content, State) ->
  ?LOG_WARNING(
    "~s, unknown event type=~p, state type=~p, content=~p~n",
     [?MODULE_STRING, EventType, StateType, Content]
  ),
  {keep_state, State}.
