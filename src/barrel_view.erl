-module(barrel_view).

-export([start_link/1]).
-export([await_refresh/2, await_refresh/3]).
-export([get_range/3]).
-export([stop_kvs_stream/1]).

-export([init/1,
         callback_mode/0,
         terminate/3]).

-export([idle/3,
         indexing/3]).

%-export([handle_event/4]).

-include("barrel.hrl").
-include("barrel_view.hrl").

get_range(Barrel, View, Options) ->
  supervisor:start_child(barrel_fold_process_sup,
                         [{fold_view, Barrel, View, self(), Options}]).

stop_kvs_stream(Pid) ->
  supervisor:terminate_child(barrel_fold_process_sup, Pid).

await_refresh(BarrelId, ViewId) ->
  Seq = barrel_db:last_updated_seq(BarrelId),
  gen_statem:call(?view_proc(BarrelId, ViewId), {refresh_index, Seq}).

await_refresh(BarrelId, ViewId, Seq) ->
  gen_statem:call(?view_proc(BarrelId, ViewId), {refresh_index, Seq}).


start_link(#{ barrel := BarrelId, view_id := ViewId } = Conf) ->
  gen_statem:start_link(?view_proc(BarrelId, ViewId), ?MODULE, Conf, []).

init(#{barrel := BarrelId,
       view_id := ViewId,
       version := Version,
       mod := ViewMod,
       config := ViewConfig0 }) ->

  process_flag(trap_exit, true),
  {ok, ViewConfig1} = ViewMod:init(ViewConfig0),

  {ok, #{ ref := Ref, uid := UID }} = barrel_db:open_barrel(BarrelId),
  case ?STORE:open_view(Ref, ViewId, Version) of
    {ok, ViewRef, LastIndexedSeq0, _OldVersion} ->
      LastIndexedSeq = maybe_migrate(LastIndexedSeq0, UID),

      View = #view{barrel=BarrelId,
                   ref=ViewRef,
                   mod=ViewMod,
                   config=ViewConfig1},

      Data = #{ barrel => BarrelId,
                 view => View,
                 since => LastIndexedSeq,
                 writer => undefined,
                 postponed => false,
                 waiters => []},

      _ = barrel_event:reg(BarrelId),

      case maybe_update(Data) of
        true ->
          Writer = barrel_view_updater:run(#{ barrel => BarrelId,
                                              view => View,
                                              since => LastIndexedSeq }),

          {ok, indexing, Data#{ writer => Writer }};
        false ->
          %%_ = barrel_event:reg_once(BarrelId),
          {ok, idle, Data}
      end;
    Error ->
      {stop, Error}
  end.

callback_mode() -> state_functions.


terminate(Reason, _State, #{ barrel := Barrel,
                             writer := Writer,
                             waiters := Waiters }) ->
  _ = barrel_event:unreg(Barrel),
  notify_all(Waiters, Reason),

  MRef = erlang:monitor(process, Writer),
  try
    _ = (catch unlink(Writer)),
    _ = (catch exit(Writer, shutdown)),
    receive
      {'DOWN', MRef, _, _, _} ->
        ok
    end
  after
    erlang:demonitor(MRef, [flush])
  end.

idle({call, From}, {refresh_index, Last}, #{ barrel := Barrel,
                                             view := View,
                                             since := Since,
                                             waiters := Waiters } = Data) ->
  if
    Last =< Since ->
      {keep_state, Data, [{reply, From, {ok, Since} }]};
    true ->
      %_ = barrel_event:unreg(Barrel),
      NewWriter = barrel_view_updater:run(#{ barrel => Barrel,
                                             view => View,
                                             since => Since }),
      Waiters2 = [{From, Last} | Waiters],
      {next_state, indexing, Data#{ writer => NewWriter,
                                    waiters => Waiters2  }}
  end;

idle(info, {'$barrel_event', Barrel, db_updated},
     #{ barrel := Barrel,
        view := View,
        since := Since} = Data) ->

  case maybe_update(Data) of
    true ->
      NewWriter = barrel_view_updater:run(#{ barrel => Barrel,
                                             view => View,
                                             since => Since }),

      {next_state, indexing, Data#{ writer => NewWriter}};
    false ->
      {keep_state, Data}
  end;

idle(EventType, Msg, Data) ->
  handle_event(EventType, idle, Msg, Data).


indexing({call, From}, {refresh_index, Last},
         #{ since := Since, waiters := Waiters } = Data)  ->

  if
    Last =< Since ->
      {keep_state, Data, [{reply, From, {ok, Since}}]};
    true ->
      Waiters2 = [{From, Last} | Waiters],
      {keep_state, Data#{ waiters => Waiters2 }}
  end;

indexing(info, {index_updated, IndexedSeq}, #{ waiters := Waiters } = Data) ->

  Waiters2 = lists:filter(fun({W, WSeq}) when WSeq =< IndexedSeq ->
                              gen_statem:reply(W, {ok, IndexedSeq}),
                              false;
                             (_) ->
                              true
                          end, Waiters),
  {keep_state, Data#{ since => IndexedSeq,
                      waiters => Waiters2}};


indexing(info, {'EXIT', Pid, {index_updated, IndexedSeq}},
         #{ writer := Pid,
            barrel := Barrel,
            view := View,
            waiters := Waiters0 } = Data) ->

  Waiters1 = lists:filter(fun({W, WSeq}) when WSeq =< IndexedSeq ->
                              gen_statem:reply(W, {ok, IndexedSeq}),
                              false;
                             (_) ->
                              true
                          end, Waiters0),
  Data2 = Data#{ since => IndexedSeq,
                 waiters => Waiters1,
                 writer => undefined,
                 postponed => false },
  case Waiters1 of
    [] ->
      %%_ = barrel_event:reg_once(Barrel),
      {next_state, idle, Data2};
    _ ->
      %% still have some updates
      NewWriter = barrel_view_updater:run(#{ barrel => Barrel,
                                             view => View,
                                             since => IndexedSeq }),
      {keep_state, Data2#{ writer => NewWriter }}
  end;

indexing(info, {'$barrel_event', _, db_updated}, #{ postponed := true } = Data) ->
  {keep_state, Data};

indexing(info, {'$barrel_event', _, db_updated}, Data) ->
  {keep_state, Data#{ postponed => true }, [postpone]};


indexing(EventType, Msg, Data) ->
  handle_event(EventType, indexing, Msg, Data).


handle_event(info, StateType, {'EXIT', Pid, Reason}, #{ writer := Pid }) ->
  ?LOG_ERROR("exit error: state=~p error=~p~n", [StateType, Reason]),
  {stop, Reason};

handle_event(Event, State, Msg, Data) ->
  ?LOG_INFO("received unknown message: event=~p, state=~p, msg=~p data=~p~n",
             [Event, State, Msg, Data]),
  {keep_state, Data}.

notify_all(Waiters, Msg) ->
  _ = [gen_statem:reply(W, Msg) || W <- Waiters],
  ok.



maybe_update(#{ barrel := BarrelId, since := IndexedSeq }) ->
  UpdatedSeq =  barrel_db:last_updated_seq(BarrelId),
  (IndexedSeq < UpdatedSeq).


maybe_migrate({_Epoch, _Seq}=T, UID) ->
  SeqBin = barrel_sequence:encode(T),
  barrel_sequence:to_string(UID, SeqBin);
maybe_migrate(S, _UID) ->
  S.
