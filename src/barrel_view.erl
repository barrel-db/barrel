-module(barrel_view).

-export([start_link/1]).
-export([await_refresh/2]).
-export([get_range/3]).
-export([stop_kvs_stream/1]).

-export([init/1,
         callback_mode/0,
         terminate/3]).

-export([idle/3,
         indexing/3]).

-export([index_worker/0]).


%-export([handle_event/4]).

-include("barrel.hrl").
-include("barrel_view.hrl").


get_range(Barrel, View, Options) ->
  supervisor:start_child(barrel_fold_process_sup,
                         [{fold_view, Barrel, View, self(), Options}]).

stop_kvs_stream(Pid) ->
  supervisor:terminate_child(barrel_fold_process_sup, Pid).

await_refresh(BarrelId, ViewId) ->
  gen_statem:call(?view_proc(BarrelId, ViewId), refresh_index).


start_link(#{ barrel := BarrelId, view_id := ViewId } = Conf) ->
  gen_statem:start_link(?view_proc(BarrelId, ViewId), ?MODULE, Conf, []).

init(#{barrel := BarrelId,
       view_id := ViewId,
       version := Version,
       mod := ViewMod,
       config := ViewConfig0 }) ->

  process_flag(trap_exit, true),
  {ok, ViewConfig1} = ViewMod:init(ViewConfig0),
  _ = barrel_event:reg(BarrelId),

  case ?STORE:open_view(BarrelId, ViewId, Version) of
    {ok, ViewRef, LastIndexedSeq, _OldVersion} ->
      View = #view{barrel=BarrelId,
                   ref=ViewRef,
                   mod=ViewMod,
                   config=ViewConfig1},
      {ok, Writer} = gen_batch_server:start_link(barrel_view_writer, View),
      {ok, idle, #{ barrel => BarrelId,
                    view => View,
                    since => LastIndexedSeq,
                    writer => Writer,
                    waiters_map => #{}}};
    Error ->
      {stop, Error}
  end.

callback_mode() -> state_functions.

terminate(_Reason, _State, #{ writer := Writer }) ->
  _ = barrel_event:unreg(),
  gen_batch_server:stop(Writer),
  ok.


idle({call, From}, refresh_index, Data) ->
  {keep_state, Data, [{reply, From, ok}]};

idle(info, {'$barrel_event', _, db_updated}, Data) ->
  NewData = fold_changes(Data),
  {next_state, indexing, NewData};

idle(EventType, Msg, Data) ->
  handle_event(EventType, idle, Msg, Data).


indexing({call, From}, refresh_index, #{ since := Since,
                                         waiters_map := WaitersMap } = Data) ->
  Waiters = maps:get(Since, WaitersMap),
  {keep_state, Data#{ waiters_map => WaitersMap#{ Since => [From | Waiters] } }};

indexing(info, {'$barrel_event', BarrelId, db_updated}, #{ barrel := BarrelId } = Data) ->
  NewData = fold_changes(Data),
  {keep_state, NewData};


indexing(info, {index_updated, Seq}, #{ waiters_map := WaitersMap0 } = Data) ->
  WaitersMap2 = case maps:take(Seq, WaitersMap0) of
                  {Waiters, WaitersMap1} ->
                    _ = [gen_statem:reply(W, ok) || W <- Waiters],
                    WaitersMap1;
                  error ->
                    WaitersMap0
                end,
  Data2 = Data#{ waiters_map =>WaitersMap2},
  case maps:size(WaitersMap2) of
    0 ->
      {next_state, idle, Data2};
    _ ->
      {keep_state, Data2}
  end;

indexing(EventType, Msg, Data) ->
  handle_event(EventType, indexing, Msg, Data).

handle_event(info, _StateType, {'EXIT', Pid, Reason},
             #{ writer := Pid,
                waiters_map := WaitersMap,
                view := View } = State) ->
  ?LOG_ERROR("view writer error=~p~n", [Reason]),
  _ = notify_all(WaitersMap, write_error),
  flush_updates(),
  NewWriter = barrel_view_writer:start_link(View),
  {next_state, idle, State#{ writer => NewWriter, waiters_map := #{} }};

handle_event(Event, State, Msg, Data) ->
  ?LOG_INFO("received unknown message: event=~p, state=~p, msg=~p~n",
             [Event, State, Msg]),
  {keep_state, Data}.




fold_changes(#{ barrel := BarrelId, since := Since, writer := Writer,
                waiters_map := WaitersMap } = Data) ->
  FoldFun = fun(Change, Acc) -> {ok, [{change, Change} | Acc]} end,
  {ok, Barrel} = barrel:open_barrel(BarrelId),
  {ok, Changes, LastSeq} = barrel_db:fold_changes(
                             Barrel, Since, FoldFun, [], #{ include_doc => true }
                            ),


  if
    LastSeq /= Since ->
      Batch = lists:reverse([{done, LastSeq, self()} | Changes]),
      ok = gen_batch_server:cast_batch(Writer, Batch),

      Data#{ since => LastSeq,
             waiters_map => WaitersMap#{ LastSeq => [] }};
    true ->
      Data
  end.

notify_all(WaitersMap, Msg) ->
  maps:fold(fun(_Seq, Waiters, _) ->
                     _ = [gen_statem:reply(W, Msg) || W <- Waiters],
                     ok
                end, ok, WaitersMap).

flush_updates() ->
  receive
    {index_updated, _} ->
      flush_updates()
  after 0 ->
          ok
  end.


index_worker() ->
  [{_Ts, Job}] = jobs:dequeue(barrel_index_queue, 1),
   {JobRef, Doc, View, ViewPid} = Job,

  Res = (catch handle_doc(Doc, View)),
  io:format(user, "ref=~p result=~p~n", [JobRef, Res]),
  ViewPid ! {JobRef, Res}.

handle_doc( #{ <<"id">> := DocId } = Doc,
             #view{mod=Mod, config=Config, ref=ViewRef} ) ->
  KVs = Mod:handle_doc(Doc, Config),
  ?STORE:update_view_index(ViewRef, DocId, KVs).


