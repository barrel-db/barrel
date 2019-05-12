-module(barrel_view).
-behaviour(gen_server).


-export([fold/5]).
-export([await_refresh/2,
         await_refresh/3]).
-export([update/3]).

-export([start_link/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2]).

-include("barrel.hrl").

fold(Barrel, View, UserFun, UserAcc, Options) ->
  {ok, #{ ref := Ref }} = barrel:open_barrel(Barrel),
  FoldFun = fun({DocId, Key, Value}, Acc) ->
                Row = #{ key => Key,
                         value => Value,
                         id => DocId },
                UserFun(Row, Acc)
            end,

  ?STORE:fold_view_index(Ref, View, FoldFun, UserAcc, Options).

await_refresh(Barrel, View) ->
  await_refresh(Barrel, View, barrel_config:get(fold_timeout)).

await_refresh(Barrel, View, Timeout) ->
  {ok, #{updated_seq := Seq}} = ?STORE:barrel_infos(Barrel),
  case gen_server:call(process_name(Barrel, View),
                       {await_refresh, self(), Seq}) of
    ok ->
      ok;
    {wait, Ref} ->
      receive
        {Ref, view_refresh} ->
          ok
      after Timeout ->
              exit(refresh_timeout)
      end
  end.

update(Barrel, View, Msg) ->
  ViewRef = process_name(Barrel, View),
  erlang:send(ViewRef, Msg, []).


start_link(#{barrel := Barrel,  view := View} = Conf) ->
  Name = process_name(Barrel, View),
  gen_server:start_link({local, Name}, ?MODULE, Conf, []).

init(Conf) ->
  process_flag(trap_exit, true),
  {ok, Adapter} = barrel_view_adapter:start_link(Conf),
  ocp:record('barrel/views/active_num', 1),
  ?LOG_INFO("view started conf=~p~n", [Conf]),
  {ok, Conf#{ waiters => [], adapter => Adapter, wait_refresh => false, conf => Conf }}.

handle_call({get_range, To, Options}, _From, #{ barrel := Barrel, view := View } = State) ->
  {ok, Pid} =
    supervisor:start_child(barrel_fold_process_sup,
                           [{fold_view, Barrel, View, To, Options}]),
  {reply, {ok, Pid}, State};


handle_call({await_refresh, Pid, Seq}, _From,
            #{ waiters := Waiters,
               indexed_seq := IndexedSeq } = State) ->

  if
    IndexedSeq >= Seq ->
      {reply, ok, State};
    true ->
      Ref = erlang:make_ref(),
      {reply, {wait, Ref}, maybe_refresh(State#{ waiters => [{Pid, Ref, Seq} | Waiters] })}
  end;

handle_call({await_refresh, Pid, Seq}, _From,
            #{ waiters := Waiters } = State) ->
  Ref = erlang:make_ref(),
  {reply, {wait, Ref}, State#{ waiters => [{Pid, Ref, Seq} | Waiters] }};

handle_call(_Msg, _From, State) ->
  {reply, bad_call, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info({view_refresh, Seq}, #{waiters := Waiters} = State) ->
  Waiters2 = notify(Waiters, Seq),
  {noreply, State#{ waiters => Waiters2, indexed_seq => Seq, wait_refresh => false }};

handle_info({'EXIT', Reason, Pid}, #{ adapter := Pid, conf := Conf } = State) ->
  ?LOG_INFO("view adapter exited reason=~p conf=~p~n", [Reason, Conf]),
  {ok, Adapter} = barrel_view_adapter:start_link(Conf),
  {noreply, State#{ adapter => Adapter }};

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, #{ adapter := Adapter }) ->
  ocp:record('barrel/views/active_num', -1),
  ok = barrel_view_adapter:stop(Adapter),
  ok.

maybe_refresh(#{ wait_refresh := true } = State) -> State;
maybe_refresh(#{ adapter := Adapter } = State) ->
  Adapter ! refresh_view,
  State#{ wait_refresh => true }.


notify(Waiters, IndexedSeq) ->
  lists:foldl(fun
                ({Pid, Ref, Seq}, Acc) when Seq =< IndexedSeq ->
                  case erlang:is_process_alive(Pid) of
                    true ->
                      Pid ! {Ref, view_refresh};
                    false ->
                      ok
                  end,
                  Acc;
                (Waiter, Acc) ->
                  [Waiter | Acc]
              end,
              [], lists:reverse(Waiters)).

process_name(BarrelId, ViewId) ->
  list_to_atom(?MODULE_STRING ++
               barrel_lib:to_list(BarrelId) ++ "_" ++
               barrel_lib:to_list(ViewId )).

