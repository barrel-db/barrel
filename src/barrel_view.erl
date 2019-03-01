-module(barrel_view).
-behaviour(gen_server).


-export([get_range/3,
         await_kvs/1,
         stop_kvs_stream/1
        ]).

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

get_range(Barrel, View, Options) ->
  supervisor:start_child(barrel_fold_process_sup,
                         [{fold_view, Barrel, View, self(), Options}]).

await_kvs(StreamRef) ->
  Timeout = barrel_config:get(fold_timeout),
  receive
    {StreamRef, {ok, Row}} ->
      {ok, Row};
    {StreamRef, done} ->
      OldTrapExit = erlang:erase(old_trap_exit),
      process_flag(trap_exit, OldTrapExit),
      done
  after Timeout ->
          erlang:exit(fold_timeout)
  end.

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

stop_kvs_stream(Pid) ->
  supervisor:terminate_child(barrel_fold_process_sup, Pid).

start_link(#{barrel := Barrel,  view := View} = Conf) ->
  Name = process_name(Barrel, View),
  gen_server:start_link({local, Name}, ?MODULE, Conf, []).

init(Conf) ->
  process_flag(trap_exit, true),
  {ok, Conf#{ waiters => [] }}.

handle_call({get_range, To, Options}, _From, #{ barrel := Barrel, view := View } = State) ->
  {ok, Pid} =
    supervisor:start_child(barrel_fold_process_sup,
                           [{fold_view, Barrel, View, To, Options}]),
  {reply, {ok, Pid}, State};


handle_call({await_refresh, Pid, Seq}, _From,
            #{ waiters := Waiters, indexed_seq := IndexedSeq } = State) ->

  if
    IndexedSeq >= Seq ->
      {reply, ok, State};
    true ->
      Ref = erlang:make_ref(),
      {reply, {wait, Ref}, State#{ waiters => [{Pid, Ref, Seq} | Waiters] }}
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
  {noreply, State#{ waiters => Waiters2, indexed_seq => Seq }};

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.


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

