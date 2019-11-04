
%% TODO: should be a supervisor

-module(barrel_changes_streams).
-behaviour(gen_server).

-export([subscr/3,
         unsubscr/1]).

-export([start_link/0]).

-export([init/1,
         handle_cast/2,
         handle_call/3,
         handle_info/2]).


-include_lib("barrel/include/barrel.hrl").

-define(STREAMS, ?MODULE).

subscr(Node, Name, Options) ->
  erlang:send({?MODULE, Node}, cast_msg({subscribe_changes, self(), Name, Options})),
  receive
    {subscribed, Name, StreamPid} ->
      {ok, StreamPid}
  after 5000 ->
          erlang:exit(5000)
  end.


unsubscr(StreamPid) ->
  Node = node(StreamPid),
  erlang:send({?MODULE, Node}, cast_msg({unsubscribe, StreamPid})).



cast_msg(Msg) -> {'$gen_cast', Msg}.

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
  process_flag(trap_exit, true),
  {ok, #{ pids => #{},
          subs => #{}  }}.


handle_cast({subscribe_changes, From, Name, Options}, #{subs := Subs }  = St) ->
  case maps:find({From, Name}, Subs) of
    {ok, #{  stream := StreamPid}} ->
      From ! {already_subscribed, Name, StreamPid},
      {noreply, St};
    error ->
      NewSt = handle_subscription(From, Name, Options, St),
      {noreply, NewSt}
  end;


handle_cast({unsub, StreamPid}, #{ pids := Pids,
                                   subs := Subs }  = St) ->
  case maps:find(StreamPid, Pids) of
    {#{ sub := Sub }, Pids2} ->
      Subs2 = maps:remove(Sub, Subs),
      {noreply, St#{ subs => Subs2,
                     pids => Pids2 }};
    error ->
      {noreply, St}
  end.

handle_call(_Msg, _From, St) -> {reply, ok, St}.

handle_info({'EXIT', Pid, Reason}, #{ pids := Pids,
                                      subs := Subs }  = St) ->
  case maps:take(Pid, Pids) of
    {#{ sub := {From, Name} = Sub }, Pids2} ->
      ?LOG_INFO("change stream closed db=~p reason=~p~n", [Name, Reason]),
      Subs2 = maps:remove(Sub, Subs),
      From ! {change_stream, Name, closed},
      {noreply, St#{ pids => Pids2,
                     subs => Subs2 }};
    error ->
      {noreply, St}
  end.




handle_subscription(From, Name, Options, #{ pids := Pids,
                                            subs := Subs } = St) ->
  Sub = {From, Name},
  case barrel_changes_stream:start_link(Name, From, Options) of
    {ok, StreamPid} ->
      From ! {subscribed, Name, StreamPid},
      St#{ pids => Pids#{ StreamPid => # { sub => Sub,
                                           name => Name }},
           subs => Subs#{ Sub => #{ stream => StreamPid,
                                    name => Name } }
         };
    Error ->
      From ! Error,
      St
  end.
