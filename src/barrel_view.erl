-module(barrel_view).
-behaviour(gen_server).

-export([start_link/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         terminate/2]).


process_name(BarrelId, ViewId) ->
  list_to_atom(barrel_lib:to_list(?MODULE) ++
               barrel_lib:to_list(BarrelId) ++ "_" ++
               barrel_lib:to_list(ViewId )).


start_link(#{barrel := Barrel,  view := View} = Conf) ->
  Name = process_name(Barrel, View),
  gen_sever:start_link({local, Name}, ?MODULE, Conf, []).

init(Conf) ->
  process_flag(trap_exit, true),
  io:format("start ~s conf=~p~n", [?MODULE_STRING, Conf]),
  {ok, Conf}.


handle_call(_Msg, _From, State) ->
  {reply, ok, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

terminate(_Reason, State) ->
  io:format("termuinate ~s conf=~p~n", [?MODULE_STRING, State]),
  ok.

