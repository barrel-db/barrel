-module(barrel_view).
-behaviour(gen_server).


-export([get_range/3,
         await_kvs/1,
         stop_kvs_stream/1
        ]).


-export([start_link/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         terminate/2]).


get_range(Barrel, View, Options) ->
  OldExit = process_flag(trap_exit, true),
  erlang:put(old_trap_exit, OldExit),
  gen_server:call(process_name(Barrel, View), {get_range, self(), Options}).

await_kvs(StreamRef) ->
  Timeout = barrel_config:get(fold_timeout),
  receive
    {StreamRef, {ok, Row}} ->
      {ok, Row};
    {StreamRef, done} ->
      OldTrapExit = erlang:erase(old_trap_exit),
      process_flag(trap_exit, OldTrapExit),
      done;
    {'EXIT', _, {fold_timeout, StreamRef}} ->
      erlang:exit(fold_timeout);
    {'EXIT', _, Reason} ->
      erlang:error(Reason)
  after Timeout ->
          erlang:exit(fold_timeout)
  end.

stop_kvs_stream(Pid) ->
  supervisor:terminate_child(barrel_fold_process_sup, Pid).

start_link(#{barrel := Barrel,  view := View} = Conf) ->
  Name = process_name(Barrel, View),
  gen_server:start_link({local, Name}, ?MODULE, Conf, []).

init(Conf) ->
  process_flag(trap_exit, true),
  io:format("start ~s conf=~p~n", [?MODULE_STRING, Conf]),
  {ok, Conf}.

handle_call({get_range, To, Options}, _From, #{ barrel := Barrel, view := View } = State) ->
  {ok, Pid} =
    supervisor:start_child(barrel_fold_process_sup,
                           [{fold_view, Barrel, View, To, Options}]),
  {reply, {ok, Pid}, State};

handle_call(_Msg, _From, State) ->
  {reply, bad_call, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

terminate(_Reason, State) ->
  io:format("termuinate ~s conf=~p~n", [?MODULE_STRING, State]),
  ok.




process_name(BarrelId, ViewId) ->
  list_to_atom(?MODULE_STRING ++
               barrel_lib:to_list(BarrelId) ++ "_" ++
               barrel_lib:to_list(ViewId )).

