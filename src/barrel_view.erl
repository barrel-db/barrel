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

await_refresh(Barrel, View) ->
  await_refresh(Barrel, View, barrel_config:get(fold_timeout)).

await_refresh(Barrel, View, Timeout) ->
  random_sleep(5),
  {ok, Ref} = gen_server:call(process_name(Barrel, View), {await_refresh, self()}),
  receive
    {Ref, view_refresh} ->
      ok
  after Timeout ->
          exit(refresh_timeout)
  end.

random_sleep(Times) ->
    _ = case Times rem 10 of
	    0 ->
		_ = rand:seed(exsplus);
	    _ ->
		ok
	end,
    %% First time 1/4 seconds, then doubling each time up to 8 seconds max.
    Tmax = if Times > 5 -> 8000;
	      true -> ((1 bsl Times) * 1000) div 8
	   end,
    T = rand:uniform(Tmax),
    receive after T -> ok end.

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

handle_call({await_refresh, Pid}, _From,
            #{ waiters := Waiters } = State) ->
  Ref = erlang:make_ref(),
  {reply, {ok, Ref}, State#{ waiters => [{Pid, Ref} | Waiters] }};

handle_call(_Msg, _From, State) ->
  {reply, bad_call, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(view_refresh, #{waiters := Waiters} = State) ->
  _ = notify(Waiters, view_refresh),
  {noreply, State#{ waiters => [] }};

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.


notify(Waiters, Msg) ->
  lists:foreach(fun({Pid, Ref}) ->
                    case erlang:is_process_alive(Pid) of
                      true ->
                        Pid ! {Ref, Msg};
                      false ->
                        ok
                    end
                end, Waiters).

process_name(BarrelId, ViewId) ->
  list_to_atom(?MODULE_STRING ++
               barrel_lib:to_list(BarrelId) ++ "_" ++
               barrel_lib:to_list(ViewId )).

