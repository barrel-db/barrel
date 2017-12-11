%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. Dec 2017 00:12
%%%-------------------------------------------------------------------
-module(barrel_replicate_monitor).
-author("benoitc").

%% API
-export([
  start_link/0,
  register/2,
  unregister/1
]).

-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2
]).

-export([watcher_loop/2]).

-define(TAB, replicator_by_nodes).
-define(NODES, replicated_nodes).
-define(SERVER, ?MODULE).

-include_lib("stdlib/include/ms_transform.hrl").

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

register(Pid, Nodes) ->
  gen_server:call(?SERVER, {register, Pid, Nodes}).

unregister(Pid) ->
  gen_server:call(?SERVER, {unregister, Pid}).


%% -----------------------------------
%% gen_server callbacks

init([]) ->
  erlang:process_flag(trap_exit, true),
  ok = net_kernel:monitor_nodes(true),
  _ = ets:new(?TAB, [ordered_set, named_table]),
  _ = ets:new(?NODES, [ordered_set, named_table]),
  {ok, #{ tasks => #{}, watchers => #{} } }.

handle_call({register, Pid, Nodes}, _From, State) ->
  NewState = do_register(Pid, Nodes, State),
  {reply, ok, NewState};

handle_call({unregister, Pid}, _From, State) ->
  NewState = do_unregister(Pid, State),
  {reply, ok, NewState};
handle_call(_Msg, _From, State) ->
  {reply, bad_call, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info({restart_replicators, Node}, State) ->
  ok = restart_replicators(Node),
  {noreply, State};
handle_info({node_up, Node}, State) ->
  case is_connected(Node) of
    true ->
      _ = lager:info("~s, replica ~p is up", [?MODULE_STRING, Node]),
      {noreply, State};
    false ->
      {noreply, State}
  end;

handle_info({node_down, Node}, State) ->
  case is_connected(Node) of
    true ->
      NewState = replica_is_down(Node, State),
      {noreply, NewState};
    false ->
      {noreply, State}
  end;

handle_info({'EXIT', Pid, _Reason}, State) ->
  NewState = watcher_exited(Pid, State),
  {noreply, NewState};

handle_info(_Info, State) ->
  {noreply, State}.


%% -----------------------------------
%% internal helpers

do_register(Pid, Nodes, State) ->
  _ = ets:insert(?TAB, {Pid, Nodes, false}),
  check_connections(Nodes, Pid, false, State).

check_connections([Node | Rest], Pid, Paused, State) ->
  ets:insert(?TAB, {{Node, Pid}, Pid}),
  _ = inc_node_refcount(Node),
  case net_adm:ping(Node) of
    pong ->
      check_connections(Rest, Pid, Paused, State);
    pang ->
      #{ tasks := Tasks, watchers := Watchers} = State,
      NewState = case maps:find(Node, Tasks) of
                   {ok, Watcher} ->
                     Watcher ! work,
                     State;
                   error ->
                     Watcher = spawn_watcher(Node),
                     Watcher ! work,
                     Tasks2 = Tasks#{ Node => Watcher },
                     Watchers2 = Watchers#{ Watcher => Watchers },
                     State#{ tasks => Tasks2, watchers => Watchers2 }
                 end,
      check_connections(Rest, Pid, maybe_pause(Node, Paused), NewState)
  end;
check_connections([], _Pid, _Paused, State) ->
  State.

maybe_pause(Pid, false) ->
  Pid ! pause,
  true = ets:update_element(?TAB, Pid, {3, true}),
  true;
maybe_pause(_Pid, true) ->
  true.

do_unregister(Pid, State) ->
  case ets:take(?TAB, Pid) of
    [] -> State;
    [{Pid, Nodes, _}] ->
      _ = [begin
             _  = ets:delete(?TAB, {Node, Pid}),
             dec_node_refcount(Node)
           end || Node <- Nodes],
      ok = stop_watchers(Nodes, State),
      State
  end.

replica_is_down(Node, State = #{ tasks := Tasks, watchers := Watchers}) ->
  ok = pause_replicators(Node),
  case maps:find(Node, Tasks) of
    {ok, Watcher} ->
      Watcher ! work,
      State;
    error ->
      Watcher = spawn_watcher(Node),
      Watcher ! work,
      Tasks2 = Tasks#{ Node => Watcher },
      Watchers2 = Watchers#{ Watcher => Watchers },
      State#{ tasks => Tasks2, watchers => Watchers2 }
  end.

watcher_exited(Watcher, State) ->
  #{ tasks := Tasks, watchers := Watchers0 } = State,
  case maps:take(Watcher, Watchers0) of
    {Node, Watchers1} ->
      NewWatcher = spawn_watcher(Node),
      NewWatcher ! work,
      Tasks2 = Tasks#{ Node => NewWatcher },
      Watchers2 = Watchers1#{ NewWatcher => Node },
      State#{ tasks => Tasks2, watchers => Watchers2 };
    error ->
      State
  end.

stop_watchers([Node | Rest], State) ->
  #{ tasks := Tasks, watchers := Watchers} = State,
  case is_connected(Node) of
    true ->
      stop_watchers(Rest, State);
    false ->
      {Watcher, Tasks2} = maps:take(Node, Tasks),
      ok = stop_watcher(Watcher),
      Watchers2 = maps:remove(Watcher, Watchers),
      stop_watchers(Rest, State#{ tasks => Tasks2, watchers => Watchers2 })
  end;
stop_watchers([], State) ->
  State.

inc_node_refcount(Node) ->
  try ets:update_counter(?NODES, Node, {2, 1})
  catch
    error:badarg ->
      ets:insert(?NODES, {Node, 1}),
      1
  end.

dec_node_refcount(Node) ->
  try ets:update_counter(?NODES, Node, {2, -1})
  catch
    error:badarg ->
      ets:insert(?NODES, {Node, 0}),
      0
  end.

is_connected(Node) ->
  (ets:update_counter(?NODES, Node, 0) > 0).

restart_replicators(Node) ->
  Replicators = replicators(Node),
  lists:foreach(
    fun(RepPid) ->
      case ets:lookup_element(?TAB, RepPid, 3) of
        true ->
          RepPid ! resume,
          ets:update_element(?TAB, RepPid, {3, false});
        false ->
          ok
      end
    end,
    Replicators
  ),
  ok.

pause_replicators(Node) ->
  Replicators = replicators(Node),
  lists:foreach(
    fun(RepPid) ->
      case ets:lookup_element(?TAB, RepPid, 3) of
        false ->
          RepPid ! pause,
          ets:update_element(?TAB, RepPid, {3, true});
        true ->
          ok
      end
    end,
    Replicators
  ),
  ok.

replicators(Node) ->
  MS = ets:fun2ms(
    fun({{N, _}, P}) when N =:= Node -> P end
  ),
  ets:select(?TAB, MS).

spawn_watcher(Node) ->
  Server = self(),
  spawn_link(?MODULE, watcher_loop, [Server, Node]).

stop_watcher(Watcher) ->
  unlink(Watcher),
  MRef = erlang:monitor(process, Watcher),
  Watcher ! stop,
  receive
    {'DOWN', MRef, process, Watcher, _Reason} ->
      ok
  end.

watcher_loop(Server, Node) ->
  receive
    work ->
      ping_loop(Server, Node);
    stop ->
      exit(normal)
  end.


ping_loop(Server, Node) ->
  case net_adm:ping(Node) of
    pong ->
      Server ! {restart_replicators, Node},
      watcher_loop(Server, Node);
    pang ->
      B = backoff:init(200, 20000, self(), ping),
      B2 = backoff:type(B, jitter),
      TRef = backoff:fire(B2),
      _ = lager:info(
        "~s: retry connection to ~p in ~p on ~p.~n",
        [?MODULE_STRING, Node, backoff:get(B2), node()]
      ),
      ping_loop(Server, Node, TRef, B2)
  end.

ping_loop(Server, Node, TRef, B) ->
  receive
    {timeout, TRef, ping} ->
      case net_adm:ping(Node) of
        pong ->
          Server ! {restart_replicators, Node},
          watcher_loop(Server, Node);
        pang ->
          {_, B2} = backoff:fail(B),
          NewTRef = backoff:fire(B2),
          _ = lager:info(
            "~s: retry connection to ~p in ~p on ~p.~n",
            [?MODULE_STRING, Node, backoff:get(B2), node()]
          ),
          ping_loop(Server, Node, NewTRef, B2)
      end;
    stop ->
      exit(normal)
  end.