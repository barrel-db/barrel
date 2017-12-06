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
-export([start_link/0]).

-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2
]).

-export([watcher_loop/2]).

-define(TAB, replicator_by_nodes).
-define(NODES, replicated_nodes).

-include_lib("stdlib/include/ms_transform.hrl").

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
  erlang:process_flag(trap_exit, true),
  ok = net_kernel:monitor_nodes(true),
  _ = ets:new(?TAB, [ordered_set, named_table]),
  _ = ets:new(?NODES, [ordered_set, named_table]),
  {ok, #{ tasks => #{}, watchers => #{} } }.

handle_call({register, Pid, Nodes}, _From, State ) ->
  _ = [ets:insert(?TAB, {{Node, Pid}, Pid}) || Node <- Nodes],
  MRef = erlang:monitor(process, Pid),
  _ = ets:insert(?TAB, {Pid, {MRef, Nodes}}),
  ok = monitor_nodes(Nodes),
  {reply, ok, State};

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
  case is_monitored(Node) of
    true ->
      _ = lager:info("~s, replica ~p is up", [?MODULE_STRING, Node]),
      {noreply, State};
    false ->
      {noreply, State}
  end;

handle_info({node_down, Node}, State) ->
  case is_monitored(Node) of
    true ->
      NewState = replica_is_down(Node, State),
      {noreply, NewState};
    false ->
      {noreply, State}
  end;

handle_info({'DOWN', _MRef, process, Pid, _Reason}, State) ->
  NewState = replicator_is_down(Pid, State),
  {noreply, NewState};

handle_info({'EXIT', Pid, _Reason}, State) ->
  NewState = watcher_exited(Pid, State),
  {noreply, NewState};

handle_info(_Info, State) ->
  {noreply, State}.

do_unregister(Pid, State) ->
  case ets:take(?TAB, Pid) of
    [] -> State;
    [{Pid, {MRef, Nodes}}] ->
      _ = [ets:delete(?TAB, {Node, Pid}) || Node <- Nodes],
      _ = (catch erlang:demonitor(MRef, [flush])),
      ok = demonitor_nodes(Nodes),
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

replicator_is_down(Pid, State) ->
  do_unregister(Pid, State).

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
  case is_monitored(Node) of
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

monitor_nodes([Node | Rest]) ->
  case ets:insert_new(?NODES, {Node, 1}) of
    true ->
      monitor_nodes(Rest);
    false ->
      _ = ets:update_counter(?NODES, Node, {2, 1}),
      monitor_nodes(Rest)
  end;
monitor_nodes([]) ->
  ok.

demonitor_nodes([Node | Rest]) ->
  _ = ets:update_counter(?NODES, Node, {2, -1}),
  demonitor_nodes(Rest);
demonitor_nodes([]) ->
  ok.

is_monitored(Node) ->
  (ets:update_counter(?NODES, Node, 0) > 0).

restart_replicators(Node) ->
  Replicators = replicators(Node),
  _ =  [begin
          RepPid ! resume
        end || RepPid <- Replicators],
  ok.

pause_replicators(Node) ->
  Replicators = replicators(Node),
  _ = [begin
         RepPid ! pause
       end || RepPid <- Replicators],
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
  