%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 13. Dec 2017 10:44
%%%-------------------------------------------------------------------
-module(barrel_node_checker).
-author("benoitc").

%% API
-export([
  monitor_node/1,
  demonitor_node/1,
  start_link/0
]).

-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2
]).


-include_lib("barrel/include/barrel_logger.hrl").
-include_lib("stdlib/include/ms_transform.hrl").


-define(TAB, barrel_checked_nodes).

-export([worker/1]).


monitor_node(Node) ->
  gen_server:call(?MODULE, {monitor_node, Node, self()}).


demonitor_node(Node) ->
  gen_server:call(?MODULE, {demonitor_node, Node, self()}).

start_link() ->
  IsNew = init_tab(),
  gen_server:start_link({local, ?MODULE}, ?MODULE, [IsNew], []).

init_tab() ->
  case ets:info(?TAB, info) of
    undefined ->
      _ = ets:new(?TAB, [ordered_set, named_table, public]),
      true;
    _ ->
      false
  end.


init([IsNew]) ->
  erlang:process_flag(trap_exit, true),
  ok = net_kernel:monitor_nodes(true),
  InitState = #{ nodes => #{}, workers => #{}},
  ok = remonitor(IsNew),
  ok = remonitor_nodes(IsNew),
  {ok, InitState}.

remonitor(true) -> ok;
remonitor(false) ->
  MS = ets:fun2ms(fun({Pid, m}) -> Pid end),
  _ = [erlang:monitor(process, Pid) || Pid <- ets:select(?TAB, MS)],
  ok.
  
remonitor_nodes(true) -> ok;
remonitor_nodes(false) ->
  MS = ets:fun2ms(
    fun({Node, Counter}) when is_atom(Node), is_integer(Counter) -> Node end
  ),
  _ = [erlang:monitor_node(Node, true) || Node <- ets:select(?TAB, MS)],
  ok.

handle_call({monitor_node, Node, Pid}, _From, State) ->
  _ = ets:insert(?TAB, {{Node, Pid}, []}),
  _ = inc_ref_counter(Node),
  _ = maybe_monitor(Pid),
  NewState = maybe_start_checker(Node, State),
  {reply, ok, NewState};
handle_call({demonitor_node, Node, Pid}, _From, State) ->
  _ = ets:delete(?TAB, {Node, Pid}),
  NewState = case dec_ref_counter(Node) of
               0 -> maybe_stop_checker(Node, State);
               _ -> State
             end,
  {reply, ok, NewState};

handle_call(_Msg, _From, State) ->
  {noreply, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info({nodeup, Node}, State) ->
  ok = notify(Node, nodeup),
  NewState = maybe_stop_checker(Node, State),
  {noreply, NewState};
handle_info({nodedown, Node}, State) ->
  ok = notify(Node, nodedown),
  NewState = maybe_start_checker(Node, State),
  {noreply, NewState};
handle_info({'DOWN', _MRef, process, Pid, _Reason}, State) ->
  NewState = process_is_down(Pid, State),
  {noreply, NewState};
handle_info({'EXIT', Pid, Reason}, State) ->
  ?LOG_ERROR(
    "~s: worker ~p, exited with reason ~p~n",
    [?MODULE_STRING, Pid, Reason]
  ),
  NewState = worker_exited(Pid, State),
  {noreply, NewState};
handle_info(_Info, State) ->
  {noreply, State}.

terminate(_State, _Reason) ->
  _ = ets:delete(?TAB),
  ok.


maybe_start_checker(Node, State = #{ nodes := Nodes, workers := Workers}) ->
  case net_adm:ping(Node) of
    pong -> State;
    pang ->
      case maps:find(Node, Nodes) of
        {ok, _} -> State;
        error ->
          Pid = spawn_link(?MODULE, worker, [Node]),
          Nodes2 = Nodes#{ Node => Pid },
          Workers2 = Workers#{ Pid => Node },
          State#{ nodes => Nodes2, workers => Workers2 }
      end
  end.
  
maybe_stop_checker(Node, State = #{ nodes := Nodes, workers := Workers}) ->
  case maps:take(Node, Nodes) of
    {Worker, Nodes2} ->
      ok = stop_worker(Worker),
      Workers2 = maps:remove(Worker, Workers),
      State#{ nodes => Nodes2, workers => Workers2};
    error ->
      State
  end.

stop_worker(Worker) ->
  unlink(Worker),
  MRef = erlang:monitor(process, Worker),
  Worker ! stop,
  receive
    {'DOWN', MRef, process, Worker, _Reason} ->
      ok
  end.

worker_exited(Worker, State = #{ nodes := Nodes, workers := Workers}) ->
  case maps:take(Worker, Workers) of
    {Node, Workers2} ->
      Pid = spawn_link(?MODULE, worker, [Node]),
      Nodes2 = Nodes#{ Node => Pid },
      Workers2 = Workers#{ Pid => Node },
      State#{ nodes => Nodes2, workers => Workers2 };
    error ->
      %% should never happen
      ?LOG_WARNING(
        "~s: unknown worker ~p exited.~n",
        [?MODULE_STRING, Worker]
      ),
      State
  end.

worker(Node) ->
  _ = net_adm:ping(Node),
  B = backoff:init(200, 20000, self(), ping),
  B2 = backoff:type(B, jitter),
  TRef = backoff:fire(B2),
  worker_loop(Node, TRef, B2).
  
worker_loop(Node, TRef, B) ->
  receive
    {timeout, TRef, ping} ->
      _ = net_adm:ping(Node),
      {_, B2} = backoff:fail(B),
      NewTRef = backoff:fire(B2),
      worker_loop(Node, NewTRef, B2);
    stop ->
      exit(normal)
  end.

inc_ref_counter(Node) ->
  try ets:update_counter(?TAB, Node, {2, 1})
  catch
    error:badarg ->
      true = ets:insert(?TAB, {Node, 1}),
      1
  end.

dec_ref_counter(Node) ->
  try ets:update_counter(?TAB, Node, {2, -1})
  catch
    error:badarg ->
      0
  end.

notify(Node, Event) ->
  MS = ets:fun2ms(
    fun({{N, P}, []}) when N =:= Node -> P end
  ),
  _ = [erlang:send(Pid, {Node, Event}) || Pid <- ets:select(?TAB, MS)],
  ok.

maybe_monitor(Pid) ->
  case ets:insert_new(?TAB, {Pid, m}) of
    true -> ok;
    false ->
      erlang:monitor(process, Pid)
  end.

process_is_down(Pid, State) ->
  case ets:take(?TAB, Pid) of
    [] -> ok;
    [_] ->
      MS = ets:fun2ms(
        fun({N, P}) when P =:= Pid -> {N, P} end
      ),
      lists:foldl(
        fun({Node, _Pid}=NodeKey, State1) ->
          _ = ets:delete(?TAB, NodeKey),
          case dec_ref_counter(Node) of
            0 -> maybe_stop_checker(Node, State1);
            _ -> State1
          end
        end,
        State,
        ets:select(?TAB, MS)
      )
  end.