%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 14. Jun 2018 00:14
%%%-------------------------------------------------------------------
-module(barrel_server).
-author("benoitc").
-behaviour(gen_server).


%% API
-export([start_link/1]).

-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2
]).


-export([do_run/3]).

-record(job, {
  client,
  worker,
  client_pid,
  worker_pid
}).

-record(state, {
  clients = #{},
  workers = #{}
}).

-include("barrel_rpc.hrl").

start_link(ServerId) ->
  gen_server:start_link({local, ServerId}, ?MODULE, [], []).

init([]) ->
  {ok, #state{}}.


handle_call(_Msg, _From, State) -> {reply, ignore, State}.

handle_cast({run, From, MFA}, State) ->
  handle_cast({run, From, undefined, MFA}, State);
handle_cast({run, {ClientPid, ClientRef} = From, Nonce, MFA}, State) ->
  {WorkerPid, WorkerRef} = spawn_monitor(?MODULE, do_run, [From, MFA, Nonce]),
  Job = #job{client=ClientRef,
             worker=WorkerRef,
             client_pid=ClientPid,
             worker_pid=WorkerPid},
  {noreply, add_job(Job, State)};
handle_cast({kill, FromRef}, State) ->
  case find_job(FromRef, State#state.clients) of
    {ok, #job{worker = Worker, worker_pid = WorkerPid} =Job} ->
      erlang:demonitor(Worker, [flush]),
      exit(WorkerPid, kill),
      {noreply, remove_job(Job, State)};
    error ->
      {noreply, State}
  end;
handle_cast(Msg, State) ->
  _ = lager:warning("unknown cast msg=~p~n", [Msg]),
  {noreply, State}.

handle_info({'DOWN', Worker, process, _, normal}, State) ->
  case find_job(Worker, State#state.workers) of
    {ok, Job} ->
      {noreply, remove_job(Job, State)};
    error ->
      {noreply, State}
  end;
handle_info({'DOWN', Worker, process, _, Error}, State) ->
  case find_job(Worker, State#state.workers) of
    {ok, Job} ->
      #job{client=C, client_pid=CPid} = Job,
      case Error of
        #rpc_error{reason = {_Class, Reason}, stack = Stack} ->
          notify_exit({CPid, C}, {Reason, Stack}),
          {noreply, remove_job(Job, State)};
        _ ->
          notify_exit({CPid, C}, Error),
          {noreply, remove_job(Job, State)}
      end;
    error ->
      {noreply, State}
  end;
handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, State) ->
  _ = maps:fold(
    fun(_, #job{worker_pid=WPid}, _) -> exit(WPid, kill), nil end,
    nil,
    State#state.workers
  ),
  ok.

do_run(From, {M, F, A}, Nonce) ->
  put(barrel_rpc_from, From),
  put(barrel_rpc_nonce, Nonce),
  put('$initial_call', {M,F,length(A)}),
  try apply(M, F, A)
  catch
    exit:normal -> ok;
    Class:Reason ->
      Stack = clean_stack(),
      exit(#rpc_error{
        timestamp = erlang:timestamp(),
        reason = {Class, Reason},
        mfa = {M,F,A},
        nonce = Nonce,
        stack = Stack
      })
  end.

clean_stack() ->
  lists:map(
    fun({M,F,A}) when is_list(A) -> {M,F,length(A)}; (X) -> X end,
    erlang:get_stacktrace()
  ).

add_job(Job, State) ->
  #state{clients=Clients, workers=Workers} = State,
  #job{client=Client, worker=Worker} = Job,
  State#state{
    clients = maps:put(Client, Job, Clients),
    workers = maps:put(Worker, Job, Workers)
  }.

remove_job(Job, State) ->
  #state{clients=Clients, workers=Workers} = State,
  State#state{
    clients = maps:remove(Job#job.client, Clients),
    workers = maps:remove(Job#job.worker, Workers)
  }.

find_job(Ref, Map) ->
  maps:find(Ref, Map).

notify_exit({Pid, Ref}, Reason) ->
  barrel_rpc_util:send(Pid, {Ref, {'barrel_rpc_EXIT', Reason}}).