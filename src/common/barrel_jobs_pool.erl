%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 15. Jan 2018 16:22
%%%-------------------------------------------------------------------

%% TODO: add regulator support, use a supervisor for idle workers?

-module(barrel_jobs_pool).
-author("benoitc").
-behaviour(gen_server).


%% API
-export([
  start_link/0,
  run/2,
  get_workers/0
]).

%% gen_server callbacks
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2
]).

-dialyzer({nowarn_function, start_worker/0}).
-dialyzer({nowarn_function, init_workers/1}).


-include("barrel.hrl").

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

run(Task, TaskFrom) ->
  case sbroker:ask(?jobs_broker) of
    {go, _Ref, WorkerPid, _RelativeTime, _SojournTime} ->
      barrel_job_worker:handle_work(WorkerPid, TaskFrom, Task),
      ok;
    {drop, _N} ->
      {error, dropped}
  end.

get_workers() ->
  gen_server:call(?jobs_pool, get_workers).

init([]) ->
  erlang:process_flag(trap_exit, true),
  JobsIdleMinLimit = application:get_env(barrel, jobs_idle_min_limit, ?JOBS_IDLE_MIN_LIMIT),
  Workers = init_workers(JobsIdleMinLimit),
  {ok, #{ workers => Workers, idle_min => JobsIdleMinLimit }}.

handle_call(get_workers, _From, #{ workers := Workers } = State) ->
  {reply, Workers, State};

handle_call(Msg, _From, State) ->
  _ = lager:debug("db pool received a synchronous event: ~p~n", [Msg]),
  {reply, ok, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info({_, {go, _, _, _, _}}, State) ->
  {noreply, State};
handle_info({Worker, {drop, _}}, State) ->
  {await, _, _} =  sbroker:async_ask_r(?jobs_broker, Worker, {self(), Worker}),
  {noreply, State};
handle_info({'EXIT', Pid, _Reason}, #{ workers := Workers0 } = State) ->
  Workers1 = Workers0 -- [Pid],
  sbroker:dirty_cancel(?jobs_broker, Pid),
  Worker = start_worker(),
  {noreply, State#{ workers => [Worker | Workers1 ]}};
handle_info(_Info, State) ->
  {noreply, State}.

start_worker() ->
  {ok, Worker} = barrel_job_worker:start_link(),
  {await, _, _} =  sbroker:async_ask_r(?jobs_broker, Worker, {self(), Worker}),
  Worker.

init_workers(Min) ->
  lists:foldl(
    fun(_, Acc) ->
      {ok, Pid} = barrel_job_worker:start_link(),
      {await, _, _} =  sbroker:async_ask_r(?jobs_broker, Pid, {self(), Pid}),
      [Pid | Acc]
    end,
    [],
    lists:seq(1, Min)
  ).