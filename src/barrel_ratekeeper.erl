-module(barrel_ratekeeper).
-behaviour(gen_server).

-include("barrel.hrl").

-export([start_link/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2]).



start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


init([]) ->
  erlang:process_flag(trap_exit, true),
  UpdateRate = barrel_config:get(metrics_update_rate),
  TRef = erlang:send_after(UpdateRate, self(), update_rate),

  TransactionsPerBytes = barrel_config:get(transactions_per_bytes),

  ok = jobs:add_queue(barrel_write_queue,
                      [{standard_rate, TransactionsPerBytes}]),

  {ok, #{ tref => TRef,
          update_rate => UpdateRate }}.

handle_call(_Msg, _From, State) ->
  {reply, bad_call, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(update_rate, #{ update_rate := UpdateRate } = State) ->
  TRef = erlang:send_after(UpdateRate, self(), update_rate),
  _Memory = barrel_memory:get_mem_usage(),


  %io:format("update_rate memory=~p~n", [Memory]),

  {noreply, State#{ tref := TRef }};

handle_info(Info, State) ->
  ?LOG_DEBUG("~s got unknown message=~p~n",
             [?MODULE_STRING, Info]),
  {noreply, State}.


terminate(_Reason, #{ tref := TRef }) ->
  _ = erlang:cancel_timer(TRef),
  ok = jobs:delete_queue(barrel_write_queue),
  ok.


