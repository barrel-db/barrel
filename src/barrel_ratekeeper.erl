-module(barrel_ratekeeper).
-behaviour(gen_server).

-include("barrel.hrl").

-export([start_link/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2]).


-export([export/2]).

export(Measures, _Config) ->
  process_measures(Measures).

process_measures([#{ name := 'barrel/ratekeeper/get_latency' } = Measure | _Rest]) ->
  io:format("~n~n========~n~n get latency=~p~n~n", [Measure]),
  set_latency(Measure),

  ok;
process_measures([_, Rest]) ->
  process_measures(Rest);

process_measures([#{ name := 'barrel/ratekeeper/get_latency'} = Measure]) ->
  io:format("~n~n========~n~n get latency=~p~n~n", [Measure]),
  set_latency(Measure),
  ok;

process_measures(_Else) ->
  io:format("~n~n========~n~n else=~p~n~n", [_Else]),

  ok.


-define(MAX_LATENCY, 200000).
set_latency(#{ data := #{ rows := [#{ value := #{ mean := Mean } }] } }) ->
  Ratio = Mean / 100000,

  Latency = if
              Ratio =< 0.05 -> 0;
              Ratio =< 0.5 -> 0.5;
              Ratio =< 1 -> 1;
              true ->
                1.5
            end,
  counters:put(persistent_term:get({barrel_rk, rate}), 1, Latency);
set_latency(_) ->
  ok.



start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


init([]) ->
  erlang:process_flag(trap_exit, true),
  UpdateRate = barrel_config:get(metrics_update_rate),
  TRef = erlang:send_after(UpdateRate, self(), update_rate),

  TransactionsPerBytes = barrel_config:get(transactions_per_bytes),

  ok = jobs:add_queue(barrel_write_queue,
                      [{standard_rate, TransactionsPerBytes}]),


  oc_stat_view:subscribe(#{ name => 'barrel/ratekeeper/get_latency',
                            description => "storage get latency view for reatekeeper",
                            measure => 'barrel/storage/get_latency',
                            aggregation => {oc_stat_aggregation_distribution,
                                            [{buckets, [0, 100, 300, 650, 800, 1000]}]}}),


  oc_stat_exporter:register(?MODULE, #{}),

  Ref = counters:new(1, []),
  ok = persistent_term:put({barrel_rk, rate}, Ref),



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
  _ = jobs:delete_queue(barrel_write_queue),
  _ = persistent_term:erase({barrel_rk, rate}),
  ok.


