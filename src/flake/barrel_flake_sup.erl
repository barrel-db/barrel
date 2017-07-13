%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 13. Jul 2017 12:23
%%%-------------------------------------------------------------------
-module(barrel_flake_sup).
-behaviour(supervisor).

%% API
-export([start_link/0]).


-export([init/1]).

-define(ALLOWABLE_DOWNTIME, 2592000000).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->
  AllowableDowntime = application:get_env(barrel, ts_allowable_downtime, ?ALLOWABLE_DOWNTIME),
  Now = barrel_flake:curr_time_millis(),
  {ok, LastTs} = barrel_flake_ts:read_timestamp(),
  _ = lager:info("last ts is ~p~n", [LastTs]),
  TimeSinceLastRun = Now - LastTs,
  
  _ = lager:info("now: ~p, last: ~p, delta: ~p~n", [Now, LastTs, TimeSinceLastRun]),
  
  %% restart if we detected a clock change
  ok = check_for_clock_error(Now >= LastTs, TimeSinceLastRun < AllowableDowntime),
  
  PersistServer = #{
    id => persist_time_server,
    start => {barrel_flake_ts, start_link, []},
    restart => permanent,
    shutdown => 5000,
    type => worker,
    modules => [barrel_flake_ts]
  },
  
  FlakeServer = #{
    id => server,
    start => {barrel_flake, start_link, []},
    restart => permanent,
    shutdown => 5000,
    type => worker,
    modules => [barrel_flake]
  },
  
  SupFlags = #{ strategy => one_for_one, intensity => 10, period => 10},
  {ok, {SupFlags, [PersistServer, FlakeServer]}}.


check_for_clock_error(true, true) -> ok;
check_for_clock_error(false, _) ->
  _ = lager:error(
    "~s: system running backwards, failing startup of flake service~n",
    [?MODULE_STRING]
  ),
  exit(clock_running_backwards);
check_for_clock_error(_, false) ->
  _ = lager:error(
    "~s: system clock too far advanced, failing startup of snowflake service~n",
    [?MODULE_STRING]
  ),
  exit(clock_advanced).