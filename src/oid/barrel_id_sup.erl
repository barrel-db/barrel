%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 13. Jul 2017 12:23
%%%-------------------------------------------------------------------
-module(barrel_id_sup).
-behaviour(supervisor).

%% API
-export([start_link/0]).


-export([init/1]).

-define(ALLOWABLE_DOWNTIME, 2592000000).

start_link() ->
  Config = [{worker_id, get_worker_id()}],
  ok = barrel_lib:load_config(barrel_ts_config, Config),
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->
  AllowableDowntime = application:get_env(barrel, ts_allowable_downtime, ?ALLOWABLE_DOWNTIME),
  {ok, LastTs} = barrel_ts:read_timestamp(),
  Now = barrel_ts:curr_time_millis(),
  TimeSinceLastRun = Now - LastTs,

  _ = lager:debug(
    "timestamp: now: ~p, last: ~p, delta: ~p~n, allowable_downtime: ~p",
    [Now, LastTs, TimeSinceLastRun, AllowableDowntime]
  ),

  %% restart if we detected a clock change
  ok = check_for_clock_error(Now >= LastTs, TimeSinceLastRun < AllowableDowntime),

  PersistTimeServer = #{
    id => persist_time_server,
    start => {barrel_ts, start_link, [LastTs]},
    restart => permanent,
    shutdown => 5000,
    type => worker,
    modules => [barrel_flake_ts]
  },

  SupFlags = #{ strategy => one_for_one, intensity => 10, period => 10},
  {ok, {SupFlags, [PersistTimeServer]}}.


check_for_clock_error(true, true) -> ok;
check_for_clock_error(false, _) ->
  _ = lager:error(
    "~s: system running backwards, failing startup of time service~n",
    [?MODULE_STRING]
  ),
  exit(clock_running_backwards);
check_for_clock_error(_, false) ->
  _ = lager:error(
    "~s: system clock too far advanced, failing startup of time service~n",
    [?MODULE_STRING]
  ),
  exit(clock_advanced).


%% ols implementation just based on hw address and node name
%%worker_id() ->
%%  If = application:get_env(barrel, ts_inteface, get_default_if()),
%%  {ok, Hw} = get_if_hw(If),
%%  % 48 bits for the first Hardware address found
%%  % with the distributed Erlang node name
%%  << WorkerId:48/integer, _/binary >> =
%%    crypto:hash(sha, erlang:list_to_binary(Hw ++ erlang:atom_to_list(node()))),
%%  WorkerId.


%% worker id is based on the UUID library from okeuday
%% https://github.com/okeuday/uuid/
get_worker_id() ->
  If = application:get_env(barrel, ts_interface, get_default_if()),
  {ok, Hw} = get_if_hw(If),
  <<NodeD01, NodeD02, NodeD03, NodeD04, NodeD05,
    NodeD06, NodeD07, NodeD08, NodeD09, NodeD10,
    NodeD11, NodeD12, NodeD13, NodeD14, NodeD15,
    NodeD16, NodeD17, NodeD18, NodeD19, NodeD20>> =
    crypto:hash(sha, erlang:list_to_binary(Hw ++ erlang:atom_to_list(node()))),
  % later, when the pid format changes, handle the different format
  ExternalTermFormatVersion = 131,
  PidExtType = 103,
  <<ExternalTermFormatVersion:8,
    PidExtType:8,
    PidBin/binary>> = erlang:term_to_binary(self()),
  % 72 bits for the Erlang pid
  <<PidID1:8, PidID2:8, PidID3:8, PidID4:8, % ID (Node specific, 15 bits)
    PidSR1:8, PidSR2:8, PidSR3:8, PidSR4:8, % Serial (extra uniqueness)
    PidCR1:8                       % Node Creation Count
  >> = binary:part(PidBin, erlang:byte_size(PidBin), -9),
  % reduce the 160 bit NodeData checksum to 16 bits
  NodeByte1 = ((((((((NodeD01 bxor NodeD02)
    bxor NodeD03)
    bxor NodeD04)
    bxor NodeD05)
    bxor NodeD06)
    bxor NodeD07)
    bxor NodeD08)
    bxor NodeD09)
    bxor NodeD10,
  NodeByte2 = (((((((((NodeD11 bxor NodeD12)
    bxor NodeD13)
    bxor NodeD14)
    bxor NodeD15)
    bxor NodeD16)
    bxor NodeD17)
    bxor NodeD18)
    bxor NodeD19)
    bxor NodeD20)
    bxor PidCR1,
  % reduce the Erlang pid to 32 bits
  PidByte1 = PidID1 bxor PidSR4,
  PidByte2 = PidID2 bxor PidSR3,
  PidByte3 = PidID3 bxor PidSR2,
  PidByte4 = PidID4 bxor PidSR1,
  %% 48 bits worker id
  << WorkerId:48/integer >> =
    << NodeByte1:8, NodeByte2:8,  PidByte1:8,
       PidByte2:8,  PidByte3:8, PidByte4:8 >>,
  WorkerId.


%% ==============================
%% internals helpers

%% get a reasonable default interface that has a valid mac address
get_default_if() ->
  {ok, SysIfs} = inet:getifaddrs(),
  Ifs = [I || {I, Props} <- SysIfs, filter_if(Props)],
  hd(Ifs).

% filter network interfaces
filter_if(Props) ->
  HwAddr = proplists:get_value(hwaddr, Props),
  filter_hwaddr(HwAddr).

% we exclude interfaces without a MAC address
filter_hwaddr(undefined) ->
  false;
% we exclude interfaces with a null MAC address, ex: loopback devices
filter_hwaddr([0,0,0,0,0,0]) ->
  false;
% all others are valid interfaces to pick from
filter_hwaddr(_) ->
  true.

%% get the mac/hardware address of the given interface
get_if_hw(undefined) ->
  {error, if_not_found};
get_if_hw(IfName) ->
  {ok, IfAddrs} = inet:getifaddrs(),
  IfProps = proplists:get_value(IfName, IfAddrs),
  case IfProps of
    undefined ->
      {error, if_not_found};
    _ ->
      HwAddr = proplists:get_value(hwaddr, IfProps),
      case HwAddr of
        undefined ->
          _ = lager:error(
            "~s: invalid interface name '~p' setup for the object id server",
            [?MODULE_STRING, IfName]
          ),
          {error, invalid_if};
        _ -> {ok, HwAddr}
      end
  end.
