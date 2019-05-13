%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 13. Jul 2017 10:51
%%%-------------------------------------------------------------------
-module(barrel_time_server).
-behaviouur(gen_server).
%% API
-export([
  start_link/0,
  get_last_ts/0
]).

-export([
  read_timestamp/0,
  write_timestamp/0,
  curr_time_millis/0
]).

-export([get_worker_id/0]).


-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2
]).

-include("barrel.hrl").

-define(DEFAULT_INTERVAL, 1000).
-define(ALLOWABLE_DOWNTIME, 2592000000).

%% ==============================
%% PUBLIC API

start_link() ->
  barrel_config:set(worker_id, get_worker_id()),
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_last_ts() ->
  gen_server:call(?MODULE, get_ts).

%% ==============================
%% gen server API

init([]) ->
  process_flag(trap_exit, true),
  ok = check_for_clock_error(), %% restart if we detected a clock change
  {ok, TRef} = timer:send_interval(get_interval(), persist_time),
  MRef = erlang:monitor(time_offset, clock_service),
  {ok, #{ tref => TRef, clock_monitor => MRef}}.

handle_call(get_ts, _From, State) ->
  Ts  = read_timestamp(),
  {reply, {ok, Ts}, State};

handle_call(Msg, _From, State) ->
  {reply, {bad_call, Msg}, State}.

handle_info({'CHANGE', MRef, time_offset, clock_service, NewTimeOffset}, State = #{ clock_monitor := MRef }) ->
  if
    NewTimeOffset < 0 ->
      ?LOG_ERROR("system running backward. time offset=~pß~n", [NewTimeOffset]),
      {stop, clock_running_backwards, State};
    true ->
      %% TODO: when cluster will be setup, synchronize the time there
      {noreply, State}
  end;

handle_info(persist_time, State ) ->
  ok = write_timestamp(),
  {noreply, State};

handle_info(_Msg, State) -> {noreply, State}.

handle_cast(_Msg, State) -> {noreply, State}.

terminate(_Reason, _State) ->
  ok.


%% ==============================
%% internals
-include_lib("kernel/include/file.hrl").

read_timestamp() ->
  case file:read_file_info(persist_file(), [raw, {time, universal}]) of
    {ok, #file_info{ mtime = MTime}} ->
      Ts = to_timestamp(MTime),
      {ok, Ts};
    {error, enoent} ->
      %% create an empty file
      ok = file:write_file(persist_file(), <<>>, [raw, write]),
      read_timestamp();
    Error ->
      Error
  end.

-spec write_timestamp() -> ok.
write_timestamp() ->
  MTime = calendar:now_to_universal_time(os:timestamp()),
  case file:write_file_info(persist_file(), #file_info{mtime=MTime}, [{time, universal}, raw]) of
    ok ->
      ok;
    {error, enoent} ->
      %% create an empty file
      ok = file:write_file(persist_file(), <<>>),
      write_timestamp();
    Error ->
      Error
  end.


curr_time_millis() -> erlang:system_time(millisecond).

persist_file() ->
  File = barrel_config:get(barrel_timestamp_file),
  ok = filelib:ensure_dir(File),
  File.


get_interval() ->
  application:get_env(barrel, persist_ts_interval, ?DEFAULT_INTERVAL).


check_for_clock_error() ->
  AllowableDowntime = barrel_config:get(ts_allowable_downtime, ?ALLOWABLE_DOWNTIME),
  {ok, LastTs} = read_timestamp(),
  Now = timestamp(),
  TimeSinceLastRun = Now - LastTs,
  check_for_clock_error(Now >= LastTs, TimeSinceLastRun < AllowableDowntime).


check_for_clock_error(true, true) -> ok;
check_for_clock_error(false, _) ->
  ?LOG_ERROR("system running backwards, failing startup of time service~n", []),
  exit(clock_running_backwards);
check_for_clock_error(_, false) ->
  ?LOG_ERROR("system clock too far advanced, failing startup of time service~n",[]),
  exit(clock_advanced).


to_timestamp({{Year,Month,Day},{Hours,Minutes,Seconds}}) ->
  (calendar:datetime_to_gregorian_seconds(
    {{Year,Month,Day},{Hours,Minutes,Seconds}}
  ) - 62167219200) * 1000.

timestamp() ->
  {Mega, Sec, _} = os:timestamp(),
  ((Mega * 1000000 + Sec) * 1000).


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
          ?LOG_ERROR(
            "~s: invalid interface name '~p' setup for the object id server",
            [?MODULE_STRING, IfName]
          ),
          {error, invalid_if};
        _ -> {ok, HwAddr}
      end
  end.
