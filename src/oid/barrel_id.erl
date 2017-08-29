%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 13. Jul 2017 11:13
%%%-------------------------------------------------------------------
-module(barrel_id).
-behaviour(gen_server).

%% API
-export([
  start_link/0,
  id/0, id/1,
  binary_id/1
]).

-export([
  as_list/2,
  timestamp/1
]).

-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).


-define(THROW_ERROR(Res), case Res of
                                  {ok, R} -> R;
                                  E -> throw(E)
                                end).

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

id() ->
  ?THROW_ERROR(gen_server:call(?MODULE, get_id)).

id(Base) ->
  ?THROW_ERROR(gen_server:call(?MODULE, {get_id, Base})).

binary_id(Base) -> erlang:list_to_binary(id(Base)).

timestamp(<<Time:64/integer, _:48/integer, _Seq:16/integer >>) -> Time;
timestamp(_) -> erlang:error(badarg).

init([]) ->
  WorkerId = worker_id(),
  MaxTime = barrel_ts:curr_time_millis(),
  {ok, #{ max_time => MaxTime, worker_id => WorkerId, sequence => 0}}.


handle_call(get_id, _From, State) ->
  {Reply, NewState} = get_id(barrel_ts:curr_time_millis(), State),
  {reply, Reply, NewState};
handle_call({get_id, Base}, _From, State) ->
  {Reply0, NewState} = get_id(barrel_ts:curr_time_millis(), State),
  case Reply0 of
    {ok, Id} ->
      <<IntId:128/integer>> = Id,
      {reply, {ok, as_list(IntId, Base)}, NewState};
    E ->
      {reply, E, NewState}
  end;
handle_call(Msg, _From, State) ->
  _ = lager:error("unrecognized msg in ~p:handle_call -> ~p~n",[?MODULE, Msg]),
  {reply, bad_call, State}.

handle_cast(_, State) -> {noreply, State}.

handle_info(_, State) -> {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_, State, _) -> {ok, State}.


%% ==============================
%% internals

get_id(Ts, State =  #{  worker_id := WID, sequence := Seq0}) ->
  Seq1 = Seq0 + 1,
  {{ok, gen_id(Ts, WID, Seq1)}, State#{ sequence => Seq1 }};
get_id(Ts, State =  #{ max_time := Max, worker_id := WID}) when Ts > Max ->
  {{ok, gen_id(Ts, WID, 0)}, State#{ sequence => 0 }};
get_id(Ts, State =  #{ max_time := Max }) when Max > Ts ->
  {{error, clock_running_backwards}, State}.


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
worker_id() ->
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

gen_id(Time, WorkerId, Sequence) ->
  <<Time:64/integer, WorkerId:48/integer, Sequence:16/integer>>.

%%
%% n.b. - unique_id_62/0 and friends pulled from riak
%%

%% @doc Convert an integer to its string representation in the given
%%      base.  Bases 2-62 are supported.
-spec as_list(I :: integer(), Base :: integer()) -> string().
as_list(I, 10) ->
  erlang:integer_to_list(I);
as_list(I, Base)
  when is_integer(I),
       is_integer(Base),
       Base >= 2,
       Base =< 1+$Z-$A+10+1+$z-$a ->
  as_list(I, Base, []);
as_list(I, Base) ->
  erlang:error(badarg, [I, Base]).

%% @spec integer_to_list(integer(), integer(), stringing()) -> string()
as_list(I0, Base, R0) ->
  D = I0 rem Base,
  I1 = I0 div Base,
  R1 =
    if
      D >= 36 ->
        [D-36+$a|R0];
      D >= 10 ->
        [D-10+$A|R0];
      true ->
        [D+$0|R0]
    end,
  if
    I1 =:= 0 ->
      R1;
    true ->
      as_list(I1, Base, R1)
  end.

%% ==============================
%% tests

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

flake_test() ->
  TS = barrel_ts:curr_time_millis(),
  << Worker:48/integer >> = list_to_binary(lists:seq(1, 6)),
  Flake = gen_id(TS, Worker, 0),
  <<Time:64/integer, WorkerId:48/integer, Sequence:16/integer>> = Flake,
  ?assert(Time =:= TS),
  ?assert(timestamp(Flake) =:= TS),
  ?assert(Worker =:= WorkerId),
  ?assert(Sequence =:= 0),
  <<FlakeInt:128/integer>> = Flake,
  _ = as_list(FlakeInt, 62),
  ok.

-endif.