%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 13. Jul 2017 11:13
%%%-------------------------------------------------------------------
-module(barrel_flake).
-behaviour(gen_server).

%% API
-export([
  start_link/0,
  id/0, id/1
]).

-export([curr_time_millis/0, as_list/2]).

-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).


-define(THROW_FLAKE_ERROR(Res), case Res of
                                  {ok, R} -> R;
                                  E -> throw(E)
                                end).

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).



id() ->
  ?THROW_FLAKE_ERROR(gen_server:call(?MODULE, get_id)).

id(Base) ->
  ?THROW_FLAKE_ERROR(gen_server:call(?MODULE, {get_id, Base})).


init([]) ->
  WorkerId = worker_id(),
  MaxTime = curr_time_millis(),
  {ok, #{ max_time => MaxTime, worker_id => WorkerId, sequence => 0}}.


handle_call(get_id, _From, State) ->
  {Reply, NewState} = get_id(curr_time_millis(), State),
  {reply, Reply, NewState};
handle_call({get_id, Base}, _From, State) ->
  {Reply0, NewState} = get_id(curr_time_millis(), State),
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


worker_id() ->
  If = application:get_env(barrel, ts_inteface, get_default_if()),
  {ok, WorkerId} = get_if_hw_int(If),
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

%% get the mac/hardware address of the given interface as a 48-bit integer
get_if_hw_int(undefined) ->
  {error, if_not_found};
get_if_hw_int(IfName) ->
  {ok, IfAddrs} = inet:getifaddrs(),
  IfProps = proplists:get_value(IfName, IfAddrs),
  case IfProps of
    undefined ->
      {error, if_not_found};
    _ ->
      HwAddr = proplists:get_value(hwaddr, IfProps),
      {ok, hw_addr_to_int(HwAddr)}
  end.

%% convert an array of 6 bytes into a 48-bit integer
hw_addr_to_int(HwAddr) ->
  <<WorkerId:48/integer>> = erlang:list_to_binary(HwAddr),
  WorkerId.

gen_id(Time,WorkerId,Sequence) ->
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

curr_time_millis() -> erlang:system_time(millisecond).


%% ==============================
%% tests

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

flake_test() ->
  TS = curr_time_millis(),
  Worker = hw_addr_to_int(lists:seq(1, 6)),
  Flake = gen_id(TS, Worker, 0),
  <<Time:64/integer, WorkerId:48/integer, Sequence:16/integer>> = Flake,
  ?assert(Time =:= TS),
  ?assert(Worker =:= WorkerId),
  ?assert(Sequence =:= 0),
  <<FlakeInt:128/integer>> = Flake,
  _ = as_list(FlakeInt, 62),
  ok.

-endif.