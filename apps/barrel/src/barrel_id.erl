za%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 13. Jul 2017 11:13
%%%-------------------------------------------------------------------
-module(barrel_id).

%% API
-export([
  id/0, id/1,
  binary_id/1
]).

-export([lid/0]).

-export([
  as_list/2,
  timestamp/1,
  worker_id/0,
  seq/0,
  t/0
]).

id() ->
  gen_id(barrel_server:curr_time_millis(), worker_id(), seq()).

id(Base) ->
  <<IntId:128/integer>> = id(),
  as_list(IntId, Base).


binary_id(Base) -> erlang:list_to_binary(id(Base)).

timestamp(<<Time:64/integer, _:48/integer, _Seq:16/integer >>) -> Time;
timestamp(_) -> erlang:error(badarg).

lid() ->
  T = erlang:monotonic_time(nanosecond),
  C = seq(),
  << T:50,C:14 >>.


%% ==============================
%% internals


gen_id(Time, WorkerId, Sequence) ->
  <<Time:64/integer, WorkerId:48/integer, Sequence:16/integer>>.

worker_id() ->
  barrel_mochiglobal:get(worker_id).


t() ->
  {A, B, C} = os:timestamp(),
  {A, B, C band 16#ffc00}.

seq() ->
  erlang:unique_integer([monotonic, positive]) band 16#ffff.


%%
%% n.b. - ≈/0 and friends pulled from riak
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

%% TODO: detail errors
-spec as_list(I :: integer(), Base :: integer(), Exr :: string()) -> string() | {error, any()}.
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
  TS = barrel_server:curr_time_millis(),
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
