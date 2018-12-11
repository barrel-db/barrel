%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 11. Dec 2018 06:02
%%%-------------------------------------------------------------------
-module(barrel_counters).
-author("benoitc").
-behaviour(gen_server).

%% API
-export([
  new/1,
  tick/1,
  tick/2,
  set/2,
  cas/3,
  get_value/1
]).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2]).


-spec new(term()) -> true | false.
new(Name) ->
  ets:insert_new(?MODULE, {Name, 0}).

-spec tick(term()) -> integer().
tick(Name) ->
  tick(Name, 1).

-spec tick(term(), integer()) -> integer().
tick(Name, Val) ->
  ets:update_counter(?MODULE, Name, {2, Val}).

-spec set(term(), integer()) -> true.
set(Name, Val) ->
  ets:insert(?MODULE, {Name, Val}).

-spec cas(Name::term(), Val::integer(), OldVal::integer()) -> true | false.
cas(Name, Val, OldVal) ->
  Old = {Name, OldVal},
  New = {Name, Val},
  (1 =:= ets:select_replace(?MODULE, [{Old, [], [{const, New}]}])).

-spec get_value(term()) -> integer().
get_value(Name) ->
  ets:update_counter(?MODULE, Name, {2, 0}).


start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
  ?MODULE = ets:new(?MODULE, [set, named_table, public,
    {read_concurrency, true}, {write_concurrency, true}]),
  
  {ok, #{}}.

handle_call(_Msg, _From, St) -> {reply, ok, St}.

handle_cast(_Msg,  St) -> {noreply, St}.