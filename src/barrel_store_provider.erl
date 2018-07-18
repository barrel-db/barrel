%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 18. Jul 2018 10:02
%%%-------------------------------------------------------------------
-module(barrel_store_provider).
-author("benoitc").

%% API
-export([
  default/0,
  get_provider/1,
  get_provider_module/1,
  get_provider_barrels/1,
  call/2, call/3,
  cast/2
]).

-export([start_link/4]).

-export([init_it/5]).

-include("barrel.hrl").

default() ->
  default_1(barrel_store_provider_sup:all_stores()).

default_1([]) -> exit(no_stores);
default_1(Stores) ->
  case lists:member(default, Stores) of
    true -> default;
    false ->
      [Default | _] = Stores,
      Default
  end.


get_provider(Store) ->
  gproc:whereis_name(?store_provider(Store)).

get_provider_module(Store) ->
  case gproc:lookup_values(?store_provider(Store)) of
    [{_Pid, Mod}] -> Mod;
    [] -> undefined
  end.

get_provider_barrels(Store) ->
  Mod = get_provider_module(Store),
  Mod:list_barrels(Store).


call(Store, Request) ->
  gen_server:call({via, gproc, ?store_provider(Store)}, Request).

call(Store, Request, Timeout) ->
  gen_server:call({via, gproc, ?store_provider(Store)}, Request, Timeout).

cast(Store, Request) ->
  gen_server:cast({via, gproc, ?store_provider(Store)}, Request).


%% start a provider
start_link(Name, Mod, IMod, Args) ->
  proc_lib:start_link(?MODULE, init_it, [self(), Name, Mod, IMod, Args]).

init_it(Starter, Name, Mod, IMod, Args) ->
  case register_name(Name, IMod) of
    true ->
      ServerName = {via, gproc, ?store_provider(Name)},
      case init_it(Name, Mod, Args) of
        {ok, {ok, State}} ->
          proc_lib:init_ack(Starter, {ok, self()}),
          gen_server:enter_loop(Mod, [], State, ServerName);
        {ok, {ok, State, Timeout}} when is_integer(Timeout) ->
          proc_lib:init_ack(Starter, {ok, self()}),
          gen_server:enter_loop(Mod, [], State, ServerName, Timeout);
        {ok, {stop, Reason}} ->
          gproc:unreg(?store_provider(Name)),
          proc_lib:init_ack(Starter, {error, Reason}),
          exit(Reason);
        {ok, ignore} ->
          gproc:unreg(?store_provider(Name)),
          proc_lib:init_ack(Starter, ignore),
          exit(normal);
        {ok, Else} ->
          gproc:unreg(?store_provider(Name)),
          Error = {bad_return_value, Else},
          proc_lib:init_ack(Starter, {error, Error}),
          exit(Error);
        {'EXIT', Class, Reason, Stacktrace} ->
          gproc:unreg(?store_provider(Name)),
          proc_lib:init_ack(Starter, {error, terminate_reason(Class, Reason, Stacktrace)}),
          erlang:raise(Class, Reason, Stacktrace)
      end;
    {false, Pid} ->
      proc_lib:init_ack(Starter, {error, {already_started, Pid}})
  end.

register_name(Name, Mod) ->
  try gproc:reg(?store_provider(Name), Mod), true
  catch
    error:_ ->
      {false, gproc:where(?store_provider(Name))}
  end.


-ifdef('OTP_RELEASE').
init_it(Name, Mod, Args) ->
  try
    {ok, Mod:init(Name, Args)}
  catch
    throw:R -> {ok, R};
    Class:R:S -> {'EXIT', Class, R, S}
  end.
-else.
init_it(Name, Mod, Args) ->
  try
    {ok, Mod:init(Name, Args)}
  catch
    throw:R -> {ok, R};
    Class:R -> {'EXIT', Class, R, erlang:get_stacktrace()}
  end.
-endif.

terminate_reason(error, Reason, Stacktrace) -> {Reason, Stacktrace};
terminate_reason(exit, Reason, _Stacktrace) -> Reason.