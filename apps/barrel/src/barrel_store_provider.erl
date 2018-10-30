%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. Oct 2018 10:55
%%%-------------------------------------------------------------------
-module(barrel_store_provider).
-author("benoitc").

%% API
-export([get_provider/1]).

%% internal
-export([start_link/3]).
-export([init_it/4]).

-include("barrel.hrl").

%% Provider  behaviour

-type provider_name() :: atom() | list() | binary().
-type provider_args() :: map().

-callback init(Name :: provider_name(), Args :: provider_args()) -> {ok, pid()} | {error, term()}.


%% Provider API

get_provider(Name) ->
  case gproc:lookup_values(?store_provider(Store)) of
    [{Pid, Mod}] ->
      #{ name => Name, pid => Pid, mod => Mod };
    [] -> undefined
  end.


%% INTERNAL API

%% start a provider
start_link(Name, Mod, Args) ->
  proc_lib:start_link(?MODULE, init_it, [self(), Name, Mod, Args]).


init_it(Starter, Name, Mod, Args) ->
  case register_name(Name, Mod) of
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


%% TODO: fix stacktrace when we fully support OTP 21
init_it(Name, Mod, Args) ->
  try
    {ok, Mod:init(Name, Args)}
  catch
    throw:R  -> {ok, R};
    Class:R ->
      {'EXIT', Class, R, erlang:get_stacktrace()}
  end.


terminate_reason(error, Reason, Stacktrace) -> {Reason, Stacktrace};
terminate_reason(exit, Reason, _Stacktrace) -> Reason.
