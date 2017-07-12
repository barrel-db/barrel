%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Jun 2017 10:17
%%%-------------------------------------------------------------------
%% TODO: do we really need supervisor3
-module(barrel_channel_transport_sup).
-author("benoitc").
-behaviour(barrel_supervisor3).

%% API
-export([
  start_link/0,
  type_mod/1
]).

%% supervisor3 callbacks
-export([init/1, post_init/1]).

type_mod(#{ type := direct }) -> {direct, barrel_direct_transport};
type_mod(#{ type := Type }) ->
  Transports = application:get_env(barrel, transports, #{}),
  case maps:find(Type, Transports) of
    {ok, Mod} ->
      {Type, Mod};
    error ->
      erlang:error(bad_connection_type)
  end.

start_link() -> barrel_supervisor3:start_link(?MODULE, []).

init([]) ->
  {ok, {{one_for_all, 0, 1}, []}}.

post_init(_) -> ignore.