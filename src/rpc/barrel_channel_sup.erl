%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Jun 2017 08:47
%%%-------------------------------------------------------------------
-module(barrel_channel_sup).
-author("benoitc").
-behaviour(supervisor).

%% API
-export([start_link/1]).

%% supervisor callbacks
-export([init/1]).

start_link(#{ channel := Name } = Params) ->
  SupName = {via, gproc, {n, l, {?MODULE, Name}}},
  case supervisor:start_link(SupName, ?MODULE, []) of
    {ok, Sup} -> start_children(Sup, Params);
    {error, Error} -> Error
  end;

start_link(Params) ->
  {ok, Sup} = supervisor:start_link(?MODULE, []),
  start_children(Sup, Params).

start_children(Sup, Params) ->
  {ok, TypeSup} = supervisor:start_child(
    Sup,
    {channel_transport_sup,
      {barrel_channel_transport_sup, start_link, []},
      transient, infinity, supervisor,
      [barrel_channel_transport_sup]}
  ),
  {ok, Connection} = supervisor:start_child(
    Sup,
    {channel,
      {barrel_channel, start_link, [TypeSup, Params]},
      transient, infinity, worker,
      [barrel_channel]}
  ),
  {ok, Sup, Connection}.

%% supervisor3 callback
init([]) ->
  {ok, {{one_for_all, 1, 10000}, []}}.
