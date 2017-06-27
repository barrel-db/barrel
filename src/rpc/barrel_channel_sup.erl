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

%% supervisor 3 callbacks
-export([init/1, post_init/1]).

start_link(Params) ->
  {ok, Sup} = supervisor3:start_link(?MODULE, []),
  {ok, TypeSup} = supervisor3:start_child(
    Sup,
    {channel_transport_sup,
      {barrel_channel_transport_sup, start_link, []},
      transient, infinity, supervisor,
      [barrel_channel_transport_sup]}
  ),
  {ok, Connection} = supervisor3:start_child(
    Sup,
    {channel,
      {barrel_channel, start_link, [TypeSup, Params]},
      intrinsic, brutal_kill, worker,
      [barrel_channel]}
  ),
  {ok, Sup, Connection}.

%% supervisor3 callback
init([]) ->
  {ok, {{one_for_all, 0, 1}, []}}.

post_init(_) -> ignore.