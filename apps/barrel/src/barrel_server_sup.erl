%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 29. Nov 2018 23:21
%%%-------------------------------------------------------------------
-module(barrel_server_sup).
-author("benoitc").
-behaviour(supervisor).

%% API
-export([start_link/0]).

-export([init/1]).

-include_lib("barrel/include/barrel.hrl").


start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
  Spec =
    #{ id => barrel,
       start => {barrel_server, start_link, []}},

  SupFlags = #{ strategy => simple_one_for_one },
  {ok, {SupFlags, [Spec]}}.
