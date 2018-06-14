%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 14. Jun 2018 01:08
%%%-------------------------------------------------------------------
-module(barrel_server_sup).
-author("benoitc").
-behaviour(supervisor).

%% API
-export([start_link/0]).

-export([init/1]).


start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
  SupFlags = #{strategy => one_for_one, intensity => 1, period => 1},
  {ok, {SupFlags, []}}.