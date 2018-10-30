%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. Oct 2018 05:54
%%%-------------------------------------------------------------------
-module(barrel_store_provider_sup).
-author("benoitc").
-behaviour(supervisor).

%% API
-export([start_link/0]).

-export([init/1]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
  Stores = application:get_env(barrel, stores, []),
  Specs = lists:map(
    fun ({Name, Mod, Args}) -> store_spec(Name, Mod, Args) end,
    Stores
  ),
  SupFlags =
    #{strategy => one_for_one,
      intensity => 5,
      period => 10},
  {ok, {SupFlags, Specs}}.

store_spec(Name, Mod, Args) ->
  #{id => Name,
    start => {barrel_store_provider, start_link, [Name, Mod, Args]},
    restart => permanent,
    shutdown => 5000,
    type => worker,
    modules => [Mod]}.

