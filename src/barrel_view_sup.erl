-module(barrel_view_sup).
-behaviour(supervisor).

-export([start_view/4]).

-export([start_link/0]).
-export([init/1]).

-include("barrel.hrl").

start_view(BarrelId, ViewId, ViewMod, ViewConfig) ->
  supervisor:start_child(?MODULE, [BarrelId, ViewId, ViewMod, ViewConfig]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->
  _ = ets:new(?VIEWS, [named_table, public, set,
                       {read_concurrency, true}]),
  Spec = #{ id => view,
            start => {barrel_view_adapter, start_link, []} },
  SupFlags = #{ strategy => simple_one_for_one },
  {ok, {SupFlags, [Spec]}}.


