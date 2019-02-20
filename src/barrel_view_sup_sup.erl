-module(barrel_view_sup_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-include("barrel.hrl").

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->
  _ = ets:new(?VIEWS, [named_table, public, set,
                       {read_concurrency, true}]),
  Spec = #{ id => view,
            start => {barrel_view_sup, start_link, []},
            type => supervisor },
  SupFlags = #{ strategy => simple_one_for_one },
  {ok, {SupFlags, [Spec]}}.
