-module(barrel_view_sup).
-behaviour(supervisor).

-export([start_link/1]).
-export([init/1]).

-include("barrel.hrl").


start_link(Conf) ->
  supervisor:start_link(?MODULE, Conf).


init(Conf) ->
  Specs = [
           #{ id => view_adapter,
              start => {barrel_view, start_link, [Conf]} },

           #{ id => view,
              start => {barrel_view_adapter, start_link, [Conf]},
              restart => transient }

          ],
  SupFlags = #{ strategy => one_for_all },
  {ok, {SupFlags, Specs}}.


