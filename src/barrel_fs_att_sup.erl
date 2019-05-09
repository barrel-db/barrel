-module(barrel_fs_att_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->
  Spec = #{ id => view,
            start => {barrel_fs_att, start_link, []},
            restart => temporary,
            type => worker },
  SupFlags = #{ strategy => simple_one_for_one },
  {ok, {SupFlags, [Spec]}}.
