-module(barrel_fold_process_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->
  Spec = #{ id => barrel_fold_process,
            start => {barrel_fold_process, start_link, []},
            restart => temporary,
            shutdown => brutal_kill },
  SupFlags = #{ strategy => simple_one_for_one },
  {ok, {SupFlags, [Spec]}}.
