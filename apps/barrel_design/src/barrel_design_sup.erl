%% @private
-module(barrel_design_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Children = [
        #{id => barrel_design_modules,
          start => {barrel_design_modules, start_link, []}},
        #{id => barrel_design_slots,
          start => {barrel_design_slots, start_link, []}},
        #{id => barrel_design_dbs,
          start => {barrel_design_dbs, start_link, []}, shutdown => 30000}
    ],
    {ok, {#{strategy => one_for_one, intensity => 10, period => 60}, Children}}.
