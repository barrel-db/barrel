%%%-------------------------------------------------------------------
%%% @doc Top supervisor for the barrel server.
%%%
%%% Children (one_for_one):
%%% <ol>
%%%   <li>{@link barrel_server_dbs} - owns open `barrel' database handles.</li>
%%%   <li>{@link barrel_server_http} - the livery HTTP service.</li>
%%% </ol>
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 5, period => 10},
    Children = [
        #{id => barrel_server_dbs,
          start => {barrel_server_dbs, start_link, []},
          restart => permanent,
          shutdown => 5000,
          type => worker,
          modules => [barrel_server_dbs]},
        #{id => barrel_server_http,
          start => {barrel_server_http, start_link, []},
          restart => permanent,
          shutdown => 5000,
          type => supervisor,
          modules => [barrel_server_http]}
    ],
    {ok, {SupFlags, Children}}.
