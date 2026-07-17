%%%-------------------------------------------------------------------
%%% @doc Top supervisor for the barrel server.
%%%
%%% Children (one_for_one):
%%% <ol>
%%%   <li>{@link barrel_server_mcp_live} - the live-query bridge
%%%       (owns every MCP-created subscription).</li>
%%%   <li>{@link barrel_server_mcp} - the MCP tool registrar (before
%%%       the HTTP service so /mcp never serves an empty registry).</li>
%%%   <li>{@link barrel_server_http} - the livery HTTP service.</li>
%%% </ol>
%%% Open database handles are owned by Barrel's database lifecycle manager
%%% ({@link barrel_dbs}, in the barrel application); handlers reach it
%%% through the {@link barrel_server_dbs} shim.
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
        #{id => barrel_server_sig_cache,
          start => {barrel_server_sig_cache, start_link, []},
          restart => permanent,
          shutdown => 5000,
          type => worker,
          modules => [barrel_server_sig_cache]},
        #{id => barrel_server_mcp_live,
          start => {barrel_server_mcp_live, start_link, []},
          restart => permanent,
          shutdown => 5000,
          type => worker,
          modules => [barrel_server_mcp_live]},
        #{id => barrel_server_mcp,
          start => {barrel_server_mcp, start_link, []},
          restart => permanent,
          shutdown => 5000,
          type => worker,
          modules => [barrel_server_mcp]},
        #{id => barrel_server_http,
          start => {barrel_server_http, start_link, []},
          restart => permanent,
          shutdown => 5000,
          type => supervisor,
          modules => [barrel_server_http]}
    ],
    {ok, {SupFlags, Children}}.
