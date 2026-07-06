%%%-------------------------------------------------------------------
%%% @doc The MCP endpoint: mounts livery_mcp (the Streamable HTTP
%%% bridge to the barrel_mcp engine) at `/mcp' inside the REST
%%% router. Configured with `{barrel_server, mcp, #{...}}':
%%%
%%% - `enabled' - mount the endpoint (default true)
%%% - `allowed_origins' - `any' or a list of Origin values (default
%%%   `any'; tighten for browser-reachable deployments)
%%% - `allow_missing_origin' - accept requests without an Origin
%%%   header (default true; non-browser MCP clients send none)
%%%
%%% Auth: `/mcp' is exempt from the global bearer middleware and
%%% authenticates through barrel_server_mcp_auth instead, which
%%% covers the same server tokens plus capability (`bsp_') tokens.
%%% Tools and resources register through barrel_mcp's registry (see
%%% barrel_server_mcp_tools in later steps).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_mcp).

-export([enabled/0, routes/0, handler_opts/0]).

%% @doc Whether the MCP endpoint is mounted.
-spec enabled() -> boolean().
enabled() ->
    maps:get(enabled, config(), true).

%% @doc The `/mcp' routes (empty when disabled). The handler serves
%% POST (JSON-RPC), GET (the SSE notification stream), DELETE
%% (session termination) and OPTIONS (preflight).
-spec routes() -> [{binary(), binary(), term()}].
routes() ->
    case enabled() of
        true ->
            Handler = livery_mcp:handler(handler_opts()),
            [
                {<<"POST">>,    <<"/mcp">>, Handler},
                {<<"GET">>,     <<"/mcp">>, Handler},
                {<<"DELETE">>,  <<"/mcp">>, Handler},
                {<<"OPTIONS">>, <<"/mcp">>, Handler}
            ];
        false ->
            []
    end.

%% @doc livery_mcp handler options derived from the env.
-spec handler_opts() -> livery_mcp:opts().
handler_opts() ->
    Cfg = config(),
    #{
        auth => #{provider => barrel_server_mcp_auth},
        allowed_origins => maps:get(allowed_origins, Cfg, any),
        allow_missing_origin => maps:get(allow_missing_origin, Cfg, true)
    }.

config() ->
    application:get_env(barrel_server, mcp, #{}).
