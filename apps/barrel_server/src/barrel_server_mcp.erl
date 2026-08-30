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
%%%
%%% Also the registrar: a supervised gen_server (started before the
%%% HTTP service) that registers the barrel tools in barrel_mcp's
%%% shared registry on start and unregisters them on shutdown.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_mcp).
-behaviour(gen_server).

-export([start_link/0]).
-export([enabled/0, routes/0, handler_opts/0]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2]).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

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

%% The HMAC key sealing multi round-trip request state (barrel_mcp
%% `request_state_key'): every node of a fleet must carry the same
%% value, or a retry landing elsewhere fails. Absent, barrel_mcp uses
%% an ephemeral key and says so at start.
apply_request_state_key(#{request_state_key := {file, Path}}) ->
    case file:read_file(Path) of
        {ok, Key} when byte_size(Key) > 0 ->
            application:set_env(barrel_mcp, request_state_key,
                                string:trim(Key, trailing, "\n"));
        {ok, _Empty} ->
            logger:error("barrel_server: mcp request_state_key file ~s "
                         "is empty", [Path]);
        {error, Reason} ->
            logger:error("barrel_server: cannot read mcp "
                         "request_state_key file ~s: ~p", [Path, Reason])
    end;
apply_request_state_key(#{request_state_key := Key})
  when is_binary(Key), byte_size(Key) > 0 ->
    application:set_env(barrel_mcp, request_state_key, Key);
apply_request_state_key(_Cfg) ->
    ok.

%%====================================================================
%% gen_server (the tool registrar)
%%====================================================================

%% @private
init([]) ->
    %% trap exits so terminate runs on supervisor shutdown and the
    %% tools leave the shared registry with the server
    process_flag(trap_exit, true),
    case enabled() of
        true ->
            ok = apply_request_state_key(config()),
            ok = barrel_server_mcp_tools:register_all(),
            ok = barrel_server_mcp_agent:register_all(),
            ok = barrel_server_mcp_resources:register_all();
        false ->
            ok
    end,
    {ok, #{}}.

%% @private
handle_call(_Req, _From, State) ->
    {reply, ok, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    %% the registry lives in the barrel_mcp app and may already be
    %% gone during a full shutdown
    try
        barrel_server_mcp_tools:unregister_all(),
        barrel_server_mcp_agent:unregister_all(),
        barrel_server_mcp_resources:unregister_all()
    catch _:_ -> ok
    end,
    ok.
