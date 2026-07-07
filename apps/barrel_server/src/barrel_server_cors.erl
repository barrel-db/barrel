%%%-------------------------------------------------------------------
%%% @doc CORS for the REST API, in front of auth.
%%%
%%% Thin wrapper over livery_cors configured from
%%% `{barrel_server, cors, Map}': `origins' (`'*'' or a list of origin
%%% binaries), `expose' (response headers readable by browser JS;
%%% defaults to the x-barrel-* set so clients can fold the HLC clock),
%%% plus livery_cors passthroughs (`methods', `headers', `credentials',
%%% `max_age'). Unconfigured = the middleware is not installed and no
%%% CORS headers are emitted. `/mcp' is skipped: the MCP endpoint
%%% carries its own origin policy (livery_mcp allowed_origins) and
%%% OPTIONS route, and double CORS headers break browsers.
%%%
%%% Runs BEFORE barrel_server_auth so preflights (which carry no
%%% Authorization by spec) answer 204, while actual-request CORS
%%% headers wrap auth 401s and browser JS can read the error body.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_cors).
-behaviour(livery_middleware).

-export([state_from_env/0]).
-export([call/3]).

%% @doc Middleware state from the app env; undefined = no CORS.
-spec state_from_env() -> map() | undefined.
state_from_env() ->
    case application:get_env(barrel_server, cors, undefined) of
        undefined ->
            undefined;
        Map when is_map(Map) ->
            maps:merge(#{expose => default_expose()}, Map);
        Other ->
            logger:warning("ignoring invalid barrel_server cors config: ~p",
                           [Other]),
            undefined
    end.

%% Browser JS only reads whitelisted response headers; the x-barrel-*
%% set is required for client clock folding and attachment sync.
default_expose() ->
    [<<"x-barrel-hlc">>, <<"x-barrel-digest">>, <<"x-barrel-att-length">>].

call(Req, Next, Cfg) ->
    case livery_req:path(Req) of
        <<"/mcp">> -> Next(Req);
        _ -> livery_cors:call(Req, Next, Cfg)
    end.
