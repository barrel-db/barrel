%%%-------------------------------------------------------------------
%%% @doc CORS middleware over real HTTP. Open group: preflights are
%%% answered before routing, actual responses carry allow-origin and
%%% the x-barrel-* expose set, and /mcp keeps its own origin policy.
%%% Locked group: preflights need no Authorization and 401s still
%%% carry CORS headers so browser JS can read the error. Default:
%%% no cors env means no CORS headers at all.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_cors_SUITE).

-export([all/0, groups/0, init_per_suite/1, end_per_suite/1,
         init_per_group/2, end_per_group/2]).
-export([
    t_preflight_answered/1,
    t_actual_expose/1,
    t_mcp_skipped/1,
    t_preflight_without_auth/1,
    t_unauthorized_carries_cors/1,
    t_default_off/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(ROOT, <<"cors-root-token">>).
-define(ORIGIN, <<"https://app.example">>).

all() ->
    [{group, open_cors}, {group, locked_cors}, {group, no_cors}].

groups() ->
    [
        {open_cors, [], [t_preflight_answered, t_actual_expose,
                         t_mcp_skipped]},
        {locked_cors, [], [t_preflight_without_auth,
                           t_unauthorized_carries_cors]},
        {no_cors, [], [t_default_off]}
    ].

init_per_suite(Config) ->
    application:load(barrel_server),
    application:set_env(barrel_server, data_dir, ?config(priv_dir, Config)),
    application:set_env(barrel_server, http_port, 0),
    {ok, _} = application:ensure_all_started(barrel_server),
    {ok, _} = application:ensure_all_started(hackney),
    Config.

end_per_suite(_Config) ->
    application:stop(barrel_server),
    ok.

init_per_group(open_cors, Config) ->
    application:set_env(barrel_server, cors,
                        #{origins => '*', max_age => 600}),
    restart_http(),
    Config;
init_per_group(locked_cors, Config) ->
    application:set_env(barrel_server, cors,
                        #{origins => '*', max_age => 600}),
    application:set_env(barrel_server, auth, #{tokens => [?ROOT]}),
    restart_http(),
    Config;
init_per_group(no_cors, Config) ->
    application:unset_env(barrel_server, cors),
    application:unset_env(barrel_server, auth),
    restart_http(),
    Config.

end_per_group(_Group, _Config) ->
    application:unset_env(barrel_server, cors),
    application:unset_env(barrel_server, auth),
    restart_http(),
    ok.

%%====================================================================
%% Open group
%%====================================================================

t_preflight_answered(_Config) ->
    {S, H, _} = req(options, "/db/foo/doc/a", preflight_headers()),
    ?assertEqual(204, S),
    ?assertEqual(<<"*">>, header(<<"access-control-allow-origin">>, H)),
    Methods = header(<<"access-control-allow-methods">>, H),
    ?assertNotEqual(nomatch, binary:match(Methods, <<"PUT">>)),
    ?assertEqual(<<"600">>, header(<<"access-control-max-age">>, H)).

t_actual_expose(_Config) ->
    {200, H, _} = req(get, "/health", [{<<"origin">>, ?ORIGIN}]),
    ?assertEqual(<<"*">>, header(<<"access-control-allow-origin">>, H)),
    Expose = header(<<"access-control-expose-headers">>, H),
    ?assertNotEqual(nomatch, binary:match(Expose, <<"x-barrel-hlc">>)).

%% /mcp is skipped by the middleware (livery_mcp owns its origin
%% policy); a cors-handled preflight would carry the configured
%% max-age, so its absence proves the skip.
t_mcp_skipped(_Config) ->
    {_, H, _} = req(options, "/mcp", preflight_headers()),
    ?assertEqual(undefined, header(<<"access-control-max-age">>, H)).

%%====================================================================
%% Locked group
%%====================================================================

t_preflight_without_auth(_Config) ->
    {S, H, _} = req(options, "/db/foo/doc/a", preflight_headers()),
    ?assertEqual(204, S),
    ?assertEqual(<<"*">>, header(<<"access-control-allow-origin">>, H)).

t_unauthorized_carries_cors(_Config) ->
    {S, H, _} = req(get, "/db/foo", [{<<"origin">>, ?ORIGIN}]),
    ?assertEqual(401, S),
    ?assertEqual(<<"*">>, header(<<"access-control-allow-origin">>, H)).

%%====================================================================
%% Default group
%%====================================================================

t_default_off(_Config) ->
    {200, H, _} = req(get, "/health", [{<<"origin">>, ?ORIGIN}]),
    ?assertEqual(undefined, header(<<"access-control-allow-origin">>, H)).

%%====================================================================
%% Helpers
%%====================================================================

preflight_headers() ->
    [{<<"origin">>, ?ORIGIN},
     {<<"access-control-request-method">>, <<"PUT">>}].

req(Method, Path, Headers) ->
    Url = list_to_binary(base_url() ++ Path),
    {ok, S, RespHeaders, Body} =
        hackney:request(Method, Url, Headers, <<>>, [with_body]),
    {S, RespHeaders, Body}.

header(Name, Headers) ->
    case lists:keyfind(Name, 1, lower(Headers)) of
        {Name, Value} -> Value;
        false -> undefined
    end.

lower(Headers) ->
    [{string:lowercase(K), V} || {K, V} <- Headers].

base_url() ->
    Children = supervisor:which_children(barrel_server_sup),
    {_, Pid, _, _} = lists:keyfind(barrel_server_http, 1, Children),
    #{h1 := Port} = livery:which_listeners(Pid),
    "http://127.0.0.1:" ++ integer_to_list(Port).

restart_http() ->
    ok = supervisor:terminate_child(barrel_server_sup, barrel_server_http),
    {ok, _} = supervisor:restart_child(barrel_server_sup, barrel_server_http),
    ok.
