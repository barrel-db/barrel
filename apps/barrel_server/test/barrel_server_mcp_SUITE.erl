%%%-------------------------------------------------------------------
%%% @doc The MCP endpoint mounted at /mcp: a real MCP client
%%% (barrel_mcp_client over Streamable HTTP) initializes, sees the
%%% (still empty) tool registry, keeps a session across requests, and
%%% the REST surface is unaffected by the mount.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_mcp_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
    t_initialize/1,
    t_empty_tools/1,
    t_session_roundtrip/1,
    t_rest_unaffected/1,
    t_disabled_not_mounted/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [t_initialize, t_empty_tools, t_session_roundtrip,
     t_rest_unaffected, t_disabled_not_mounted].

init_per_suite(Config) ->
    application:load(barrel_server),
    application:set_env(barrel_server, data_dir, ?config(priv_dir, Config)),
    application:set_env(barrel_server, http_port, 0),
    {ok, _} = application:ensure_all_started(barrel_server),
    {ok, _} = application:ensure_all_started(hackney),
    Base = base_url(),
    [{base, Base} | Config].

end_per_suite(_Config) ->
    application:stop(barrel_server),
    ok.

%%====================================================================
%% Cases
%%====================================================================

t_initialize(Config) ->
    C = connect(Config),
    {ok, Info} = barrel_mcp_client:server_info(C),
    ?assertMatch(#{<<"name">> := _}, Info),
    {ok, Caps} = barrel_mcp_client:server_capabilities(C),
    ?assert(is_map(Caps)),
    {ok, Proto} = barrel_mcp_client:protocol_version(C),
    ?assert(is_binary(Proto)),
    barrel_mcp_client:close(C),
    ok.

t_empty_tools(Config) ->
    C = connect(Config),
    {ok, Tools} = barrel_mcp_client:list_tools(C),
    ?assertEqual([], Tools),
    barrel_mcp_client:close(C),
    ok.

t_session_roundtrip(Config) ->
    C = connect(Config),
    %% several requests ride one Mcp-Session-Id
    {ok, _} = barrel_mcp_client:ping(C),
    {ok, _} = barrel_mcp_client:list_tools(C),
    {ok, _} = barrel_mcp_client:ping(C),
    barrel_mcp_client:close(C),
    ok.

t_rest_unaffected(Config) ->
    B = ?config(base, Config),
    C = connect(Config),
    {ok, 200, _, _} = hackney:request(get, B ++ "/health", [], <<>>, []),
    {ok, 201, _, _} = hackney:request(
        put, B ++ "/db/mcp_mount_db",
        [{<<"content-type">>, <<"application/json">>}], <<"{}">>, []),
    {ok, 201, _, _} = hackney:request(
        put, B ++ "/db/mcp_mount_db/doc/d1",
        [{<<"content-type">>, <<"application/json">>}],
        <<"{\"v\":1}">>, []),
    {ok, 200, _, Body} = hackney:request(
        get, B ++ "/db/mcp_mount_db/doc/d1", [], <<>>, [with_body]),
    ?assertMatch(#{<<"v">> := 1}, json:decode(Body)),
    {ok, _} = barrel_mcp_client:ping(C),
    barrel_mcp_client:close(C),
    ok.

t_disabled_not_mounted(_Config) ->
    %% flip the env off and restart the http service: /mcp 404s, REST
    %% still answers
    application:set_env(barrel_server, mcp, #{enabled => false}),
    try
        restart_http(),
        B2 = base_url(),
        {ok, Status, _, _} = hackney:request(
            post, B2 ++ "/mcp",
            [{<<"content-type">>, <<"application/json">>}],
            <<"{}">>, []),
        ?assertEqual(404, Status),
        {ok, 200, _, _} = hackney:request(get, B2 ++ "/health", [],
                                          <<>>, [])
    after
        application:unset_env(barrel_server, mcp),
        restart_http()
    end,
    %% back on after the restore
    B3 = base_url(),
    {ok, 200, _, _} = hackney:request(get, B3 ++ "/health", [], <<>>, []),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

base_url() ->
    Children = supervisor:which_children(barrel_server_sup),
    {_, Pid, _, _} = lists:keyfind(barrel_server_http, 1, Children),
    #{h1 := Port} = livery:which_listeners(Pid),
    "http://127.0.0.1:" ++ integer_to_list(Port).

restart_http() ->
    ok = supervisor:terminate_child(barrel_server_sup, barrel_server_http),
    {ok, _} = supervisor:restart_child(barrel_server_sup, barrel_server_http),
    ok.

connect(_Config) ->
    Base = base_url(),
    {ok, C} = barrel_mcp_client:start(#{
        transport => {http, list_to_binary(Base ++ "/mcp")}
    }),
    ok = wait_ready(C, 100),
    C.

wait_ready(_C, 0) ->
    {error, not_ready};
wait_ready(C, N) ->
    case barrel_mcp_client:ping(C) of
        {ok, _} -> ok;
        {error, not_ready} ->
            timer:sleep(50),
            wait_ready(C, N - 1);
        {error, _} = Err ->
            Err
    end.
