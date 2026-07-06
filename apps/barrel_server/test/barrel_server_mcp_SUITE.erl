%%%-------------------------------------------------------------------
%%% @doc The MCP endpoint mounted at /mcp. Open group: a real MCP
%%% client (barrel_mcp_client over Streamable HTTP) initializes, sees
%%% the (still empty) tool registry, keeps a session across requests,
%%% and the REST surface is unaffected by the mount. Locked group:
%%% /mcp authenticates through its own provider (server bearers and
%%% capability tokens pass, everything else answers 401) while the
%%% global middleware keeps covering the REST routes.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_mcp_SUITE).

-export([all/0, groups/0, init_per_suite/1, end_per_suite/1,
         init_per_group/2, end_per_group/2]).
-export([
    t_initialize/1,
    t_registered_tools/1,
    t_session_roundtrip/1,
    t_rest_unaffected/1,
    t_disabled_not_mounted/1,
    t_locked_requires_auth/1,
    t_locked_bad_token/1,
    t_locked_server_token/1,
    t_locked_capability_token/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(ROOT, <<"mcp-root-token">>).

all() ->
    [{group, open}, {group, locked}].

groups() ->
    [
        {open, [], [t_initialize, t_registered_tools, t_session_roundtrip,
                    t_rest_unaffected, t_disabled_not_mounted]},
        {locked, [], [t_locked_requires_auth, t_locked_bad_token,
                      t_locked_server_token, t_locked_capability_token]}
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

init_per_group(locked, Config) ->
    application:set_env(barrel_server, auth, #{tokens => [?ROOT]}),
    restart_http(),
    Config;
init_per_group(_Group, Config) ->
    Config.

end_per_group(locked, _Config) ->
    application:unset_env(barrel_server, auth),
    restart_http(),
    ok;
end_per_group(_Group, _Config) ->
    ok.

%%====================================================================
%% Open group
%%====================================================================

t_initialize(_Config) ->
    C = connect(#{}),
    {ok, Info} = barrel_mcp_client:server_info(C),
    ?assertMatch(#{<<"name">> := _}, Info),
    {ok, Caps} = barrel_mcp_client:server_capabilities(C),
    ?assert(is_map(Caps)),
    {ok, Proto} = barrel_mcp_client:protocol_version(C),
    ?assert(is_binary(Proto)),
    barrel_mcp_client:close(C),
    ok.

t_registered_tools(_Config) ->
    C = connect(#{}),
    {ok, Tools} = barrel_mcp_client:list_tools(C),
    Names = [maps:get(<<"name">>, T) || T <- Tools],
    ?assert(lists:member(<<"doc_put">>, Names)),
    ?assert(lists:member(<<"query">>, Names)),
    barrel_mcp_client:close(C),
    ok.

t_session_roundtrip(_Config) ->
    C = connect(#{}),
    %% several requests ride one Mcp-Session-Id
    {ok, _} = barrel_mcp_client:ping(C),
    {ok, _} = barrel_mcp_client:list_tools(C),
    {ok, _} = barrel_mcp_client:ping(C),
    barrel_mcp_client:close(C),
    ok.

t_rest_unaffected(_Config) ->
    B = base_url(),
    C = connect(#{}),
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
%% Locked group
%%====================================================================

t_locked_requires_auth(_Config) ->
    {ok, 401, _, _} = post_initialize([]),
    %% the REST routes stay behind the global middleware too
    B = base_url(),
    {ok, 401, _, _} = hackney:request(
        put, B ++ "/db/locked_db",
        [{<<"content-type">>, <<"application/json">>}], <<"{}">>, []),
    ok.

t_locked_bad_token(_Config) ->
    {ok, 401, _, _} = post_initialize(auth(<<"not-a-real-token">>)),
    %% a capability-shaped token that resolves to nothing
    {ok, 401, _, _} = post_initialize(
        auth(<<"bsp_aaaaaaaaaaaaaaaa_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb">>)),
    ok.

t_locked_server_token(_Config) ->
    C = connect(#{auth => {bearer, ?ROOT}}),
    {ok, Tools} = barrel_mcp_client:list_tools(C),
    ?assert(length(Tools) > 0),
    {ok, _} = barrel_mcp_client:ping(C),
    barrel_mcp_client:close(C),
    ok.

t_locked_capability_token(Config) ->
    Vec = #{dimension => 3, bm25_backend => memory,
            db_path => filename:join(?config(priv_dir, Config),
                                     "mcp_cap_vec")},
    {ok, #{id := SpaceId}} = barrel_spaces:create_space(
        #{label => <<"mcp-cap">>, vectordb => Vec}),
    {ok, Token, _} = barrel_caps:grant(SpaceId,
                                       #{rights => [read, write]}),
    C = connect(#{auth => {bearer, Token}}),
    {ok, _} = barrel_mcp_client:ping(C),
    {ok, Tools} = barrel_mcp_client:list_tools(C),
    ?assert(length(Tools) > 0),
    barrel_mcp_client:close(C),
    %% a revoked capability stops initializing
    ok = barrel_caps:revoke(Token),
    {ok, 401, _, _} = post_initialize(auth(Token)),
    ok = barrel_spaces:drop_space(SpaceId),
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

connect(Extra) ->
    Base = base_url(),
    Spec = Extra#{transport => {http, list_to_binary(Base ++ "/mcp")}},
    {ok, C} = barrel_mcp_client:start(Spec),
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

auth(Token) ->
    [{<<"authorization">>, <<"Bearer ", Token/binary>>}].

post_initialize(Headers) ->
    B = base_url(),
    Body = json:encode(#{
        <<"jsonrpc">> => <<"2.0">>,
        <<"id">> => 1,
        <<"method">> => <<"initialize">>,
        <<"params">> => #{
            <<"protocolVersion">> => <<"2025-06-18">>,
            <<"capabilities">> => #{},
            <<"clientInfo">> => #{<<"name">> => <<"ct">>,
                                  <<"version">> => <<"0">>}
        }
    }),
    hackney:request(
        post, B ++ "/mcp",
        [{<<"content-type">>, <<"application/json">>},
         {<<"accept">>, <<"application/json, text/event-stream">>}
         | Headers],
        iolist_to_binary(Body), []).
