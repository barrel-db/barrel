%%%-------------------------------------------------------------------
%%% @doc Static bearer auth: when a token is configured the whole REST
%%% API (data and sync routes) requires it, /health stays open, and
%%% the replication transport authenticates from its endpoint or the
%%% sync_auth env.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_auth_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
    t_data_routes_locked/1,
    t_sync_routes_locked/1,
    t_health_open/1,
    t_token_rotation_list/1,
    t_transport_endpoint_auth/1,
    t_transport_env_auth/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(TOKEN, <<"secret-sync-token">>).
-define(TOKEN2, <<"rotated-token">>).

all() ->
    [t_data_routes_locked, t_sync_routes_locked, t_health_open,
     t_token_rotation_list, t_transport_endpoint_auth,
     t_transport_env_auth].

init_per_suite(Config) ->
    application:load(barrel_server),
    application:set_env(barrel_server, data_dir, ?config(priv_dir, Config)),
    application:set_env(barrel_server, http_port, 0),
    application:set_env(barrel_server, auth,
                        #{tokens => [?TOKEN, ?TOKEN2]}),
    {ok, _} = application:ensure_all_started(barrel_server),
    {ok, _} = application:ensure_all_started(hackney),
    Children = supervisor:which_children(barrel_server_sup),
    {_, Pid, _, _} = lists:keyfind(barrel_server_http, 1, Children),
    #{h1 := Port} = livery:which_listeners(Pid),
    Base = "http://127.0.0.1:" ++ integer_to_list(Port),
    [{base, Base} | Config].

end_per_suite(_Config) ->
    application:stop(barrel_server),
    application:unset_env(barrel_server, auth),
    application:unset_env(barrel_docdb, sync_auth),
    ok.

req(Method, Url, Headers, Body) ->
    {ok, S, _H, RespBody} = hackney:request(
        Method, list_to_binary(Url), Headers, Body, [with_body]),
    {S, RespBody}.

bearer(Token) ->
    [{<<"authorization">>, <<"Bearer ", Token/binary>>}].

%%====================================================================
%% Cases
%%====================================================================

t_data_routes_locked(Config) ->
    B = ?config(base, Config),
    {401, Body} = req(put, B ++ "/db/authdb", [], <<>>),
    ?assertMatch(#{<<"error">> := <<"unauthorized">>}, json:decode(Body)),
    {401, _} = req(put, B ++ "/db/authdb", bearer(<<"wrong">>), <<>>),
    {201, _} = req(put, B ++ "/db/authdb", bearer(?TOKEN), <<>>),
    {200, _} = req(get, B ++ "/db/authdb", bearer(?TOKEN), <<>>),
    ok.

t_sync_routes_locked(Config) ->
    B = ?config(base, Config),
    {401, _} = req(get, B ++ "/db/authdb/_sync/info", [], <<>>),
    {200, _} = req(get, B ++ "/db/authdb/_sync/info", bearer(?TOKEN),
                   <<>>),
    ok.

t_health_open(Config) ->
    B = ?config(base, Config),
    {200, _} = req(get, B ++ "/health", [], <<>>),
    ok.

t_token_rotation_list(Config) ->
    B = ?config(base, Config),
    %% both configured tokens are accepted during a rollover
    {200, _} = req(get, B ++ "/db/authdb/_sync/info", bearer(?TOKEN2),
                   <<>>),
    ok.

t_transport_endpoint_auth(Config) ->
    B = ?config(base, Config),
    Local = <<"auth_local_a">>,
    {ok, _} = barrel_docdb:create_db(Local, #{
        data_dir => filename:join(?config(priv_dir, Config), "l1")}),
    try
        {ok, _} = barrel_docdb:put_doc(Local, #{<<"id">> => <<"a">>}),
        %% without credentials the transport surfaces unauthorized
        Bare = barrel_rep_transport_http:endpoint(
            list_to_binary(B ++ "/db/auth_tgt")),
        ?assertMatch({error, _},
                     barrel_rep:replicate(
                         Local, Bare,
                         #{target_transport => barrel_rep_transport_http})),
        %% with the endpoint token it flows
        Authed = Bare#{auth => #{token => ?TOKEN}},
        {ok, #{docs_written := 1}} =
            barrel_rep:replicate(
                Local, Authed,
                #{target_transport => barrel_rep_transport_http}),
        {ok, _} = barrel_docdb:get_doc(<<"auth_tgt">>, <<"a">>)
    after
        _ = barrel_docdb:delete_db(Local)
    end.

t_transport_env_auth(Config) ->
    B = ?config(base, Config),
    Local = <<"auth_local_b">>,
    {ok, _} = barrel_docdb:create_db(Local, #{
        data_dir => filename:join(?config(priv_dir, Config), "l2")}),
    try
        {ok, _} = barrel_docdb:put_doc(Local, #{<<"id">> => <<"b">>}),
        Endpoint = barrel_rep_transport_http:endpoint(
            list_to_binary(B ++ "/db/auth_tgt2")),
        %% token resolved from the sync_auth env, keyed by origin
        Origin = list_to_binary(B),
        application:set_env(barrel_docdb, sync_auth,
                            #{Origin => ?TOKEN}),
        {ok, #{docs_written := 1}} =
            barrel_rep:replicate(
                Local, Endpoint,
                #{target_transport => barrel_rep_transport_http}),
        {ok, _} = barrel_docdb:get_doc(<<"auth_tgt2">>, <<"b">>)
    after
        application:unset_env(barrel_docdb, sync_auth),
        _ = barrel_docdb:delete_db(Local)
    end.
