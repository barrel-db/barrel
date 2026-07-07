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
    t_transport_env_auth/1,
    t_conflict_409/1,
    t_capability_read_scope/1,
    t_capability_write_scope/1,
    t_capability_wrong_space/1,
    t_capability_dead_tokens/1,
    t_capability_fail_closed/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(TOKEN, <<"secret-sync-token">>).
-define(TOKEN2, <<"rotated-token">>).

all() ->
    [t_data_routes_locked, t_sync_routes_locked, t_health_open,
     t_token_rotation_list, t_transport_endpoint_auth,
     t_transport_env_auth, t_conflict_409, t_capability_read_scope,
     t_capability_write_scope, t_capability_wrong_space,
     t_capability_dead_tokens, t_capability_fail_closed].

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

t_conflict_409(Config) ->
    B = ?config(base, Config),
    {201, _} = jreq(put, B ++ "/db/authdb/doc/c1", bearer(?TOKEN),
                    #{<<"v">> => 1}),
    %% an existing live doc without _rev is a CAS miss, now a 409
    {409, Body} = jreq(put, B ++ "/db/authdb/doc/c1", bearer(?TOKEN),
                       #{<<"v">> => 2}),
    ?assertMatch(#{<<"error">> := <<"conflict">>}, json:decode(Body)),
    ok.

%%====================================================================
%% Capability tokens on /db/:db (scoped to the granted space)
%%====================================================================

t_capability_read_scope(Config) ->
    B = ?config(base, Config),
    {Sp, WToken, RToken} = space_with_grants(),
    Db = B ++ "/db/" ++ binary_to_list(Sp),
    %% seed a doc through the write grant
    {201, _} = jreq(put, Db ++ "/doc/a", bearer(WToken), #{<<"v">> => 1}),
    %% the whole pull leg opens to a read grant
    {200, _} = req(get, Db, bearer(RToken), <<>>),
    {200, _} = req(get, Db ++ "/doc/a", bearer(RToken), <<>>),
    {200, _} = req(get, Db ++ "/changes", bearer(RToken), <<>>),
    {200, _} = req(get, Db ++ "/_sync/info", bearer(RToken), <<>>),
    {200, _} = jreq(post, Db ++ "/_sync/changes", bearer(RToken), #{}),
    {200, _} = req(get, Db ++ "/_sync/doc/a", bearer(RToken), <<>>),
    %% but nothing that writes
    {403, _} = jreq(put, Db ++ "/doc/b", bearer(RToken), #{}),
    {403, _} = jreq(put, Db ++ "/_sync/doc/b", bearer(RToken), #{}),
    {403, _} = jreq(post, Db ++ "/_bulk_docs", bearer(RToken),
                    #{<<"docs">> => []}),
    ok.

t_capability_write_scope(Config) ->
    B = ?config(base, Config),
    {Sp, WToken, _RToken} = space_with_grants(),
    Db = B ++ "/db/" ++ binary_to_list(Sp),
    {201, _} = jreq(put, Db ++ "/doc/w1", bearer(WToken),
                    #{<<"v">> => 1}),
    %% a client-minted version pushes through the sync wire
    Author = <<"aabbccddeeff0011">>,
    Hlc = barrel_hlc:decode(<<1:64/big, 0:32/big>>),
    Token = barrel_version:to_token({Hlc, Author}),
    VV = barrel_vv:bump(barrel_vv:new(), {Hlc, Author}),
    {200, Body} = jreq(put, Db ++ "/_sync/doc/w2", bearer(WToken),
                       #{<<"doc">> => #{<<"v">> => 2},
                         <<"version">> => Token,
                         <<"vv">> => base64:encode(barrel_vv:encode(VV))}),
    ?assertMatch(#{<<"winner">> := _}, json:decode(Body)),
    %% write does not reach lifecycle or timeline
    {403, _} = req(delete, Db, bearer(WToken), <<>>),
    {403, _} = jreq(post, Db ++ "/_timeline/branch", bearer(WToken),
                    #{<<"name">> => <<"nope">>}),
    ok.

t_capability_wrong_space(Config) ->
    B = ?config(base, Config),
    {_Sp, WToken, RToken} = space_with_grants(),
    {ok, #{id := Other}} = barrel_spaces:create_space(
        #{label => <<"other">>}),
    OtherDb = B ++ "/db/" ++ binary_to_list(Other),
    {403, _} = req(get, OtherDb, bearer(RToken), <<>>),
    {403, _} = jreq(put, OtherDb ++ "/doc/x", bearer(WToken), #{}),
    %% and a non-space db is just as much the wrong space
    {403, _} = req(get, B ++ "/db/authdb", bearer(RToken), <<>>),
    ok.

t_capability_dead_tokens(Config) ->
    B = ?config(base, Config),
    {Sp, _WToken, RToken} = space_with_grants(),
    Db = B ++ "/db/" ++ binary_to_list(Sp),
    {200, _} = req(get, Db, bearer(RToken), <<>>),
    ok = barrel_caps:revoke(RToken),
    {401, _} = req(get, Db, bearer(RToken), <<>>),
    {401, _} = req(get, Db, bearer(<<"bsp_garbage">>), <<>>),
    ok.

t_capability_fail_closed(Config) ->
    B = ?config(base, Config),
    {Sp, WToken, _RToken} = space_with_grants(),
    Db = B ++ "/db/" ++ binary_to_list(Sp),
    %% unmapped tails, db lifecycle, and non-db paths all answer 403
    {403, _} = req(get, Db ++ "/_bogus", bearer(WToken), <<>>),
    {403, _} = req(put, Db, bearer(WToken), <<>>),
    {403, _} = req(get, Db ++ "/_timeline", bearer(WToken), <<>>),
    {403, _} = req(get, B ++ "/", bearer(WToken), <<>>),
    ok.

space_with_grants() ->
    {ok, #{id := Sp}} = barrel_spaces:create_space(
        #{label => <<"caps-auth">>}),
    {ok, WToken, _} = barrel_caps:grant(Sp, #{rights => [write]}),
    {ok, RToken, _} = barrel_caps:grant(Sp, #{rights => [read]}),
    {Sp, WToken, RToken}.

jreq(Method, Url, Headers, JsonTerm) ->
    req(Method, Url,
        [{<<"content-type">>, <<"application/json">>} | Headers],
        json:encode(JsonTerm)).

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
