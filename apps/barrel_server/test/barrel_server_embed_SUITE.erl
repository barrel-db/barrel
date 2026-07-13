%%%-------------------------------------------------------------------
%%% @doc Embedding tests: mount barrel's routes inside a host livery
%%% service under a sub-path, with the host owning authentication.
%%%
%%% No barrel_server application is started here. The suite starts only
%%% `barrel' and `barrel_spaces', builds its own livery router with a
%%% host-owned `/ping' route plus barrel's default DB surface
%%% ({@link barrel_server_api:router/0}) nested under `/barrel', and
%%% installs its own auth middleware. This proves the DB surface is
%%% mountable with a prefix and governed by the host's middleware.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_embed_SUITE).
-behaviour(livery_middleware).

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
    t_host_route/1,
    t_embedded_db_lifecycle/1,
    t_embedded_doc_crud/1,
    t_embedded_sync_info/1,
    t_host_owns_auth/1,
    t_default_excludes_spaces/1
]).

%% Host router handler and middleware (referenced from the route table
%% and the service middleware stack).
-export([ping/1, call/3]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(DB, "embeddb").
-define(AUTH, {<<"x-host-auth">>, <<"let-me-in">>}).

all() ->
    [t_host_route, t_embedded_db_lifecycle, t_embedded_doc_crud,
     t_embedded_sync_info, t_host_owns_auth, t_default_excludes_spaces].

init_per_suite(Config) ->
    application:set_env(barrel_docdb, data_dir, ?config(priv_dir, Config)),
    {ok, _} = application:ensure_all_started(barrel),
    {ok, _} = application:ensure_all_started(barrel_spaces),
    {ok, _} = application:ensure_all_started(livery),
    {ok, _} = application:ensure_all_started(hackney),
    %% Host router: a host-owned /ping plus barrel's default DB surface
    %% mounted under /barrel. The host middleware owns auth for the whole
    %% service (barrel ships none in embedded mode).
    HostRouter = livery_router:compile(
        [{<<"GET">>, <<"/ping">>, {?MODULE, ping}}]),
    Router = livery_router:nest(<<"/barrel">>,
                                barrel_server_api:router(),
                                HostRouter),
    {ok, Pid} = livery:start_service(#{
        http => #{port => 0},
        router => Router,
        middleware => [{?MODULE, #{}}]
    }),
    %% start_service links to this (ephemeral) init process; unlink so the
    %% service survives until end_per_suite stops it explicitly.
    true = unlink(Pid),
    #{h1 := Port} = livery:which_listeners(Pid),
    Base = "http://127.0.0.1:" ++ integer_to_list(Port),
    {201, _} = req(put, Base ++ "/barrel/db/" ++ ?DB, <<>>),
    [{svc, Pid}, {base, Base} | Config].

end_per_suite(Config) ->
    livery:stop_service(?config(svc, Config)),
    application:stop(barrel_spaces),
    application:stop(barrel),
    ok.

%%====================================================================
%% Cases
%%====================================================================

%% The host's own route coexists with the mounted barrel subtree.
t_host_route(Config) ->
    B = base(Config),
    {200, <<"pong">>} = req_raw(get, B ++ "/ping"),
    ok.

t_embedded_db_lifecycle(Config) ->
    B = base(Config),
    {200, Info} = req(get, B ++ "/barrel/db/" ++ ?DB, <<>>),
    ?assert(is_map(Info)),
    ok.

t_embedded_doc_crud(Config) ->
    B = base(Config),
    {201, _} = req_json(put, B ++ "/barrel/db/" ++ ?DB ++ "/doc/d1",
                        #{<<"v">> => 42}),
    {200, #{<<"v">> := 42}} =
        req(get, B ++ "/barrel/db/" ++ ?DB ++ "/doc/d1", <<>>),
    ok.

%% The sync wire (what barrel-lite drives) is reachable under the prefix.
t_embedded_sync_info(Config) ->
    B = base(Config),
    {200, Info} = req(get, B ++ "/barrel/db/" ++ ?DB ++ "/_sync/info", <<>>),
    ?assertMatch(#{<<"db">> := _}, Info),
    ok.

%% Without the host's header the host middleware rejects the request,
%% proving the host governs auth for barrel's routes.
t_host_owns_auth(Config) ->
    B = base(Config),
    {401, _} = req_noauth(get, B ++ "/barrel/db/" ++ ?DB),
    ok.

%% The agent layer is not part of the default DB surface.
t_default_excludes_spaces(Config) ->
    B = base(Config),
    {404, _} = req(post, B ++ "/barrel/spaces", <<>>),
    ok.

%%====================================================================
%% Host router handler + middleware
%%====================================================================

ping(_Req) ->
    livery_resp:text(200, <<"pong">>).

call(Req, Next, _Cfg) ->
    case livery_req:header(<<"x-host-auth">>, Req, undefined) of
        <<"let-me-in">> ->
            Next(Req);
        _ ->
            livery_resp:new(
                401,
                [{<<"content-type">>, <<"application/json">>}],
                {full, <<"{\"error\":\"host_auth\"}">>})
    end.

%%====================================================================
%% HTTP helpers
%%====================================================================

base(Config) -> ?config(base, Config).

req(Method, Url, Body) ->
    decode(hackney:request(Method, list_to_binary(Url), [?AUTH], Body,
                           [with_body])).

req_json(Method, Url, Map) ->
    Headers = [?AUTH, {<<"content-type">>, <<"application/json">>}],
    decode(hackney:request(Method, list_to_binary(Url), Headers,
                           json:encode(Map), [with_body])).

req_raw(Method, Url) ->
    {ok, S, _H, Body} = hackney:request(Method, list_to_binary(Url), [?AUTH],
                                        <<>>, [with_body]),
    {S, Body}.

req_noauth(Method, Url) ->
    decode(hackney:request(Method, list_to_binary(Url), [], <<>>,
                           [with_body])).

decode({ok, S, _H, Body}) ->
    Decoded = case Body of
        <<>> -> #{};
        _ -> try json:decode(Body) catch _:_ -> Body end
    end,
    {S, Decoded};
decode(Other) ->
    error({http, Other}).
