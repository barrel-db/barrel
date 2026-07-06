%%%-------------------------------------------------------------------
%%% @doc Resources and live queries over MCP: the templates list and
%%% read, a live query end to end (subscribe, updated notification
%%% over the SSE stream, re-read, delete, updated again), explicit
%%% unsubscribe, the bridge's session GC, the per-session cap, and
%%% the live_only resources mode.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_mcp_live_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
    t_templates_list_read/1,
    t_live_e2e/1,
    t_unsubscribe/1,
    t_session_gc/1,
    t_per_session_limit/1,
    t_live_only_mode/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [t_templates_list_read, t_live_e2e, t_unsubscribe, t_session_gc,
     t_per_session_limit, t_live_only_mode].

init_per_suite(Config) ->
    application:load(barrel_server),
    application:set_env(barrel_server, data_dir, ?config(priv_dir, Config)),
    application:set_env(barrel_server, http_port, 0),
    application:set_env(barrel_server, open_opts,
                        #{vectordb => #{dimension => 3,
                                        bm25_backend => memory}}),
    {ok, _} = application:ensure_all_started(barrel_server),
    {ok, _} = application:ensure_all_started(hackney),
    Config.

end_per_suite(_Config) ->
    application:stop(barrel_server),
    application:unset_env(barrel_server, open_opts),
    ok.

%%====================================================================
%% Cases
%%====================================================================

t_templates_list_read(_Config) ->
    C = connect(),
    {ok, Templates} = barrel_mcp_client:list_resource_templates(C),
    Uris = [maps:get(<<"uriTemplate">>, T) || T <- Templates],
    ?assertEqual(lists:sort([<<"barrel://db/{db}">>,
                             <<"barrel://db/{db}/doc/{id}">>,
                             <<"barrel://db/{db}/live/{sub}">>]),
                 lists:sort(Uris)),
    {false, _} = call(C, <<"db_create">>, #{<<"db">> => <<"res_db">>}),
    {false, _} = call(C, <<"doc_put">>,
                      #{<<"db">> => <<"res_db">>,
                        <<"doc">> => #{<<"id">> => <<"r1">>,
                                       <<"v">> => 7}}),
    Info = read(C, <<"barrel://db/res_db">>),
    ?assert(is_map(Info)),
    Doc = read(C, <<"barrel://db/res_db/doc/r1">>),
    ?assertEqual(7, maps:get(<<"v">>, Doc)),
    ?assertMatch(#{<<"error">> := <<"not_found">>},
                 read(C, <<"barrel://db/res_db/doc/missing">>)),
    barrel_mcp_client:close(C),
    ok.

t_live_e2e(_Config) ->
    C = connect(),
    {false, _} = call(C, <<"db_create">>, #{<<"db">> => <<"live_db">>}),
    {false, #{<<"sub">> := _Sub, <<"uri">> := Uri}} =
        call(C, <<"query_subscribe">>,
             #{<<"db">> => <<"live_db">>,
               <<"query">> =>
                   <<"SELECT * FROM db WHERE kind = 'live' SUBSCRIBE">>}),
    {ok, _} = barrel_mcp_client:subscribe(C, Uri),
    ok = wait_ready(C, Uri, 50),
    %% a matching write flows: notification then re-read
    {false, _} = call(C, <<"doc_put">>,
                      #{<<"db">> => <<"live_db">>,
                        <<"doc">> => #{<<"id">> => <<"l1">>,
                                       <<"kind">> => <<"live">>}}),
    ok = wait_updated(Uri),
    #{<<"rows">> := [Row]} = read(C, Uri),
    ?assertEqual(<<"l1">>, maps:get(<<"id">>, Row)),
    %% a non-matching write does not appear in the rows
    {false, _} = call(C, <<"doc_put">>,
                      #{<<"db">> => <<"live_db">>,
                        <<"doc">> => #{<<"id">> => <<"other">>,
                                       <<"kind">> => <<"cold">>}}),
    %% deletion flows too
    {false, _} = call(C, <<"doc_delete">>, #{<<"db">> => <<"live_db">>,
                                             <<"id">> => <<"l1">>}),
    ok = wait_updated(Uri),
    ?assertMatch(#{<<"rows">> := [], <<"count">> := 0}, read(C, Uri)),
    barrel_mcp_client:close(C),
    ok.

t_unsubscribe(_Config) ->
    C = connect(),
    {false, _} = call(C, <<"db_create">>, #{<<"db">> => <<"unsub_db">>}),
    {false, #{<<"sub">> := Sub, <<"uri">> := Uri}} =
        call(C, <<"query_subscribe">>,
             #{<<"db">> => <<"unsub_db">>,
               <<"query">> =>
                   <<"SELECT * FROM db WHERE kind = 'u' SUBSCRIBE">>}),
    ok = wait_ready(C, Uri, 50),
    {false, #{<<"ok">> := true}} =
        call(C, <<"query_unsubscribe">>, #{<<"sub">> => Sub}),
    ?assertMatch(#{<<"error">> := <<"not_found">>}, read(C, Uri)),
    {true, #{<<"error">> := <<"not_found">>}} =
        call(C, <<"query_unsubscribe">>, #{<<"sub">> => Sub}),
    %% a plain SELECT is pushed toward query_subscribe's dual
    {true, #{<<"error">> := <<"missing_subscribe">>}} =
        call(C, <<"query_subscribe">>,
             #{<<"db">> => <<"unsub_db">>,
               <<"query">> => <<"SELECT * FROM db WHERE kind = 'u'">>}),
    barrel_mcp_client:close(C),
    ok.

t_session_gc(_Config) ->
    C = connect(),
    {false, _} = call(C, <<"db_create">>, #{<<"db">> => <<"gc_db">>}),
    {false, #{<<"sub">> := Sub}} =
        call(C, <<"query_subscribe">>,
             #{<<"db">> => <<"gc_db">>,
               <<"query">> =>
                   <<"SELECT * FROM db WHERE kind = 'g' SUBSCRIBE">>}),
    {ok, _} = barrel_server_mcp_live:snapshot(Sub),
    %% closing the client DELETEs its MCP session; the bridge sweep
    %% then drops the orphaned live query
    barrel_mcp_client:close(C),
    ok = wait_swept(Sub, 50),
    ?assertEqual({error, not_found}, barrel_server_mcp_live:snapshot(Sub)),
    ok.

t_per_session_limit(_Config) ->
    application:set_env(barrel_server, mcp,
                        #{live => #{max_per_session => 2}}),
    C = connect(),
    try
        {false, _} = call(C, <<"db_create">>, #{<<"db">> => <<"cap_db">>}),
        Q = <<"SELECT * FROM db WHERE kind = 'c' SUBSCRIBE">>,
        {false, #{<<"sub">> := S1}} =
            call(C, <<"query_subscribe">>, #{<<"db">> => <<"cap_db">>,
                                             <<"query">> => Q}),
        {false, #{<<"sub">> := S2}} =
            call(C, <<"query_subscribe">>, #{<<"db">> => <<"cap_db">>,
                                             <<"query">> => Q}),
        {true, #{<<"error">> := <<"too_many_live_queries">>}} =
            call(C, <<"query_subscribe">>, #{<<"db">> => <<"cap_db">>,
                                             <<"query">> => Q}),
        {false, _} = call(C, <<"query_unsubscribe">>, #{<<"sub">> => S1}),
        {false, #{<<"sub">> := S3}} =
            call(C, <<"query_subscribe">>, #{<<"db">> => <<"cap_db">>,
                                             <<"query">> => Q}),
        {false, _} = call(C, <<"query_unsubscribe">>, #{<<"sub">> => S2}),
        {false, _} = call(C, <<"query_unsubscribe">>, #{<<"sub">> => S3})
    after
        application:unset_env(barrel_server, mcp),
        barrel_mcp_client:close(C)
    end,
    ok.

t_live_only_mode(_Config) ->
    application:set_env(barrel_server, mcp, #{resources => live_only}),
    try
        restart_registrar(),
        C = connect(),
        {ok, Templates} = barrel_mcp_client:list_resource_templates(C),
        ?assertEqual([<<"barrel://db/{db}/live/{sub}">>],
                     [maps:get(<<"uriTemplate">>, T) || T <- Templates]),
        barrel_mcp_client:close(C)
    after
        application:unset_env(barrel_server, mcp),
        restart_registrar()
    end,
    ok.

%%====================================================================
%% Helpers
%%====================================================================

base_url() ->
    Children = supervisor:which_children(barrel_server_sup),
    {_, Pid, _, _} = lists:keyfind(barrel_server_http, 1, Children),
    #{h1 := Port} = livery:which_listeners(Pid),
    "http://127.0.0.1:" ++ integer_to_list(Port).

restart_registrar() ->
    ok = supervisor:terminate_child(barrel_server_sup, barrel_server_mcp),
    {ok, _} = supervisor:restart_child(barrel_server_sup,
                                       barrel_server_mcp),
    ok.

connect() ->
    Base = base_url(),
    {ok, C} = barrel_mcp_client:start(#{
        transport => {http, list_to_binary(Base ++ "/mcp")}
    }),
    ok = wait_client_ready(C, 100),
    C.

wait_client_ready(_C, 0) ->
    {error, not_ready};
wait_client_ready(C, N) ->
    case barrel_mcp_client:ping(C) of
        {ok, _} -> ok;
        {error, not_ready} ->
            timer:sleep(50),
            wait_client_ready(C, N - 1);
        {error, _} = Err ->
            Err
    end.

call(C, Name, Args) ->
    {ok, Res} = barrel_mcp_client:call_tool(C, Name, Args),
    IsErr = maps:get(<<"isError">>, Res, false),
    Data = case maps:get(<<"content">>, Res, []) of
        [#{<<"type">> := <<"text">>, <<"text">> := Text} | _] ->
            try json:decode(Text) catch _:_ -> Text end;
        Other ->
            Other
    end,
    {IsErr, Data}.

read(C, Uri) ->
    {ok, #{<<"contents">> := [#{<<"text">> := Text} | _]}} =
        barrel_mcp_client:read_resource(C, Uri),
    json:decode(Text).

wait_ready(_C, _Uri, 0) ->
    {error, never_ready};
wait_ready(C, Uri, N) ->
    case read(C, Uri) of
        #{<<"ready">> := true} -> ok;
        _ -> timer:sleep(50), wait_ready(C, Uri, N - 1)
    end.

wait_updated(Uri) ->
    receive
        {mcp_resource_updated, Uri, _Params} -> ok
    after 5000 ->
        {error, {no_update_notification, Uri}}
    end.

%% the client's session DELETE is asynchronous to close/1 returning:
%% sweep until this sub is really gone
wait_swept(Sub, N) ->
    {ok, _} = barrel_server_mcp_live:sweep(),
    case barrel_server_mcp_live:snapshot(Sub) of
        {error, not_found} ->
            ok;
        {ok, _} when N > 0 ->
            timer:sleep(100),
            wait_swept(Sub, N - 1);
        {ok, _} ->
            {error, never_swept}
    end.
