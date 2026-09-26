%%%-------------------------------------------------------------------
%%% @doc Remote query client (B5) against a scripted HTTP server: rows
%%% only with a final meta, failures classified, no rows kept on failure.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ctx_remote_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         end_per_testcase/2]).
-export([rows_and_meta/1,
         request_shape_and_credentials/1,
         closed_port_unreachable/1,
         stall_times_out/1,
         error_mid_stream/1,
         missing_meta/1,
         data_after_meta/1,
         byte_cap/1,
         http_statuses/1,
         node_slots/1,
         no_connection_leak/1]).

all() ->
    [rows_and_meta, request_shape_and_credentials, closed_port_unreachable,
     stall_times_out, error_mid_stream, missing_meta, data_after_meta,
     byte_cap, http_statuses, node_slots, no_connection_leak].

init_per_suite(Config) ->
    application:load(barrel_docdb),
    application:set_env(barrel_docdb, data_dir, ?config(priv_dir, Config)),
    {ok, _} = application:ensure_all_started(barrel),
    Config.

end_per_suite(_Config) ->
    ok.

end_per_testcase(_TC, _Config) ->
    application:unset_env(barrel, ctx_credentials),
    application:unset_env(barrel, ctx_node_remote_max),
    ok.

row(I) ->
    [json:encode(#{row => #{id => integer_to_binary(I), n => I}}), $\n].

meta() ->
    [json:encode(#{meta => #{has_more => false, bound => <<"exhausted">>,
                             instance_id => <<"abcd">>,
                             last_seq => <<"AAAAAAAAAAAAAAAA">>}}), $\n].

loc(Server) ->
    #{endpoint => endpoint(barrel_ctx_fake_server:port(Server)),
      db => <<"src">>}.

endpoint(Port) ->
    <<"http://127.0.0.1:", (integer_to_binary(Port))/binary>>.

with_server(Script, Fun) ->
    Server = barrel_ctx_fake_server:start(Script),
    try Fun(Server) after barrel_ctx_fake_server:stop(Server) end.

rows_and_meta(_Config) ->
    %% rows split across chunks, a line cut mid-way included
    Body = iolist_to_binary([row(1), row(2), row(3), meta()]),
    <<A:10/binary, B/binary>> = Body,
    with_server({chunks, 200, [A, B]}, fun(S) ->
        {ok, Rows, Meta} = barrel_ctx_remote:query(loc(S), <<"q">>, #{}),
        ?assertEqual([<<"1">>, <<"2">>, <<"3">>],
                     [maps:get(<<"id">>, R) || R <- Rows]),
        ?assertMatch(#{has_more := false, bound := <<"exhausted">>,
                       instance_id := <<"abcd">>,
                       last_seq := <<"AAAAAAAAAAAAAAAA">>}, Meta),
        ?assertEqual(byte_size(Body), maps:get(bytes, Meta))
    end).

request_shape_and_credentials(_Config) ->
    with_server({chunks, 200, [meta()]}, fun(S) ->
        #{endpoint := Endpoint} = Loc = loc(S),
        application:set_env(barrel, ctx_credentials,
                            #{<<Endpoint/binary, "/">> => <<"tok1">>,
                              "named" => "tok2"}),
        {ok, [], _} = barrel_ctx_remote:query(
            Loc, <<"SELECT * FROM c LIMIT 2">>,
            #{max_rows => 2, timeout => 1500, params => #{<<"k">> => 1}}),
        receive
            {fake_request, Headers, Body} ->
                ?assertEqual(<<"Bearer tok1">>,
                             maps:get(<<"authorization">>, Headers)),
                ?assertEqual(#{<<"query">> => <<"SELECT * FROM c LIMIT 2">>,
                               <<"max_rows">> => 2,
                               <<"deadline_ms">> => 1500,
                               <<"params">> => #{<<"k">> => 1}},
                             json:decode(Body))
        after 1000 -> ct:fail(no_request)
        end,
        {ok, [], _} = barrel_ctx_remote:query(
            Loc, <<"q">>, #{credential_ref => <<"named">>}),
        receive
            {fake_request, H2, _} ->
                ?assertEqual(<<"Bearer tok2">>,
                             maps:get(<<"authorization">>, H2))
        after 1000 -> ct:fail(no_request)
        end
    end).

closed_port_unreachable(_Config) ->
    Loc = #{endpoint => endpoint(barrel_ctx_fake_server:closed_port()),
            db => <<"src">>},
    ?assertMatch({error, #{status := unreachable, rows_received := 0}},
                 barrel_ctx_remote:query(Loc, <<"q">>, #{timeout => 1000})).

%% The server never answers, so returning at all proves the deadline
%% fired; no upper bound on wall time.
stall_times_out(_Config) ->
    with_server(stall, fun(S) ->
        T0 = erlang:monotonic_time(millisecond),
        Result = barrel_ctx_remote:query(loc(S), <<"q">>, #{timeout => 300}),
        Elapsed = erlang:monotonic_time(millisecond) - T0,
        ?assertMatch({error, #{status := timeout, reason := deadline,
                               after_ms := 300, rows_received := 0}}, Result),
        ?assert(Elapsed >= 300),
        %% the client closed its connection and left no late message
        ok = await_closed(),
        receive {hackney_response, _, _} = M -> ct:fail({leftover, M})
        after 0 -> ok
        end
    end),
    %% a stall after some rows: rows dropped, count reported (the budget
    %% only has to cover delivering two rows over loopback)
    with_server({chunks, 200, [row(1), row(2), stall]}, fun(S) ->
        ?assertMatch({error, #{status := timeout, reason := deadline,
                               after_ms := 2000, rows_received := 2}},
                     barrel_ctx_remote:query(loc(S), <<"q">>,
                                             #{timeout => 2000})),
        ok = await_closed()
    end).

error_mid_stream(_Config) ->
    Err = [json:encode(#{error => <<"boom">>}), $\n],
    with_server({chunks, 200, [row(1), row(2), Err]}, fun(S) ->
        ?assertMatch({error, #{status := error,
                               reason := {remote_error, <<"boom">>},
                               rows_received := 2}},
                     barrel_ctx_remote:query(loc(S), <<"q">>, #{}))
    end).

missing_meta(_Config) ->
    with_server({chunks, 200, [row(1), row(2)]}, fun(S) ->
        ?assertMatch({error, #{status := error, reason := missing_meta,
                               rows_received := 2}},
                     barrel_ctx_remote:query(loc(S), <<"q">>, #{}))
    end).

data_after_meta(_Config) ->
    with_server({chunks, 200, [row(1), meta(), row(2)]}, fun(S) ->
        ?assertMatch({error, #{status := error, reason := data_after_meta}},
                     barrel_ctx_remote:query(loc(S), <<"q">>, #{}))
    end).

byte_cap(_Config) ->
    Rows = [row(I) || I <- lists:seq(1, 50)],
    with_server({chunks, 200, Rows ++ [meta()]}, fun(S) ->
        ?assertMatch({error, #{status := error,
                               reason := response_too_large}},
                     barrel_ctx_remote:query(loc(S), <<"q">>,
                                             #{max_bytes => 200}))
    end).

http_statuses(_Config) ->
    Unauth = iolist_to_binary(json:encode(#{error => <<"unauthorized">>})),
    with_server({chunks, 401, [Unauth]}, fun(S) ->
        ?assertMatch({error, #{status := unauthorized,
                               reason := {http_status, 401,
                                          <<"unauthorized">>}}},
                     barrel_ctx_remote:query(loc(S), <<"q">>, #{}))
    end),
    Bad = iolist_to_binary(json:encode(#{error => <<"invalid_query">>})),
    with_server({chunks, 400, [Bad]}, fun(S) ->
        ?assertMatch({error, #{status := error,
                               reason := {http_status, 400,
                                          <<"invalid_query">>}}},
                     barrel_ctx_remote:query(loc(S), <<"q">>, #{}))
    end).

node_slots(_Config) ->
    application:set_env(barrel, ctx_node_remote_max, 2),
    Before = barrel_ctx_remote:slots_in_use(),
    ok = barrel_ctx_remote:acquire_slot(),
    ok = barrel_ctx_remote:acquire_slot(),
    ?assertEqual({error, busy}, barrel_ctx_remote:acquire_slot()),
    ok = barrel_ctx_remote:release_slot(),
    ok = barrel_ctx_remote:acquire_slot(),
    ok = barrel_ctx_remote:release_slot(),
    ok = barrel_ctx_remote:release_slot(),
    ?assertEqual(Before, barrel_ctx_remote:slots_in_use()).

%% Every path closes its connection: the server sees each one closed
%% and no hackney connection process is left alive.
no_connection_leak(_Config) ->
    Before = live_conns(),
    with_server({chunks, 200, [row(1), meta()]}, fun(S) ->
        [begin
             {ok, _, _} = barrel_ctx_remote:query(loc(S), <<"q">>, #{}),
             ok = await_closed()
         end || _ <- lists:seq(1, 5)]
    end),
    with_server(stall, fun(S) ->
        [ok = stall_closed(barrel_ctx_remote:query(loc(S), <<"q">>,
                                                    #{timeout => 50}))
         || _ <- lists:seq(1, 3)]
    end),
    ?assertEqual(Before, live_conns()).

%% A request that was sent is seen closed by the server; a 50 ms budget
%% may also run out while connecting.
stall_closed({error, #{status := timeout, reason := deadline}}) ->
    await_closed();
stall_closed({error, #{status := timeout, reason := connect_timeout}}) ->
    ok.

%% hackney:close/1 stops the connection synchronously; a child the
%% supervisor has not reaped yet is already dead.
live_conns() ->
    lists:sort([P || {_, P, _, _} <- supervisor:which_children(hackney_conn_sup),
                     is_pid(P), is_process_alive(P)]).

%% The fake server reports a connection once the client closed it; the
%% bound only guards against a hang.
await_closed() ->
    receive {fake_closed, _} -> ok
    after 10000 -> {error, connection_left_open}
    end.
