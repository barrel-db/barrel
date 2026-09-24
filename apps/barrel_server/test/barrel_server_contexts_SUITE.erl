%%%-------------------------------------------------------------------
%%% @doc Contexts over the wire (B5 against a live server, B7 REST and
%%% MCP, M1 gate): one local and two remote members (this server reached
%%% through a TCP proxy that injects latency, stalls or truncation).
%%% One OTP application per context, from a small module corpus.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_contexts_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, groups/0, init_per_suite/1, end_per_suite/1,
         init_per_group/2, end_per_group/2]).
-export([t_cards_rest/1,
         t_remote_client_matches_local/1,
         t_union_oracle/1,
         t_retrieval_grouped/1,
         t_member_failures/1,
         t_rejected_shapes_rest/1,
         t_mcp_matches_rest/1,
         t_no_leases_no_copies/1,
         t_capability_members/1,
         t_register_needs_global/1,
         t_worksets_rest/1,
         t_worksets_mcp/1,
         t_fingerprint_meta/1,
         t_worksets_need_global/1,
         t_error_catalog/1,
         t_names/1,
         t_summaries/1,
         t_capabilities/1,
         t_worksets_cleanup/1]).

-define(APPS, [<<"et">>, <<"ftp">>, <<"os_mon">>]).
-define(ROOT, <<"ctx-root-token">>).
-define(ORDERED, <<"SELECT id, path, lines FROM c WHERE lines > 50 "
                   "ORDER BY lines DESC LIMIT 12">>).

all() ->
    [{group, open}, {group, locked}].

groups() ->
    [{open, [sequence],
      [t_cards_rest, t_remote_client_matches_local, t_union_oracle,
       t_retrieval_grouped, t_member_failures, t_rejected_shapes_rest,
       t_mcp_matches_rest, t_no_leases_no_copies, t_worksets_rest,
       t_worksets_mcp, t_fingerprint_meta, t_error_catalog, t_names,
       t_summaries, t_capabilities, t_worksets_cleanup]},
     {locked, [sequence], [t_capability_members, t_register_needs_global,
                           t_worksets_need_global]}].

%%====================================================================
%% Setup
%%====================================================================

init_per_suite(Config) ->
    application:load(barrel_server),
    application:set_env(barrel_server, data_dir, ?config(priv_dir, Config)),
    application:set_env(barrel_server, http_port, 0),
    application:set_env(barrel_server, open_opts,
                        #{vectordb => #{dimension => 3,
                                        bm25_backend => memory}}),
    {ok, _} = application:ensure_all_started(barrel_server),
    {ok, _} = application:ensure_all_started(hackney),
    mock_embed(),
    Docs = load_corpus(filename:join(?config(data_dir, Config),
                                     "otp_small.jsonl")),
    Port = port(),
    Proxy = barrel_ctx_delay_proxy:start(Port, {delay, 10}),
    [{_, EtDb}, {_, FtpDb}, {_, OsDb}] = Dbs =
        [{App, seed(<<"srvctx_", App/binary>>,
                    [D || #{<<"app">> := A} = D <- Docs, A =:= App])}
         || App <- ?APPS],
    _ = seed(<<"srvctx_union">>,
             [D || #{<<"app">> := A} = D <- Docs, lists:member(A, ?APPS)]),
    Via = endpoint(barrel_ctx_delay_proxy:port(Proxy)),
    Local = register_card(<<"otp/et">>, local(EtDb)),
    Ftp = register_card(<<"otp/ftp">>, remote(Via, FtpDb)),
    Os = register_card(<<"otp/os_mon">>, remote(Via, OsDb)),
    [{port, Port}, {proxy, Proxy}, {dbs, Dbs},
     {ctxs, [Local, Ftp, Os]} | Config].

end_per_suite(Config) ->
    barrel_ctx_delay_proxy:stop(?config(proxy, Config)),
    try meck:unload(barrel_embed) catch _:_ -> ok end,
    application:stop(barrel_server),
    application:unset_env(barrel_server, open_opts),
    ok.

init_per_group(locked, Config) ->
    application:set_env(barrel_server, auth, #{tokens => [?ROOT]}),
    restart_http(),
    %% the remote legs now need the server token
    Port = port(),
    Proxy = barrel_ctx_delay_proxy:start(Port, {delay, 0}),
    application:set_env(barrel, ctx_credentials,
                        #{endpoint(barrel_ctx_delay_proxy:port(Proxy)) =>
                              ?ROOT}),
    [{locked_proxy, Proxy}, {port, Port} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(locked, Config) ->
    application:unset_env(barrel_server, auth),
    application:unset_env(barrel, ctx_credentials),
    barrel_ctx_delay_proxy:stop(?config(locked_proxy, Config)),
    restart_http(),
    ok;
end_per_group(_Group, _Config) ->
    ok.

mock_embed() ->
    _ = try meck:unload(barrel_embed) catch _:_ -> ok end,
    meck:new(barrel_embed, [passthrough, no_link]),
    %% the text <<"boom">> fails, to raise an error after the 200
    meck:expect(barrel_embed, embed,
                fun(<<"boom">>, _State) -> {error, boom};
                   (Text, _State) -> {ok, mock_vec(Text)}
                end),
    meck:expect(barrel_embed, embed_batch,
                fun(Texts, _State) -> {ok, [mock_vec(T) || T <- Texts]} end).

mock_vec(Text) ->
    Hash = erlang:phash2(Text, 1000000),
    [Hash / 1000000.0, (Hash rem 1000) / 1000.0, (Hash rem 100) / 100.0].

load_corpus(Path) ->
    {ok, Bin} = file:read_file(Path),
    [json:decode(L) || L <- binary:split(Bin, <<"\n">>, [global]), L =/= <<>>].

%% Record-mode db opened in-VM first (so the server reuses the handle),
%% loaded over HTTP.
seed(Db, Docs) ->
    {ok, _} = barrel_server_dbs:ensure(Db, #{
        embedding => #{fields => [<<"moduledoc">>], mode => sync}}),
    ok = barrel_dbs:pin(Db),
    {201, _} = http(post, db_url(Db, "/_bulk_docs"), [],
                    #{<<"docs">> => Docs}),
    Db.

register_card(Name, Location) ->
    {ok, #{<<"id">> := Id}} = barrel_ctx:register(
        #{<<"name">> => Name, <<"locations">> => [Location]}),
    Id.

local(Db) -> #{<<"kind">> => <<"local">>, <<"db">> => Db}.

remote(Endpoint, Db) ->
    #{<<"kind">> => <<"remote">>, <<"endpoint">> => Endpoint, <<"db">> => Db}.

%%====================================================================
%% Open group
%%====================================================================

t_cards_rest(_Config) ->
    Card = #{<<"name">> => <<"rest/card">>, <<"title">> => <<"t">>,
             <<"locations">> => [local(<<"srvctx_et">>)]},
    {201, #{<<"id">> := Id} = Created} = http(post, "/contexts", [], Card),
    ?assertMatch(<<"ctx_", _/binary>>, Id),
    {200, Created} = http(get, "/contexts/" ++ binary_to_list(Id), [], none),
    {200, #{<<"contexts">> := Cards}} = http(get, "/contexts", [], none),
    ?assert(lists:member(Created, Cards)),
    {200, #{<<"contexts">> := [#{<<"name">> := <<"rest/card">>}]}} =
        http(get, "/contexts?prefix=rest/", [], none),
    {404, #{<<"error">> := <<"unknown_context">>}} =
        http(get, "/contexts/ctx_nope", [], none),
    {400, #{<<"error">> := <<"invalid_card">>,
            <<"details">> := #{<<"reason">> := <<"credential_field">>}}} =
        http(post, "/contexts", [], Card#{<<"token">> => <<"x">>}),
    {409, #{<<"error">> := <<"already_exists">>}} =
        http(post, "/contexts", [], Card#{<<"id">> => Id}),
    ok = barrel_ctx_catalog:unregister(Id).

%% B5 acceptance: the client against a live server returns the rows a
%% local query returns on the same data.
t_remote_client_matches_local(Config) ->
    Stmt = <<"SELECT id, path FROM c WHERE lines > 100 ORDER BY path "
             "LIMIT 20">>,
    Loc = #{endpoint => endpoint(?config(port, Config)),
            db => <<"srvctx_union">>},
    {ok, Rows, Meta} = barrel_ctx_remote:query(Loc, Stmt, #{}),
    {ok, Db} = barrel_dbs:ensure(<<"srvctx_union">>),
    {ok, Local, #{instance_id := Iid}} = barrel:query(Db, Stmt),
    ?assertEqual(Local, Rows),
    ?assertMatch(#{instance_id := Iid, last_seq := <<_/binary>>,
                   bound := <<"exhausted">>}, Meta).

t_union_oracle(Config) ->
    Stmts = [?ORDERED,
             <<"SELECT * FROM c ORDER BY size LIMIT 9">>,
             <<"SELECT path AS p, lines FROM c WHERE size > 5000 "
               "ORDER BY path DESC LIMIT 15">>],
    lists:foreach(
        fun(Stmt) ->
            {200, #{<<"execution">> := <<"succeeded">>,
                    <<"rows">> := Rows, <<"sources">> := Sources}} =
                ctx_query(Config, #{<<"query">> => Stmt}),
            {200, Body} = raw(post, db_url(<<"srvctx_union">>, "/query"), [],
                              #{<<"query">> => Stmt}),
            Expected = [R || #{<<"row">> := R} <- ndjson(Body)],
            ?assertEqual({Stmt, Expected},
                         {Stmt, [maps:without([<<"_ctx">>, <<"_ctx_name">>], R)
                                 || R <- Rows]}),
            ?assertMatch([#{<<"location">> := #{<<"kind">> := <<"local">>}},
                          #{<<"location">> := #{<<"kind">> := <<"remote">>}},
                          #{<<"location">> := #{<<"kind">> := <<"remote">>}}],
                         Sources)
        end, Stmts).

t_retrieval_grouped(Config) ->
    {200, #{<<"merge">> := <<"grouped">>, <<"groups">> := Groups,
            <<"sources">> := Sources, <<"coverage">> := Cov}} =
        ctx_query(Config, #{<<"query">> =>
                                <<"SELECT b.id, b._score FROM "
                                  "bm25_top_k('event trace', k => 3) AS b">>}),
    ?assertEqual(?config(ctxs, Config), [C || #{<<"context">> := C} <- Groups]),
    lists:foreach(
        fun(#{<<"status">> := <<"ok">>, <<"retrieval">> := <<"exact">>,
              <<"version">> := #{<<"kind">> := <<"live">>,
                                 <<"observed">> := #{<<"instance_id">> := I,
                                                     <<"last_seq">> := S}}}) ->
            ?assert(is_binary(I) andalso is_binary(S))
        end, Sources),
    ?assertMatch(#{<<"requested">> := 3, <<"answered">> := 3}, Cov).

%% Closed port, stall, in-band error and a stream cut mid-way: each is
%% reported with its status and no rows kept. The stalled proxy never
%% answers: the response arriving proves it was not waited for.
t_member_failures(Config) ->
    Port = ?config(port, Config),
    Stall = barrel_ctx_delay_proxy:start(Port, stall),
    Cut = barrel_ctx_delay_proxy:start(Port, {truncate, 400}),
    [Local | _] = ?config(ctxs, Config),
    Closed = register_card(<<"f/closed">>,
        remote(endpoint(closed_port()), <<"srvctx_ftp">>)),
    Stalled = register_card(<<"f/stall">>,
        remote(endpoint(barrel_ctx_delay_proxy:port(Stall)),
               <<"srvctx_ftp">>)),
    Truncated = register_card(<<"f/cut">>,
        remote(endpoint(barrel_ctx_delay_proxy:port(Cut)),
               <<"srvctx_union">>)),
    %% the member budget must cover the members that do answer; the
    %% global deadline is far enough not to cut it
    Budget = 1000,
    {200, R} = ctx_query(Config, #{
        <<"query">> => <<"SELECT * FROM c ORDER BY lines LIMIT 30">>,
        <<"contexts">> => [Local, Closed, Stalled, Truncated],
        <<"per_context_timeout_ms">> => Budget,
        <<"deadline_ms">> => 30000}),
    #{<<"execution">> := <<"partial">>, <<"rows">> := Rows,
      <<"sources">> := [S1, S2, S3, S4], <<"coverage">> := Cov} = R,
    ?assertMatch(#{<<"status">> := <<"ok">>}, S1),
    ?assertMatch(#{<<"status">> := <<"unreachable">>, <<"rows">> := 0}, S2),
    ?assertMatch(#{<<"status">> := <<"timeout">>, <<"rows">> := 0,
                   <<"error">> := #{<<"after_ms">> := Budget}}, S3),
    ?assert(maps:get(<<"elapsed_ms">>, S3) >= Budget),
    ?assertMatch(#{<<"status">> := <<"error">>, <<"rows">> := 0,
                   <<"error">> := #{<<"reason">> := <<"missing_meta">>}}, S4),
    ?assertEqual([Local], lists:usort([C || #{<<"_ctx">> := C} <- Rows])),
    ?assertMatch(#{<<"answered">> := 1, <<"failed">> := 3}, Cov),
    %% an error line from the remote server after its 200
    [_, Ftp, _] = ?config(ctxs, Config),
    {200, R2} = ctx_query(Config, #{
        <<"query">> => <<"SELECT * FROM vector_top_k('boom', k => 2) AS v">>,
        <<"contexts">> => [Ftp]}),
    ?assertMatch(#{<<"execution">> := <<"failed">>,
                   <<"sources">> := [#{<<"status">> := <<"error">>,
                                       <<"error">> := #{<<"reason">> := _,
                                                        <<"message">> := _,
                                                        <<"hint">> := _}}]},
                 R2),
    barrel_ctx_delay_proxy:stop(Stall),
    barrel_ctx_delay_proxy:stop(Cut).

t_rejected_shapes_rest(Config) ->
    Cases = [{<<"SELECT * FROM c">>, <<"limit_required">>},
             {<<"SELECT * FROM c ORDER BY path">>, <<"limit_required">>},
             {<<"SELECT * FROM c LIMIT 2000">>, <<"limit_too_large">>},
             {<<"SELECT id FROM c ORDER BY path LIMIT 3">>,
              <<"order_key_not_projected">>},
             {<<"SELECT * FROM c ORDER BY path LIMIT 3 OFFSET 1">>,
              <<"offset">>},
             {<<"SELECT * FROM c WHERE a = 1 SUBSCRIBE">>, <<"subscribe">>}],
    lists:foreach(
        fun({Stmt, Reason}) ->
            {400, #{<<"error">> := Code, <<"details">> := D}} =
                ctx_query(Config, #{<<"query">> => Stmt}),
            ?assertEqual({Stmt, Reason},
                         {Stmt, case Code of
                                    <<"unsupported_federated_query">> ->
                                        maps:get(<<"reason">>, D);
                                    _ -> Code
                                end})
        end, Cases),
    {400, #{<<"error">> := <<"merge_not_allowed">>,
            <<"details">> := #{<<"merge">> := <<"score">>}}} =
        ctx_query(Config, #{<<"query">> =>
                                <<"SELECT * FROM bm25_top_k('x', k => 2) "
                                  "AS b">>,
                            <<"merge">> => <<"score">>}),
    {400, #{<<"error">> := <<"merge_not_supported">>,
            <<"details">> := #{<<"merge">> := <<"rrf">>}}} =
        ctx_query(Config, #{<<"query">> =>
                                <<"SELECT * FROM hybrid_top_k('x', k => 2) "
                                  "AS h">>,
                            <<"merge">> => <<"rrf">>}),
    {400, #{<<"error">> := <<"invalid_query">>}} =
        ctx_query(Config, #{<<"query">> => <<"SELECT FROM">>}),
    {404, #{<<"error">> := <<"unknown_context">>}} =
        ctx_query(Config, #{<<"query">> => <<"SELECT * FROM c LIMIT 1">>,
                            <<"contexts">> => [<<"ctx_nope">>]}),
    {400, #{<<"error">> := <<"invalid_argument">>,
            <<"details">> := #{<<"field">> := <<"merge">>}}} =
        ctx_query(Config, #{<<"query">> => <<"SELECT * FROM c LIMIT 1">>,
                            <<"merge">> => <<"best">>}).

t_mcp_matches_rest(Config) ->
    C = connect(#{}),
    {false, #{<<"contexts">> := Cards}} = call(C, <<"context_list">>, #{}),
    {200, #{<<"contexts">> := Cards}} = http(get, "/contexts", [], none),
    [Id | _] = ?config(ctxs, Config),
    {false, Card} = call(C, <<"context_inspect">>, #{<<"context">> => Id}),
    {200, Card} = http(get, "/contexts/" ++ binary_to_list(Id), [], none),
    Req = #{<<"query">> => ?ORDERED, <<"contexts">> => ?config(ctxs, Config)},
    {false, ViaMcp} = call(C, <<"context_query">>, Req),
    {200, ViaRest} = ctx_query(Config, Req),
    ?assertEqual(strip_timing(ViaRest), strip_timing(ViaMcp)),
    {true, #{<<"error">> := <<"limit_required">>}} =
        call(C, <<"context_query">>, Req#{<<"query">> => <<"SELECT * FROM c">>}),
    barrel_mcp_client:close(C).

%% M1 gate: no db left leased, and remote rows never land on disk here.
t_no_leases_no_copies(Config) ->
    Before = barrel_docdb:list_dbs(),
    Files = data_files(Config),
    [{200, _} = ctx_query(Config, #{<<"query">> => ?ORDERED})
     || _ <- lists:seq(1, 5)],
    ?assertEqual(#{}, barrel_dbs:leases()),
    ?assertEqual(0, barrel_ctx_remote:slots_in_use()),
    ?assertEqual(lists:sort(Before), lists:sort(barrel_docdb:list_dbs())),
    ?assertEqual(Files, data_files(Config)).

%% Working sets over REST: attach a local and a remote member, save a
%% slice of a third context, then answer offline from local copies.
t_worksets_rest(Config) ->
    [Local, Ftp, Os] = ?config(ctxs, Config),
    {201, #{<<"id">> := Ws, <<"members">> := []}} =
        http(post, "/worksets", [], #{<<"owner">> => <<"session:rest">>}),
    WsPath = "/worksets/" ++ binary_to_list(Ws),
    {200, #{<<"members">> := [#{<<"mode">> := <<"local">>}]}} =
        http(post, WsPath ++ "/members", [], #{<<"context">> => Local}),
    {200, #{<<"members">> := [_, #{<<"mode">> := <<"remote">>,
                                   <<"membership">> := <<"live">>,
                                   <<"answers_offline">> := false}]}} =
        http(post, WsPath ++ "/members", [], #{<<"context">> => Ftp}),
    {409, #{<<"error">> := <<"already_attached">>}} =
        http(post, WsPath ++ "/members", [], #{<<"context">> => Local}),
    {200, #{<<"slices">> := [#{<<"context">> := Os,
                               <<"status">> := <<"complete">>,
                               <<"docs">> := Docs,
                               <<"derived">> := #{<<"observed">> := Obs}}]}} =
        http(post, WsPath ++ "/_materialize", [],
             #{<<"from_query">> =>
                   #{<<"query">> => <<"SELECT b.id FROM bm25_top_k("
                                      "'event trace', k => 3) AS b">>,
                     <<"contexts">> => [Os]},
               <<"include">> => #{<<"embeddings">> => false}}),
    ?assert(Docs > 0),
    ?assertMatch(#{<<"instance_id">> := I} when is_binary(I), Obs),
    Q = #{<<"query">> => <<"SELECT id, lines FROM c ORDER BY lines DESC "
                           "LIMIT 40">>,
          <<"working_set">> => Ws},
    {200, #{<<"offline">> := true}} =
        http(put, "/contexts/_offline", [], #{<<"offline">> => true}),
    {200, Off} = http(post, "/contexts/_query", [], Q),
    {200, #{<<"offline">> := false}} =
        http(put, "/contexts/_offline", [], #{<<"offline">> => false}),
    ?assertMatch(#{<<"execution">> := <<"partial">>,
                   <<"working_set">> := Ws,
                   <<"coverage">> := #{<<"skipped">> := 1,
                                       <<"missing">> := [Ftp]},
                   <<"sources">> :=
                       [#{<<"status">> := <<"ok">>,
                          <<"membership">> := <<"live">>},
                        #{<<"status">> := <<"skipped_offline">>},
                        #{<<"status">> := <<"ok">>,
                          <<"membership">> := <<"retrieved_set">>,
                          <<"rows">> := Docs,
                          <<"version">> :=
                              #{<<"kind">> := <<"retrieved_set">>}}]}, Off),
    {200, On} = http(post, "/contexts/_query", [], Q),
    ?assertMatch(#{<<"execution">> := <<"succeeded">>}, On),
    {200, #{<<"members">> := [_, _]}} =
        http(delete, WsPath ++ "/members/" ++ binary_to_list(Os), [], none),
    {404, #{<<"error">> := <<"not_attached">>}} =
        http(delete, WsPath ++ "/members/" ++ binary_to_list(Os), [], none),
    {404, #{<<"error">> := <<"unknown_working_set">>}} =
        http(get, "/worksets/ws_nope", [], none),
    {200, _} = http(delete, WsPath, [], none).

%% The MCP tools return the REST maps; attach without a working set
%% creates one.
t_worksets_mcp(Config) ->
    [Local, _Ftp, Os] = ?config(ctxs, Config),
    C = connect(#{}),
    {false, #{<<"id">> := Ws, <<"members">> := [_]}} =
        call(C, <<"context_attach">>, #{<<"context">> => Local}),
    {false, #{<<"slices">> := [#{<<"status">> := <<"complete">>}]}} =
        call(C, <<"context_materialize">>,
             #{<<"working_set">> => Ws,
               <<"from_query">> =>
                   #{<<"query">> => <<"SELECT id, lines FROM c ORDER BY "
                                      "lines DESC LIMIT 5">>,
                     <<"contexts">> => [Os]},
               <<"include">> => #{<<"embeddings">> => false}}),
    Q = #{<<"query">> => <<"SELECT id, lines FROM c ORDER BY lines DESC "
                           "LIMIT 20">>,
          <<"working_set">> => Ws},
    {false, ViaMcp} = call(C, <<"context_query">>, Q),
    {200, ViaRest} = http(post, "/contexts/_query", [], Q),
    ?assertEqual(strip_timing(ViaRest), strip_timing(ViaMcp)),
    {false, #{<<"contexts">> := [_ | _]}} =
        call(C, <<"context_discover">>, #{<<"q">> => <<"otp/">>}),
    {false, #{<<"members">> := [_]}} =
        call(C, <<"context_detach">>, #{<<"working_set">> => Ws,
                                        <<"context">> => Os}),
    barrel_mcp_client:close(C),
    ok = barrel_ctx:delete_ws(Ws).

%% B11: vector results carry the embedding fingerprint in the HTTP meta,
%% so two remote members with one model merge by score.
t_fingerprint_meta(Config) ->
    Docs = load_corpus(filename:join(?config(data_dir, Config),
                                     "otp_small.jsonl")),
    Policy = #{fields => [<<"moduledoc">>], mode => sync,
               embedder => {ollama, #{model => <<"nomic-embed-text">>}}},
    Dbs = [begin
               {ok, _} = barrel_server_dbs:ensure(Db, #{embedding => Policy}),
               ok = barrel_dbs:pin(Db),
               {201, _} = http(post, db_url(Db, "/_bulk_docs"), [],
                               #{<<"docs">> => [D || #{<<"app">> := A} = D
                                                         <- Docs, A =:= App]}),
               Db
           end || {Db, App} <- [{<<"srvctx_fp_et">>, <<"et">>},
                                {<<"srvctx_fp_ftp">>, <<"ftp">>}]],
    Vq = <<"SELECT * FROM vector_top_k('trace events', k => 3) AS v">>,
    {200, Body} = raw(post, db_url(hd(Dbs), "/query"), [],
                      #{<<"query">> => Vq}),
    #{<<"meta">> := #{<<"embedding">> := #{<<"fingerprint">> := Fp,
                                           <<"distance">> := <<"cosine">>}}} =
        lists:last(ndjson(Body)),
    ?assertMatch(<<"sha256:", _/binary>>, Fp),
    Via = endpoint(barrel_ctx_delay_proxy:port(?config(proxy, Config))),
    Ids = [register_card(<<"fp/", Db/binary>>,
                         remote(Via, Db)) || Db <- Dbs],
    {200, R} = ctx_query(Config, #{<<"query">> => Vq, <<"contexts">> => Ids}),
    ?assertMatch(#{<<"merge">> := <<"score">>, <<"relevance">> := true,
                   <<"sources">> := [#{<<"embedding">> :=
                                           #{<<"fingerprint">> := Fp}},
                                     #{<<"embedding">> :=
                                           #{<<"fingerprint">> := Fp}}]}, R),
    Scores = [S || #{<<"_score">> := S} <- maps:get(<<"rows">>, R)],
    ?assertEqual(lists:reverse(lists:sort(Scores)), Scores),
    %% a card may advertise the fingerprint (optional, validated)
    {201, #{<<"embedding">> := #{<<"fingerprint">> := Fp}}} =
        http(post, "/contexts", [],
             #{<<"name">> => <<"fp/card">>,
               <<"embedding">> => #{<<"fingerprint">> => Fp,
                                    <<"distance">> => <<"cosine">>},
               <<"locations">> => [remote(Via, hd(Dbs))]}).

%%====================================================================
%% Self-explaining API
%%====================================================================

%% Every request-level code is reachable over REST and has the one
%% shape: error, message, hint, details. MCP answers the same bodies.
t_error_catalog(Config) ->
    [Local, Ftp, _Os] = ?config(ctxs, Config),
    Q = fun(Body) -> http(post, "/contexts/_query", [], Body) end,
    Lim = <<"SELECT id FROM c LIMIT 2">>,
    Bm25 = <<"SELECT b.id FROM bm25_top_k('trace', k => 2) AS b">>,
    {201, #{<<"id">> := Ws}} =
        http(post, "/worksets", [], #{<<"budget">> => #{<<"contexts">> => 1}}),
    WsPath = "/worksets/" ++ binary_to_list(Ws),
    {200, _} = http(post, WsPath ++ "/members", [], #{<<"context">> => Local}),
    {201, #{<<"id">> := Ws2}} = http(post, "/worksets", [], #{}),
    Ws2Path = "/worksets/" ++ binary_to_list(Ws2),
    Dup = [register_card(<<"cat/dup">>, local(<<"srvctx_et">>))
           || _ <- [1, 2]],
    Cases =
        [{invalid_argument, Q(#{<<"query">> => Lim, <<"contexts">> => [Local],
                                <<"merge">> => <<"best">>})},
         {invalid_argument, raw_json(post, "/contexts/_query", <<"[1]">>)},
         {invalid_argument, http(post, "/worksets", [],
                                 #{<<"budget">> => #{<<"bytes">> => <<"x">>}})},
         {invalid_query, Q(#{<<"query">> => <<"SELECT b.id FROM "
                                              "bm25_top_k(\"x\", k => 2) AS b">>,
                             <<"contexts">> => [Local]})},
         {limit_required, Q(#{<<"query">> => <<"SELECT id FROM c">>,
                              <<"contexts">> => [Local]})},
         {limit_too_large, Q(#{<<"query">> => <<"SELECT id FROM c LIMIT 5000">>,
                               <<"contexts">> => [Local]})},
         {too_many_contexts, Q(#{<<"query">> => Lim,
                                 <<"contexts">> => lists:duplicate(9, Local)})},
         {duplicate_context, Q(#{<<"query">> => Lim,
                                 <<"contexts">> => [Local, <<"otp/et">>]})},
         {unsupported_federated_query,
          Q(#{<<"query">> => <<"SELECT id FROM c ORDER BY id LIMIT 2 OFFSET 2">>,
              <<"contexts">> => [Local]})},
         {merge_not_allowed, Q(#{<<"query">> => Bm25, <<"contexts">> => [Local],
                                 <<"merge">> => <<"score">>})},
         {merge_not_supported, Q(#{<<"query">> => Bm25,
                                   <<"contexts">> => [Local],
                                   <<"merge">> => <<"rrf">>})},
         {scores_not_comparable,
          Q(#{<<"query">> => <<"SELECT v.id FROM vector_top_k('trace', "
                               "k => 2) AS v">>,
              <<"contexts">> => [<<"otp/et">>, <<"fp/srvctx_fp_et">>],
              <<"merge">> => <<"score">>})},
         {unknown_context, Q(#{<<"query">> => Lim,
                               <<"contexts">> => [<<"otp/ett">>]})},
         {ambiguous_context, Q(#{<<"query">> => Lim,
                                 <<"contexts">> => [<<"cat/dup">>]})},
         {unknown_working_set, http(get, "/worksets/ws_nope", [], none)},
         {not_attached, http(delete, WsPath ++ "/members/otp%2Fftp", [], none)},
         {already_attached, http(post, WsPath ++ "/members", [],
                                 #{<<"context">> => <<"otp/et">>})},
         {already_exists, http(post, "/contexts", [],
                               #{<<"id">> => Local, <<"name">> => <<"x">>,
                                 <<"locations">> => [local(<<"x">>)]})},
         {invalid_card, http(post, "/contexts", [], #{<<"name">> => <<"x">>})},
         {no_location, http(post, Ws2Path ++ "/members", [],
                            #{<<"context">> => Ftp, <<"mode">> => <<"local">>})},
         {over_budget, http(post, WsPath ++ "/members", [],
                            #{<<"context">> => Ftp})},
         {invalid_snapshot, http(post, WsPath ++ "/_import", [],
                                 #{<<"dir">> => <<"/nonexistent/export">>})}],
    {200, _} = http(put, "/contexts/_offline", [], #{<<"offline">> => true}),
    Offline = http(post, WsPath ++ "/_materialize", [],
                   #{<<"from_query">> => #{<<"query">> => Bm25,
                                           <<"contexts">> => [Ftp]}}),
    {200, _} = http(put, "/contexts/_offline", [], #{<<"offline">> => false}),
    Seen = [assert_error(Code, Resp)
            || {Code, Resp} <- Cases ++ [{offline, Offline}]],
    Catalog = [C || {C, _} <- barrel_ctx_error:codes(),
                    not lists:member(C, [forbidden, internal,
                                         source_unavailable])],
    ?assertEqual(lists:sort(Catalog), lists:usort(Seen)),
    %% the details say what to change
    {400, #{<<"details">> := #{<<"max_contexts">> := 8,
                               <<"requested">> := 9}}} =
        Q(#{<<"query">> => Lim, <<"contexts">> => lists:duplicate(9, Local)}),
    {404, #{<<"details">> := #{<<"suggestions">> :=
                                   [#{<<"name">> := <<"otp/et">>} | _]}}} =
        Q(#{<<"query">> => Lim, <<"contexts">> => [<<"otp/ett">>]}),
    {409, #{<<"details">> := #{<<"candidates">> := [_, _]}}} =
        Q(#{<<"query">> => Lim, <<"contexts">> => [<<"cat/dup">>]}),
    {400, #{<<"hint">> := QuoteHint}} =
        Q(#{<<"query">> => <<"SELECT b.id FROM bm25_top_k(\"x\", k => 2) "
                             "AS b">>, <<"contexts">> => [Local]}),
    ?assertNotEqual(nomatch, string:find(QuoteHint, <<"single quotes">>)),
    %% MCP: same bodies, schema checks included
    C = connect(#{}),
    {true, McpBody} = call(C, <<"context_query">>,
                           #{<<"query">> => <<"SELECT id FROM c">>,
                             <<"contexts">> => [Local]}),
    {400, McpBody} = Q(#{<<"query">> => <<"SELECT id FROM c">>,
                         <<"contexts">> => [Local]}),
    {true, Missing} = call(C, <<"context_inspect">>, #{}),
    ?assertMatch(#{<<"error">> := <<"invalid_argument">>,
                   <<"details">> := #{<<"field">> := <<"context">>}}, Missing),
    {true, Unknown} = call(C, <<"context_inspect">>, #{<<"id">> => Local}),
    ?assertMatch(#{<<"error">> := <<"invalid_argument">>,
                   <<"details">> := #{<<"field">> := <<"id">>}}, Unknown),
    {true, BadMerge} = call(C, <<"context_query">>,
                            #{<<"query">> => Lim, <<"contexts">> => [Local],
                              <<"merge">> => <<"best">>}),
    ?assertMatch(#{<<"details">> := #{<<"allowed">> := [_ | _]}}, BadMerge),
    [assert_error(invalid_argument, {400, B}) || B <- [Missing, Unknown]],
    barrel_mcp_client:close(C),
    [ok = barrel_ctx:unregister(D) || D <- Dup],
    ok = barrel_ctx:delete_ws(Ws),
    ok = barrel_ctx:delete_ws(Ws2).

assert_error(Code, {Status, #{<<"error">> := Got, <<"message">> := Msg,
                              <<"hint">> := Hint, <<"details">> := D} = B}) ->
    ?assertEqual({atom_to_binary(Code), B}, {Got, B}),
    ?assert(Status >= 400),
    ?assert(is_map(D)),
    [?assert(is_binary(T) andalso byte_size(T) > 0) || T <- [Msg, Hint]],
    [?assertEqual(nomatch, string:find(T, <<"\x{2014}"/utf8>>))
     || T <- [Msg, Hint]],
    Code;
assert_error(Code, Other) ->
    ct:fail({no_error_shape, Code, Other}).

raw_json(Method, Path, Bin) ->
    {ok, S, _H, B} = hackney:request(
                       Method, list_to_binary(base() ++ Path),
                       [{<<"content-type">>, <<"application/json">>}], Bin,
                       [with_body]),
    {S, json:decode(B)}.

%% Names are accepted wherever ids are: query, inspect (REST path and
%% MCP), attach, detach. Sources, groups and rows carry the names.
t_names(Config) ->
    [Local, Ftp, _Os] = ?config(ctxs, Config),
    {200, #{<<"sources">> := Sources, <<"rows">> := Rows}} =
        ctx_query(Config, #{<<"query">> => ?ORDERED,
                            <<"contexts">> => [<<"otp/et">>, <<"otp/ftp">>]}),
    ?assertEqual([{Local, <<"otp/et">>}, {Ftp, <<"otp/ftp">>}],
                 [{I, N} || #{<<"context">> := I, <<"name">> := N} <- Sources]),
    ?assert(lists:all(fun(#{<<"_ctx_name">> := N}) ->
                              lists:member(N, [<<"otp/et">>, <<"otp/ftp">>])
                      end, Rows)),
    {200, #{<<"groups">> := [#{<<"name">> := <<"otp/et">>} | _]}} =
        ctx_query(Config, #{<<"query">> => <<"SELECT b.id FROM "
                                             "bm25_top_k('trace', k => 2) "
                                             "AS b">>,
                            <<"contexts">> => [<<"otp/et">>]}),
    {200, #{<<"id">> := Local}} = http(get, "/contexts/otp%2Fet", [], none),
    C = connect(#{}),
    {false, #{<<"id">> := Ftp}} =
        call(C, <<"context_inspect">>, #{<<"context">> => <<"otp/ftp">>}),
    {false, #{<<"id">> := Ws, <<"members">> := [M]}} =
        call(C, <<"context_attach">>, #{<<"context">> => <<"otp/ftp">>}),
    ?assertMatch(#{<<"context">> := Ftp, <<"name">> := <<"otp/ftp">>}, M),
    {200, #{<<"members">> := []}} =
        http(delete, "/worksets/" ++ binary_to_list(Ws) ++ "/members/otp%2Fftp",
             [], none),
    barrel_mcp_client:close(C),
    ok = barrel_ctx:delete_ws(Ws).

%% Answers say in words what happened: who answered, why a source did
%% not, how rows were merged, what a local copy covers.
t_summaries(Config) ->
    [Local, _Ftp, Os] = ?config(ctxs, Config),
    {200, #{<<"summary">> := All}} =
        ctx_query(Config, #{<<"query">> => ?ORDERED}),
    ?assertNotEqual(nomatch, string:find(All, <<"All 3 contexts answered">>)),
    ?assertNotEqual(nomatch, string:find(All, <<"ORDER BY order">>)),
    Closed = register_card(<<"sum/closed">>,
                           remote(endpoint(closed_port()), <<"srvctx_ftp">>)),
    {200, #{<<"summary">> := Part, <<"sources">> := [_, Failed]}} =
        ctx_query(Config, #{<<"query">> => ?ORDERED,
                            <<"contexts">> => [Local, <<"sum/closed">>]}),
    ?assertNotEqual(nomatch, string:find(Part, <<"1 of 2 contexts answered">>)),
    ?assertNotEqual(nomatch, string:find(Part, <<"sum/closed:">>)),
    ?assertMatch(#{<<"status">> := <<"unreachable">>,
                   <<"error">> := #{<<"message">> := _, <<"hint">> := _}},
                 Failed),
    {200, #{<<"summary">> := Grouped}} =
        ctx_query(Config, #{<<"query">> => <<"SELECT b.id FROM bm25_top_k("
                                             "'trace', k => 2) AS b">>}),
    ?assertNotEqual(nomatch, string:find(Grouped, <<"BM25 scores are not "
                                                    "comparable">>)),
    C = connect(#{}),
    {true, #{<<"execution">> := <<"failed">>, <<"summary">> := None}} =
        call(C, <<"context_query">>, #{<<"query">> => ?ORDERED,
                                       <<"contexts">> => [Closed]}),
    ?assertNotEqual(nomatch, string:find(None, <<"No context answered">>)),
    {false, #{<<"working_set">> := Ws, <<"summary">> := Saved,
              <<"slices">> := [#{<<"name">> := <<"otp/os_mon">>}]}} =
        call(C, <<"context_materialize">>,
             #{<<"from_query">> =>
                   #{<<"query">> => <<"SELECT b.id FROM bm25_top_k("
                                      "'event trace', k => 2) AS b">>,
                     <<"contexts">> => [<<"otp/os_mon">>]}}),
    ?assertNotEqual(nomatch, string:find(Saved, <<"saved ">>)),
    {false, #{<<"summary">> := WsSum, <<"members">> := [Member]}} =
        call(C, <<"context_working_sets">>, #{<<"working_set">> => Ws}),
    ?assertMatch(#{<<"context">> := Os, <<"mode">> := <<"retrieved_set">>,
                   <<"membership">> := <<"retrieved_set">>,
                   <<"answers_offline">> := true}, Member),
    ?assertNotEqual(nomatch, string:find(WsSum, <<"saved slice">>)),
    {false, #{<<"contexts">> := [], <<"summary">> := Empty}} =
        call(C, <<"context_discover">>, #{<<"q">> => <<"nothing here">>}),
    ?assertNotEqual(nomatch, string:find(Empty, <<"context_list">>)),
    barrel_mcp_client:close(C),
    ok = barrel_ctx:unregister(Closed),
    ok = barrel_ctx:delete_ws(Ws).

%% Shapes, merges and limits are readable before a first query, and
%% every tool documents its inputs.
t_capabilities(_Config) ->
    {200, Caps} = http(get, "/contexts/_capabilities", [], none),
    ?assertMatch(#{<<"shapes">> := [_, _, _],
                   <<"limits">> := #{<<"max_contexts">> := 8,
                                     <<"max_limit">> := 1000},
                   <<"merges">> := #{<<"ordered">> := _, <<"grouped">> := _},
                   <<"refused_merges">> := #{<<"rrf">> := _},
                   <<"working_set_budget">> := #{<<"bytes">> := _},
                   <<"offline">> := false}, Caps),
    {200, #{<<"offline">> := false}} = http(get, "/contexts/_offline", [], none),
    C = connect(#{}),
    {false, Caps} = call(C, <<"context_capabilities">>, #{}),
    {ok, Tools} = barrel_mcp_client:list_tools(C),
    Ctx = [T || #{<<"name">> := <<"context_", _/binary>>} = T <- Tools],
    ?assertEqual(12, length(Ctx)),
    lists:foreach(
        fun(#{<<"name">> := Name, <<"description">> := Desc,
              <<"inputSchema">> := Schema}) ->
            ?assert(byte_size(Desc) < 700),
            ?assertEqual({Name, nomatch},
                         {Name, string:find(Desc, <<"\x{2014}"/utf8>>)}),
            Props = maps:get(<<"properties">>, Schema, #{}),
            [?assertMatch({Name, P, #{<<"description">> := _,
                                      <<"type">> := _}},
                          {Name, P, Spec})
             || P := Spec <- Props]
        end, Ctx),
    barrel_mcp_client:close(C).

%% Working sets can be listed, read, deleted and filled from MCP; the
%% node's offline mode can be read and switched.
t_worksets_cleanup(Config) ->
    [Local | _] = ?config(ctxs, Config),
    C = connect(#{}),
    {false, #{<<"id">> := Ws}} =
        call(C, <<"context_attach">>, #{<<"context">> => Local}),
    {false, #{<<"working_sets">> := List}} =
        call(C, <<"context_working_sets">>, #{}),
    ?assertMatch([#{<<"members">> := [#{<<"name">> := <<"otp/et">>}]}],
                 [W || #{<<"id">> := I} = W <- List, I =:= Ws]),
    {200, #{<<"working_sets">> := List}} = http(get, "/worksets", [], none),
    {true, #{<<"error">> := <<"invalid_snapshot">>}} =
        call(C, <<"context_import">>, #{<<"working_set">> => Ws,
                                        <<"dir">> => <<"/nonexistent">>}),
    {false, #{<<"offline">> := false}} = call(C, <<"context_offline">>, #{}),
    {false, #{<<"offline">> := true}} =
        call(C, <<"context_offline">>, #{<<"offline">> => true}),
    {200, #{<<"offline">> := true}} = http(get, "/contexts/_offline", [], none),
    {false, #{<<"offline">> := false}} =
        call(C, <<"context_offline">>, #{<<"offline">> => false}),
    {false, #{<<"deleted">> := Ws}} =
        call(C, <<"context_working_set_delete">>, #{<<"working_set">> => Ws}),
    {true, #{<<"error">> := <<"unknown_working_set">>}} =
        call(C, <<"context_working_sets">>, #{<<"working_set">> => Ws}),
    barrel_mcp_client:close(C).

%%====================================================================
%% Locked group
%%====================================================================

%% A capability token queries its space and a remote context; the other
%% local member is reported unauthorized, not a request failure.
t_capability_members(Config) ->
    Vec = #{dimension => 3, bm25_backend => memory,
            db_path => filename:join(?config(priv_dir, Config), "cap_vec")},
    {ok, #{id := Space, db := SpaceDb}} =
        barrel_spaces:create_space(#{label => <<"ctx">>, vectordb => Vec}),
    {ok, _} = barrel:put_doc(SpaceDb, #{<<"id">> => <<"s1">>,
                                        <<"lines">> => 7}),
    {ok, Token, _} = barrel_caps:grant(Space, #{rights => [read],
                                                subject => <<"agent">>}),
    Mine = register_card(<<"cap/space">>, local(Space)),
    Other = register_card(<<"cap/other">>, local(<<"srvctx_et">>)),
    Via = endpoint(barrel_ctx_delay_proxy:port(?config(locked_proxy, Config))),
    Public = register_card(<<"cap/remote">>, remote(Via, <<"srvctx_ftp">>)),
    Auth = [{<<"authorization">>, <<"Bearer ", Token/binary>>}],
    {200, R} = http(post, "/contexts/_query", Auth,
                    #{<<"query">> => <<"SELECT id FROM c LIMIT 3">>,
                      <<"contexts">> => [Mine, Other, Public]}),
    ?assertMatch(#{<<"execution">> := <<"partial">>,
                   <<"sources">> := [#{<<"status">> := <<"ok">>,
                                       <<"rows">> := 1},
                                     #{<<"status">> := <<"unauthorized">>},
                                     #{<<"status">> := <<"ok">>}]}, R),
    %% MCP with the same token: same verdicts
    C = connect(#{auth => {bearer, Token}}),
    {false, M} = call(C, <<"context_query">>,
                      #{<<"query">> => <<"SELECT id FROM c LIMIT 3">>,
                        <<"contexts">> => [Mine, Other, Public]}),
    ?assertEqual([<<"ok">>, <<"unauthorized">>, <<"ok">>],
                 [S || #{<<"status">> := S} <- maps:get(<<"sources">>, M)]),
    barrel_mcp_client:close(C),
    %% without the configured credential the remote leg is refused
    application:set_env(barrel, ctx_credentials, #{}),
    {200, R2} = http(post, "/contexts/_query", Auth,
                     #{<<"query">> => <<"SELECT id FROM c LIMIT 3">>,
                       <<"contexts">> => [Public]}),
    ?assertMatch(#{<<"execution">> := <<"failed">>,
                   <<"sources">> := [#{<<"status">> := <<"unauthorized">>}]},
                 R2).

t_register_needs_global(_Config) ->
    {ok, #{id := Space}} = barrel_spaces:create_space(#{label => <<"r">>}),
    {ok, Token, _} = barrel_caps:grant(Space, #{rights => [admin]}),
    Card = #{<<"name">> => <<"x">>, <<"locations">> => [local(Space)]},
    {403, _} = http(post, "/contexts",
                    [{<<"authorization">>, <<"Bearer ", Token/binary>>}], Card),
    {401, _} = http(post, "/contexts", [], Card),
    {201, _} = http(post, "/contexts",
                    [{<<"authorization">>, <<"Bearer ", ?ROOT/binary>>}], Card),
    %% reading cards is open to a live capability token
    {200, _} = http(get, "/contexts",
                    [{<<"authorization">>, <<"Bearer ", Token/binary>>}], none).

%% Working sets hold node-wide copies and credentials: capability
%% tokens are refused on REST and MCP.
t_worksets_need_global(Config) ->
    {ok, #{id := Space}} = barrel_spaces:create_space(#{label => <<"w">>}),
    {ok, Token, _} = barrel_caps:grant(Space, #{rights => [admin]}),
    Auth = [{<<"authorization">>, <<"Bearer ", Token/binary>>}],
    Root = [{<<"authorization">>, <<"Bearer ", ?ROOT/binary>>}],
    {403, _} = http(post, "/worksets", Auth, #{}),
    {403, _} = http(put, "/contexts/_offline", Auth, #{<<"offline">> => true}),
    {201, #{<<"id">> := Ws}} = http(post, "/worksets", Root, #{}),
    {403, #{<<"error">> := <<"forbidden">>}} =
        http(post, "/contexts/_query", Auth,
             #{<<"query">> => <<"SELECT id FROM c LIMIT 1">>,
               <<"working_set">> => Ws}),
    C = connect(#{auth => {bearer, Token}}),
    [Local | _] = ?config(ctxs, Config),
    {true, #{<<"error">> := <<"forbidden">>}} =
        call(C, <<"context_attach">>, #{<<"working_set">> => Ws,
                                        <<"context">> => Local}),
    barrel_mcp_client:close(C),
    ok = barrel_ctx:delete_ws(Ws).

%%====================================================================
%% Helpers
%%====================================================================

ctx_query(Config, Req0) ->
    Req = case maps:is_key(<<"contexts">>, Req0) of
        true -> Req0;
        false -> Req0#{<<"contexts">> => ?config(ctxs, Config)}
    end,
    http(post, "/contexts/_query", [], Req).

strip_timing(#{<<"sources">> := Sources} = R) ->
    (maps:remove(<<"elapsed_ms">>, R))#{
        <<"sources">> => [maps:without([<<"elapsed_ms">>], S)
                          || S <- Sources]}.

data_files(Config) ->
    Dir = ?config(priv_dir, Config),
    lists:sort([F || F <- filelib:wildcard("*", Dir),
                     filelib:is_dir(filename:join(Dir, F))]).

port() ->
    Children = supervisor:which_children(barrel_server_sup),
    {_, Pid, _, _} = lists:keyfind(barrel_server_http, 1, Children),
    barrel_server_test:h1_port(Pid).

base() ->
    "http://127.0.0.1:" ++ integer_to_list(port()).

endpoint(Port) ->
    <<"http://127.0.0.1:", (integer_to_binary(Port))/binary>>.

closed_port() ->
    {ok, L} = gen_tcp:listen(0, []),
    {ok, P} = inet:port(L),
    ok = gen_tcp:close(L),
    P.

db_url(Db, Path) ->
    "/db/" ++ binary_to_list(Db) ++ Path.

restart_http() ->
    ok = supervisor:terminate_child(barrel_server_sup, barrel_server_http),
    {ok, _} = supervisor:restart_child(barrel_server_sup, barrel_server_http),
    ok.

http(Method, Path, Headers, Body) ->
    case raw(Method, Path, Headers, Body) of
        {Status, <<>>} -> {Status, #{}};
        {Status, Bin} ->
            {Status, try json:decode(Bin) catch _:_ -> Bin end}
    end.

raw(Method, Path, Headers0, none) ->
    {ok, S, _H, B} = hackney:request(Method, list_to_binary(base() ++ Path),
                                     Headers0, <<>>, [with_body]),
    {S, B};
raw(Method, Path, Headers0, Map) ->
    Headers = [{<<"content-type">>, <<"application/json">>} | Headers0],
    {ok, S, _H, B} = hackney:request(Method, list_to_binary(base() ++ Path),
                                     Headers, json:encode(Map), [with_body]),
    {S, B}.

ndjson(Body) ->
    [json:decode(L) || L <- binary:split(Body, <<"\n">>, [global]),
                       L =/= <<>>].

connect(Extra) ->
    Spec = Extra#{transport => {http, list_to_binary(base() ++ "/mcp")}},
    {ok, C} = barrel_mcp_client:start(Spec),
    ok = wait_ready(C, 100),
    C.

wait_ready(_C, 0) ->
    {error, not_ready};
wait_ready(C, N) ->
    case barrel_mcp_client:list_tools(C) of
        {ok, _} -> ok;
        {error, not_ready} ->
            timer:sleep(50),
            wait_ready(C, N - 1);
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
