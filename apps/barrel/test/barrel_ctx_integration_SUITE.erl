%%%-------------------------------------------------------------------
%%% @doc Contexts integration: working sets in the executor (local,
%%% remote, snapshot and retrieved-set members), offline coverage,
%%% facade materialize, cross-context merges (score with fingerprints,
%%% interleave), open-existing-only local members, export secret guard.
%%% Vectors are feature-hashed through a meck stub: no network.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ctx_integration_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([ws_query_equals_contexts/1,
         offline_working_set/1,
         snapshot_member/1,
         typo_card_creates_nothing/1,
         score_merge_fingerprints/1,
         interleave_is_presentation/1,
         discover_filters/1,
         export_refuses_policy_secret/1]).

-define(APPS, [<<"tools">>, <<"sasl">>, <<"eunit">>]).
-define(WS_STORE, <<"_barrel_worksets">>).
-define(ROWS, <<"SELECT id, path FROM c ORDER BY path ASC LIMIT 8">>).

all() ->
    [ws_query_equals_contexts, offline_working_set, snapshot_member,
     typo_card_creates_nothing, score_merge_fingerprints,
     interleave_is_presentation, discover_filters,
     export_refuses_policy_secret].

%%====================================================================
%% Setup
%%====================================================================

init_per_suite(Config) ->
    Priv = ?config(priv_dir, Config),
    application:load(barrel_docdb),
    application:set_env(barrel_docdb, data_dir, filename:join(Priv, "data")),
    {ok, _} = application:ensure_all_started(barrel),
    %% suites share the VM: never reuse a store another suite left open
    _ = barrel_docdb:close_db(?WS_STORE),
    ok = application:set_env(barrel, ctx_dir, filename:join(Priv, "ctx")),
    ok = application:set_env(barrel, ctx_catalog_db, <<"_ctxi_catalog">>),
    ok = barrel_ctx_test_corpus:mock_embed(),
    Docs = barrel_ctx_test_corpus:docs(?APPS),
    Ctxs = [begin
                Db = <<"ctxi_", App/binary>>,
                ok = seed_plain(Db, Priv, [D || #{<<"app">> := A} = D <- Docs,
                                                A =:= App]),
                {ok, #{<<"id">> := Id}} = barrel_ctx:register(
                    #{<<"name">> => <<"otp/", App/binary>>,
                      <<"title">> => <<"OTP ", App/binary>>,
                      <<"topics">> => [<<"erlang">>, App],
                      <<"locations">> => [#{<<"kind">> => <<"local">>,
                                            <<"db">> => Db}]}),
                {App, Id, Db}
            end || App <- ?APPS],
    [{ctxs, Ctxs}, {docs, Docs} | Config].

end_per_suite(_Config) ->
    [ok = barrel_ctx_ws:delete(Ws) || Ws <- barrel_ctx_ws:list()],
    [ok = barrel_ctx_export:remove_import(N)
     || N <- barrel_ctx_export:list_imports()],
    [ok = barrel_dbs:close(Db) || <<"ctxi_", _/binary>> = Db <- barrel_dbs:list()],
    _ = barrel_docdb:close_db(?WS_STORE),
    application:unset_env(barrel, ctx_dir),
    application:unset_env(barrel, ctx_catalog_db),
    application:unset_env(barrel, ctx_offline),
    barrel_ctx_test_corpus:unmock_embed().

init_per_testcase(_TC, Config) ->
    Config.

end_per_testcase(_TC, _Config) ->
    application:unset_env(barrel, ctx_offline),
    ?assertEqual(#{}, barrel_dbs:leases()),
    ok.

seed_plain(Db, Priv, Docs) ->
    {ok, H} = barrel_dbs:ensure(Db, #{
        vectordb => #{dimension => barrel_ctx_test_corpus:dim(),
                      db_path => filename:join(Priv, "vec_" ++
                                                   binary_to_list(Db)),
                      bm25_backend => memory}}),
    ok = barrel_dbs:pin(Db),
    [{ok, _} = R || R <- barrel:put_docs(H, Docs)],
    Batch = [{Id, T, #{}, barrel_ctx_test_corpus:vec(T)}
             || #{<<"id">> := Id} = D <- Docs,
                T <- [barrel_ctx_test_corpus:text(D)]],
    {ok, _} = barrel:vector_add_batch(H, Batch),
    ok.

ctx(App, Config) ->
    {App, Id, _Db} = lists:keyfind(App, 1, ?config(ctxs, Config)),
    Id.

%%====================================================================
%% Cases
%%====================================================================

%% A working set of local members answers like the same contexts listed.
ws_query_equals_contexts(Config) ->
    [A, B] = [ctx(App, Config) || App <- [<<"tools">>, <<"sasl">>]],
    {ok, Ws} = barrel_ctx:create_ws(#{owner => <<"session:t1">>}),
    Open0 = lists:sort(barrel_dbs:list()),
    {ok, _} = barrel_ctx:attach(Ws, A, #{}),
    {ok, #{members := [#{mode := local}, #{mode := local}]}} =
        barrel_ctx:attach(Ws, B, #{}),
    ?assertEqual(Open0, lists:sort(barrel_dbs:list())),
    {ok, ByCtx} = barrel_ctx:query(#{query => ?ROWS, contexts => [A, B]}),
    {ok, ByWs} = barrel_ctx:query(#{query => ?ROWS, working_set => Ws}),
    ?assertEqual(maps:get(rows, ByCtx), maps:get(rows, ByWs)),
    ?assertMatch(#{execution := succeeded, working_set := Ws,
                   merge := ordered}, ByWs),
    ?assert(lists:all(fun(#{membership := M}) -> M =:= live end,
                      maps:get(sources, ByWs))),
    ?assertMatch({error, {invalid_argument, #{field := working_set}}},
                 barrel_ctx:query(#{query => ?ROWS, working_set => Ws,
                                    contexts => [A]})),
    ?assertMatch({error, {unknown_working_set,
                          #{working_set := <<"ws_nope">>}}},
                 barrel_ctx:query(#{query => ?ROWS,
                                    working_set => <<"ws_nope">>})),
    ok = barrel_ctx:delete_ws(Ws).

%% Remote member skipped offline without contact; a slice answers with
%% retrieved-set membership; online, the dead remote is unreachable.
offline_working_set(Config) ->
    A = ctx(<<"tools">>, Config),
    C = ctx(<<"eunit">>, Config),
    Server = barrel_ctx_fake_server:start(stall),
    Endpoint = iolist_to_binary(["http://127.0.0.1:",
                                 integer_to_list(
                                   barrel_ctx_fake_server:port(Server))]),
    {ok, #{<<"id">> := R}} = barrel_ctx:register(
        #{<<"name">> => <<"remote/probe">>,
          <<"locations">> => [#{<<"kind">> => <<"remote">>,
                                <<"endpoint">> => Endpoint,
                                <<"db">> => <<"probe">>}]}),
    {ok, Ws} = barrel_ctx:create_ws(#{}),
    {ok, _} = barrel_ctx:attach(Ws, A, #{}),
    {ok, #{members := [_, #{mode := remote}]}} = barrel_ctx:attach(Ws, R, #{}),
    Bm25 = <<"SELECT b.id, b._score FROM bm25_top_k('test', k => 5) AS b">>,
    {ok, #{working_set := Ws, slices := [Slice]}} =
        barrel_ctx:materialize(Ws, #{from_query => #{query => Bm25,
                                                     contexts => [C]}}),
    ?assertMatch(#{context := C, status := complete, docs := N} when N > 0,
                 Slice),
    SavedIds = lists:sort(saved_ids(Slice)),
    ok = barrel_ctx:set_offline(true),
    Q = <<"SELECT id, path FROM c ORDER BY path ASC LIMIT 50">>,
    {ok, Off} = barrel_ctx:query(#{query => Q, working_set => Ws}),
    receive {fake_request, _, _} -> ct:fail(remote_contacted_offline)
    after 200 -> ok
    end,
    #{execution := partial, sources := [SA, SR, SC],
      coverage := #{requested := 3, answered := 2, skipped := 1,
                    missing := [R]}} = Off,
    ?assertMatch(#{status := ok, membership := live}, SA),
    ?assertMatch(#{status := skipped_offline,
                   error := #{reason := no_local_copy}}, SR),
    ?assertMatch(#{status := ok, membership := retrieved_set,
                   version := #{kind := retrieved_set}, note := _}, SC),
    SliceRows = [Id || #{<<"_ctx">> := Ctx, <<"id">> := Id}
                           <- maps:get(rows, Off), Ctx =:= C],
    ?assertEqual(SavedIds, lists:sort(SliceRows)),
    %% online again: the stalled remote times out with its budget (it
    %% never answers), nothing is downloaded; the budget covers the two
    %% local members that answer
    ok = barrel_ctx:set_offline(false),
    {ok, On} = barrel_ctx:query(#{query => Q, working_set => Ws,
                                  per_context_timeout_ms => 1000}),
    ?assertMatch(#{execution := partial,
                   sources := [#{status := ok},
                               #{status := timeout, rows := 0,
                                 error := #{after_ms := 1000}},
                               #{status := ok}]}, On),
    barrel_ctx_fake_server:stop(Server),
    ok = barrel_ctx:delete_ws(Ws).

saved_ids(#{local_db := Db}) ->
    {ok, Opts} = barrel_ctx_slice:open_opts(Db),
    {ok, H} = barrel_dbs:ensure(Db, Opts),
    {ok, Rows, _} = barrel:'query'(H, <<"SELECT id FROM c LIMIT 100">>, #{}),
    [Id || #{<<"id">> := Id} <- Rows].

%% An imported generation answers as `generation', read only.
snapshot_member(Config) ->
    Priv = ?config(priv_dir, Config),
    Src = <<"ctxi_snapsrc">>,
    Docs = [D || #{<<"app">> := <<"sasl">>} = D <- ?config(docs, Config)],
    ok = seed_plain(Src, Priv, Docs),
    ok = barrel_dbs:unpin(Src),
    Dest = filename:join(Priv, "export_sasl"),
    {ok, #{context := Ctx}} = barrel_ctx_export:export(
                                Src, Dest, #{generation => 3}),
    {ok, Ws} = barrel_ctx:create_ws(#{}),
    {ok, #{members := [#{mode := snapshot, generation := 3}]}} =
        barrel_ctx:import(Ws, Dest, #{name => <<"ctxi_snap3">>}),
    {ok, #{sources := [S], rows := Rows}} =
        barrel_ctx:query(#{query => ?ROWS, working_set => Ws,
                           offline => true}),
    ?assertMatch(#{context := Ctx, status := ok,
                   membership := complete_generation,
                   version := #{kind := generation, generation := 3}}, S),
    ?assertEqual(8, length(Rows)),
    ok = barrel_ctx:delete_ws(Ws).

%% A local card naming a missing database reports an error and creates
%% nothing.
typo_card_creates_nothing(_Config) ->
    Typo = <<"ctxi_no_such_db">>,
    {ok, #{<<"id">> := Id}} = barrel_ctx:register(
        #{<<"name">> => <<"typo">>,
          <<"locations">> => [#{<<"kind">> => <<"local">>,
                                <<"db">> => Typo}]}),
    {ok, #{execution := failed, sources := [S]}} =
        barrel_ctx:query(#{query => ?ROWS, contexts => [Id]}),
    ?assertMatch(#{status := error, error := #{reason := db_not_found}}, S),
    ?assertNot(barrel_docdb:db_exists(Typo, #{})),
    ?assertNot(lists:member(Typo, barrel_dbs:list())).

%% vector_top_k merges by score when fingerprints match; otherwise the
%% automatic merge falls back to grouped and a requested one is refused.
score_merge_fingerprints(Config) ->
    Priv = ?config(priv_dir, Config),
    Docs = ?config(docs, Config),
    [S1, S2, S3] =
        [record_ctx(Name, Model, Priv,
                    [D || #{<<"app">> := A} = D <- Docs, A =:= App])
         || {Name, Model, App} <-
                [{<<"ctxi_rec_tools">>, <<"nomic-embed-text">>, <<"tools">>},
                 {<<"ctxi_rec_sasl">>, <<"nomic-embed-text">>, <<"sasl">>},
                 {<<"ctxi_rec_eunit">>, <<"all-minilm">>, <<"eunit">>}]],
    Vq = <<"SELECT * FROM vector_top_k('release upgrade handling', k => 4) "
           "AS v">>,
    {ok, Same} = barrel_ctx:query(#{query => Vq, contexts => [S1, S2]}),
    #{merge := score, relevance := true, rows := Rows,
      sources := Sources} = Same,
    Scores = [maps:get(<<"_score">>, R) || R <- Rows],
    ?assertEqual(lists:reverse(lists:sort(Scores)), Scores),
    ?assertEqual(4, length(Rows)),
    [Fp] = lists:usort([F || #{embedding := #{fingerprint := F}} <- Sources]),
    ?assertMatch(<<"sha256:", _/binary>>, Fp),
    %% union of each member's top 4 holds the global top 4
    {ok, #{groups := Groups}} = barrel_ctx:query(#{query => Vq,
                                                   contexts => [S1, S2],
                                                   merge => grouped}),
    All = lists:reverse(lists:sort(
            [maps:get(<<"_score">>, R) || #{rows := Rs} <- Groups, R <- Rs])),
    ?assertEqual(lists:sublist(All, 4), Scores),
    {ok, Mixed} = barrel_ctx:query(#{query => Vq, contexts => [S1, S3]}),
    ?assertMatch(#{merge := grouped, relevance := false,
                   merge_fallback := #{requested := score,
                                       reason := fingerprint_mismatch,
                                       context := S3}}, Mixed),
    ?assertMatch({error, {scores_not_comparable,
                          #{reason := fingerprint_mismatch, context := S3}}},
                 barrel_ctx:query(#{query => Vq, contexts => [S1, S3],
                                    merge => score})).

record_ctx(Db, Model, Priv, Docs) ->
    {ok, H} = barrel_dbs:ensure(Db, #{
        embedding => #{fields => [<<"moduledoc">>, <<"path">>], mode => sync,
                       embedder => {ollama, #{model => Model}}},
        vectordb => #{dimension => barrel_ctx_test_corpus:dim(),
                      db_path => filename:join(Priv, "vec_" ++
                                                   binary_to_list(Db))}}),
    ok = barrel_dbs:pin(Db),
    [{ok, _} = R || R <- barrel:put_docs(H, Docs)],
    {ok, #{<<"id">> := Id}} = barrel_ctx:register(
        #{<<"name">> => Db, <<"locations">> => [#{<<"kind">> => <<"local">>,
                                                  <<"db">> => Db}]}),
    Id.

%% Interleave is round-robin presentation, never labelled relevance.
interleave_is_presentation(Config) ->
    [A, B] = [ctx(App, Config) || App <- [<<"tools">>, <<"sasl">>]],
    Q = <<"SELECT b.id, b._score FROM bm25_top_k('module', k => 3) AS b">>,
    {ok, #{merge := interleave, relevance := false, rows := Rows}} =
        barrel_ctx:query(#{query => Q, contexts => [A, B],
                           merge => interleave}),
    ?assertMatch([A, B | _], [maps:get(<<"_ctx">>, R) || R <- Rows]),
    {ok, #{merge := grouped, relevance := false}} =
        barrel_ctx:query(#{query => Q, contexts => [A, B]}).

discover_filters(Config) ->
    {ok, Cards} = barrel_ctx:discover(<<"OTP SASL">>, #{}),
    ?assertEqual([ctx(<<"sasl">>, Config)], [I || #{<<"id">> := I} <- Cards]),
    {ok, All} = barrel_ctx:discover(<<>>, #{}),
    ?assert(length(All) >= 3),
    ?assertMatch({error, {invalid_card, #{reason := embedding,
                                          detail := <<"fingerprint">>}}},
                 barrel_ctx:register(
                   #{<<"name">> => <<"bad">>,
                     <<"embedding">> => #{<<"fingerprint">> => <<"md5:x">>},
                     <<"locations">> => [#{<<"kind">> => <<"local">>,
                                           <<"db">> => <<"x">>}]})).

%% A persisted record policy naming an API key is never exported.
export_refuses_policy_secret(Config) ->
    Priv = ?config(priv_dir, Config),
    Db = <<"ctxi_secret">>,
    {ok, _} = barrel_dbs:ensure(Db, #{
        embedding => #{fields => [<<"moduledoc">>], mode => sync,
                       embedder => {ollama, #{model => <<"m">>,
                                              api_key => <<"sk-test">>}}},
        vectordb => #{dimension => barrel_ctx_test_corpus:dim(),
                      db_path => filename:join(Priv, "vec_secret")}}),
    Dest = filename:join(Priv, "export_secret"),
    ?assertEqual({error, {policy_holds_secret, <<"api_key">>}},
                 barrel_ctx_export:export(Db, Dest, #{})),
    ?assertEqual({ok, []}, file:list_dir(Dest)),
    %% the database is back under the manager
    {ok, _} = barrel_dbs:ensure(Db).
