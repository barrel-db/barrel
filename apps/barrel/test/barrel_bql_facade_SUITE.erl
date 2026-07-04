%%%-------------------------------------------------------------------
%%% @doc BQL at the facade: table functions (vector_top_k, bm25_top_k,
%%% hybrid_top_k) over record-mode databases with a mock embedder, the
%%% residual WHERE and over-fetch contract, score columns, and the
%%% collection delegation path.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_bql_facade_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([vector_rank_and_columns/1,
         bm25_rows/1,
         hybrid_rows/1,
         residual_where_overfetch/1,
         underfill_contract/1,
         projections_and_order/1,
         limit_offset/1,
         orphan_hits_dropped/1,
         unnest_over_hits/1,
         collection_delegation/1,
         query_fold_stops/1,
         error_paths/1,
         plain_store_without_embedder/1]).

all() ->
    [vector_rank_and_columns,
     bm25_rows,
     hybrid_rows,
     residual_where_overfetch,
     underfill_contract,
     projections_and_order,
     limit_offset,
     orphan_hits_dropped,
     unnest_over_hits,
     collection_delegation,
     query_fold_stops,
     error_paths,
     plain_store_without_embedder].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel),
    Dir = "/tmp/barrel_bql_facade_test_"
        ++ integer_to_list(erlang:system_time(millisecond)),
    [{dir, Dir} | Config].

end_per_suite(Config) ->
    os:cmd("rm -rf " ++ ?config(dir, Config)),
    ok.

init_per_testcase(TC, Config) ->
    mock_embed(),
    Dir = ?config(dir, Config),
    Name = list_to_atom("bqlf_" ++ atom_to_list(TC)),
    {ok, Db} = barrel:open(Name, #{
        embedding => #{fields => [<<"title">>], mode => sync,
                       metadata_fields => [<<"kind">>]},
        docdb => #{data_dir => Dir},
        vectordb => #{dimension => 3,
                      db_path => Dir ++ "/" ++ atom_to_list(TC)}}),
    seed(Db),
    [{db, Db} | Config].

end_per_testcase(_TC, Config) ->
    try meck:unload(barrel_embed) catch _:_ -> ok end,
    Db = ?config(db, Config),
    try barrel:close(Db) catch _:_ -> ok end,
    try barrel_docdb:delete_db(maps:get(docdb, Db)) catch _:_ -> ok end,
    ok.

mock_embed() ->
    _ = try meck:unload(barrel_embed) catch _:_ -> ok end,
    meck:new(barrel_embed, [passthrough, no_link]),
    meck:expect(barrel_embed, embed,
                fun(Text, _State) -> {ok, mock_vec(Text)} end),
    meck:expect(barrel_embed, embed_batch,
                fun(Texts, _State) -> {ok, [mock_vec(T) || T <- Texts]} end).

mock_vec(Text) ->
    Hash = erlang:phash2(Text, 1000000),
    [Hash / 1000000.0, (Hash rem 1000) / 1000.0, (Hash rem 100) / 100.0].

seed(Db) ->
    Docs = [
        #{<<"id">> => <<"d1">>, <<"title">> => <<"erlang in anger">>,
          <<"kind">> => <<"book">>, <<"lang">> => <<"en">>,
          <<"tags">> => [<<"beam">>, <<"ops">>]},
        #{<<"id">> => <<"d2">>, <<"title">> => <<"learn you some erlang">>,
          <<"kind">> => <<"book">>, <<"lang">> => <<"en">>},
        #{<<"id">> => <<"d3">>, <<"title">> => <<"programmation erlang">>,
          <<"kind">> => <<"book">>, <<"lang">> => <<"fr">>},
        #{<<"id">> => <<"d4">>, <<"title">> => <<"rust for rustaceans">>,
          <<"kind">> => <<"book">>, <<"lang">> => <<"en">>},
        #{<<"id">> => <<"d5">>, <<"title">> => <<"the rust book">>,
          <<"kind">> => <<"manual">>, <<"lang">> => <<"fr">>}
    ],
    lists:foreach(fun(D) -> {ok, _} = barrel:put_doc(Db, D) end, Docs).

%%====================================================================
%% Cases
%%====================================================================

vector_rank_and_columns(Config) ->
    Db = ?config(db, Config),
    %% the mock embedder is deterministic: querying an exact title puts
    %% that document at rank 1 with distance 0
    {ok, [Top | _] = Rows, #{has_more := false}} =
        barrel:query(Db,
            "SELECT * FROM vector_top_k('erlang in anger', k => 3) AS v"),
    ?assertEqual(3, length(Rows)),
    ?assertEqual(<<"d1">>, maps:get(<<"id">>, Top)),
    %% stored vectors are float32, so an exact-title query is near but
    %% not exactly distance 0
    Score = maps:get(<<"_score">>, Top),
    Distance = maps:get(<<"_distance">>, Top),
    ?assert(Score > 0.99),
    ?assert(Distance < 0.01),
    ?assert(abs(Distance - (1.0 - Score)) < 1.0e-9),
    %% SELECT * flattens the doc in
    ?assertEqual(<<"erlang in anger">>, maps:get(<<"title">>, Top)),
    %% rank order: scores are non-increasing
    Scores = [maps:get(<<"_score">>, R) || R <- Rows],
    ?assertEqual(lists:reverse(lists:sort(Scores)), Scores).

bm25_rows(Config) ->
    Db = ?config(db, Config),
    {ok, Rows, _} = barrel:query(Db,
        "SELECT m._score, title FROM bm25_top_k('rust', k => 5) AS m"),
    Ids = lists:sort([maps:get(<<"id">>, R) || R <- Rows]),
    ?assertEqual([<<"d4">>, <<"d5">>], Ids),
    lists:foreach(
        fun(Row) ->
            ?assert(is_float(maps:get(<<"_score">>, Row))),
            %% no distance column outside vector_top_k
            ?assertNot(maps:is_key(<<"_distance">>, Row)),
            ?assert(maps:is_key(<<"title">>, Row))
        end,
        Rows).

hybrid_rows(Config) ->
    Db = ?config(db, Config),
    %% record store has no embedder: the facade embeds the query and
    %% injects query_vector; the BM25 leg uses the text
    {ok, Rows, _} = barrel:query(Db,
        "SELECT h._score FROM hybrid_top_k('erlang in anger', k => 2) AS h"),
    ?assertEqual(2, length(Rows)),
    [Top | _] = Rows,
    ?assertEqual(<<"d1">>, maps:get(<<"id">>, Top)).

residual_where_overfetch(Config) ->
    Db = ?config(db, Config),
    %% k => 2 with a WHERE that filters: the single over-fetch (3x)
    %% must find the french docs even though they rank below the
    %% english ones for this query
    {ok, Rows, _} = barrel:query(Db,
        "SELECT title FROM vector_top_k('erlang in anger', k => 2) AS v "
        "WHERE v.lang = 'fr'"),
    Ids = lists:sort([maps:get(<<"id">>, R) || R <- Rows]),
    ?assertEqual([<<"d3">>, <<"d5">>], Ids).

underfill_contract(Config) ->
    Db = ?config(db, Config),
    %% heavy filtering can return fewer than k rows: up-to-k contract
    {ok, Rows, #{count := Count}} = barrel:query(Db,
        "SELECT * FROM vector_top_k('anything', k => 5) AS v "
        "WHERE v.lang = 'nope'"),
    ?assertEqual([], Rows),
    ?assertEqual(0, Count).

projections_and_order(Config) ->
    Db = ?config(db, Config),
    {ok, Rows, _} = barrel:query(Db,
        "SELECT v._score AS s, v._distance, title FROM "
        "vector_top_k('erlang in anger', k => 3) AS v "
        "ORDER BY v._distance DESC"),
    ?assertEqual(3, length(Rows)),
    [First | _] = Rows,
    ?assert(maps:is_key(<<"s">>, First)),
    ?assert(maps:is_key(<<"_distance">>, First)),
    ?assert(maps:is_key(<<"title">>, First)),
    %% ORDER BY distance DESC puts the best match last
    Last = lists:last(Rows),
    ?assert(maps:get(<<"_distance">>, Last) < 0.01),
    Distances = [maps:get(<<"_distance">>, R) || R <- Rows],
    ?assertEqual(lists:reverse(lists:sort(Distances)), Distances).

limit_offset(Config) ->
    Db = ?config(db, Config),
    {ok, All, _} = barrel:query(Db,
        "SELECT * FROM vector_top_k('erlang in anger', k => 4) AS v"),
    {ok, Sliced, _} = barrel:query(Db,
        "SELECT * FROM vector_top_k('erlang in anger', k => 4) AS v "
        "LIMIT 2 OFFSET 1"),
    ExpectedIds = [maps:get(<<"id">>, R)
                   || R <- lists:sublist(tl(All), 2)],
    ?assertEqual(ExpectedIds, [maps:get(<<"id">>, R) || R <- Sliced]).

orphan_hits_dropped(Config) ->
    Db = ?config(db, Config),
    %% delete the doc behind a vector THROUGH DOCDB (bypassing the
    %% facade, so the vector stays): the stale hit must be dropped
    DbBin = maps:get(docdb, Db),
    {ok, #{<<"_rev">> := Rev}} = barrel_docdb:get_doc(DbBin, <<"d1">>),
    {ok, _} = barrel_docdb:delete_doc(DbBin, <<"d1">>, #{rev => Rev}),
    {ok, Rows, _} = barrel:query(Db,
        "SELECT * FROM vector_top_k('erlang in anger', k => 5) AS v"),
    Ids = [maps:get(<<"id">>, R) || R <- Rows],
    ?assertNot(lists:member(<<"d1">>, Ids)),
    ?assertEqual(4, length(Ids)).

unnest_over_hits(Config) ->
    Db = ?config(db, Config),
    {ok, Rows, _} = barrel:query(Db,
        "SELECT t AS tag, v._score FROM "
        "vector_top_k('erlang in anger', k => 1) AS v, "
        "UNNEST(v.tags) AS t"),
    ?assertEqual(
        [{<<"d1">>, <<"beam">>}, {<<"d1">>, <<"ops">>}],
        lists:sort([{maps:get(<<"id">>, R), maps:get(<<"tag">>, R)}
                    || R <- Rows])),
    lists:foreach(
        fun(R) -> ?assert(maps:get(<<"_score">>, R) > 0.99) end,
        Rows).

collection_delegation(Config) ->
    Db = ?config(db, Config),
    {ok, Rows, #{has_more := false}} = barrel:query(Db,
        "SELECT title FROM db WHERE lang = 'en' ORDER BY title"),
    ?assertEqual(
        [<<"erlang in anger">>, <<"learn you some erlang">>,
         <<"rust for rustaceans">>],
        [maps:get(<<"title">>, R) || R <- Rows]),
    {ok, Explain} = barrel:explain_query(Db,
        "SELECT * FROM db WHERE lang = 'en'"),
    ?assertMatch(#{source := collection,
                   engine := #{strategy := index_seek}}, Explain),
    {ok, TfExplain} = barrel:explain_query(Db,
        "SELECT * FROM vector_top_k('q', k => 2) AS v "
        "WHERE v.lang = 'en'"),
    ?assertMatch(#{source := vector_top_k,
                   args := #{k := 2},
                   residual_conditions := 1}, TfExplain).

query_fold_stops(Config) ->
    Db = ?config(db, Config),
    {ok, Collected, _} = barrel:query_fold(Db,
        "SELECT * FROM db WHERE kind = 'book'", #{chunk_size => 2},
        fun(Row, Acc) ->
            case length(Acc) >= 2 of
                true -> {stop, Acc};
                false -> {ok, [maps:get(<<"id">>, Row) | Acc]}
            end
        end,
        []),
    ?assertEqual(2, length(Collected)).

error_paths(Config) ->
    Db = ?config(db, Config),
    ?assertMatch(
        {error, {invalid_query, _, {unbound_param, <<"q">>}}},
        barrel:query(Db, "SELECT * FROM vector_top_k($q, k => 2) AS v")),
    ?assertEqual(
        {error, {unsupported, subscribe}},
        barrel:query(Db, "SELECT * FROM db WHERE a = 1 SUBSCRIBE")),
    ?assertMatch(
        {error, {parse_error, _, _}},
        barrel:query(Db, "SELECT")),
    ?assertEqual(
        {error, {unsupported, continuation}},
        barrel:query(Db, "SELECT * FROM vector_top_k('q', k => 2) AS v",
                     #{continuation => <<"tok">>})).

plain_store_without_embedder(Config) ->
    %% real barrel_embed for this one: the plain store has no embedder
    %% and the error must surface through the table function
    ok = meck:unload(barrel_embed),
    Dir = ?config(dir, Config),
    {ok, Plain} = barrel:open(bqlf_plain, #{
        docdb => #{data_dir => Dir},
        vectordb => #{dimension => 3, db_path => Dir ++ "/plain_vec"}}),
    {ok, _} = barrel:put_doc(Plain, #{<<"id">> => <<"p1">>,
                                      <<"kind">> => <<"plain">>}),
    %% plain collection queries work
    {ok, [Row], _} = barrel:query(Plain,
        "SELECT * FROM db WHERE kind = 'plain'"),
    ?assertEqual(<<"p1">>, maps:get(<<"id">>, Row)),
    %% no embedder anywhere: vector table functions surface the error
    ?assertEqual(
        {error, embedder_not_configured},
        barrel:query(Plain, "SELECT * FROM vector_top_k('q') AS v")),
    ok = barrel:close(Plain),
    barrel_docdb:delete_db(maps:get(docdb, Plain)),
    ok.
