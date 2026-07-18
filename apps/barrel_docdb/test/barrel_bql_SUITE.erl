%%%-------------------------------------------------------------------
%%% @doc End-to-end BQL tests against a real database: sargability
%%% preserved through lowering, execution correctness, UNNEST, id
%%% scans, continuations, and a differential gate proving BQL results
%%% equal hand-written query specs.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_bql_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(DB, <<"bql_suite_db">>).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, bql}].

groups() ->
    [{bql, [sequence], [
        sargability_preserved,
        basic_execution,
        projection_shapes,
        order_limit_offset,
        id_scans,
        unnest_execution,
        continuation_streaming,
        params_e2e,
        empty_results,
        differential_gate,
        deleted_docs_excluded,
        query_rejections
    ]}].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    DataDir = "/tmp/barrel_test_bql",
    os:cmd("rm -rf " ++ DataDir),
    {ok, _} = barrel_docdb:create_db(?DB, #{data_dir => DataDir}),
    seed_docs(),
    [{data_dir, DataDir} | Config].

end_per_suite(Config) ->
    barrel_docdb:delete_db(?DB),
    ok = application:stop(barrel_docdb),
    os:cmd("rm -rf " ++ ?config(data_dir, Config)),
    ok.

seed_docs() ->
    Docs = [
        #{<<"id">> => <<"user:1">>, <<"type">> => <<"user">>,
          <<"name">> => <<"Alice">>, <<"age">> => 30,
          <<"email">> => <<"alice@x.io">>, <<"status">> => <<"active">>,
          <<"org">> => <<"acme">>,
          <<"address">> => #{<<"city">> => <<"Paris">>},
          <<"tags">> => [<<"erlang">>, <<"db">>]},
        #{<<"id">> => <<"user:2">>, <<"type">> => <<"user">>,
          <<"name">> => <<"Bob">>, <<"age">> => 25,
          <<"email">> => <<"bob@y.io">>, <<"status">> => <<"pending">>,
          <<"org">> => <<"acme">>, <<"tags">> => [<<"rust">>]},
        #{<<"id">> => <<"user:3">>, <<"type">> => <<"user">>,
          <<"name">> => <<"Carol">>, <<"age">> => 35,
          <<"status">> => <<"active">>, <<"org">> => <<"globex">>,
          <<"tags">> => []},
        #{<<"id">> => <<"user:4">>, <<"type">> => <<"user">>,
          <<"name">> => <<"Dave">>, <<"age">> => null,
          <<"status">> => <<"inactive">>, <<"org">> => <<"acme">>},
        #{<<"id">> => <<"post:1">>, <<"type">> => <<"post">>,
          <<"title">> => <<"Intro to Erlang">>, <<"rank">> => 5,
          <<"author">> => <<"user:1">>,
          <<"tags">> => [<<"erlang">>, <<"intro">>],
          <<"comments">> => [
              #{<<"author">> => <<"bob">>, <<"stars">> => 4},
              #{<<"author">> => <<"carol">>, <<"stars">> => 5}]},
        #{<<"id">> => <<"post:2">>, <<"type">> => <<"post">>,
          <<"title">> => <<"Rust ORM notes">>, <<"rank">> => 3,
          <<"author">> => <<"user:2">>,
          <<"tags">> => [<<"rust">>, <<"orm">>]},
        #{<<"id">> => <<"post:3">>, <<"type">> => <<"post">>,
          <<"title">> => <<"BM25 deep dive">>, <<"rank">> => 4,
          <<"tags">> => [<<"search">>, <<"bm25">>, <<"erlang">>]},
        #{<<"id">> => <<"misc:1">>, <<"type">> => <<"misc">>}
    ],
    lists:foreach(
        fun(Doc) -> {ok, _} = barrel_docdb:put_doc(?DB, Doc) end,
        Docs).

%%====================================================================
%% Helpers
%%====================================================================

rows(Bql) ->
    rows(Bql, #{}).

rows(Bql, Opts) ->
    {ok, Rows, _Meta} = barrel_docdb:query(?DB, Bql, Opts),
    Rows.

ids(Bql) ->
    lists:sort([maps:get(<<"id">>, Row) || Row <- rows(Bql)]).

%%====================================================================
%% Cases
%%====================================================================

%% Lowered specs must keep the engine's index classification: BQL is a
%% surface syntax, not a planner bypass.
sargability_preserved(_Config) ->
    Expect = [
        {"SELECT * FROM db WHERE type = 'user'", index_seek},
        {"SELECT * FROM db WHERE type = 'user' AND status = 'active'",
         multi_index},
        {"SELECT * FROM db WHERE age > 26", index_scan},
        {"SELECT * FROM db WHERE email LIKE 'alice@%'", index_scan},
        {"SELECT * FROM db WHERE email IS NOT MISSING", index_scan},
        {"SELECT * FROM db WHERE type = 'user' OR rank = 5", full_scan},
        {"SELECT * FROM db WHERE tags IN ('a')", full_scan}
    ],
    lists:foreach(
        fun({Bql, Strategy}) ->
            {ok, #{spec := Spec}} = barrel_bql:compile(Bql),
            {ok, #{strategy := Actual}} = barrel_docdb:explain(?DB, Spec),
            ?assertEqual({Bql, Strategy}, {Bql, Actual})
        end,
        Expect).

basic_execution(_Config) ->
    ?assertEqual([<<"user:1">>, <<"user:3">>],
        ids("SELECT * FROM db WHERE type = 'user' AND status = 'active'")),
    %% age > 26: null and missing ages are filtered out
    ?assertEqual([<<"user:1">>, <<"user:3">>],
        ids("SELECT * FROM db WHERE type = 'user' AND age > 26")),
    ?assertEqual([<<"user:1">>, <<"user:2">>],
        ids("SELECT * FROM db WHERE status IN ('pending') OR org = 'acme' "
            "AND age <= 30")),
    ?assertEqual([<<"user:1">>],
        ids("SELECT * FROM db WHERE email LIKE 'alice@%'")),
    ?assertEqual([<<"user:1">>, <<"user:2">>],
        ids("SELECT * FROM db WHERE email LIKE '%@_.io'")),
    ?assertEqual([<<"user:4">>],
        ids("SELECT * FROM db WHERE type = 'user' AND age IS NULL")),
    ?assertEqual([<<"user:3">>, <<"user:4">>],
        ids("SELECT * FROM db WHERE type = 'user' AND email IS MISSING")),
    ?assertEqual([<<"user:1">>, <<"user:2">>, <<"user:3">>],
        ids("SELECT * FROM db WHERE type = 'user' AND age IS NOT NULL")),
    %% NOT dualizes: status must exist and differ
    ?assertEqual([<<"user:2">>, <<"user:4">>],
        ids("SELECT * FROM db WHERE type = 'user' AND NOT status = 'active'")),
    ?assertEqual([<<"post:1">>, <<"post:3">>, <<"user:1">>],
        ids("SELECT * FROM db WHERE CONTAINS(tags, 'erlang')")),
    %% #5: infix CONTAINS and `IN <array>' are the same membership
    ?assertEqual([<<"post:1">>, <<"post:3">>, <<"user:1">>],
        ids("SELECT * FROM db WHERE tags CONTAINS 'erlang'")),
    ?assertEqual([<<"post:1">>, <<"post:3">>, <<"user:1">>],
        ids("SELECT * FROM db WHERE 'erlang' IN tags")),
    ?assertEqual([<<"user:2">>],
        ids("SELECT * FROM db WHERE type = 'user' AND age BETWEEN 20 AND 26")),
    ?assertEqual([<<"post:2">>, <<"post:3">>],
        ids("SELECT * FROM db WHERE type = 'post' AND "
            "rank NOT BETWEEN 5 AND 9")),
    ?assertEqual([<<"user:1">>],
        ids("SELECT * FROM db WHERE address.city = 'Paris'")).

projection_shapes(_Config) ->
    [Row] = rows("SELECT name AS who, address.city FROM db "
                 "WHERE id = 'user:1'"),
    ?assertEqual(#{<<"id">> => <<"user:1">>, <<"who">> => <<"Alice">>,
                   <<"city">> => <<"Paris">>}, Row),
    %% a missing attribute leaves its key absent, PartiQL style
    [Row3] = rows("SELECT name, email FROM db WHERE id = 'user:3'"),
    ?assertEqual(#{<<"id">> => <<"user:3">>, <<"name">> => <<"Carol">>},
                 Row3),
    %% SELECT * flattens: no doc key, no _rev, id present
    [Star] = rows("SELECT * FROM db WHERE id = 'misc:1'"),
    ?assertEqual(#{<<"id">> => <<"misc:1">>, <<"type">> => <<"misc">>},
                 Star).

order_limit_offset(_Config) ->
    Names = fun(Rows) -> [maps:get(<<"name">>, R) || R <- Rows] end,
    %% numbers sort before null (Erlang term order); null age last
    ?assertEqual([<<"Bob">>, <<"Alice">>, <<"Carol">>, <<"Dave">>],
        Names(rows("SELECT name FROM db WHERE type = 'user' "
                   "ORDER BY age"))),
    ?assertEqual([<<"Dave">>, <<"Carol">>, <<"Alice">>, <<"Bob">>],
        Names(rows("SELECT name FROM db WHERE type = 'user' "
                   "ORDER BY age DESC"))),
    ?assertEqual([<<"Alice">>, <<"Carol">>],
        Names(rows("SELECT name FROM db WHERE type = 'user' "
                   "ORDER BY age LIMIT 2 OFFSET 1"))),
    %% string sort key
    ?assertEqual([<<"BM25 deep dive">>, <<"Intro to Erlang">>,
                  <<"Rust ORM notes">>],
        [maps:get(<<"title">>, R)
         || R <- rows("SELECT title FROM db WHERE type = 'post' "
                      "ORDER BY title")]).

id_scans(_Config) ->
    ?assertEqual([<<"user:2">>],
                 ids("SELECT * FROM db WHERE id = 'user:2'")),
    ?assertEqual([<<"post:1">>, <<"post:2">>, <<"post:3">>],
                 ids("SELECT * FROM db WHERE id LIKE 'post:%'")),
    ?assertEqual([<<"user:2">>, <<"user:3">>],
                 ids("SELECT * FROM db WHERE id >= 'user:2' "
                     "AND id < 'user:4'")),
    ?assertEqual([],
                 ids("SELECT * FROM db WHERE id >= 'z' AND id < 'a'")),
    %% id scan combined with a where condition
    ?assertEqual([<<"user:1">>, <<"user:3">>],
                 ids("SELECT * FROM db WHERE id LIKE 'user:%' "
                     "AND status = 'active'")).

unnest_execution(_Config) ->
    %% one row per element
    Tags = [maps:get(<<"t">>, R)
            || R <- rows("SELECT t FROM db d, UNNEST(d.tags) AS t "
                         "WHERE d.type = 'post'")],
    ?assertEqual(
        [<<"bm25">>, <<"erlang">>, <<"erlang">>, <<"intro">>,
         <<"orm">>, <<"rust">>, <<"search">>],
        lists:sort(Tags)),
    %% element predicate + doc predicate + doc projection
    Rows = rows("SELECT d.title, t AS tag FROM db d, "
                "UNNEST(d.tags) AS t "
                "WHERE d.type = 'post' AND t = 'erlang'"),
    ?assertEqual(
        [{<<"BM25 deep dive">>, <<"erlang">>},
         {<<"Intro to Erlang">>, <<"erlang">>}],
        lists:sort([{maps:get(<<"title">>, R), maps:get(<<"tag">>, R)}
                    || R <- Rows])),
    %% map elements with nested predicates
    [Comment] = rows("SELECT c.author AS author FROM db d, "
                     "UNNEST(d.comments) AS c WHERE c.stars >= 5"),
    ?assertEqual(<<"carol">>, maps:get(<<"author">>, Comment)),
    %% empty and missing arrays yield no rows
    ?assertEqual([],
        rows("SELECT t FROM db d, UNNEST(d.tags) AS t "
             "WHERE d.id = 'user:3'")),
    ?assertEqual([],
        rows("SELECT t FROM db d, UNNEST(d.tags) AS t "
             "WHERE d.id = 'user:4'")),
    %% SELECT * with unnest carries the element under its alias
    [StarRow | _] = rows("SELECT * FROM db d, UNNEST(d.tags) AS t "
                         "WHERE d.id = 'user:2'"),
    ?assertEqual(<<"rust">>, maps:get(<<"t">>, StarRow)),
    ?assertEqual(<<"Bob">>, maps:get(<<"name">>, StarRow)),
    %% ordering by element value
    Sorted = [maps:get(<<"t">>, R)
              || R <- rows("SELECT t FROM db d, UNNEST(d.tags) AS t "
                           "WHERE d.id = 'post:3' ORDER BY t DESC")],
    ?assertEqual([<<"search">>, <<"erlang">>, <<"bm25">>], Sorted).

continuation_streaming(_Config) ->
    {ok, All, #{has_more := false}} =
        barrel_docdb:query(?DB, "SELECT * FROM db"),
    Expected = lists:sort([maps:get(<<"id">>, R) || R <- All]),
    Collected = collect_chunks("SELECT * FROM db", undefined, []),
    ?assertEqual(Expected, lists:sort(Collected)),
    ?assert(length(Expected) >= 8).

collect_chunks(Bql, Cont, Acc) ->
    Opts = case Cont of
        undefined -> #{chunk_size => 3};
        _ -> #{chunk_size => 3, continuation => Cont}
    end,
    {ok, Rows, Meta} = barrel_docdb:query(?DB, Bql, Opts),
    Ids = [maps:get(<<"id">>, R) || R <- Rows],
    case Meta of
        #{has_more := true, continuation := Cont1} ->
            collect_chunks(Bql, Cont1, Acc ++ Ids);
        _ ->
            Acc ++ Ids
    end.

params_e2e(_Config) ->
    {ok, Rows, _} = barrel_docdb:query(?DB,
        "SELECT name FROM db WHERE type = 'user' AND org = $org "
        "ORDER BY name",
        #{params => #{<<"org">> => <<"acme">>}}),
    ?assertEqual([<<"Alice">>, <<"Bob">>, <<"Dave">>],
                 [maps:get(<<"name">>, R) || R <- Rows]).

empty_results(_Config) ->
    {ok, [], #{has_more := false, count := 0}} =
        barrel_docdb:query(?DB, "SELECT * FROM db LIMIT 0"),
    ?assertEqual([], ids("SELECT * FROM db WHERE type = 'nothing'")).

%% The semantic gate: BQL output must equal the equivalent hand-written
%% query spec on the same data.
differential_gate(_Config) ->
    Pairs = [
        {"SELECT * FROM db WHERE type = 'user'",
         #{where => [{path, [<<"type">>], <<"user">>}]}},
        {"SELECT * FROM db WHERE type = 'user' AND status = 'active'",
         #{where => [{path, [<<"type">>], <<"user">>},
                     {path, [<<"status">>], <<"active">>}]}},
        {"SELECT * FROM db WHERE age > 26",
         #{where => [{compare, [<<"age">>], '>', 26}]}},
        {"SELECT * FROM db WHERE age >= 25 AND age <= 30",
         #{where => [{compare, [<<"age">>], '>=', 25},
                     {compare, [<<"age">>], '=<', 30}]}},
        {"SELECT * FROM db WHERE age != 30",
         #{where => [{compare, [<<"age">>], '=/=', 30}]}},
        {"SELECT * FROM db WHERE status IN ('active', 'pending')",
         #{where => [{in, [<<"status">>], [<<"active">>, <<"pending">>]}]}},
        {"SELECT * FROM db WHERE email LIKE 'alice@%'",
         #{where => [{prefix, [<<"email">>], <<"alice@">>}]}},
        {"SELECT * FROM db WHERE name LIKE 'A%e'",
         #{where => [{regex, [<<"name">>], <<"^A.*e$">>}]}},
        {"SELECT * FROM db WHERE email IS MISSING",
         #{where => [{missing, [<<"email">>]}]}},
        {"SELECT * FROM db WHERE email IS NOT MISSING",
         #{where => [{exists, [<<"email">>]}]}},
        {"SELECT * FROM db WHERE age IS NULL",
         #{where => [{'or', [{path, [<<"age">>], null},
                             {missing, [<<"age">>]}]}]}},
        {"SELECT * FROM db WHERE type = 'user' OR rank = 5",
         #{where => [{'or', [{path, [<<"type">>], <<"user">>},
                             {path, [<<"rank">>], 5}]}]}},
        {"SELECT * FROM db WHERE CONTAINS(tags, 'erlang')",
         #{where => [{contains, [<<"tags">>], <<"erlang">>}]}},
        {"SELECT * FROM db WHERE NOT status = 'active'",
         #{where => [{compare, [<<"status">>], '=/=', <<"active">>}]}},
        {"SELECT * FROM db WHERE status NOT IN ('active')",
         #{where => [{'and', [{exists, [<<"status">>]},
                              {'not', {in, [<<"status">>],
                                       [<<"active">>]}}]}]}},
        {"SELECT * FROM db WHERE address.city = 'Paris'",
         #{where => [{path, [<<"address">>, <<"city">>], <<"Paris">>}]}}
    ],
    lists:foreach(
        fun({Bql, Spec}) ->
            {ok, BqlRows, _} = barrel_docdb:query(?DB, Bql),
            {ok, FindRows, _} = barrel_docdb:find(?DB, Spec),
            BqlIds = lists:sort([maps:get(<<"id">>, R) || R <- BqlRows]),
            FindIds = lists:sort([maps:get(<<"id">>, R) || R <- FindRows]),
            ?assertEqual({Bql, FindIds}, {Bql, BqlIds})
        end,
        Pairs).

deleted_docs_excluded(_Config) ->
    {ok, #{<<"rev">> := Rev}} = barrel_docdb:put_doc(
        ?DB, #{<<"id">> => <<"zzz:tmp">>, <<"type">> => <<"user">>,
               <<"status">> => <<"active">>}),
    ?assert(lists:member(<<"zzz:tmp">>,
                         ids("SELECT * FROM db WHERE type = 'user'"))),
    {ok, _} = barrel_docdb:delete_doc(?DB, <<"zzz:tmp">>, #{rev => Rev}),
    ?assertNot(lists:member(<<"zzz:tmp">>,
                            ids("SELECT * FROM db WHERE type = 'user'"))).

query_rejections(_Config) ->
    ?assertEqual(
        {error, {table_fn_requires_barrel, vector_top_k}},
        barrel_docdb:query(?DB, "SELECT * FROM vector_top_k('q') AS v")),
    ?assertEqual(
        {error, {unsupported, subscribe}},
        barrel_docdb:query(?DB, "SELECT * FROM db WHERE a = 1 SUBSCRIBE")),
    ?assertMatch(
        {error, {parse_error, {1, _}, _}},
        barrel_docdb:query(?DB, "SELECT FROM")),
    ?assertMatch(
        {error, {invalid_query, _, {reserved_field, _}}},
        barrel_docdb:query(?DB, "SELECT * FROM db WHERE _x = 1")),
    %% continuations only make sense on streamable plans
    ?assertEqual(
        {error, {unsupported, continuation}},
        barrel_docdb:query(?DB, "SELECT * FROM db ORDER BY name",
                           #{continuation => <<"tok">>})).
