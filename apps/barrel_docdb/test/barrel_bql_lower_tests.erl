-module(barrel_bql_lower_tests).

-include_lib("eunit/include/eunit.hrl").

plan(Source) ->
    plan(Source, #{}).

plan(Source, Params) ->
    {ok, Plan} = barrel_bql:compile(Source, #{params => Params}),
    Plan.

spec_where(Source) ->
    #{spec := #{where := Where}} = plan(Source),
    Where.

warnings(Source) ->
    #{warnings := Warnings} = plan(Source),
    Warnings.

%%--------------------------------------------------------------------
%% Operator table -> engine conditions (+ full-scan warnings)
%%--------------------------------------------------------------------

equality_test() ->
    ?assertEqual([{path, [<<"a">>], 1}],
                 spec_where("SELECT * FROM db WHERE a = 1")),
    ?assertEqual([], warnings("SELECT * FROM db WHERE a = 1")).

not_equal_test() ->
    ?assertEqual([{compare, [<<"a">>], '=/=', 1}],
                 spec_where("SELECT * FROM db WHERE a != 1")),
    ?assertEqual([{full_scan, '!='}],
                 warnings("SELECT * FROM db WHERE a != 1")).

range_operators_test() ->
    ?assertEqual([{compare, [<<"a">>], '<', 1}],
                 spec_where("SELECT * FROM db WHERE a < 1")),
    ?assertEqual([{compare, [<<"a">>], '=<', 1}],
                 spec_where("SELECT * FROM db WHERE a <= 1")),
    ?assertEqual([{compare, [<<"a">>], '>', 1}],
                 spec_where("SELECT * FROM db WHERE a > 1")),
    ?assertEqual([{compare, [<<"a">>], '>=', 1}],
                 spec_where("SELECT * FROM db WHERE a >= 1")),
    ?assertEqual([], warnings("SELECT * FROM db WHERE a >= 1")).

between_is_two_sargable_conjuncts_test() ->
    ?assertEqual(
        [{compare, [<<"a">>], '>=', 1}, {compare, [<<"a">>], '=<', 5}],
        spec_where("SELECT * FROM db WHERE a BETWEEN 1 AND 5")),
    ?assertEqual([], warnings("SELECT * FROM db WHERE a BETWEEN 1 AND 5")).

in_test() ->
    ?assertEqual([{in, [<<"a">>], [1, 2]}],
                 spec_where("SELECT * FROM db WHERE a IN (1, 2)")),
    ?assertEqual([{full_scan, in}],
                 warnings("SELECT * FROM db WHERE a IN (1, 2)")).

not_in_has_exists_guard_test() ->
    ?assertEqual(
        [{'and', [{exists, [<<"a">>]},
                  {'not', {in, [<<"a">>], [1, 2]}}]}],
        spec_where("SELECT * FROM db WHERE a NOT IN (1, 2)")),
    ?assertEqual([{full_scan, not_in}],
                 warnings("SELECT * FROM db WHERE a NOT IN (1, 2)")).

like_prefix_test() ->
    ?assertEqual([{prefix, [<<"a">>], <<"p">>}],
                 spec_where("SELECT * FROM db WHERE a LIKE 'p%'")),
    ?assertEqual([], warnings("SELECT * FROM db WHERE a LIKE 'p%'")).

like_regex_test() ->
    ?assertEqual([{regex, [<<"a">>], <<"^p.*x$">>}],
                 spec_where("SELECT * FROM db WHERE a LIKE 'p%x'")),
    ?assertEqual([{full_scan, like}],
                 warnings("SELECT * FROM db WHERE a LIKE 'p%x'")).

like_regex_escapes_metachars_test() ->
    %% 'a.b%' is prefix-shaped (the dot is a literal in LIKE)
    ?assertEqual([{prefix, [<<"a">>], <<"a.b">>}],
                 spec_where("SELECT * FROM db WHERE a LIKE 'a.b%'")),
    ?assertEqual([{regex, [<<"a">>], <<"^a\\.b.*c$">>}],
                 spec_where("SELECT * FROM db WHERE a LIKE 'a.b%c'")),
    ?assertEqual([{regex, [<<"a">>], <<"^x.y$">>}],
                 spec_where("SELECT * FROM db WHERE a LIKE 'x_y'")).

not_like_test() ->
    ?assertEqual(
        [{'and', [{exists, [<<"a">>]},
                  {'not', {prefix, [<<"a">>], <<"p">>}}]}],
        spec_where("SELECT * FROM db WHERE a NOT LIKE 'p%'")).

is_missing_family_test() ->
    ?assertEqual([{missing, [<<"a">>]}],
                 spec_where("SELECT * FROM db WHERE a IS MISSING")),
    ?assertEqual([{exists, [<<"a">>]}],
                 spec_where("SELECT * FROM db WHERE a IS NOT MISSING")),
    ?assertEqual([], warnings("SELECT * FROM db WHERE a IS NOT MISSING")).

is_null_family_test() ->
    ?assertEqual(
        [{'or', [{path, [<<"a">>], null}, {missing, [<<"a">>]}]}],
        spec_where("SELECT * FROM db WHERE a IS NULL")),
    ?assertEqual(
        [{compare, [<<"a">>], '=/=', null}],
        spec_where("SELECT * FROM db WHERE a IS NOT NULL")).

contains_test() ->
    ?assertEqual([{contains, [<<"tags">>], <<"x">>}],
                 spec_where("SELECT * FROM db WHERE CONTAINS(tags, 'x')")).

or_test() ->
    ?assertEqual(
        [{'or', [{path, [<<"a">>], 1}, {path, [<<"b">>], 2}]}],
        spec_where("SELECT * FROM db WHERE a = 1 OR b = 2")),
    ?assertEqual([{full_scan, 'or'}],
                 warnings("SELECT * FROM db WHERE a = 1 OR b = 2")).

nested_and_inside_or_test() ->
    ?assertEqual(
        [{'or', [{path, [<<"a">>], 1},
                 {'and', [{path, [<<"b">>], 2}, {path, [<<"c">>], 3}]}]}],
        spec_where("SELECT * FROM db WHERE a = 1 OR (b = 2 AND c = 3)")).

%%--------------------------------------------------------------------
%% Document-id lowering
%%--------------------------------------------------------------------

id_spec(Source) ->
    #{spec := Spec} = plan(Source),
    {maps:get(id_prefix, Spec, undefined),
     maps:get(id_range, Spec, undefined),
     maps:is_key(where, Spec)}.

id_equality_test() ->
    ?assertEqual({undefined, {<<"x">>, <<"x", 0>>}, false},
                 id_spec("SELECT * FROM db WHERE id = 'x'")).

id_range_test() ->
    ?assertEqual({undefined, {<<"a">>, <<"b">>}, false},
                 id_spec("SELECT * FROM db WHERE id >= 'a' AND id < 'b'")),
    ?assertEqual({undefined, {<<"a", 0>>, undefined}, false},
                 id_spec("SELECT * FROM db WHERE id > 'a'")),
    ?assertEqual({undefined, {undefined, <<"b", 0>>}, false},
                 id_spec("SELECT * FROM db WHERE id <= 'b'")).

id_prefix_test() ->
    ?assertEqual({<<"user:">>, undefined, false},
                 id_spec("SELECT * FROM db WHERE id LIKE 'user:%'")).

id_prefix_merges_with_range_test() ->
    %% prefix becomes a range when combined: [max(user:, user:m), user;)
    ?assertEqual(
        {undefined, {<<"user:m">>, <<"user;">>}, false},
        id_spec("SELECT * FROM db WHERE id LIKE 'user:%' AND id >= 'user:m'")).

id_contradiction_is_empty_test() ->
    #{post := #{empty := true}} =
        plan("SELECT * FROM db WHERE id >= 'b' AND id < 'a'"),
    #{post := #{empty := true}} =
        plan("SELECT * FROM db WHERE id = 'x' AND id = 'y'").

id_scan_combines_with_where_test() ->
    #{spec := Spec} =
        plan("SELECT * FROM db WHERE id >= 'a' AND type = 'x'"),
    ?assertEqual({<<"a">>, undefined}, maps:get(id_range, Spec)),
    ?assertEqual([{path, [<<"type">>], <<"x">>}], maps:get(where, Spec)).

unconstrained_query_scans_ids_test() ->
    #{spec := Spec} = plan("SELECT * FROM db"),
    ?assertEqual({undefined, undefined}, maps:get(id_range, Spec)),
    ?assertNot(maps:is_key(where, Spec)).

%%--------------------------------------------------------------------
%% Placement: limit/offset pushdown, streamable, order, unnest
%%--------------------------------------------------------------------

limit_pushdown_test() ->
    #{spec := Spec, post := Post, streamable := true} =
        plan("SELECT a FROM db WHERE b = 1 LIMIT 5 OFFSET 2"),
    ?assertEqual(5, maps:get(limit, Spec)),
    ?assertEqual(2, maps:get(offset, Spec)),
    ?assertEqual(undefined, maps:get(limit, Post)),
    ?assertEqual(0, maps:get(offset, Post)).

order_by_blocks_pushdown_test() ->
    #{spec := Spec, post := Post, streamable := false,
      warnings := Warnings} =
        plan("SELECT a FROM db WHERE b = 1 ORDER BY a LIMIT 5"),
    ?assertNot(maps:is_key(limit, Spec)),
    ?assertEqual(5, maps:get(limit, Post)),
    ?assertEqual({{b, [<<"a">>]}, asc}, maps:get(order, Post)),
    ?assert(lists:member({order_by_materializes, [<<"a">>]}, Warnings)).

order_desc_test() ->
    #{post := #{order := Order}} =
        plan("SELECT * FROM db ORDER BY x.y DESC"),
    ?assertEqual({{b, [<<"x">>, <<"y">>]}, desc}, Order).

limit_zero_is_empty_test() ->
    #{post := #{empty := true}} = plan("SELECT * FROM db LIMIT 0").

unnest_seed_test() ->
    #{spec := Spec, unnest := Unnest, streamable := false} =
        plan("SELECT t FROM db d, UNNEST(d.tags) AS t"),
    ?assertEqual([{exists, [<<"tags">>]}], maps:get(where, Spec)),
    ?assertEqual(#{path => [<<"tags">>], alias => <<"t">>}, Unnest).

unnest_split_test() ->
    %% the worked example: base conjuncts stay sargable, element
    %% conjuncts go residual, ORDER BY and LIMIT run in the post stage
    Plan = plan("SELECT d.title, t AS tag FROM posts AS d, "
                "UNNEST(d.tags) AS t "
                "WHERE d.type = 'post' AND d.rank >= 3 AND t = 'erlang' "
                "ORDER BY d.title LIMIT 10"),
    #{spec := Spec, source := Source, unnest := Unnest, post := Post,
      streamable := false, subscribe := false} = Plan,
    ?assertEqual({collection, <<"posts">>}, Source),
    ?assertEqual(
        [{path, [<<"type">>], <<"post">>},
         {compare, [<<"rank">>], '>=', 3}],
        maps:get(where, Spec)),
    ?assertNot(maps:is_key(limit, Spec)),
    ?assertEqual(#{path => [<<"tags">>], alias => <<"t">>}, Unnest),
    ?assertEqual(
        #{residual => [{path, {u, []}, <<"erlang">>}],
          doc_where => [],
          order => {{b, [<<"title">>]}, asc},
          offset => 0,
          limit => 10,
          project => [{b, [<<"title">>], <<"title">>},
                      {u, [], <<"tag">>}],
          empty => false},
        Post).

mixed_unnest_or_goes_residual_test() ->
    #{spec := Spec, post := #{residual := Residual}} =
        plan("SELECT t FROM db d, UNNEST(d.xs) AS t "
             "WHERE t = 1 OR d.a = 2"),
    %% the whole disjunction is frame-residual; the seed keeps the
    %% engine query index-friendly
    ?assertEqual([{exists, [<<"xs">>]}], maps:get(where, Spec)),
    ?assertEqual(
        [{'or', [{path, {u, []}, 1}, {path, {b, [<<"a">>]}, 2}]}],
        Residual).

%%--------------------------------------------------------------------
%% SELECT lowering
%%--------------------------------------------------------------------

select_star_projects_star_test() ->
    #{post := #{project := star}} = plan("SELECT * FROM db").

projection_names_test() ->
    #{post := #{project := Project}} =
        plan("SELECT a, b.c AS x, d.e FROM db"),
    ?assertEqual(
        [{b, [<<"a">>], <<"a">>},
         {b, [<<"b">>, <<"c">>], <<"x">>},
         {b, [<<"d">>, <<"e">>], <<"e">>}],
        Project).

whole_doc_projection_test() ->
    #{post := #{project := Project}} = plan("SELECT d FROM db AS d"),
    ?assertEqual([{b, [], <<"d">>}], Project).

%%--------------------------------------------------------------------
%% Table functions
%%--------------------------------------------------------------------

table_fn_plan_test() ->
    Plan = plan("SELECT h._score, title FROM hybrid_top_k('q', k => 4) AS h "
                "WHERE h.lang = 'en' LIMIT 2"),
    #{spec := Spec, source := Source, post := Post,
      streamable := false} = Plan,
    ?assertEqual({table_fn, hybrid_top_k, #{query => <<"q">>, k => 4}},
                 Source),
    %% the engine is not consulted: fetch template only
    ?assertNot(maps:is_key(where, Spec)),
    ?assertNot(maps:is_key(id_range, Spec)),
    ?assertEqual([{path, [<<"lang">>], <<"en">>}],
                 maps:get(doc_where, Post)),
    ?assertEqual([], maps:get(residual, Post)),
    ?assertEqual(2, maps:get(limit, Post)),
    ?assertEqual(
        [{score, <<"_score">>, <<"_score">>},
         {b, [<<"title">>], <<"title">>}],
        maps:get(project, Post)).

table_fn_defaults_test() ->
    #{source := {table_fn, vector_top_k, Args}} =
        plan("SELECT * FROM vector_top_k('q') AS v"),
    ?assertEqual(#{query => <<"q">>, k => 10}, Args).

table_fn_ef_search_test() ->
    #{source := {table_fn, vector_top_k, Args}} =
        plan("SELECT * FROM vector_top_k('q', k => 3, ef_search => 64) AS v"),
    ?assertEqual(#{query => <<"q">>, k => 3, ef_search => 64}, Args).

table_fn_param_query_test() ->
    #{source := {table_fn, bm25_top_k, Args}} =
        plan("SELECT * FROM bm25_top_k($q, k => 7) AS m",
             #{<<"q">> => <<"needle">>}),
    ?assertEqual(#{query => <<"needle">>, k => 7}, Args).

score_order_test() ->
    #{post := #{order := Order}} =
        plan("SELECT v._distance AS d2 FROM vector_top_k('q') AS v "
             "ORDER BY v._distance"),
    ?assertEqual({{score, <<"_distance">>}, asc}, Order).

%%--------------------------------------------------------------------
%% Subscribe passthrough
%%--------------------------------------------------------------------

subscribe_flag_test() ->
    #{subscribe := true, streamable := true} =
        plan("SELECT * FROM db WHERE a = 1 SUBSCRIBE").
