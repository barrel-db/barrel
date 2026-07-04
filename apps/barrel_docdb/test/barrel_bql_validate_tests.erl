-module(barrel_bql_validate_tests).

-include_lib("eunit/include/eunit.hrl").

validate(Source) ->
    validate(Source, #{}).

validate(Source, Params) ->
    {ok, Tokens, _} = barrel_bql_lexer:string(Source),
    {ok, Ast} = barrel_bql_parser:parse(Tokens),
    {ok, Canon} = barrel_bql_lower:canonicalize(Ast, Params),
    barrel_bql_lower:validate(Canon).

reason(Source) ->
    reason(Source, #{}).

reason(Source, Params) ->
    {error, {invalid_query, _Loc, Reason}} = validate(Source, Params),
    Reason.

%%--------------------------------------------------------------------
%% Happy paths
%%--------------------------------------------------------------------

valid_queries_test() ->
    Queries = [
        "SELECT * FROM db",
        "SELECT a, b.c AS x FROM db WHERE a = 1 AND b > 2 LIMIT 10 OFFSET 2",
        "SELECT * FROM db WHERE a LIKE 'p%' OR b IN (1, 2)",
        "SELECT t FROM db d, UNNEST(d.tags) AS t WHERE t = 'x'",
        "SELECT * FROM db WHERE id = 'doc1'",
        "SELECT * FROM db WHERE id >= 'a' AND id < 'b'",
        "SELECT * FROM db WHERE id LIKE 'user:%'",
        "SELECT * FROM db WHERE a = 1 SUBSCRIBE",
        "SELECT * FROM db WHERE a = 1 LIMIT 5 SUBSCRIBE",
        "SELECT * FROM db LIMIT 0",
        "SELECT * FROM db ORDER BY a DESC",
        "SELECT v._score, title FROM vector_top_k('q', k => 5) AS v",
        "SELECT v._score AS s, v._distance FROM vector_top_k('q') AS v",
        "SELECT m._score FROM bm25_top_k('q', k => 3) AS m",
        "SELECT h._score FROM hybrid_top_k('q') AS h WHERE h.lang = 'en'"
    ],
    lists:foreach(
        fun(Q) -> ?assertEqual({Q, ok}, {Q, validate(Q)}) end,
        Queries).

%%--------------------------------------------------------------------
%% Reserved fields and wildcards
%%--------------------------------------------------------------------

reserved_field_test() ->
    ?assertEqual({reserved_field, <<"_internal">>},
                 reason("SELECT * FROM db WHERE _internal = 1")),
    ?assertEqual({reserved_field, <<"_rev">>},
                 reason("SELECT d._rev FROM db AS d")).

score_on_collection_is_reserved_test() ->
    ?assertEqual({reserved_field, <<"_score">>},
                 reason("SELECT d._score FROM db AS d")).

distance_only_on_vector_test() ->
    ?assertEqual(ok,
                 validate("SELECT v._distance FROM vector_top_k('q') AS v")),
    ?assertEqual({reserved_field, <<"_distance">>},
                 reason("SELECT m._distance FROM bm25_top_k('q') AS m")),
    ?assertEqual({reserved_field, <<"_distance">>},
                 reason("SELECT h._distance FROM hybrid_top_k('q') AS h")).

nested_underscore_key_is_allowed_test() ->
    %% only TOP-LEVEL _-fields are reserved
    ?assertEqual(ok, validate("SELECT a.b FROM db WHERE a._x = 1")),
    %% unnest elements are not top-level
    ?assertEqual(ok,
        validate("SELECT t FROM db d, UNNEST(d.xs) AS t WHERE t._y = 1")).

wildcard_rejected_test() ->
    ?assertEqual({unsupported, wildcard_path, use_unnest},
                 reason("SELECT * FROM db WHERE a[*].b = 1")),
    ?assertEqual({unsupported, wildcard_path, use_unnest},
                 reason("SELECT a[*] FROM db")),
    ?assertEqual({unsupported, wildcard_path, use_unnest},
                 reason("SELECT * FROM db ORDER BY a[*]")).

%%--------------------------------------------------------------------
%% NULL literal traps
%%--------------------------------------------------------------------

eq_null_test() ->
    ?assertEqual({use_is_null, cmp},
                 reason("SELECT * FROM db WHERE a = NULL")),
    ?assertEqual({use_is_null, cmp},
                 reason("SELECT * FROM db WHERE a != NULL")),
    ?assertEqual({use_is_null, cmp},
                 reason("SELECT * FROM db WHERE a > NULL")).

null_in_list_test() ->
    ?assertEqual({use_is_null, in},
                 reason("SELECT * FROM db WHERE a IN (1, NULL)")).

null_between_bound_test() ->
    %% BETWEEN expands to comparisons, so the null bound is caught there
    ?assertEqual({use_is_null, cmp},
                 reason("SELECT * FROM db WHERE a BETWEEN NULL AND 5")).

%%--------------------------------------------------------------------
%% Document id rules
%%--------------------------------------------------------------------

id_conditions_test() ->
    ?assertEqual({unsupported, id_condition},
                 reason("SELECT * FROM db WHERE id != 'x'")),
    ?assertEqual({unsupported, id_condition},
                 reason("SELECT * FROM db WHERE id = 5")),
    ?assertEqual({unsupported, id_condition},
                 reason("SELECT * FROM db WHERE id LIKE '%x'")),
    ?assertEqual({unsupported, id_condition},
                 reason("SELECT * FROM db WHERE id LIKE 'a_c%'")),
    ?assertEqual({unsupported, id_condition},
                 reason("SELECT * FROM db WHERE id IN ('a', 'b')")),
    ?assertEqual({unsupported, id_condition},
                 reason("SELECT * FROM db WHERE id IS MISSING")),
    ?assertEqual({unsupported, id_condition},
                 reason("SELECT * FROM db WHERE NOT id = 'x'")).

id_in_disjunction_test() ->
    ?assertEqual({unsupported, id_in_disjunction},
                 reason("SELECT * FROM db WHERE id = 'x' OR a = 1")).

%%--------------------------------------------------------------------
%% ORDER BY and SELECT rules
%%--------------------------------------------------------------------

multi_key_order_by_test() ->
    ?assertEqual({unsupported, multi_key_order_by},
                 reason("SELECT * FROM db ORDER BY a, b DESC")).

order_by_source_reference_test() ->
    ?assertEqual({invalid_order_key, source_reference},
                 reason("SELECT * FROM db AS d ORDER BY d")).

duplicate_output_name_test() ->
    ?assertEqual({duplicate_output_name, <<"a">>},
                 reason("SELECT a, b AS a FROM db")),
    %% derived names collide too: both project as "x"
    ?assertEqual({duplicate_output_name, <<"x">>},
                 reason("SELECT a.x, b.x FROM db")).

indexed_projection_needs_alias_test() ->
    ?assertEqual({alias_required, indexed_projection},
                 reason("SELECT a[0] FROM db")),
    ?assertEqual(ok, validate("SELECT a[0] AS first FROM db")).

bare_source_reference_in_where_test() ->
    ?assertEqual({unsupported, bare_source_reference},
                 reason("SELECT * FROM db AS d WHERE d = 1")).

%%--------------------------------------------------------------------
%% Table functions
%%--------------------------------------------------------------------

unknown_table_function_test() ->
    ?assertEqual({unknown_table_function, <<"foo_top_k">>},
                 reason("SELECT * FROM foo_top_k('q') AS v")).

table_fn_arg_errors_test() ->
    %% missing positional query
    ?assertEqual({invalid_table_fn_arg, vector_top_k, query},
                 reason("SELECT * FROM vector_top_k(k => 5) AS v")),
    %% non-string query
    ?assertEqual({invalid_table_fn_arg, vector_top_k, query},
                 reason("SELECT * FROM vector_top_k(42) AS v")),
    %% two positionals
    ?assertEqual({invalid_table_fn_arg, vector_top_k, extra_positional},
                 reason("SELECT * FROM vector_top_k('a', 'b') AS v")),
    %% bad k
    ?assertEqual({invalid_table_fn_arg, bm25_top_k, k},
                 reason("SELECT * FROM bm25_top_k('q', k => 'x') AS m")),
    ?assertEqual({invalid_table_fn_arg, bm25_top_k, k},
                 reason("SELECT * FROM bm25_top_k('q', k => 0) AS m")),
    %% ef_search is vector-only
    ?assertEqual({invalid_table_fn_arg, bm25_top_k, ef_search},
                 reason("SELECT * FROM bm25_top_k('q', ef_search => 9) AS m")),
    %% unknown option
    ?assertEqual({invalid_table_fn_arg, vector_top_k, <<"fuzz">>},
                 reason("SELECT * FROM vector_top_k('q', fuzz => 1) AS v")),
    %% duplicate option
    ?assertEqual({invalid_table_fn_arg, vector_top_k, {duplicate, <<"k">>}},
                 reason("SELECT * FROM vector_top_k('q', k => 1, k => 2) AS v")).

%%--------------------------------------------------------------------
%% SUBSCRIBE restrictions
%%--------------------------------------------------------------------

score_in_where_test() ->
    %% _score/_distance are SELECT and ORDER BY columns only
    ?assertEqual({unsupported, score_in_where},
                 reason("SELECT * FROM vector_top_k('q') AS v "
                        "WHERE v._score > 0.5")),
    ?assertEqual({unsupported, score_in_where},
                 reason("SELECT * FROM vector_top_k('q') AS v "
                        "WHERE v._distance < 0.1 OR v.lang = 'en'")).

subscribe_restrictions_test() ->
    ?assertEqual({unsupported_with_subscribe, order_by},
                 reason("SELECT * FROM db ORDER BY a SUBSCRIBE")),
    ?assertEqual({unsupported_with_subscribe, offset},
                 reason("SELECT * FROM db OFFSET 2 SUBSCRIBE")),
    ?assertEqual({unsupported_with_subscribe, unnest},
                 reason("SELECT t FROM db d, UNNEST(d.xs) AS t SUBSCRIBE")),
    ?assertEqual({unsupported_with_subscribe, table_function},
                 reason("SELECT * FROM vector_top_k('q') AS v SUBSCRIBE")).

%%--------------------------------------------------------------------
%% Aliases
%%--------------------------------------------------------------------

duplicate_alias_test() ->
    ?assertEqual({duplicate_alias, <<"t">>},
                 reason("SELECT * FROM db t, UNNEST(tags) AS t")).
