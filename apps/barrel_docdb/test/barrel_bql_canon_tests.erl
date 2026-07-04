-module(barrel_bql_canon_tests).

-include_lib("eunit/include/eunit.hrl").

canon(Source) ->
    canon(Source, #{}).

canon(Source, Params) ->
    {ok, Tokens, _} = barrel_bql_lexer:string(Source),
    {ok, Ast} = barrel_bql_parser:parse(Tokens),
    {ok, Canon} = barrel_bql_lower:canonicalize(Ast, Params),
    Canon.

canon_error(Source) ->
    canon_error(Source, #{}).

canon_error(Source, Params) ->
    {ok, Tokens, _} = barrel_bql_lexer:string(Source),
    {ok, Ast} = barrel_bql_parser:parse(Tokens),
    {error, {invalid_query, _Loc, Reason}} =
        barrel_bql_lower:canonicalize(Ast, Params),
    Reason.

conjuncts(Source) ->
    #{where := Where} = canon(Source),
    Where.

one_conjunct(Source) ->
    [Conjunct] = conjuncts(Source),
    Conjunct.

%%--------------------------------------------------------------------
%% Params
%%--------------------------------------------------------------------

param_substitution_test() ->
    #{where := [C]} =
        canon("SELECT * FROM db WHERE org = $org",
              #{<<"org">> => <<"acme">>}),
    ?assertMatch(
        {cmp, _, '=', {path, _, base, [<<"org">>]}, {lit, _, <<"acme">>}},
        C).

param_in_table_fn_test() ->
    #{source := {table_fn, _, <<"bm25_top_k">>, Args}} =
        canon("SELECT * FROM bm25_top_k($q, k => 3) AS m",
              #{<<"q">> => <<"hello">>}),
    ?assertMatch([{pos, {lit, _, <<"hello">>}}, {named, <<"k">>, _}], Args).

unbound_param_test() ->
    ?assertEqual({unbound_param, <<"org">>},
                 canon_error("SELECT * FROM db WHERE org = $org")).

invalid_param_value_test() ->
    ?assertEqual(
        {invalid_param, <<"org">>},
        canon_error("SELECT * FROM db WHERE org = $org",
                    #{<<"org">> => #{bad => value}})).

%%--------------------------------------------------------------------
%% Alias resolution and path tagging
%%--------------------------------------------------------------------

alias_stripping_test() ->
    #{select := [#{path := Path}]} = canon("SELECT d.name FROM db AS d"),
    ?assertMatch({path, _, base, [<<"name">>]}, Path).

bare_field_is_base_test() ->
    #{select := [#{path := Path}]} = canon("SELECT name FROM db d"),
    ?assertMatch({path, _, base, [<<"name">>]}, Path).

default_alias_is_collection_name_test() ->
    #{alias := Alias, select := [#{path := Path}]} =
        canon("SELECT db.x FROM db"),
    ?assertEqual(<<"db">>, Alias),
    ?assertMatch({path, _, base, [<<"x">>]}, Path).

non_alias_first_component_stays_test() ->
    #{select := [#{path := Path}]} = canon("SELECT other.x FROM db AS d"),
    ?assertMatch({path, _, base, [<<"other">>, <<"x">>]}, Path).

range_variable_shadows_field_test() ->
    %% d.d means field "d" of the doc, not a doc field named "d.d"
    #{select := [#{path := Path}]} = canon("SELECT d.d FROM db AS d"),
    ?assertMatch({path, _, base, [<<"d">>]}, Path).

bare_source_reference_test() ->
    #{select := [#{path := Path}]} = canon("SELECT d FROM db AS d"),
    ?assertMatch({path, _, base, []}, Path).

unnest_tagging_test() ->
    Canon = canon("SELECT t FROM db d, UNNEST(d.tags) AS t "
                  "WHERE t = 'erlang' AND t.author = 'joe' AND d.type = 'post'"),
    #{unnest := #{path := UPath},
      select := [#{path := SPath}],
      where := [W1, W2, W3]} = Canon,
    ?assertMatch({path, _, base, [<<"tags">>]}, UPath),
    ?assertMatch({path, _, unnest, []}, SPath),
    ?assertMatch({cmp, _, '=', {path, _, unnest, []}, {lit, _, <<"erlang">>}}, W1),
    ?assertMatch({cmp, _, '=', {path, _, unnest, [<<"author">>]}, _}, W2),
    ?assertMatch({cmp, _, '=', {path, _, base, [<<"type">>]}, _}, W3).

unnest_self_reference_test() ->
    {ok, Tokens, _} = barrel_bql_lexer:string(
        "SELECT * FROM db d, UNNEST(t.x) AS t"),
    {ok, Ast} = barrel_bql_parser:parse(Tokens),
    ?assertMatch(
        {error, {invalid_query, _, {invalid_unnest_path, self_reference}}},
        barrel_bql_lower:canonicalize(Ast, #{})).

unnest_of_source_test() ->
    {ok, Tokens, _} = barrel_bql_lexer:string(
        "SELECT * FROM db d, UNNEST(d) AS t"),
    {ok, Ast} = barrel_bql_parser:parse(Tokens),
    ?assertMatch(
        {error, {invalid_query, _, {invalid_unnest_path, source_reference}}},
        barrel_bql_lower:canonicalize(Ast, #{})).

table_fn_alias_required_test() ->
    ?assertEqual({alias_required, table_fn},
                 canon_error("SELECT * FROM vector_top_k('q', k => 2)")).

index_components_test() ->
    #{select := [#{path := Path}]} =
        canon("SELECT d.tags[0].name FROM db AS d"),
    ?assertMatch({path, _, base, [<<"tags">>, 0, <<"name">>]}, Path).

%%--------------------------------------------------------------------
%% Operand normalization
%%--------------------------------------------------------------------

operand_flip_test() ->
    C = one_conjunct("SELECT * FROM db WHERE 21 <= age"),
    ?assertMatch(
        {cmp, _, '>=', {path, _, base, [<<"age">>]}, {lit, _, 21}}, C).

operand_flip_equality_test() ->
    C = one_conjunct("SELECT * FROM db WHERE 'x' = a"),
    ?assertMatch({cmp, _, '=', {path, _, base, [<<"a">>]}, _}, C).

constant_predicate_test() ->
    ?assertEqual({constant_predicate, cmp},
                 canon_error("SELECT * FROM db WHERE 1 = 2")).

path_to_path_test() ->
    ?assertEqual({unsupported, path_to_path_comparison},
                 canon_error("SELECT * FROM db WHERE a = b")).

path_between_bound_test() ->
    ?assertEqual({unsupported, path_between_bound},
                 canon_error("SELECT * FROM db WHERE a BETWEEN b AND 5")).

constant_in_test() ->
    ?assertEqual({constant_predicate, in},
                 canon_error("SELECT * FROM db WHERE 1 IN (1, 2)")).

%%--------------------------------------------------------------------
%% BETWEEN expansion
%%--------------------------------------------------------------------

between_expands_to_conjuncts_test() ->
    [C1, C2] = conjuncts("SELECT * FROM db WHERE age BETWEEN 18 AND 65"),
    ?assertMatch({cmp, _, '>=', {path, _, base, [<<"age">>]}, {lit, _, 18}}, C1),
    ?assertMatch({cmp, _, '<=', {path, _, base, [<<"age">>]}, {lit, _, 65}}, C2).

not_between_is_a_disjunction_test() ->
    C = one_conjunct("SELECT * FROM db WHERE age NOT BETWEEN 18 AND 65"),
    ?assertMatch(
        {'or', _,
         {cmp, _, '<', _, {lit, _, 18}},
         {cmp, _, '>', _, {lit, _, 65}}},
        C).

%%--------------------------------------------------------------------
%% NOT dualization: the full table. Engine leaves are
%% exists-and-satisfies, so canonical output must never contain a
%% 'not' node over a bare path predicate.
%%--------------------------------------------------------------------

not_cmp_dualization_test() ->
    Table = [
        {"NOT a = 1", '!='},
        {"NOT a != 1", '='},
        {"NOT a < 1", '>='},
        {"NOT a <= 1", '>'},
        {"NOT a > 1", '<='},
        {"NOT a >= 1", '<'}
    ],
    lists:foreach(
        fun({Where, Op}) ->
            C = one_conjunct("SELECT * FROM db WHERE " ++ Where),
            ?assertMatch({cmp, _, ActualOp, _, _} when ActualOp =:= Op, C)
        end,
        Table).

not_is_null_family_test() ->
    ?assertMatch({is_null, _, _, true},
        one_conjunct("SELECT * FROM db WHERE NOT a IS NULL")),
    ?assertMatch({is_null, _, _, false},
        one_conjunct("SELECT * FROM db WHERE NOT a IS NOT NULL")),
    ?assertMatch({is_missing, _, _, true},
        one_conjunct("SELECT * FROM db WHERE NOT a IS MISSING")),
    ?assertMatch({is_missing, _, _, false},
        one_conjunct("SELECT * FROM db WHERE NOT a IS NOT MISSING")).

not_in_like_contains_test() ->
    ?assertMatch({in, _, _, _, true},
        one_conjunct("SELECT * FROM db WHERE NOT a IN (1, 2)")),
    ?assertMatch({in, _, _, _, false},
        one_conjunct("SELECT * FROM db WHERE NOT a NOT IN (1, 2)")),
    ?assertMatch({like, _, _, <<"x%">>, true},
        one_conjunct("SELECT * FROM db WHERE NOT a LIKE 'x%'")),
    ?assertMatch({contains, _, _, _, true},
        one_conjunct("SELECT * FROM db WHERE NOT CONTAINS(tags, 'x')")).

de_morgan_and_test() ->
    C = one_conjunct("SELECT * FROM db WHERE NOT (a = 1 AND b = 2)"),
    ?assertMatch(
        {'or', _, {cmp, _, '!=', _, _}, {cmp, _, '!=', _, _}}, C).

de_morgan_or_flattens_test() ->
    %% NOT (a=1 OR b=2) -> a!=1 AND b!=2 -> two top-level conjuncts
    [C1, C2] = conjuncts("SELECT * FROM db WHERE NOT (a = 1 OR b = 2)"),
    ?assertMatch({cmp, _, '!=', {path, _, base, [<<"a">>]}, _}, C1),
    ?assertMatch({cmp, _, '!=', {path, _, base, [<<"b">>]}, _}, C2).

double_negation_test() ->
    C = one_conjunct("SELECT * FROM db WHERE NOT NOT a = 1"),
    ?assertMatch({cmp, _, '=', _, _}, C).

not_between_negated_twice_test() ->
    [C1, C2] = conjuncts("SELECT * FROM db WHERE NOT (a NOT BETWEEN 1 AND 5)"),
    ?assertMatch({cmp, _, '>=', _, {lit, _, 1}}, C1),
    ?assertMatch({cmp, _, '<=', _, {lit, _, 5}}, C2).

%%--------------------------------------------------------------------
%% Conjunction flattening
%%--------------------------------------------------------------------

and_flattening_test() ->
    [C1, C2, C3] =
        conjuncts("SELECT * FROM db WHERE a = 1 AND b = 2 AND c = 3"),
    ?assertMatch({cmp, _, '=', {path, _, base, [<<"a">>]}, _}, C1),
    ?assertMatch({cmp, _, '=', {path, _, base, [<<"b">>]}, _}, C2),
    ?assertMatch({cmp, _, '=', {path, _, base, [<<"c">>]}, _}, C3).

or_is_one_conjunct_test() ->
    C = one_conjunct("SELECT * FROM db WHERE a = 1 OR b = 2"),
    ?assertMatch({'or', _, {cmp, _, '=', _, _}, {cmp, _, '=', _, _}}, C).

no_where_is_empty_conjuncts_test() ->
    ?assertEqual([], conjuncts("SELECT * FROM db")).
