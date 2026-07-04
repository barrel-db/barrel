-module(barrel_bql_parser_tests).

-include_lib("eunit/include/eunit.hrl").

parse(Source) ->
    {ok, Tokens, _} = barrel_bql_lexer:string(Source),
    {ok, Ast} = barrel_bql_parser:parse(Tokens),
    Ast.

parse_error(Source) ->
    {ok, Tokens, _} = barrel_bql_lexer:string(Source),
    {error, {Loc, barrel_bql_parser, _Msg}} = barrel_bql_parser:parse(Tokens),
    Loc.

%%--------------------------------------------------------------------
%% Statement skeleton
%%--------------------------------------------------------------------

select_star_test() ->
    ?assertEqual(
        #{type => select,
          select => star,
          from => #{source => {collection, {1, 15}, <<"db">>},
                    alias => undefined,
                    unnest => undefined},
          where => undefined,
          order_by => [],
          limit => undefined,
          offset => undefined,
          subscribe => false},
        parse("SELECT * FROM db")).

alias_as_test() ->
    #{from := #{alias := A}} = parse("SELECT * FROM db AS d"),
    ?assertEqual(<<"d">>, A).

alias_bare_test() ->
    #{from := #{alias := A}} = parse("SELECT * FROM db d"),
    ?assertEqual(<<"d">>, A).

quoted_collection_test() ->
    #{from := #{source := {collection, _, Name}}} =
        parse("SELECT * FROM \"my db\""),
    ?assertEqual(<<"my db">>, Name).

%%--------------------------------------------------------------------
%% Projections and paths
%%--------------------------------------------------------------------

projection_list_test() ->
    #{select := [P1, P2, P3]} =
        parse("SELECT a, b.c AS x, d[0] FROM db"),
    ?assertEqual(
        #{expr => {path, {1, 8}, [{key, <<"a">>}]},
          as => undefined, loc => {1, 8}},
        P1),
    ?assertEqual(
        #{expr => {path, {1, 11}, [{key, <<"b">>}, {key, <<"c">>}]},
          as => <<"x">>, loc => {1, 11}},
        P2),
    ?assertEqual(
        #{expr => {path, {1, 21}, [{key, <<"d">>}, {index, 0}]},
          as => undefined, loc => {1, 21}},
        P3).

deep_path_test() ->
    #{select := [#{expr := Path}]} =
        parse("SELECT d.a.b[2].c FROM db"),
    ?assertEqual(
        {path, {1, 8},
         [{key, <<"d">>}, {key, <<"a">>}, {key, <<"b">>},
          {index, 2}, {key, <<"c">>}]},
        Path).

wildcard_path_test() ->
    #{select := [#{expr := Path}]} = parse("SELECT tags[*] FROM db"),
    ?assertEqual({path, {1, 8}, [{key, <<"tags">>}, wildcard]}, Path).

keyword_as_path_key_test() ->
    #{select := [#{expr := P1}, #{expr := P2}]} =
        parse("SELECT d.order, d.limit FROM db"),
    ?assertEqual({path, {1, 8}, [{key, <<"d">>}, {key, <<"order">>}]}, P1),
    ?assertEqual({path, {1, 17}, [{key, <<"d">>}, {key, <<"limit">>}]}, P2).

quoted_first_path_component_test() ->
    #{select := [#{expr := Path}]} = parse("SELECT \"limit\".x FROM db"),
    ?assertEqual({path, {1, 8}, [{key, <<"limit">>}, {key, <<"x">>}]}, Path).

%%--------------------------------------------------------------------
%% ORDER BY / LIMIT / OFFSET / SUBSCRIBE
%%--------------------------------------------------------------------

order_by_default_asc_test() ->
    #{order_by := Order} = parse("SELECT * FROM db ORDER BY title"),
    ?assertEqual([{{path, {1, 27}, [{key, <<"title">>}]}, asc}], Order).

order_by_desc_test() ->
    #{order_by := Order} = parse("SELECT * FROM db ORDER BY title DESC"),
    ?assertEqual([{{path, {1, 27}, [{key, <<"title">>}]}, desc}], Order).

order_by_multi_key_parses_test() ->
    %% the validator rejects >1 key; the grammar accepts the list
    #{order_by := [{_, asc}, {_, desc}]} =
        parse("SELECT * FROM db ORDER BY a, b DESC").

limit_offset_test() ->
    #{limit := Limit, offset := Offset} =
        parse("SELECT * FROM db LIMIT 10 OFFSET 5"),
    ?assertEqual({integer, {1, 24}, 10}, Limit),
    ?assertEqual({integer, {1, 34}, 5}, Offset).

subscribe_test() ->
    #{subscribe := true} = parse("SELECT * FROM db SUBSCRIBE"),
    #{subscribe := false} = parse("SELECT * FROM db").

full_clause_order_test() ->
    Ast = parse("SELECT a FROM db d ORDER BY a LIMIT 3 OFFSET 1 SUBSCRIBE"),
    ?assertMatch(
        #{select := [_],
          from := #{alias := <<"d">>},
          order_by := [_],
          limit := {integer, _, 3},
          offset := {integer, _, 1},
          subscribe := true},
        Ast).

%%--------------------------------------------------------------------
%% WHERE: predicates and precedence
%%--------------------------------------------------------------------

where(Source) ->
    #{where := Where} = parse(Source),
    Where.

simple_equality_test() ->
    ?assertEqual(
        {cmp, {1, 31}, '=',
         {path, {1, 24}, [{key, <<"d">>}, {key, <<"type">>}]},
         {lit, {1, 33}, <<"user">>}},
        where("SELECT * FROM db WHERE d.type = 'user'")).

comparison_operators_test() ->
    Ops = [{"=", '='}, {"!=", '!='}, {"<>", '!='}, {"<", '<'},
           {"<=", '<='}, {">", '>'}, {">=", '>='}],
    lists:foreach(
        fun({Sql, Atom}) ->
            W = where("SELECT * FROM db WHERE a " ++ Sql ++ " 1"),
            ?assertMatch({cmp, _, Op, _, _} when Op =:= Atom, W)
        end,
        Ops).

or_binds_looser_than_and_test() ->
    W = where("SELECT * FROM db WHERE a = 1 OR b = 2 AND c = 3"),
    ?assertMatch(
        {'or', _,
         {cmp, _, '=', {path, _, [{key, <<"a">>}]}, _},
         {'and', _,
          {cmp, _, '=', {path, _, [{key, <<"b">>}]}, _},
          {cmp, _, '=', {path, _, [{key, <<"c">>}]}, _}}},
        W).

parens_override_precedence_test() ->
    W = where("SELECT * FROM db WHERE (a = 1 OR b = 2) AND c = 3"),
    ?assertMatch({'and', _, {'or', _, _, _}, {cmp, _, '=', _, _}}, W).

not_nesting_test() ->
    W = where("SELECT * FROM db WHERE NOT NOT a = 1"),
    ?assertMatch({'not', _, {'not', _, {cmp, _, '=', _, _}}}, W).

not_binds_tighter_than_and_test() ->
    W = where("SELECT * FROM db WHERE NOT a = 1 AND b = 2"),
    ?assertMatch({'and', _, {'not', _, _}, {cmp, _, '=', _, _}}, W).

is_null_family_test() ->
    ?assertMatch({is_null, _, _, false},
                 where("SELECT * FROM db WHERE a IS NULL")),
    ?assertMatch({is_null, _, _, true},
                 where("SELECT * FROM db WHERE a IS NOT NULL")),
    ?assertMatch({is_missing, _, _, false},
                 where("SELECT * FROM db WHERE a IS MISSING")),
    ?assertMatch({is_missing, _, _, true},
                 where("SELECT * FROM db WHERE a IS NOT MISSING")).

in_list_test() ->
    W = where("SELECT * FROM db WHERE status IN ('a', 'b', 3, -4.5)"),
    ?assertMatch(
        {in, _, {path, _, [{key, <<"status">>}]},
         [{lit, _, <<"a">>}, {lit, _, <<"b">>}, {lit, _, 3},
          {lit, _, -4.5}],
         false},
        W),
    ?assertMatch({in, _, _, [_], true},
                 where("SELECT * FROM db WHERE a NOT IN (1)")).

like_test() ->
    ?assertMatch({like, _, _, <<"abc%">>, false},
                 where("SELECT * FROM db WHERE a LIKE 'abc%'")),
    ?assertMatch({like, _, _, <<"a_c">>, true},
                 where("SELECT * FROM db WHERE a NOT LIKE 'a_c'")).

between_test() ->
    W = where("SELECT * FROM db WHERE age BETWEEN 18 AND 65"),
    ?assertMatch(
        {between, _, {path, _, [{key, <<"age">>}]},
         {lit, _, 18}, {lit, _, 65}, false},
        W),
    ?assertMatch({between, _, _, _, _, true},
                 where("SELECT * FROM db WHERE age NOT BETWEEN 1 AND 5")).

between_then_and_test() ->
    %% the AND at 1 AND 5 belongs to BETWEEN; the second is a conjunction
    W = where("SELECT * FROM db WHERE a BETWEEN 1 AND 5 AND b = 2"),
    ?assertMatch(
        {'and', _, {between, _, _, _, _, false}, {cmp, _, '=', _, _}},
        W).

contains_test() ->
    W = where("SELECT * FROM db WHERE CONTAINS(tags, 'erlang')"),
    ?assertMatch(
        {contains, _, {path, _, [{key, <<"tags">>}]},
         {lit, _, <<"erlang">>}},
        W).

param_operand_test() ->
    W = where("SELECT * FROM db WHERE org = $org"),
    ?assertMatch({cmp, _, '=', _, {param, _, <<"org">>}}, W).

literal_on_the_left_parses_test() ->
    %% canonicalize flips it later; the grammar accepts both orders
    W = where("SELECT * FROM db WHERE 21 <= age"),
    ?assertMatch({cmp, _, '<=', {lit, _, 21}, {path, _, _}}, W).

keyword_path_key_in_where_test() ->
    W = where("SELECT * FROM db WHERE d.order = 1"),
    ?assertMatch(
        {cmp, _, '=', {path, _, [{key, <<"d">>}, {key, <<"order">>}]}, _},
        W).

boolean_and_null_literals_test() ->
    ?assertMatch({cmp, _, '=', _, {lit, _, true}},
                 where("SELECT * FROM db WHERE a = TRUE")),
    ?assertMatch({cmp, _, '=', _, {lit, _, false}},
                 where("SELECT * FROM db WHERE a = false")),
    ?assertMatch({cmp, _, '=', _, {lit, _, null}},
                 where("SELECT * FROM db WHERE a = NULL")).

%%--------------------------------------------------------------------
%% UNNEST and table functions
%%--------------------------------------------------------------------

unnest_test() ->
    #{from := From} =
        parse("SELECT d.name, t FROM db AS d, UNNEST(d.tags) AS t"),
    ?assertEqual(
        #{source => {collection, {1, 23}, <<"db">>},
          alias => <<"d">>,
          unnest => #{path => {path, {1, 39},
                               [{key, <<"d">>}, {key, <<"tags">>}]},
                      alias => <<"t">>,
                      loc => {1, 32}}},
        From).

table_fn_named_args_test() ->
    #{from := #{source := Source, alias := Alias}} =
        parse("SELECT * FROM vector_top_k('rust orm', k => 5, ef_search => 64) AS v"),
    ?assertEqual(<<"v">>, Alias),
    ?assertEqual(
        {table_fn, {1, 15}, <<"vector_top_k">>,
         [{pos, {lit, {1, 28}, <<"rust orm">>}},
          {named, <<"k">>, {lit, {1, 45}, 5}},
          {named, <<"ef_search">>, {lit, {1, 61}, 64}}]},
        Source).

table_fn_param_query_test() ->
    #{from := #{source := Source}} =
        parse("SELECT * FROM bm25_top_k($q, k => 3) AS m"),
    ?assertEqual(
        {table_fn, {1, 15}, <<"bm25_top_k">>,
         [{pos, {param, {1, 26}, <<"q">>}},
          {named, <<"k">>, {lit, {1, 35}, 3}}]},
        Source).

table_fn_with_where_test() ->
    Ast = parse("SELECT * FROM hybrid_top_k('q', k => 4) AS h "
                "WHERE h.lang = 'en' LIMIT 2"),
    ?assertMatch(
        #{from := #{source := {table_fn, _, <<"hybrid_top_k">>, _}},
          where := {cmp, _, '=', _, _},
          limit := {integer, _, 2}},
        Ast).

%%--------------------------------------------------------------------
%% Errors
%%--------------------------------------------------------------------

bare_boolean_path_is_rejected_test() ->
    %% a bare path is not a predicate; write d.active = true
    ?assertMatch({1, _}, parse_error("SELECT * FROM db WHERE d.active")),
    ?assertMatch({1, _},
                 parse_error("SELECT * FROM db WHERE a = 1 AND b")).

in_needs_literals_test() ->
    %% paths are not allowed inside an IN list
    ?assertMatch({1, _}, parse_error("SELECT * FROM db WHERE a IN (b)")).

like_needs_string_test() ->
    ?assertMatch({1, _}, parse_error("SELECT * FROM db WHERE a LIKE 5")).

missing_select_list_test() ->
    ?assertEqual({1, 8}, parse_error("SELECT FROM db")).

missing_from_test() ->
    %% input ends after the select list
    {ok, Tokens, _} = barrel_bql_lexer:string("SELECT a"),
    ?assertMatch({error, {_, barrel_bql_parser, _}},
                 barrel_bql_parser:parse(Tokens)).

double_select_test() ->
    ?assertEqual({1, 10}, parse_error("SELECT a SELECT")).

limit_needs_integer_test() ->
    ?assertEqual({1, 24}, parse_error("SELECT * FROM db LIMIT x")).

clause_order_is_fixed_test() ->
    %% OFFSET before LIMIT is a syntax error
    ?assertMatch({1, _}, parse_error("SELECT * FROM db OFFSET 1 LIMIT 2")).
