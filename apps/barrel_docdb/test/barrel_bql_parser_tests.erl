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
%% Errors
%%--------------------------------------------------------------------

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
