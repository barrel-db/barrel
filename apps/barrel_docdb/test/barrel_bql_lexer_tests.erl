-module(barrel_bql_lexer_tests).

-include_lib("eunit/include/eunit.hrl").

tokens(Source) ->
    {ok, Tokens, _EndLoc} = barrel_bql_lexer:string(Source),
    Tokens.

types(Source) ->
    [element(1, T) || T <- tokens(Source)].

%%--------------------------------------------------------------------
%% Keywords and identifiers
%%--------------------------------------------------------------------

keyword_case_insensitive_test() ->
    ?assertEqual([{select, {1, 1}}], tokens("SELECT")),
    ?assertEqual([{select, {1, 1}}], tokens("select")),
    ?assertEqual([{select, {1, 1}}], tokens("SeLeCt")).

all_keywords_test() ->
    Pairs = [
        {"SELECT", select}, {"FROM", from}, {"WHERE", where},
        {"ORDER", order}, {"BY", by}, {"LIMIT", limit},
        {"OFFSET", offset}, {"AS", as}, {"UNNEST", unnest},
        {"ASC", asc}, {"DESC", desc}, {"AND", 'and'}, {"OR", 'or'},
        {"NOT", 'not'}, {"IN", in}, {"LIKE", like},
        {"BETWEEN", between}, {"IS", is}, {"NULL", null},
        {"MISSING", missing}, {"TRUE", true}, {"FALSE", false},
        {"SUBSCRIBE", subscribe}, {"CONTAINS", contains}
    ],
    lists:foreach(
        fun({Word, Atom}) ->
            ?assertEqual([{Atom, {1, 1}}], tokens(Word)),
            ?assertEqual([{Atom, {1, 1}}], tokens(string:lowercase(Word)))
        end,
        Pairs).

ident_is_case_sensitive_test() ->
    ?assertEqual([{ident, {1, 1}, <<"Users">>}], tokens("Users")),
    ?assertEqual([{ident, {1, 1}, <<"users">>}], tokens("users")).

quoted_ident_test() ->
    ?assertEqual([{ident, {1, 1}, <<"a field">>}], tokens("\"a field\"")),
    %% "" escapes a quote inside a quoted identifier
    ?assertEqual([{ident, {1, 1}, <<"a\"b">>}], tokens("\"a\"\"b\"")),
    %% quoted identifiers are never keyword-checked
    ?assertEqual([{ident, {1, 1}, <<"select">>}], tokens("\"select\"")).

%%--------------------------------------------------------------------
%% Literals
%%--------------------------------------------------------------------

string_test() ->
    ?assertEqual([{string, {1, 1}, <<"abc">>}], tokens("'abc'")),
    ?assertEqual([{string, {1, 1}, <<"a'b">>}], tokens("'a''b'")),
    ?assertEqual([{string, {1, 1}, <<>>}], tokens("''")).

unicode_string_test() ->
    ?assertEqual(
        [{string, {1, 1}, <<"héllo"/utf8>>}],
        tokens(unicode:characters_to_list(<<"'héllo'"/utf8>>))).

integer_test() ->
    ?assertEqual([{integer, {1, 1}, 42}], tokens("42")),
    ?assertEqual([{integer, {1, 1}, 0}], tokens("0")).

float_test() ->
    ?assertEqual([{float, {1, 1}, 3.14}], tokens("3.14")),
    ?assertEqual([{float, {1, 1}, 2.5e10}], tokens("2.5e10")),
    ?assertEqual([{float, {1, 1}, 2.5e10}], tokens("2.5E+10")),
    ?assertEqual([{float, {1, 1}, 1.5e-3}], tokens("1.5e-3")).

trailing_dot_is_not_a_float_test() ->
    %% "1." lexes as integer then dot; the grammar rejects it
    ?assertEqual([{integer, {1, 1}, 1}, {'.', {1, 2}}], tokens("1.")).

negative_number_is_two_tokens_test() ->
    ?assertEqual([{'-', {1, 1}}, {integer, {1, 2}, 7}], tokens("-7")).

param_test() ->
    ?assertEqual([{param, {1, 1}, <<"who">>}], tokens("$who")),
    ?assertEqual(
        [{'=', {1, 1}}, {param, {1, 3}, <<"k_1">>}],
        tokens("= $k_1")).

%%--------------------------------------------------------------------
%% Operators, punctuation, comments
%%--------------------------------------------------------------------

two_char_operators_test() ->
    ?assertEqual(['=>'], types("=>")),
    ?assertEqual(['<='], types("<=")),
    ?assertEqual(['>='], types(">=")),
    ?assertEqual(['<>'], types("<>")),
    ?assertEqual(['!='], types("!=")).

single_char_operators_test() ->
    ?assertEqual(
        ['=', '<', '>', '(', ')', ',', '.', '[', ']', '*', '-'],
        types("= < > ( ) , . [ ] * -")).

longest_match_test() ->
    %% "<=" is one token, not '<' '='
    ?assertEqual(['<='], types("<=")),
    ?assertEqual(['<', ident], types("<a")).

comment_test() ->
    ?assertEqual(
        [{select, {1, 1}}, {ident, {2, 1}, <<"x">>}],
        tokens("SELECT -- pick x\nx")),
    %% comment at end of input, no trailing newline
    ?assertEqual([{select, {1, 1}}], tokens("SELECT -- done")).

%%--------------------------------------------------------------------
%% Locations and errors
%%--------------------------------------------------------------------

column_locations_test() ->
    ?assertEqual(
        [{select, {1, 1}}, {ident, {1, 8}, <<"foo">>}],
        tokens("SELECT foo")),
    ?assertEqual(
        [{select, {1, 1}}, {ident, {2, 3}, <<"foo">>}],
        tokens("SELECT\n  foo")).

statement_shape_test() ->
    ?assertEqual(
        [select, '*', from, ident, where, ident, '.', ident, '=',
         string, 'and', ident, '.', ident, '>=', integer, limit, integer],
        types("SELECT * FROM db WHERE d.type = 'user' AND d.age >= 21 LIMIT 10")).

lex_error_location_test() ->
    {error, {Loc, barrel_bql_lexer, _Reason}, _EndLoc} =
        barrel_bql_lexer:string("foo 'unterminated"),
    ?assertMatch({1, _Col}, Loc),
    {error, {Loc2, barrel_bql_lexer, _}, _} =
        barrel_bql_lexer:string("a ; b"),
    ?assertEqual({1, 3}, Loc2).

%% Parser round trips live in barrel_bql_parser_tests.
