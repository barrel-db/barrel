%% Step-1 skeleton: locations, case-insensitivity, string escapes and the
%% lexer/parser round trip. Grows with the lexer in step 2.
-module(barrel_bql_lexer_tests).

-include_lib("eunit/include/eunit.hrl").

tokens(Source) ->
    {ok, Tokens, _EndLoc} = barrel_bql_lexer:string(Source),
    Tokens.

keyword_case_insensitive_test() ->
    ?assertEqual([{select, {1, 1}}], tokens("SELECT")),
    ?assertEqual([{select, {1, 1}}], tokens("select")),
    ?assertEqual([{select, {1, 1}}], tokens("SeLeCt")).

ident_is_case_sensitive_test() ->
    ?assertEqual([{ident, {1, 1}, <<"Users">>}], tokens("Users")),
    ?assertEqual([{ident, {1, 1}, <<"users">>}], tokens("users")).

column_locations_test() ->
    ?assertEqual(
        [{select, {1, 1}}, {ident, {1, 8}, <<"foo">>}],
        tokens("SELECT foo")),
    ?assertEqual(
        [{select, {1, 1}}, {ident, {2, 3}, <<"foo">>}],
        tokens("SELECT\n  foo")).

string_test() ->
    ?assertEqual([{string, {1, 1}, <<"abc">>}], tokens("'abc'")),
    ?assertEqual([{string, {1, 1}, <<"a'b">>}], tokens("'a''b'")),
    ?assertEqual([{string, {1, 1}, <<>>}], tokens("''")).

integer_test() ->
    ?assertEqual([{integer, {1, 1}, 42}], tokens("42")).

lex_error_location_test() ->
    {error, {Loc, barrel_bql_lexer, _Reason}, _EndLoc} =
        barrel_bql_lexer:string("foo 'unterminated"),
    ?assertMatch({1, _Col}, Loc).

parser_round_trip_test() ->
    {ok, Tokens, _} = barrel_bql_lexer:string("SELECT a FROM b"),
    ?assertEqual({ok, {probe, <<"a">>, <<"b">>}},
                 barrel_bql_parser:parse(Tokens)).

parser_error_location_test() ->
    {ok, Tokens, _} = barrel_bql_lexer:string("SELECT a SELECT"),
    {error, {Loc, barrel_bql_parser, _Msg}} = barrel_bql_parser:parse(Tokens),
    ?assertEqual({1, 10}, Loc).
