%% BQL scanner. Keywords are case-insensitive (PartiQL); identifiers are
%% case-sensitive. Bare identifiers are keyword-checked; double-quoted
%% identifiers never are ("" escapes a quote). Strings are single-quoted
%% with '' as the escape. $name is a named parameter. -- starts a line
%% comment. Token locations are {Line, Column} (error_location column in
%% xrl_opts). Negative numbers are grammar territory: the scanner only
%% emits '-'.

Definitions.

D = [0-9]
W = [a-zA-Z_][a-zA-Z0-9_]*

Rules.

{W}                            : keyword_or_ident(TokenChars, TokenLoc).
\"(\"\"|[^\"])*\"              : {token, {ident, TokenLoc, unquote(TokenChars, $\")}}.
'(''|[^'])*'                   : {token, {string, TokenLoc, unquote(TokenChars, $')}}.
{D}+\.{D}+((E|e)(\+|\-)?{D}+)? : {token, {float, TokenLoc, list_to_float(TokenChars)}}.
{D}+                           : {token, {integer, TokenLoc, list_to_integer(TokenChars)}}.
\${W}                          : {token, {param, TokenLoc, unicode:characters_to_binary(tl(TokenChars))}}.
=>|<=|>=|<>|!=                 : {token, {list_to_atom(TokenChars), TokenLoc}}.
[=<>(),\.\[\]\*\-]             : {token, {list_to_atom(TokenChars), TokenLoc}}.
--[^\n]*                       : skip_token.
(\s|\t|\r|\n)+                 : skip_token.

Erlang code.

keyword_or_ident(Chars, Loc) ->
    case maps:find(string:uppercase(Chars), keywords()) of
        {ok, Keyword} -> {token, {Keyword, Loc}};
        error -> {token, {ident, Loc, unicode:characters_to_binary(Chars)}}
    end.

keywords() ->
    #{
        "SELECT" => select,
        "FROM" => from,
        "WHERE" => where,
        "ORDER" => order,
        "BY" => by,
        "LIMIT" => limit,
        "OFFSET" => offset,
        "AS" => as,
        "UNNEST" => unnest,
        "ASC" => asc,
        "DESC" => desc,
        "AND" => 'and',
        "OR" => 'or',
        "NOT" => 'not',
        "IN" => in,
        "LIKE" => like,
        "BETWEEN" => between,
        "IS" => is,
        "NULL" => null,
        "MISSING" => missing,
        "TRUE" => true,
        "FALSE" => false,
        "SUBSCRIBE" => subscribe,
        "CONTAINS" => contains
    }.

unquote(Chars, Quote) ->
    Inner = lists:sublist(Chars, 2, length(Chars) - 2),
    unicode:characters_to_binary(collapse_quotes(Inner, Quote)).

collapse_quotes([Q, Q | Rest], Q) -> [Q | collapse_quotes(Rest, Q)];
collapse_quotes([C | Rest], Q) -> [C | collapse_quotes(Rest, Q)];
collapse_quotes([], _Q) -> [].
