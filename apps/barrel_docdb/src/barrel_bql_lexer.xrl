%% BQL scanner. Keywords are case-insensitive (PartiQL); identifiers are
%% case-sensitive. Strings are single-quoted with '' as the escape.
%% Token locations are {Line, Column} (error_location column in xrl_opts).

Definitions.

D = [0-9]
W = [a-zA-Z_][a-zA-Z0-9_]*
S = '([^']|'')*'

Rules.

{W}            : keyword_or_ident(TokenChars, TokenLoc).
{S}            : {token, {string, TokenLoc, unquote_string(TokenChars)}}.
{D}+           : {token, {integer, TokenLoc, list_to_integer(TokenChars)}}.
(\s|\t|\r|\n)+ : skip_token.

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
        "WHERE" => where
    }.

unquote_string(Chars) ->
    Inner = lists:sublist(Chars, 2, length(Chars) - 2),
    unicode:characters_to_binary(collapse_quotes(Inner)).

collapse_quotes([$', $' | Rest]) -> [$' | collapse_quotes(Rest)];
collapse_quotes([C | Rest]) -> [C | collapse_quotes(Rest)];
collapse_quotes([]) -> [].
