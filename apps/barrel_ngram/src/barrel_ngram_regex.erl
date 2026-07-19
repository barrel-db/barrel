%%%-------------------------------------------------------------------
%%% @doc Regex to mandatory-trigram query (Russ Cox / Google Code Search).
%%%
%%% Turns a regex into a boolean trigram query that every matching
%%% document must satisfy, so intersecting it over the index yields a
%%% superset of matches that the real regex engine then confirms. The
%%% query is only ever a NECESSARY condition: wherever the analysis is
%%% unsure it emits `all' (no constraint), which is always sound and just
%%% widens the candidate set.
%%%
%%% A small recursive-descent parser builds an AST for a practical subset
%%% (adjacent literal bytes coalesced into one run, so `abcdef' yields
%%% every internal trigram). The analysis assigns each node a query:
%%% concatenation ANDs, alternation ORs, `*'/`?'/`.'/char-class/anchors
%%% contribute `all', `+' takes its child's query, and a literal run ANDs
%%% its trigrams. This deliberately stops short of the full Cox
%%% exact/prefix/suffix cross-product (boundary trigrams spanning
%%% alternations); literal-run coalescing already covers the common case.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_regex).

-export([trigram_query/1]).
%% exported for tests
-export([parse/1]).

-type query() :: {'and', [query()]} | {'or', [query()]}
               | {gram, barrel_ngram_selector:gram()} | all | none.
-export_type([query/0]).

%% @doc The mandatory-trigram query for a regex. Always sound; returns
%% `all' when the pattern carries no usable trigram constraint.
-spec trigram_query(binary()) -> query().
trigram_query(Bin) when is_binary(Bin) ->
    try
        {Node, _Rest} = p_alt(binary_to_list(Bin)),
        {_CanEmpty, Q} = info(Node),
        simplify(Q)
    catch
        _:_ -> all
    end.

%% @doc Parse to the internal AST (exported for tests).
-spec parse(binary()) -> term().
parse(Bin) ->
    {Node, _Rest} = p_alt(binary_to_list(Bin)),
    Node.

%%====================================================================
%% Parser (recursive descent over a char list)
%%====================================================================

%% alternation: cat ('|' cat)*
p_alt(Chars) ->
    {N, R} = p_cat(Chars),
    p_alt_rest(R, [N]).

p_alt_rest([$| | R], Acc) ->
    {N, R2} = p_cat(R),
    p_alt_rest(R2, [N | Acc]);
p_alt_rest(R, [Single]) ->
    {Single, R};
p_alt_rest(R, Acc) ->
    {{alt, lists:reverse(Acc)}, R}.

%% concatenation: quantified atoms until '|', ')', or end
p_cat(Chars) ->
    p_cat(Chars, []).

p_cat([C | _] = Chars, Items) when C =/= $|, C =/= $) ->
    {Atom, R1} = p_atom(Chars),
    {Quant, R2} = p_quant(R1),
    p_cat(R2, [{Atom, Quant} | Items]);
p_cat(Chars, Items) ->
    {build_cat(lists:reverse(Items)), Chars}.

%% atom
p_atom([$( | R]) ->
    {Node, R2} = p_alt(R),
    R3 = case R2 of [$) | Rr] -> Rr; _ -> R2 end,
    {{group, Node}, R3};
p_atom([$[ | R]) ->
    R2 = skip_class(R),
    {{class, ignored}, R2};
p_atom([$. | R]) -> {any, R};
p_atom([$^ | R]) -> {bol, R};
p_atom([$$ | R]) -> {eol, R};
p_atom([$\\, E | R]) -> {escape_atom(E), R};
p_atom([$\\]) -> {{lit, <<$\\>>}, []};
p_atom([C | R]) -> {{lit, <<C>>}, R}.

%% quantifier
p_quant([$* | R]) -> {star, R};
p_quant([$+ | R]) -> {plus, R};
p_quant([$? | R]) -> {quest, R};
p_quant([${ | R] = All) ->
    case try_brace(R) of
        {ok, Min, R2} -> {{rep, Min}, R2};
        error -> {none, All}
    end;
p_quant(R) ->
    {none, R}.

try_brace(Chars) ->
    case take_digits(Chars, []) of
        {[], _} -> error;
        {Ds, Rest} ->
            Min = list_to_integer(Ds),
            case Rest of
                [$} | R] -> {ok, Min, R};
                [$, | R1] ->
                    {_Max, R2} = take_digits(R1, []),
                    case R2 of
                        [$} | R] -> {ok, Min, R};
                        _ -> error
                    end;
                _ -> error
            end
    end.

take_digits([D | R], Acc) when D >= $0, D =< $9 ->
    take_digits(R, [D | Acc]);
take_digits(R, Acc) ->
    {lists:reverse(Acc), R}.

%% class: skip to the closing ']' (contents are treated as `any', so only
%% finding the right close matters). Handles a leading '^', a leading ']'
%% literal member, and '\x' escapes.
skip_class([$^ | R]) -> skip_class_body(strip_leading_bracket(R));
skip_class(R) -> skip_class_body(strip_leading_bracket(R)).

strip_leading_bracket([$] | R]) -> R;
strip_leading_bracket(R) -> R.

skip_class_body([$\\, _ | R]) -> skip_class_body(R);
skip_class_body([$] | R]) -> R;
skip_class_body([_ | R]) -> skip_class_body(R);
skip_class_body([]) -> [].

escape_atom($n) -> {lit, <<10>>};
escape_atom($t) -> {lit, <<9>>};
escape_atom($r) -> {lit, <<13>>};
escape_atom($f) -> {lit, <<12>>};
escape_atom($d) -> any;
escape_atom($D) -> any;
escape_atom($w) -> any;
escape_atom($W) -> any;
escape_atom($s) -> any;
escape_atom($S) -> any;
escape_atom($b) -> bol;   %% zero-width -> no trigram
escape_atom($B) -> bol;
escape_atom($A) -> bol;
escape_atom($Z) -> bol;
escape_atom($z) -> bol;
escape_atom(C) -> {lit, <<C>>}.   %% escaped literal (\. \* \( ...)

%% Build a concatenation node, coalescing runs of unquantified literal
%% bytes into a single {lit, Bytes}.
build_cat(Items) ->
    Nodes = coalesce(Items, <<>>, []),
    case Nodes of
        [] -> {lit, <<>>};
        [N] -> N;
        _ -> {cat, Nodes}
    end.

coalesce([], Buf, Acc) ->
    lists:reverse(flush(Buf, Acc));
coalesce([{{lit, <<Ch>>}, none} | Rest], Buf, Acc) ->
    coalesce(Rest, <<Buf/binary, Ch>>, Acc);
coalesce([{Atom, Quant} | Rest], Buf, Acc) ->
    Acc1 = flush(Buf, Acc),
    coalesce(Rest, <<>>, [apply_quant(atom_node(Atom), Quant) | Acc1]).

flush(<<>>, Acc) -> Acc;
flush(Buf, Acc) -> [{lit, Buf} | Acc].

atom_node({lit, B}) -> {lit, B};
atom_node(any) -> any;
atom_node({class, _}) -> any;
atom_node(bol) -> bol;
atom_node(eol) -> eol;
atom_node({group, Node}) -> Node.

apply_quant(Node, none) -> Node;
apply_quant(Node, star) -> {star, Node};
apply_quant(Node, plus) -> {plus, Node};
apply_quant(Node, quest) -> {quest, Node};
apply_quant(Node, {rep, Min}) when Min >= 1 -> Node;
apply_quant(Node, {rep, _}) -> {star, Node}.

%%====================================================================
%% Analysis: node -> {CanEmpty, Query}
%%====================================================================

info({lit, B}) ->
    {byte_size(B) =:= 0, {'and', [{gram, G} || G <- trigrams(B)]}};
info({cat, Xs}) ->
    Infos = [info(X) || X <- Xs],
    {lists:all(fun({E, _}) -> E end, Infos),
     {'and', [Q || {_, Q} <- Infos]}};
info({alt, Xs}) ->
    Infos = [info(X) || X <- Xs],
    {lists:any(fun({E, _}) -> E end, Infos),
     {'or', [Q || {_, Q} <- Infos]}};
info({star, _}) -> {true, all};
info({quest, _}) -> {true, all};
info({plus, X}) -> info(X);
info(any) -> {false, all};
info(bol) -> {true, all};
info(eol) -> {true, all};
info({group, X}) -> info(X).

trigrams(B) when byte_size(B) >= 3 ->
    N = byte_size(B),
    lists:usort([gram_at(B, I) || I <- lists:seq(0, N - 3)]);
trigrams(_) ->
    [].

gram_at(B, I) ->
    A = binary:at(B, I),
    Bb = binary:at(B, I + 1),
    C = binary:at(B, I + 2),
    (A bsl 16) bor (Bb bsl 8) bor C.

%%====================================================================
%% Simplify
%%====================================================================

simplify({'and', Qs}) ->
    Qs1 = [simplify(Q) || Q <- Qs],
    case lists:member(none, Qs1) of
        true -> none;
        false ->
            case flatten('and', [Q || Q <- Qs1, Q =/= all]) of
                [] -> all;
                [Single] -> Single;
                Flat -> {'and', Flat}
            end
    end;
simplify({'or', Qs}) ->
    Qs1 = [simplify(Q) || Q <- Qs],
    case lists:member(all, Qs1) of
        true -> all;
        false ->
            case flatten('or', [Q || Q <- Qs1, Q =/= none]) of
                [] -> none;
                [Single] -> Single;
                Flat -> {'or', Flat}
            end
    end;
simplify(Q) ->
    Q.

%% Merge one level of same-type children and dedup.
flatten(Op, Qs) ->
    lists:usort(
      lists:append(
        [case Q of {Op, Sub} -> Sub; _ -> [Q] end || Q <- Qs])).
