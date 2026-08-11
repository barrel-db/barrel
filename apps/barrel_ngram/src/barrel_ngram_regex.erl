%%%-------------------------------------------------------------------
%%% @doc Regex to mandatory-trigram query (Russ Cox / Google Code Search),
%%% plus width/anchor analysis for the positional planner.
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
%%%
%%% `analyze/1' is the strict entry point: any construct outside an
%%% explicitly-supported list (lookarounds, backreferences, named groups,
%%% `\x' escapes, `\Q...\E', conditionals, a scoped/mid-pattern inline
%%% modifier) makes the whole pattern `unsupported' rather than being
%%% silently mis-parsed as literal text. A leading `(?i)'/`(?s)'/`(?m)' is
%%% the one inline-modifier form understood.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_regex).

-export([trigram_query/1, analyze/1, width_bound/1, literal_runs/1]).
%% exported for tests
-export([parse/1]).

-type query() :: {'and', [query()]} | {'or', [query()]}
               | {gram, barrel_ngram_selector:gram()} | all | none.
-export_type([query/0]).

-type width() :: {fixed, non_neg_integer()}
                | {bounded, non_neg_integer(), non_neg_integer() | infinity}
                | unbounded.
-export_type([width/0]).

-type width_info() :: #{
    width => width(),
    has_anchor_or_boundary => boolean(),
    leading_flags => [dotall | multiline | caseless]
}.
-export_type([width_info/0]).

-type literal_run() :: #{bytes := binary(),
                         prefix_max := non_neg_integer() | unbounded,
                         suffix_max := non_neg_integer() | unbounded}.
-export_type([literal_run/0]).

%% @doc The mandatory-trigram query for a regex -- always sound (never a
%% guessed-at partial parse of an unsupported construct); `all' when the
%% pattern is unsupported or carries no usable trigram constraint.
-spec trigram_query(binary()) -> query().
trigram_query(Bin) ->
    case analyze(Bin) of
        unsupported -> all;
        {ok, _AST, Query, _WidthInfo} -> Query
    end.

%% @doc Full analysis: AST, trigram query, and width/anchor/leading-modifier
%% info. See the moduledoc for exactly what makes a pattern `unsupported'.
-spec analyze(binary()) -> {ok, term(), query(), width_info()} | unsupported.
analyze(Bin) when is_binary(Bin) ->
    try
        {LeadingFlags, Body} = strip_leading_modifiers(binary_to_list(Bin)),
        {Node, Rest} = p_alt(Body),
        [] = Rest, %% must consume the whole pattern -- a stray ')' etc. is malformed
        {_CanEmpty, Query} = info(Node),
        WidthInfo = #{
            width => width_bound(Node),
            has_anchor_or_boundary => has_anchor_or_boundary(Node),
            leading_flags => LeadingFlags
        },
        {ok, Node, simplify(Query), WidthInfo}
    catch
        _:_ -> unsupported
    end.

%% @doc Parse to the internal AST (exported for tests). Raises on any
%% construct `analyze/1' treats as unsupported -- callers wanting the
%% fail-closed behavior should use `analyze/1', not this directly.
-spec parse(binary()) -> term().
parse(Bin) ->
    {_LeadingFlags, Body} = strip_leading_modifiers(binary_to_list(Bin)),
    {Node, _Rest} = p_alt(Body),
    Node.

%%====================================================================
%% Leading inline-modifier detection ( (?i) (?s) (?m), whole-pattern only )
%%====================================================================

strip_leading_modifiers(Chars) ->
    case take_leading_modifier_group(Chars) of
        {ok, Flags, Rest} -> {lists:usort(Flags), Rest};
        no -> {[], Chars}
    end.

take_leading_modifier_group([$(, $? | R]) ->
    case take_modifier_flags(R, []) of
        {ok, Flags, [$) | Rest]} -> {ok, Flags, Rest};
        _ -> no
    end;
take_leading_modifier_group(_) ->
    no.

take_modifier_flags([$i | R], Acc) -> take_modifier_flags(R, [caseless | Acc]);
take_modifier_flags([$s | R], Acc) -> take_modifier_flags(R, [dotall | Acc]);
take_modifier_flags([$m | R], Acc) -> take_modifier_flags(R, [multiline | Acc]);
take_modifier_flags([$x | R], Acc) -> take_modifier_flags(R, Acc);
take_modifier_flags(R, Acc) when Acc =/= [] orelse R =/= [] ->
    %% stops at the first non-flag char; caller checks it's ')'
    case R of
        [$) | _] -> {ok, Acc, R};
        _ -> error
    end;
take_modifier_flags(_, _) ->
    error.

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

%% atom -- (?...) forms are checked before the plain '(' fallback; order
%% matters (longer/more-specific prefixes first) since e.g. "(?<=" must
%% not be caught by the shorter "(?<" named-group clause.
p_atom([$(, $?, $:  | R]) -> p_group(R);          %% non-capturing: same as a plain group
p_atom([$(, $?, $=  | _R]) -> throw(unsupported); %% lookahead
p_atom([$(, $?, $!  | _R]) -> throw(unsupported); %% negative lookahead
p_atom([$(, $?, $<, $= | _R]) -> throw(unsupported); %% lookbehind
p_atom([$(, $?, $<, $! | _R]) -> throw(unsupported); %% negative lookbehind
p_atom([$(, $?, $<  | _R]) -> throw(unsupported); %% named group (?<name>...)
p_atom([$(, $?, $'  | _R]) -> throw(unsupported); %% named group (?'name'...)
p_atom([$(, $?, $P  | _R]) -> throw(unsupported); %% named group (?P<name>...)
p_atom([$(, $?, $(  | _R]) -> throw(unsupported); %% conditional
p_atom([$(, $?      | _R]) -> throw(unsupported); %% any other (?... -- mid-pattern/
                                                    %% scoped inline modifier, or
                                                    %% unrecognized -- fail closed
p_atom([$( | R]) -> p_group(R);
p_atom([$[ | R]) ->
    R2 = skip_class(R),
    {{class, ignored}, R2};
p_atom([$. | R]) -> {any, R};
p_atom([$^ | R]) -> {bol, R};
p_atom([$$ | R]) -> {eol, R};
p_atom([$\\, E | R]) -> {escape_atom(E), R};
p_atom([$\\]) -> throw(unsupported); %% trailing bare backslash: malformed
p_atom([C | R]) -> {{lit, <<C>>}, R}.
%% p_atom is only called from p_cat's guarded clause ([C|_] = Chars, C =/=
%% $|, C =/= $)), so Chars is never empty here -- an empty pattern/empty
%% remainder is handled one level up, by p_cat's own base clause.

p_group(R) ->
    {Node, R2} = p_alt(R),
    case R2 of
        [$) | Rr] -> {{group, Node}, Rr};
        _ -> throw(unsupported) %% unclosed group: malformed
    end.

%% quantifier
p_quant([$* | R]) -> {star, R};
p_quant([$+ | R]) -> {plus, R};
p_quant([$? | R]) -> {quest, R};
p_quant([${ | R] = All) ->
    case try_brace(R) of
        {ok, Min, Max, R2} -> {{rep, Min, Max}, R2};
        error -> {none, All}
    end;
p_quant(R) ->
    {none, R}.

%% {n}, {n,}, {n,m} -- both bounds are preserved (the original parser
%% discarded the max, which made a correct width bound impossible).
try_brace(Chars) ->
    case take_digits(Chars, []) of
        {[], _} -> error;
        {Ds, Rest} ->
            Min = list_to_integer(Ds),
            case Rest of
                [$} | R] -> {ok, Min, Min, R};
                [$, | R1] ->
                    case take_digits(R1, []) of
                        {[], [$} | R]} -> {ok, Min, infinity, R};
                        {[], _} -> error;
                        {MaxDs, [$} | R]} -> {ok, Min, list_to_integer(MaxDs), R};
                        {_, _} -> error
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
skip_class_body([]) -> throw(unsupported). %% unclosed class: malformed

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
escape_atom($x) -> throw(unsupported); %% \xHH / \x{...} -- not interpreted
escape_atom($Q) -> throw(unsupported); %% \Q...\E -- not interpreted
escape_atom($k) -> throw(unsupported); %% \k<name> backreference
escape_atom(C) when C >= $1, C =< $9 -> throw(unsupported); %% \1-\9 backreference
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
apply_quant(Node, {rep, Min, Max}) -> {rep, Node, Min, Max}.

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
info({rep, X, Min, _Max}) when Min >= 1 -> info(X);
info({rep, _X, 0, _Max}) -> {true, all};
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
%% Width bound: node -> {fixed, N} | {bounded, Min, Max} | unbounded
%%
%% `.' and a character class are conservatively bounded at 1-4 bytes (one
%% UTF-8 codepoint's worst case) -- too wide just costs a larger read,
%% too narrow could truncate a real match.
%%====================================================================

-spec width_bound(term()) -> width().
width_bound(Node) ->
    collapse(width_raw(Node)).

collapse(unbounded) -> unbounded;
collapse({Mn, Mn}) -> {fixed, Mn};
collapse({Mn, Mx}) -> {bounded, Mn, Mx}.

width_raw({lit, B}) ->
    N = byte_size(B),
    {N, N};
width_raw({cat, Xs}) ->
    lists:foldl(fun(X, Acc) -> add_w(width_raw(X), Acc) end, {0, 0}, Xs);
width_raw({alt, [First | Rest]}) ->
    lists:foldl(fun(X, Acc) -> union_w(width_raw(X), Acc) end, width_raw(First), Rest);
width_raw({star, _}) -> unbounded;
width_raw({plus, _}) -> unbounded;
width_raw({quest, X}) -> quest_w(width_raw(X));
width_raw({rep, X, Min, Max}) -> rep_w(width_raw(X), Min, Max);
width_raw(any) -> {1, 4};
width_raw({class, _}) -> {1, 4};
width_raw(bol) -> {0, 0};
width_raw(eol) -> {0, 0};
width_raw({group, X}) -> width_raw(X).

add_w(unbounded, _) -> unbounded;
add_w(_, unbounded) -> unbounded;
add_w({MnA, MxA}, {MnB, MxB}) -> {MnA + MnB, add_inf(MxA, MxB)}.

union_w(unbounded, _) -> unbounded;
union_w(_, unbounded) -> unbounded;
union_w({MnA, MxA}, {MnB, MxB}) -> {min(MnA, MnB), max_inf(MxA, MxB)}.

quest_w(unbounded) -> unbounded;
quest_w({_Mn, Mx}) -> {0, Mx}.

rep_w(_, _Min, infinity) -> unbounded;
rep_w(unbounded, _Min, _Max) -> unbounded;
rep_w({Mn, Mx}, Min, Max) -> {Mn * Min, mult_inf(Mx, Max)}.

add_inf(infinity, _) -> infinity;
add_inf(_, infinity) -> infinity;
add_inf(A, B) -> A + B.

max_inf(infinity, _) -> infinity;
max_inf(_, infinity) -> infinity;
max_inf(A, B) -> max(A, B).

mult_inf(_, infinity) -> infinity;
mult_inf(infinity, _) -> infinity;
mult_inf(A, B) -> A * B.

%%====================================================================
%% Literal runs eligible as a windowing anchor
%%====================================================================

%% @doc Literal runs a positional planner may anchor a window on: only a
%% pure AND-chain (a `{cat, Nodes}' with no `{alt, _}' child, or a bare
%% `{lit, _}'), each paired with its own `PrefixMax'/`SuffixMax' (the
%% upper-bound width of everything before/after it in the chain,
%% `unbounded' if that side has an unbounded quantifier). `ineligible'
%% for anything else, including a `{lit, _}' merely sitting next to an
%% `{alt, _}' -- a real match could come from a different branch, so the
%% whole chain is rejected rather than reasoning about which literals
%% would still be safe.
-spec literal_runs(term()) -> [literal_run()] | ineligible.
literal_runs({lit, Bytes}) ->
    [#{bytes => Bytes, prefix_max => 0, suffix_max => 0}];
literal_runs({cat, Nodes}) ->
    case lists:any(fun({alt, _}) -> true; (_) -> false end, Nodes) of
        true -> ineligible;
        false -> chain_literal_runs(Nodes)
    end;
literal_runs(_) ->
    ineligible.

chain_literal_runs(Nodes) ->
    N = length(Nodes),
    Widths = [width_bound(Node) || Node <- Nodes],
    Indexed = lists:zip(lists:seq(1, N), Nodes),
    lists:filtermap(
        fun({I, {lit, Bytes}}) ->
            {true, #{bytes => Bytes,
                    prefix_max => sum_upper(lists:sublist(Widths, I - 1)),
                    suffix_max => sum_upper(lists:nthtail(I, Widths))}};
           ({_I, _Node}) ->
            false
        end, Indexed).

%% @private Sum of upper bounds; `unbounded' if any width is unbounded or
%% an unbounded-max `{bounded, _, infinity}'.
sum_upper(Widths) ->
    lists:foldl(fun add_upper/2, 0, Widths).

add_upper(_, unbounded) -> unbounded;
add_upper({fixed, M}, Acc) -> Acc + M;
add_upper({bounded, _Min, infinity}, _Acc) -> unbounded;
add_upper({bounded, _Min, Max}, Acc) -> Acc + Max;
add_upper(unbounded, _Acc) -> unbounded.

%%====================================================================
%% Anchor/boundary detection ( ^ $ \b \B \A \z \Z -- all parsed as bol/eol )
%%====================================================================

has_anchor_or_boundary(bol) -> true;
has_anchor_or_boundary(eol) -> true;
has_anchor_or_boundary({cat, Xs}) -> lists:any(fun has_anchor_or_boundary/1, Xs);
has_anchor_or_boundary({alt, Xs}) -> lists:any(fun has_anchor_or_boundary/1, Xs);
has_anchor_or_boundary({star, X}) -> has_anchor_or_boundary(X);
has_anchor_or_boundary({plus, X}) -> has_anchor_or_boundary(X);
has_anchor_or_boundary({quest, X}) -> has_anchor_or_boundary(X);
has_anchor_or_boundary({rep, X, _, _}) -> has_anchor_or_boundary(X);
has_anchor_or_boundary({group, X}) -> has_anchor_or_boundary(X);
has_anchor_or_boundary(_) -> false.

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
