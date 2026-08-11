%%%-------------------------------------------------------------------
%%% @doc Query planner: decides, per segment, how much a literal's or
%%% regex's phase-2 (positional) postings can narrow candidates.
%%%
%%% `literal_plan/2' picks a literal's reliable (interior) grams once;
%%% `segment_plan/2' ranks them by this segment's own doc-count and picks
%%% the cheapest one or two to distance-check -- which ones only affects
%%% speed, never correctness. A plan survivor is a candidate match start,
%%% not a confirmed one; verification (see {@link barrel_ngram_verify})
%%% always follows.
%%%
%%% `case_mode/1'/`regex_case_mode/2' skip phase-2 for a case-insensitive
%%% query (its sampling is itself case-sensitive): an ASCII-only
%%% literal/pattern narrows via case-variant expansion and verifies
%%% `[caseless]'; any non-ASCII byte narrows not at all (`all') and
%%% verifies `[caseless, unicode]'. Verification always compiles
%%% {@link escape_literal/1}'s output, never the raw literal --
%%% `re:compile' rejects `[literal, caseless]' together.
%%%
%%% `regex_plan/2' is the regex analog: `full_scan' for anything not a
%%% clean, anchor-free literal-run chain with a bounded, reliably-sampled
%%% run (see {@link barrel_ngram_regex:literal_runs/1}); otherwise
%%% `{windowed, AnchorBytes, PrefixMax, SuffixMax, GramOffs}' on the
%%% longest such run.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_planner).

-export([literal_plan/2, segment_plan/2]).
-export([case_mode/1, escape_literal/1, ascii_caseless_query/1, utf8_valid/1]).
-export([regex_plan/2, regex_case_mode/2]).

-type gram() :: barrel_ngram_selector:gram().
-type offset() :: barrel_ngram_postings_positional:offset().
-type ordinal() :: barrel_ngram_postings:ordinal().

-type segment_plan() :: dense | {positional, [{ordinal(), [offset()]}]}.
-export_type([segment_plan/0]).

%% The anchor literal, its Prefix/SuffixMax (see
%% barrel_ngram_regex:literal_runs/1), and its reliable phase-2 grams.
-type regex_plan() ::
    full_scan | {windowed, binary(), non_neg_integer(), non_neg_integer(),
                [{gram(), offset()}]}.
-export_type([regex_plan/0]).

-type re_opts() :: [caseless | unicode].
-type case_mode() ::
    {barrel_ngram_regex:query(), re_opts(), ValidateDocs :: boolean()} |
    {error, {invalid_literal_encoding, binary()}}.
-export_type([case_mode/0]).

%% @doc A literal's reliable (interior) phase-2 grams, each with its own
%% offset -- computed once, reused across every segment. `brute_force'
%% below `3 + 2*radius' bytes (no interior position possible).
-spec literal_plan(binary(), map()) ->
    brute_force | {reliable, [{gram(), offset()}]}.
literal_plan(Literal, PositionalOpts) ->
    barrel_ngram_selector_sparse:reliable_grams_positional(Literal, PositionalOpts).

%% @doc This segment's best narrowing for a literal's reliable grams:
%% `dense' if none have phase-2 data here, a single-gram candidate list
%% for one, distance-checked pair for two or more.
-spec segment_plan(barrel_ngram_segment:handle(),
                   brute_force | {reliable, [{gram(), offset()}]}) ->
    segment_plan().
segment_plan(_Handle, brute_force) ->
    dense;
segment_plan(Handle, {reliable, GramOffs}) ->
    case rank_by_doc_count(Handle, GramOffs) of
        [] ->
            dense;
        [{G, D}] ->
            {positional, single_gram(Handle, G, D)};
        [{G1, D1}, {G2, D2} | _] ->
            {positional, pair(Handle, G1, D1, G2, D2)}
    end.

%% @private Reliable grams with phase-2 data in this segment, ascending by
%% doc-count (rarest first); grams with none here are dropped.
rank_by_doc_count(Handle, GramOffs) ->
    Ranked = lists:filtermap(
        fun({G, D}) ->
            case barrel_ngram_segment:positional_doc_count(Handle, G) of
                {ok, Count} -> {true, {Count, G, D}};
                not_found -> false
            end
        end, GramOffs),
    [{G, D} || {_Count, G, D} <- lists:sort(Ranked)].

single_gram(Handle, G, D) ->
    case barrel_ngram_segment:lookup_positional_block(Handle, G) of
        {ok, Block} -> barrel_ngram_postings_positional:single_gram_candidates(Block, D);
        not_found -> []
    end.

pair(Handle, G1, D1, G2, D2) ->
    case {barrel_ngram_segment:lookup_positional_block(Handle, G1),
          barrel_ngram_segment:lookup_positional_block(Handle, G2)} of
        {{ok, B1}, {ok, B2}} ->
            barrel_ngram_postings_positional:distance_check(B1, D1, B2, D2);
        _ ->
            []
    end.

%%====================================================================
%% Case-insensitive literals
%%====================================================================

%% @doc The phase-1 narrowing query and `re' verification options for a
%% case-insensitive literal (see the moduledoc). `{error,
%% {invalid_literal_encoding, Literal}}' for a non-ASCII, non-UTF-8 literal.
-spec case_mode(binary()) -> case_mode().
case_mode(Literal) ->
    case is_ascii(Literal) of
        true ->
            {ascii_caseless_query(Literal), [caseless], false};
        false ->
            case utf8_valid(Literal) of
                true -> {all, [caseless, unicode], true};
                false -> {error, {invalid_literal_encoding, Literal}}
            end
    end.

is_ascii(<<>>) -> true;
is_ascii(<<B, Rest/binary>>) when B < 128 -> is_ascii(Rest);
is_ascii(_) -> false.

%% @doc Whether `Bin' is valid UTF-8, via a round-trip through
%% `unicode:characters_to_binary/1' (valid input comes back unchanged).
-spec utf8_valid(binary()) -> boolean().
utf8_valid(Bin) ->
    case unicode:characters_to_binary(Bin) of
        Bin -> true;
        _ -> false
    end.

%% @doc `Literal' with every PCRE metacharacter escaped, so compiling it
%% as an ordinary pattern matches the literal bytes and nothing else.
%% Byte-level, safe for UTF-8: a continuation/lead byte is always `>= 16#80'.
-spec escape_literal(binary()) -> binary().
escape_literal(Bin) ->
    iolist_to_binary([escape_byte(B) || <<B>> <= Bin]).

escape_byte(B) when B =:= $\\; B =:= $.; B =:= $^; B =:= $$; B =:= $|;
                    B =:= $?; B =:= $*; B =:= $+; B =:= $(; B =:= $);
                    B =:= $[; B =:= $]; B =:= ${; B =:= $} ->
    <<$\\, B>>;
escape_byte(B) ->
    <<B>>.

%% @doc The ASCII case-insensitive narrowing query: OR each trigram
%% position's case variants, AND across positions. `all' below a trigram.
-spec ascii_caseless_query(binary()) -> barrel_ngram_regex:query().
ascii_caseless_query(Bin) when byte_size(Bin) >= 3 ->
    N = byte_size(Bin),
    {'and', [{'or', [{gram, G} || G <- trigram_case_variants(Bin, I)]}
             || I <- lists:seq(0, N - 3)]};
ascii_caseless_query(_) ->
    all.

trigram_case_variants(Bin, I) ->
    <<A, B, C>> = binary:part(Bin, I, 3),
    lists:usort(
      [(A1 bsl 16) bor (B1 bsl 8) bor C1
       || A1 <- byte_case_variants(A), B1 <- byte_case_variants(B),
          C1 <- byte_case_variants(C)]).

byte_case_variants(B) when B >= $a, B =< $z -> [B, B - 32];
byte_case_variants(B) when B >= $A, B =< $Z -> [B, B + 32];
byte_case_variants(B) -> [B].

%%====================================================================
%% Bounded regex
%%====================================================================

%% Not a correctness bound -- over this just falls back to full_scan.
-define(WINDOW_CEILING, 4096).

%% @doc Whether a successfully-{@link barrel_ngram_regex:analyze/1}'d
%% regex can anchor a window, and on which literal run -- see the
%% moduledoc.
-spec regex_plan(term(), map()) -> regex_plan().
regex_plan(unsupported, _PositionalOpts) ->
    full_scan;
regex_plan({ok, _Node, _Query, #{has_anchor_or_boundary := true}}, _PositionalOpts) ->
    full_scan;
regex_plan({ok, Node, _Query, _WidthInfo}, PositionalOpts) ->
    case barrel_ngram_regex:literal_runs(Node) of
        ineligible -> full_scan;
        Runs -> choose_regex_anchor(sort_longest_first(Runs), PositionalOpts)
    end.

sort_longest_first(Runs) ->
    lists:sort(fun(#{bytes := A}, #{bytes := B}) -> byte_size(A) >= byte_size(B) end, Runs).

choose_regex_anchor([], _PositionalOpts) ->
    full_scan;
choose_regex_anchor([#{bytes := Bytes, prefix_max := PrefixMax, suffix_max := SuffixMax} | Rest],
                    PositionalOpts) ->
    case within_window_ceiling(PrefixMax, SuffixMax, byte_size(Bytes)) of
        false ->
            choose_regex_anchor(Rest, PositionalOpts);
        true ->
            case barrel_ngram_selector_sparse:reliable_grams_positional(Bytes, PositionalOpts) of
                brute_force -> choose_regex_anchor(Rest, PositionalOpts);
                {reliable, GramOffs} -> {windowed, Bytes, PrefixMax, SuffixMax, GramOffs}
            end
    end.

within_window_ceiling(PrefixMax, SuffixMax, LitLen) ->
    is_integer(PrefixMax) andalso is_integer(SuffixMax) andalso
    PrefixMax + SuffixMax + LitLen =< ?WINDOW_CEILING.

%%====================================================================
%% Case-insensitive regex
%%====================================================================

%% @doc The `re' compile options for a case-insensitive regex -- same
%% ASCII/non-ASCII split as {@link case_mode/1}, but never a narrowing
%% query (always phase-1 `all'). `HasLeadingCaseless': the pattern's own
%% leading `(?i)' already implies `caseless', but `unicode' still needs
%% adding explicitly for a non-ASCII pattern.
-spec regex_case_mode(binary(), boolean()) ->
    {re_opts(), ValidateDocs :: boolean()} | {error, {invalid_literal_encoding, binary()}}.
regex_case_mode(Regex, HasLeadingCaseless) ->
    case is_ascii(Regex) of
        true ->
            {case HasLeadingCaseless of true -> []; false -> [caseless] end, false};
        false ->
            case utf8_valid(Regex) of
                true ->
                    {case HasLeadingCaseless of
                         true -> [unicode];
                         false -> [caseless, unicode]
                     end, true};
                false ->
                    {error, {invalid_literal_encoding, Regex}}
            end
    end.
