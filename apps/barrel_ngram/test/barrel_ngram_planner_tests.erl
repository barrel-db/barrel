%%%-------------------------------------------------------------------
%%% @doc EUnit tests for the literal query planner.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_planner_tests).

-include_lib("eunit/include/eunit.hrl").

-define(M, barrel_ngram_planner).
-define(SEG, barrel_ngram_segment).

gram(A, B, C) -> (A bsl 16) bor (B bsl 8) bor C.
wm() -> <<9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9>>.
hlc(N) -> <<N:96>>.
entry(K, N) -> #{key => K, hlc => hlc(N), deleted => false}.

segment_plan_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun brute_force_is_dense/1,
      fun no_data_grams_are_dense/1,
      fun single_gram_available/1,
      fun cheapest_pair_chosen/1
     ]}.

%% Fixture segment (written here, opened by each test itself -- a
%% barrel_ngram_segment handle owns a raw fd bound to its opening
%% process, and EUnit runs setup/0 and each test fun in different
%% processes, so the handle can't be shared via setup/0). Ga has phase-2
%% data for 1 doc (cheapest), Gb for 3 docs, Gd for 5 docs (most
%% expensive); Gc and any other gram have none at all. Ga/Gb share
%% ordinal 0 with offsets 4 bytes apart, so a literal whose two reliable
%% grams are Ga (offset 0) and Gb (offset 4) has a real distance-check
%% candidate at document offset 10.
setup() ->
    Dir = filename:join(["/tmp",
                         "barrel_ngram_planner_" ++ integer_to_list(erlang:unique_integer([positive]))]),
    ok = filelib:ensure_dir(filename:join(Dir, "dummy")),
    Path = filename:join(Dir, "seg.ngseg"),
    Ga = gram($a, $a, $a),
    Gb = gram($b, $b, $b),
    Gd = gram($d, $d, $d),
    Spec = #{
        doc_count => 5,
        watermark => wm(),
        postings => [{Ga, [0]}, {Gb, [0, 1, 2]}, {Gd, [0, 1, 2, 3, 4]}],
        positional_postings => [
            {Ga, [{0, [10]}]},
            {Gb, [{0, [14]}, {1, [999]}, {2, [999]}]},
            {Gd, [{0, [1]}, {1, [1]}, {2, [1]}, {3, [1]}, {4, [1]}]}
        ],
        entries => [entry(<<"k0">>, 1), entry(<<"k1">>, 2), entry(<<"k2">>, 3),
                    entry(<<"k3">>, 4), entry(<<"k4">>, 5)]
    },
    ok = ?SEG:write(Path, Spec),
    {Dir, Path}.

cleanup({Dir, _Path}) ->
    os:cmd("rm -rf " ++ Dir),
    ok.

with_segment(Path, Fun) ->
    {ok, H} = ?SEG:open(Path),
    try Fun(H) after ?SEG:close(H) end.

brute_force_is_dense({_Dir, Path}) ->
    fun() ->
        with_segment(Path, fun(H) ->
            ?assertEqual(dense, ?M:segment_plan(H, brute_force))
        end)
    end.

%% Every gram the literal considers reliable has no phase-2 data at all in
%% THIS segment -- degrade to dense (today's phase-1 narrowing).
no_data_grams_are_dense({_Dir, Path}) ->
    fun() ->
        with_segment(Path, fun(H) ->
            Gc = gram($c, $c, $c),
            Gz = gram($z, $z, $z),
            ?assertEqual(dense, ?M:segment_plan(H, {reliable, [{Gc, 0}, {Gz, 4}]}))
        end)
    end.

%% Only one reliable gram has phase-2 data here -- single_gram_candidates,
%% not distance_check (nothing to cross-check against).
single_gram_available({_Dir, Path}) ->
    fun() ->
        with_segment(Path, fun(H) ->
            Ga = gram($a, $a, $a),
            Gc = gram($c, $c, $c),
            Result = ?M:segment_plan(H, {reliable, [{Ga, 0}, {Gc, 4}]}),
            ?assertEqual({positional, [{0, [10]}]}, Result)
        end)
    end.

%% Three reliable grams have data here (Ga: 1 doc, Gb: 3 docs, Gd: 5 docs)
%% -- the two CHEAPEST (Ga, Gb) are chosen, not Gd, regardless of the
%% order they're passed in.
cheapest_pair_chosen({_Dir, Path}) ->
    fun() ->
        with_segment(Path, fun(H) ->
            Ga = gram($a, $a, $a),
            Gb = gram($b, $b, $b),
            Gd = gram($d, $d, $d),
            Result = ?M:segment_plan(H, {reliable, [{Gd, 12}, {Gb, 4}, {Ga, 0}]}),
            ?assertEqual({positional, [{0, [10]}]}, Result)
        end)
    end.

%%====================================================================
%% escape_literal/1
%%====================================================================

escape_literal_basic_test() ->
    ?assertEqual(<<"a\\.b">>, ?M:escape_literal(<<"a.b">>)),
    ?assertEqual(<<"\\(x\\)">>, ?M:escape_literal(<<"(x)">>)),
    ?assertEqual(<<"a\\[1\\]\\{2\\}">>, ?M:escape_literal(<<"a[1]{2}">>)),
    ?assertEqual(<<"plain">>, ?M:escape_literal(<<"plain">>)).

%% Proves the escaping actually prevents regex interpretation (not just
%% that the escaped pattern compiles): a literal metacharacter must match
%% only itself, never stand in for an arbitrary character/operator.
escape_literal_compiles_and_matches_only_literal_bytes_test() ->
    {ok, RE} = re:compile(?M:escape_literal(<<"a.b">>), [caseless]),
    ?assertMatch({match, _}, re:run(<<"A.B">>, RE)),
    ?assertEqual(nomatch, re:run(<<"AxB">>, RE)).

%% Multi-byte UTF-8 sequences must round-trip untouched: none of their
%% bytes are ASCII metacharacters (all are >= 16#80).
escape_literal_utf8_safe_test() ->
    Bin = <<"café"/utf8>>,
    ?assertEqual(Bin, ?M:escape_literal(Bin)).

%%====================================================================
%% utf8_valid/1
%%====================================================================

utf8_valid_test() ->
    ?assert(?M:utf8_valid(<<"hello">>)),
    ?assert(?M:utf8_valid(<<"café"/utf8>>)),
    ?assertNot(?M:utf8_valid(<<16#80>>)),          %% lone continuation byte
    ?assertNot(?M:utf8_valid(<<16#FF, 16#FE>>)).   %% never valid in UTF-8

%%====================================================================
%% case_mode/1
%%====================================================================

case_mode_ascii_long_literal_narrows_test() ->
    {Query, Opts, Validate} = ?M:case_mode(<<"connect_timeout">>),
    ?assertEqual([caseless], Opts),
    ?assertEqual(false, Validate),
    ?assertNotEqual(all, Query).

case_mode_ascii_short_literal_is_all_test() ->
    ?assertEqual({all, [caseless], false}, ?M:case_mode(<<"ab">>)).

case_mode_non_ascii_valid_utf8_test() ->
    Bin = <<"café"/utf8>>,
    ?assertEqual({all, [caseless, unicode], true}, ?M:case_mode(Bin)).

case_mode_non_ascii_invalid_utf8_test() ->
    Bin = <<"caf", 255, 255>>,
    ?assertEqual({error, {invalid_literal_encoding, Bin}}, ?M:case_mode(Bin)).

%%====================================================================
%% ascii_caseless_query/1
%%====================================================================

ascii_caseless_query_below_trigram_is_all_test() ->
    ?assertEqual(all, ?M:ascii_caseless_query(<<>>)),
    ?assertEqual(all, ?M:ascii_caseless_query(<<"ab">>)).

%% THE core invariant: every ASCII case variant of the literal must
%% satisfy the query built from it -- if it failed, a real
%% case-insensitive match would be silently narrowed away before
%% verification ever sees it.
ascii_caseless_query_soundness_test() ->
    lists:foreach(
        fun(Literal) ->
            Query = ?M:ascii_caseless_query(Literal),
            lists:foreach(
                fun(Variant) ->
                    Grams = ordsets:from_list(
                              barrel_ngram_selector_dense:select_grams(Variant, #{})),
                    ?assert(satisfies(Query, Grams))
                end, case_variants(Literal))
        end, [<<"AbC">>, <<"connect_TIMEOUT">>, <<"Retry_Backoff_MS">>, <<"a1B2c3">>]).

satisfies(all, _) -> true;
satisfies(none, _) -> false;
satisfies({gram, G}, S) -> ordsets:is_element(G, S);
satisfies({'and', Qs}, S) -> lists:all(fun(Q) -> satisfies(Q, S) end, Qs);
satisfies({'or', Qs}, S) -> lists:any(fun(Q) -> satisfies(Q, S) end, Qs).

%% Every combination of upper/lower for each ASCII letter byte in Bin,
%% non-letters held fixed -- an independent reference generator (not
%% reusing ?M's own byte_case_variants/1) for the variants a real document
%% could contain.
case_variants(Bin) ->
    VariantLists = [byte_variants(B) || B <- binary_to_list(Bin)],
    [list_to_binary(Combo) || Combo <- combinations(VariantLists)].

byte_variants(B) when B >= $a, B =< $z -> [B, B - 32];
byte_variants(B) when B >= $A, B =< $Z -> [B, B + 32];
byte_variants(B) -> [B].

combinations([]) -> [[]];
combinations([Options | Rest]) ->
    [[O | C] || O <- Options, C <- combinations(Rest)].

%%====================================================================
%% regex_plan/2
%%====================================================================

%% Aggressive sampling (small radius/high rate) so short-ish literal runs
%% reliably get an interior sampled position in these tests.
regex_pos_opts() -> #{radius => 2, sample_rate => 2}.

regex_plan_unsupported_is_full_scan_test() ->
    Analyzed = barrel_ngram_regex:analyze(<<"(?=foo)">>),
    ?assertEqual(unsupported, Analyzed),
    ?assertEqual(full_scan, ?M:regex_plan(Analyzed, regex_pos_opts())).

regex_plan_anchor_or_boundary_is_full_scan_test() ->
    Analyzed = barrel_ngram_regex:analyze(<<"^connect_timeout_error">>),
    ?assertEqual(full_scan, ?M:regex_plan(Analyzed, regex_pos_opts())).

regex_plan_top_level_alternation_is_full_scan_test() ->
    Analyzed = barrel_ngram_regex:analyze(<<"connect_timeout|retry_backoff">>),
    ?assertEqual(full_scan, ?M:regex_plan(Analyzed, regex_pos_opts())).

%% Below even a trigram -- literal_runs/1 would return a run, but no
%% interior sampled position can ever exist for it.
regex_plan_short_literal_is_full_scan_test() ->
    Analyzed = barrel_ngram_regex:analyze(<<"ab">>),
    ?assertEqual(full_scan, ?M:regex_plan(Analyzed, regex_pos_opts())).

%% An eligible AND-chain regex with a long-enough literal run picks a
%% real windowed anchor -- one of the chain's own literal runs, with a
%% Prefix/SuffixMax and reliable grams to show for it. The gap between
%% the two literal runs (a bounded class-repeat) is FINITE, unlike
%% e.g. `\w+', which would leave both neighbors unbounded and ineligible
%% on that side -- see regex_plan_unbounded_gap_is_full_scan_test.
regex_plan_windowed_for_eligible_pattern_test() ->
    Analyzed = barrel_ngram_regex:analyze(<<"connect_[0-9]{2}_backoff_ms">>),
    Plan = ?M:regex_plan(Analyzed, regex_pos_opts()),
    ?assertMatch({windowed, _Bytes, _PrefixMax, _SuffixMax, [_ | _]}, Plan),
    {windowed, Bytes, PrefixMax, SuffixMax, GramOffs} = Plan,
    ?assert(lists:member(Bytes, [<<"connect_">>, <<"_backoff_ms">>])),
    ?assert(is_integer(PrefixMax)),
    ?assert(is_integer(SuffixMax)),
    ?assert(length(GramOffs) > 0).

%% An unbounded quantifier sitting between two literal runs leaves BOTH
%% of them with an unbounded neighbor on the side facing it -- neither is
%% eligible, so this falls back even though each literal individually
%% would have reliable grams.
regex_plan_unbounded_gap_is_full_scan_test() ->
    Analyzed = barrel_ngram_regex:analyze(<<"connect_timeout_\\w+_retry_backoff">>),
    ?assertEqual(full_scan, ?M:regex_plan(Analyzed, regex_pos_opts())).

%% A window ceiling large enough to never trip in practice, but still a
%% real, finite bound: an absurdly wide prefix (many repetitions of a
%% wide-bounded class) pushes PrefixMax over it, so this falls back
%% rather than requesting an enormous read.
regex_plan_over_ceiling_is_full_scan_test() ->
    %% each "." conservatively costs up to 4 bytes; 2000 of them push
    %% PrefixMax to 8000, comfortably over the 4096 ceiling
    Pattern = iolist_to_binary([binary:copy(<<".">>, 2000), <<"connect_timeout">>]),
    Analyzed = barrel_ngram_regex:analyze(Pattern),
    ?assertEqual(full_scan, ?M:regex_plan(Analyzed, regex_pos_opts())).

%% `choose_regex_anchor/2' must try the NEXT literal run when the first
%% (longest, tried first) has no reliable phase-2 grams of its own --
%% falling back to `full_scan' outright would be sound but needlessly slow
%% when a later run could still anchor a window. `<<"8j1r5vSy0wfT">>' and
%% `<<"f3ZLTPxcSxUK">>' are two 12-byte strings, precomputed offline for
%% radius=2/sample_rate=11: the first samples no interior position at all
%% (brute_force) under those exact params, the second samples at least
%% one. Equal length keeps the (stable) longest-first sort in original
%% left-to-right order, so the brute_force run is genuinely tried first.
regex_plan_falls_back_to_next_literal_run_test() ->
    Pattern = <<"8j1r5vSy0wfT.f3ZLTPxcSxUK">>,
    Opts = #{radius => 2, sample_rate => 11},
    %% sanity: confirms the precomputed premise the rest of the test
    %% relies on, so a drift in barrel_ngram_selector_sparse's hashing
    %% fails here with a clear message instead of a confusing plan mismatch
    ?assertEqual(brute_force,
                 barrel_ngram_selector_sparse:reliable_grams_positional(<<"8j1r5vSy0wfT">>, Opts)),
    ?assertMatch({reliable, [_ | _]},
                 barrel_ngram_selector_sparse:reliable_grams_positional(<<"f3ZLTPxcSxUK">>, Opts)),
    Analyzed = barrel_ngram_regex:analyze(Pattern),
    Plan = ?M:regex_plan(Analyzed, Opts),
    ?assertMatch({windowed, <<"f3ZLTPxcSxUK">>, _PrefixMax, _SuffixMax, [_ | _]}, Plan).

%%====================================================================
%% regex_case_mode/2
%%====================================================================

regex_case_mode_ascii_no_leading_i_test() ->
    ?assertEqual({[caseless], false}, ?M:regex_case_mode(<<"connect_[0-9]+">>, false)).

%% The pattern's own leading `(?i)' already makes it caseless -- no extra
%% `re' option is needed on top (re:compile understands the inline flag
%% itself), and there's still no corpus-document encoding concern for a
%% pure-ASCII pattern.
regex_case_mode_ascii_leading_i_test() ->
    ?assertEqual({[], false}, ?M:regex_case_mode(<<"(?i)connect_[0-9]+">>, true)).

regex_case_mode_non_ascii_no_leading_i_test() ->
    Bin = <<"café[0-9]+"/utf8>>,
    ?assertEqual({[caseless, unicode], true}, ?M:regex_case_mode(Bin, false)).

%% Non-ASCII with its own leading `(?i)': caseless is already implied by
%% the pattern text, but `unicode' is still required explicitly since
%% Unicode case folding isn't implied by the pattern text alone.
regex_case_mode_non_ascii_leading_i_test() ->
    Bin = <<"(?i)café[0-9]+"/utf8>>,
    ?assertEqual({[unicode], true}, ?M:regex_case_mode(Bin, true)).

regex_case_mode_non_ascii_invalid_utf8_test() ->
    Bin = <<"caf", 255, 255, "[0-9]+">>,
    ?assertEqual({error, {invalid_literal_encoding, Bin}}, ?M:regex_case_mode(Bin, false)).
