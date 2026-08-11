%%%-------------------------------------------------------------------
%%% @doc EUnit tests for windowed literal verification.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_verify_tests).

-include_lib("eunit/include/eunit.hrl").

-define(M, barrel_ngram_verify).

docs() ->
    #{<<"a">> => <<"error: connect_timeout in the pool">>,
      <<"b">> => <<"nothing here matches at all">>}.

source() -> {barrel_ngram_source_mem, docs()}.

exact_match_kept_test() ->
    %% "connect_timeout" starts at byte 7 in doc "a"
    ?assertEqual([7], ?M:windowed(source(), <<"a">>, <<"connect_timeout">>, [7])).

near_miss_content_dropped_test() ->
    %% a candidate start whose window's real bytes don't equal the
    %% literal (a distance-check false positive) is dropped, not kept
    ?assertEqual([], ?M:windowed(source(), <<"a">>, <<"connect_timeout">>, [0])).

%% Several candidate starts for the same key are verified independently:
%% only the ones whose window actually matches survive.
multiple_starts_independently_verified_test() ->
    ?assertEqual([7], ?M:windowed(source(), <<"a">>, <<"connect_timeout">>, [0, 7, 15])).

no_candidates_returns_empty_test() ->
    ?assertEqual([], ?M:windowed(source(), <<"a">>, <<"connect_timeout">>, [])).

%% A key the source has no content for (deleted/missing) drops every
%% candidate silently rather than crashing or treating it as a match.
missing_key_drops_all_test() ->
    ?assertEqual([], ?M:windowed(source(), <<"missing">>, <<"connect_timeout">>, [0, 5])).

%% A candidate start too close to the document's end for the literal to
%% fit (a short/EOF read) is dropped, not treated as a partial match.
start_past_available_length_dropped_test() ->
    ?assertEqual([], ?M:windowed(source(), <<"a">>, <<"connect_timeout">>, [9999])).

%% No literal in doc "b" at all -- every candidate is a false positive.
unrelated_content_all_dropped_test() ->
    ?assertEqual([], ?M:windowed(source(), <<"b">>, <<"connect_timeout">>, [0, 5, 10])).

%% "aaa" genuinely occurs at BOTH offset 0 and offset 1 of "aaaa" -- both
%% are real, independently byte-verified matches, but a single
%% left-to-right scan (`binary:matches(<<"aaaa">>, <<"aaa">>)' ==
%% `[{0,3}]') only ever reports the first, since it resumes scanning from
%% the end of each match found. Without the non-overlapping reduction this
%% would return `[0, 1]', diverging from what the dense/buffer lanes'
%% `binary:matches'-based confirm reports for the identical document.
overlapping_matches_reduced_to_leftmost_test() ->
    Source = {barrel_ngram_source_mem, #{<<"aaaa">> => <<"aaaa">>}},
    ?assertEqual([0], ?M:windowed(Source, <<"aaaa">>, <<"aaa">>, [0, 1])),
    ?assertEqual(binary:matches(<<"aaaa">>, <<"aaa">>),
                 [{S, 3} || S <- ?M:windowed(Source, <<"aaaa">>, <<"aaa">>, [0, 1])]).

%%====================================================================
%% non_overlapping/1
%%====================================================================

non_overlapping_empty_test() ->
    ?assertEqual([], ?M:non_overlapping([])).

non_overlapping_no_overlap_kept_test() ->
    ?assertEqual([{0, 3}, {5, 3}], ?M:non_overlapping([{5, 3}, {0, 3}])).

%% {0,3} and {1,3} overlap (1 < 0+3); the earlier-starting one wins and the
%% cursor jumps to its end, so {1,3} is dropped, matching the greedy
%% left-to-right scan a single-pass `binary:matches'/`re:run' does.
non_overlapping_drops_overlap_test() ->
    ?assertEqual([{0, 3}], ?M:non_overlapping([{1, 3}, {0, 3}])).

%% Matches of DIFFERENT lengths (as regex matches can have): {0,5} wins
%% and its end (5) excludes {2,4} (starts before 5) but not {5,2} (starts
%% exactly at the cursor).
non_overlapping_variable_length_test() ->
    ?assertEqual([{0, 5}, {5, 2}], ?M:non_overlapping([{2, 4}, {5, 2}, {0, 5}])).

%%====================================================================
%% windowed_regex/7
%%====================================================================

%% "0123456789" (0-9) + "connect_timeout" (10-24) + "ABCDEFGHIJ" (25-34),
%% length 35 -- exact, controlled byte positions for window-math tests.
regex_docs() ->
    #{<<"a">> => <<"0123456789connect_timeoutABCDEFGHIJ">>}.

regex_source() -> {barrel_ngram_source_mem, regex_docs()}.

re(Pattern) ->
    {ok, RE} = re:compile(Pattern),
    RE.

windowed_regex_basic_match_test() ->
    RE = re(<<"connect_timeout">>),
    ?assertEqual([{10, 15}],
                 ?M:windowed_regex(regex_source(), <<"a">>, RE, 5, 15, 5, [10])).

%% PrefixMax far larger than the actual available prefix clamps to the
%% document start rather than erroring or reading a negative offset.
windowed_regex_clamps_at_document_start_test() ->
    RE = re(<<"connect_timeout">>),
    ?assertEqual([{10, 15}],
                 ?M:windowed_regex(regex_source(), <<"a">>, RE, 50, 15, 5, [10])).

%% SuffixMax far larger than the actual available suffix clamps to the
%% document end.
windowed_regex_clamps_at_document_end_test() ->
    RE = re(<<"connect_timeout">>),
    ?assertEqual([{10, 15}],
                 ?M:windowed_regex(regex_source(), <<"a">>, RE, 5, 15, 50, [10])).

%% THE core proof that PrefixMax genuinely widens the window backward,
%% not just documents an intent: the real full match is "89connect_timeout"
%% (two digits immediately before the anchor literal). With PrefixMax
%% wide enough to reach them, the match is found; with it one byte too
%% narrow, the window never sees both digits and the match is missed.
windowed_regex_prefix_max_widens_window_test() ->
    RE = re(<<"[0-9]{2}connect_timeout">>),
    ?assertEqual([{8, 17}],
                 ?M:windowed_regex(regex_source(), <<"a">>, RE, 2, 15, 0, [10])),
    ?assertEqual([],
                 ?M:windowed_regex(regex_source(), <<"a">>, RE, 1, 15, 0, [10])).

%% The same real match rediscovered via more than one anchor start (a
%% repeated gram, or overlapping windows) is reported once, not twice.
windowed_regex_dedups_across_anchor_starts_test() ->
    RE = re(<<"connect_timeout">>),
    ?assertEqual([{10, 15}],
                 ?M:windowed_regex(regex_source(), <<"a">>, RE, 5, 15, 5, [10, 10])).

windowed_regex_no_candidates_returns_empty_test() ->
    RE = re(<<"connect_timeout">>),
    ?assertEqual([], ?M:windowed_regex(regex_source(), <<"a">>, RE, 5, 15, 5, [])).

windowed_regex_missing_key_returns_empty_test() ->
    RE = re(<<"connect_timeout">>),
    ?assertEqual([], ?M:windowed_regex(regex_source(), <<"missing">>, RE, 5, 15, 5, [10])).

windowed_regex_no_match_in_window_test() ->
    RE = re(<<"nonexistent_pattern_xyz">>),
    ?assertEqual([], ?M:windowed_regex(regex_source(), <<"a">>, RE, 5, 15, 5, [10])).

%% The regex analog of overlapping_matches_reduced_to_leftmost_test: "aa"
%% genuinely matches at both offset 0 and offset 1 of "aaaa", but a single
%% left-to-right `re:run(..., [global])' scan only ever reports the first
%% (it resumes from each match's end) -- proving windowed_regex/7 reduces
%% multi-anchor-discovered overlapping matches the same way.
windowed_regex_overlapping_matches_reduced_to_leftmost_test() ->
    Source = {barrel_ngram_source_mem, #{<<"aaaa">> => <<"aaaa">>}},
    RE = re(<<"aa">>),
    Oracle = case re:run(<<"aaaa">>, RE, [global, {capture, first, index}]) of
        {match, Ms} -> [{S, L} || [{S, L}] <- Ms]
    end,
    ?assertEqual(Oracle, ?M:windowed_regex(Source, <<"aaaa">>, RE, 0, 2, 0, [0, 1, 2])).
