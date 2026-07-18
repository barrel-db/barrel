%%%-------------------------------------------------------------------
%%% @doc EUnit tests for the dense gram selector.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_selector_dense_tests).

-include_lib("eunit/include/eunit.hrl").

-define(M, barrel_ngram_selector_dense).

gram(A, B, C) -> (A bsl 16) bor (B bsl 8) bor C.

empty_below_trigram_test() ->
    ?assertEqual([], ?M:select_grams(<<>>)),
    ?assertEqual([], ?M:select_grams(<<"a">>)),
    ?assertEqual([], ?M:select_grams(<<"ab">>)).

single_trigram_test() ->
    ?assertEqual([gram($a, $b, $c)], ?M:select_grams(<<"abc">>)).

overlapping_trigrams_test() ->
    %% "abcd" -> "abc", "bcd"
    Expected = lists:usort([gram($a, $b, $c), gram($b, $c, $d)]),
    ?assertEqual(Expected, ?M:select_grams(<<"abcd">>)).

dedup_and_sorted_test() ->
    %% Repeated content yields a single, ascending, de-duplicated set.
    Grams = ?M:select_grams(<<"aaaa">>),
    ?assertEqual([gram($a, $a, $a)], Grams),
    ?assertEqual(lists:usort(Grams), Grams).

count_matches_length_test() ->
    %% N-byte input has exactly N-2 overlapping trigram positions
    %% (before de-duplication).
    Bin = <<"the quick brown fox">>,
    N = byte_size(Bin),
    Positions = [begin
                     <<_:I/binary, A, B, C, _/binary>> = Bin,
                     gram(A, B, C)
                 end || I <- lists:seq(0, N - 3)],
    ?assertEqual(N - 2, length(Positions)),
    ?assertEqual(lists:usort(Positions), ?M:select_grams(Bin)).

subset_property_test() ->
    %% A literal's grams are a subset of any text that contains it. This
    %% is the invariant the whole index relies on.
    Text = <<"error: connect_timeout exceeded in pool">>,
    Literal = <<"connect_timeout">>,
    TextGrams = ordsets:from_list(?M:select_grams(Text)),
    LitGrams = ordsets:from_list(?M:select_grams(Literal)),
    ?assert(ordsets:is_subset(LitGrams, TextGrams)).

reliable_grams_test() ->
    ?assertEqual(brute_force, ?M:reliable_grams(<<>>)),
    ?assertEqual(brute_force, ?M:reliable_grams(<<"ab">>)),
    ?assertEqual({reliable, ?M:select_grams(<<"abc">>)},
                 ?M:reliable_grams(<<"abc">>)).

non_ascii_bytes_test() ->
    %% Selection is byte-level: UTF-8 multibyte sequences are just bytes.
    Bin = <<"café"/utf8>>,
    Grams = ?M:select_grams(Bin),
    ?assertEqual(byte_size(Bin) - 2, length(Grams)).
