%%%-------------------------------------------------------------------
%%% @doc EUnit tests for the sparse (content-defined) gram selector.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_selector_sparse_tests).

-include_lib("eunit/include/eunit.hrl").

-define(M, barrel_ngram_selector_sparse).
-define(DENSE, barrel_ngram_selector_dense).

opts() -> #{radius => 3, sample_rate => 4}.

sg(Bytes) -> ?M:select_grams(Bytes, opts()).
rg(Query) -> ?M:reliable_grams(Query, opts()).

deterministic_test() ->
    Bin = <<"error: connect_timeout exceeded in the pool">>,
    ?assertEqual(sg(Bin), sg(Bin)).

subset_of_dense_test() ->
    %% Sparse keeps a subset of the dense grams (a value could repeat at
    %% both a sampled and an unsampled position, so compare as sets).
    Bin = <<"the quick brown fox jumps over the lazy dog">>,
    Sparse = ordsets:from_list(sg(Bin)),
    Dense = ordsets:from_list(?DENSE:select_grams(Bin, #{})),
    ?assert(ordsets:is_subset(Sparse, Dense)).

sampling_reduces_test() ->
    %% Over a reasonably long text, sparse selects materially fewer grams.
    Bin = iolist_to_binary(
            lists:duplicate(20, <<"connect_timeout retry_backoff jitter pool ">>)),
    Sparse = length(sg(Bin)),
    Dense = length(?DENSE:select_grams(Bin, #{})),
    ?assert(Sparse < Dense).

short_literal_brute_force_test() ->
    %% Below 3 + 2r = 9 bytes there is no interior gram.
    ?assertEqual(brute_force, rg(<<>>)),
    ?assertEqual(brute_force, rg(<<"abc">>)),
    ?assertEqual(brute_force, rg(<<"12345678">>)).   %% 8 bytes < 9

reliable_is_subset_of_full_test() ->
    %% reliable_grams over a literal is a subset of select_grams over it.
    Q = <<"connect_timeout_exceeded_in_the_pool">>,
    case rg(Q) of
        brute_force -> ok;
        {reliable, Grams} ->
            Full = ordsets:from_list(sg(Q)),
            ?assert(ordsets:is_subset(ordsets:from_list(Grams), Full))
    end.

%% THE core invariant: for a text and any substring of it, every reliable
%% gram of the substring is selected in the full text. If it failed, the
%% intersection filter would drop a real match (a silent false negative).
interior_subset_invariant_test() ->
    lists:foreach(
        fun(Seed) ->
            rand:seed(exsss, {Seed, Seed * 5 + 1, Seed * 9 + 2}),
            Text = random_text(60 + rand:uniform(200)),
            TextGrams = ordsets:from_list(sg(Text)),
            lists:foreach(
                fun(_) ->
                    Sub = random_substring(Text),
                    case rg(Sub) of
                        brute_force ->
                            ok;
                        {reliable, Grams} ->
                            ?assert(ordsets:is_subset(ordsets:from_list(Grams),
                                                      TextGrams))
                    end
                end, lists:seq(1, 20))
        end, lists:seq(1, 50)).

%%====================================================================
%% Positional callbacks (phase-2 index/query)
%%====================================================================

sgp(Bytes) -> ?M:select_grams_positional(Bytes, opts()).
rgp(Query) -> ?M:reliable_grams_positional(Query, opts()).

%% select_grams/2 is implemented as select_grams_positional/2 with the
%% offsets dropped -- this is definitional (guaranteed by the shared
%% implementation), asserted here as documentation and a regression guard.
select_grams_positional_gram_set_matches_select_grams_test() ->
    Bin = <<"error: connect_timeout exceeded in the pool">>,
    ?assertEqual(ordsets:from_list(sg(Bin)),
                 ordsets:from_list([G || {G, _I} <- sgp(Bin)])).

%% Every returned offset really is where that gram occurs, and stays
%% within the byte string's trigram-position range.
select_grams_positional_offsets_are_real_positions_test() ->
    Bin = <<"the quick brown fox jumps over the lazy dog">>,
    N = byte_size(Bin),
    lists:foreach(
        fun({G, I}) ->
            ?assert(I >= 0 andalso I =< N - 3),
            <<_:I/binary, A, B, C, _/binary>> = Bin,
            ?assertEqual((A bsl 16) bor (B bsl 8) bor C, G)
        end, sgp(Bin)).

select_grams_positional_below_trigram_is_empty_test() ->
    ?assertEqual([], sgp(<<>>)),
    ?assertEqual([], sgp(<<"ab">>)).

%% reliable_grams/2 is the gram set of reliable_grams_positional/2 with
%% the offsets dropped, same relationship as the index-side pair above.
reliable_grams_positional_gram_set_matches_reliable_grams_test() ->
    Q = <<"connect_timeout_exceeded_in_the_pool">>,
    case {rg(Q), rgp(Q)} of
        {brute_force, brute_force} -> ok;
        {{reliable, Grams}, {reliable, GramOffs}} ->
            ?assertEqual(ordsets:from_list(Grams),
                         ordsets:from_list([G || {G, _I} <- GramOffs]))
    end.

%% Offsets from the query side are always interior positions (the
%% boundary rule): [R, N-3-R], never touching the literal's own edges.
reliable_grams_positional_offsets_are_interior_test() ->
    #{radius := R} = opts(),
    Q = <<"connect_timeout_exceeded_in_the_pool">>,
    N = byte_size(Q),
    case rgp(Q) of
        brute_force -> ok;
        {reliable, GramOffs} ->
            lists:foreach(
                fun({_G, I}) -> ?assert(I >= R andalso I =< N - 3 - R) end,
                GramOffs)
    end.

reliable_grams_positional_short_literal_brute_force_test() ->
    ?assertEqual(brute_force, rgp(<<>>)),
    ?assertEqual(brute_force, rgp(<<"abc">>)),
    ?assertEqual(brute_force, rgp(<<"12345678">>)).

%%====================================================================
%% normalize_opts/1 (config persisted/compared across corpus reopens)
%%====================================================================

normalize_opts_fills_defaults_test() ->
    ?assertEqual(#{radius => 3, sample_rate => 4}, ?M:normalize_opts(#{})).

normalize_opts_keeps_explicit_values_test() ->
    ?assertEqual(#{radius => 5, sample_rate => 8},
                 ?M:normalize_opts(#{radius => 5, sample_rate => 8})).

normalize_opts_partial_fills_the_rest_test() ->
    ?assertEqual(#{radius => 3, sample_rate => 8},
                 ?M:normalize_opts(#{sample_rate => 8})),
    ?assertEqual(#{radius => 5, sample_rate => 4},
                 ?M:normalize_opts(#{radius => 5})).

%% Equivalent option maps (explicit-default vs omitted) must normalize
%% identically -- this is what makes reopening a corpus with the same
%% effective tuning, phrased differently, not a spurious config_mismatch.
normalize_opts_equivalent_maps_agree_test() ->
    ?assertEqual(?M:normalize_opts(#{}),
                 ?M:normalize_opts(#{radius => 3, sample_rate => 4})).

%%====================================================================
%% Helpers
%%====================================================================

random_text(N) ->
    %% bytes from a small alphabet so substrings recur and grams overlap
    list_to_binary([$a + rand:uniform(8) - 1 || _ <- lists:seq(1, N)]).

random_substring(Text) ->
    N = byte_size(Text),
    case N of
        0 -> <<>>;
        _ ->
            Start = rand:uniform(N) - 1,
            MaxLen = N - Start,
            Len = rand:uniform(MaxLen),
            binary_part(Text, Start, Len)
    end.
