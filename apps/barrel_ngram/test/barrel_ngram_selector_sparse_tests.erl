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
