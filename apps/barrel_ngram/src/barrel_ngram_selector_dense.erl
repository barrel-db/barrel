%%%-------------------------------------------------------------------
%%% @doc Dense gram selector: every overlapping byte trigram.
%%%
%%% This is the reference selector. It emits every trigram of the input,
%%% so a literal's grams are always a subset of its containing document's
%%% grams, and every gram is reliable to intersect over. It is also the
%%% oracle against which the content-defined selector is validated.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_selector_dense).
-behaviour(barrel_ngram_selector).

-export([select_grams/1, reliable_grams/1]).

%% @doc Every overlapping trigram of `Bytes', deduplicated and ascending.
%% Inputs shorter than three bytes contribute no grams.
-spec select_grams(binary()) -> [barrel_ngram_selector:gram()].
select_grams(Bytes) when byte_size(Bytes) >= 3 ->
    N = byte_size(Bytes),
    lists:usort([gram_at(Bytes, I) || I <- lists:seq(0, N - 3)]);
select_grams(_) ->
    [].

%% @doc For the dense selector every gram is reliable. Literals shorter
%% than a trigram carry no grams to intersect, so the planner must fall
%% back to a brute-force scan of the live set.
-spec reliable_grams(binary()) -> barrel_ngram_selector:reliable().
reliable_grams(Query) when byte_size(Query) >= 3 ->
    {reliable, select_grams(Query)};
reliable_grams(_) ->
    brute_force.

%% @private Pack the trigram at byte offset I big-endian into 24 bits.
gram_at(Bytes, I) ->
    A = binary:at(Bytes, I),
    B = binary:at(Bytes, I + 1),
    C = binary:at(Bytes, I + 2),
    (A bsl 16) bor (B bsl 8) bor C.
