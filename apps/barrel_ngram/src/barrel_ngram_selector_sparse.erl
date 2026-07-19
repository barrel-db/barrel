%%%-------------------------------------------------------------------
%%% @doc Sparse (content-defined) gram selector.
%%%
%%% Selects only a content-determined subset of trigrams, shrinking the
%%% index and the candidate sets. A gram at byte position `i' (the trigram
%%% over `Bytes[i..i+2]') is kept iff a hash of its local window passes a
%%% sampling test: `phash2(Window, SampleRate) =:= 0'. The window has
%%% radius `r', spanning `Bytes[i-r .. i+2+r]' (width `3 + 2r'), so the
%%% choice depends only on local bytes and identical substrings select
%%% identical grams.
%%%
%%% == The boundary rule ==
%%% When a literal `L' occurs in a document `D' at position `p', a gram at
%%% literal-position `i' whose window lies entirely inside `L' (`i >= r'
%%% and `i+2+r =< len(L)-1') has, in `D', the identical window (that range
%%% sits inside the matched region), so it is selected in `D' iff selected
%%% in `L'. Grams whose window spills past `L''s edge see different bytes
%%% in `D' and are NOT reliable.
%%%
%%% So `select_grams/2' (index side) selects over every position, padding
%%% document-edge windows with a sentinel, while `reliable_grams/2' (query
%%% side) selects only over INTERIOR positions whose window is fully inside
%%% the literal, and falls back to `brute_force' when there is no such
%%% gram. Relying on a boundary gram would be a silent false negative, so
%%% the query never does.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_selector_sparse).
-behaviour(barrel_ngram_selector).

-export([select_grams/2, reliable_grams/2, covers_all_grams/1]).

%% @doc Sparse samples grams, so an arbitrary trigram may be absent; the
%% regex planner must not rely on the index being complete.
-spec covers_all_grams(map()) -> boolean().
covers_all_grams(_Opts) -> false.

-define(DEFAULT_RADIUS, 3).
-define(DEFAULT_SAMPLE_RATE, 4).
-define(SENTINEL, 0).

%% @doc Grams selected over the full byte string, with sentinel-padded
%% windows at the edges. Used by the indexer.
-spec select_grams(binary(), map()) -> [barrel_ngram_selector:gram()].
select_grams(Bytes, Opts) when byte_size(Bytes) >= 3 ->
    N = byte_size(Bytes),
    {R, Rate} = params(Opts),
    lists:usort(
      [gram_at(Bytes, I)
       || I <- lists:seq(0, N - 3),
          sampled(window_padded(Bytes, I, R, N), Rate)]);
select_grams(_, _Opts) ->
    [].

%% @doc The literal's grams the planner may intersect over: only interior
%% positions whose window is fully inside the literal. `brute_force' when
%% there is no such sampled gram.
-spec reliable_grams(binary(), map()) -> barrel_ngram_selector:reliable().
reliable_grams(Query, Opts) ->
    N = byte_size(Query),
    {R, Rate} = params(Opts),
    %% interior positions i where [i-r, i+2+r] is fully inside the literal
    Grams = case N >= 3 + 2 * R of
        true ->
            lists:usort(
              [gram_at(Query, I)
               || I <- lists:seq(R, N - 3 - R),
                  sampled(binary_part(Query, I - R, 3 + 2 * R), Rate)]);
        false ->
            []
    end,
    case Grams of
        [] -> brute_force;
        _ -> {reliable, Grams}
    end.

%%====================================================================
%% Internal
%%====================================================================

params(Opts) ->
    {maps:get(radius, Opts, ?DEFAULT_RADIUS),
     maps:get(sample_rate, Opts, ?DEFAULT_SAMPLE_RATE)}.

sampled(Window, Rate) ->
    erlang:phash2(Window, Rate) =:= 0.

%% @private The window around position I, padding out-of-range bytes with
%% the sentinel (index side, where edge windows are unavoidable).
window_padded(Bytes, I, R, N) ->
    list_to_binary([byte_at(Bytes, J, N) || J <- lists:seq(I - R, I + 2 + R)]).

byte_at(_Bytes, J, N) when J < 0; J >= N -> ?SENTINEL;
byte_at(Bytes, J, _N) -> binary:at(Bytes, J).

%% @private Pack the trigram at byte offset I big-endian into 24 bits.
gram_at(Bytes, I) ->
    A = binary:at(Bytes, I),
    B = binary:at(Bytes, I + 1),
    C = binary:at(Bytes, I + 2),
    (A bsl 16) bor (B bsl 8) bor C.
