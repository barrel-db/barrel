%%%-------------------------------------------------------------------
%%% @doc Positional (phase-2) posting-list codec: like
%%% {@link barrel_ngram_postings}, but per ordinal it stores the byte
%%% offsets where the gram occurs, not just the ordinal itself.
%%%
%%% On disk: `[DocCount:varint]([OrdinalDelta:varint][OffsetCount:varint]
%%% [OffsetDelta:varint]*OffsetCount)*DocCount', both deltas ascending.
%%%
%%% Reading is cursor-based: intersecting two blocks by lockstep
%%% merge-join (advance whichever side has the smaller ordinal, decode
%%% offsets only when both agree) never materializes either block in
%%% full. `decode/1' is `cursor/1' drained to a list.
%%%
%%% `distance_check/4' merge-joins two grams at known literal offsets
%%% `d1'/`d2', keeping an offset pair only if its document-space distance
%%% matches the grams' literal-space distance (`off2-off1 =:= d2-d1') --
%%% a candidate match start at `off1-d1', still subject to verification.
%%% `single_gram_candidates/2' is the same `off-d' math for one gram.
%%% Both reject a negative start.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_postings_positional).

-export([encode/1, decode/1]).
-export([cursor/1, next/1]).
-export([distance_check/4, single_gram_candidates/2]).

-type ordinal() :: barrel_ngram_postings:ordinal().
-type offset() :: non_neg_integer().
-type entry() :: {ordinal(), [offset()]}.
-export_type([offset/0, entry/0]).

-record(cursor, {bin :: binary(), remaining :: non_neg_integer(), prev :: ordinal()}).
-opaque cursor() :: #cursor{}.
-export_type([cursor/0]).

%%====================================================================
%% Codec
%%====================================================================

%% @doc Encode `[{Ordinal, [Offset]}]' as a positional block. Entries need
%% not be pre-sorted; a duplicate ordinal has its offset lists merged
%% rather than one silently overwriting the other.
-spec encode([entry()]) -> binary().
encode(Entries) ->
    Merged = merge_dup_ordinals(lists:keysort(1, Entries)),
    DocCount = length(Merged),
    iolist_to_binary([varint(DocCount) | encode_entries(Merged, 0)]).

%% @doc Decode a positional block back to `[{Ordinal, [Offset]}]', ascending
%% by ordinal, each offset list ascending.
-spec decode(binary()) -> [entry()].
decode(Bin) ->
    drain(cursor(Bin), []).

drain(Cursor, Acc) ->
    case next(Cursor) of
        done -> lists:reverse(Acc);
        {Ord, Offs, Cursor1} -> drain(Cursor1, [{Ord, Offs} | Acc])
    end.

%%====================================================================
%% Cursor (lockstep merge-join primitive)
%%====================================================================

%% @doc A cursor positioned before the first entry of a positional block.
-spec cursor(binary()) -> cursor().
cursor(<<>>) ->
    #cursor{bin = <<>>, remaining = 0, prev = 0};
cursor(Bin) ->
    {DocCount, Rest} = varint_decode(Bin),
    #cursor{bin = Rest, remaining = DocCount, prev = 0}.

%% @doc The next `{Ordinal, Offsets, Cursor}', or `done'.
-spec next(cursor()) -> {ordinal(), [offset()], cursor()} | done.
next(#cursor{remaining = 0}) ->
    done;
next(#cursor{bin = Bin, remaining = N, prev = Prev}) ->
    {OrdDelta, Rest0} = varint_decode(Bin),
    Ord = Prev + OrdDelta,
    {OffCount, Rest1} = varint_decode(Rest0),
    {Offsets, Rest2} = decode_offsets(Rest1, OffCount, 0, []),
    {Ord, Offsets, #cursor{bin = Rest2, remaining = N - 1, prev = Ord}}.

%%====================================================================
%% Distance-check intersection
%%====================================================================

%% @doc Candidate match starts common to both blocks -- see the moduledoc.
%% Ascending by ordinal; each ordinal's starts are ascending and deduplicated.
-spec distance_check(binary(), non_neg_integer(), binary(), non_neg_integer()) ->
    [entry()].
distance_check(BlockA, D1, BlockB, D2) ->
    walk(next(cursor(BlockA)), D1, next(cursor(BlockB)), D2, []).

walk(done, _D1, _NB, _D2, Acc) ->
    lists:reverse(Acc);
walk(_NA, _D1, done, _D2, Acc) ->
    lists:reverse(Acc);
walk({OA, OffsA, CA}, D1, {OB, OffsB, CB}, D2, Acc) when OA =:= OB ->
    Acc1 = case match_starts(OffsA, D1, OffsB, D2) of
        [] -> Acc;
        Starts -> [{OA, Starts} | Acc]
    end,
    walk(next(CA), D1, next(CB), D2, Acc1);
walk({OA, _OffsA, CA}, D1, {OB, _OffsB, _CB} = NB, D2, Acc) when OA < OB ->
    walk(next(CA), D1, NB, D2, Acc);
walk({OA, _OffsA, _CA} = NA, D1, {OB, _OffsB, CB}, D2, Acc) when OB < OA ->
    walk(NA, D1, next(CB), D2, Acc).

%% @private Every (OffA, OffB) pair whose document-space distance matches
%% the grams' literal-space distance, converted to the (deduplicated,
%% ascending) list of candidate match starts.
match_starts(OffsA, D1, OffsB, D2) ->
    Dist = D2 - D1,
    lists:usort(
      [OffA - D1
       || OffA <- OffsA, OffB <- OffsB,
          OffB - OffA =:= Dist, OffA - D1 >= 0]).

%% @doc The one-gram case: every offset of `Block''s gram is itself a
%% candidate start (`off - D'). Same negative-start rejection as
%% {@link distance_check/4}.
-spec single_gram_candidates(binary(), non_neg_integer()) -> [entry()].
single_gram_candidates(Block, D) ->
    lists:filtermap(
        fun({Ord, Offs}) ->
            case lists:usort([Off - D || Off <- Offs, Off - D >= 0]) of
                [] -> false;
                Starts -> {true, {Ord, Starts}}
            end
        end, decode(Block)).

%%====================================================================
%% Internal
%%====================================================================

%% @private Entries arrive keysorted by ordinal; fold adjacent duplicates
%% into one entry with the union of their offsets.
merge_dup_ordinals([]) ->
    [];
merge_dup_ordinals([{O, Offs} | Rest]) ->
    merge_dup_ordinals(Rest, O, lists:usort(Offs), []).

merge_dup_ordinals([], O, Offs, Acc) ->
    lists:reverse([{O, Offs} | Acc]);
merge_dup_ordinals([{O, Offs2} | Rest], O, Offs, Acc) ->
    merge_dup_ordinals(Rest, O, lists:umerge(Offs, lists:usort(Offs2)), Acc);
merge_dup_ordinals([{O2, Offs2} | Rest], O, Offs, Acc) ->
    merge_dup_ordinals(Rest, O2, lists:usort(Offs2), [{O, Offs} | Acc]).

encode_entries([], _Prev) ->
    [];
encode_entries([{O, Offs} | Rest], Prev) ->
    [varint(O - Prev), varint(length(Offs)), encode_offsets(Offs, 0)
     | encode_entries(Rest, O)].

encode_offsets([], _Prev) ->
    [];
encode_offsets([Off | Rest], Prev) ->
    [varint(Off - Prev) | encode_offsets(Rest, Off)].

decode_offsets(Bin, 0, _Prev, Acc) ->
    {lists:reverse(Acc), Bin};
decode_offsets(Bin, N, Prev, Acc) ->
    {Delta, Rest} = varint_decode(Bin),
    Off = Prev + Delta,
    decode_offsets(Rest, N - 1, Off, [Off | Acc]).

%% @private LEB128 varint encode (same scheme as barrel_ngram_postings).
varint(N) when N < 128 ->
    <<N:8>>;
varint(N) ->
    Byte = 16#80 bor (N band 16#7F),
    <<Byte:8, (varint(N bsr 7))/binary>>.

%% @private LEB128 varint decode.
varint_decode(Bin) ->
    varint_decode(Bin, 0, 0).

varint_decode(<<Byte:8, Rest/binary>>, Acc, Shift) ->
    Acc1 = Acc bor ((Byte band 16#7F) bsl Shift),
    case Byte band 16#80 of
        0 -> {Acc1, Rest};
        _ -> varint_decode(Rest, Acc1, Shift + 7)
    end.
