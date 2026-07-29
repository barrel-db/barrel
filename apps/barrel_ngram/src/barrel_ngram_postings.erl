%%%-------------------------------------------------------------------
%%% @doc Posting-list codec and intersection.
%%%
%%% A posting list is the set of local document ordinals that contain a
%%% given gram, stored ascending. On disk it is `[Count][Delta]...' where
%%% each value is a LEB128 varint and ordinals are delta-encoded against
%%% their predecessor (ordinals are strictly ascending, so deltas are
%%% non-negative and no zigzag is needed). This is the integer-ordinal
%%% form of the block codec used by the BM25 disk index.
%%%
%%% `intersect_all/1' ANDs several posting lists. It starts from the
%%% shortest list and galloping-searches each remaining list, so a rare
%%% gram bounds the work rather than the largest list.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_postings).

-export([encode/1, decode/1]).
-export([intersect_all/1, union_all/1]).

-type ordinal() :: non_neg_integer().
-export_type([ordinal/0]).

%%====================================================================
%% Codec
%%====================================================================

%% @doc Encode a set of ordinals as a delta+varint block. The input is
%% sorted and de-duplicated first, so callers need not pre-sort.
-spec encode([ordinal()]) -> binary().
encode(Ordinals) ->
    Sorted = lists:usort(Ordinals),
    Count = length(Sorted),
    iolist_to_binary([varint(Count) | deltas(Sorted, 0)]).

%% @doc Decode a delta+varint block back to ascending ordinals.
-spec decode(binary()) -> [ordinal()].
decode(Bin) ->
    {Count, Rest} = varint_decode(Bin),
    decode_deltas(Rest, Count, 0, []).

%% @private
deltas([], _Prev) ->
    [];
deltas([O | Rest], Prev) ->
    [varint(O - Prev) | deltas(Rest, O)].

%% @private
decode_deltas(_Bin, 0, _Prev, Acc) ->
    lists:reverse(Acc);
decode_deltas(Bin, N, Prev, Acc) ->
    {Delta, Rest} = varint_decode(Bin),
    O = Prev + Delta,
    decode_deltas(Rest, N - 1, O, [O | Acc]).

%% @private LEB128 varint encode.
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

%%====================================================================
%% Intersection
%%====================================================================

%% @doc Union several ascending posting lists into one ascending, unique
%% list (k-way merge with dedup).
-spec union_all([[ordinal()]]) -> [ordinal()].
union_all([]) ->
    [];
union_all(Lists) ->
    lists:umerge(Lists).

%% @doc Intersect several ascending posting lists. Any empty input makes
%% the whole intersection empty. The result is ascending.
-spec intersect_all([[ordinal()]]) -> [ordinal()].
intersect_all([]) ->
    [];
intersect_all(Lists) ->
    case lists:any(fun(L) -> L =:= [] end, Lists) of
        true ->
            [];
        false ->
            [Shortest | Rest] =
                lists:sort(fun(A, B) -> length(A) =< length(B) end, Lists),
            lists:foldl(fun gallop_intersect/2, Shortest, Rest)
    end.

%% @private Intersect ascending list `Small' against ascending list
%% `Large' by galloping through `Large'. Returns the ascending result.
gallop_intersect([], _Large) ->
    [];
gallop_intersect(_Small, []) ->
    [];
gallop_intersect(Small, Large) ->
    LargeT = list_to_tuple(Large),
    gi(Small, LargeT, tuple_size(LargeT), 1, []).

gi([], _T, _Size, _Lo, Acc) ->
    lists:reverse(Acc);
gi(_Small, _T, Size, Lo, Acc) when Lo > Size ->
    lists:reverse(Acc);
gi([X | Xs], T, Size, Lo, Acc) ->
    case gallop_find(T, Size, X, Lo) of
        {found, Pos} -> gi(Xs, T, Size, Pos + 1, [X | Acc]);
        {not_found, Pos} -> gi(Xs, T, Size, Pos, Acc)
    end.

%% @private Locate X in the ascending tuple T[Lo..Size]. On a hit returns
%% `{found, Pos}'; on a miss `{not_found, Pos}' where Pos is the index of
%% the first element greater than X (the new low bound to resume from).
gallop_find(T, Size, X, Lo) ->
    case element(Lo, T) of
        V when V =:= X -> {found, Lo};
        V when V > X -> {not_found, Lo};
        _ -> gallop(T, Size, X, Lo, 1)
    end.

%% Exponential probe from Lo to bound the range, then binary search.
gallop(T, Size, X, Lo, Step) ->
    Probe = Lo + Step,
    case Probe > Size of
        true ->
            bin_search(T, X, Lo + (Step div 2), Size);
        false ->
            case element(Probe, T) of
                V when V =:= X -> {found, Probe};
                V when V < X -> gallop(T, Size, X, Lo, Step * 2);
                _ -> bin_search(T, X, Lo + (Step div 2), Probe)
            end
    end.

%% Binary search for X in T[Low..High]. Returns `{found, Pos}' or
%% `{not_found, Pos}' with Pos the first index whose value exceeds X.
bin_search(_T, _X, Low, High) when Low > High ->
    {not_found, Low};
bin_search(T, X, Low, High) ->
    Mid = (Low + High) div 2,
    case element(Mid, T) of
        V when V =:= X -> {found, Mid};
        V when V < X -> bin_search(T, X, Mid + 1, High);
        _ -> bin_search(T, X, Low, Mid - 1)
    end.
