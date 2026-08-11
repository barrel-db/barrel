%%%-------------------------------------------------------------------
%%% @doc EUnit tests for the positional (phase-2) posting-list codec.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_postings_positional_tests).

-include_lib("eunit/include/eunit.hrl").

-define(M, barrel_ngram_postings_positional).

%%====================================================================
%% Roundtrip
%%====================================================================

roundtrip_empty_test() ->
    ?assertEqual([], ?M:decode(?M:encode([]))).

roundtrip_single_test() ->
    ?assertEqual([{0, [0]}], ?M:decode(?M:encode([{0, [0]}]))),
    ?assertEqual([{42, [7]}], ?M:decode(?M:encode([{42, [7]}]))).

roundtrip_dense_test() ->
    %% every ordinal 0..N-1 present, one offset each
    Entries = [{O, [O * 3]} || O <- lists:seq(0, 99)],
    ?assertEqual(Entries, ?M:decode(?M:encode(Entries))).

roundtrip_sparse_test() ->
    %% widely-spaced ordinals, several offsets each
    Entries = [{0, [5, 100, 4000]},
               {200, [0]},
               {16777215, [1, 2, 3]}],
    ?assertEqual(Entries, ?M:decode(?M:encode(Entries))).

roundtrip_large_offsets_test() ->
    Entries = [{1, [0, 1000000, 5000000, 16000000]}],
    ?assertEqual(Entries, ?M:decode(?M:encode(Entries))).

roundtrip_random_test() ->
    lists:foreach(
        fun(Seed) ->
            rand:seed(exsss, {Seed, Seed * 5 + 1, Seed * 11 + 3}),
            Ords = lists:usort([rand:uniform(1 bsl 20) - 1 || _ <- lists:seq(1, 40)]),
            Entries = [{O, lists:usort([rand:uniform(100000) - 1
                                         || _ <- lists:seq(1, 1 + rand:uniform(5))])}
                       || O <- Ords],
            ?assertEqual(Entries, ?M:decode(?M:encode(Entries)))
        end, lists:seq(1, 30)).

encode_sorts_and_merges_duplicates_test() ->
    %% out-of-order input, and a duplicate ordinal whose offsets must be
    %% unioned rather than one silently overwriting the other
    Entries = [{5, [10]}, {1, [1]}, {5, [2]}],
    ?assertEqual([{1, [1]}, {5, [2, 10]}], ?M:decode(?M:encode(Entries))).

%%====================================================================
%% Cursor
%%====================================================================

cursor_drain_matches_decode_test() ->
    Entries = [{0, [1, 2]}, {3, [0]}, {9, [5, 6, 7]}],
    Bin = ?M:encode(Entries),
    ?assertEqual(?M:decode(Bin), cursor_drain(?M:cursor(Bin))).

cursor_empty_block_is_immediately_done_test() ->
    ?assertEqual(done, ?M:next(?M:cursor(?M:encode([])))).

cursor_drain(Cursor) ->
    case ?M:next(Cursor) of
        done -> [];
        {Ord, Offs, Cursor1} -> [{Ord, Offs} | cursor_drain(Cursor1)]
    end.

%% Cursor-based lockstep merge-join must find the same set of common
%% ordinals (with their per-side offsets) as a naive reference that
%% decodes both blocks in full and intersects by ordinal via maps. This is
%% the primitive the distance-check intersection will build on.
cursor_merge_join_matches_naive_test() ->
    lists:foreach(
        fun(Seed) ->
            rand:seed(exsss, {Seed, Seed * 13 + 7, Seed * 17 + 1}),
            EntriesA = random_entries(),
            EntriesB = random_entries(),
            BinA = ?M:encode(EntriesA),
            BinB = ?M:encode(EntriesB),
            Expected = naive_common(EntriesA, EntriesB),
            Actual = lockstep_common(?M:cursor(BinA), ?M:cursor(BinB)),
            ?assertEqual(Expected, Actual)
        end, lists:seq(1, 50)).

random_entries() ->
    Ords = lists:usort([rand:uniform(50) || _ <- lists:seq(1, 20)]),
    [{O, lists:usort([rand:uniform(1000) || _ <- lists:seq(1, 1 + rand:uniform(3))])}
     || O <- Ords].

naive_common(EntriesA, EntriesB) ->
    MapB = maps:from_list(EntriesB),
    lists:sort(
      [{O, OffsA, maps:get(O, MapB)}
       || {O, OffsA} <- EntriesA, maps:is_key(O, MapB)]).

%% @private Advance whichever cursor has the smaller ordinal; on a match,
%% collect {Ordinal, OffsetsA, OffsetsB} and advance both.
lockstep_common(CA, CB) ->
    lists:sort(lockstep_walk(?M:next(CA), ?M:next(CB), [])).

lockstep_walk(done, _NB, Acc) ->
    lists:reverse(Acc);
lockstep_walk(_NA, done, Acc) ->
    lists:reverse(Acc);
lockstep_walk({OA, OffsA, CA1}, {OB, OffsB, CB1}, Acc) when OA =:= OB ->
    lockstep_walk(?M:next(CA1), ?M:next(CB1), [{OA, OffsA, OffsB} | Acc]);
lockstep_walk({OA, _OffsA, CA1}, {OB, _OffsB, _CB1} = NB, Acc) when OA < OB ->
    lockstep_walk(?M:next(CA1), NB, Acc);
lockstep_walk({OA, _OffsA, _CA1} = NA, {OB, _OffsB, CB1}, Acc) when OB < OA ->
    lockstep_walk(NA, ?M:next(CB1), Acc).

%%====================================================================
%% distance_check/4
%%====================================================================

distance_check_basic_test() ->
    %% gram A at literal offset 0, gram B at literal offset 4 ("abcd_"):
    %% a real occurrence in a document has OffB - OffA =:= 4.
    BlockA = ?M:encode([{0, [10]}, {1, [20]}]),
    BlockB = ?M:encode([{0, [14]}, {1, [999]}]),   %% doc 1: no valid pairing
    ?assertEqual([{0, [10]}], ?M:distance_check(BlockA, 0, BlockB, 4)).

%% A gram repeated at several positions in the same document: only the
%% pairs whose distance matches contribute a candidate, and a document
%% can legitimately produce more than one candidate start.
distance_check_repeated_grams_test() ->
    %% doc 0: gram A at 5 and 50; gram B at 9 (matches 5, dist 4) and 200
    %% (matches nothing). doc 1: gram A at 1, 2; gram B at 5, 6 -- both
    %% pairs (1,5) and (2,6) satisfy dist 4, two independent candidates.
    BlockA = ?M:encode([{0, [5, 50]}, {1, [1, 2]}]),
    BlockB = ?M:encode([{0, [9, 200]}, {1, [5, 6]}]),
    ?assertEqual([{0, [5]}, {1, [1, 2]}], ?M:distance_check(BlockA, 0, BlockB, 4)).

%% Distance 0: the two grams must occur at the exact same document offset.
distance_check_distance_zero_test() ->
    BlockA = ?M:encode([{0, [7, 12]}]),
    BlockB = ?M:encode([{0, [7, 99]}]),
    ?assertEqual([{0, [7]}], ?M:distance_check(BlockA, 0, BlockB, 0)).

%% Adjacent literal positions (dist 1) -- the common case for two
%% consecutive trigrams extracted from a literal.
distance_check_adjacent_test() ->
    BlockA = ?M:encode([{0, [10]}]),
    BlockB = ?M:encode([{0, [11]}]),
    ?assertEqual([{0, [10]}], ?M:distance_check(BlockA, 0, BlockB, 1)).

%% A pairing whose derived match start would be negative (the gram sits
%% too close to the start of the document to fit the literal's prefix
%% before it) is rejected, not clamped or wrapped.
distance_check_rejects_negative_start_test() ->
    %% gram A at literal offset 10, but its only document occurrence is at
    %% byte 3 -- 3 - 10 < 0, so even though gram B's offset satisfies the
    %% raw distance equation, the candidate is impossible and dropped.
    BlockA = ?M:encode([{0, [3]}]),
    BlockB = ?M:encode([{0, [3 + (20 - 10)]}]),
    ?assertEqual([], ?M:distance_check(BlockA, 10, BlockB, 20)).

distance_check_no_matching_ordinal_returns_empty_test() ->
    BlockA = ?M:encode([{0, [1]}]),
    BlockB = ?M:encode([{1, [1]}]),
    ?assertEqual([], ?M:distance_check(BlockA, 0, BlockB, 0)).

distance_check_empty_block_returns_empty_test() ->
    BlockA = ?M:encode([{0, [1]}]),
    Empty = ?M:encode([]),
    ?assertEqual([], ?M:distance_check(BlockA, 0, Empty, 0)),
    ?assertEqual([], ?M:distance_check(Empty, 0, BlockA, 0)).

%% Exhaustive correctness: the cursor-based implementation must match a
%% naive reference that fully decodes both sides and nested-loops every
%% offset pair, across many random blocks and literal-offset distances.
distance_check_random_matches_naive_test() ->
    lists:foreach(
        fun(Seed) ->
            rand:seed(exsss, {Seed, Seed * 19 + 3, Seed * 23 + 7}),
            EntriesA = random_entries(),
            EntriesB = random_entries(),
            D1 = rand:uniform(20) - 1,
            D2 = rand:uniform(20) - 1,
            BlockA = ?M:encode(EntriesA),
            BlockB = ?M:encode(EntriesB),
            Expected = naive_distance_check(EntriesA, D1, EntriesB, D2),
            Actual = lists:sort(?M:distance_check(BlockA, D1, BlockB, D2)),
            ?assertEqual(Expected, Actual)
        end, lists:seq(1, 100)).

naive_distance_check(EntriesA, D1, EntriesB, D2) ->
    MapB = maps:from_list(EntriesB),
    Dist = D2 - D1,
    Results = [begin
                   OffsB = maps:get(O, MapB),
                   Starts = lists:usort(
                       [OffA - D1 || OffA <- OffsA, OffB <- OffsB,
                                     OffB - OffA =:= Dist, OffA - D1 >= 0]),
                   {O, Starts}
               end || {O, OffsA} <- EntriesA, maps:is_key(O, MapB)],
    lists:sort([R || {_O, Starts} = R <- Results, Starts =/= []]).

%%====================================================================
%% single_gram_candidates/2
%%====================================================================

single_gram_candidates_basic_test() ->
    Block = ?M:encode([{0, [5, 10]}, {1, [3]}]),
    ?assertEqual([{0, [2, 7]}, {1, [0]}], ?M:single_gram_candidates(Block, 3)).

%% An offset too close to the start of its document (off - d < 0) is
%% dropped; an ordinal whose every offset is dropped this way disappears
%% entirely rather than surviving with an empty offset list.
single_gram_candidates_rejects_negative_test() ->
    Block = ?M:encode([{0, [1, 10]}, {1, [2]}]),
    ?assertEqual([{0, [5]}], ?M:single_gram_candidates(Block, 5)).

single_gram_candidates_random_matches_naive_test() ->
    lists:foreach(
        fun(Seed) ->
            rand:seed(exsss, {Seed, Seed * 29 + 5, Seed * 31 + 11}),
            Entries = random_entries(),
            D = rand:uniform(20) - 1,
            Block = ?M:encode(Entries),
            Expected = naive_single_gram(Entries, D),
            Actual = ?M:single_gram_candidates(Block, D),
            ?assertEqual(Expected, Actual)
        end, lists:seq(1, 100)).

naive_single_gram(Entries, D) ->
    Results = [{O, lists:usort([Off - D || Off <- Offs, Off - D >= 0])}
               || {O, Offs} <- Entries],
    [R || {_O, Starts} = R <- Results, Starts =/= []].
