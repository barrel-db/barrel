%%%-------------------------------------------------------------------
%%% @doc EUnit tests for segment compaction (merge).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_merge_tests).

-include_lib("eunit/include/eunit.hrl").

-define(SEG, barrel_ngram_segment).
-define(M, barrel_ngram_merge).

merge_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun collapse_supersede/1,
      fun tombstone_retained/1,
      fun tombstone_dropped/1,
      fun tombstone_superseded_by_live/1,
      fun positional_survives_supersede/1,
      fun positional_ordinal_remap/1,
      fun positional_dropped_with_tombstone/1
     ]}.

setup() ->
    Dir = filename:join(["/tmp",
                         "barrel_ngram_merge_" ++ integer_to_list(erlang:unique_integer([positive]))]),
    ok = filelib:ensure_dir(filename:join(Dir, "dummy")),
    Dir.

cleanup(Dir) ->
    os:cmd("rm -rf " ++ Dir),
    ok.

hlc(N) -> <<N:96>>.
gram(A, B, C) -> (A bsl 16) bor (B bsl 8) bor C.
entry(K, N, Del) -> #{key => K, hlc => hlc(N), deleted => Del}.

write_seg(Dir, Name, Postings, Entries) ->
    write_seg(Dir, Name, Postings, [], Entries).

write_seg(Dir, Name, Postings, PositionalPostings, Entries) ->
    Path = filename:join(Dir, Name),
    ok = ?SEG:write(Path, #{doc_count => length(Entries), watermark => <<0:96>>,
                            postings => Postings,
                            positional_postings => PositionalPostings,
                            entries => Entries}),
    Path.

%% Open a merged segment and return {DocCount, SortedEntries, SortedPostings}.
inspect(Path) ->
    {ok, H} = ?SEG:open(Path),
    try
        Entries = [{K, Hlc, Del} || {_O, K, Hlc, Del} <- ?SEG:entries(H)],
        {?SEG:doc_count(H),
         lists:sort(Entries),
         lists:sort(?SEG:all_postings(H))}
    after
        ?SEG:close(H)
    end.

inspect_positional(Path) ->
    {ok, H} = ?SEG:open(Path),
    try
        lists:sort(?SEG:all_positional_postings(H))
    after
        ?SEG:close(H)
    end.

collapse_supersede(Dir) ->
    fun() ->
        Gabc = gram($a, $b, $c),
        Gxyz = gram($x, $y, $z),
        Gb = gram($b, $b, $b),
        S0 = write_seg(Dir, "s0.ngseg", [{Gabc, [0]}, {Gb, [1]}],
                       [entry(<<"a">>, 10, false), entry(<<"b">>, 11, false)]),
        S1 = write_seg(Dir, "s1.ngseg", [{Gxyz, [0]}],
                       [entry(<<"a">>, 20, false)]),
        {ok, Out, DocCount, _Wm} = ?M:merge([S0, S1], false),
        {DC, Entries, Postings} = inspect(Out),
        ?assertEqual(2, DocCount),
        ?assertEqual(2, DC),
        ?assertEqual([{<<"a">>, hlc(20), false}, {<<"b">>, hlc(11), false}], Entries),
        %% a now carries only its newest grams; the superseded Gabc is gone
        OrdOf = fun(K) -> ord_of(Out, K) end,
        ?assertEqual([{Gb, [OrdOf(<<"b">>)]}, {Gxyz, [OrdOf(<<"a">>)]}],
                     lists:sort(Postings))
    end.

tombstone_retained(Dir) ->
    fun() ->
        Gabc = gram($a, $b, $c),
        S0 = write_seg(Dir, "s0.ngseg", [{Gabc, [0]}], [entry(<<"a">>, 10, false)]),
        S1 = write_seg(Dir, "s1.ngseg", [], [entry(<<"a">>, 20, true)]),
        {ok, Out, DocCount, _Wm} = ?M:merge([S0, S1], false),
        {_DC, Entries, Postings} = inspect(Out),
        ?assertEqual(1, DocCount),
        ?assertEqual([{<<"a">>, hlc(20), true}], Entries),
        ?assertEqual([], Postings)   %% tombstone carries no grams
    end.

tombstone_dropped(Dir) ->
    fun() ->
        Gabc = gram($a, $b, $c),
        S0 = write_seg(Dir, "s0.ngseg", [{Gabc, [0]}], [entry(<<"a">>, 10, false)]),
        S1 = write_seg(Dir, "s1.ngseg", [], [entry(<<"a">>, 20, true)]),
        {ok, Out, DocCount, _Wm} = ?M:merge([S0, S1], true),
        {_DC, Entries, Postings} = inspect(Out),
        ?assertEqual(0, DocCount),
        ?assertEqual([], Entries),
        ?assertEqual([], Postings)
    end.

tombstone_superseded_by_live(Dir) ->
    fun() ->
        Gabc = gram($a, $b, $c),
        %% delete then re-create with a higher HLC: the live version wins
        S0 = write_seg(Dir, "s0.ngseg", [], [entry(<<"a">>, 10, true)]),
        S1 = write_seg(Dir, "s1.ngseg", [{Gabc, [0]}], [entry(<<"a">>, 20, false)]),
        {ok, Out, DocCount, _Wm} = ?M:merge([S0, S1], true),
        {_DC, Entries, Postings} = inspect(Out),
        ?assertEqual(1, DocCount),
        ?assertEqual([{<<"a">>, hlc(20), false}], Entries),
        ?assertEqual([{Gabc, [0]}], Postings)
    end.

%% Phase-2 offsets for the surviving (newest) version of a superseded key
%% must appear in the merged output, keyed by the NEW ordinal the merge
%% assigns -- not the old ordinal from either input segment. The
%% superseded version's positional data is dropped along with its grams.
positional_survives_supersede(Dir) ->
    fun() ->
        Gabc = gram($a, $b, $c),
        Gxyz = gram($x, $y, $z),
        S0 = write_seg(Dir, "s0.ngseg", [{Gabc, [0]}], [{Gabc, [{0, [5]}]}],
                       [entry(<<"a">>, 10, false)]),
        S1 = write_seg(Dir, "s1.ngseg", [{Gxyz, [0]}], [{Gxyz, [{0, [9]}]}],
                       [entry(<<"a">>, 20, false)]),
        {ok, Out, _DocCount, _Wm} = ?M:merge([S0, S1], false),
        NewOrd = ord_of(Out, <<"a">>),
        ?assertEqual([{Gxyz, [{NewOrd, [9]}]}], inspect_positional(Out))
    end.

%% Multiple surviving keys, sorted differently than the input's ordinals,
%% so the merge genuinely reassigns ordinals -- proves offsets follow the
%% reassignment, not the stale input ordinal.
positional_ordinal_remap(Dir) ->
    fun() ->
        Ga = gram($a, $a, $a),
        Gb = gram($b, $b, $b),
        %% "zebra" is ordinal 0 in s0 (frozen first) and "apple" is
        %% ordinal 1, but the merge re-sorts by key, so the output swaps
        %% them: apple -> 0, zebra -> 1. Offsets must follow the swap.
        S0 = write_seg(Dir, "s0.ngseg", [{Ga, [0]}, {Gb, [1]}],
                       [{Ga, [{0, [2]}]}, {Gb, [{1, [4]}]}],
                       [entry(<<"zebra">>, 10, false), entry(<<"apple">>, 11, false)]),
        {ok, Out, DocCount, _Wm} = ?M:merge([S0], false),
        ?assertEqual(2, DocCount),
        AppleOrd = ord_of(Out, <<"apple">>),
        ZebraOrd = ord_of(Out, <<"zebra">>),
        ?assertEqual(0, AppleOrd),   %% "apple" < "zebra"
        ?assertEqual(1, ZebraOrd),
        ?assertEqual(lists:sort([{Ga, [{ZebraOrd, [2]}]}, {Gb, [{AppleOrd, [4]}]}]),
                     inspect_positional(Out))
    end.

%% A tombstone carries no grams (already asserted for phase-1 in
%% tombstone_retained/1); the same must hold for phase-2.
positional_dropped_with_tombstone(Dir) ->
    fun() ->
        Gabc = gram($a, $b, $c),
        S0 = write_seg(Dir, "s0.ngseg", [{Gabc, [0]}], [{Gabc, [{0, [3]}]}],
                       [entry(<<"a">>, 10, false)]),
        S1 = write_seg(Dir, "s1.ngseg", [], [], [entry(<<"a">>, 20, true)]),
        {ok, Out, DocCount, _Wm} = ?M:merge([S0, S1], false),
        ?assertEqual(1, DocCount),
        ?assertEqual([], inspect_positional(Out))
    end.

%% @private ordinal of a key in a written segment
ord_of(Path, Key) ->
    {ok, H} = ?SEG:open(Path),
    try
        [Ord] = [O || {O, K, _Hlc, _Del} <- ?SEG:entries(H), K =:= Key],
        Ord
    after
        ?SEG:close(H)
    end.
