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
      fun tombstone_superseded_by_live/1
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
    Path = filename:join(Dir, Name),
    ok = ?SEG:write(Path, #{doc_count => length(Entries), watermark => <<0:96>>,
                            postings => Postings, entries => Entries}),
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

%% @private ordinal of a key in a written segment
ord_of(Path, Key) ->
    {ok, H} = ?SEG:open(Path),
    try
        [Ord] = [O || {O, K, _Hlc, _Del} <- ?SEG:entries(H), K =:= Key],
        Ord
    after
        ?SEG:close(H)
    end.
