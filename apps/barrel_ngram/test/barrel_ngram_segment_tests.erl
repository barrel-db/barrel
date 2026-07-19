%%%-------------------------------------------------------------------
%%% @doc EUnit tests for the immutable segment format (v2).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_segment_tests).

-include_lib("eunit/include/eunit.hrl").

-define(M, barrel_ngram_segment).

segment_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun round_trip/1,
      fun tombstone_entry/1,
      fun all_postings_directory/1,
      fun empty_segment/1,
      fun absent_gram/1,
      fun high_gram_beyond_table/1
     ]}.

setup() ->
    Dir = filename:join(["/tmp",
                         "barrel_ngram_seg_" ++ integer_to_list(erlang:unique_integer([positive]))]),
    ok = filelib:ensure_dir(filename:join(Dir, "dummy")),
    Dir.

cleanup(Dir) ->
    os:cmd("rm -rf " ++ Dir),
    ok.

wm() -> <<9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9>>.
hlc(N) -> <<N:96>>.
gram(A, B, C) -> (A bsl 16) bor (B bsl 8) bor C.

entry(Key, N, Del) -> #{key => Key, hlc => hlc(N), deleted => Del}.

round_trip(Dir) ->
    fun() ->
        Path = filename:join(Dir, "seg.ngseg"),
        G1 = gram($a, $b, $c),
        G2 = gram($x, $y, $z),
        Spec = #{
            doc_count => 3,
            watermark => wm(),
            postings => [{G2, [1, 2]}, {G1, [0, 2]}],
            entries => [entry(<<"doc-zero">>, 10, false),
                        entry(<<"doc-one">>, 20, false),
                        entry(<<"doc-two">>, 30, false)]
        },
        ok = ?M:write(Path, Spec),
        {ok, H} = ?M:open(Path),
        try
            ?assertEqual(3, ?M:doc_count(H)),
            ?assertEqual(wm(), ?M:watermark(H)),
            ?assertEqual({ok, [0, 2]}, ?M:lookup_postings(H, G1)),
            ?assertEqual({ok, [1, 2]}, ?M:lookup_postings(H, G2)),
            ?assertEqual([{0, <<"doc-zero">>}, {2, <<"doc-two">>}],
                         ?M:keys(H, [0, 2])),
            %% per-ordinal hlc + deleted round-trips
            ?assertEqual([{0, <<"doc-zero">>, hlc(10), false},
                          {1, <<"doc-one">>, hlc(20), false},
                          {2, <<"doc-two">>, hlc(30), false}],
                         ?M:entries(H))
        after
            ?M:close(H)
        end
    end.

tombstone_entry(Dir) ->
    fun() ->
        Path = filename:join(Dir, "tomb.ngseg"),
        G = gram($a, $b, $c),
        Spec = #{
            doc_count => 2,
            watermark => wm(),
            postings => [{G, [0]}],
            entries => [entry(<<"live">>, 5, false),
                        entry(<<"dead">>, 7, true)]  %% tombstone, no grams
        },
        ok = ?M:write(Path, Spec),
        {ok, H} = ?M:open(Path),
        try
            ?assertEqual([{0, <<"live">>, hlc(5), false},
                          {1, <<"dead">>, hlc(7), true}],
                         ?M:entries(H)),
            %% the tombstone ordinal appears in no posting list
            ?assertEqual({ok, [0]}, ?M:lookup_postings(H, G))
        after
            ?M:close(H)
        end
    end.

all_postings_directory(Dir) ->
    fun() ->
        Path = filename:join(Dir, "allp.ngseg"),
        G1 = gram($a, $b, $c),
        G2 = gram($d, $e, $f),
        G3 = gram($x, $y, $z),
        Spec = #{
            doc_count => 3,
            watermark => wm(),
            postings => [{G3, [2]}, {G1, [0, 1]}, {G2, [1]}],
            entries => [entry(<<"k0">>, 1, false),
                        entry(<<"k1">>, 2, false),
                        entry(<<"k2">>, 3, false)]
        },
        ok = ?M:write(Path, Spec),
        {ok, H} = ?M:open(Path),
        try
            All = lists:sort(?M:all_postings(H)),
            ?assertEqual(lists:sort([{G1, [0, 1]}, {G2, [1]}, {G3, [2]}]), All)
        after
            ?M:close(H)
        end
    end.

empty_segment(Dir) ->
    fun() ->
        Path = filename:join(Dir, "empty.ngseg"),
        ok = ?M:write(Path, #{doc_count => 0, watermark => wm(),
                              postings => [], entries => []}),
        {ok, H} = ?M:open(Path),
        try
            ?assertEqual(0, ?M:doc_count(H)),
            ?assertEqual(empty, ?M:lookup_postings(H, gram($a, $b, $c))),
            ?assertEqual([], ?M:keys(H, [0, 1])),
            ?assertEqual([], ?M:entries(H)),
            ?assertEqual([], ?M:all_postings(H))
        after
            ?M:close(H)
        end
    end.

absent_gram(Dir) ->
    fun() ->
        Path = filename:join(Dir, "absent.ngseg"),
        G = gram($a, $b, $c),
        ok = ?M:write(Path, #{doc_count => 1, watermark => wm(),
                              postings => [{G, [0]}],
                              entries => [entry(<<"k">>, 1, false)]}),
        {ok, H} = ?M:open(Path),
        try
            ?assertEqual(empty, ?M:lookup_postings(H, 0)),
            ?assertEqual({ok, [0]}, ?M:lookup_postings(H, G))
        after
            ?M:close(H)
        end
    end.

high_gram_beyond_table(Dir) ->
    fun() ->
        Path = filename:join(Dir, "high.ngseg"),
        Low = gram(0, 0, 5),
        ok = ?M:write(Path, #{doc_count => 1, watermark => wm(),
                              postings => [{Low, [0]}],
                              entries => [entry(<<"k">>, 1, false)]}),
        {ok, H} = ?M:open(Path),
        try
            ?assertEqual(empty, ?M:lookup_postings(H, 16#FFFFFF)),
            ?assertEqual({ok, [0]}, ?M:lookup_postings(H, Low))
        after
            ?M:close(H)
        end
    end.
