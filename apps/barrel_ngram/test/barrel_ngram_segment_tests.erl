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
      fun high_gram_beyond_table/1,
      fun positional_composite_round_trip/1,
      fun positional_doc_count_table/1,
      fun roaring_with_positional_uncorrupted/1,
      fun unsupported_version_error/1,
      fun all_positional_postings_directory/1
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

%% v4: a composite posting block carrying phase-2 (positional) data for
%% some grams and not others. lookup_postings/lookup_block must see only
%% the phase-1 bytes either way; lookup_positional_block must see exactly
%% the phase-2 bytes, or not_found when there are none.
positional_composite_round_trip(Dir) ->
    fun() ->
        Path = filename:join(Dir, "positional.ngseg"),
        G1 = gram($a, $b, $c),
        G2 = gram($x, $y, $z),
        Spec = #{
            doc_count => 2,
            watermark => wm(),
            postings => [{G1, [0, 1]}, {G2, [1]}],
            positional_postings => [{G1, [{0, [3]}, {1, [7, 20]}]}],
            entries => [entry(<<"doc-zero">>, 10, false),
                        entry(<<"doc-one">>, 20, false)]
        },
        ok = ?M:write(Path, Spec),
        {ok, H} = ?M:open(Path),
        try
            %% phase-1 lookups are unaffected by phase-2 data riding along
            ?assertEqual({ok, [0, 1]}, ?M:lookup_postings(H, G1)),
            ?assertEqual({ok, [1]}, ?M:lookup_postings(H, G2)),
            %% phase-2 present for G1, absent for G2 and for an unrelated gram
            {ok, PosBlock} = ?M:lookup_positional_block(H, G1),
            ?assertEqual([{0, [3]}, {1, [7, 20]}],
                         barrel_ngram_postings_positional:decode(PosBlock)),
            ?assertEqual(not_found, ?M:lookup_positional_block(H, G2)),
            ?assertEqual(not_found, ?M:lookup_positional_block(H, gram(0, 0, 1)))
        after
            ?M:close(H)
        end
    end.

positional_doc_count_table(Dir) ->
    fun() ->
        Path = filename:join(Dir, "doccount.ngseg"),
        G1 = gram($a, $b, $c),
        G2 = gram($x, $y, $z),
        Spec = #{
            doc_count => 3,
            watermark => wm(),
            postings => [{G1, [0, 1, 2]}, {G2, [0]}],
            positional_postings => [{G1, [{0, [1]}, {1, [2]}, {2, [3]}]},
                                     {G2, [{0, [0]}]}],
            entries => [entry(<<"k0">>, 1, false),
                        entry(<<"k1">>, 2, false),
                        entry(<<"k2">>, 3, false)]
        },
        ok = ?M:write(Path, Spec),
        {ok, H} = ?M:open(Path),
        try
            ?assertEqual({ok, 3}, ?M:positional_doc_count(H, G1)),
            ?assertEqual({ok, 1}, ?M:positional_doc_count(H, G2)),
            ?assertEqual(not_found, ?M:positional_doc_count(H, gram(0, 0, 9)))
        after
            ?M:close(H)
        end
    end.

%% A roaring-codec segment (phase-1 blocks are opaque NIF binaries) still
%% carrying phase-2 payloads for some grams: the roaring set-op must only
%% ever see the phase-1 bytes, never corrupted by trailing phase-2 data.
roaring_with_positional_uncorrupted(Dir) ->
    fun() ->
        Path = filename:join(Dir, "roaring_positional.ngseg"),
        G1 = gram($a, $b, $c),
        G2 = gram($x, $y, $z),
        Ords1 = lists:seq(0, 49),
        Ords2 = lists:seq(25, 74),
        Spec = #{
            doc_count => 75,
            watermark => wm(),
            postings => [{G1, Ords1}, {G2, Ords2}],
            positional_postings => [{G1, [{O, [O rem 17]} || O <- Ords1]}],
            entries => [entry(iolist_to_binary([<<"k">>, integer_to_binary(N)]), N, false)
                        || N <- lists:seq(0, 74)],
            codec => roaring
        },
        ok = ?M:write(Path, Spec),
        {ok, H} = ?M:open(Path),
        try
            {ok, B1} = ?M:lookup_block(H, G1),
            {ok, B2} = ?M:lookup_block(H, G2),
            Intersected = barrel_ngram_roaring:decode(
                            barrel_ngram_roaring:intersect_all([B1, B2])),
            ?assertEqual(lists:seq(25, 49), Intersected),
            {ok, PosBlock} = ?M:lookup_positional_block(H, G1),
            ?assertEqual([{O, [O rem 17]} || O <- Ords1],
                         barrel_ngram_postings_positional:decode(PosBlock)),
            ?assertEqual(not_found, ?M:lookup_positional_block(H, G2))
        after
            ?M:close(H)
        end
    end.

%% A pre-v4 segment (version field forced back to 3) must be rejected with
%% a distinguishable error, not collapsed into a generic invalid_header.
unsupported_version_error(Dir) ->
    fun() ->
        Path = filename:join(Dir, "oldver.ngseg"),
        ok = ?M:write(Path, #{doc_count => 0, watermark => wm(),
                              postings => [], entries => []}),
        {ok, Bin} = file:read_file(Path),
        <<Magic:8/binary, _OldVersion:32/little, Rest/binary>> = Bin,
        Corrupted = <<Magic/binary, 3:32/little, Rest/binary>>,
        ok = file:write_file(Path, Corrupted),
        ?assertEqual({error, {unsupported_segment_version, 3, 4}}, ?M:open(Path))
    end.

%% all_positional_postings/1 (the merger's read side): every gram that
%% carries phase-2 data, decoded, and nothing for a gram that doesn't.
all_positional_postings_directory(Dir) ->
    fun() ->
        Path = filename:join(Dir, "allpos.ngseg"),
        G1 = gram($a, $b, $c),
        G2 = gram($d, $e, $f),
        G3 = gram($x, $y, $z),
        Spec = #{
            doc_count => 3,
            watermark => wm(),
            postings => [{G3, [2]}, {G1, [0, 1]}, {G2, [1]}],
            positional_postings => [{G1, [{0, [3]}, {1, [7]}]}, {G3, [{2, [0, 5]}]}],
            entries => [entry(<<"k0">>, 1, false),
                        entry(<<"k1">>, 2, false),
                        entry(<<"k2">>, 3, false)]
        },
        ok = ?M:write(Path, Spec),
        {ok, H} = ?M:open(Path),
        try
            All = lists:sort(?M:all_positional_postings(H)),
            ?assertEqual(lists:sort([{G1, [{0, [3]}, {1, [7]}]}, {G3, [{2, [0, 5]}]}]), All)
        after
            ?M:close(H)
        end
    end.
