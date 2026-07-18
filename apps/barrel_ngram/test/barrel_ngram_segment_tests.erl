%%%-------------------------------------------------------------------
%%% @doc EUnit tests for the immutable segment format.
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

%% A 12-byte watermark without depending on hlc internals here.
watermark() -> <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12>>.

gram(A, B, C) -> (A bsl 16) bor (B bsl 8) bor C.

round_trip(Dir) ->
    fun() ->
        Path = filename:join(Dir, "seg.ngseg"),
        %% ordinals 0..2 -> keys; two grams with overlapping posting sets.
        G1 = gram($a, $b, $c),
        G2 = gram($x, $y, $z),
        Spec = #{
            doc_count => 3,
            watermark => watermark(),
            postings => [{G2, [1, 2]}, {G1, [0, 2]}],
            keys => [<<"doc-zero">>, <<"doc-one">>, <<"doc-two">>]
        },
        ok = ?M:write(Path, Spec),
        {ok, H} = ?M:open(Path),
        try
            ?assertEqual(3, ?M:doc_count(H)),
            ?assertEqual(watermark(), ?M:watermark(H)),
            ?assertEqual({ok, [0, 2]}, ?M:lookup_postings(H, G1)),
            ?assertEqual({ok, [1, 2]}, ?M:lookup_postings(H, G2)),
            %% ordinal -> key resolution, batched and order-preserving.
            ?assertEqual([{0, <<"doc-zero">>}, {2, <<"doc-two">>}],
                         ?M:keys(H, [0, 2])),
            ?assertEqual([{1, <<"doc-one">>}], ?M:keys(H, [1])),
            %% out-of-range ordinals dropped.
            ?assertEqual([{2, <<"doc-two">>}], ?M:keys(H, [2, 99]))
        after
            ?M:close(H)
        end
    end.

empty_segment(Dir) ->
    fun() ->
        Path = filename:join(Dir, "empty.ngseg"),
        ok = ?M:write(Path, #{doc_count => 0, watermark => watermark(),
                              postings => [], keys => []}),
        {ok, H} = ?M:open(Path),
        try
            ?assertEqual(0, ?M:doc_count(H)),
            ?assertEqual(empty, ?M:lookup_postings(H, gram($a, $b, $c))),
            ?assertEqual([], ?M:keys(H, [0, 1]))
        after
            ?M:close(H)
        end
    end.

absent_gram(Dir) ->
    fun() ->
        Path = filename:join(Dir, "absent.ngseg"),
        G = gram($a, $b, $c),
        ok = ?M:write(Path, #{doc_count => 1, watermark => watermark(),
                              postings => [{G, [0]}],
                              keys => [<<"k">>]}),
        {ok, H} = ?M:open(Path),
        try
            %% A gram below the present one, within the table span, reads 0.
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
        ok = ?M:write(Path, #{doc_count => 1, watermark => watermark(),
                              postings => [{Low, [0]}],
                              keys => [<<"k">>]}),
        {ok, H} = ?M:open(Path),
        try
            %% A gram past the table span (table only covers up to Low)
            %% reads as empty rather than erroring.
            ?assertEqual(empty, ?M:lookup_postings(H, 16#FFFFFF)),
            ?assertEqual({ok, [0]}, ?M:lookup_postings(H, Low))
        after
            ?M:close(H)
        end
    end.
