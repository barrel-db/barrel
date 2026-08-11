%%%-------------------------------------------------------------------
%%% @doc Manual benchmark for the positional (phase-2) posting codec.
%%%
%%% Three questions, each with a direct bearing on later steps of the
%%% positional-index work:
%%%
%%% <ul>
%%%   <li>Codec cost: how much slower is encode/decode than the plain
%%%       (non-positional) posting codec, and how much bigger is the
%%%       block, for a given offsets-per-document count?</li>
%%%   <li>Merge-join: is the cursor-based lockstep walk (never
%%%       materializing either block fully -- see
%%%       {@link barrel_ngram_postings_positional}) actually faster than
%%%       the naive "decode both fully, intersect via a map" approach?
%%%       Step 4 (distance-check intersection) builds directly on the
%%%       cursor primitive; this is the "only if measured" check for
%%%       whether that design choice pays for itself.</li>
%%%   <li>Segment overhead: the v4 composite posting block
%%%       (`barrel_ngram_segment') always carries a `Phase2Len:32' prefix
%%%       per gram, even when a corpus has no phase-2 data at all (true of
%%%       every corpus today -- phase-2 indexing isn't wired in until a
%%%       later step). How much does that cost, at rest and to write,
%%%       before phase-2 is providing any benefit?</li>
%%% </ul>
%%%
%%% Not run by the test suite (no test functions); invoke run/0 directly.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_bench_positional).

-export([run/0, run/1]).

-define(PP, barrel_ngram_postings_positional).

run() ->
    run(#{sizes => [50000, 10000, 1000, 100, 10], offsets_per_doc => 4,
          reps => 30, segment_doc_count => 20000}).

run(#{sizes := Sizes, offsets_per_doc := K, reps := Reps,
      segment_doc_count := SegDocCount}) ->
    bench_codec(Sizes, K, Reps),
    bench_merge_join(Sizes, K, Reps),
    bench_segment_overhead(SegDocCount),
    ok.

%%====================================================================
%% Codec cost: positional vs plain, at varying document counts per gram
%%====================================================================

bench_codec(Sizes, K, Reps) ->
    io:format("~n== positional postings codec (encode/decode) ==~n"
              "offsets/doc ~p, ~p reps~n~n", [K, Reps]),
    io:format("~-12s ~-14s ~-14s ~-12s ~-12s~n",
              ["doc count", "encode us", "decode us", "pos bytes", "plain bytes"]),
    lists:foreach(fun(Sz) -> bench_codec_size(Sz, K, Reps) end, Sizes),
    ok.

bench_codec_size(Sz, K, Reps) ->
    _ = rand:seed(exsss, {Sz + 1, K, 13}),
    Entries = [{Ord, gen_offsets(K)} || Ord <- lists:seq(0, Sz - 1)],
    Ords = [O || {O, _} <- Entries],
    Tenc = time(Reps, fun() -> ?PP:encode(Entries) end),
    Block = ?PP:encode(Entries),
    Tdec = time(Reps, fun() -> ?PP:decode(Block) end),
    PlainBlock = barrel_ngram_postings:encode(Ords),
    io:format("~-12B ~-14.1f ~-14.1f ~-12B ~-12B~n",
              [Sz, Tenc, Tdec, byte_size(Block), byte_size(PlainBlock)]).

gen_offsets(K) ->
    lists:usort([rand:uniform(100000) - 1 || _ <- lists:seq(1, K)]).

%%====================================================================
%% Merge-join: cursor lockstep walk vs naive decode-both-then-intersect
%%====================================================================

bench_merge_join(Sizes, K, Reps) ->
    io:format("~n== merge-join: cursor lockstep vs naive decode+intersect ==~n"
              "offsets/doc ~p, ~p reps~n~n", [K, Reps]),
    io:format("~-12s ~-16s ~-16s ~-10s~n",
              ["list size", "naive us", "cursor us", "common"]),
    lists:foreach(fun(Sz) -> bench_merge_size(Sz, K, Reps) end, Sizes),
    ok.

bench_merge_size(Sz, K, Reps) ->
    _ = rand:seed(exsss, {Sz + 3, K, 21}),
    N = Sz * 2,   %% ordinal space sized so the two sides overlap partially
    EntriesA = gen_entries(N, Sz, K),
    EntriesB = gen_entries(N, Sz, K),
    BinA = ?PP:encode(EntriesA),
    BinB = ?PP:encode(EntriesB),
    Tnaive = time(Reps, fun() -> naive_common(EntriesA, EntriesB) end),
    Tcursor = time(Reps, fun() -> cursor_common(BinA, BinB) end),
    Common = cursor_common(BinA, BinB),
    io:format("~-12B ~-16.1f ~-16.1f ~-10B~n", [Sz, Tnaive, Tcursor, length(Common)]).

gen_entries(N, Sz, K) ->
    Ords = take(Sz, lists:usort([rand:uniform(N) - 1 || _ <- lists:seq(1, Sz * 2)])),
    [{O, gen_offsets(K)} || O <- Ords].

take(Sz, L) when length(L) >= Sz -> lists:sublist(L, Sz);
take(_Sz, L) -> L.

naive_common(EntriesA, EntriesB) ->
    MapB = maps:from_list(EntriesB),
    [{O, OffsA, maps:get(O, MapB)} || {O, OffsA} <- EntriesA, maps:is_key(O, MapB)].

cursor_common(BinA, BinB) ->
    cursor_walk(?PP:next(?PP:cursor(BinA)), ?PP:next(?PP:cursor(BinB)), []).

cursor_walk(done, _NB, Acc) ->
    lists:reverse(Acc);
cursor_walk(_NA, done, Acc) ->
    lists:reverse(Acc);
cursor_walk({OA, OffsA, CA}, {OB, OffsB, CB}, Acc) when OA =:= OB ->
    cursor_walk(?PP:next(CA), ?PP:next(CB), [{OA, OffsA, OffsB} | Acc]);
cursor_walk({OA, _OffsA, CA}, {OB, _OffsB, _CB} = NB, Acc) when OA < OB ->
    cursor_walk(?PP:next(CA), NB, Acc);
cursor_walk({OA, _OffsA, _CA} = NA, {OB, _OffsB, CB}, Acc) when OB < OA ->
    cursor_walk(NA, ?PP:next(CB), Acc).

%%====================================================================
%% Segment overhead: the composite block's Phase2Len prefix, paid on
%% every gram whether or not phase-2 data is actually present.
%%====================================================================

bench_segment_overhead(DocCount) ->
    io:format("~n== segment v4 composite-block overhead (doc_count ~p) ==~n~n",
              [DocCount]),
    io:format("~-20s ~-12s ~-14s~n", ["phase-2 density", "write ms", "file bytes"]),
    lists:foreach(fun(Density) -> bench_segment_density(DocCount, Density) end,
                  [0.0, 0.05, 0.25]),
    ok.

bench_segment_density(DocCount, Density) ->
    _ = rand:seed(exsss, {DocCount, round(Density * 1000), 7}),
    Dir = filename:join("/tmp", "barrel_ngram_bench_" ++
                         integer_to_list(erlang:unique_integer([positive]))),
    ok = filelib:ensure_dir(filename:join(Dir, "dummy")),
    Path = filename:join(Dir, "bench.ngseg"),
    Grams = 2000,
    Postings = [{G, gen_ordinals(DocCount, 0.3)} || G <- lists:seq(1, Grams)],
    PositionalPostings = if
        Density =:= +0.0 -> [];
        true ->
            NPos = max(1, round(Grams * Density)),
            [{G, [{O, [O rem 97, (O + 11) rem 97]} || O <- gen_ordinals(DocCount, 0.3)]}
             || G <- lists:seq(1, NPos)]
    end,
    Entries = [#{key => doc_key(N), hlc => <<N:96>>, deleted => false}
               || N <- lists:seq(0, DocCount - 1)],
    Spec = #{doc_count => DocCount, watermark => <<0:96>>,
             postings => Postings, positional_postings => PositionalPostings,
             entries => Entries},
    {T, ok} = timer:tc(fun() -> barrel_ngram_segment:write(Path, Spec) end),
    Bytes = filelib:file_size(Path),
    io:format("~-20.2f ~-12.2f ~-14B~n", [Density, T / 1000, Bytes]),
    _ = file:delete(Path),
    _ = file:del_dir(Dir),
    ok.

gen_ordinals(DocCount, Fraction) ->
    Sz = max(1, round(DocCount * Fraction)),
    take(Sz, lists:usort([rand:uniform(DocCount) - 1 || _ <- lists:seq(1, Sz * 2)])).

doc_key(N) ->
    iolist_to_binary([<<"doc">>, integer_to_binary(N)]).

%%====================================================================
%% Helpers
%%====================================================================

time(Reps, Fun) ->
    _ = Fun(),   %% warm up
    {T, _} = timer:tc(fun() -> repeat(Reps, Fun) end),
    T / Reps.

repeat(0, _Fun) -> ok;
repeat(N, Fun) -> _ = Fun(), repeat(N - 1, Fun).
