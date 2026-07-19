%%%-------------------------------------------------------------------
%%% @doc Manual benchmark for posting-list intersection.
%%%
%%% Measures the current query-side cost (decode delta+varint blocks then
%%% galloping intersect) across posting-list sizes, to decide whether a
%%% roaring intersection primitive is warranted ("only if measured").
%%% Not run by the test suite (no test functions); invoke run/0 directly.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_bench).

-export([run/0, run/1]).

run() ->
    run(#{ordinals => 100000, grams => 12, reps => 50,
          sizes => [50000, 10000, 1000, 100, 10]}).

run(#{ordinals := N, grams := K, reps := Reps, sizes := Sizes}) ->
    io:format("~n== posting-list intersection ==~n"
              "ordinal space ~p, intersecting ~p lists, ~p reps~n~n",
              [N, K, Reps]),
    io:format("~-12s ~-14s ~-14s ~-10s~n",
              ["list size", "decode us/op", "intersect us/op", "result"]),
    lists:foreach(fun(Sz) -> bench_size(N, Sz, K, Reps) end, Sizes),
    ok.

bench_size(N, Sz, K, Reps) ->
    _ = rand:seed(exsss, {Sz + 1, 7, 13}),
    Lists = [gen_sorted(N, Sz) || _ <- lists:seq(1, K)],
    Blocks = [barrel_ngram_postings:encode(L) || L <- Lists],
    %% decode cost (per full set of K blocks)
    Tdec = time(Reps, fun() -> [barrel_ngram_postings:decode(B) || B <- Blocks] end),
    %% intersect cost (over already-decoded lists)
    Tint = time(Reps, fun() -> barrel_ngram_postings:intersect_all(Lists) end),
    Result = barrel_ngram_postings:intersect_all(Lists),
    io:format("~-12B ~-14.1f ~-14.1f ~-10B~n",
              [Sz, Tdec, Tint, length(Result)]).

time(Reps, Fun) ->
    _ = Fun(),   %% warm up
    {T, _} = timer:tc(fun() -> repeat(Reps, Fun) end),
    T / Reps.

repeat(0, _Fun) -> ok;
repeat(N, Fun) -> _ = Fun(), repeat(N - 1, Fun).

%% Sz distinct ordinals from 0..N-1, ascending.
gen_sorted(N, Sz) ->
    take(Sz, lists:usort([rand:uniform(N) - 1
                          || _ <- lists:seq(1, Sz * 2)])).

take(Sz, L) when length(L) >= Sz -> lists:sublist(L, Sz);
take(_Sz, L) -> L.
