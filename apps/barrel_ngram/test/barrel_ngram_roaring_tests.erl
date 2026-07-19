%%%-------------------------------------------------------------------
%%% @doc EUnit tests for the roaring bitmap NIF.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_roaring_tests).

-include_lib("eunit/include/eunit.hrl").

-define(M, barrel_ngram_roaring).

%% decode(encode(L)) == sorted unique L
roundtrip_test() ->
    lists:foreach(
        fun(L) -> ?assertEqual(lists:usort(L), ?M:decode(?M:encode(L))) end,
        [[], [0], [42], [0, 1, 2, 3], [5, 3, 1, 3, 5],
         [0, 1000000, 4294967295], lists:seq(0, 5000)]).

intersect_basic_test() ->
    A = ?M:encode([1, 2, 3, 4]),
    B = ?M:encode([2, 4, 6]),
    C = ?M:encode([0, 2, 4, 8]),
    ?assertEqual([2, 4], ?M:decode(?M:intersect_all([A, B, C]))),
    ?assertEqual([], ?M:decode(?M:intersect_all([A, ?M:encode([9, 10])]))).

intersect_empty_inputs_test() ->
    ?assertEqual([], ?M:decode(?M:intersect_all([]))),
    ?assertEqual([1, 2], ?M:decode(?M:intersect_all([?M:encode([1, 2])]))).

union_basic_test() ->
    A = ?M:encode([1, 3, 5]),
    B = ?M:encode([2, 3, 6]),
    ?assertEqual([1, 2, 3, 5, 6], ?M:decode(?M:union_all([A, B]))),
    ?assertEqual([], ?M:decode(?M:union_all([]))).

intersect_property_test() ->
    lists:foreach(
        fun(Seed) ->
            rand:seed(exsss, {Seed, Seed * 3 + 1, Seed * 7 + 2}),
            Lists = [rand_set() || _ <- lists:seq(1, 2 + rand:uniform(4))],
            Expected = ordsets:to_list(
                         lists:foldl(fun(L, Acc) ->
                                         ordsets:intersection(Acc, ordsets:from_list(L))
                                     end, ordsets:from_list(hd(Lists)), tl(Lists))),
            Bins = [?M:encode(L) || L <- Lists],
            ?assertEqual(Expected, ?M:decode(?M:intersect_all(Bins)))
        end, lists:seq(1, 200)).

union_property_test() ->
    lists:foreach(
        fun(Seed) ->
            rand:seed(exsss, {Seed, Seed * 5 + 3, Seed * 11 + 1}),
            Lists = [rand_set() || _ <- lists:seq(1, 2 + rand:uniform(4))],
            Expected = lists:usort(lists:append(Lists)),
            Bins = [?M:encode(L) || L <- Lists],
            ?assertEqual(Expected, ?M:decode(?M:union_all(Bins)))
        end, lists:seq(1, 200)).

large_set_test() ->
    L = lists:seq(0, 200000, 3),
    ?assertEqual(L, ?M:decode(?M:encode(L))).

malformed_binary_test() ->
    ?assertError(badarg, ?M:decode(<<"not a roaring bitmap">>)),
    ?assertError(badarg, ?M:intersect_all([<<0, 1, 2>>])).

rand_set() ->
    N = rand:uniform(60),
    lists:usort([rand:uniform(200) - 1 || _ <- lists:seq(1, N)]).
