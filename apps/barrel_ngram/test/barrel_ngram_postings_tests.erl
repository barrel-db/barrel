%%%-------------------------------------------------------------------
%%% @doc EUnit tests for the posting-list codec and intersection.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_postings_tests).

-include_lib("eunit/include/eunit.hrl").

-define(M, barrel_ngram_postings).

%%====================================================================
%% Codec
%%====================================================================

roundtrip_empty_test() ->
    ?assertEqual([], ?M:decode(?M:encode([]))).

roundtrip_single_test() ->
    ?assertEqual([0], ?M:decode(?M:encode([0]))),
    ?assertEqual([42], ?M:decode(?M:encode([42]))).

roundtrip_ascending_test() ->
    Ords = [0, 1, 5, 200, 201, 100000, 16777215],
    ?assertEqual(Ords, ?M:decode(?M:encode(Ords))).

encode_sorts_and_dedups_test() ->
    ?assertEqual([1, 2, 3], ?M:decode(?M:encode([3, 1, 2, 1, 3]))).

roundtrip_random_test() ->
    lists:foreach(
        fun(Seed) ->
            rand:seed(exsss, {Seed, Seed * 7 + 1, Seed * 13 + 3}),
            Ords = lists:usort([rand:uniform(1 bsl 24) - 1
                                || _ <- lists:seq(1, 500)]),
            ?assertEqual(Ords, ?M:decode(?M:encode(Ords)))
        end, lists:seq(1, 50)).

%%====================================================================
%% Intersection (against a naive reference)
%%====================================================================

intersect_empty_inputs_test() ->
    ?assertEqual([], ?M:intersect_all([])),
    ?assertEqual([], ?M:intersect_all([[]])),
    ?assertEqual([], ?M:intersect_all([[1, 2, 3], []])).

intersect_single_test() ->
    ?assertEqual([1, 2, 3], ?M:intersect_all([[1, 2, 3]])).

intersect_basic_test() ->
    ?assertEqual([2, 4], ?M:intersect_all([[1, 2, 3, 4], [2, 4, 6], [0, 2, 4, 8]])).

intersect_disjoint_test() ->
    ?assertEqual([], ?M:intersect_all([[1, 3, 5], [2, 4, 6]])).

intersect_identical_test() ->
    L = [1, 2, 3, 4, 5],
    ?assertEqual(L, ?M:intersect_all([L, L, L])).

intersect_random_property_test() ->
    lists:foreach(
        fun(Seed) ->
            rand:seed(exsss, {Seed, Seed * 3 + 5, Seed * 11 + 7}),
            NumLists = 2 + rand:uniform(4),
            Lists = [random_sorted_set() || _ <- lists:seq(1, NumLists)],
            Expected = naive_intersect(Lists),
            ?assertEqual(Expected, ?M:intersect_all(Lists))
        end, lists:seq(1, 200)).

intersect_skewed_test() ->
    %% One rare list against a dense one exercises the galloping path.
    Small = [7, 5000, 99999],
    Large = lists:seq(0, 100000),
    ?assertEqual([7, 5000, 99999], ?M:intersect_all([Large, Small])).

%%====================================================================
%% Helpers
%%====================================================================

random_sorted_set() ->
    N = rand:uniform(40),
    lists:usort([rand:uniform(60) - 1 || _ <- lists:seq(1, N)]).

naive_intersect([]) ->
    [];
naive_intersect([First | Rest]) ->
    S = lists:foldl(fun(L, Acc) -> ordsets:intersection(Acc, ordsets:from_list(L)) end,
                    ordsets:from_list(First), Rest),
    ordsets:to_list(S).
