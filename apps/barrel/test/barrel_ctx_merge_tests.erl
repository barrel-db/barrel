-module(barrel_ctx_merge_tests).

-include_lib("eunit/include/eunit.hrl").

-define(FP, <<"sha256:aa">>).

m(Ctx, Scored) ->
    #{ctx => Ctx, fingerprint => ?FP, distance => cosine,
      rows => [#{<<"id">> => Id, <<"_score">> => S} || {Id, S} <- Scored]}.

ids({ok, Rows, _Meta}) -> [maps:get(<<"id">>, R) || R <- Rows].

members() ->
    [m(<<"a">>, [{<<"a1">>, 0.9}, {<<"a2">>, 0.5}, {<<"a3">>, 0.1}]),
     m(<<"b">>, [{<<"b1">>, 0.7}, {<<"b2">>, 0.6}]),
     m(<<"c">>, [])].

grouped_test() ->
    {ok, Groups, Meta} = barrel_ctx_merge:grouped(members()),
    ?assertEqual(#{merge => grouped, relevance => false}, Meta),
    ?assertEqual([<<"a">>, <<"b">>, <<"c">>], [C || #{ctx := C} <- Groups]),
    [#{rows := [First | _]} | _] = Groups,
    ?assertMatch(#{<<"_ctx">> := <<"a">>, <<"_rank">> := 1}, First).

interleave_test() ->
    ?assertEqual([<<"a1">>, <<"b1">>, <<"a2">>, <<"b2">>, <<"a3">>],
                 ids(barrel_ctx_merge:interleave(members(), 10))),
    ?assertEqual([<<"a1">>, <<"b1">>, <<"a2">>],
                 ids(barrel_ctx_merge:interleave(members(), 3))),
    {ok, _, Meta} = barrel_ctx_merge:interleave(members(), 3),
    ?assertEqual(#{merge => interleave, relevance => false}, Meta).

score_test() ->
    ?assertEqual([<<"a1">>, <<"b1">>, <<"b2">>, <<"a2">>],
                 ids(barrel_ctx_merge:score(members(), 4, #{}))),
    {ok, _, Meta} = barrel_ctx_merge:score(members(), 4, #{}),
    ?assertEqual(#{merge => score, relevance => true}, Meta).

score_ties_are_deterministic_test() ->
    Ms = [m(<<"b">>, [{<<"x">>, 0.5}]), m(<<"a">>, [{<<"y">>, 0.5}])],
    ?assertEqual([<<"y">>, <<"x">>], ids(barrel_ctx_merge:score(Ms, 2, #{}))).

score_equals_union_top_k_test() ->
    %% exact per-member top k merged by score is the union's top k
    All = [{<<"d", (integer_to_binary(I))/binary>>, rand:uniform()}
           || I <- lists:seq(1, 60)],
    Union = [Id || {Id, _} <- lists:sublist(
                                lists:sort(fun({_, A}, {_, B}) -> A >= B end,
                                           All), 10)],
    Split = fun(N) -> [P || {I, P} <- lists:zip(lists:seq(1, 60), All),
                            I rem 3 =:= N] end,
    Top = fun(L) -> lists:sublist(
                      lists:sort(fun({_, A}, {_, B}) -> A >= B end, L), 10) end,
    Ms = [m(<<"c", (integer_to_binary(N))/binary>>, Top(Split(N)))
          || N <- [0, 1, 2]],
    ?assertEqual(Union, ids(barrel_ctx_merge:score(Ms, 10, #{}))).

score_refused_test_() ->
    [?_assertEqual({error, {not_score_comparable, <<"a">>}},
                   barrel_ctx_merge:score(
                     [maps:remove(fingerprint, M) || M <- members()], 3, #{})),
     ?_assertEqual({error, {fingerprint_mismatch, <<"b">>}},
                   barrel_ctx_merge:score(
                     [m(<<"a">>, []), (m(<<"b">>, []))#{fingerprint => <<"x">>}],
                     3, #{})),
     ?_assertEqual({error, {fingerprint_mismatch, <<"b">>}},
                   barrel_ctx_merge:score(
                     [m(<<"a">>, []), (m(<<"b">>, []))#{distance => euclidean}],
                     3, #{})),
     ?_assertEqual({error, {not_score_comparable, <<"a">>}},
                   barrel_ctx_merge:score(
                     [(m(<<"a">>, []))#{fingerprint => undefined}], 3, #{}))].

rrf_is_rank_based_test() ->
    %% every member's first hit gets the same weight, whatever its score
    {ok, Rows, Meta} = barrel_ctx_merge:rrf(members(), 2, #{}),
    ?assertEqual(#{merge => rrf, relevance => false}, Meta),
    ?assertEqual([<<"a1">>, <<"b1">>], [maps:get(<<"id">>, R) || R <- Rows]),
    [#{<<"_rrf">> := S1}, #{<<"_rrf">> := S2}] = Rows,
    ?assertEqual(S1, S2).

rerank_test() ->
    Pool = barrel_ctx_merge:rerank_pool(members(), 4),
    ?assertEqual([<<"a1">>, <<"b1">>, <<"a2">>, <<"b2">>],
                 [maps:get(<<"id">>, R) || R <- Pool]),
    Scores = [{0, 0.1}, {1, 0.2}, {2, 0.9}, {3, 0.3}],
    ?assertEqual([<<"a2">>, <<"b2">>],
                 ids(barrel_ctx_merge:rerank(Pool, Scores, 2))),
    %% a top_k subset of scores is accepted
    ?assertEqual([<<"b1">>],
                 ids(barrel_ctx_merge:rerank(Pool, [{1, 0.5}], 5))),
    ?assertEqual({error, {rerank_index_out_of_pool, 7}},
                 barrel_ctx_merge:rerank(Pool, [{7, 0.5}], 2)).
