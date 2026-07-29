%%%-------------------------------------------------------------------
%%% @doc EUnit tests for rendezvous sharding.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_shards_tests).

-include_lib("eunit/include/eunit.hrl").

-define(M, barrel_ngram_shards).

single_shard_test() ->
    ?assertEqual(0, ?M:shard_for(<<"anything">>, 1)),
    ?assertEqual([<<"c">>], ?M:refs(<<"c">>, 1)).

in_range_test() ->
    lists:foreach(
        fun(N) ->
            lists:foreach(
                fun(K) ->
                    Key = integer_to_binary(K),
                    I = ?M:shard_for(Key, N),
                    ?assert(I >= 0 andalso I < N)
                end, lists:seq(1, 200))
        end, [2, 3, 4, 8]).

deterministic_and_stable_test() ->
    Key = <<"connect_timeout">>,
    ?assertEqual(?M:shard_for(Key, 4), ?M:shard_for(Key, 4)),
    ?assertEqual(?M:shard_for(Key, 8), ?M:shard_for(Key, 8)).

refs_shape_test() ->
    ?assertEqual([{<<"c">>, 0}, {<<"c">>, 1}, {<<"c">>, 2}],
                 ?M:refs(<<"c">>, 3)).

balanced_distribution_test() ->
    N = 4,
    Keys = [integer_to_binary(K) || K <- lists:seq(1, 4000)],
    Counts = lists:foldl(
        fun(Key, Acc) ->
            I = ?M:shard_for(Key, N),
            maps:update_with(I, fun(C) -> C + 1 end, 1, Acc)
        end, #{}, Keys),
    %% every shard gets some keys, and none is wildly over its fair share
    ?assertEqual(N, maps:size(Counts)),
    Fair = length(Keys) div N,
    lists:foreach(
        fun(C) -> ?assert(C > Fair div 2 andalso C < Fair * 2) end,
        maps:values(Counts)).

meta_roundtrip_test() ->
    Corpus = {meta_test, erlang:unique_integer([positive])},
    ?assertEqual(undefined, ?M:get_meta(Corpus)),
    ok = ?M:put_meta(Corpus, #{shards => 4, config => #{db => <<"d">>}}),
    ?assertEqual({ok, #{shards => 4, config => #{db => <<"d">>}}},
                 ?M:get_meta(Corpus)),
    ok = ?M:erase_meta(Corpus),
    ?assertEqual(undefined, ?M:get_meta(Corpus)).
