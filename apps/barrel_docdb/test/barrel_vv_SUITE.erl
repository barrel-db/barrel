%%%-------------------------------------------------------------------
%%% @doc Test suite for barrel_vv: version-vector semantics (bump,
%%% merge, compare, contains) and the deterministic codec.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vv_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("hlc/include/hlc.hrl").

-export([all/0]).

-export([bump_is_monotonic/1,
         merge_is_pointwise_max/1,
         compare_relations/1,
         contains_covers/1,
         codec_roundtrip/1,
         merge_properties/1]).

all() ->
    [bump_is_monotonic,
     merge_is_pointwise_max,
     compare_relations,
     contains_covers,
     codec_roundtrip,
     merge_properties].

%%====================================================================
%% Test cases
%%====================================================================

bump_is_monotonic(_Config) ->
    VV0 = barrel_vv:new(),
    VV1 = barrel_vv:bump(VV0, {ts(10, 0), <<"a">>}),
    ?assertEqual(#{<<"a">> => ts(10, 0)}, VV1),
    %% newer HLC advances
    VV2 = barrel_vv:bump(VV1, {ts(20, 0), <<"a">>}),
    ?assertEqual(#{<<"a">> => ts(20, 0)}, VV2),
    %% older HLC never goes backward
    VV3 = barrel_vv:bump(VV2, {ts(5, 0), <<"a">>}),
    ?assertEqual(#{<<"a">> => ts(20, 0)}, VV3),
    %% other nodes are independent
    VV4 = barrel_vv:bump(VV3, {ts(1, 0), <<"b">>}),
    ?assertEqual(#{<<"a">> => ts(20, 0), <<"b">> => ts(1, 0)}, VV4).

merge_is_pointwise_max(_Config) ->
    A = #{<<"a">> => ts(10, 0), <<"b">> => ts(5, 0)},
    B = #{<<"a">> => ts(7, 0), <<"b">> => ts(9, 0), <<"c">> => ts(1, 0)},
    Expected = #{<<"a">> => ts(10, 0), <<"b">> => ts(9, 0), <<"c">> => ts(1, 0)},
    ?assertEqual(Expected, barrel_vv:merge(A, B)),
    ?assertEqual(Expected, barrel_vv:merge(B, A)).

compare_relations(_Config) ->
    Empty = barrel_vv:new(),
    A = #{<<"a">> => ts(10, 0)},
    A2 = #{<<"a">> => ts(20, 0)},
    AB = #{<<"a">> => ts(10, 0), <<"b">> => ts(5, 0)},
    B = #{<<"b">> => ts(5, 0)},
    ?assertEqual(eq, barrel_vv:compare(Empty, Empty)),
    ?assertEqual(eq, barrel_vv:compare(A, A)),
    ?assertEqual(dominates, barrel_vv:compare(A, Empty)),
    ?assertEqual(dominated, barrel_vv:compare(Empty, A)),
    ?assertEqual(dominates, barrel_vv:compare(A2, A)),
    ?assertEqual(dominated, barrel_vv:compare(A, A2)),
    ?assertEqual(dominates, barrel_vv:compare(AB, A)),
    ?assertEqual(dominated, barrel_vv:compare(A, AB)),
    %% disjoint or crossing entries are concurrent
    ?assertEqual(concurrent, barrel_vv:compare(A, B)),
    ?assertEqual(concurrent,
                 barrel_vv:compare(#{<<"a">> => ts(10, 0), <<"b">> => ts(1, 0)},
                                   #{<<"a">> => ts(1, 0), <<"b">> => ts(10, 0)})).

contains_covers(_Config) ->
    VV = #{<<"a">> => ts(10, 0)},
    ?assert(barrel_vv:contains(VV, {ts(10, 0), <<"a">>})),
    ?assert(barrel_vv:contains(VV, {ts(9, 99), <<"a">>})),
    ?assertNot(barrel_vv:contains(VV, {ts(10, 1), <<"a">>})),
    ?assertNot(barrel_vv:contains(VV, {ts(1, 0), <<"b">>})),
    ?assertNot(barrel_vv:contains(barrel_vv:new(), {ts(1, 0), <<"a">>})).

codec_roundtrip(_Config) ->
    rand:seed(exsss, {11, 12, 13}),
    ?assertEqual(#{}, barrel_vv:decode(barrel_vv:encode(barrel_vv:new()))),
    lists:foreach(
        fun(_) ->
            VV = random_vv(),
            ?assertEqual(VV, barrel_vv:decode(barrel_vv:encode(VV)))
        end,
        lists:seq(1, 200)),
    %% deterministic: same map, same bytes, regardless of build order
    A = barrel_vv:bump(barrel_vv:bump(barrel_vv:new(), {ts(1, 0), <<"x">>}),
                       {ts(2, 0), <<"y">>}),
    B = barrel_vv:bump(barrel_vv:bump(barrel_vv:new(), {ts(2, 0), <<"y">>}),
                       {ts(1, 0), <<"x">>}),
    ?assertEqual(barrel_vv:encode(A), barrel_vv:encode(B)).

merge_properties(_Config) ->
    rand:seed(exsss, {21, 22, 23}),
    lists:foreach(
        fun(_) ->
            A = random_vv(),
            B = random_vv(),
            M = barrel_vv:merge(A, B),
            %% commutative
            ?assertEqual(M, barrel_vv:merge(B, A)),
            %% the merge covers both inputs
            ?assertNotEqual(dominated, barrel_vv:compare(M, A)),
            ?assertNotEqual(dominated, barrel_vv:compare(M, B)),
            ?assertNotEqual(concurrent, barrel_vv:compare(M, A)),
            %% every version in the inputs is contained
            maps:foreach(
                fun(Node, Hlc) ->
                    ?assert(barrel_vv:contains(M, {Hlc, Node}))
                end,
                maps:merge(A, B)),
            %% idempotent
            ?assertEqual(M, barrel_vv:merge(M, M))
        end,
        lists:seq(1, 200)).

%%====================================================================
%% Helpers
%%====================================================================

ts(Wall, Logical) ->
    #timestamp{wall_time = Wall, logical = Logical}.

random_vv() ->
    Nodes = [<<"a">>, <<"b">>, <<"c">>, <<"d">>],
    lists:foldl(
        fun(Node, Acc) ->
            case rand:uniform(2) of
                1 -> Acc#{Node => ts(rand:uniform(100), rand:uniform(10) - 1)};
                2 -> Acc
            end
        end,
        barrel_vv:new(),
        Nodes).
