-module(barrel_design_key_tests).

-include_lib("eunit/include/eunit.hrl").

%% Golden vectors: the encoding is part of the on-disk format.
golden_test_() ->
    Vectors = [
        {null, <<16#01>>},
        {false, <<16#02>>},
        {true, <<16#03>>},
        {0, <<16#20, 16#80, 0, 0, 0, 0, 0, 0, 0>>},
        {-1, <<16#20, 16#7F, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF>>},
        {-(1 bsl 63), <<16#20, 0, 0, 0, 0, 0, 0, 0, 0>>},
        {(1 bsl 63) - 1, <<16#20, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF>>},
        {<<>>, <<16#40, 0, 0>>},
        {<<"Ada">>, <<16#40, "Ada", 0, 0>>},
        {<<"a", 0, "b">>, <<16#40, "a", 0, 16#FF, "b", 0, 0>>},
        {[], <<16#60, 0>>},
        {[<<"Ada">>, <<"2026-12-01">>],
         <<16#60, 16#40, "Ada", 0, 0, 16#40, "2026-12-01", 0, 0, 0>>}
    ],
    [?_assertEqual(Bin, barrel_design_key:encode(Key)) || {Key, Bin} <- Vectors]
    ++ [?_assertEqual(Key, barrel_design_key:decode(Bin)) || {Key, Bin} <- Vectors].

%% Byte order of encodings equals the documented collation.
order_test() ->
    Sorted = [null, false, true,
              -(1 bsl 63), -1000, -1, 0, 1, 24, 1000, (1 bsl 63) - 1,
              <<>>, <<"a">>, <<"a", 0>>, <<"a", 0, 0>>, <<"a", 1>>, <<"ab">>,
              <<"b">>, <<"é"/utf8>>,
              [], [null], [<<"Ada">>], [<<"Ada">>, <<"2026-12-01">>],
              [<<"Ada">>, <<"2026-12-15">>], [<<"Adam">>], [<<"Bea">>], [[]]],
    Encoded = [barrel_design_key:encode(K) || K <- Sorted],
    ?assertEqual(Encoded, lists:sort(Encoded)),
    ?assertEqual(length(Encoded), length(lists:usort(Encoded))).

%% A whole-component prefix selects exactly the keys starting with it.
prefix_test() ->
    P = barrel_design_key:prefix([<<"Ada">>]),
    Upper = barrel_design_key:after_prefix(P),
    In = [[<<"Ada">>], [<<"Ada">>, <<"x">>], [<<"Ada">>, 1, true]],
    Out = [[<<"Adam">>], [<<"Ad">>], [<<"Bea">>], <<"Ada">>, [], [<<"Ac">>, <<"z">>]],
    [?assert(in_range(barrel_design_key:encode(K), P, Upper)) || K <- In],
    [?assertNot(in_range(barrel_design_key:encode(K), P, Upper)) || K <- Out].

in_range(Bin, Lower, Upper) -> Bin >= Lower andalso Bin < Upper.

valid_test() ->
    ?assert(barrel_design_key:valid([<<"a">>, 1, null, true, [[]]])),
    ?assertNot(barrel_design_key:valid(1.5)),
    ?assertNot(barrel_design_key:valid(1 bsl 63)),
    ?assertNot(barrel_design_key:valid(<<255>>)),
    ?assertNot(barrel_design_key:valid(#{})),
    ?assertNot(barrel_design_key:valid([[[[[1]]]]])).

roundtrip_test() ->
    Keys = [[I, <<"t", (integer_to_binary(I))/binary>>, I rem 2 =:= 0]
            || I <- lists:seq(-50, 50)],
    [?assertEqual(K, barrel_design_key:decode(barrel_design_key:encode(K))) || K <- Keys].

cbor_test() ->
    D = fun barrel_design_cbor:decode/1,
    ?assertEqual({ok, 1800}, D(<<16#19, 16#07, 16#08>>)),
    ?assertEqual({ok, -201}, D(<<16#38, 16#C8>>)),
    ?assertEqual({ok, [<<"Ada">>, <<"2026-12-01">>]},
                 D(<<16#82, 16#63, "Ada", 16#6A, "2026-12-01">>)),
    ?assertEqual({ok, null}, D(<<16#F6>>)),
    %% byte strings, maps, floats, tags, trailing bytes, bad UTF-8
    [?assertEqual({error, invalid_cbor}, D(B))
     || B <- [<<16#43, "abc">>, <<16#A0>>, <<16#F9, 0, 0>>, <<16#C1, 0>>,
              <<16#01, 16#01>>, <<16#61, 255>>, <<16#9F, 16#FF>>, <<>>,
              <<16#1B, 16#80, 0, 0, 0, 0, 0, 0, 0>>]].
