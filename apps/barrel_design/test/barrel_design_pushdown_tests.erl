-module(barrel_design_pushdown_tests).

-include_lib("eunit/include/eunit.hrl").

-define(ARRAY, {array, [#{type => text, category => true}, #{type => text}]}).
-define(SCALAR, {scalar, #{type => text}}).

b(Where) -> barrel_design_pushdown:bounds(?ARRAY, Where, map).

enc(K) -> barrel_design_key:encode(K).
pre(Cs) -> barrel_design_key:prefix(Cs).
up(P) -> barrel_design_key:after_prefix(P).

in({L, U}, K) -> enc(K) >= L andalso enc(K) < U.

prefix_test() ->
    B = b([{path, [<<"key">>, 0], <<"Ada">>}]),
    ?assertEqual({pre([<<"Ada">>]), up(pre([<<"Ada">>]))}, B),
    ?assert(in(B, [<<"Ada">>, <<"2026-12-01">>])),
    ?assertNot(in(B, [<<"Adam">>, <<"2026-12-01">>])).

prefix_and_range_test() ->
    B = b([{path, [<<"key">>, 0], <<"Ada">>},
           {compare, [<<"key">>, 1], '>', <<"2026-12-01">>},
           {compare, [<<"key">>, 1], '=<', <<"2026-12-15">>}]),
    ?assertNot(in(B, [<<"Ada">>, <<"2026-12-01">>])),
    ?assert(in(B, [<<"Ada">>, <<"2026-12-02">>])),
    ?assert(in(B, [<<"Ada">>, <<"2026-12-15">>])),
    ?assertNot(in(B, [<<"Ada">>, <<"2026-12-16">>])),
    ?assertNot(in(B, [<<"Bea">>, <<"2026-12-05">>])).

like_test() ->
    B = b([{prefix, [<<"key">>, 0], <<"Ad">>}]),
    ?assert(in(B, [<<"Ad">>, <<"x">>])),
    ?assert(in(B, [<<"Adam">>, <<"x">>])),
    ?assertNot(in(B, [<<"Ac">>, <<"x">>])),
    ?assertNot(in(B, [<<"Ae">>, <<"x">>])).

%% A gap in the leading equalities stops the prefix; later components
%% are left to the residual filter.
gap_test() ->
    ?assertEqual(none, b([{path, [<<"key">>, 1], <<"2026-12-01">>}])),
    ?assertEqual(none, b([{path, [<<"key">>], <<"Ada">>}])),
    ?assertEqual(none, b([{compare, [<<"value">>], '>', 3}])).

%% Grouped reads only use components below the group level.
grouped_test() ->
    W = [{path, [<<"key">>, 0], <<"Ada">>}, {path, [<<"key">>, 1], <<"2026-12-01">>}],
    ?assertEqual({pre([<<"Ada">>]), up(pre([<<"Ada">>]))},
                 barrel_design_pushdown:bounds(?ARRAY, W, {reduce, 1})),
    ?assertEqual(none, barrel_design_pushdown:bounds(?ARRAY, W, {reduce, 0})),
    P2 = pre([<<"Ada">>, <<"2026-12-01">>]),
    ?assertEqual({P2, up(P2)}, barrel_design_pushdown:bounds(?ARRAY, W, {reduce, exact})).

scalar_test() ->
    E = enc(<<"Ada">>),
    ?assertEqual({E, up(E)}, barrel_design_pushdown:bounds(?SCALAR, [{path, [<<"key">>], <<"Ada">>}], map)),
    R = barrel_design_pushdown:bounds(?SCALAR, [{compare, [<<"key">>], '>=', <<"B">>}], map),
    ?assert(in(R, <<"Bea">>)),
    ?assertNot(in(R, <<"Ada">>)),
    ?assertEqual(none, barrel_design_pushdown:bounds(?SCALAR, [{path, [<<"key">>], <<"Ada">>}], {reduce, 0})).

%% Values that are not keys (floats) never narrow.
invalid_values_test() ->
    ?assertEqual(none, b([{path, [<<"key">>, 0], 1.5}])),
    ?assertEqual({pre([<<"Ada">>]), up(pre([<<"Ada">>]))},
                 b([{path, [<<"key">>, 0], <<"Ada">>}, {compare, [<<"key">>, 1], '>', 1.5}])).
