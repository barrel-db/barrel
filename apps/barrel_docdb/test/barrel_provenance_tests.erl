-module(barrel_provenance_tests).

-include_lib("eunit/include/eunit.hrl").

validate_ok_test() ->
    Prov = #{actor => <<"a">>, session => <<"s">>, source => <<"mcp">>},
    {ok, Enc} = barrel_provenance:validate(Prov),
    ?assert(is_binary(Enc)),
    ?assertEqual(Prov, barrel_provenance:decode(Enc)),
    %% partial maps are fine
    {ok, Enc2} = barrel_provenance:validate(#{actor => <<"a">>}),
    ?assertEqual(#{actor => <<"a">>}, barrel_provenance:decode(Enc2)).

validate_reject_test() ->
    ?assertEqual({error, bad_provenance},
                 barrel_provenance:validate([{actor, <<"a">>}])),
    ?assertEqual({error, empty_provenance},
                 barrel_provenance:validate(#{})),
    ?assertMatch({error, {unknown_provenance_keys, [who]}},
                 barrel_provenance:validate(#{who => <<"a">>})),
    ?assertMatch({error, {bad_provenance_values, [actor]}},
                 barrel_provenance:validate(#{actor => nope})),
    Big = binary:copy(<<"x">>, 257),
    ?assertMatch({error, {bad_provenance_values, [session]}},
                 barrel_provenance:validate(#{session => Big})).

value_cap_boundary_test() ->
    Max = binary:copy(<<"y">>, 256),
    ?assertMatch({ok, _}, barrel_provenance:validate(#{actor => Max})),
    %% even three max-size values stay under the blob cap (the blob cap
    %% is a guard for future keys, the per-value cap binds today)
    ?assertMatch({ok, _}, barrel_provenance:validate(#{actor => Max,
                                                       session => Max,
                                                       source => Max})).

decode_ignores_unknown_test() ->
    Enc = barrel_docdb_codec_cbor:encode_cbor(
        #{<<"actor">> => <<"a">>, <<"future">> => <<"z">>}),
    ?assertEqual(#{actor => <<"a">>}, barrel_provenance:decode(Enc)).
