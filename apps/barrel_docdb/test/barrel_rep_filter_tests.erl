-module(barrel_rep_filter_tests).

-include_lib("eunit/include/eunit.hrl").

roundtrip(Filter) ->
    Wire = barrel_rep_filter:to_wire(Filter),
    %% the wire form must survive a JSON round trip
    Decoded = json:decode(iolist_to_binary(json:encode(Wire))),
    {ok, Filter2} = barrel_rep_filter:from_wire(Decoded),
    Filter2.

%%--------------------------------------------------------------------
%% Round trips
%%--------------------------------------------------------------------

empty_test() ->
    ?assertEqual(#{}, roundtrip(#{})).

paths_and_channel_test() ->
    F = #{paths => [<<"type/post">>, <<"org/acme/#">>],
          channel => <<"acme">>},
    ?assertEqual(F, roundtrip(F)).

all_condition_forms_test() ->
    Where = [
        {path, [<<"type">>], <<"user">>},
        {compare, [<<"rank">>], '>', 3},
        {compare, [<<"rank">>], '=<', 9},
        {compare, [<<"flag">>], '=/=', null},
        {'and', [{path, [<<"a">>], 1}, {path, [<<"b">>], true}]},
        {'or', [{exists, [<<"x">>]}, {missing, [<<"y">>]}]},
        {'not', {in, [<<"s">>], [<<"a">>, <<"b">>]}},
        {contains, [<<"tags">>], <<"erlang">>},
        {regex, [<<"name">>], <<"^A.*">>},
        {prefix, [<<"email">>], <<"admin@">>},
        {path, [<<"nested">>, 0, <<"key">>], 2.5}
    ],
    F = #{query => #{where => Where}},
    ?assertEqual(F, roundtrip(F)).

wildcard_component_test() ->
    F = #{query => #{where => [{exists, [<<"tags">>, '*']}]}},
    ?assertEqual(F, roundtrip(F)).

%%--------------------------------------------------------------------
%% Rejections (no atoms from the wire)
%%--------------------------------------------------------------------

reject_test() ->
    Bad = [
        #{<<"paths">> => <<"not-a-list">>},
        #{<<"paths">> => [42]},
        #{<<"channel">> => 42},
        #{<<"query">> => #{<<"where">> => [[<<"nope">>, [<<"a">>], 1]]}},
        #{<<"query">> => #{<<"where">> =>
                               [[<<"compare">>, [<<"a">>], <<"~=">>, 1]]}},
        #{<<"query">> => #{<<"where">> => [[<<"path">>, <<"a">>, 1]]}},
        #{<<"query">> => #{<<"where">> =>
                               [[<<"path">>, [<<"a">>], #{}]]}},
        #{<<"query">> => #{<<"where">> => [[<<"path">>, [-1], 1]]}},
        #{<<"query">> => <<"junk">>}
    ],
    lists:foreach(
        fun(W) ->
            ?assertMatch({error, {bad_filter, _}},
                         barrel_rep_filter:from_wire(W))
        end,
        Bad),
    ?assertMatch({error, {bad_filter, _}},
                 barrel_rep_filter:from_wire(not_a_map)).

%%--------------------------------------------------------------------
%% Rep-id stability
%%--------------------------------------------------------------------

legacy_local_id_unchanged_test() ->
    %% local transports contribute the endpoint term itself, and the
    %% unfiltered hash is the legacy 2-tuple: existing checkpoints keep
    %% their ids
    Legacy = binary:encode_hex(
        crypto:hash(md5, term_to_binary({<<"a">>, <<"b">>})), lowercase),
    ?assertEqual(Legacy,
                 barrel_rep:rep_id(<<"a">>, barrel_rep_transport_local,
                                   <<"b">>, barrel_rep_transport_local,
                                   #{})).

filter_changes_id_test() ->
    Base = barrel_rep:rep_id(<<"a">>, barrel_rep_transport_local,
                             <<"b">>, barrel_rep_transport_local, #{}),
    Filtered = barrel_rep:rep_id(<<"a">>, barrel_rep_transport_local,
                                 <<"b">>, barrel_rep_transport_local,
                                 #{channel => <<"c">>}),
    ?assertNotEqual(Base, Filtered).

credentials_do_not_change_id_test() ->
    Url = <<"http://host:1234/db/x">>,
    Id1 = barrel_rep:rep_id(<<"a">>, barrel_rep_transport_local,
                            #{url => Url, auth => #{token => <<"t1">>}},
                            barrel_rep_stub_transport, #{}),
    Id2 = barrel_rep:rep_id(<<"a">>, barrel_rep_transport_local,
                            #{url => Url, auth => #{token => <<"t2">>}},
                            barrel_rep_stub_transport, #{}),
    ?assertEqual(Id1, Id2),
    Id3 = barrel_rep:rep_id(<<"a">>, barrel_rep_transport_local,
                            #{url => <<"http://other:1/db/x">>},
                            barrel_rep_stub_transport, #{}),
    ?assertNotEqual(Id1, Id3).

%%--------------------------------------------------------------------
%% Checkpoint seq codec
%%--------------------------------------------------------------------

seq_codec_test() ->
    ?assertEqual(<<"first">>, barrel_rep_checkpoint:encode_seq(first)),
    ?assertEqual(first, barrel_rep_checkpoint:decode_seq(<<"first">>)),
    Hlc = barrel_hlc:from_wall_time(1234567),
    Enc = barrel_rep_checkpoint:encode_seq(Hlc),
    ?assert(is_binary(Enc)),
    ?assertEqual(Hlc, barrel_rep_checkpoint:decode_seq(Enc)).
