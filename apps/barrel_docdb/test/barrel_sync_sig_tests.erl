%%%-------------------------------------------------------------------
%%% @doc Unit tests for the sync-wire signing helpers.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_sync_sig_tests).

-include_lib("eunit/include/eunit.hrl").

-define(METHOD, <<"POST">>).
-define(PATH, <<"/db/mydb/_sync/changes">>).

keypair() ->
    crypto:generate_key(eddsa, ed25519).

roundtrip_test() ->
    {Pub, Priv} = keypair(),
    KeyId = <<"node1">>,
    Signers = #{KeyId => Pub},
    CH = barrel_sync_sig:content_sha256(<<"{\"since\":0}">>),
    Ts = erlang:system_time(millisecond),
    Auth = barrel_sync_sig:sign(KeyId, Priv, ?METHOD, ?PATH, CH, Ts),
    {ok, Parsed} = barrel_sync_sig:parse_auth(Auth),
    ?assertEqual(ok,
        barrel_sync_sig:verify(?METHOD, ?PATH, CH, Parsed, Signers, 300000)).

unknown_key_test() ->
    {_Pub, Priv} = keypair(),
    KeyId = <<"node1">>,
    CH = barrel_sync_sig:content_sha256(<<>>),
    Ts = erlang:system_time(millisecond),
    Auth = barrel_sync_sig:sign(KeyId, Priv, ?METHOD, ?PATH, CH, Ts),
    {ok, Parsed} = barrel_sync_sig:parse_auth(Auth),
    ?assertEqual({error, unknown_key},
        barrel_sync_sig:verify(?METHOD, ?PATH, CH, Parsed, #{}, 300000)).

%% A different content hash than was signed fails verification (this is
%% how a tampered body is caught: the handler re-derives the hash).
bad_signature_test() ->
    {Pub, Priv} = keypair(),
    KeyId = <<"node1">>,
    Signers = #{KeyId => Pub},
    CH = barrel_sync_sig:content_sha256(<<"real">>),
    Ts = erlang:system_time(millisecond),
    Auth = barrel_sync_sig:sign(KeyId, Priv, ?METHOD, ?PATH, CH, Ts),
    {ok, Parsed} = barrel_sync_sig:parse_auth(Auth),
    Tampered = barrel_sync_sig:content_sha256(<<"tampered">>),
    ?assertEqual({error, bad_signature},
        barrel_sync_sig:verify(?METHOD, ?PATH, Tampered, Parsed, Signers,
                               300000)).

%% A different path than was signed also fails (binds method+path).
wrong_path_test() ->
    {Pub, Priv} = keypair(),
    KeyId = <<"node1">>,
    Signers = #{KeyId => Pub},
    CH = barrel_sync_sig:content_sha256(<<>>),
    Ts = erlang:system_time(millisecond),
    Auth = barrel_sync_sig:sign(KeyId, Priv, ?METHOD, ?PATH, CH, Ts),
    {ok, Parsed} = barrel_sync_sig:parse_auth(Auth),
    ?assertEqual({error, bad_signature},
        barrel_sync_sig:verify(?METHOD, <<"/db/other/_sync/changes">>, CH,
                               Parsed, Signers, 300000)).

stale_test() ->
    {Pub, Priv} = keypair(),
    KeyId = <<"node1">>,
    Signers = #{KeyId => Pub},
    CH = barrel_sync_sig:content_sha256(<<>>),
    OldTs = erlang:system_time(millisecond) - 600000,
    Auth = barrel_sync_sig:sign(KeyId, Priv, ?METHOD, ?PATH, CH, OldTs),
    {ok, Parsed} = barrel_sync_sig:parse_auth(Auth),
    ?assertEqual({error, stale},
        barrel_sync_sig:verify(?METHOD, ?PATH, CH, Parsed, Signers, 300000)).

parse_bearer_test() ->
    ?assertEqual(not_signature,
                 barrel_sync_sig:parse_auth(<<"Bearer some-token">>)),
    ?assertEqual(not_signature, barrel_sync_sig:parse_auth(undefined)).

parse_malformed_test() ->
    ?assertEqual({error, malformed},
                 barrel_sync_sig:parse_auth(<<"Signature nonsense">>)).

content_sha256_test() ->
    %% known vector: sha256("") lowercase hex
    ?assertEqual(
        <<"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855">>,
        barrel_sync_sig:content_sha256(<<>>)).

%%====================================================================
%% Signature v2: nonce + signed target (path plus raw query)
%%====================================================================

-define(QUERY, <<"since=first&limit=1">>).
-define(OPTS, #{skew_ms => 300000}).

v2_roundtrip_query_test() ->
    {Pub, Priv} = keypair(),
    KeyId = <<"node1">>,
    Signers = #{KeyId => Pub},
    CH = barrel_sync_sig:content_sha256(<<>>),
    Ts = erlang:system_time(millisecond),
    Auth = barrel_sync_sig:sign(KeyId, Priv, <<"GET">>, ?PATH, ?QUERY, CH,
                                Ts),
    {ok, #{nonce := _} = Parsed} = barrel_sync_sig:parse_auth(Auth),
    ?assertEqual(ok, barrel_sync_sig:verify(<<"GET">>, ?PATH, ?QUERY, CH,
                                            Parsed, Signers, ?OPTS)),
    %% an altered query no longer verifies
    ?assertEqual({error, bad_signature},
                 barrel_sync_sig:verify(<<"GET">>, ?PATH,
                                        <<"since=first&limit=2">>, CH,
                                        Parsed, Signers, ?OPTS)),
    %% a v1 verifier (path only) cannot accept a v2 header
    ?assertEqual({error, bad_signature},
                 barrel_sync_sig:verify(<<"GET">>, ?PATH, CH, Parsed,
                                        Signers, 300000)).

v2_no_query_test() ->
    {Pub, Priv} = keypair(),
    KeyId = <<"node1">>,
    Signers = #{KeyId => Pub},
    CH = barrel_sync_sig:content_sha256(<<"{}">>),
    Ts = erlang:system_time(millisecond),
    Auth = barrel_sync_sig:sign(KeyId, Priv, ?METHOD, ?PATH, <<>>, CH, Ts),
    {ok, Parsed} = barrel_sync_sig:parse_auth(Auth),
    ?assertEqual(ok, barrel_sync_sig:verify(?METHOD, ?PATH, <<>>, CH,
                                            Parsed, Signers, ?OPTS)),
    %% `/p?' and `/p' sign the same target
    ?assertEqual(?PATH, barrel_sync_sig:target(?PATH, <<>>)),
    ?assertEqual(<<?PATH/binary, "?a=1">>,
                 barrel_sync_sig:target(?PATH, <<"a=1">>)).

%% The whole point: two requests in the same millisecond differ.
v2_same_ms_distinct_test() ->
    {_Pub, Priv} = keypair(),
    CH = barrel_sync_sig:content_sha256(<<>>),
    Ts = erlang:system_time(millisecond),
    A = barrel_sync_sig:sign(<<"k">>, Priv, <<"GET">>, ?PATH, <<>>, CH, Ts),
    B = barrel_sync_sig:sign(<<"k">>, Priv, <<"GET">>, ?PATH, <<>>, CH, Ts),
    ?assertNotEqual(A, B),
    %% v1 was byte-identical in that situation
    ?assertEqual(barrel_sync_sig:sign(<<"k">>, Priv, <<"GET">>, ?PATH, CH, Ts),
                 barrel_sync_sig:sign(<<"k">>, Priv, <<"GET">>, ?PATH, CH, Ts)).

nonce_tamper_test() ->
    {Pub, Priv} = keypair(),
    KeyId = <<"node1">>,
    Signers = #{KeyId => Pub},
    CH = barrel_sync_sig:content_sha256(<<>>),
    Ts = erlang:system_time(millisecond),
    Auth = barrel_sync_sig:sign(KeyId, Priv, <<"GET">>, ?PATH, <<>>, CH, Ts,
                                barrel_sync_sig:new_nonce()),
    {ok, Parsed} = barrel_sync_sig:parse_auth(Auth),
    Swapped = Parsed#{nonce => barrel_sync_sig:new_nonce()},
    ?assertEqual({error, bad_signature},
                 barrel_sync_sig:verify(<<"GET">>, ?PATH, <<>>, CH, Swapped,
                                        Signers, ?OPTS)).

malformed_nonce_test() ->
    Sig = base64:encode(<<0:512>>),
    Bad = <<"Signature keyId=\"k\",ts=\"1\",nonce=\"!!!\",sig=\"",
            Sig/binary, "\"">>,
    ?assertEqual({error, malformed}, barrel_sync_sig:parse_auth(Bad)),
    %% decodes, but to fewer than 8 bytes
    Short = <<"Signature keyId=\"k\",ts=\"1\",nonce=\"AAAA\",sig=\"",
              Sig/binary, "\"">>,
    ?assertEqual({error, malformed}, barrel_sync_sig:parse_auth(Short)).

%% v1 headers keep verifying through verify/7 until require_nonce.
v1_through_verify7_test() ->
    {Pub, Priv} = keypair(),
    KeyId = <<"node1">>,
    Signers = #{KeyId => Pub},
    CH = barrel_sync_sig:content_sha256(<<>>),
    Ts = erlang:system_time(millisecond),
    Auth = barrel_sync_sig:sign(KeyId, Priv, ?METHOD, ?PATH, CH, Ts),
    {ok, Parsed} = barrel_sync_sig:parse_auth(Auth),
    ?assertNot(maps:is_key(nonce, Parsed)),
    ?assertEqual(ok, barrel_sync_sig:verify(?METHOD, ?PATH, <<>>, CH,
                                            Parsed, Signers, ?OPTS)),
    ?assertEqual({error, nonce_required},
                 barrel_sync_sig:verify(?METHOD, ?PATH, <<>>, CH, Parsed,
                                        Signers,
                                        #{skew_ms => 300000,
                                          require_nonce => true})).

%%====================================================================
%% v3: semantic headers bound
%%====================================================================

v3_sign(Priv, Headers) ->
    {ok, Picked} = barrel_sync_sig:pick_headers(Headers),
    barrel_sync_sig:sign_v3(<<"node1">>, Priv, ?METHOD, ?PATH, <<>>,
                            barrel_sync_sig:content_sha256(<<>>), Picked,
                            erlang:system_time(millisecond)).

v3_verify(Auth, Pub, Headers) ->
    {ok, Parsed} = barrel_sync_sig:parse_auth(Auth),
    {ok, Picked} = barrel_sync_sig:pick_headers(Headers),
    barrel_sync_sig:verify(?METHOD, ?PATH, <<>>,
                           barrel_sync_sig:content_sha256(<<>>), Picked,
                           Parsed, #{<<"node1">> => Pub},
                           #{skew_ms => 300000}).

v3_roundtrip_test() ->
    {Pub, Priv} = keypair(),
    Hs = [{<<"X-Barrel-Att-Origin">>, <<"AAA=">>},
          {<<"content-type">>, <<"text/plain">>},
          {<<"accept">>, <<"application/json">>}],
    Auth = v3_sign(Priv, Hs),
    {ok, #{version := 3}} = barrel_sync_sig:parse_auth(Auth),
    ?assertEqual(ok, v3_verify(Auth, Pub, Hs)),
    %% unsigned headers (accept) may change freely
    ?assertEqual(ok, v3_verify(Auth, Pub, lists:keydelete(<<"accept">>, 1,
                                                         Hs))).

v3_header_tamper_test() ->
    {Pub, Priv} = keypair(),
    Hs = [{<<"x-barrel-att-origin">>, <<"AAA=">>},
          {<<"x-barrel-digest">>, <<"sha256-00">>}],
    Auth = v3_sign(Priv, Hs),
    Bad = {error, bad_signature},
    ?assertEqual(Bad, v3_verify(Auth, Pub,
        lists:keystore(<<"x-barrel-att-origin">>, 1, Hs,
                       {<<"x-barrel-att-origin">>, <<"BBB=">>}))),
    ?assertEqual(Bad, v3_verify(Auth, Pub, tl(Hs))),
    ?assertEqual(Bad, v3_verify(Auth, Pub,
        [{<<"x-barrel-att-touch">>, <<"1">>} | Hs])).

%% A header sent empty is not the same as one not sent.
v3_absent_vs_empty_test() ->
    {Pub, Priv} = keypair(),
    Auth = v3_sign(Priv, []),
    ?assertEqual(ok, v3_verify(Auth, Pub, [])),
    ?assertEqual({error, bad_signature},
                 v3_verify(Auth, Pub, [{<<"x-barrel-att-touch">>, <<>>}])),
    {ok, Empty} = barrel_sync_sig:pick_headers(
        [{<<"x-barrel-att-touch">>, <<>>}]),
    ?assertNotEqual(
        barrel_sync_sig:canonical_v3(<<"1">>, <<"k">>, <<"n">>, ?METHOD,
                                     ?PATH, <<"h">>, #{}),
        barrel_sync_sig:canonical_v3(<<"1">>, <<"k">>, <<"n">>, ?METHOD,
                                     ?PATH, <<"h">>, Empty)).

v3_duplicate_header_test() ->
    ?assertEqual({error, duplicate_header},
                 barrel_sync_sig:pick_headers(
                     [{<<"x-barrel-att-origin">>, <<"A">>},
                      {<<"X-Barrel-Att-Origin">>, <<"B">>}])).

%% Dropping the version marker does not turn a v3 signature into a valid
%% v2 one, and an unknown version is malformed.
v3_no_downgrade_test() ->
    {Pub, Priv} = keypair(),
    <<"Signature v=\"3\",", Rest/binary>> = v3_sign(Priv, []),
    {ok, Parsed} = barrel_sync_sig:parse_auth(<<"Signature ", Rest/binary>>),
    ?assertEqual({error, bad_signature},
                 barrel_sync_sig:verify(?METHOD, ?PATH, <<>>,
                     barrel_sync_sig:content_sha256(<<>>), Parsed,
                     #{<<"node1">> => Pub}, #{skew_ms => 300000})),
    ?assertEqual({error, malformed},
                 barrel_sync_sig:parse_auth(
                     <<"Signature v=\"4\",", Rest/binary>>)).
