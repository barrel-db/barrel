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
