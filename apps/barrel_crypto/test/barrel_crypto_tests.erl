-module(barrel_crypto_tests).

-include_lib("eunit/include/eunit.hrl").

-define(KEY, binary:copy(<<16#5A>>, 32)).
-define(KEY2, binary:copy(<<16#A5>>, 32)).
-define(STUB, barrel_keyprovider_test_stub).

%%====================================================================
%% GCM envelope
%%====================================================================

gcm_roundtrip_test() ->
    Enc = barrel_crypto:encrypt(<<"secret">>, ?KEY),
    ?assert(barrel_crypto:is_encrypted(Enc)),
    ?assertNot(barrel_crypto:is_encrypted(<<"secret">>)),
    ?assertEqual(<<"secret">>, barrel_crypto:decrypt(Enc, ?KEY)).

gcm_wrong_key_test() ->
    Enc = barrel_crypto:encrypt(<<"secret">>, ?KEY),
    ?assertEqual({error, decryption_failed}, barrel_crypto:decrypt(Enc, ?KEY2)).

gcm_tamper_test() ->
    Enc = barrel_crypto:encrypt(<<"a longer secret payload">>, ?KEY),
    Pos = byte_size(Enc) - 20,
    <<Head:Pos/binary, B, Tail/binary>> = Enc,
    Tampered = <<Head/binary, (B bxor 1), Tail/binary>>,
    ?assertEqual({error, decryption_failed},
                 barrel_crypto:decrypt(Tampered, ?KEY)).

gcm_passthrough_test() ->
    ?assertEqual(<<"plain">>, barrel_crypto:decrypt(<<"plain">>, ?KEY)).

term_roundtrip_test() ->
    Term = #{a => [1, 2, 3], b => <<"x">>},
    Enc = barrel_crypto:encrypt_term(Term, ?KEY),
    ?assertEqual(Term, barrel_crypto:decrypt_term(Enc, ?KEY)).

%%====================================================================
%% Offset-addressable CTR
%%====================================================================

ctr_symmetric_test() ->
    N = barrel_crypto:new_nonce(8),
    Data = crypto:strong_rand_bytes(1000),
    Ct = barrel_crypto:ctr_crypt(?KEY, N, 0, Data),
    ?assertEqual(byte_size(Data), byte_size(Ct)),
    ?assertNotEqual(Data, Ct),
    ?assertEqual(Data, barrel_crypto:ctr_crypt(?KEY, N, 0, Ct)).

ctr_offset_slices_test() ->
    N = barrel_crypto:new_nonce(8),
    Data = crypto:strong_rand_bytes(4096),
    Ct = barrel_crypto:ctr_crypt(?KEY, N, 0, Data),
    lists:foreach(
        fun({Off, Len}) ->
            Slice = barrel_crypto:ctr_crypt(?KEY, N, Off,
                                            binary:part(Ct, Off, Len)),
            ?assertEqual(binary:part(Data, Off, Len), Slice)
        end,
        [{0, 16}, {13, 37}, {16, 16}, {15, 2}, {31, 1}, {100, 1000},
         {4095, 1}, {17, 4079}]).

ctr_positioned_write_test() ->
    %% encrypting a slice at its offset yields the same bytes whole-file
    %% encryption produces there
    N = barrel_crypto:new_nonce(8),
    Data = crypto:strong_rand_bytes(512),
    Ct = barrel_crypto:ctr_crypt(?KEY, N, 0, Data),
    Off = 33,
    Len = 111,
    Part = barrel_crypto:ctr_crypt(?KEY, N, Off, binary:part(Data, Off, Len)),
    ?assertEqual(binary:part(Ct, Off, Len), Part).

ctr_raw_iv_test() ->
    N = barrel_crypto:new_nonce(8),
    IV = <<N/binary, 0:64/big>>,
    Data = crypto:strong_rand_bytes(64),
    ?assertEqual(barrel_crypto:ctr_crypt(?KEY, N, 0, Data),
                 barrel_crypto:ctr_crypt(?KEY, IV, Data)).

%%====================================================================
%% Key-check tokens and nonces
%%====================================================================

key_check_test() ->
    Token = barrel_crypto:key_check_new(?KEY),
    ?assert(barrel_crypto:key_check_verify(?KEY, Token)),
    ?assertNot(barrel_crypto:key_check_verify(?KEY2, Token)),
    ?assertNot(barrel_crypto:key_check_verify(?KEY, <<"garbage">>)),
    ?assertNot(barrel_crypto:key_check_verify(?KEY, <<>>)).

new_nonce_test() ->
    ?assertEqual(8, byte_size(barrel_crypto:new_nonce(8))),
    ?assertEqual(12, byte_size(barrel_crypto:new_nonce(12))),
    ?assertNotEqual(barrel_crypto:new_nonce(12), barrel_crypto:new_nonce(12)).

%%====================================================================
%% Dispatcher
%%====================================================================

dispatcher_disabled_test() ->
    ?assertEqual({ok, plaintext},
                 barrel_keyprovider:key_for_db(disabled, <<"db">>)).

dispatcher_provider_test() ->
    Spec = #{provider => ?STUB},
    ?assertMatch({ok, <<_:32/binary>>},
                 barrel_keyprovider:key_for_db(Spec, <<"db">>)),
    ?assertEqual({error, {key_provider_error, ?STUB, boom}},
                 barrel_keyprovider:key_for_db(Spec, <<"boom">>)),
    ?assertEqual({error, {bad_key_size, ?STUB}},
                 barrel_keyprovider:key_for_db(Spec, <<"short">>)),
    ?assertEqual({error, {bad_provider_return, ?STUB}},
                 barrel_keyprovider:key_for_db(Spec, <<"bad_return">>)),
    ?assertMatch({error, {key_provider_error, ?STUB, {error, kaboom}}},
                 barrel_keyprovider:key_for_db(Spec, <<"crash">>)),
    ?assertMatch({error, {bad_encryption_spec, _}},
                 barrel_keyprovider:key_for_db(nonsense, <<"db">>)).

%%====================================================================
%% Built-in env provider
%%====================================================================

env_provider_test_() ->
    {foreach, fun save_env/0, fun restore_env/1, [
        fun raw32_master/1,
        fun hex64_master/1,
        fun passphrase_master/1,
        fun hex_length_passphrase/1,
        fun app_config_fallback/1,
        fun no_key/1,
        fun dispatcher_default/1
    ]}.

save_env() ->
    Old = os:getenv("BARREL_ENCRYPTION_KEY"),
    os:unsetenv("BARREL_ENCRYPTION_KEY"),
    application:unset_env(barrel_crypto, encryption_key),
    Old.

restore_env(false) ->
    os:unsetenv("BARREL_ENCRYPTION_KEY"),
    application:unset_env(barrel_crypto, encryption_key),
    ok;
restore_env(Old) ->
    os:putenv("BARREL_ENCRYPTION_KEY", Old),
    application:unset_env(barrel_crypto, encryption_key),
    ok.

raw32_master(_) ->
    fun() ->
        Master = <<"0123456789abcdefFEDCBA9876543210">>,
        32 = byte_size(Master),
        os:putenv("BARREL_ENCRYPTION_KEY", binary_to_list(Master)),
        {ok, K} = barrel_keyprovider_env:key_for_db(<<"mydb">>),
        ?assertEqual(barrel_crypto:derive_key(Master, <<"barrel_db:mydb">>), K),
        %% deterministic, distinct per keyspace
        ?assertEqual({ok, K}, barrel_keyprovider_env:key_for_db(<<"mydb">>)),
        {ok, K2} = barrel_keyprovider_env:key_for_db(<<"otherdb">>),
        ?assertNotEqual(K, K2)
    end.

hex64_master(_) ->
    fun() ->
        Hex = "00112233445566778899aabbccddeeff"
              "00112233445566778899aabbccddeeff",
        os:putenv("BARREL_ENCRYPTION_KEY", Hex),
        Ikm = binary:decode_hex(list_to_binary(Hex)),
        {ok, K} = barrel_keyprovider_env:key_for_db(<<"mydb">>),
        ?assertEqual(barrel_crypto:derive_key(Ikm, <<"barrel_db:mydb">>), K)
    end.

passphrase_master(_) ->
    fun() ->
        os:putenv("BARREL_ENCRYPTION_KEY", "correct horse battery staple"),
        {ok, K} = barrel_keyprovider_env:key_for_db(<<"mydb">>),
        Expected = barrel_crypto:derive_key(<<"correct horse battery staple">>,
                                            <<"barrel_db:mydb">>),
        ?assertEqual(Expected, K)
    end.

hex_length_passphrase(_) ->
    fun() ->
        %% 64 chars that are not hex stay a passphrase
        Pass = lists:duplicate(64, $z),
        os:putenv("BARREL_ENCRYPTION_KEY", Pass),
        {ok, K} = barrel_keyprovider_env:key_for_db(<<"mydb">>),
        Expected = barrel_crypto:derive_key(list_to_binary(Pass),
                                            <<"barrel_db:mydb">>),
        ?assertEqual(Expected, K)
    end.

app_config_fallback(_) ->
    fun() ->
        application:set_env(barrel_crypto, encryption_key, <<"from config">>),
        {ok, K} = barrel_keyprovider_env:key_for_db(<<"mydb">>),
        Expected = barrel_crypto:derive_key(<<"from config">>,
                                            <<"barrel_db:mydb">>),
        ?assertEqual(Expected, K)
    end.

no_key(_) ->
    fun() ->
        ?assertEqual({error, no_encryption_key},
                     barrel_keyprovider_env:key_for_db(<<"mydb">>)),
        ?assertEqual({error, {key_provider_error, barrel_keyprovider_env,
                              no_encryption_key}},
                     barrel_keyprovider:key_for_db(default, <<"mydb">>))
    end.

dispatcher_default(_) ->
    fun() ->
        os:putenv("BARREL_ENCRYPTION_KEY", "a passphrase"),
        ?assertMatch({ok, <<_:32/binary>>},
                     barrel_keyprovider:key_for_db(default, <<"mydb">>))
    end.
