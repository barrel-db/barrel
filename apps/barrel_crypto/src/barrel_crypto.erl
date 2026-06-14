%% @doc Value-level encryption for Barrel.
%%
%% Provides AES-256-GCM encryption with HKDF-SHA256 key derivation, used to
%% encrypt values at storage boundaries (document bodies, vector text/metadata,
%% object payloads). It is the object-layer companion to the RocksDB EncryptedEnv
%% at-rest layer: keys come from a {@link barrel_crypto_keyprovider}, encryption
%% is off by default, and unencrypted data is detected and passed through so a
%% database can hold mixed plaintext and ciphertext during migration.
%%
%% == Encrypted value format ==
%% ```
%% +------+------+----------+--------------+------+
%% |  BR  | Ver  |  Nonce   |  Ciphertext  | Tag  |
%% | 2B   | 1B   |   12B    |   Variable   | 16B  |
%% +------+------+----------+--------------+------+
%% '''
%% Magic bytes "BR" (0x42, 0x52) mark an encrypted value; a version byte allows
%% future key rotation; a 12-byte random nonce and 16-byte GCM tag follow.
%% @end
-module(barrel_crypto).

-export([
    init/0,
    is_enabled/0,
    derive_key/1,
    derive_key/2,
    encrypt/2,
    decrypt/2,
    is_encrypted/1,
    encrypt_term/2,
    decrypt_term/2
]).

-define(MAGIC, <<16#42, 16#52>>).  %% "BR"
-define(VERSION, 1).
-define(NONCE_SIZE, 12).  %% 96 bits
-define(TAG_SIZE, 16).    %% 128 bits
-define(KEY_SIZE, 32).    %% 256 bits (AES-256)

-define(KEY_CACHE_TABLE, barrel_crypto_key_cache).

%%====================================================================
%% API
%%====================================================================

%% @doc Initialise the key derivation cache.
-spec init() -> ok.
init() ->
    case ets:whereis(?KEY_CACHE_TABLE) of
        undefined ->
            ?KEY_CACHE_TABLE = ets:new(?KEY_CACHE_TABLE, [
                named_table,
                public,
                set,
                {read_concurrency, true}
            ]),
            ok;
        _Tid ->
            ok
    end.

%% @doc Whether encryption is enabled (env var overrides application config).
-spec is_enabled() -> boolean().
is_enabled() ->
    case os:getenv("BARREL_ENCRYPTION_ENABLED") of
        "true" -> true;
        "1" -> true;
        "false" -> false;
        "0" -> false;
        _ -> application:get_env(barrel_crypto, encryption_enabled, false)
    end.

%% @doc Derive a 256-bit key from input key material using HKDF-SHA256.
-spec derive_key(binary()) -> binary().
derive_key(Ikm) ->
    derive_key(Ikm, <<"barrel_encryption_v1">>).

%% @doc Derive a 256-bit key with an explicit HKDF info label (per-database key).
-spec derive_key(binary(), binary()) -> binary().
derive_key(Ikm, Info) when is_binary(Ikm), is_binary(Info) ->
    CacheKey = crypto:hash(sha256, <<Ikm/binary, 0, Info/binary>>),
    case cache_lookup(CacheKey) of
        {ok, Cached} ->
            Cached;
        miss ->
            Salt = get_salt(),
            Derived = hkdf_sha256(Ikm, Salt, Info, ?KEY_SIZE),
            cache_insert(CacheKey, Derived),
            Derived
    end.

%% @doc Encrypt plaintext using AES-256-GCM. Returns the framed envelope.
-spec encrypt(binary(), binary()) -> binary().
encrypt(Plaintext, Key) when is_binary(Plaintext), byte_size(Key) =:= ?KEY_SIZE ->
    Nonce = crypto:strong_rand_bytes(?NONCE_SIZE),
    {Ciphertext, Tag} = crypto:crypto_one_time_aead(
        aes_256_gcm, Key, Nonce, Plaintext, <<>>, ?TAG_SIZE, true
    ),
    <<?MAGIC/binary, ?VERSION:8, Nonce/binary, Ciphertext/binary, Tag/binary>>.

%% @doc Decrypt a framed envelope. Unencrypted data is returned unchanged.
-spec decrypt(binary(), binary()) -> binary() | {error, decryption_failed}.
decrypt(Data, Key) when is_binary(Data), byte_size(Key) =:= ?KEY_SIZE ->
    case is_encrypted(Data) of
        false ->
            Data;
        true ->
            <<_Magic:2/binary, _Version:8, Nonce:?NONCE_SIZE/binary, Rest/binary>> = Data,
            CiphertextLen = byte_size(Rest) - ?TAG_SIZE,
            <<Ciphertext:CiphertextLen/binary, Tag:?TAG_SIZE/binary>> = Rest,
            case crypto:crypto_one_time_aead(
                aes_256_gcm, Key, Nonce, Ciphertext, <<>>, Tag, false
            ) of
                Plaintext when is_binary(Plaintext) ->
                    Plaintext;
                error ->
                    {error, decryption_failed}
            end
    end.

%% @doc Whether the value carries the encryption envelope header.
-spec is_encrypted(binary()) -> boolean().
is_encrypted(<<16#42, 16#52, _/binary>>) -> true;
is_encrypted(_) -> false.

%% @doc Encrypt an arbitrary Erlang term (serialised with term_to_binary).
-spec encrypt_term(term(), binary()) -> binary().
encrypt_term(Term, Key) ->
    encrypt(term_to_binary(Term), Key).

%% @doc Decrypt back to the original term. Unencrypted input is returned as-is.
-spec decrypt_term(binary(), binary()) -> term().
decrypt_term(Data, Key) when is_binary(Data) ->
    case is_encrypted(Data) of
        true ->
            case decrypt(Data, Key) of
                {error, _} = Err ->
                    Err;
                Decrypted ->
                    try binary_to_term(Decrypted)
                    catch _:_ -> Decrypted
                    end
            end;
        false ->
            Data
    end;
decrypt_term(Term, _Key) ->
    Term.

%%====================================================================
%% Internal
%%====================================================================

cache_lookup(CacheKey) ->
    case ets:whereis(?KEY_CACHE_TABLE) of
        undefined ->
            miss;
        _ ->
            case ets:lookup(?KEY_CACHE_TABLE, CacheKey) of
                [{_, Cached}] -> {ok, Cached};
                [] -> miss
            end
    end.

cache_insert(CacheKey, Value) ->
    case ets:whereis(?KEY_CACHE_TABLE) of
        undefined -> ok;
        _ -> ets:insert(?KEY_CACHE_TABLE, {CacheKey, Value}), ok
    end.

-spec get_salt() -> binary().
get_salt() ->
    case os:getenv("BARREL_ENCRYPTION_SALT") of
        false ->
            case application:get_env(barrel_crypto, encryption_salt) of
                {ok, Salt} when is_binary(Salt), byte_size(Salt) > 0 ->
                    Salt;
                {ok, Salt} when is_list(Salt) ->
                    decode_salt(list_to_binary(Salt));
                _ ->
                    <<"barrel_default_salt_v1">>
            end;
        "" ->
            <<"barrel_default_salt_v1">>;
        SaltHex ->
            decode_salt(list_to_binary(SaltHex))
    end.

decode_salt(Bin) ->
    try binary:decode_hex(Bin)
    catch _:_ -> Bin
    end.

%% @private HKDF-SHA256 (RFC 5869).
-spec hkdf_sha256(binary(), binary(), binary(), pos_integer()) -> binary().
hkdf_sha256(Ikm, Salt, Info, Length) ->
    Prk = crypto:mac(hmac, sha256, Salt, Ikm),
    hkdf_expand(Prk, Info, Length, <<>>, 1, <<>>).

hkdf_expand(_Prk, _Info, Length, _Prev, _Counter, Acc) when byte_size(Acc) >= Length ->
    binary:part(Acc, 0, Length);
hkdf_expand(Prk, Info, Length, Prev, Counter, Acc) ->
    T = crypto:mac(hmac, sha256, Prk, <<Prev/binary, Info/binary, Counter:8>>),
    hkdf_expand(Prk, Info, Length, T, Counter + 1, <<Acc/binary, T/binary>>).
