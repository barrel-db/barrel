%% @doc Encryption primitives for Barrel.
%%
%% Provides AES-256-GCM envelope encryption with HKDF-SHA256 key derivation,
%% offset-addressable AES-256-CTR for sector-encrypted flat files, and
%% key-check tokens for fail-closed wrong-key detection at database open.
%% Keys come from a {@link barrel_keyprovider}; encryption is off by default.
%%
%% == Encrypted value format (GCM envelope) ==
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
    derive_key/1,
    derive_key/2,
    encrypt/2,
    decrypt/2,
    is_encrypted/1,
    encrypt_term/2,
    decrypt_term/2,
    ctr_crypt/3,
    ctr_crypt/4,
    new_nonce/1,
    key_check_new/1,
    key_check_verify/2
]).

-define(MAGIC, <<16#42, 16#52>>).  %% "BR"
-define(VERSION, 1).
-define(NONCE_SIZE, 12).  %% 96 bits
-define(TAG_SIZE, 16).    %% 128 bits
-define(KEY_SIZE, 32).    %% 256 bits (AES-256)
-define(CTR_NONCE_SIZE, 8).
-define(AES_BLOCK, 16).

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

%% @doc XOR Data with the AES-256-CTR keystream starting at the given full
%% 16-byte counter block. CTR is symmetric: the same call encrypts and
%% decrypts, and ciphertext length equals plaintext length.
-spec ctr_crypt(binary(), binary(), binary()) -> binary().
ctr_crypt(Key, IV, Data)
  when byte_size(Key) =:= ?KEY_SIZE, byte_size(IV) =:= ?AES_BLOCK,
       is_binary(Data) ->
    crypto:crypto_one_time(aes_256_ctr, Key, IV, Data, true).

%% @doc XOR Data with the keystream positioned at ByteOffset within the
%% stream defined by counter blocks `<<Nonce:8/binary, BlockIndex:64/big>>'.
%% Any byte range of a file encrypted this way decrypts independently, which
%% is what the mmap'd flat-file readers need.
-spec ctr_crypt(binary(), binary(), non_neg_integer(), binary()) -> binary().
ctr_crypt(Key, Nonce, ByteOffset, Data)
  when byte_size(Key) =:= ?KEY_SIZE, byte_size(Nonce) =:= ?CTR_NONCE_SIZE,
       is_integer(ByteOffset), ByteOffset >= 0, is_binary(Data) ->
    Block = ByteOffset div ?AES_BLOCK,
    Skip = ByteOffset rem ?AES_BLOCK,
    IV = <<Nonce/binary, Block:64/big-unsigned>>,
    %% Leading zero bytes burn the first Skip keystream bytes so Data lines
    %% up with its in-stream position; XOR with zero leaves pure keystream,
    %% which binary:part discards.
    Out = crypto:crypto_one_time(
        aes_256_ctr, Key, IV, <<0:(Skip * 8), Data/binary>>, true
    ),
    binary:part(Out, Skip, byte_size(Data)).

%% @doc Fresh random nonce of the given size.
-spec new_nonce(pos_integer()) -> binary().
new_nonce(Size) when is_integer(Size), Size > 0 ->
    crypto:strong_rand_bytes(Size).

%% @doc Build a key-check token: 16 random bytes sealed under Key. Stored in
%% cleartext next to encrypted data, it lets an open distinguish a wrong key
%% from corruption without keeping any key material on disk.
-spec key_check_new(binary()) -> binary().
key_check_new(Key) when byte_size(Key) =:= ?KEY_SIZE ->
    encrypt(crypto:strong_rand_bytes(16), Key).

%% @doc Verify a key-check token against Key. Anything that is not a valid
%% envelope sealed under Key is a mismatch (fail closed).
-spec key_check_verify(binary(), binary()) -> boolean().
key_check_verify(Key, Token)
  when byte_size(Key) =:= ?KEY_SIZE, is_binary(Token) ->
    case is_encrypted(Token) of
        false ->
            false;
        true ->
            case decrypt(Token, Key) of
                {error, decryption_failed} -> false;
                Plain when is_binary(Plain) -> true
            end
    end;
key_check_verify(_Key, _Token) ->
    false.

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
