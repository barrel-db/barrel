%%%-------------------------------------------------------------------
%%% @doc Encryption of S3 attachment objects for encrypted databases.
%%%
%%% Each object gets a random salt, kept in its `x-amz-meta-enc' header as
%%% `v1.<hex salt>'. The object key is derived from the database key and
%%% the salt, and the bytes are AES-256-CTR from offset 0, so the stored
%%% object has the plaintext's length and any byte range decrypts on its
%%% own. Nothing binds to the object's S3 key, so a branch copy stays
%%% readable under the shared keyspace key. The digest in the metadata is
%%% sealed with AES-GCM.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_att_s3_crypto).

-export([new_salt/0, object_key/2, crypt/3,
         enc_meta/1, parse_enc/1,
         seal_digest/2, open_digest/2]).

-define(SALT_SIZE, 16).
-define(NONCE, <<0:64>>).

-spec new_salt() -> binary().
new_salt() ->
    crypto:strong_rand_bytes(?SALT_SIZE).

%% @doc The key that encrypts one object.
-spec object_key(binary(), binary()) -> binary().
object_key(Key, Salt) ->
    barrel_crypto:derive_key(Key, <<"barrel_att_s3:", Salt/binary>>).

%% @doc Encrypt or decrypt `Data' found at byte `Offset' of the object.
-spec crypt(binary(), non_neg_integer(), binary()) -> binary().
crypt(ObjKey, Offset, Data) ->
    barrel_crypto:ctr_crypt(ObjKey, ?NONCE, Offset, Data).

-spec enc_meta(binary()) -> binary().
enc_meta(Salt) ->
    <<"v1.", (binary:encode_hex(Salt, lowercase))/binary>>.

%% @doc The salt of an `enc' metadata value; `none' when absent.
-spec parse_enc(binary() | undefined) -> {ok, binary()} | none | {error, term()}.
parse_enc(undefined) ->
    none;
parse_enc(<<"v1.", Hex/binary>>) when byte_size(Hex) =:= 2 * ?SALT_SIZE ->
    try {ok, binary:decode_hex(Hex)}
    catch _:_ -> {error, bad_enc_metadata}
    end;
parse_enc(_) ->
    {error, bad_enc_metadata}.

-spec seal_digest(binary(), binary()) -> binary().
seal_digest(Key, Digest) ->
    binary:encode_hex(barrel_crypto:encrypt(Digest, Key), lowercase).

-spec open_digest(binary(), binary() | undefined) -> binary() | undefined.
open_digest(_Key, undefined) ->
    undefined;
open_digest(Key, Hex) ->
    try barrel_crypto:decrypt(binary:decode_hex(Hex), Key) of
        Digest when is_binary(Digest) -> Digest;
        {error, _} -> undefined
    catch _:_ -> undefined
    end.
