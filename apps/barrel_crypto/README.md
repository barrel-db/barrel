# barrel_crypto

Encryption primitives and key providers for Barrel's encryption at rest.
Provides an AES-256-GCM envelope with HKDF-SHA256 key derivation,
offset-addressable AES-256-CTR for sector-encrypted flat files, key-check
tokens for fail-closed wrong-key detection, and the `barrel_keyprovider`
behaviour that maps a database keyspace to its 256-bit key.

Encryption is off by default. Databases opt in with an encryption spec at
open: `default` resolves keys through `barrel_keyprovider_env`
(`BARREL_ENCRYPTION_KEY` as 32 raw bytes, 64 hex chars, or a passphrase;
per-database keys are derived from it with the keyspace as HKDF info), and
`#{provider => Mod}` plugs in a custom provider (KMS, token-derived).

## Use

```erlang
{ok, Key} = barrel_keyprovider:key_for_db(default, <<"mydb">>),

%% GCM envelope for whole values
Enc = barrel_crypto:encrypt(<<"secret">>, Key),
true = barrel_crypto:is_encrypted(Enc),
<<"secret">> = barrel_crypto:decrypt(Enc, Key),

%% offset-addressable CTR for flat files
Nonce = barrel_crypto:new_nonce(8),
Ct = barrel_crypto:ctr_crypt(Key, Nonce, 0, <<"some file bytes">>),
<<"file">> = barrel_crypto:ctr_crypt(Key, Nonce, 5, binary:part(Ct, 5, 4)),

%% wrong-key detection without key material on disk
Token = barrel_crypto:key_check_new(Key),
true = barrel_crypto:key_check_verify(Key, Token).
```

`barrel_crypto:decrypt/2` returns unencrypted input unchanged, so it is safe
to call on values written before encryption was enabled.

## Custom provider

```erlang
-module(my_kms_provider).
-behaviour(barrel_keyprovider).
-export([key_for_db/1]).

key_for_db(Keyspace) ->
    {ok, my_kms:fetch_data_key(Keyspace)}.  %% must be exactly 32 bytes
```
