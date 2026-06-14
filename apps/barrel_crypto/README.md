# barrel_crypto

Value-level encryption for Barrel. Provides AES-256-GCM with HKDF-SHA256 key
derivation and a versioned envelope, used to encrypt values at storage boundaries
(document bodies, vector text and metadata, object payloads). It is the
object-layer companion to the RocksDB EncryptedEnv at-rest layer.

Encryption is off by default and unencrypted data is detected and passed through,
so a database can hold mixed plaintext and ciphertext during migration. Keys come
from a `barrel_crypto_keyprovider`; the default reads `BARREL_ENCRYPTION_KEY`.

## Use

```erlang
Key = barrel_crypto:derive_key(<<"some key material">>, <<"db:mydb">>),
Enc = barrel_crypto:encrypt(<<"secret">>, Key),
true = barrel_crypto:is_encrypted(Enc),
<<"secret">> = barrel_crypto:decrypt(Enc, Key).
```

`barrel_crypto:decrypt/2` returns unencrypted input unchanged, so it is safe to
call on values written before encryption was enabled.
