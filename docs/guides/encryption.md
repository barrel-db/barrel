# Encryption at rest

Barrel can encrypt a whole logical database at rest under one per-database
key: the document and attachment stores, the vector store, and the disk
index files (BM25, DiskANN). Read this when your data must not be readable
from disk, or when you want per-database keys as an isolation boundary
between agents or tenants.

## What it is

You pass an `encryption` spec at open. The RocksDB stores encrypt through
an EncryptedEnv (AES-256-CTR on every file RocksDB writes, including the
WAL and blob files), and the mmap'd flat files of the disk indexes encrypt
per sector with the same key. Keys never live on disk: a cleartext
key-check token next to the data lets an open fail closed with
`wrong_encryption_key` instead of surfacing corruption, and distinguishes
encrypted from plaintext databases (`db_is_encrypted`,
`cannot_encrypt_existing_db`).

The key is resolved by a `barrel_keyprovider` from the database KEYSPACE,
the identity data is stored under. A timeline branch shares its parent's
keyspace, so it opens with the parent's key; a composed database resolves
one key that covers both its docdb and vectordb sides.

## When to use it

- Data at rest must be unreadable without the key (stolen disk, shared
  hosts, backups of raw files).
- Per-agent or per-tenant databases where the key doubles as an isolation
  boundary: no key, no data.

## How (built-in provider)

Set one master secret in the environment; every database derives its own
key from it (HKDF with the keyspace):

```bash
export BARREL_ENCRYPTION_KEY="a long passphrase"   # or 32 raw bytes / 64 hex chars
```

```erlang
%% composed database: docdb + vectordb under one key
{ok, Db} = barrel:open(mydb, #{encryption => default,
                               vectordb => #{db_path => "data/mydb_vec"}}),

%% docdb alone
{ok, _} = barrel_docdb:create_db(<<"mydb">>, #{encryption => default}).
```

Encryption is runtime config, exactly like `channels`: pass it again on
every open. A database created plaintext cannot be opened encrypted and
vice versa.

## How (custom provider)

Implement `barrel_keyprovider` for KMS- or token-derived keys:

```erlang
-module(my_kms_provider).
-behaviour(barrel_keyprovider).
-export([key_for_db/1]).

key_for_db(Keyspace) ->
    {ok, my_kms:data_key(Keyspace)}.   %% must return exactly 32 bytes
```

```erlang
{ok, Db} = barrel:open(mydb, #{encryption => #{provider => my_kms_provider}}).
```

A provider error fails the open (fail closed).

## How (REST server)

Configure the spec in `sys.config`; the server applies it to every
database it opens. Key material never rides the HTTP API:

```erlang
{barrel_server, [
    {open_opts, #{encryption => default}}
]}
```

## How (timeline)

Nothing extra. A branch's checkpoint is ciphertext under the parent's key
and the branch shares the parent's keyspace, so `barrel:branch/3`,
PITR (`at => T`), and `barrel:merge/1` work unchanged on encrypted
databases. The branch handle inherits the parent's encryption spec;
reopening a branch after a restart needs the spec again, like any other
runtime config.

## Notes

- Migration is by replication: create a fresh encrypted database and
  replicate into it. There is no in-place encryption of existing files.
- A lost key means lost data. There is no recovery path.
- Key rotation is not built in yet; rotate by replicating into a database
  under a new key.
- The threat model is data at rest. CTR (both RocksDB's EncryptedEnv and
  the sector cipher) is not authenticated, so an active attacker with
  write access to the files is out of scope. In-memory keys are regular
  BEAM binaries and are not zeroized.
- Vector search indexes are covered: HNSW and FAISS state persists in the
  encrypted RocksDB; the disk BM25 and DiskANN flat files use the sector
  cipher, and their id-mapping RocksDBs open under the same env. An
  encrypted DiskANN index always uses its own standalone id database.
- Wrong-key and mismatch errors at open: `wrong_encryption_key`,
  `db_is_encrypted`, `cannot_encrypt_existing_db`, and for disk index
  files `index_is_encrypted` / `cannot_encrypt_legacy_index`.
