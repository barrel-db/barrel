# Back up and restore a database

A backup is a copy of a database's documents and attachments at one instant,
taken while the database stays open and keeps taking writes. A restore turns
a backup into a database again, under the same name or a new one. Read this
when you plan backups of a node, recover from a lost disk, or copy a database
to start a new node from.

## What a backup holds

A backup is a directory:

```
notes-20261115/
  docs/            the document store (RocksDB files)
  attachments/     the attachment blobs (absent with the `none` backend)
  CRYPTO           present when the database is encrypted
  MANIFEST.json    what it is, and the size and sha256 of every file
```

It holds every write the database applied before `backup_hlc`, an instant its
writer picks, and none after. Documents, conflicts, past versions within the
retention window, local documents and attachments all come along.

The directory is built under a temporary name and renamed once the manifest is
written: a backup directory that exists is a whole backup. An encrypted
database stays encrypted in its backup.

## Back up from Erlang

```erlang
{ok, #{dir := Dir, backup_hlc := Hlc}} =
    barrel_docdb:backup_db(<<"notes">>, "/backups/notes-20261115").
```

The target directory must not exist. On the database's own filesystem the
files are hard links, so a backup takes little time and space until the
database moves on; on another filesystem they are copied.

## Restore from Erlang

```erlang
{ok, Pid} = barrel_docdb:restore_db("/backups/notes-20261115", <<"notes_copy">>).
```

The checksums are verified before anything is put in place. The name must be
free: to restore over a lost database, delete it first, or restore on a node
that does not have it.

```erlang
ok = barrel_docdb:delete_db(<<"notes">>),
{ok, _} = barrel_docdb:restore_db("/backups/notes-20261115", <<"notes">>).
```

Options are those of `create_db/2`. `retention_period` defaults to the one in
the backup. An encrypted backup needs the key provider that opened the source:

```erlang
{ok, _} = barrel_docdb:restore_db(Dir, <<"vault">>,
                                  #{encryption => #{provider => my_keys}}).
```

## Back up a running node with barrel-backup

The barrel_server release ships `bin/barrel-backup`. It connects to the
running node and asks it to do the work, so every path is a path on the node.

```console
$ bin/barrel-backup create /backups --all        # or: create /backups notes tasks
$ bin/barrel-backup list /backups                 # newest first: db, stamp, backup_hlc, bytes
$ bin/barrel-backup restore /backups/notes/20261115T020000Z notes_copy
$ bin/barrel-backup prune /backups --keep 7       # keep the 7 newest per database
```

`create` writes `/backups/<db>/<UTC stamp>/`. `--all` covers every database
in the node's data directory, open or not. It stops at the first failure,
prints the database and the reason, and exits 1, so you can run it from cron:

```
0 2 * * *  /opt/barrel/bin/barrel-backup create /backups --all && /opt/barrel/bin/barrel-backup prune /backups --keep 7
```

On a barrel_server node the databases are barrel databases, and
`barrel-backup` backs them up with their vector store (see
[barrel's backups](embedding.md#back-up-and-restore)). A record-mode
database rebuilds its index from its stored embeddings when restored.

In Docker, the same command is `barrel backup`:

```console
$ docker exec barrel barrel backup create /data/backups --all
```

Put `/data/backups` on a volume, or copy it off the host.

## barrel-backup and distribution

The script reads the node name, the cookie and the distribution flags from
the node's `vm.args`, and loads its `sys.config`. It looks for them in this
order: `BARREL_VMARGS` and `BARREL_SYS_CONFIG`, then `VMARGS_PATH` and
`RELX_CONFIG_PATH`, then `/etc/barrel/`, then the release's own files.

It works whether the node runs TCP, TLS or QUIC distribution. On a QUIC node
(`-proto_dist quic`) it opens its own QUIC endpoint on a free port and
presents the node's certificate and key, taken from `-quic_dist_cert` and
`-quic_dist_key`. With `{verify, verify_peer}` the node must trust that
certificate, which it does since it is its own. To present another one:

```console
$ BARREL_BACKUP_CERT=/etc/barrel/backup.pem BARREL_BACKUP_KEY=/etc/barrel/backup.key \
    bin/barrel-backup list /backups
```

The certificate needs both TLS server and client usage, and an IP SAN for the
host when the node checks it.

## Notes

- **A restored database is a new author.** It gets a new source id, so it
  never writes under the identity of the database it came from or of that
  database's replicas. Replication checkpoints copied with it no longer
  match: its peers resync it from scratch, or from the retention window.
- **Retention.** The first sweep after a restore counts from now. A backup
  older than the retention period loses the past versions beyond it.
- **Branches.** A database restored under a new name cannot be branched
  (`branch_db/3` answers `cannot_branch_a_branch`). Restore under the
  original name to branch it.
- **S3 attachments.** A database whose attachments live in S3 is refused
  (`unsupported_att_backend`): the objects are not on the node. Back up the
  bucket with your storage provider's tools.
- **Errors.** `backup_dir_exists`, `db_exists`, `corrupt_backup` (with the
  files that do not match), `not_a_backup` (no manifest, as an interrupted
  backup), `bad_manifest`.
