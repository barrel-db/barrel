# End-to-end tests

Tests that run the real release across separate processes, not in one VM.

## Replication

`replication.sh` brings up two `barrel_server` containers on one network and
replicates between them over HTTP. Unlike `barrel_server_rep_SUITE`, which runs
both peers in a single VM (the server is a registered singleton), these are
genuinely separate OS processes: the wire path, process isolation, and network
are all real.

```console
$ test/e2e/replication.sh
```

It builds the image, starts `peer-a` and `peer-b`, then:

- writes documents to peer-a and pushes to peer-b, asserting convergence;
- writes to peer-b and pulls into peer-a;
- deletes a document on peer-a and re-pushes, asserting the delete propagates;
- starts a continuous `barrel_rep_tasks` push task between the peers, writes
  a doc and asserts it converges, then puts an attachment on that same doc
  with **no further doc change** and asserts it converges too. Attachments
  live on their own feed, independent of the document changes feed, so this
  is the scenario that only passes once a continuous task notices
  attachment-only activity on its own (bounded wake), not because a doc
  write happens to nudge it.

Exit 0 means every assertion passed. The script tears the stack down on exit.

Replication is triggered inside a peer with `barrel_server eval`, which evaluates
against the running node, so it runs the real `barrel_rep` algorithm (or, for the
continuous-task scenario, `barrel_rep_tasks:start_task/1`) against the other
peer's `_sync` endpoints.

## S3 attachment backend

`attachments-s3.sh` brings up real MinIO and Garage containers plus two
`barrel_server` peers built with the S3 attachment backend included (the
`s3_server` rebar3 profile), and exercises it over real HTTP routes and a
real network -- not the CT suite against the backend module directly (that's
`apps/barrel_att_s3/test/barrel_att_s3_SUITE.erl`).

```console
$ test/e2e/attachments-s3.sh
```

peer-a keeps the default (RocksDB) attachment backend; peer-b's databases
point their `att_opts` at MinIO or Garage. It:

- provisions MinIO and Garage (`attachments-s3-setup.sh` -- see below), then
  round-trips a small and a large (multipart-crossing) attachment against
  each store, and confirms a delete actually 404s afterward;
- exercises `create_only` write-conflict detection directly against the
  running node with `barrel_server eval` (there is no HTTP-level
  `If-Match`/`If-None-Match` wiring yet, so this is Erlang-API-only, same as
  it is today outside of HTTP): confirms MinIO genuinely rejects a colliding
  write, and confirms Garage refuses fast with
  `conditional_writes_unsupported` rather than silently accepting an
  unprotected write -- Garage cannot enforce this by its own documented
  design, and the whole point of the capability probe in
  `barrel_att_s3_store:open/2` is to fail loudly instead of pretending;
- replicates a document and its attachment from peer-a (RocksDB) to peer-b
  (S3/MinIO) over the real `barrel_rep` HTTP transport, confirming the
  documented source-has-a-feed/target-doesn't asymmetry doesn't stop puts
  from landing.

### attachments-s3-setup.sh

Starts `docker-compose.attachments-s3.yml` (MinIO + Garage + two peer
images) and provisions both stores -- `barrel_att_s3_store:open/2` never
creates a bucket itself, and Garage additionally needs a one-time layout
assignment before it serves any S3 request at all. Used by both
`attachments-s3.sh` and the `s3` CI leg; safe to run on its own for local
iteration against the CT suite:

```console
$ eval "$(test/e2e/attachments-s3-setup.sh)"
$ rebar3 as s3 ct --suite apps/barrel_att_s3/test/barrel_att_s3_SUITE
```

Idempotent for MinIO. Not idempotent for Garage past the first run -- Garage
never reveals a key's secret again after creation, so re-running against an
already-provisioned volume fails loudly rather than silently reusing a key
whose secret is gone. Run `docker compose -f
test/e2e/docker-compose.attachments-s3.yml down -v` first to start clean.

## Requirements

Docker and the Compose plugin. The image builds the `barrel_server` release on
Debian (compiling rocksdb and the vector NIF), so the first run takes a few
minutes; later runs reuse the cached image. `attachments-s3.sh` additionally
pulls the `minio/minio`, `dxflrs/garage`, and `minio/mc` images.
