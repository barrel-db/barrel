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

## Active-active replication

`active-active.sh` brings up three peers that all take writes and checks
that they converge without losing a write:

```console
$ test/e2e/active-active.sh
```

It covers concurrent edits on two peers, nine edits over the winner (the
losing write stays), a delete against an edit, resolving over HTTP with
`_observed`, a new empty peer joining, a peer that stops and returns with
its own edit (learning the others' through a relay), two peers resolving
at once, a filtered replication carrying a delete, and a continuous task in
both directions with concurrent writes. Each scenario compares the
document state digest (`_sync/state`) on every peer.

## Mixed versions

`mixed-version.sh` builds a peer from an older release (`OLD_REF`, default
`barrel_server-v1.11.1`, the last release before whole-state replication)
and replicates it with two upgraded peers, both ways, with the replicator on
either side:

```console
$ test/e2e/mixed-version.sh
```

The old peer receives each document's winner. Its edits and deletes reach
upgraded peers as new versions next to the ones they replaced, so they show
as conflicts there and nothing is lost; a resolution made on an upgraded
peer reaches the old peer and is not undone by it.

## Client vectors

`client-vectors.sh` checks that the vector you supply with a document stays
with its version across three peers and a peer on the previous release
(`OLD_REF`, default `barrel_server-v1.14.2`; `OLD_PORT`, default 8094, is its
host port):

```console
$ test/e2e/client-vectors.sh
```

Two peers write the same document with their own vectors. Every peer keeps
the winner's vector and the losing version's, a resolution back to the losing
version reaches every peer with its vector, and a resolution sent with
`_observed` and `_embedding` carries its own. A version relayed through the
old peer loses its vector there and gets it back on the next direct sync from
an upgraded peer.

## Crashes

`crash.sh` kills peers with SIGKILL during replication:

```console
$ test/e2e/crash.sh
```

It kills the target of a continuous push mid-batch, then the peer running
the task (acknowledged writes must survive and the restored task must
finish), then one of two peers writing the same ids while a two-way task
runs. Every acknowledged write must be on both peers once they converge.

## Power cuts

`power-fail.sh` puts each peer's data directory on
[LazyFS](https://github.com/dsrhaslab/lazyfs), a FUSE file system that keeps
every write that was not fsynced in its own memory. A power cut kills the
server, drops that memory and restarts the container, so what was not
synced is gone, as after a real power loss. The containers run privileged
(FUSE).

```console
$ test/e2e/power-fail.sh
```

It checks the harness first (an unsynced file loses its data, a synced one
keeps it), then: writes made with `sync => true` survive; a target that
loses a batch it acknowledged gets it again, from a one-shot run and from a
continuous task; and a peer that loses writes which had already reached the
other peer gets them back through a two-way task.

## S3 attachment backend

`attachments-s3.sh` brings up real RustFS and Garage containers plus two
`barrel_server` peers built with the S3 attachment backend included (the
`s3_server` rebar3 profile), and exercises it over real HTTP routes and a
real network -- not the CT suite against the backend module directly (that's
`apps/barrel_att_s3/test/barrel_att_s3_SUITE.erl`).

```console
$ test/e2e/attachments-s3.sh
```

peer-a keeps the default (RocksDB) attachment backend; peer-b's databases
point their `att_opts` at RustFS or Garage. It:

- provisions RustFS and Garage (`attachments-s3-setup.sh` -- see below), then
  round-trips a small and a large (multipart-crossing) attachment against
  each store, and confirms a delete actually 404s afterward;
- exercises `create_only` write-conflict detection directly against the
  running node with `barrel_server eval` (there is no HTTP-level
  `If-Match`/`If-None-Match` wiring yet, so this is Erlang-API-only, same as
  it is today outside of HTTP): confirms RustFS genuinely rejects a colliding
  write, and confirms Garage refuses fast with
  `conditional_writes_unsupported` rather than silently accepting an
  unprotected write -- Garage cannot enforce this by its own documented
  design, and the whole point of the capability probe in
  `barrel_att_s3_store:open/2` is to fail loudly instead of pretending;
- replicates a document and its attachment from peer-a (RocksDB) to peer-b
  (S3/RustFS) over the real `barrel_rep` HTTP transport, confirming the
  documented source-has-a-feed/target-doesn't asymmetry doesn't stop puts
  from landing.

### attachments-s3-setup.sh

Starts `docker-compose.attachments-s3.yml` (RustFS + Garage + two peer
images) and provisions both stores -- `barrel_att_s3_store:open/2` never
creates a bucket itself, and Garage additionally needs a one-time layout
assignment before it serves any S3 request at all. Used by both
`attachments-s3.sh` and the `s3` CI leg; safe to run on its own for local
iteration against the CT suite:

```console
$ eval "$(test/e2e/attachments-s3-setup.sh)"
$ rebar3 as s3 ct --suite apps/barrel_att_s3/test/barrel_att_s3_SUITE
```

Idempotent for RustFS. Not idempotent for Garage past the first run -- Garage
never reveals a key's secret again after creation, so re-running against an
already-provisioned volume fails loudly rather than silently reusing a key
whose secret is gone. Run `docker compose -f
test/e2e/docker-compose.attachments-s3.yml down -v` first to start clean.

## Requirements

Docker and the Compose plugin. The image builds the `barrel_server` release on
Debian (compiling rocksdb and the vector NIF), so the first run takes a few
minutes; later runs reuse the cached image. `attachments-s3.sh` additionally
pulls the pinned `rustfs/rustfs` and `dxflrs/garage` images; the setup script
exits non-zero, printing nothing, if a pull or provisioning step fails.
