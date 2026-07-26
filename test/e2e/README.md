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

## Requirements

Docker and the Compose plugin. The image builds the `barrel_server` release on
Debian (compiling rocksdb and the vector NIF), so the first run takes a few
minutes; later runs reuse the cached image.
