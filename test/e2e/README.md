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
- deletes a document on peer-a and re-pushes, asserting the delete propagates.

Exit 0 means every assertion passed. The script tears the stack down on exit.

Replication is triggered inside a peer with `barrel_server eval`, which evaluates
against the running node, so it runs the real `barrel_rep` algorithm against the
other peer's `_sync` endpoints.

## Requirements

Docker and the Compose plugin. The image builds the `barrel_server` release on
Debian (compiling rocksdb and the vector NIF), so the first run takes a few
minutes; later runs reuse the cached image.
