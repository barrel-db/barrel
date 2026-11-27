# Barrel documentation

Start here.

- [Features](features.md): what Barrel does today, what is opt-in, what is planned.

## Guides

- [Embed barrel in an Erlang app](guides/embedding.md): the `barrel` API.
- [Record mode](guides/record-mode.md): policy-driven vector indexing over documents.
- [Query with BQL](guides/query-bql.md): one query language over documents, vectors, and keyword search.
- [Run the REST server](guides/rest-server.md): `barrel_server` over HTTP.
- [Run barrel in Docker](guides/docker.md): the `barrel-server` image, its settings and your own config.
- [Embed the barrel server in a livery app](guides/embedding-barrel-server.md): mount barrel's routes into a host livery service, under a sub-path.
- [Synchronization](guides/synchronization.md): replication between databases.
- [Replicate inside an Erlang cluster](guides/cluster-replication.md): replication over distributed Erlang, TCP or QUIC.
- [Back up and restore](guides/backup.md): online backups, restores, and the `barrel-backup` command.
- [Writes while a database restarts](guides/write-availability.md): the errors a write returns and how to retry it.
- [Timeline](guides/timeline.md): branch a database, work in isolation, merge back (incl. PITR).
- [Encryption at rest](guides/encryption.md): per-database keys over every store and index file.
- [Audit and provenance](guides/audit-provenance.md): who wrote what, when, and past versions.
- [Spaces, sessions, and handoffs](guides/spaces.md): the agent layer with capability tokens.
- [Contexts](guides/contexts.md): query named datasets on several nodes together, keep working sets, work offline.
- [MCP endpoint](guides/mcp.md): tools, resources, and live queries for MCP clients.
- [barrel-lite (browser client)](guides/barrel-lite.md): an offline-first TypeScript client that syncs over the wire.
- [Distribute the umbrella apps](guides/distributing-apps.md): Hex packages, git mirrors, and consuming an app elsewhere.
- [Releases](guides/releases.md): the Barrel bundle `YYYY.MM.N`, its app versions, tarballs and image.

## Architecture

- [Vision](architecture/vision.md): what barrel is becoming (the database for agents) and the settled decisions.
- [Contexts](architecture/contexts.md): the design of contexts (identity, cards, manifests, working sets, query contract, merge rules, budgets, permissions) and the measured decisions behind it.
- [Overview](architecture/overview.md): how the umbrella is split and why.
- [Barrel design templates and WebAssembly views](architecture/wasm-views.md) proposes `barrel_design` for Notes-style templates and automatically maintained views with on-demand refresh. [Implementation prompt](architecture/barrel-design-implementation-prompt.md) · [Measured indexing performance](architecture/wasm-views.md#measured-invoice-indexing-performance) · [Runnable receivables example](architecture/examples/receivables/README.md)
- [Responsibilities](migration/responsibilities.md): per-app responsibilities.

## Proofs

- [Replication proofs](architecture/replication-proofs.md): the properties document replication relies on, and why each holds.
- [Machine-checked proofs](architecture/proofs/README.md): the TLA+ model (`proofs/tla/DocState.tla`), Ivy and Lean checks of those properties, with negative controls, and how to run them.
- [Spaces layout model](../apps/barrel_spaces/spec/SpacesLayout.tla): a TLA+ model of barrel_spaces documents replicated on several nodes; run it with `apps/barrel_spaces/scripts/check-tla.sh`.
