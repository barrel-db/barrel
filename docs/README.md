# Barrel documentation

Start here.

- [Features](features.md): what Barrel does today, what is opt-in, what is planned.

## Guides

- [Embed barrel in an Erlang app](guides/embedding.md): the `barrel` facade.
- [Record mode](guides/record-mode.md): policy-driven vector indexing over documents.
- [Query with BQL](guides/query-bql.md): one query language over documents, vectors, and keyword search.
- [Run the REST server](guides/rest-server.md): `barrel_server` over HTTP.
- [Synchronization](guides/synchronization.md): replication between databases.
- [Timeline](guides/timeline.md): branch a database, work in isolation, merge back (incl. PITR).
- [Encryption at rest](guides/encryption.md): per-database keys over every store and index file.
- [Audit and provenance](guides/audit-provenance.md): who wrote what, when, and past versions.
- [Spaces, sessions, and handoffs](guides/spaces.md): the agent layer with capability tokens.
- [MCP endpoint](guides/mcp.md): tools, resources, and live queries for MCP clients.
- [barrel-lite (browser client)](guides/barrel-lite.md): an offline-first TypeScript client that syncs over the wire.
- [Distribute the umbrella apps](guides/distributing-apps.md): Hex packages, git mirrors, and consuming an app elsewhere.

## Architecture

- [Vision](architecture/vision.md): what barrel is becoming (the database for agents) and the settled decisions.
- [Overview](architecture/overview.md): how the umbrella is split and why.
- [Responsibilities](migration/responsibilities.md): per-app responsibilities.
