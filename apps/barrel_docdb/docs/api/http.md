# HTTP API

barrel_docdb is embedded-only: it has no built-in HTTP server. Use it as an
Erlang library through the [`barrel_docdb`](erlang.md) module.

For a REST/JSON (and MCP) surface over the same data, run `barrel_server`,
which exposes databases, documents, BQL queries, search, the timeline, the
agent layer, and the replication `_sync` endpoints over HTTP. See:

- The `barrel_server` REST guide: `docs/guides/rest-server.md` in the umbrella.
- Replication over the wire (`/db/:db/_sync/*`): [replication.md](../replication.md)
  and `docs/guides/synchronization.md`.
