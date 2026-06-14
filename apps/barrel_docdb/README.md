<div align="center">

# barrel_docdb

**Embeddable document database for Erlang with MVCC, declarative queries, and P2P replication**

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Status](https://img.shields.io/badge/Status-Alpha-orange.svg)]()

[Documentation](https://docs.barrel-db.eu/docdb) |
[Examples](./examples) |
[barrel-db.eu](https://barrel-db.eu)

</div>

---

> **Alpha Software** - API may change. Feedback welcome via [GitHub Issues](https://github.com/barrel-db/barrel_docdb/issues).

## Overview

barrel_docdb provides:

- **Document CRUD** with MVCC revision trees and conflict resolution
- **Declarative Queries** with automatic path indexing
- **Real-time Subscriptions** via MQTT-style path patterns and queries
- **Changes Feed** with long-poll and Server-Sent Events
- **Attachments** with streaming for large binaries
- **HTTP API** with REST endpoints for all operations
- **Peer-to-Peer Replication** (one-shot or continuous) with pluggable transports
- **Prometheus Metrics** for monitoring and alerting
- **HLC Ordering** for distributed event coordination

### Use Cases

- **Edge Computing**: Deploy nodes that sync to cloud when connected
- **Multi-Region**: Replicate data across regions with conflict resolution
- **Offline-First Apps**: Full MVCC for seamless sync
- **Application Backend**: Document storage with queries, changes feed, and attachments

## Quick Start

```erlang
%% Start the application
application:ensure_all_started(barrel_docdb).

%% Create a database
{ok, _} = barrel_docdb:create_db(<<"mydb">>).

%% Save a document
{ok, #{<<"id">> := DocId, <<"rev">> := Rev}} = barrel_docdb:put_doc(<<"mydb">>, #{
    <<"type">> => <<"user">>,
    <<"name">> => <<"Alice">>
}).

%% Fetch the document
{ok, Doc} = barrel_docdb:get_doc(<<"mydb">>, DocId).

%% Query documents
{ok, Users, _Meta} = barrel_docdb:find(<<"mydb">>, #{
    where => [{path, [<<"type">>], <<"user">>}]
}).

%% Subscribe to changes (MQTT-style patterns)
{ok, SubRef} = barrel_docdb:subscribe(<<"mydb">>, <<"type/user/#">>),
receive {barrel_change, _, Change} -> io:format("~p~n", [Change]) end.
```

## HTTP API

The HTTP server starts automatically with the application when
`{http_enabled, true}` is set (the default). Port defaults to 8080;
override via the `http_port` config key or the `BARREL_HTTP_PORT`
environment variable. See [Configuration](#configuration) below.

### Basic Operations

```bash
# Health check
curl http://localhost:8080/health

# Create database
curl -X PUT http://localhost:8080/db/mydb

# Create document with auto-generated ID
curl -X POST http://localhost:8080/db/mydb \
  -H "Content-Type: application/json" \
  -d '{"type": "user", "name": "Alice"}'

# Or create document with specific ID
curl -X PUT http://localhost:8080/db/mydb/doc1 \
  -H "Content-Type: application/json" \
  -d '{"type": "user", "name": "Alice"}'

# Get document
curl http://localhost:8080/db/mydb/doc1

# Query documents
curl -X POST http://localhost:8080/db/mydb/_find \
  -H "Content-Type: application/json" \
  -d '{"where": [{"path": ["type"], "value": "user"}]}'
```

### Changes Feed

```bash
# Poll for changes
curl "http://localhost:8080/db/mydb/_changes?since=first"

# Long-poll (wait for changes)
curl "http://localhost:8080/db/mydb/_changes?feed=longpoll&timeout=30000"

# Server-Sent Events stream
curl http://localhost:8080/db/mydb/_changes/stream
```

### Attachments

```bash
# Put attachment
curl -X PUT http://localhost:8080/db/mydb/doc1/_attachments/photo.jpg \
  -H "Content-Type: image/jpeg" \
  --data-binary @photo.jpg

# Get attachment
curl http://localhost:8080/db/mydb/doc1/_attachments/photo.jpg > photo.jpg
```

## Replication

### Basic Replication

```erlang
%% One-shot replication
{ok, Result} = barrel_rep:replicate(<<"source">>, <<"target">>).

%% With filter (path patterns)
{ok, _} = barrel_rep:replicate(<<"source">>, <<"target">>, #{
    filter => #{paths => [<<"users/#">>]}
}).

%% With query filter
{ok, _} = barrel_rep:replicate(<<"source">>, <<"target">>, #{
    filter => #{query => #{where => [{path, [<<"status">>], <<"active">>}]}}
}).
```

### HTTP Replication

```erlang
%% Replicate to remote node
{ok, _} = barrel_rep:replicate(<<"mydb">>, <<"http://remote:8080/db/mydb">>, #{
    source_transport => barrel_rep_transport_local,
    target_transport => barrel_rep_transport_http
}).
```

## Conflict Resolution

barrel_docdb uses revision trees (CRDT-style) for conflict handling:

```erlang
%% Detect conflicts
case barrel_docdb:get_conflicts(<<"mydb">>, DocId) of
    {ok, []} ->
        %% No conflicts
        ok;
    {ok, Conflicts} ->
        %% Resolve by choosing a winner
        barrel_docdb:resolve_conflict(<<"mydb">>, DocId, WinningRev, choose)
end.
```

## Prometheus Metrics

Metrics are exposed at `/metrics` in Prometheus text format:

```bash
curl http://localhost:8080/metrics
```

Available metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `barrel_doc_operations_total` | Counter | Document operations by db/operation |
| `barrel_doc_operation_duration_seconds` | Histogram | Operation latency |
| `barrel_query_operations_total` | Counter | Query operations by db |
| `barrel_query_duration_seconds` | Histogram | Query latency |
| `barrel_replication_docs_total` | Counter | Documents replicated |
| `barrel_replication_errors_total` | Counter | Replication errors |
| `barrel_http_requests_total` | Counter | HTTP requests by method/path/status |

## Configuration

In your `sys.config`:

```erlang
{barrel_docdb, [
    {data_dir, "data/barrel_docdb"},
    {http_port, 8080},
    {http_enabled, true}
]}.
```

## Requirements

- Erlang/OTP 28 or 29
- RocksDB (via rocksdb hex package)

## Installation

Add to your `rebar.config`:

```erlang
{deps, [
    {barrel_docdb, "0.7.7"}
]}.
```

## Architecture

```
barrel_docdb_sup
├── barrel_metrics         (OpenTelemetry metrics)
├── barrel_cache           (RocksDB block cache)
├── barrel_hlc_clock       (Hybrid Logical Clock)
├── barrel_sub             (Path subscriptions)
├── barrel_query_sub       (Query subscriptions)
├── barrel_path_dict       (Path interning for posting lists)
├── barrel_query_cursor    (Chunked query cursors)
├── barrel_parallel        (Worker pool for parallel queries)
├── barrel_db_sup          (Database supervisor)
│   └── barrel_db_server   (Per-database process)
├── barrel_rep_tasks       (Replication task manager)
├── barrel_http_api_keys   (HTTP API key management)
├── barrel_peer_auth       (HTTP replication auth)
└── barrel_http_server     (HTTP/2 server, when http_enabled=true)
```

## API Reference

### Document Operations

| Function | Description |
|----------|-------------|
| `put_doc/2,3` | Create or update document |
| `get_doc/2,3` | Get document by ID |
| `delete_doc/2,3` | Delete document |
| `find/2,3` | Query documents |
| `fold_docs/3` | Iterate documents |

### Replication

| Function | Description |
|----------|-------------|
| `barrel_rep:replicate/2,3` | One-shot replication |
| `barrel_rep_tasks:start_task/1` | Start/manage a continuous replication task |

### Attachments & Changes

| Function | Description |
|----------|-------------|
| `put_attachment/4`, `get_attachment/3` | Store and read attachments |
| `get_changes/3` | Read the changes feed |
| `subscribe/2`, `subscribe_query/2` | Subscribe to changes |

## Support

| Channel | For |
|---------|-----|
| [GitHub Issues](https://github.com/barrel-db/barrel_docdb/issues) | Bug reports, feature requests |
| [Email](mailto:support@barrel-db.eu) | Commercial inquiries |

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.

---

Built by [Enki Multimedia](https://enki-multimedia.eu) | [barrel-db.eu](https://barrel-db.eu)
