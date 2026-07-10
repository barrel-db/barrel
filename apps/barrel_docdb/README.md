<div align="center">

# barrel_docdb

**Embeddable document database for Erlang with version-vector MVCC, declarative queries, and P2P replication**

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Status](https://img.shields.io/badge/Status-Alpha-orange.svg)]()

[Documentation](https://barrel-db.eu/docs/lib/docdb/) |
[barrel-db.eu](https://barrel-db.eu)

</div>

---

> **Alpha Software** - API may change. Feedback welcome via [GitHub Issues](https://github.com/barrel-db/barrel/issues).

## Overview

barrel_docdb provides:

- **Document CRUD** with HLC version-vector MVCC and conflict resolution
- **Declarative Queries** (BQL) with automatic path indexing
- **Real-time Subscriptions** via MQTT-style path patterns and queries
- **Changes Feed** with a retained history log
- **Attachments** with streaming for large binaries
- **Timeline**: branch, point-in-time restore, and merge
- **Peer-to-Peer Replication** (one-shot or continuous) with pluggable transports
- **TTL and retention** for expiring documents and bounding history
- **HLC Ordering** for distributed event coordination

It is embeddable and transport-free. For an HTTP (REST/JSON) or MCP surface over
the same data, use `barrel_server`.

## Quick Start

```erlang
%% Start the application
application:ensure_all_started(barrel_docdb).

%% Create a database
{ok, _} = barrel_docdb:create_db(<<"mydb">>).

%% Save a document
{ok, #{<<"id">> := DocId, <<"rev">> := Rev}} = barrel_docdb:put_doc(<<"mydb">>, #{
    <<"id">> => <<"doc1">>,
    <<"type">> => <<"user">>,
    <<"name">> => <<"Alice">>
}).

%% Fetch the document
{ok, Doc} = barrel_docdb:get_doc(<<"mydb">>, DocId).

%% Query documents (BQL)
{ok, Users, _Meta} = barrel_docdb:query(<<"mydb">>,
    <<"SELECT * FROM db WHERE type = 'user'">>).

%% Subscribe to changes (MQTT-style patterns)
{ok, SubRef} = barrel_docdb:subscribe(<<"mydb">>, <<"type/user/#">>),
receive {barrel_change, _, Change} -> io:format("~p~n", [Change]) end.
```

`_rev` is the concurrency token. It is HLC-backed (format `<hex(hlc)>@<author>`),
not a revision tree: conflicts are resolved by HLC last-write-wins with the
superseded version retained and an optional merge hook, tracked with version
vectors (`barrel_version` / `barrel_vv`).

## Attachments

```erlang
{ok, _} = barrel_docdb:put_attachment(<<"mydb">>, <<"doc1">>, <<"photo.jpg">>, Bytes).
{ok, Info, Reader} = barrel_docdb:open_attachment_stream(<<"mydb">>, <<"doc1">>, <<"photo.jpg">>).
```

Blobs are content-addressed, stream in chunks, and ride their own feed for
replication.

## Replication

```erlang
%% One-shot replication (same VM)
{ok, Result} = barrel_rep:replicate(<<"source">>, <<"target">>).

%% With a filter (path patterns)
{ok, _} = barrel_rep:replicate(<<"source">>, <<"target">>, #{
    filter => #{paths => [<<"users/#">>]}
}).

%% To a remote node over HTTP (via barrel_server's _sync endpoints)
{ok, _} = barrel_rep:replicate(<<"mydb">>, <<"http://remote:8080/db/mydb">>, #{
    source_transport => barrel_rep_transport_local,
    target_transport => barrel_rep_transport_http
}).
```

Replication diffs by version-vector containment and applies each remote version
as skip / fast-forward / last-write-wins, keeping the loser as a retained
conflict sibling.

## Conflict Resolution

Conflicts are HLC last-write-wins with the superseded version retained, so a
concurrent write leaves a live conflict you can inspect and resolve:

```erlang
case barrel_docdb:get_conflicts(<<"mydb">>, DocId) of
    {ok, []} ->
        ok;
    {ok, Conflicts} ->
        barrel_docdb:resolve_conflict(<<"mydb">>, DocId, WinningRev, choose)
end.
```

## Timeline

```erlang
{ok, _} = barrel_docdb:branch_db(<<"mydb">>, <<"mydb_wip">>, #{}).
{ok, _} = barrel_docdb:merge_branch(<<"mydb_wip">>, <<"mydb">>).
```

Branch forks at now (or a past HLC for point-in-time restore); merge is a
one-shot replication since the fork point.

## Configuration

In your `sys.config`:

```erlang
{barrel_docdb, [
    {data_dir, "data/barrel_docdb"}
]}.
```

## Requirements

- Erlang/OTP 28 or 29
- RocksDB (via the `rocksdb` hex package)

## Installation

Add to your `rebar.config`:

```erlang
{deps, [
    {barrel_docdb, "~> 1.0"}
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
└── barrel_rep_tasks       (Replication task manager)
```

## API Reference

### Document Operations

| Function | Description |
|----------|-------------|
| `put_doc/2,3`, `put_docs/2,3` | Create or update documents |
| `get_doc/2,3`, `get_docs/2,3` | Get documents by ID |
| `delete_doc/2,3`, `delete_docs/2` | Delete documents |
| `query/2,3`, `find/2,3` | Query documents (BQL / find) |
| `fold_docs/3,4` | Iterate documents |
| `get_conflicts/2`, `resolve_conflict/4` | Inspect and resolve conflicts |

### Timeline & Replication

| Function | Description |
|----------|-------------|
| `branch_db/3`, `merge_branch/2`, `list_branches/1` | Branch / merge / PITR |
| `barrel_rep:replicate/2,3` | One-shot replication |
| `barrel_rep_tasks:start_task/1` | Start/manage a continuous replication task |

### Attachments & Changes

| Function | Description |
|----------|-------------|
| `put_attachment/4,5`, `get_attachment/3` | Store and read attachments |
| `open_attachment_stream/3`, `open_attachment_writer/4,5` | Streamed attachments |
| `get_changes/3` | Read the changes feed |
| `subscribe/2`, `subscribe_query/2` | Subscribe to changes |

## Support

| Channel | For |
|---------|-----|
| [GitHub Issues](https://github.com/barrel-db/barrel/issues) | Bug reports, feature requests |
| [Email](mailto:support@barrel-db.eu) | Commercial inquiries |

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.

---

Built by [Enki Multimedia](https://enki-multimedia.eu) | [barrel-db.eu](https://barrel-db.eu)
