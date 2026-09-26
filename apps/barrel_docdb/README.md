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

## Batch writes with per-document options

Concurrent writes to one database are committed together: one `write_batch`
and at most one sync per group, up to `max_group` requests (default 256).
Each request keeps its own conflict check and answer. In `put_docs`, give a
document its own `outbox` tags and `sync` flag with a `{Doc, DocOpts}` entry:

```erlang
[{ok, _}, {ok, _}] = barrel_docdb:put_docs(<<"mydb">>, [
    {Block, #{outbox => [<<"blocks">>], sync => true}},
    Record
], #{}).
```

The call's documents share one batch, synced when the call or any document
asks for `sync`. Another key in `DocOpts` answers
`{error, {invalid_doc_opts, DocOpts}}` for that document. Tune the writer
with the `max_group` and `write_chunk` (default 16) database options; see
[design](docs/design.md#write-serialization-and-group-commit).

## Open a copy read only

Pass `read_only => true` on every open to serve a copy (an export, an
imported snapshot) without changing its files:

```erlang
{ok, _} = barrel_docdb:create_db(<<"snapshot">>, #{
    data_dir => "/srv/imports", read_only => true
}).
{error, read_only} = barrel_docdb:put_doc(<<"snapshot">>, #{<<"id">> => <<"x">>}).
```

- The stores open with RocksDB `OpenForReadOnly`: open, reads and close
  write no file, and several nodes can open the same directory.
- A missing store fails with `{read_only_store_missing, Path}`; nothing is
  created.
- A store an older version wrote, without a column family added since,
  fails with `{read_only_upgrade_needed, #{store, missing_cfs}}` until one
  writable open adds it.
- `barrel_docdb:db_exists(Name, #{data_dir => Dir})` tells you whether a
  database is open or has files, without creating it.

## Know which state answered

`db_observed_version/1` returns the database's instance id and the HLC of
its last write, and BQL results carry the same two fields in their meta:

```erlang
{ok, #{instance_id := Id, last_seq := Seq}} =
    barrel_docdb:db_observed_version(<<"mydb">>).
```

It is a freshness hint read after the rows, not a snapshot.

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
    {barrel_docdb, "~> 1.7"}
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
| `db_exists/2`, `db_info/1`, `db_observed_version/1` | Database presence, stats, instance id and last write HLC |

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
