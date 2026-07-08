# Barrel DocDB

**A document database built in Erlang. Embed it in your app or run it standalone over HTTP.**

Sync anywhere. Query anything. Your data, wherever you need it.

<div class="grid cards" markdown>

-   :rocket: __Quick Start__

    ---

    Get up and running in 5 minutes with our step-by-step guide

    [:octicons-arrow-right-24: Quick Start Guide](getting-started.md)

-   :mag: __Declarative Queries__

    ---

    Query documents with automatic path indexing and powerful filters

    [:octicons-arrow-right-24: Query Guide](queries.md)

-   :zap: __Real-time Changes__

    ---

    Subscribe to changes with MQTT-style path patterns

    [:octicons-arrow-right-24: Changes Feed](changes.md)

-   :arrows_counterclockwise: __P2P Replication__

    ---

    One-shot or continuous replication with pluggable transports

    [:octicons-arrow-right-24: Replication](replication.md)

-   :books: __Advanced Features__

    ---

    Practical curl examples for replication, attachments, and the changes feed

    [:octicons-arrow-right-24: Advanced Guide](advanced-features.md)

-   :chart_with_upwards_trend: __Observability__

    ---

    Distributed tracing, metrics, and logging with OpenTelemetry support

    [:octicons-arrow-right-24: Observability Guide](observability.md)

</div>

## What is Barrel DocDB?

Barrel DocDB is a production-ready document database built on Erlang/OTP that provides:

- **Document CRUD** with HLC version-vector MVCC and last-write-wins conflict handling
- **Declarative Queries** with automatic path indexing - no manual index creation needed
- **Real-time Subscriptions** via MQTT-style path patterns and query subscriptions
- **Peer-to-Peer Replication** (one-shot or continuous) with pluggable transports
- **Changes Feed** with long-poll and Server-Sent Events
- **Attachments** with streaming for large binaries
- **HTTP API** with REST endpoints, SSE streaming, and Prometheus metrics

## Why Barrel DocDB?

### Use Cases

- **Edge Computing**: Deploy nodes that sync to cloud when connected
- **Multi-Region**: Replicate data across regions with automatic conflict resolution
- **Tiered Caching**: Hot/warm/cold data tiers with automatic migration
- **Event Distribution**: Fan-out patterns for event streaming architectures
- **Offline-First Apps**: Full MVCC support for seamless sync when back online

## Quick Example

Barrel DocDB is **embedded** directly into your Erlang/OTP application. For an
HTTP (REST/JSON) surface over the same data, run `barrel_server`.

=== "Embedded (Erlang API)"

    Embed Barrel directly into your Erlang/OTP application for maximum performance:

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

## Core Features

### :brain: Documents with MVCC

Every write is an HLC version and every document carries a version vector. Replication diffs by vector containment and resolves concurrent writes with a deterministic last-write-wins winner, retaining the losing versions as conflict siblings. No silent data loss, even in distributed scenarios.

### :mag: Declarative Queries

Query documents using path-based conditions with automatic indexing. No need to create indexes manually - Barrel handles it for you.

```erlang
{ok, Results, _} = barrel_docdb:find(<<"mydb">>, #{
    where => [
        {path, [<<"status">>], <<"active">>},
        {'and', [
            {path, [<<"age">>], '>=', 18},
            {path, [<<"role">>], <<"admin">>}
        ]}
    ],
    limit => 100
}).
```

## Get Started

<div class="grid cards" markdown>

-   :material-clock-fast: __5 minutes__

    ---

    Add to your Erlang project and start storing documents

    ```erlang
    {deps, [
        {barrel_docdb, "~> 0.9"}
    ]}.
    ```

-   :material-api: __HTTP surface__

    ---

    Run `barrel_server` for a REST/JSON and MCP API over the same data

    ```erlang
    {deps, [{barrel_server, "~> 0.2"}]}.
    ```

</div>

## Community & Support

- [:fontawesome-brands-github: GitHub Repository](https://github.com/barrel-db/barrel)
- [:material-file-document: API Reference](api/erlang.md)
- [:material-bug: Report Issues](https://github.com/barrel-db/barrel/issues)
