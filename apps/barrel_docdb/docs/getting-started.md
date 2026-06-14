# Getting Started

This guide walks you through installing and using Barrel DocDB. You'll have a working database in under 5 minutes.

## Prerequisites

- **Erlang/OTP 28+** (for embedded use or building from source)
- **Docker** (optional, for standalone deployment)

## Installation

=== "Docker (Standalone)"

    The fastest way to run Barrel DocDB as a standalone server:

    ```bash
    docker run -d \
      --name barrel \
      -p 8080:8080 \
      -v barrel_data:/data \
      ghcr.io/barrel-db/barrel_docdb:latest
    ```

    Verify it's running:

    ```bash
    curl http://localhost:8080/health
    ```

=== "Erlang (Embedded)"

    Add to your `rebar.config`:

    ```erlang
    {deps, [
        {barrel_docdb, "0.7.7"}
    ]}.
    ```

    Then fetch dependencies:

    ```bash
    rebar3 get-deps
    rebar3 compile
    ```

=== "Build from Source"

    Clone and build:

    ```bash
    git clone https://github.com/barrel-db/barrel_docdb.git
    cd barrel_docdb
    rebar3 compile
    rebar3 shell
    ```

## Your First Database

=== "HTTP API"

    ### Create a Database

    ```bash
    curl -X PUT http://localhost:8080/db/myapp
    ```

    Response:

    ```json
    {"name": "myapp", "doc_count": 0}
    ```

    ### Store a Document

    Create a document with an auto-generated ID:

    ```bash
    curl -X POST http://localhost:8080/db/myapp \
      -H "Content-Type: application/json" \
      -d '{
        "type": "user",
        "name": "Alice",
        "email": "alice@example.com",
        "role": "admin"
      }'
    ```

    Response:

    ```json
    {"id": "d7f3e2a1...", "rev": "1-abc123..."}
    ```

    Or store with a specific ID:

    ```bash
    curl -X PUT http://localhost:8080/db/myapp/user-alice \
      -H "Content-Type: application/json" \
      -d '{
        "type": "user",
        "name": "Alice",
        "email": "alice@example.com"
      }'
    ```

    ### Retrieve a Document

    ```bash
    curl http://localhost:8080/db/myapp/user-alice
    ```

    Response:

    ```json
    {
      "id": "user-alice",
      "_rev": "1-abc123...",
      "type": "user",
      "name": "Alice",
      "email": "alice@example.com"
    }
    ```

    ### Update a Document

    Include the `_rev` field from the previous response:

    ```bash
    curl -X PUT http://localhost:8080/db/myapp/user-alice \
      -H "Content-Type: application/json" \
      -d '{
        "_rev": "1-abc123...",
        "type": "user",
        "name": "Alice Smith",
        "email": "alice@example.com",
        "role": "admin"
      }'
    ```

    ### Delete a Document

    ```bash
    curl -X DELETE "http://localhost:8080/db/myapp/user-alice?rev=2-def456..."
    ```

=== "Erlang API"

    ### Start the Application

    ```erlang
    application:ensure_all_started(barrel_docdb).
    ```

    ### Create a Database

    ```erlang
    {ok, _Info} = barrel_docdb:create_db(<<"myapp">>).
    ```

    ### Store a Document

    With auto-generated ID:

    ```erlang
    {ok, #{<<"id">> := DocId, <<"rev">> := Rev}} =
        barrel_docdb:put_doc(<<"myapp">>, #{
            <<"type">> => <<"user">>,
            <<"name">> => <<"Alice">>,
            <<"email">> => <<"alice@example.com">>
        }).
    ```

    With specific ID:

    ```erlang
    {ok, #{<<"rev">> := Rev}} =
        barrel_docdb:put_doc(<<"myapp">>, #{
            <<"id">> => <<"user-alice">>,
            <<"type">> => <<"user">>,
            <<"name">> => <<"Alice">>
        }).
    ```

    ### Retrieve a Document

    ```erlang
    {ok, Doc} = barrel_docdb:get_doc(<<"myapp">>, <<"user-alice">>).
    %% Doc = #{<<"id">> => <<"user-alice">>, <<"_rev">> => <<"1-...">>, ...}
    ```

    ### Update a Document

    ```erlang
    {ok, #{<<"rev">> := NewRev}} =
        barrel_docdb:put_doc(<<"myapp">>, Doc#{
            <<"name">> => <<"Alice Smith">>,
            <<"role">> => <<"admin">>
        }).
    ```

    ### Delete a Document

    ```erlang
    {ok, _} = barrel_docdb:delete_doc(<<"myapp">>, <<"user-alice">>, #{rev => Rev}).
    ```

## Querying Documents

Barrel DocDB provides declarative queries with automatic path indexing. No need to create indexes manually.

=== "HTTP API"

    Find all users:

    ```bash
    curl -X POST http://localhost:8080/db/myapp/_find \
      -H "Content-Type: application/json" \
      -d '{
        "where": [{"path": ["type"], "value": "user"}]
      }'
    ```

    Find admins:

    ```bash
    curl -X POST http://localhost:8080/db/myapp/_find \
      -H "Content-Type: application/json" \
      -d '{
        "where": [
          {"path": ["type"], "value": "user"},
          {"path": ["role"], "value": "admin"}
        ]
      }'
    ```

    With pagination:

    ```bash
    curl -X POST http://localhost:8080/db/myapp/_find \
      -H "Content-Type: application/json" \
      -d '{
        "where": [{"path": ["type"], "value": "user"}],
        "limit": 10,
        "offset": 0
      }'
    ```

=== "Erlang API"

    Find all users:

    ```erlang
    {ok, Users, _Meta} = barrel_docdb:find(<<"myapp">>, #{
        where => [{path, [<<"type">>], <<"user">>}]
    }).
    ```

    Find admins:

    ```erlang
    {ok, Admins, _Meta} = barrel_docdb:find(<<"myapp">>, #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {path, [<<"role">>], <<"admin">>}
        ]
    }).
    ```

    With pagination:

    ```erlang
    {ok, Results, #{total := Total}} = barrel_docdb:find(<<"myapp">>, #{
        where => [{path, [<<"type">>], <<"user">>}],
        limit => 10,
        offset => 0
    }).
    ```

## Watching for Changes

Subscribe to real-time changes using MQTT-style path patterns.

=== "HTTP API (SSE)"

    Stream all changes:

    ```bash
    curl http://localhost:8080/db/myapp/_changes/stream
    ```

    Long-poll for changes:

    ```bash
    curl "http://localhost:8080/db/myapp/_changes?feed=longpoll&timeout=30000"
    ```

    Get changes since a specific point:

    ```bash
    curl "http://localhost:8080/db/myapp/_changes?since=5"
    ```

=== "Erlang API"

    Subscribe to all changes:

    ```erlang
    {ok, _SubRef} = barrel_docdb:subscribe(<<"myapp">>, <<"#">>),
    receive
        {barrel_change, <<"myapp">>, Change} ->
            io:format("Change: ~p~n", [Change])
    end.
    ```

    Subscribe to user changes only:

    ```erlang
    {ok, SubRef} = barrel_docdb:subscribe(<<"myapp">>, <<"type/user/#">>).
    ```

    Unsubscribe:

    ```erlang
    barrel_docdb:unsubscribe(SubRef).
    ```

## Working with Attachments

Store binary files (images, documents, etc.) alongside your documents.

=== "HTTP API"

    Store an attachment:

    ```bash
    curl -X PUT http://localhost:8080/db/myapp/user-alice/_attachments/photo.jpg \
      -H "Content-Type: image/jpeg" \
      --data-binary @photo.jpg
    ```

    Retrieve an attachment:

    ```bash
    curl http://localhost:8080/db/myapp/user-alice/_attachments/photo.jpg > photo.jpg
    ```

    Delete an attachment:

    ```bash
    curl -X DELETE http://localhost:8080/db/myapp/user-alice/_attachments/photo.jpg
    ```

=== "Erlang API"

    Store an attachment:

    ```erlang
    Data = <<"Hello, World!">>,
    {ok, _} = barrel_docdb:put_attachment(<<"myapp">>, <<"user-alice">>,
        <<"greeting.txt">>, Data).
    ```

    Retrieve an attachment:

    ```erlang
    {ok, Binary} = barrel_docdb:get_attachment(<<"myapp">>, <<"user-alice">>,
        <<"greeting.txt">>).
    ```

    List attachments:

    ```erlang
    AttNames = barrel_docdb:list_attachments(<<"myapp">>, <<"user-alice">>).
    %% Returns [<<"greeting.txt">>]
    ```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BARREL_DATA_DIR` | `data/barrel_docdb` | Data directory |
| `BARREL_HTTP_PORT` | `8080` | HTTP server port |
| `BARREL_HTTP_ENABLED` | `true` | Enable HTTP server |

### Erlang Configuration

In `sys.config`:

```erlang
[
  {barrel_docdb, [
    {data_dir, "data/barrel_docdb"},
    {http_port, 8080},
    {http_enabled, true}
  ]}
].
```

## Next Steps

Now that you have Barrel DocDB running, explore these features:

<div class="grid cards" markdown>

-   :mag: **[Queries](queries.md)**

    ---

    Advanced query syntax, operators, and filtering

-   :zap: **[Changes Feed](changes.md)**

    ---

    Real-time subscriptions and MQTT-style patterns

-   :arrows_counterclockwise: **[Replication](replication.md)**

    ---

    Sync data between nodes with filtering

-   :books: **[HTTP API Reference](api/http.md)**

    ---

    Complete endpoint documentation

</div>

## Troubleshooting

!!! warning "Document Update Fails with Conflict"

    If you get a conflict error when updating, ensure you're including the current `_rev` value from the document. Barrel uses MVCC (Multi-Version Concurrency Control) to prevent lost updates.

    ```bash
    # Get the current revision first
    curl http://localhost:8080/db/myapp/doc1

    # Then include _rev in your update
    curl -X PUT http://localhost:8080/db/myapp/doc1 \
      -H "Content-Type: application/json" \
      -d '{"_rev": "1-abc...", "name": "updated"}'
    ```

!!! warning "Connection Refused"

    Check that the HTTP server is running:

    ```bash
    curl http://localhost:8080/health
    ```

    If using Docker, verify the container is running:

    ```bash
    docker ps | grep barrel
    ```

!!! tip "Enable HTTP/2 for Better Performance"

    For production deployments, enable HTTPS with HTTP/2:

    ```bash
    export BARREL_HTTP_TLS_ENABLED=true
    export BARREL_HTTP_CERTFILE=/path/to/cert.pem
    export BARREL_HTTP_KEYFILE=/path/to/key.pem
    ```
