# Getting Started

This guide walks you through installing and using Barrel DocDB from Erlang. barrel_docdb is an embedded document store: you call it in-process through the `barrel_docdb` module. You'll have a working database in under 5 minutes.

!!! note "HTTP access"
    barrel_docdb has no built-in HTTP server. To reach a database over the wire (REST/JSON), run the `barrel_server` app, which exposes barrel_docdb over HTTP. See the umbrella REST server guide (`docs/guides/rest-server.md`).

## Prerequisites

- **Erlang/OTP 28+**

## Installation

=== "Erlang (Embedded)"

    Add to your `rebar.config`:

    ```erlang
    {deps, [
        {barrel_docdb, "~> 0.9"}
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
    git clone https://github.com/barrel-db/barrel.git
    cd barrel
    rebar3 compile
    rebar3 shell
    ```

## Your First Database

### Start the Application

```erlang
application:ensure_all_started(barrel_docdb).
```

### Create a Database

```erlang
{ok, _Db} = barrel_docdb:create_db(<<"myapp">>).
```

### Store a Document

With an auto-generated ID:

```erlang
{ok, #{<<"id">> := DocId, <<"rev">> := Rev}} =
    barrel_docdb:put_doc(<<"myapp">>, #{
        <<"type">> => <<"user">>,
        <<"name">> => <<"Alice">>,
        <<"email">> => <<"alice@example.com">>
    }).
```

With a specific ID:

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

Include the current `<<"_rev">>` value from the document you fetched:

```erlang
{ok, #{<<"rev">> := NewRev}} =
    barrel_docdb:put_doc(<<"myapp">>, Doc#{
        <<"name">> => <<"Alice Smith">>,
        <<"role">> => <<"admin">>
    }).
```

### Delete a Document

Pass the current revision in the options:

```erlang
{ok, _} = barrel_docdb:delete_doc(<<"myapp">>, <<"user-alice">>, #{rev => NewRev}).
```

## Querying Documents

Barrel DocDB provides declarative queries with automatic path indexing. No need to create indexes manually.

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

With pagination. `Meta` carries `has_more` and, when there is more, a `continuation` token to pass back:

```erlang
{ok, Page, Meta} = barrel_docdb:find(<<"myapp">>, #{
    where => [{path, [<<"type">>], <<"user">>}],
    limit => 10
}),
case Meta of
    #{has_more := true, continuation := Token} ->
        {ok, More, _} = barrel_docdb:find(<<"myapp">>,
            #{where => [{path, [<<"type">>], <<"user">>}], limit => 10},
            #{continuation => Token});
    #{has_more := false} ->
        ok
end.
```

You can also run BQL text with `barrel_docdb:query/2,3`:

```erlang
{ok, Rows, _Meta} = barrel_docdb:query(<<"myapp">>,
    <<"SELECT name FROM db WHERE type = 'user' LIMIT 10">>).
```

## Watching for Changes

Subscribe to real-time changes using MQTT-style path patterns. Notifications are delivered to the calling process as messages.

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

To read changes since a point instead of subscribing, use `barrel_docdb:get_changes/2,3`. See [Changes Feed](changes.md).

## Working with Attachments

Store binary files (images, documents, etc.) alongside your documents.

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

For large files, stream with `open_attachment_stream/3` and `read_attachment_chunk/1`.

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BARREL_DATA_DIR` | `data/barrel_docdb` | Data directory |

### Erlang Configuration

In `sys.config`:

```erlang
[
  {barrel_docdb, [
    {data_dir, "data/barrel_docdb"}
  ]}
].
```

HTTP listener settings (port, TLS) live in `barrel_server`, not barrel_docdb. See the umbrella REST server guide (`docs/guides/rest-server.md`).

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

-   :books: **[Erlang API Reference](api/erlang.md)**

    ---

    Complete function documentation

</div>

## Troubleshooting

!!! warning "Document Update Fails with Conflict"

    If you get a conflict error when updating, ensure you're passing the current revision. barrel_docdb uses MVCC (Multi-Version Concurrency Control) to prevent lost updates: read the document first, then include its `<<"_rev">>` in the update (or pass `#{rev => Rev}` to `delete_doc/3`).

    ```erlang
    %% Get the current revision first
    {ok, Doc} = barrel_docdb:get_doc(<<"myapp">>, <<"doc1">>),

    %% Then write back the document you just read
    {ok, _} = barrel_docdb:put_doc(<<"myapp">>, Doc#{<<"name">> => <<"updated">>}).
    ```

!!! tip "Serving over HTTP"

    To expose a database over REST/JSON (including TLS), run the `barrel_server` app rather than barrel_docdb on its own. See the umbrella REST server guide (`docs/guides/rest-server.md`).
</content>
</invoke>
