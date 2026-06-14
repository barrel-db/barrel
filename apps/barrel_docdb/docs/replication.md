# Replication Guide

barrel_docdb supports CouchDB-style replication for synchronizing documents between databases. This guide covers:

- Basic replication between local databases
- Filtered replication (by path pattern or query)
- Understanding the replication process
- HLC-based checkpoints and incremental sync
- Custom transport implementations

## Basic Replication

### One-Shot Replication

The simplest form of replication copies all documents from source to target:

```erlang
%% Create source and target databases
{ok, _} = barrel_docdb:create_db(<<"source">>),
{ok, _} = barrel_docdb:create_db(<<"target">>),

%% Add documents to source
{ok, _} = barrel_docdb:put_doc(<<"source">>, #{
    <<"id">> => <<"doc1">>,
    <<"value">> => <<"hello">>
}),
{ok, _} = barrel_docdb:put_doc(<<"source">>, #{
    <<"id">> => <<"doc2">>,
    <<"value">> => <<"world">>
}),

%% Replicate source -> target
{ok, Result} = barrel_rep:replicate(<<"source">>, <<"target">>),

%% Check results
DocsWritten = maps:get(docs_written, Result),
io:format("Replicated ~p documents~n", [DocsWritten]).
```

### Replication with Options

Control batch size and checkpoint frequency:

```erlang
{ok, Result} = barrel_rep:replicate(<<"source">>, <<"target">>, #{
    batch_size => 50,       %% Process 50 changes per batch
    checkpoint_size => 25   %% Write checkpoint every 25 documents
}).
```

### Result Statistics

The replication result includes detailed statistics:

```erlang
#{
    ok := true,                 %% Replication succeeded
    docs_read := 10,            %% Documents read from source
    docs_written := 10,         %% Documents written to target
    doc_read_failures := 0,     %% Failed reads
    doc_write_failures := 0,    %% Failed writes
    start_seq := first,         %% Starting HLC timestamp
    last_seq := #timestamp{}    %% Final HLC timestamp
} = Result.
```

## Filtered Replication

Replicate only documents matching specific criteria using the `filter` option.

### Filter by Path Pattern

Use MQTT-style path patterns to select documents:

```erlang
%% Replicate only user documents
{ok, Result} = barrel_rep:replicate(<<"source">>, <<"target">>, #{
    filter => #{
        paths => [<<"type/user/#">>]
    }
}).

%% Replicate orders and invoices
{ok, Result} = barrel_rep:replicate(<<"source">>, <<"target">>, #{
    filter => #{
        paths => [<<"type/order/#">>, <<"type/invoice/#">>]
    }
}).
```

### Filter by Query

Use declarative queries for complex filtering:

```erlang
%% Replicate only active users
{ok, Result} = barrel_rep:replicate(<<"source">>, <<"target">>, #{
    filter => #{
        query => #{
            where => [
                {path, [<<"type">>], <<"user">>},
                {path, [<<"active">>], true}
            ]
        }
    }
}).

%% Replicate orders over $100
{ok, Result} = barrel_rep:replicate(<<"source">>, <<"target">>, #{
    filter => #{
        query => #{
            where => [
                {path, [<<"type">>], <<"order">>},
                {compare, [<<"total">>], '>', 100}
            ]
        }
    }
}).
```

### Combined Filters (AND Logic)

When both `paths` and `query` are specified, documents must match **both** filters:

```erlang
%% Replicate only active users (path AND query must match)
{ok, Result} = barrel_rep:replicate(<<"source">>, <<"target">>, #{
    filter => #{
        paths => [<<"type/user/#">>],      %% Must match path pattern
        query => #{                        %% AND must match query
            where => [{path, [<<"active">>], true}]
        }
    }
}).
```

### Path Pattern Syntax

| Pattern | Matches |
|---------|---------|
| `type/user` | Exact path |
| `type/+` | Single segment wildcard: `type/user`, `type/order` |
| `type/#` | Multi-segment wildcard: `type/user`, `type/user/profile` |
| `org/+/users/#` | Mixed: `org/acme/users/alice`, `org/corp/users/bob/profile` |

## How Replication Works

### The Replication Algorithm

1. **Read checkpoint**: Find the last replicated HLC timestamp
2. **Fetch changes**: Get document changes from source since that timestamp
3. **Sync HLC**: Synchronize target clock with source timestamps (ensures causal ordering)
4. **Compare revisions**: Use `revsdiff` to find which revisions target is missing
5. **Transfer documents**: Fetch missing revisions with their history
6. **Write to target**: Store documents using `put_rev` to preserve history
7. **Save checkpoint**: Record progress with HLC for incremental sync

### Understanding Revisions

barrel_docdb uses MVCC (Multi-Version Concurrency Control) with revision trees:

```erlang
%% Each document has a revision like "1-abc123"
%% Format: <generation>-<hash>

%% Create document
{ok, #{<<"rev">> := <<"1-abc123">>}} = barrel_docdb:put_doc(Db, Doc),

%% Update creates new revision
{ok, #{<<"rev">> := <<"2-def456">>}} = barrel_docdb:put_doc(Db, UpdatedDoc),
```

### revsdiff - Finding Missing Revisions

The `revsdiff` function compares revisions between databases:

```erlang
%% Check which revisions target is missing
{ok, Missing, Ancestors} = barrel_docdb:revsdiff(
    <<"target">>,
    <<"doc1">>,
    [<<"3-abc">>, <<"2-def">>, <<"1-ghi">>]
),
%% Missing = [<<"3-abc">>, <<"2-def">>]  (revisions target doesn't have)
%% Ancestors = [<<"1-xyz">>]             (target's revisions that could be ancestors)
```

### put_rev - Storing with History

The `put_rev` function stores documents with explicit revision history:

```erlang
%% Store document with full history
Doc = #{<<"id">> => <<"doc1">>, <<"value">> => <<"replicated">>},
History = [<<"2-def456">>, <<"1-abc123">>],  %% Newest first
{ok, DocId, Rev} = barrel_docdb:put_rev(<<"target">>, Doc, History, false).
```

## Checkpoints and Incremental Replication

### How Checkpoints Work

Checkpoints are stored as local documents (not replicated) in both databases:

```erlang
%% Checkpoints are stored at:
%% _local/replication-checkpoint-<rep_id>

%% They contain:
#{
    <<"history">> => [
        #{
            <<"source_last_hlc">> => <<...>>,  %% Encoded HLC timestamp
            <<"session_id">> => <<"abc123">>,
            <<"end_time">> => <<"2024-01-15T10:30:00Z">>
        }
    ]
}
```

### HLC-Based Ordering

Replication uses HLC (Hybrid Logical Clock) timestamps instead of sequence numbers:

- **Distributed ordering**: HLC provides consistent ordering across nodes
- **Causal consistency**: Events are ordered by cause-effect relationships
- **Clock synchronization**: Target clock is synced with source during replication

### Incremental Sync

Subsequent replications automatically resume from the last checkpoint:

```erlang
%% First replication - syncs all documents
{ok, R1} = barrel_rep:replicate(<<"source">>, <<"target">>),

%% Add more documents to source
{ok, _} = barrel_docdb:put_doc(<<"source">>, #{<<"id">> => <<"doc3">>}),

%% Second replication - only syncs new documents
{ok, R2} = barrel_rep:replicate(<<"source">>, <<"target">>),
%% R2 shows fewer docs_read than R1
```

## Transport Abstraction

### The Transport Behaviour

Replication uses a transport layer for database communication. The `barrel_rep_transport` behaviour defines:

```erlang
-callback get_doc(Endpoint, DocId, Opts) -> {ok, Doc, Meta} | {error, term()}.
-callback put_rev(Endpoint, Doc, History, Deleted) -> {ok, DocId, Rev} | {error, term()}.
-callback revsdiff(Endpoint, DocId, RevIds) -> {ok, Missing, Ancestors}.
-callback get_changes(Endpoint, Since, Opts) -> {ok, Changes, LastSeq}.
-callback get_local_doc(Endpoint, DocId) -> {ok, Doc} | {error, not_found}.
-callback put_local_doc(Endpoint, DocId, Doc) -> ok.
-callback delete_local_doc(Endpoint, DocId) -> ok.
-callback db_info(Endpoint) -> {ok, Info}.
```

### Built-in Transports

**barrel_rep_transport_local** - For databases in the same Erlang VM:

```erlang
%% Default transport, no configuration needed
{ok, _} = barrel_rep:replicate(<<"source">>, <<"target">>).
```

### Implementing Custom Transports

Create a custom transport for remote databases:

```erlang
-module(my_http_transport).
-behaviour(barrel_rep_transport).
-export([get_doc/3, put_rev/4, revsdiff/3, ...]).

%% Endpoint is your connection info
%% e.g., #{host => "remote.example.com", db => <<"mydb">>}

get_doc(#{host := Host, db := Db}, DocId, Opts) ->
    Url = build_url(Host, Db, DocId, Opts),
    case httpc:request(get, {Url, []}, [], []) of
        {ok, {{_, 200, _}, _, Body}} ->
            Doc = json:decode(Body),
            {ok, Doc, extract_meta(Doc)};
        {ok, {{_, 404, _}, _, _}} ->
            {error, not_found}
    end.

%% ... implement other callbacks
```

Using a custom transport:

```erlang
Config = #{
    source => #{host => "source.example.com", db => <<"mydb">>},
    target => <<"local_target">>,
    source_transport => my_http_transport,
    target_transport => barrel_rep_transport_local
},
{ok, Result} = barrel_rep:replicate_one_shot(Config, #{}).
```

## Conflict Handling

### How Conflicts Occur

Conflicts happen when the same document is modified in both databases before replication:

```
Source:  doc1: v1 -> v2a
Target:  doc1: v1 -> v2b

After replication, doc1 has two leaf revisions (conflict)
```

### Conflict Resolution

barrel_docdb preserves all conflicting revisions. Your application must resolve conflicts:

```erlang
%% Check for conflicts in a document
{ok, Doc} = barrel_docdb:get_doc(Db, DocId),
case maps:get(<<"_conflicts">>, Doc, []) of
    [] ->
        %% No conflicts
        ok;
    ConflictRevs ->
        %% Resolve by keeping one revision
        %% and deleting the others
        resolve_conflicts(Db, DocId, ConflictRevs)
end.
```

## Best Practices

1. **Use meaningful document IDs** - Makes debugging easier
2. **Handle failures gracefully** - Check `doc_read_failures` and `doc_write_failures`
3. **Monitor checkpoint progress** - Detect stalled replications
4. **Consider batch sizes** - Larger batches are faster but use more memory
5. **Implement conflict resolution** - Don't ignore conflicts

## API Reference

See the [Erlang API Reference](api/erlang.md) for complete function documentation.

- `barrel_rep` - Replication public API
- `barrel_rep_transport` - Transport behaviour
- `barrel_docdb:put_rev/4` - Low-level document write
- `barrel_docdb:revsdiff/3` - Revision comparison
