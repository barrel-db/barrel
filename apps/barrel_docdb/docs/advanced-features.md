# Advanced Features Guide

This guide gives practical Erlang examples for two of Barrel DocDB's advanced features: replication and clock synchronization. All calls run in-process against the `barrel_docdb`, `barrel_rep`, and `barrel_rep_tasks` modules.

> #### HTTP access
>
> barrel_docdb has no built-in HTTP server. To drive replication or reach a database over the wire, run the `barrel_server` app. For the sync wire and over-the-network setup, see the umbrella guides `docs/guides/synchronization.md` and `docs/guides/rest-server.md`, and this app's [Replication Guide](replication.md).

---

## Replication

Replication synchronizes documents between databases. Documents are transferred with their full revision history, enabling automatic conflict detection.

### One-Shot Replication

Copy all documents from source to target within the same VM:

```erlang
%% Create source and target databases
{ok, _} = barrel_docdb:create_db(<<"source">>),
{ok, _} = barrel_docdb:create_db(<<"target">>),

%% Add documents to source
{ok, _} = barrel_docdb:put_doc(<<"source">>, #{
    <<"id">> => <<"user1">>,
    <<"name">> => <<"Alice">>, <<"role">> => <<"admin">>, <<"active">> => true
}),
{ok, _} = barrel_docdb:put_doc(<<"source">>, #{
    <<"id">> => <<"user2">>,
    <<"name">> => <<"Bob">>, <<"role">> => <<"user">>, <<"active">> => true
}),

%% Replicate source -> target
{ok, Result} = barrel_rep:replicate(<<"source">>, <<"target">>).
```

`Result` is a statistics map:

```erlang
#{
    ok => true,
    docs_read => 2,
    docs_written => 2,
    last_seq => ...
}
```

### Replication to a Remote Node

Replicate to another node by using the HTTP transport for the remote endpoint. The remote must run `barrel_server`, which serves the `/db/:db/_sync/*` wire:

```erlang
{ok, Result} = barrel_rep:replicate(
    <<"source">>,
    <<"http://remote-node:8080/db/target">>,
    #{
        source_transport => barrel_rep_transport_local,
        target_transport => barrel_rep_transport_http
    }).
```

Credentials come from `auth => #{token => Bin}` on the endpoint or the `{barrel_docdb, sync_auth, #{Origin => Token}}` app env, so secrets never land in persisted task configs. See the [Replication Guide](replication.md) for transport details.

### Filtered Replication by Path

Replicate only documents matching specific path patterns:

```erlang
{ok, _} = barrel_docdb:put_doc(<<"source">>, #{
    <<"id">> => <<"order1">>,
    <<"type">> => <<"order">>, <<"total">> => 150, <<"customer">> => <<"alice">>
}),
{ok, _} = barrel_docdb:put_doc(<<"source">>, #{
    <<"id">> => <<"invoice1">>,
    <<"type">> => <<"invoice">>, <<"amount">> => 150, <<"customer">> => <<"alice">>
}),

%% Replicate only orders (filter by the type/order path)
{ok, Result} = barrel_rep:replicate(<<"source">>, <<"orders_only">>, #{
    filter => #{paths => [<<"type/order">>]}
}).
```

### Filtered Replication by Query

Replicate documents matching query conditions:

```erlang
%% Replicate only active users
{ok, Result} = barrel_rep:replicate(<<"source">>, <<"active_users">>, #{
    filter => #{
        query => #{
            where => [
                {path, [<<"role">>], <<"user">>},
                {path, [<<"active">>], true}
            ]
        }
    }
}).
```

Path and query filters combine with AND logic: a document must match all specified filters to be replicated.

### Bidirectional Replication

Sync changes in both directions by running replication each way:

```erlang
%% Replicate A -> B
{ok, _} = barrel_rep:replicate(<<"db_a">>, <<"db_b">>),

%% Replicate B -> A
{ok, _} = barrel_rep:replicate(<<"db_b">>, <<"db_a">>).
```

### Continuous Replication

`barrel_rep:replicate/2,3` runs once and returns. For replication that keeps running as changes happen and survives node restarts, start a task with `barrel_rep_tasks`:

```erlang
{ok, TaskId} = barrel_rep_tasks:start_task(#{
    source => <<"source">>,
    target => <<"http://remote-node:8080/db/target">>,
    mode => continuous,
    target_transport => barrel_rep_transport_http
}),

ok = barrel_rep_tasks:pause_task(TaskId),
ok = barrel_rep_tasks:resume_task(TaskId),
{ok, Tasks} = barrel_rep_tasks:list_tasks().
```

### Verify Replication with the Changes Feed

Check that documents were replicated by comparing changes feeds:

```erlang
%% Changes from source
{ok, SourceChanges, _} = barrel_docdb:get_changes(<<"source">>, first),

%% Changes from target (should match)
{ok, TargetChanges, _} = barrel_docdb:get_changes(<<"target">>, first).
```

---

## Clock Synchronization (HLC)

Barrel DocDB uses Hybrid Logical Clocks (HLC) to maintain causal ordering across distributed nodes. HLC combines physical wall clock time with a logical counter, ensuring that events are correctly ordered even when physical clocks drift.

### Reading and Advancing the Clock

```erlang
%% Current HLC timestamp
Ts = barrel_docdb:get_hlc(),

%% Generate a new timestamp (advances the clock)
NewTs = barrel_docdb:new_hlc().
```

### Synchronizing with a Remote Clock

When you receive an HLC from another node, fold it into the local clock:

```erlang
{ok, SyncedTs} = barrel_docdb:sync_hlc(RemoteHlc).
```

Clock synchronization also happens automatically during replication: every exchange between peers piggybacks the current clock value, so causal ordering across nodes is preserved without an explicit sync step.

### Why HLC Matters

HLC ensures:

1. **Causal ordering**: Events that causally depend on each other are correctly ordered
2. **Conflict detection**: During replication, conflicts are detected based on HLC timestamps
3. **Change feed consistency**: The changes feed returns events in HLC order
4. **Cross-node consistency**: All nodes agree on event ordering regardless of clock drift

### HLC in the Changes Feed

`get_changes/2,3` returns the last HLC it observed. Use it to resume the feed from where you left off:

```erlang
{ok, Changes, LastHlc} = barrel_docdb:get_changes(<<"mydb">>, first),

%% ... process Changes ...

%% Resume from the last HLC
{ok, NewChanges, NewHlc} = barrel_docdb:get_changes(<<"mydb">>, LastHlc).
```

---

## Troubleshooting

### Count Documents

```erlang
{ok, Count} = barrel_docdb:fold_docs(<<"mydb">>,
    fun(_Doc, Acc) -> {ok, Acc + 1} end, 0).
```

### Inspect the Changes Feed

```erlang
{ok, Changes, LastHlc} = barrel_docdb:get_changes(<<"mydb">>, first),
Total = length(Changes).
```

### Database Info

```erlang
{ok, Info} = barrel_docdb:db_info(<<"mydb">>).
```
</content>
