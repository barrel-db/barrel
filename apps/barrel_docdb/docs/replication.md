# Replication Guide

barrel_docdb replicates documents between databases using a version-vector protocol: each write is a version (`<hex(hlc)>@<author>`), each document carries a version vector, and the target decides what it is missing by vector containment. This guide covers:

- Basic replication between local databases
- Continuous replication tasks
- Filtered replication (by path pattern or query)
- How the version-vector protocol works
- HLC-based checkpoints and incremental sync
- Transports (local and HTTP)

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

The replication result includes statistics:

```erlang
#{
    ok := true,                 %% Replication succeeded
    docs_read := 10,            %% Documents read from source
    docs_written := 10,         %% Documents written to target
    doc_read_failures := 0,     %% Failed reads
    doc_write_failures := 0,    %% Failed writes
    start_seq := first,         %% Starting HLC timestamp
    last_seq := #timestamp{},   %% Final HLC timestamp
    att_sync := skipped         %% Attachment phase outcome
} = Result.
```

## Continuous Replication

`barrel_rep:replicate/2,3` runs once and returns. For replication that keeps running and syncs changes as they happen (and survives node restarts), start a task with `barrel_rep_tasks`:

```erlang
{ok, TaskId} = barrel_rep_tasks:start_task(#{
    source => <<"mydb">>,
    target => <<"http://remote:8080/db/mydb">>,
    mode => continuous,
    direction => push
}).

%% Manage the task
ok = barrel_rep_tasks:pause_task(TaskId),
ok = barrel_rep_tasks:resume_task(TaskId),
Tasks = barrel_rep_tasks:list_tasks().
```

Tasks move through `pending`, `running`, `paused`, `completed` (one-shot), and `failed`. Task config is persisted in a system database, so a task resumes from its checkpoint after a restart.

## Filtered Replication

Replicate only documents matching specific criteria using the `filter` option. A filtered stream keeps its own checkpoint, separate from the full replication.

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

Replication makes one diff round-trip per batch of changes, then reads and applies each missing document. Conflict handling lives entirely in the target's `put_version`, so the algorithm needs no ancestor negotiation.

1. **Read checkpoint**: Find the last replicated HLC timestamp.
2. **Fetch changes**: Get changes from source since that timestamp (optionally filtered). Each change carries the document's current version token.
3. **Sync HLC**: Advance the target clock past the newest source change so the target's change sequence stays strictly increasing.
4. **Diff versions**: Call `diff_versions` on the target with `#{DocId => Token}`. The target answers `have` (its version vector already covers the offered version) or `missing`.
5. **Transfer documents**: For each missing document, read it from the source with its version, vector, and deleted flag.
6. **Apply to target**: Call `put_version`. The target no-ops, fast-forwards, or records a conflict sibling (see below).
7. **Save checkpoint**: Record progress with the HLC for incremental sync.

### Versions and Version Vectors

barrel_docdb uses MVCC based on Hybrid Logical Clocks, not revision trees.

- A **version** is `{HLC, Author}`. Its API token is `<hex(hlc)>@<author>`: the HLC gives causal last-write-wins ordering and the author is the id of the database that made the write (per-database, so two databases on one node detect each other's writes as concurrent). This is the value in the `<<"_rev">>` key.
- A **version vector** maps each author to the highest HLC this replica has seen from it for the document. Comparing two vectors decides how a replicated write relates to the local one:

```erlang
%% Create document
{ok, #{<<"rev">> := V1}} = barrel_docdb:put_doc(Db, Doc),
%% V1 = <<"0000018abc...@f1e0...">>

%% Update produces a new version with a later HLC
{ok, #{<<"rev">> := V2}} = barrel_docdb:put_doc(Db, UpdatedDoc#{<<"_rev">> => V1}).
```

### diff_versions - Finding Missing Versions

`diff_versions` answers, per document, whether the target already covers an offered version:

```erlang
{ok, Diff} = barrel_docdb:diff_versions(<<"target">>, #{
    <<"doc1">> => V1,
    <<"doc2">> => V2
}),
%% Diff = #{<<"doc1">> => have, <<"doc2">> => missing}
%% `have`    = the doc's version vector contains the offered version
%% `missing` = the target needs it
```

### put_version - Applying a Version

`put_version` applies a version read from the source. The source's token and vector are preserved; only the change-sequence HLC is issued locally. The outcome depends on how the vectors relate:

- **Already covered** (`contains`): idempotent no-op.
- **Remote dominates**: fast-forward. The old winner is retained as a superseded version.
- **Concurrent**: an optional per-database `conflict_merger` hook may resolve it with a merged body; otherwise last-write-wins by version picks the winner and the loser stays live as a retained conflict sibling. The vectors merge either way.

```erlang
{ok, Doc, #{version := Token, vv := VVBin, deleted := Del}} =
    barrel_docdb:get_doc_for_replication(<<"source">>, <<"doc1">>),
{ok, _DocId, _Winner} =
    barrel_docdb:put_version(<<"target">>, Doc, Token, VVBin, Del).
```

## Checkpoints and Incremental Replication

### How Checkpoints Work

Checkpoints are stored as local documents (not replicated) in both databases:

```erlang
%% Checkpoints are stored at:
%% _local/replication-checkpoint-<rep_id>

%% They contain the last replicated HLC and session history:
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

The replication id (and so the checkpoint) is derived from the source and target identities plus any filter. A filtered stream and an unfiltered stream do not share a checkpoint.

### HLC-Based Ordering

Replication uses HLC (Hybrid Logical Clock) timestamps as the change sequence:

- **Distributed ordering**: HLC provides consistent ordering across nodes.
- **Causal consistency**: Events are ordered by cause-effect relationships.
- **Clock synchronization**: The target clock is advanced with the source's during replication.

If a checkpoint predates the source's history floor (its retention window), the run forces a full resync so current state still converges.

### Incremental Sync

Subsequent replications automatically resume from the last checkpoint:

```erlang
%% First replication - syncs all documents
{ok, R1} = barrel_rep:replicate(<<"source">>, <<"target">>),

%% Add more documents to source
{ok, _} = barrel_docdb:put_doc(<<"source">>, #{<<"id">> => <<"doc3">>}),

%% Second replication - only syncs new documents
{ok, R2} = barrel_rep:replicate(<<"source">>, <<"target">>).
%% R2 shows fewer docs_read than R1
```

## Transports

### The Transport Behaviour

Replication talks to databases through the `barrel_rep_transport` behaviour, so the same algorithm works locally or over the network:

```erlang
-callback get_doc(Endpoint, DocId, Opts) ->
    {ok, Doc, Meta} | {error, term()}.
    %% Meta = #{version := Token, vv := EncodedVV, deleted := boolean()}
-callback put_version(Endpoint, Doc, VersionToken, VVBin, Deleted) ->
    {ok, DocId, WinnerToken} | {error, term()}.
-callback diff_versions(Endpoint, TokenMap) ->
    {ok, #{DocId => missing | have}} | {error, term()}.
-callback get_changes(Endpoint, Since, Opts) -> {ok, Changes, LastSeq}.
-callback get_local_doc(Endpoint, DocId) -> {ok, Doc} | {error, not_found}.
-callback put_local_doc(Endpoint, DocId, Doc) -> ok.
-callback delete_local_doc(Endpoint, DocId) -> ok.
-callback db_info(Endpoint) -> {ok, Info}.
-callback sync_hlc(Endpoint, Hlc) -> {ok, Hlc}.
```

Attachment sync, plus `rep_id_term/1` for checkpoint-stable endpoint identity, are optional callbacks.

### Built-in Transports

**barrel_rep_transport_local** - for databases in the same Erlang VM (the default for both endpoints):

```erlang
%% No configuration needed
{ok, _} = barrel_rep:replicate(<<"source">>, <<"target">>).
```

**barrel_rep_transport_http** - for a remote database, speaking the `/db/:db/_sync/*` wire served by `barrel_server`:

```erlang
{ok, Result} = barrel_rep:replicate(
    <<"mydb">>,
    <<"http://remote:8080/db/mydb">>,
    #{
        source_transport => barrel_rep_transport_local,
        target_transport => barrel_rep_transport_http
    }).
```

The HTTP endpoint is a normalized URL; credentials come from `auth => #{token => Bin}` on the endpoint or the `{barrel_docdb, sync_auth, #{Origin => Token}}` app env, so secrets never land in persisted task configs.

### Implementing a Custom Transport

Implement the `barrel_rep_transport` callbacks for another wire (TCP, a message bus, ...). Use `barrel_rep_transport_http` as the reference implementation.

```erlang
-module(my_transport).
-behaviour(barrel_rep_transport).
-export([get_doc/3, put_version/5, diff_versions/2, get_changes/3,
         get_local_doc/2, put_local_doc/3, delete_local_doc/2,
         db_info/1, sync_hlc/2]).

%% ... implement each callback against your endpoint term
```

Using a custom transport:

```erlang
Config = #{
    source => <<"local_source">>,
    target => #{host => "remote.example.com", db => <<"mydb">>},
    source_transport => barrel_rep_transport_local,
    target_transport => my_transport
},
{ok, Result} = barrel_rep:replicate_one_shot(Config, #{}).
```

## Conflict Handling

### How Conflicts Occur

A conflict happens when the same document is written concurrently in two databases (neither version's vector dominates the other) before replication:

```
Source:  doc1: v1 -> v2a
Target:  doc1: v1 -> v2b   (concurrent with v2a)
```

### How Conflicts Are Resolved

Replication never fails on a conflict. `put_version` picks a deterministic last-write-wins winner (max by version: HLC first, author id as tie-break) and retains the loser as a live conflict sibling. Every replica that sees the same version set picks the same winner. A per-database `conflict_merger` hook, if configured, gets the first say and can supersede both sides with a merged body.

### Surfacing and Resolving Conflicts

```erlang
%% List the live sibling tokens that conflict with the winner
{ok, Conflicts} = barrel_docdb:get_conflicts(Db, DocId),

%% Or read them inline
{ok, Doc} = barrel_docdb:get_doc(Db, DocId, #{conflicts => true}),
Siblings = maps:get(<<"_conflicts">>, Doc, []).
```

Resolve by choosing an existing sibling or writing a merged body. `BaseRev` is the current winning token (optimistic lock):

```erlang
%% Keep one version, drop the others
{ok, _} = barrel_docdb:resolve_conflict(Db, DocId, Winner, {choose, Sibling}),

%% Or write a merged body that supersedes all siblings
{ok, _} = barrel_docdb:resolve_conflict(Db, DocId, Winner, {merge, MergedDoc}).
```

A resolution is an ordinary write, so it replicates like any other.

## Best Practices

1. **Use meaningful document IDs** - Makes debugging easier.
2. **Handle failures gracefully** - Check `doc_read_failures` and `doc_write_failures`.
3. **Monitor checkpoint progress** - Detect stalled replications.
4. **Consider batch sizes** - Larger batches are faster but use more memory.
5. **Resolve conflicts** - Do not ignore retained siblings; converge them with `resolve_conflict`.

## API Reference

See the [Erlang API Reference](api/erlang.md) for complete function documentation.

- `barrel_rep` - Replication public API (`replicate/2,3`, `replicate_one_shot/1,2`)
- `barrel_rep_tasks` - Continuous / persistent replication tasks
- `barrel_rep_transport` - Transport behaviour
- `barrel_docdb:put_version/5` - Apply a replicated version
- `barrel_docdb:diff_versions/2` - Version diff by vector containment
- `barrel_docdb:get_doc_for_replication/2` - Read a document with its version metadata
