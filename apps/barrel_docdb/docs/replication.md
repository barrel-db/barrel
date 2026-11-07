# Replication Guide

Replication copies documents between databases that all accept writes. Each document carries its whole replicated state: its current versions and every version it has seen. Replication sends that state and the target merges it, so concurrent writes are all kept, a version that was replaced never comes back, and replicas can leave, return or join empty at any time. This guide covers:

- Basic and continuous replication
- Filtered replication
- How replication works
- Checkpoints and incremental sync
- Transports (local and HTTP)
- Conflicts: how they appear and how to resolve them
- Deletes and how long they are kept

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

A task whose local database closes pauses until the database is open again. After a restart, databases open on first use, so tell the task manager how your application opens one; barrel_server does this for you:

```erlang
%% called as my_app:open_db(Name) for a task database still on disk
application:set_env(barrel_docdb, task_db_opener, {my_app, open_db}).
```

### Credentials for tasks

A task config is saved to disk, so it must not hold secrets. `start_task/1` refuses an endpoint with `auth`, `signing`, `ssl_options` or `headers`, and a URL with user info (`user:pass@host`), with `{error, {inline_credentials, Keys}}`. Put the credentials in the app env instead, keyed by the database URL or by its origin (the URL wins):

```erlang
application:set_env(barrel_docdb, sync_auth, #{
    <<"https://remote:8443/db/mydb">> => <<"token-for-mydb">>,
    <<"https://remote:8443">> => <<"token-for-the-rest">>
}).
%% likewise sync_signing (#{key_id, priv_key}) and sync_ssl (ssl options)

{ok, TaskId} = barrel_rep_tasks:start_task(#{
    source => <<"mydb">>,
    target => #{url => <<"https://remote:8443/db/mydb">>,
                recv_timeout => 60000},
    mode => continuous
}).
```

Notes:
- The URL key is the normalized URL: lowercase scheme and host, no trailing slash.
- Tuning keys (`pool`, `connect_timeout`, `recv_timeout`, `request_timeout`) are saved with the task and kept across pause, resume and restart.
- `barrel_rep:replicate/3` still accepts inline credentials (nothing is saved), but not a URL with user info.
- `request_timeout` (default 5 minutes) bounds each whole HTTP request, an attachment download or upload included; past it the call returns `{error, request_timeout}`.

### Attachments

Tasks replicate attachments by default, same as `barrel_rep:replicate/2,3`. Attachments live on their own feed (independent of the document changes feed), so they have their own checkpoint (`att_seq`, persisted alongside `last_seq`) and their own idle-wake handling: a continuous task periodically re-checks attachments even when no document change occurs, so an attachment added to an already-replicated document still converges without any further doc write.

```erlang
{ok, TaskId} = barrel_rep_tasks:start_task(#{
    source => <<"mydb">>,
    target => <<"http://remote:8080/db/mydb">>,
    mode => continuous,
    direction => push,
    attachments => true,      %% default; set false to skip attachments
    att_batch_size => 100      %% attachment feed batch size (default 100)
}).
```

Set `attachments => false` to replicate documents only. This is persisted with the rest of the task config, so it survives `pause_task`/`resume_task` and node restarts.

A `both` task keeps one attachment position per direction (`att_seq` for push, `pull_att_seq` for pull).

### Attachment durability

By default (`att_durability => required`) the target makes its attachment writes durable before each attachment checkpoint, so a power cut on the target never loses attachments a run reported as copied. A target that cannot (a custom backend without `flush/1`, a server without `_sync/att_flush`) fails the run with `{error, att_flush_unsupported}` before anything is written.

```erlang
%% replicate to a target that cannot sync; a power cut there can lose the last batch
{ok, _} = barrel_rep:replicate(Source, Target, #{att_durability => best_effort}).
```

A failed attachment transfer stops the attachment phase with `{error, {att_transfer_failed, Stats}}` (wrapped in `att_sync_failed` by `barrel_rep`); the checkpoint stays before it and the next run sends it again. A task records it as `last_error` and retries after a backoff.

### Attachment Storage Backends

Where attachment bytes actually live is pluggable, via the `barrel_att_backend` behaviour: `barrel_att_store` picks a backend per database from `att_opts.backend` (default is `barrel_att_store_blob`, an embedded RocksDB instance with BlobDB enabled). `barrel_att_s3` (a separate sibling app, opt in with the `s3` rebar3 profile) is an S3-compatible backend for AWS S3, MinIO, or Garage -- see its [getting started](../../barrel_att_s3/docs/getting-started.md) guide.

This matters for replication because the behaviour splits its callbacks in two:

- **Required**: `put`/`get`/`delete`/streaming, storing and reading bytes. Any backend needs these.
- **Optional**: `att_changes/4`, `att_floor/2`, `sweep_att_feed/3`, `rebuild_feed/2` (the attachment change feed replication reads from), `checkpoint/2` (timeline branching), `destroy/2` (backend-owned state beyond the local directory, erased on `delete_db`).

`barrel_att_store_blob` implements both sets: it keeps its feed in the *same* RocksDB instance as the blobs, committed in the same write batch as the blob write, so the feed can never fall out of sync with what was actually stored. `barrel_att_s3` implements both sets too, as of a later revision than its first release: it keeps its feed in a small local RocksDB instance alongside the (remote) attachment bytes, since S3 has no equivalent to a local atomic write batch spanning both -- committed just after each S3 write succeeds, not atomically with it. Either way, replication treats both backends the same: full LWW-checked sync as both source and target.

A backend that implements only the required set (put/get/delete/streaming, no feed at all) still degrades gracefully rather than erroring, detected per call via `barrel_att_store:supports_sync/1` (`erlang:function_exported/3`): attachment sync reports `att_sync => skipped` as a replication *source*, for one-shot replication, timeline merges, and tasks alike. Attachments still work locally (put/get/delete) in that case; they just don't replicate. This check only covers the source -- such a backend as a replication *target* is not detected, since `put`/`delete` are required callbacks and land regardless, with no feed on the target to check `origin_hlc` against, so the last-write-wins guard a feed-backed target enforces silently does not apply for it.

`none` (`att_opts => #{backend => none}`) stores nothing and opens no RocksDB instance. It has no feed, so as a replication source attachment sync reports `att_sync => skipped`. As a target, every attachment write or delete returns `{error, attachments_disabled}`; document replication is unaffected. barrel uses it for its internal `_barrel_system` and `_replication_tasks` databases. Do not point attachment replication at a `none` database.

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

### What a filter lets through

- A document passes when its winner's body, or the body of one of its live conflicts, matches. The whole state travels, conflicts included.
- A delete passes every filter. It has no body to match, and a peer that holds the document must get it.
- A document edited out of the filter's scope stays on the peer as its last matching version. A filter chooses what to copy; it does not take back what was copied.
- Changing a filter is a new stream with its own checkpoint, replayed from the start for the new scope.
- Filters are not access control: a matching document travels with all its live versions.
- Attachments follow the document: an attachment is copied when its document passes, an attachment delete always passes, and a document that enters the filter brings its existing attachments. Over HTTP the source applies the filter; a server that cannot (no `att_filter` in `_sync/info`) fails the run with `{error, att_filter_unsupported}`. An unknown channel or a filter error stops the run.

## How Replication Works

### The Replication Algorithm

1. **Read checkpoints** on both sides; start from the lower position and retry the documents left pending by the last run.
2. **Fetch changes** from the source since that position (filtered if asked). Each row names a document whose state changed.
3. **Read the full state** of each changed document from the source: its live versions with their bodies, and the versions it has seen.
4. **Merge it into the target** (`put_doc_state/4`). The target keeps a version if both sides hold it or if it never saw it; a version one side saw and dropped stays dropped. A state the target already holds writes nothing.
5. **Save the checkpoint** with the new position and any document that failed, which the next run retries.

A failed document never gets skipped: it stays pending until it replicates. A target that cannot move its clock past its last change (`clock_skew`) stops the batch without moving the position.

### Versions and seen sets

- A **version** is `{HLC, Writer}`; its token `<hex(hlc)>@<writer>` is the `<<"_rev">>` value. A database gets a new writer id every time it opens, so a restored backup or a copied database can never reuse one.
- A document's **live versions** are its winner (the highest token) and its conflicts.
- Its **seen set** records every version it has seen. Merging two states keeps what neither side has replaced.

```erlang
%% Read and merge a whole document state by hand
{ok, #{state := State, bodies := Bodies}} =
    barrel_docdb:get_doc_state(<<"source">>, <<"doc1">>),
{ok, _DocId, _Winner} =
    barrel_docdb:put_doc_state(<<"target">>, <<"doc1">>, State, Bodies).
```

### Older peers

A server or client from before whole-state replication sends one version at a time (`put_version/5`, `diff_versions/2`). Current replicators detect it (`_sync/info` has no `full_state`) and fall back. Such a version never removes anything: it is ignored if the document has seen it, otherwise kept as a conflict. Upgrade old peers to stop the extra conflicts.

## Checkpoints and Incremental Replication

### How Checkpoints Work

Checkpoints are stored as local documents (not replicated) in both databases:

```erlang
%% Checkpoints are stored at:
%% _local/replication-checkpoint-<rep_id>

%% They contain the last replicated HLC, the documents still to retry
%% (pending), and session history:
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

### Power cuts

Writes return before they reach the disk unless you pass `sync => true`, so a power cut can lose the last writes a database acknowledged, including documents it received by replication. Replication repairs that: a one-shot run starts from the lower of the source's and the target's checkpoints, and a continuous task leaves a marker in its target after each batch, so a target that lost a batch is sent it again. A write lost on the database that made it, before it reached another replica, is gone.

```erlang
%% a write that must survive a power cut
{ok, _} = barrel_docdb:put_doc(Db, Doc, #{sync => true}).
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

The HTTP endpoint is a normalized URL; credentials come from `auth => #{token => Bin}` on the endpoint or the `{barrel_docdb, sync_auth, #{UrlOrOrigin => Token}}` app env, so secrets never land in persisted task configs (see [Credentials for tasks](#credentials-for-tasks)).

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

### When there is a conflict

Two versions conflict when neither was written by someone who had seen the other, for example two replicas editing the same document before they sync. Both are kept, on every replica.

### What a read shows

```erlang
{ok, Doc} = barrel_docdb:get_doc(Db, DocId, #{conflicts => true}),
Winner    = maps:get(<<"_rev">>, Doc),            %% highest token
Conflicts = maps:get(<<"_conflicts">>, Doc, []),  %% the other versions
Observed  = maps:get(<<"_observed">>, Doc, [Winner]).
```

Every replica shows the same winner for the same versions. Showing a winner deletes nothing.

### Editing a document in conflict

Editing with `_rev` replaces only the winner; the conflicts stay. Editing with `_observed` replaces every version you read:

```erlang
%% replaces the winner; the conflict stays listed
{ok, _} = barrel_docdb:put_doc(Db, Doc#{<<"v">> => 2}),

%% replaces every version you read
{ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => DocId,
                                    <<"_observed">> => Observed,
                                    <<"v">> => 2}).
```

A `_rev` that is not the winner returns `{error, conflict}`. Writing over a deleted document replaces every delete, with or without a rev.

Over HTTP, read with `?conflicts=true` and send `_observed` back in the body:

```console
$ curl -s "$URL/db/notes/doc/doc1?conflicts=true"
{"id":"doc1","v":"a","_rev":"...","_conflicts":["..."],"_observed":["...","..."]}
$ curl -X PUT "$URL/db/notes/doc/doc1" -H 'content-type: application/json' \
    -d '{"v":"merged","_observed":["...","..."]}'
```

### Resolving a conflict

```erlang
{ok, _} = barrel_docdb:resolve_conflict(Db, DocId, Observed, {choose, Token}),
{ok, _} = barrel_docdb:resolve_conflict(Db, DocId, Observed, {merge, Body}),
{ok, _} = barrel_docdb:resolve_conflict(Db, DocId, Observed, delete).
```

The resolution replaces the versions in `Observed` on every replica and stays resolved. A version that arrived after your read stays as a conflict, so nothing is lost. Passing the winner's `_rev` instead of a list replaces every current version, including one that arrived after your read. Two replicas resolving the same conflict at once leave their two resolutions in conflict; resolve once more.

### Automatic resolution

```erlang
Resolver = fun(Versions) ->
    %% Versions: [#{token, deleted, body}], sorted by token
    Live = [B || #{deleted := false, body := B} <- Versions],
    case Live of
        [] -> delete;
        _ -> {merge, lists:max(Live)}
    end
end,
{ok, _} = barrel_docdb:create_db(<<"db">>, #{conflict_resolver => Resolver}).
```

The function runs whenever a write or a sync leaves more than one version, and returns `{choose, Token}`, `{merge, Body}`, `delete` or `keep` (leave the conflict). It must be pure and give the same result whatever order it sees the versions in ("highest price wins", "union of tags"). Every replica then writes the same resolution, so a conflict ends in one version everywhere. A crash or an error counts as `keep`. If replicas run different functions, nothing is lost and resolution still ends, but which result survives depends on sync order; `resolver_disagreement` is logged when it is detected.

`conflict_merger` is no longer used; a database configured with it logs a warning.

### Too many conflicts

Above `max_conflict_versions` (default 8) conflicts, the oldest versions go first, older peers' versions before the rest. The winner always stays. An evicted version's body stays readable until retention.

## Deletes and Retention

A delete is a version. After `retention_period` (default 30 days), a deleted document becomes a **stub**: its bodies, history and index entries go, but its state and its change row stay. A peer that was offline, or joins empty, still gets the delete, and an old copy of the document arriving later is recognised as replaced.

Stubs are kept for good unless you set `tombstone_purge_after` (seconds):

```erlang
{ok, _} = barrel_docdb:create_db(<<"db">>, #{tombstone_purge_after => 90 * 86400}).
```

Attachment deletes follow the same window: they stay in the attachment feed until `tombstone_purge_after`, and for good when it is 0.

A purged document can come back if anything older than that reaches the database: a replica offline longer, a backup, or a message read before the delete and delivered late. Purge only when you accept that risk.

## Best Practices

1. **Resolve conflicts** or set a `conflict_resolver`; conflicts stay until something resolves them.
2. **Pass `_observed`** when a client resolves conflicts, so a version it did not see is never replaced.
3. **Watch `pending`** in the replication result: documents that failed and will be retried.
4. **Upgrade older peers**: their edits show as conflicts.
5. **Leave purge off** unless you accept that a long-offline replica can bring a delete back.

## API Reference

See the [API reference](api-reference.html) for complete function documentation.

- `barrel_rep` - Replication public API (`replicate/2,3`, `replicate_one_shot/1,2`)
- `barrel_rep_tasks` - Continuous / persistent replication tasks
- `barrel_rep_transport` - Transport behaviour
- `barrel_docdb:get_doc_state/2`, `put_doc_state/4`, `diff_states/2` - Whole document states
- `barrel_docdb:put_version/5`, `diff_versions/2` - One version at a time (older peers)
- `barrel_docdb:resolve_conflict/4` - Resolve a conflict
