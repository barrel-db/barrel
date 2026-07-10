# Timeline

Barrel can branch a database: fork it instantly (at now, or rewound to a
past instant), work on the branch in complete isolation, and merge the
branch's edits back through the same version-vector machinery replication
uses. Read this when you want per-agent or per-experiment working copies,
point-in-time recovery, or a review-then-merge flow over your data.

## What it is

A branch is a hard-link checkpoint of both RocksDB stores (documents and
attachments): creating one is O(1) in data size, and parent and branch
share physical files copy-on-write until either side compacts. The fork
instant is minted inside the parent's writer, so the branch holds exactly
the writes applied before it. The branch is then a completely normal
database: reads, writes, queries, channels, changes, attachments, and the
`/db/:db/_sync/*` replication wire all work on it unchanged, and its
writes are authored under its own source id.

Merging ships only what the branch did: its changes since the fork apply
to the parent through `put_version`, so already-known versions are no-ops,
edits land, and concurrent edits on both sides become regular conflicts
(deterministic LWW, or the parent's `conflict_merger`). A merge checkpoint
makes repeated merges incremental.

Lineage is linear in v1: branching a branch is rejected.

## When to use it

- Give an agent or an experiment a private working copy, then merge what
  survived review.
- Recover a past state (PITR): branch at an instant inside the retention
  window and read or serve yesterday's data next to today's.
- Take a cheap consistent snapshot before a risky migration.

## How (branch and merge, docdb)

```erlang
{ok, _} = barrel_docdb:create_db(<<"main">>),
{ok, _} = barrel_docdb:put_doc(<<"main">>, #{<<"id">> => <<"a">>, <<"v">> => 1}),

%% instant fork
{ok, _} = barrel_docdb:branch_db(<<"main">>, <<"exp1">>, #{}),

%% the branch is a normal db; edits stay on it
{ok, #{<<"_rev">> := Rev}} = barrel_docdb:get_doc(<<"exp1">>, <<"a">>),
{ok, _} = barrel_docdb:put_doc(<<"exp1">>, #{<<"id">> => <<"a">>, <<"v">> => 2,
                                             <<"_rev">> => Rev}),

%% ship the branch's edits back; rerun any time, it is incremental
{ok, #{docs_written := 1}} = barrel_docdb:merge_branch(<<"exp1">>, #{}),

barrel_docdb:list_branches(<<"main">>),        %% [<<"exp1">>]
ok = barrel_docdb:delete_db(<<"exp1">>).
```

## How (branch at a past instant, PITR)

`at` takes a changes cursor (the HLC the feed returns):

```erlang
{ok, _Changes, T} = barrel_docdb:get_changes(<<"main">>, first),
%% ... more writes happen ...
{ok, _} = barrel_docdb:branch_db(<<"main">>, <<"yesterday">>, #{at => T}).
```

Every doc changed after T is restored to its latest state at or before T
(including live conflict siblings if the instant landed inside a
concurrent window); docs created after T do not exist on the branch. The
window is the retention window: a T below the history floor fails with
`pitr_window_exceeded`, as does a doc whose pre-T history was already
swept (the fork aborts cleanly, nothing is left behind).

## How (the `barrel` API, incl. record mode)

```erlang
{ok, Db} = barrel:open(main, #{embedding => #{fields => [<<"title">>]},
                               vectordb => #{db_path => "data/main_vec"}}),
{ok, Branch} = barrel:branch(Db, exp1,
                             #{vectordb => #{db_path => "data/exp1_vec"}}),
%% search works on the branch right away (see backfill below)
{ok, _Hits} = barrel:search(Branch, <<"query">>, #{k => 5}),
{ok, _Report} = barrel:merge(Branch),
ok = barrel:delete(Branch).
```

A record-mode branch gets a FRESH vector store, rebuilt synchronously
during `branch/3` from the embeddings already stored in doc bodies: docs
carrying an `_embedding` (client or computed) index with zero embedder
calls; docs restored by a PITR rewind lost that column and re-embed from
their restored text. `backfill => none` skips the pass;
`barrel_record_backfill:run/1` runs it manually and returns
`#{indexed, embedded, skipped, failed}`.

After a merge, the parent re-indexes merged docs through its own tagged
outbox; `barrel:merge` nudges its indexer when it is open locally.

## How (REST)

```bash
# fork at now, or at a cursor from /db/main/changes ("at"), or a
# wall-clock instant ("at_time", RFC3339)
curl -XPOST localhost:8080/db/main/_timeline/branch \
     -H 'content-type: application/json' -d '{"name": "exp1"}'

curl localhost:8080/db/main/_timeline
# {"db":"main","branches":["exp1"]}
curl localhost:8080/db/exp1/_timeline
# {"db":"exp1","parent":"main","fork_hlc":"...", "branches":[]}

curl -XPOST localhost:8080/db/exp1/_timeline/merge -d '{}'
# {"docs_written":1,...,"last_merged":"..."}

curl -XDELETE 'localhost:8080/db/exp1?purge=true'
```

Branches ride the whole existing API, including replication under
`/db/exp1/_sync/*`. Errors: 400 `invalid_name` / `bad_at` /
`pitr_window_exceeded`, 409 `already_exists` / `cannot_branch_a_branch` /
`not_a_branch`.

## Notes

- Reopening a branch after a restart follows the normal contract: its
  identity (parent, fork instant) persists on disk, but runtime config
  (channels, conflict_merger, retention) must be passed to `create_db`
  again, exactly as for any database. Channels are always inherited at
  fork time.
- Deleting the parent is safe for its branches (hard links make them
  independent). A branch that is not open is not listed by
  `list_branches`.
- A branch forks with the parent's retained history, so audit provenance
  (who wrote what before the fork) travels with it; post-fork writes build
  each side's own trail. See [audit-provenance](audit-provenance.md).
- Attachments: a PITR branch keeps fork-time attachments (they are LWW
  state with no history, so their state at T is not reconstructable).
  Merges carry post-fork attachment work, digest-deduplicated.
- Merge at least once per retention window: branch retention can forget
  an expired tombstone, and a forgotten delete never ships.
- A PITR rewind itself is not merged (restored versions predate the
  fork); "revert the parent to T" is a different operation and not in v1.
- Plain (non-record) `barrel` branches do not carry vectors: they live only
  in the vector store, which the branch gets fresh. Re-add them or use
  record mode.
- The parent's conflict_merger is database-open-time config; there is no
  per-merge override, and the merge report carries stats, not per-doc
  conflict detail.
