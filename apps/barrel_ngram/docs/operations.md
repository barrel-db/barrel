# Operations

This page covers running a corpus: keeping it current, bounding its growth, where it
stores data, how it recovers, and one caveat about deletes. Read it before putting a
corpus into production.

## Keeping the index current

A corpus subscribes to its database's changes feed and applies changes in the background,
so it is eventually current on its own. For a synchronous catch-up point, use `refresh/1`:
it drains the feed up to now and freezes the buffer. It is a no-op when nothing has
changed, so calling it before a query is cheap and makes results deterministic.

```erlang
{ok, _} = barrel_ngram:refresh(<<"code">>).
```

## Bounding growth

Segments accumulate as documents change. Compaction merges them, collapsing each document
to its newest version and physically evicting superseded and deleted entries. It runs
automatically when the live segment count crosses `compact_threshold` (default 16), and you
can force it:

```erlang
{ok, #{segments := N}} = barrel_ngram:compact(<<"code">>).
```

`compact/1` returns `{error, busy}` if a background compaction is already running.

## Storage

Segments live under `data_dir/<corpus>/` (per shard, `.../shard-<i>/`). Set `data_dir` at
open time, or leave it to the `barrel_ngram` app env (`data/barrel_ngram`).

```erlang
ok = barrel_ngram:open(<<"code">>,
                       #{db => <<"mydb">>, data_dir => "/var/lib/barrel/ngram"}).
```

Tuning options at open: `freeze_threshold` (buffer size before an automatic freeze),
`compact_threshold` (live segment count before an automatic compaction; `infinity`
disables it), and `postings` (the posting codec, `varint` default or `roaring` for large
dense corpora, see [design](design.md)).

Set `data_dir` in the app env before the application is loaded, or pass it to `open/2`.
`application:set_env/3` on an app that is not loaded yet is overwritten by the `.app`
default when the app loads; use `[{persistent, true}]` if you set it from code.

## Retiring a corpus

A corpus binding (database name and instance id) lives in `data_dir/<corpus>/corpus.meta`
and survives restarts. When you no longer need a corpus, or its database was destroyed and
recreated (reopening then fails with `{config_mismatch, db_instance_id, Old, New}`),
delete it:

```erlang
ok = barrel_ngram:delete_corpus(<<"code">>).
%% not open in this VM and not under the app env data_dir:
ok = barrel_ngram:delete_corpus(<<"code">>, #{data_dir => "/var/lib/barrel/ngram"}).
```

It closes the corpus if open and removes `data_dir/<corpus>/`. It is idempotent. The name
can be reopened right away, no restart needed. Deleting an open corpus with a `data_dir`
other than the one it runs from returns `{error, {config_mismatch, data_dir, Live, Given}}`.

## Upgrading the on-disk format

The index is derived from the database, so an old format is rebuilt, never migrated. If
`open/2` fails with `{legacy_corpus_requires_reindex, _}`, `{unsupported_manifest_version, _,
_}`, `{unsupported_segment_version, _, _, _}` or `{unsupported_corpus_meta_version, _, _}`,
let open rebuild it:

```erlang
ok = barrel_ngram:open(<<"code">>, #{db => <<"mydb">>, on_legacy => reindex}).
```

The corpus directory is wiped and reindexed from the start of the changes feed. A config
mismatch is never treated as legacy. To do it by hand instead: `delete_corpus/1,2`, then
`open/2`. Both run in the live VM; neither the app nor the VM needs a restart.

## Recovery

The manifest rename is the only commit point. On restart the corpus loads the manifest and
resubscribes from its watermark, replaying only the feed tail. A crash mid-freeze or
mid-merge leaves an orphan segment that is cleaned up on the next open; the committed
segments are intact.

## The delete caveat

Deletes are observed from the feed, where a deletion is a tombstone that the database keeps
for `retention_period` (default 30 days), then purges. If a corpus is offline longer than
the retention window and then resumes from an older watermark, it can miss a deletion (the
tombstone is gone). Queries stay correct regardless (the confirm pass drops a deleted
document when it fetches it), but the deleted document's grams may linger un-evicted until
the next compaction. Keep a corpus's downtime well under `retention_period`, or run the
database with retention disabled, if durable delete propagation matters.
