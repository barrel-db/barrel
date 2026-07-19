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
disables it).

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
