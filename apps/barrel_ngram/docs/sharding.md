# Sharding

A corpus can be split across N shards so each shard indexes only a slice of the documents.
Use it for large databases where one segment set would be too big, or to parallelize
indexing and queries. Sharding is transparent: a sharded corpus returns the same results
as a single-shard one.

## Open a sharded corpus

```erlang
ok = barrel_ngram:open(<<"big">>, #{db => <<"mydb">>, shards => 4}).
```

Every operation (`refresh`, `compact`, `search`, `regex`, `close`) works the same; the
fan-out and merge happen for you.

## How ownership works

Each document key is mapped to exactly one shard by rendezvous (HRW) hashing. Because the
key is stable, every change to a document lands in the same shard, so a document is indexed
once and queries never double-count. A query fans across all shards and unions the hits.

## When to shard

- The database is large enough that a single segment set is unwieldy, or compaction of one
  set is too coarse.
- You want indexing spread across more processes.

For small or moderate databases, a single shard (the default) is simpler and enough.

## Notes

- `shards` is fixed for a corpus. Changing N changes the ownership mapping, so it requires
  reindexing into a new corpus (a different data dir).
- Each shard keeps its own segments under `data_dir/<corpus>/shard-<i>/`; a single-shard
  corpus keeps them directly under `data_dir/<corpus>/`.
- Sharding is orthogonal to the selector: a corpus can be sparse and sharded.
