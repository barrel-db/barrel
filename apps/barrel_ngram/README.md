# barrel_ngram

Exact substring and regex search over Barrel documents. A byte-level
trigram index gives the lexical recall that semantic search misses:
identifiers, error strings, config keys, punctuation-heavy literals.

[Documentation](https://barrel-db.eu/docs/lib/ngram/) |
[HexDocs](https://hexdocs.pm/barrel_ngram) |
[Repository](https://github.com/barrel-db/barrel)

A corpus is bound to a `barrel_docdb` database and a gram selector.
Indexing is driven by the database's changes feed, and every query
result is confirmed against the real document text, so a trigram false
positive is never returned.

## Use

```erlang
%% Open a corpus over a database and build its index.
ok = barrel_ngram:open(<<"code">>, #{db => <<"mydb">>}),
{ok, _Summary} = barrel_ngram:index(<<"code">>),

%% Exact substring search. Each hit carries the document id and the
%% match spans within its indexed text.
{ok, Hits} = barrel_ngram:search(<<"code">>, <<"connect_timeout">>).
```

## How it works

- Byte-level trigrams over a 2^24 gram space, direct-addressed through a
  flat `u32` offset table.
- Delta+varint posting lists of local document ordinals, intersected by
  galloping search.
- Immutable segment files read with `file:pread`; a local ordinal maps
  back to its document key through a sidecar.
- A query turns the literal into its overlapping trigrams, intersects
  their posting lists, then fetches the candidates and runs the real
  substring match (trigram presence is necessary, not sufficient).
- Literals shorter than a trigram fall back to a scan of the live set.

## Status

Dense trigram selection over multiple immutable segments, kept live by a
push subscription to the changes feed (updates and deletes reflected via
the confirm pass), with crash-safe manifest recovery. Compaction and a
live-docs bitmap, sparse (content-defined) selection, regex, sharding, and
the MCP tool land in later milestones.

## License

Apache 2.0. See [LICENSE](LICENSE).
