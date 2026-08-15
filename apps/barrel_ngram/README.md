# barrel_ngram

Exact substring and regex search over Barrel documents. A byte-level
trigram index gives the lexical recall that semantic search misses:
identifiers, error strings, config keys, punctuation-heavy literals.

[Documentation](https://barrel-db.eu/docs/lib/ngram/) |
[HexDocs](https://hexdocs.pm/barrel_ngram) |
[Repository](https://github.com/barrel-db/barrel)

A corpus is bound to a `barrel_docdb` database. Indexing is driven by the
database's changes feed, and every query result is confirmed against the
real document text, so a trigram false positive is never returned.

## Use

```erlang
%% Open a corpus over a database and build its index.
ok = barrel_ngram:open(<<"code">>, #{db => <<"mydb">>}),
{ok, _Summary} = barrel_ngram:index(<<"code">>),

%% Exact substring search. Each hit carries the document id and the
%% match spans within its indexed text.
{ok, Hits} = barrel_ngram:search(<<"code">>, <<"connect_timeout">>),

%% Regex search (PCRE syntax).
{ok, More} = barrel_ngram:regex(<<"code">>, <<"connect_\\w+timeout">>).

%% Case-insensitive, either mode.
{ok, Ci} = barrel_ngram:search(<<"code">>, <<"connect_timeout">>, #{case_sensitive => false}).
```

## Documentation

- [Getting started](docs/getting-started.md) - open, index, search, regex.
- [Selectors](docs/selectors.md) - what gets indexed, phase-2 tuning.
- [Regex](docs/regex.md) - patterns, what accelerates.
- [Sharding](docs/sharding.md) - spread a corpus across N shards.
- [Operations](docs/operations.md) - refresh, compact, recovery, deletes.
- [MCP tool](docs/mcp.md) - the `ngram_search` server tool.
- [Design](docs/design.md) - how the index is built and stored.

## How it works

- Byte-level trigrams over a 2^24 gram space, direct-addressed through a
  flat `u32` offset table.
- Delta+varint posting lists of local document ordinals, intersected by
  galloping search; a corpus can opt into roaring bitmaps
  (`postings => roaring`) for a native intersection AND on large dense
  corpora.
- Immutable segment files read with `file:pread`; a local ordinal maps
  back to its document key through a sidecar.
- A query turns the literal into its overlapping trigrams, intersects
  their posting lists, then fetches the candidates and runs the real
  substring match (trigram presence is necessary, not sufficient). A
  second, content-defined (sparse) index narrows further to a candidate
  byte position, and, with a `source` configured, confirms by reading
  just a window of the document instead of the whole thing.
- Literals shorter than a trigram fall back to a scan of the live set.
- `case_sensitive => false` on either `search` or `regex` for
  case-insensitive matching.

## Status

Substring and regex search over a dense trigram index, across multiple
immutable segments kept live by a push subscription to the changes feed
(updates and deletes reflected via the confirm pass), with crash-safe
manifest recovery and compaction that evicts superseded and deleted
entries. A corpus can be sharded across N nodes by rendezvous hashing
(`open` option `shards => N`), and barrel_server exposes it as the
read-only `ngram_search` MCP tool (mode literal or regex).

Every corpus also builds a second, content-defined (sparse) positional
index alongside the dense one (tuned with `phase2_selector_opts`): it
narrows candidates to specific byte positions and, with a `source`
configured, verifies a match by reading a small window instead of the
whole document, for both substring and (a bounded subset of) regex
queries. See [selectors](docs/selectors.md).

## License

Apache 2.0. See [LICENSE](LICENSE).
