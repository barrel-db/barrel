# Getting started

`barrel_ngram` gives you exact substring and regex search over the documents in a
barrel_docdb database. It is the lexical counterpart to semantic search: it finds
identifiers, error strings, config keys, and punctuation-heavy literals that embeddings
miss. Use it when you need to find documents that literally contain a string or match a
pattern. This page takes you from an empty database to your first search.

## Requirements

The `barrel_ngram` application must be started (it runs a supervision subtree). Under a
release it starts with barrel_ngram in the boot order; in a shell or a test:

```erlang
{ok, _} = application:ensure_all_started(barrel_ngram).
```

## Create a corpus

A *corpus* is a named index bound to one database. You create it with
`barrel_ngram:open/2`: there is no separate "create" step, `open/2` creates the corpus if
it does not exist and re-attaches to it (resuming from its on-disk state) if it does.
Opening it starts a background subscription to the database's changes feed, so the index
stays in sync as documents are written.

The name is any binary or atom and is yours to choose; a common convention is the database
name.

```erlang
%% index the database <<"mydb">> under a corpus named <<"code">>
ok = barrel_ngram:open(<<"code">>, #{db => <<"mydb">>}).
```

`db` is the only required option. The rest tune the index:

| Option | Default | What it does |
|--------|---------|--------------|
| `db` | (required) | the barrel_docdb database to index |
| `phase2_selector_opts` | `#{}` | phase-2 sampling tuning: `radius`/`sample_rate` (see [selectors](selectors.md)) |
| `fields` | `all` | `all`, or a list of document field names to index |
| `shards` | `1` | spread the corpus across N shards (see [sharding](sharding.md)) |
| `postings` | `varint` | posting codec; `roaring` for large dense corpora (see [design](design.md)) |
| `data_dir` | app env | where segments are stored (`<data_dir>/<corpus>/`) |
| `freeze_threshold` | 1000 | buffer size before an automatic freeze |
| `compact_threshold` | 16 | live segment count before an automatic compaction (`infinity` disables) |
| `source` | none | a `{Module, InitArg}` byte-source for windowed verification (see [design](design.md)) |

Some examples:

```erlang
%% a sharded, roaring corpus for a large code database
ok = barrel_ngram:open(<<"code">>,
                       #{db => <<"repo">>,
                         shards => 8,
                         postings => roaring,
                         data_dir => "/var/lib/barrel/ngram"}),

%% index only two fields of each document
ok = barrel_ngram:open(<<"notes">>,
                       #{db => <<"mydb">>, fields => [<<"title">>, <<"body">>]}).
```

The corpus persists on disk under `data_dir/<corpus>/`. After a restart, calling
`open/2` again with the same name and `data_dir` re-attaches and resumes from where it
left off (it replays only the feed tail since its last commit). Its options
(`phase2_selector_opts`, `fields`, `shards`, `postings`) are fixed for the life of the
corpus: reopening with a different `phase2_selector_opts` or `fields` fails with
`{error, {config_mismatch, Field, Persisted, Requested}}` rather than silently reindexing.
To change one, open a new corpus under a different name or `data_dir` and let it reindex.

## Index and search

Writes reach the index through the feed subscription. For a deterministic point where the
index has caught up (tests, ops), call `refresh/1`, then search:

```erlang
{ok, _Summary} = barrel_ngram:refresh(<<"code">>),

%% substring search: every document whose text contains the literal
{ok, Hits} = barrel_ngram:search(<<"code">>, <<"connect_timeout">>).
%% Hits = [#{id => <<"doc-a">>, spans => [{7, 15}]}, ...]
```

Each hit carries the document id and the byte spans where the literal occurs in the
document's indexed text.

## Regex search

```erlang
{ok, More} = barrel_ngram:regex(<<"code">>, <<"connect_\\w+timeout">>).
```

Regex uses PCRE syntax (it compiles with `re`). A malformed pattern returns
`{error, {bad_regex, Reason}}`. See [regex](regex.md) for what accelerates.

## Case-insensitive search

Both `search` and `regex` take `case_sensitive => false` as a third-argument option
(default `true`):

```erlang
{ok, Hits} = barrel_ngram:search(<<"code">>, <<"connect_timeout">>, #{case_sensitive => false}),
{ok, More} = barrel_ngram:regex(<<"code">>, <<"error">>, #{case_sensitive => false}).
```

See [regex](regex.md#case-insensitive-search) for the ASCII/non-ASCII split and the
errors a non-ASCII pattern or a non-UTF-8 document can return.

## Close a corpus

```erlang
ok = barrel_ngram:close(<<"code">>).
```

## Notes

- `index/1` is an alias of `refresh/1`.
- The corpus indexes the binary string values of each document's non-reserved top-level
  fields (see [selectors](selectors.md) and [design](design.md) for what gets indexed).
- Results are always exact: the trigram index only narrows candidates, and a confirm pass
  re-checks each candidate against the current document.
