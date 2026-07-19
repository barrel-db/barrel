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

## Open a corpus over a database

A *corpus* is a named index bound to one database. Opening it starts a background
subscription to the database's changes feed, so the index stays in sync as documents are
written.

```erlang
%% index the database <<"mydb">> under a corpus of the same name
ok = barrel_ngram:open(<<"code">>, #{db => <<"mydb">>}).
```

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
`{error, {bad_regex, Reason}}`.

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
