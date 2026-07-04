# Record mode: policy-driven vector indexing

Record mode makes a document and its vector one unit: you write documents,
and barrel keeps the vector index in sync with them, driven by a per-database
embedding policy. The vector store holds only vectors and indexes; search
results read text and metadata from the current documents. Read this when you
want semantic or hybrid search over your documents without managing vectors
yourself.

## When to use it

- You write JSON documents and want vector/BM25/hybrid search over chosen
  fields, kept consistent with every update and delete.
- You do not want to call `vector_add` by hand or duplicate text into a
  vector store.
- For direct vector management (you own ids and vectors separately from
  documents), use a plain database and the `vector_*` API instead.

## Open a record-mode database

Pass an `embedding` policy to `barrel:open/2`. The `barrel` application must
be running (it supervises the per-database indexer).

```erlang
{ok, _} = application:ensure_all_started(barrel),

{ok, Db} = barrel:open(notes, #{
    embedding => #{
        fields => [<<"title">>, [<<"body">>, <<"text">>]],
        mode => async,                     %% default; sync = read-your-write
        embedder => {local, #{}},          %% barrel_embed provider chain
        dimensions => 768,
        metadata_fields => [<<"kind">>]    %% optional projection
    }
}).
```

- `fields` are paths into the document; their binary values are joined
  (`join`, default `<<"\n">>`) into the text that embeds.
- Without `metadata_fields`, search metadata is the document minus `id` and
  `_`-prefixed keys.
- BM25 defaults to the disk backend in record mode so keyword search
  survives restarts.
- The policy is persisted in the database; reopening with a different policy
  logs a warning and applies it to new writes only (no automatic reindex).

## Write documents, search them

```erlang
{ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>,
                               <<"title">> => <<"quick brown fox">>,
                               <<"kind">> => <<"animal">>}),

{ok, Hits}  = barrel:search(Db, <<"fast fox">>, #{k => 5}),
{ok, HHits} = barrel:search_hybrid(Db, <<"fox">>, #{k => 5}),
{ok, BHits} = barrel:search_bm25(Db, <<"fox">>, #{k => 5}).
```

Hits carry `key`, `score`, and the document-derived `text` and `metadata`.
Updates re-embed; deletes remove the vector; documents without policy fields
get no vector.

## Async and sync

`mode => async` (default): writes return immediately and a supervised
indexer embeds in the background. The write and its indexing intent commit
atomically, so a crash at any point is healed by the indexer; no write is
ever lost between the document and its vector.

`mode => sync`: the text embeds before the write and the vector is indexed
before `put_doc` returns, so a search issued right after sees the document.
An embed failure fails the put with `{error, {embed_failed, Reason}}` and
nothing is written.

## Explicit vectors

Supply your own embedding for one document; it skips the embedder and
indexes synchronously (works in both modes):

```erlang
{ok, _} = barrel:put_doc(Db, Doc, #{vector => Vector}).
```

The vector length must match the database dimension, checked before the
write.

## Notes

- `vector_add` and `vector_add_batch` return `{error, record_mode}` on
  record databases: the document is the only write path.
- Embed failures in async mode retry per document; after 5 failures the
  document is parked (logged, its indexing entry stays pending and visible)
  and the rest of the queue keeps moving.
- `barrel:info/1` reports the active policy and dimension.
- On `barrel_server`, set the `open_opts` app env of `barrel_server` to open
  every database with a policy, for example
  `{barrel_server, [{open_opts, #{embedding => ...}}]}` in `sys.config`.
