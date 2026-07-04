# Embed barrel in an Erlang app

`barrel` is the embeddable edge database. It composes the document layer
(`barrel_docdb`) and the vector layer (`barrel_vectordb`) behind one API, where a
document, its attachments (blobs), and its vector share a single id. Read this
when you want a database inside your Erlang release without running a server.

## When to use it

- You want documents, vectors, BM25/hybrid search, attachments, and a changes
  feed from one handle, in-process.
- You do not need a network endpoint. For HTTP access, run `barrel_server` instead.

## Open and close

```erlang
{ok, _} = application:ensure_all_started(barrel_docdb),
{ok, _} = application:ensure_all_started(barrel_vectordb),

{ok, Db} = barrel:open(mydb),
%% pass options per layer:
{ok, Db2} = barrel:open(mydb, #{
    vectordb => #{dimension => 768, bm25_backend => memory}
}),
ok = barrel:close(Db).
```

A database links its vector store to the process that opened it. Open it from a
long-lived process (a gen_server or supervisor), not a transient one.

## Documents

```erlang
{ok, _}   = barrel:put_doc(Db, #{<<"id">> => <<"a">>, <<"title">> => <<"hello">>}),
{ok, Doc} = barrel:get_doc(Db, <<"a">>),
{ok, _}   = barrel:delete_doc(Db, <<"a">>),
{ok, Rows, _Meta} = barrel:find(Db, #{where => [{path, [<<"title">>], <<"hello">>}]}).
```

## Batches

```erlang
[{ok, _}, {ok, _}] = barrel:put_docs(Db, [#{<<"id">> => <<"a">>}, #{<<"id">> => <<"b">>}]),
Results = barrel:get_docs(Db, [<<"a">>, <<"b">>]),     %% one result per id, in order
_       = barrel:delete_docs(Db, [<<"a">>, <<"b">>]).
```

## Attachments (blobs)

Blobs are document attachments; the storage backend is pluggable per database via
the docdb `barrel_att_backend` seam (RocksDB BlobDB by default).

```erlang
{ok, _}          = barrel:put_attachment(Db, <<"a">>, <<"f.txt">>, <<"bytes">>),
{ok, <<"bytes">>} = barrel:get_attachment(Db, <<"a">>, <<"f.txt">>),
[<<"f.txt">>]    = barrel:list_attachments(Db, <<"a">>),
ok               = barrel:delete_attachment(Db, <<"a">>, <<"f.txt">>).
```

Stream large blobs:

```erlang
{ok, W0} = barrel:open_attachment_writer(Db, <<"a">>, <<"big">>, <<"application/octet-stream">>),
{ok, W1} = barrel:write_attachment(W0, <<"chunk">>),
{ok, _}  = barrel:finish_attachment(W1),

{ok, R0} = barrel:open_attachment_reader(Db, <<"a">>, <<"big">>),
{ok, _Chunk, R1} = barrel:read_attachment(R0),    %% eof at the end
ok = barrel:close_attachment_reader(R1).
```

## Vectors and search

To keep vectors in sync with documents automatically, open the database with
an embedding policy instead of managing them by hand: see
[record mode](record-mode.md). The direct vector API below applies to plain
databases.

```erlang
ok = barrel:vector_add(Db, <<"a">>, <<"hello world">>, #{}, Vector),
{ok, #{inserted := 2}} = barrel:vector_add_batch(Db, [
    {<<"a">>, <<"t1">>, #{}, V1},
    {<<"b">>, <<"t2">>, #{}, V2}
]),
{ok, Hits}  = barrel:search_vector(Db, Vector, #{k => 5}),
{ok, BHits} = barrel:search_bm25(Db, <<"hello">>, #{k => 5}).
```

Notes:

- `vector_add_batch/2` takes `{Id, Text, Metadata}` (text embedded by the store)
  or `{Id, Text, Metadata, Vector}` (explicit) tuples; a batch must be all one
  shape.
- BM25 is opt-in: open with `vectordb => #{bm25_backend => memory}` (or `disk`).
- `search_hybrid/3` and auto-embedding adds need an embedder configured via
  `barrel_embed`; without one they return `{error, embedder_not_configured}`.

## Changes

```erlang
{ok, Changes, Last} = barrel:changes(Db, first),
Cursor  = barrel:hlc_encode(Last),         %% JSON/URL-safe cursor
Last2   = barrel:hlc_decode(Cursor),
{ok, More, _} = barrel:changes(Db, Last2),  %% changes since the cursor
{ok, Pid} = barrel:subscribe(Db, Last).
```
