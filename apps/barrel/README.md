# barrel

The embeddable edge-AI database. `barrel` composes the document layer
(`barrel_docdb`) and the vector layer (`barrel_vectordb`) behind one API, so an
Erlang application can embed a single database that does documents, vectors,
BM25, hybrid search, attachments (blobs), and a changes feed.

[Documentation](https://barrel-db.eu/docs/lib/barrel/) |
[HexDocs](https://hexdocs.pm/barrel) |
[Repository](https://github.com/barrel-db/barrel)

A barrel database is a docdb database plus a vectordb store that share a name and
a single id space: a document, its attachments (blobs), and its vector are all
addressed by the same id. Blobs are docdb attachments; the storage backend is
pluggable per database via the docdb `barrel_att_backend` seam (RocksDB BlobDB by
default). The facade adds no storage of its own; it coordinates the layers. Each
underlying app stays usable on its own.

## Open and documents

```erlang
{ok, Db} = barrel:open(mydb),
{ok, _}  = barrel:put_doc(Db, #{<<"id">> => <<"a">>, <<"title">> => <<"hello">>}),
{ok, Doc} = barrel:get_doc(Db, <<"a">>),
{ok, Rows, _Meta} = barrel:find(Db, #{where => [{path, [<<"title">>], <<"hello">>}]}),
ok = barrel:close(Db).
```

`barrel:open/2` accepts `#{docdb => Map, vectordb => Map}` to pass options to each
layer, including `docdb => #{att_opts => #{backend => ...}}` to choose an
attachment backend.

## Batches

```erlang
[{ok, _}, {ok, _}] = barrel:put_docs(Db, [#{<<"id">> => <<"a">>}, #{<<"id">> => <<"b">>}]),
[{ok, _}, {ok, _}] = barrel:get_docs(Db, [<<"a">>, <<"b">>]),
{ok, #{inserted := 2}} = barrel:vector_add_batch(Db, [
    {<<"a">>, <<"t1">>, #{}, V1},
    {<<"b">>, <<"t2">>, #{}, V2}
]).
```

`vector_add_batch/2` takes `{Id, Text, Metadata}` (text embedded by the store) or
`{Id, Text, Metadata, Vector}` (explicit) tuples; a batch must be all one shape.

## Vectors and search

```erlang
ok = barrel:vector_add(Db, <<"a">>, <<"hello world">>, #{}, [0.1, 0.2, 0.3]),
{ok, Hits} = barrel:search_vector(Db, [0.1, 0.2, 0.3], #{k => 5}),
{ok, Hits2} = barrel:search_hybrid(Db, <<"hello">>, #{k => 5}).
```

## Attachments

```erlang
{ok, _} = barrel:put_attachment(Db, <<"a">>, <<"file.txt">>, <<"bytes">>),
{ok, <<"bytes">>} = barrel:get_attachment(Db, <<"a">>, <<"file.txt">>),
[<<"file.txt">>] = barrel:list_attachments(Db, <<"a">>).
```

Large attachments stream: `open_attachment_writer/4` + `write_attachment/2` +
`finish_attachment/1`, and `open_attachment_reader/3` + `read_attachment/1`.

## Changes

```erlang
{ok, Changes, LastHlc} = barrel:changes(Db, first),
{ok, StreamPid} = barrel:subscribe(Db, LastHlc).
```

## API surface

- Lifecycle: `open/1,2`, `close/1`, `info/1`
- Documents: `put_doc/2,3`, `put_docs/2,3`, `get_doc/2,3`, `get_docs/2,3`,
  `delete_doc/2`, `delete_docs/2`, `find/2,3`
- Attachments: `put_attachment/4`, `get_attachment/3`, `delete_attachment/3`,
  `list_attachments/2`, `attachment_info/3`, plus the streaming reader/writer
- Changes: `changes/2,3`, `subscribe/2,3`, `hlc_encode/1`, `hlc_decode/1`
- Vectors: `vector_add/4,5`, `vector_add_batch/2`, `vector_get/2`,
  `vector_delete/2`, `search/3`, `search_vector/3`, `search_bm25/3`,
  `search_hybrid/3`, `vector_stats/1`
