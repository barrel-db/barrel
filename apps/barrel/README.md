# barrel

The embeddable edge-AI database. `barrel` composes the document layer
(`barrel_docdb`), the vector layer (`barrel_vectordb`), and object storage
(`barrel_objectdb`, via the docdb attachment backend) behind one API, so an
Erlang application can embed a single database that does documents, vectors,
BM25, hybrid search, attachments, and a changes feed.

A barrel database is a docdb database plus a vectordb store that share a name and
a single id space: a document and its vector are addressed by the same id. The
facade adds no storage of its own; it coordinates the layers. Each underlying app
stays usable on its own.

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
- Documents: `put_doc/2,3`, `get_doc/2,3`, `delete_doc/2`, `find/2,3`
- Attachments: `put_attachment/4`, `get_attachment/3`, `delete_attachment/3`,
  `list_attachments/2`, `attachment_info/3`, plus the streaming reader/writer
- Changes: `changes/2,3`, `subscribe/2,3`
- Vectors: `vector_add/4,5`, `vector_get/2`, `vector_delete/2`, `search/3`,
  `search_vector/3`, `search_bm25/3`, `search_hybrid/3`, `vector_stats/1`
