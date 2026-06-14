# barrel

The embeddable edge-AI database. `barrel` composes the document layer
(`barrel_docdb`) and the vector layer (`barrel_vectordb`) behind one API, so an
Erlang application can embed a single database that does documents, vectors, BM25,
and hybrid search.

This is the Phase 0 facade: it opens a docdb database and a vectordb store under a
shared name and delegates to them. Later phases add a single document/vector id
space, attachments backed by `barrel_objectdb`, changes and replication, optional
encryption, and a network server (`barrel_server`).

## Use

```erlang
{ok, Db} = barrel:open(mydb),
{ok, _}  = barrel:put_doc(Db, #{<<"id">> => <<"a">>, <<"title">> => <<"hello">>}),
{ok, Doc} = barrel:get_doc(Db, <<"a">>),
ok = barrel:close(Db).
```

`barrel:open/2` accepts `#{docdb => Map, vectordb => Map}` to pass options through
to each layer. Each underlying app stays usable on its own.
