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
default). `barrel` adds no storage of its own; it coordinates the layers. Each
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

## Identify the embedder

```erlang
{ok, #{fingerprint := Fp, model := Model, dimensions := Dim}} = barrel:embedder_info(Db).
```

`embedder_info/1` returns `provider`, `model`, `revision`, `dimensions`,
`distance`, `preprocessing` and a `fingerprint` (`sha256:` of their canonical
JSON). Two databases with the same fingerprint produce comparable vector
scores. `info/1` carries the same identity under `embedder`. There is no
fingerprint without a configured embedder.

## Know which state answered a query

`query/2,3` and `query_fold/5` results carry the database's `instance_id`
and `last_seq` in their meta, for collection queries and table functions
alike:

```erlang
{ok, _Rows, #{instance_id := Id, last_seq := Seq}} =
    barrel:query(Db, <<"SELECT id FROM c LIMIT 10">>).
```

## Open a copy read only

```erlang
{ok, Ro} = barrel:open(<<"snapshot">>, #{read_only => true}).
{error, read_only} = barrel:put_doc(Ro, #{<<"id">> => <<"x">>}).
```

Both stores open read only and write no file; record mode persists no policy
and starts no indexer. Add `embedding => stored` to run record mode with the
policy the database persisted (`{error, no_stored_policy}` on a plain
database). A store an older version wrote fails with
`read_only_upgrade_needed` until one writable open upgrades it.

## Keep databases open with barrel_dbs

`barrel_dbs` owns long-lived handles for servers: it opens lazily, closes idle
databases and evicts at `dbs_max_open`.

```erlang
{ok, Db} = barrel_dbs:ensure(<<"docs">>, #{must_exist => true}),
{ok, Db, Lease} = barrel_dbs:lease(<<"docs">>, #{}),
%% ... the database stays open while the lease is held ...
ok = barrel_dbs:release(Lease).
```

- `must_exist => true` answers `{error, not_found}` instead of creating a
  database on a cold open.
- `lease/2` is counted and monitored: idle close and eviction skip a leased
  database until `release/1` or until the holder exits. `leases/0` lists the
  counts.
- `hold/2` takes exclusive file access: it closes the database and refuses
  every `ensure` until `unhold/1`. It is refused on a pinned or leased
  database, one owned by another tag, or one open outside the manager.
- `lookup/1` returns the pinned flag, the owner tag and the open options.

## Export and import a database

Export copies a closed database with a checksummed manifest; import verifies
every file and serves the copy read only.

```erlang
{ok, _} = barrel_ctx_export:export(<<"docs">>, "/srv/export/docs_g1",
                                   #{generation => 1}),
{ok, #{name := Name, db := Copy}} = barrel_ctx_export:import("/srv/export/docs_g1", #{}).
```

- The export holds the database (`barrel_dbs:hold/2`) during the copy.
  Encrypted databases export as ciphertext.
- A record-mode policy that holds a secret (`api_key`, `token`, ...) is
  refused with `{policy_holds_secret, Key}`.
- Import resumes from files already verified and renames into place only
  when complete, then opens the copy (`open => false` skips it; reopen later
  with `barrel_ctx_export:open(Name)`). Opening an import writes nothing, so several nodes can
  serve one directory. `list_imports/0`, `import_info/1` and
  `remove_import/1` manage them.

## Contexts

`barrel_ctx` queries several databases, local, imported or on other
`barrel_server` nodes, with one BQL statement, and keeps working sets you can
query offline:

```erlang
{ok, #{<<"id">> := _}} = barrel_ctx:register(
    #{<<"name">> => <<"otp/sasl">>,
      <<"locations">> => [#{<<"kind">> => <<"local">>, <<"db">> => <<"otp_sasl">>}]}),
{ok, #{execution := succeeded, rows := Rows, summary := Summary}} =
    barrel_ctx:query(#{query => <<"SELECT id, lines FROM c ORDER BY lines DESC LIMIT 10">>,
                       contexts => [<<"otp/sasl">>]}).
```

Read [the contexts guide](https://github.com/barrel-db/barrel/blob/main/docs/guides/contexts.md) for the query
shapes, merges, working sets, slices, offline mode and errors.

## API surface

- Lifecycle: `open/1,2` (`read_only`, `embedding => stored`), `close/1`, `info/1`
- Documents: `put_doc/2,3`, `put_docs/2,3`, `get_doc/2,3`, `get_docs/2,3`,
  `delete_doc/2`, `delete_docs/2`, `find/2,3`
- Attachments: `put_attachment/4`, `get_attachment/3`, `delete_attachment/3`,
  `list_attachments/2`, `attachment_info/3`, plus the streaming reader/writer
- Changes: `changes/2,3`, `subscribe/2,3`, `hlc_encode/1`, `hlc_decode/1`
- Vectors: `vector_add/4,5`, `vector_add_batch/2`, `vector_get/2`,
  `vector_delete/2`, `search/3`, `search_vector/3`, `search_bm25/3`,
  `search_hybrid/3`, `vector_stats/1`
- Embedding with the database's own embedder: `embed/2`, `embed_batch/2`,
  `embedder_info/1`
- Queries: `query/2,3`, `query_fold/5` (BQL, observed version in meta)
- Lifecycle manager: `barrel_dbs:ensure/1,2`, `lease/2`, `release/1`,
  `leases/0`, `hold/2`, `unhold/1`, `lookup/1`, `pin/1`, `unpin/1`
- Portability: `barrel_ctx_export:export/3`, `import/2`, `open/1`,
  `list_imports/0`, `import_info/1`, `remove_import/1`
- Contexts: `barrel_ctx` (catalog, `query/1`, working sets, offline mode)
