# barrel_server

The multi-protocol server for the barrel edge database. It exposes the `barrel`
facade (documents, attachments, vectors, search, changes) as a REST/JSON API
over HTTP/1.1 and HTTP/2 using `livery`. It holds no database logic: every
handler calls `barrel` through a database lifecycle manager.

You need this when you want to reach a barrel database over the network (other
languages, remote clients) instead of embedding it in an Erlang application. For
embedded use, depend on `barrel` directly and skip this app.

## Build and run

`barrel_server` is opt-in, behind the umbrella `server` profile (it pulls
`livery` and its transports). It is not part of the default embeddable build.

```console
$ rebar3 as server compile
$ rebar3 as server shell
1> application:ensure_all_started(barrel_server).
```

The HTTP port defaults to `8080` (`barrel_server` app env `http_port`). Set
`data_dir` in the same app env to choose where databases are stored.

## Endpoints

Databases open lazily on first use and are cached by name.

```
GET    /                          liveness text
GET    /health                    {"status":"ok"}

PUT    /db/:db                     open/create a database
GET    /db/:db                     database info
DELETE /db/:db                     close a database

PUT    /db/:db/doc/:id             body = JSON document
GET    /db/:db/doc/:id             fetch a document
DELETE /db/:db/doc/:id             delete a document
POST   /db/:db/_bulk_docs          {"docs":[...]} -> {"results":[...]}
POST   /db/:db/_bulk_get           {"ids":[...]}  -> {"results":[...]}
POST   /db/:db/find                body = query, returns rows
GET    /db/:db/changes            changes feed (JSON, or SSE via Accept)

PUT    /db/:db/doc/:id/att/:name   body = raw bytes
GET    /db/:db/doc/:id/att/:name   fetch attachment bytes
DELETE /db/:db/doc/:id/att/:name   delete attachment

POST   /db/:db/vector              {"id","text","metadata","vector"}
POST   /db/:db/search/vector       {"vector":[...],"k":10}
POST   /db/:db/search/bm25         {"query":"...","k":10}
POST   /db/:db/search/hybrid       {"query":"...","k":10}
```

## Examples

```console
$ curl -X PUT localhost:8080/db/mydb
{"db":"mydb","ok":true}

$ curl -X PUT localhost:8080/db/mydb/doc/a \
    -H 'content-type: application/json' -d '{"title":"hello"}'
{"id":"a","ok":true,...}

$ curl localhost:8080/db/mydb/doc/a
{"_rev":"1-...","id":"a","title":"hello"}

$ curl localhost:8080/db/mydb/changes
{"changes":[{"id":"a","rev":"1-...","hlc":"..."}],"last":"..."}
```

## Notes

- The changes feed returns JSON by default. Request `Accept: text/event-stream`
  (or `?feed=sse`) for Server-Sent Events. The `?since=<cursor>` parameter takes
  a cursor from a prior response's `last` field.
- The database manager does not trap exits: if an open store crashes, the
  manager restarts with an empty cache and databases reopen on the next request.
- gRPC, WebTransport, a unix-socket adapter, and replication transport are later
  phases.
