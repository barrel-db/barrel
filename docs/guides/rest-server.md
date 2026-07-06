# Run the REST server

`barrel_server` exposes the `barrel` database over HTTP/1.1 and HTTP/2 (REST/JSON)
using `livery`. It holds no database logic: every handler calls the `barrel`
facade through a database lifecycle manager. Read this when you want to reach a
barrel database over the network instead of embedding it.

## When to use it

- You want HTTP access to documents, attachments, vectors, search, and the
  changes feed (from other languages or remote clients).
- For in-process Erlang use, embed `barrel` directly instead (see the embedding
  guide).

## Build and run

`barrel_server` is opt-in, behind the umbrella `server` profile (it pulls
`livery` and its transports). It is not part of the default embeddable build.

```console
$ rebar3 as server shell
1> application:ensure_all_started(barrel_server).
```

Configure with the `barrel_server` app env: `http_port` (default `8080`) and
`data_dir` (where databases are stored). Set them before the app starts, for
example in `sys.config`.

## Endpoints

Databases open lazily on first use through the facade's lifecycle manager
(`barrel_dbs`): handles are cached by name, idle databases close after
`dbs_idle_timeout` (barrel app env, default 5 minutes, 0 disables), and
`dbs_max_open` evicts the least recently used past a cap.

```
GET    /                          liveness text
GET    /health                    {"status":"ok"}

PUT    /db/:db                     open/create a database
GET    /db/:db                     database info
DELETE /db/:db                     close a database (?purge=true deletes)

PUT    /db/:db/doc/:id             body = JSON document
GET    /db/:db/doc/:id             fetch a document
DELETE /db/:db/doc/:id             delete a document
POST   /db/:db/_bulk_docs          {"docs":[...]} -> {"results":[...]}
POST   /db/:db/_bulk_get           {"ids":[...]}  -> {"results":[...]}
POST   /db/:db/find                body = query, returns rows
POST   /db/:db/query               BQL (ndjson rows; SUBSCRIBE over SSE)
GET    /db/:db/changes            changes feed (JSON, or SSE via Accept)

GET    /db/:db/_history            audit trail (see audit-provenance guide)
GET    /db/:db/doc/:id/_versions[/:rev]   past versions and bodies

GET    /db/:db/_timeline           lineage; POST .../branch, .../merge
POST   /db/:db/_sync/*             replication wire (see synchronization)

PUT    /db/:db/doc/:id/att/:name   body = raw bytes
GET    /db/:db/doc/:id/att/:name   fetch attachment bytes
DELETE /db/:db/doc/:id/att/:name   delete attachment

POST   /db/:db/vector              {"id","text","metadata","vector"}
POST   /db/:db/search/vector       {"vector":[...],"k":10}
POST   /db/:db/search/bm25         {"query":"...","k":10}
POST   /db/:db/search/hybrid       {"query":"...","k":10}

POST|GET /spaces, /spaces/:space, .../grants, .../sessions, /handoffs
                                   the agent layer (see the spaces guide)
POST|GET /mcp                      the MCP endpoint (see the mcp guide)
```

## Auth

Unconfigured, the server is open. Set bearer tokens to lock it:

```erlang
{barrel_server, [{auth, #{tokens => [<<"s3cret">>]}}]}
```

Every route except `/health` then requires `Authorization: Bearer <token>`.
Two kinds of bearer: global tokens (the list above, full access, a list
makes rotation possible) and capability tokens (`bsp_...`, issued per space
by `barrel_caps`), which authenticate only the `/spaces` and `/handoffs`
routes and are checked per route against their space and rights. `/mcp`
authenticates through its own provider covering both kinds. See
[spaces](spaces.md) and [mcp](mcp.md).

## Examples

```console
$ curl -X PUT localhost:8080/db/mydb
{"db":"mydb","ok":true}

$ curl -X PUT localhost:8080/db/mydb/doc/a \
    -H 'content-type: application/json' -d '{"title":"hello"}'
{"id":"a","ok":true,...}

$ curl localhost:8080/db/mydb/doc/a
{"_rev":"1-...","id":"a","title":"hello"}

$ curl -X POST localhost:8080/db/mydb/_bulk_docs \
    -H 'content-type: application/json' -d '{"docs":[{"id":"b"},{"id":"c"}]}'
{"results":[{"id":"b",...},{"id":"c",...}]}

$ curl localhost:8080/db/mydb/changes
{"changes":[{"id":"a","rev":"1-...","hlc":"..."}],"last":"..."}
```

## Notes

- The changes feed returns JSON by default. Request `Accept: text/event-stream`
  (or `?feed=sse`) for Server-Sent Events. `?since=<cursor>` takes a cursor from a
  prior response's `last` field.
- Databases open with the default vector store (768-dim, BM25 off). The
  `/search/bm25` and `/search/hybrid` endpoints need BM25 enabled, and hybrid
  needs an embedder.
- gRPC, HTTP/3, WebTransport, a unix-socket adapter, OpenAPI, and replication
  over the wire are later phases.
