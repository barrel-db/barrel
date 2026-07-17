# barrel_server

The multi-protocol server for the barrel edge database. It exposes the `barrel`
API (documents, attachments, vectors, search, changes, timeline) as a
REST/JSON API over HTTP/1.1 and HTTP/2 using `livery`, and the same data as an
MCP endpoint for agents. It holds no database logic: every handler calls
`barrel` through a database lifecycle manager.

You need this when you want to reach a barrel database over the network (other
languages, remote clients, agents) instead of embedding it in an Erlang
application. For embedded use, depend on `barrel` directly and skip this app.

To serve barrel's routes from your own `livery` service (under a sub-path, with
your own auth), use `barrel_server_api:routes/router` rather than the standalone
listener; see the [embedding guide](https://github.com/barrel-db/barrel/blob/main/docs/guides/embedding-barrel-server.md).

[Documentation](https://barrel-db.eu/docs/lib/server/) |
[HexDocs](https://hexdocs.pm/barrel_server) |
[Repository](https://github.com/barrel-db/barrel)

## Build and run

`barrel_server` is opt-in, behind the umbrella `server` profile (it pulls
`livery` and its transports). It is not part of the default embeddable build.

```console
$ rebar3 as server compile
$ rebar3 as server shell
1> application:ensure_all_started(barrel_server).
```

## Configuration

All keys live in the `barrel_server` app env. Set them in `sys.config`, or with
`application:set_env/3` after the app is loaded (loading resets the env from the
`.app` file, so a `set_env` before `ensure_all_started/1` is discarded).

```erlang
[{barrel_server, [
    {http_port, 8080},
    {data_dir, "/var/lib/barrel"},
    %% Request body ceiling. Must clear the largest attachment you sync.
    {max_body, 1073741824},
    %% Options passed to barrel:open_db/2 when a database opens lazily.
    {open_opts, #{}},
    %% Auth. Omit the key entirely to leave the server open. Without an
    %% `accept' key this is bearer-only (unchanged). With `accept' it opts
    %% into bearer | signed (Ed25519) | mtls; see the synchronization guide.
    {auth, #{accept  => [bearer, signed],
             tokens  => [<<"secret-one">>, <<"secret-two">>],
             signers => #{<<"node1">> => <<"...32 raw bytes...">>},
             skew_ms => 300000}},
    %% Listeners. Omit for a single cleartext HTTP/1.1 on http_port; set to
    %% serve HTTP/2 and HTTP/3 and TLS (https/http3 and `tls => true' on http
    %% require the `tls' config below).
    {listeners, #{http => #{port => 8443, tls => true},
                  https => #{port => 8444},
                  http3 => #{port => 8444}}},
    %% Shared TLS for the TLS listeners. `verify => verify_peer' = mTLS gate.
    {tls, #{certfile => "server.pem", keyfile => "server.key",
            cacertfile => "ca.pem", verify => verify_peer}},
    %% CORS. Omit to emit no CORS headers.
    {cors, #{origins => '*'}},
    %% MCP endpoint. Enabled by default when the key is absent.
    {mcp, #{enabled => true, allowed_origins => any}}
]}].
```

## Endpoints

Databases open lazily on first use and are cached by name.

```
GET    /                               liveness text
GET    /health                         {"status":"ok"}

PUT    /db/:db                          open/create a database
GET    /db/:db                          database info
DELETE /db/:db                          close a database

PUT    /db/:db/doc/:id                  body = JSON document
GET    /db/:db/doc/:id                  fetch a document
DELETE /db/:db/doc/:id                  delete a document
GET    /db/:db/doc/:id/_versions        live versions (conflict siblings)
GET    /db/:db/doc/:id/_versions/:rev   one version's body
POST   /db/:db/_bulk_docs               {"docs":[...]}  -> {"results":[...]}
POST   /db/:db/_bulk_get                {"ids":[...]}   -> {"results":[...]}
POST   /db/:db/find                     body = query, returns rows
POST   /db/:db/query                    BQL: {"query":"..."}
GET    /db/:db/query                    BQL via query string
GET    /db/:db/changes                  changes feed (JSON, or SSE)
GET    /db/:db/_history                 provenance history

GET    /db/:db/_timeline                timeline info
POST   /db/:db/_timeline/branch         fork a timeline
POST   /db/:db/_timeline/merge          merge a timeline

PUT    /db/:db/doc/:id/att/:name        body = raw bytes
GET    /db/:db/doc/:id/att/:name        fetch attachment bytes
DELETE /db/:db/doc/:id/att/:name        delete attachment

POST   /db/:db/vector                   {"id","text","metadata","vector"}
POST   /db/:db/search/vector            {"vector":[...],"k":10}
POST   /db/:db/search/bm25              {"query":"...","k":10}
POST   /db/:db/search/hybrid            {"query":"...","k":10}
```

### Replication

Replication runs over the wire against these endpoints. A remote barrel pulls
and pushes through them; you do not call them by hand.

```
GET    /db/:db/_sync/info               peer id, HLC, sync state
POST   /db/:db/_sync/hlc                fold the peer's clock
POST   /db/:db/_sync/changes            changes since a version vector
POST   /db/:db/_sync/diff               which versions the peer is missing
GET    /db/:db/_sync/doc/:id            fetch one version
PUT    /db/:db/_sync/doc/:id            push one version
GET    /db/:db/_sync/local/:id          replication checkpoints
PUT    /db/:db/_sync/local/:id
DELETE /db/:db/_sync/local/:id
GET    /db/:db/_sync/att_changes        attachment feed
POST   /db/:db/_sync/att_diff
GET    /db/:db/_sync/att/:id/:name
PUT    /db/:db/_sync/att/:id/:name
DELETE /db/:db/_sync/att/:id/:name
```

### Agent layer

Spaces, capability grants, sessions, and handoffs from `barrel_spaces`.

```
POST   /spaces                          create a space
GET    /spaces                          list spaces
GET    /spaces/:space                   space info
DELETE /spaces/:space                   drop a space

POST   /spaces/:space/grants            mint a capability token
GET    /spaces/:space/grants            list grants
DELETE /spaces/:space/grants/:token_id  revoke a grant

POST   /spaces/:space/sessions          open a session
GET    /spaces/:space/sessions          list sessions
GET    /spaces/:space/sessions/:sid     session info
DELETE /spaces/:space/sessions/:sid     close a session
POST   /spaces/:space/sessions/:sid/touch      extend the TTL
POST   /spaces/:space/sessions/:sid/messages   append a message
GET    /spaces/:space/sessions/:sid/messages   read messages
PUT    /spaces/:space/sessions/:sid/data/:key  set session data
GET    /spaces/:space/sessions/:sid/data/:key  read session data

POST   /handoffs                        offer a handoff
GET    /handoffs                        list handoffs
POST   /handoffs/accept                 accept a handoff
POST   /handoffs/complete               complete a handoff
```

### MCP

When `mcp` is enabled, `/mcp` serves the Model Context Protocol (`POST`, `GET`,
`DELETE`, `OPTIONS`) over the same databases: resources, tools, and the agent
layer. It carries its own origin policy, so the CORS middleware skips it.

## Examples

```console
$ curl -X PUT localhost:8080/db/mydb
{"ok":true,"db":"mydb"}

$ curl -X PUT localhost:8080/db/mydb/doc/a \
    -H 'content-type: application/json' -d '{"title":"hello"}'
{"id":"a","ok":true,"rev":"0000019f46b4c08900000000@8c0010c983917d4b"}

$ curl localhost:8080/db/mydb/doc/a
{"_rev":"0000019f46b4c08900000000@8c0010c983917d4b","id":"a","title":"hello"}

$ curl localhost:8080/db/mydb/changes
{"last":"AAABn0a0wIkAAAAA","changes":[{"id":"a","rev":"0000019f46b4c089...",
 "hlc":"AAABn0a0wIkAAAAA","changes":[{"rev":"0000019f46b4c089..."}],
 "num_conflicts":0}]}

$ curl localhost:8080/db/mydb
{"name":"mydb","config":{},"db_path":"/var/lib/barrel/mydb","att_floor":null,
 "history_floor":null,"keyspace":"mydb","retention_period":2592000}
```

A `rev` is a version token, `<hex(hlc)>@<author>`: the HLC of the write and the
id of the database that authored it. There is no revision tree.

## Authentication

Omit the `auth` key and the server stays open. Configure it and every route
except `/health` requires `Authorization: Bearer <token>`.

Two kinds of bearer are accepted:

- **Global tokens**, from `{auth, #{tokens => [Bin]}}`. They open every route.
  Pass a list so you can rotate. Comparison is constant time.
- **Capability tokens** (`bsp_...`), minted by `barrel_caps` for one space. They
  authenticate the agent-layer routes, and the `/db/:db` surface scoped to the
  space they grant. The server maps method and path to a required right (reads
  need `read`, writes and push need `write`) and checks the database is the
  granted space.

Unmapped routes answer 403, so a new route has to be classified before it can be
reached with a capability token. Bad or revoked tokens answer 401.

## Notes

- The changes feed returns JSON by default. Request `Accept: text/event-stream`
  (or `?feed=sse`) for Server-Sent Events. The `?since=<cursor>` parameter takes
  a cursor from a prior response's `last` field.
- CORS runs in front of auth, so preflights answer 204 without a token and 401
  bodies still carry CORS headers. `expose` defaults to the `x-barrel-*` headers
  a client needs to fold the HLC clock.
- The database manager does not trap exits: if an open store crashes, the
  manager restarts with an empty cache and databases reopen on the next request.
- gRPC, WebTransport, and a unix-socket adapter are later phases.
