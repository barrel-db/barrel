# Run the REST server

`barrel_server` exposes the `barrel` database as a REST/JSON API using `livery`.
By default it serves cleartext HTTP/1.1; HTTP/2 and HTTP/3 (over TLS) are opt-in
via the `listeners` config (see [synchronization](synchronization.md)). It holds
no database logic: every handler calls the `barrel` module through a database
lifecycle manager. Read this when you want to reach a barrel database over the
network instead of embedding it.

## When to use it

- You want HTTP access to documents, attachments, vectors, search, the
  changes feed, and contexts (from other languages or remote clients).
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

Databases open lazily on first use through Barrel's database lifecycle manager
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
GET    /db/:db/query?q=            the same, for EventSource clients
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
POST|GET /contexts, /contexts/:id, /contexts/_query, /worksets, ...
                                   contexts and working sets (below)
POST|GET /mcp                      the MCP endpoint (see the mcp guide)
```

## Bound a query and read what answered

`POST /db/:db/query` takes the statement as raw BQL text, or as JSON with
`query`, `params`, `continuation`, `max_rows` and `deadline_ms`. `GET`
takes the same as `?q=`, `?max_rows=`, `?deadline_ms=`.

```console
$ curl -XPOST localhost:8080/db/mydb/query -H 'content-type: application/json' \
    -d '{"query": "SELECT id, title FROM c ORDER BY id LIMIT 2", "max_rows": 100, "deadline_ms": 2000}'
{"row":{"id":"a","title":"hello"}}
{"row":{"id":"b","title":"world"}}
{"meta":{"has_more":false,"bound":"limit_reached","instance_id":"19ab40285574d671","last_seq":"AAABoNO_U7sAAAAE"}}
```

- `max_rows` caps the rows streamed (at most 1000); `deadline_ms` bounds the
  request (at most 300000). A value that is not a positive integer answers
  400 `invalid_max_rows` or `invalid_deadline_ms`.
- The last line is `{"meta": ...}`: `bound` is `limit_reached` when the cap
  cut the stream or the statement's `LIMIT` or `k` was filled (more rows may
  exist), `exhausted` otherwise; `instance_id` and `last_seq` (base64url)
  name the state of the database that answered; `continuation` is present
  when the statement pages.
- A `vector_top_k` answer adds `embedding: {fingerprint, distance,
  dimensions}` to the meta, so a caller merging answers from several
  databases can check that their scores compare.
- When the deadline passes after rows started, the stream ends with an
  in-band `{"error":"deadline"}` line and no meta. Any other failure after
  the first row ends the same way with its reason.

## Contexts and working sets

The `contexts` route group serves context cards, federated queries, and
working sets. The [contexts guide](contexts.md) walks through each call
with real responses.

```
POST   /contexts                    register a card (201)
GET    /contexts                    list (?prefix=, ?unlisted=true); ?q= discovers
GET    /contexts/:id                one card, by id or URL-encoded name
DELETE /contexts/:id                unregister
GET    /contexts/_capabilities      accepted query shapes, merges, limits, budgets
POST   /contexts/_query             one BQL statement over several contexts
GET    /contexts/_offline           {"offline": bool}
PUT    /contexts/_offline           {"offline": true | false}

POST   /worksets                    create ({"owner", "budget"}, 201)
GET    /worksets                    list
GET    /worksets/:ws                read one
DELETE /worksets/:ws                delete it and its slices
POST   /worksets/:ws/members        attach {"context", "mode", "credential_ref"}
DELETE /worksets/:ws/members/:ctx   detach
POST   /worksets/:ws/_materialize   save a query's documents as slices
POST   /worksets/:ws/_import        import an exported snapshot {"dir", "name"}
```

```console
$ curl -XPOST localhost:8080/contexts/_query -H 'content-type: application/json' \
    -d '{"query": "SELECT id, path FROM c ORDER BY path LIMIT 10",
         "contexts": ["otp/tools", "otp/sasl"]}'
```

The answer (200) carries `execution` (`succeeded`, `partial`, `failed`),
`rows` or `groups`, one `sources` entry per context, `coverage`, and a
`summary`. A context that did not answer is reported in its source, not as
a request error. A request error answers one shape with an HTTP status per
code (400 for a bad argument or statement, 404 `unknown_context`, 409
`ambiguous_context`, 413 `over_budget`, 403 `forbidden`, 502
`source_unavailable`, 500 `internal`):

```json
{"error": "limit_required",
 "message": "A row query over contexts needs a LIMIT.",
 "hint": "Add LIMIT n (n <= 1000), and ORDER BY a selected field to merge rows in order.",
 "details": {"max_limit": 1000}}
```

A capability token can read cards and run `POST /contexts/_query`; each
local context is checked as a `POST /db/:db/query` on that database.
Registering cards, working sets, imports and switching offline need a
global token. Tokens for remote servers live in the node's
`ctx_credentials`, never in a card.

## Auth

Unconfigured, the server is open. Set bearer tokens to lock it:

```erlang
{barrel_server, [{auth, #{tokens => [<<"s3cret">>]}}]}
```

Every route except `/health` then requires `Authorization: Bearer <token>`.
Two kinds of bearer: global tokens (the list above, full access, a list
makes rotation possible) and capability tokens (`bsp_...`, issued per space
by `barrel_caps`). A capability token authenticates the `/spaces` and
`/handoffs` routes, and its own space's `/db/:db/*` routes when `:db` is the
granted space: `read` opens the pull leg (GETs, `changes`, `query`,
`search`, and the `_sync` reads), `write` adds document writes and the push
leg (`_sync/doc` PUT, `_sync/local` and `_sync/att` writes). Database
lifecycle (`PUT`/`DELETE /db/:db`), `_timeline`, and any unmapped route stay
off-limits to capability tokens (403, fail closed); for the contexts routes
see [Contexts and working sets](#contexts-and-working-sets); dead or wrong-space
tokens answer 401. `/mcp` authenticates through its own provider covering
both kinds. See [spaces](spaces.md), [mcp](mcp.md), and
[barrel-lite](barrel-lite.md).

The bearer behavior above is unchanged and is what you get with no extra
config. Two stronger methods are **opt-in**, added by an `accept` list on the
same `auth` key (its absence leaves everything exactly as described above):
Ed25519 **signed requests** (replay-protected, no TLS required) and an **mTLS**
transport gate. `accept => [bearer, signed]` accepts either, so a fleet can
roll over node by node. See the [synchronization guide](synchronization.md) for
the config and the client side.

## CORS

Browser clients need CORS. Unconfigured, no CORS headers are sent; set an
origin policy to enable it:

```erlang
{barrel_server, [{cors, #{
    origins => '*',                        %% or [<<"https://app.example">>]
    expose  => [<<"x-barrel-hlc">>,        %% default; the client folds this
                <<"x-barrel-digest">>, <<"x-barrel-att-length">>],
    max_age => 600
}}]}
```

Preflight `OPTIONS` requests are answered without a bearer, and error
responses still carry CORS headers so browser JS can read them. `/mcp` keeps
its own origin policy. See [barrel-lite](barrel-lite.md).

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
  (or `?feed=sse`) for Server-Sent Events (one-shot: the current window then
  close). `?feed=continuous` holds the SSE stream open, pushing each change as a
  data line with a 30s heartbeat, until the client disconnects. `?since=<cursor>`
  takes a cursor from a prior response's `last` field (or a change's `hlc`).
- Databases open with the default vector store (768-dim, BM25 off). The
  `/search/bm25` and `/search/hybrid` endpoints need BM25 enabled, and hybrid
  needs an embedder.
- Optimistic concurrency: `PUT /db/:db/doc/:id` with a `_rev` in the body that
  is not the current winner answers 409 `{"error":"conflict"}`.
- Replication over the wire ships today (the `/db/:db/_sync/*` endpoints; see
  [synchronization](synchronization.md)), with bearer, Ed25519 signed-request,
  and mTLS auth (all opt-in; bearer is the default). HTTP/2 and HTTP/3 serving
  ship (opt-in via `listeners`); note H3 is TLS-serving but not yet a client-cert
  gate. gRPC, WebTransport, a unix-socket adapter, and OpenAPI are later phases.
