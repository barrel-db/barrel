# Contexts

A context is a named dataset (one Barrel database today) that you query
together with other contexts, wherever they live: on this node or on other
`barrel_server` nodes. You register one card per context on the node your
agent talks to, then run one BQL statement over several contexts by name.
Every answer says which contexts answered, why the others did not, and what
each answer covers, in fields and in a one-paragraph `summary`. When you need
answers without the network, you keep a working set: a list of contexts,
some remote, some saved locally as slices of query results or as imported
snapshots. Use this guide when an agent (over MCP) or a program (over REST or
Erlang) needs to search or list data spread over several Barrel servers, or
keep working offline.

Contexts ship in barrel 1.10.0 (the Erlang API, `barrel_ctx`) and
barrel_server 1.10.0 (the REST routes and MCP tools). Every request and
response below comes from a run of `scripts/contexts-demo.sh`, trimmed with
`"..."`; ids and sequence numbers differ on each run. The design behind it
is in [contexts](../architecture/contexts.md).

## Run the demo

The script starts three nodes on loopback (L on 18081, R1 on 18082, R2 on
18083), loads one OTP application per node, and drives node L over MCP.

```sh
rebar3 as server compile
scripts/contexts-demo.sh /path/to/corpus.jsonl
# ... passed: 36, failed: 0
```

`WORK_DIR` and `OUT_DIR` choose where node data and the transcript go (the
script wipes `WORK_DIR`); `PORT_L`, `PORT_R1`, `PORT_R2` and `NODE_PREFIX` let
two runs share a host.

## Register a context

Register one card per context on the node that runs the queries (global
auth). A local card names a database on this node; a remote card names
another server and a database there.

```sh
curl -XPOST localhost:18081/contexts -H 'content-type: application/json' -d '
  {"name": "otp/sasl", "title": "OTP sasl", "topics": ["erlang", "release"],
   "locations": [{"kind": "remote", "endpoint": "http://127.0.0.1:18082",
                  "db": "otp_sasl"}]}'
```

```json
{"id": "ctx_5loieq7q3hdaxluhwkklz5px", "name": "otp/sasl", "type": "context_card",
 "discoverable": "listed", "format": 1, "title": "OTP sasl", "topics": ["erlang", "release"],
 "locations": [{"db": "otp_sasl", "endpoint": "http://127.0.0.1:18082", "kind": "remote"}]}
```

From here on you can use the name `otp/sasl` or the id everywhere. Cards
never hold secrets; tokens for remote servers live in node configuration:

```erlang
{barrel, [{ctx_credentials, #{<<"http://127.0.0.1:18082">> => <<"token">>}}]}
```

## Learn what a query may look like

Call `context_capabilities` (REST: `GET /contexts/_capabilities`) before a
first query.

```json
{"name": "context_capabilities", "arguments": {}}
```

```json
{"shapes": [
   {"shape": "ordered_rows", "requires": ["LIMIT n", "ORDER BY a selected field"], "merge": "ordered",
    "example": "SELECT id, path, lines FROM c WHERE lines > 300 ORDER BY lines DESC LIMIT 10"},
   {"shape": "unordered_rows", "requires": ["LIMIT n"], "merge": "grouped", "...": "..."},
   {"shape": "retrieval", "functions": ["bm25_top_k", "vector_top_k", "hybrid_top_k"],
    "merge": "grouped; vector_top_k is merged by score when every context reports the same embedding",
    "example": "SELECT b.id, b.path, b._score FROM bm25_top_k('release upgrade', k => 5) AS b"}],
 "bql": "Strings use single quotes ('text'); double quotes name fields. The collection alias is free (FROM c).",
 "limits": {"max_contexts": 8, "max_limit": 1000,
            "deadline_ms": {"default": 5000, "max": 60000},
            "per_context_timeout_ms": {"default": 4000, "max": 60000},
            "max_parallel": {"default": 8, "max": 16}},
 "refused_merges": {"rrf": "rank fusion over disjoint corpora is not a relevance order",
                    "rerank": "no validated reranker yet"},
 "rejected": ["SUBSCRIBE", "OFFSET", "UNNEST", "continuation"],
 "offline": false, "...": "..."}
```

## Find a context

`context_list` returns every listed card; `context_discover` keeps the cards
where every word of `q` appears in the name, title, description or topics;
`context_inspect` reads one card by name or id.

```json
{"name": "context_discover", "arguments": {"q": "release"}}
```

```json
{"contexts": [{"id": "ctx_5loieq7q3hdaxluhwkklz5px", "name": "otp/sasl", "...": "..."}],
 "summary": "1 context matches 'release': otp/sasl. A filter, not a ranking: other contexts may still hold answers."}
```

A mistyped name answers the closest names:

```json
{"name": "context_inspect", "arguments": {"context": "otp/sasll"}}
```

```json
{"error": "unknown_context",
 "message": "No context has the id or name otp/sasll.",
 "hint": "Did you mean: otp/sasl? context_list (GET /contexts) shows every context.",
 "details": {"context": "otp/sasll",
             "suggestions": [{"id": "ctx_5loieq7q3hdaxluhwkklz5px", "name": "otp/sasl"}]}}
```

## Query several contexts

`context_query` (REST: `POST /contexts/_query`) takes one statement and the
contexts by name. Rows are merged in order when the statement has `ORDER BY`
on a selected field and a `LIMIT`:

```json
{"name": "context_query",
 "arguments": {"query": "SELECT id, path, lines FROM c WHERE lines > 300 ORDER BY lines DESC LIMIT 10",
               "contexts": ["otp/tools", "otp/sasl", "otp/eunit"]}}
```

```json
{"execution": "succeeded", "merge": "ordered",
 "summary": "All 3 contexts answered (otp/tools, otp/sasl, otp/eunit). 10 rows are merged across contexts in ORDER BY order. otp/tools filled the LIMIT or k; more matches may exist.",
 "rows": [
   {"_ctx": "ctx_uuojp36sme7ym4pjb2faocuo", "_ctx_name": "otp/tools", "id": "fprof", "lines": 3631, "path": "tools-4.2.1/src/fprof.erl"},
   {"_ctx": "ctx_uuojp36sme7ym4pjb2faocuo", "_ctx_name": "otp/tools", "id": "cover", "lines": 3120, "path": "tools-4.2.1/src/cover.erl"},
   {"_ctx": "ctx_5loieq7q3hdaxluhwkklz5px", "_ctx_name": "otp/sasl", "id": "release_handler", "lines": 3092, "path": "sasl-4.4/src/release_handler.erl"},
   "..."],
 "sources": [
   {"context": "ctx_uuojp36sme7ym4pjb2faocuo", "name": "otp/tools", "status": "ok", "rows": 10,
    "bound": "limit_reached", "retrieval": "exact", "membership": "live",
    "location": {"kind": "local", "db": "otp_tools"},
    "version": {"kind": "live", "observed": {"instance_id": "19ab40285574d671", "last_seq": "AAABoNO_U7sAAAAE"}}},
   {"context": "ctx_5loieq7q3hdaxluhwkklz5px", "name": "otp/sasl", "status": "ok", "rows": 8,
    "bound": "exhausted", "bytes": 753, "...": "..."},
   "..."],
 "coverage": {"requested": 3, "answered": 3, "failed": 0, "skipped": 0, "missing": [],
              "scope_origin": "explicit"}}
```

Retrieval (`bm25_top_k`, `hybrid_top_k`) comes back grouped per context:

```json
{"name": "context_query",
 "arguments": {"query": "SELECT b.id, b.path, b._score FROM bm25_top_k('release upgrade', k => 3) AS b",
               "contexts": ["otp/tools", "otp/sasl", "otp/eunit"]}}
```

```json
{"execution": "succeeded", "merge": "grouped", "relevance": false,
 "summary": "All 3 contexts answered (otp/tools, otp/sasl, otp/eunit). Results are grouped per context because BM25 scores are not comparable across contexts. otp/tools, otp/sasl filled the LIMIT or k; more matches may exist.",
 "groups": [
   {"context": "ctx_uuojp36sme7ym4pjb2faocuo", "name": "otp/tools",
    "rows": [{"id": "xref", "_score": 2.798410656725898, "...": "..."}, "..."]},
   {"context": "ctx_5loieq7q3hdaxluhwkklz5px", "name": "otp/sasl",
    "rows": [{"id": "systools_relup", "_score": 5.142367995611812, "...": "..."}, "..."]},
   {"context": "ctx_zuepupiv6vxtzjlqz2cszqsv", "name": "otp/eunit", "rows": []}],
 "...": "..."}
```

A statement that cannot run over several contexts is refused before any
context is contacted, with the fix in `hint`:

```json
{"name": "context_query", "arguments": {"query": "SELECT * FROM c ORDER BY path", "contexts": ["otp/tools"]}}
```

```json
{"error": "limit_required",
 "message": "A row query over contexts needs a LIMIT.",
 "hint": "Add LIMIT n (n <= 1000), and ORDER BY a selected field to merge rows in order.",
 "details": {"max_limit": 1000}}
```

## Reading a response

Read `summary` first; the fields carry the same facts for a program.

| Field | Meaning |
|---|---|
| `summary` | One paragraph: who answered, why the others did not, how rows were merged, what local copies cover. |
| `execution` | `succeeded` (every context answered), `partial`, or `failed` (none answered; MCP sets `isError`). |
| `merge` | `ordered` (rows by the ORDER BY key), `grouped` (one group per context), `interleave` (round-robin, not a ranking), `score` (vector scores, same embedding). |
| `rows[]._ctx`, `_ctx_name` | The context a row came from, id and name. Grouped answers carry `context` and `name` per group. |
| `relevance` | Retrieval only: `true` when rows are in one relevance order across contexts, `false` when they are not. |
| `merge_fallback` | Present when a `vector_top_k` score merge was not possible; says why and at which context. |
| `sources[].status` | `ok`, `timeout`, `unreachable`, `unauthorized`, `error`, `skipped_budget`, `skipped_offline`. |
| `sources[].error` | When not `ok`: `reason` (a code), `message` (what happened), `hint` (what to do). |
| `sources[].bound` | `limit_reached`: the context filled LIMIT or k, more rows may exist. `exhausted`: it returned everything that matched. |
| `sources[].membership` | What the answer covers: `live` (the database now), `retrieved_set` (only the saved slice), `complete_generation` (a whole imported snapshot). |
| `sources[].version` | `live` with `observed` (instance id and last seq after the query, a freshness hint), `generation`, `retrieved_set`, or `unknown`. |
| `sources[].retrieval` | `exact` (rows, BM25) or `approximate` (ANN: vector, hybrid). |
| `coverage` | Counts, and `missing`: the contexts whose rows are not in the answer. |

Rows from a context that failed are never returned, even the ones received
before the failure.

## When a context does not answer

With node R2 stopped, the same ordered query answers from the other two:

```json
{"execution": "partial",
 "summary": "2 of 3 contexts answered; rows from the others are not included. otp/eunit: the server did not accept the connection (econnrefused). 10 rows are merged across contexts in ORDER BY order. otp/tools filled the LIMIT or k; more matches may exist.",
 "sources": ["...", "...",
   {"context": "ctx_zuepupiv6vxtzjlqz2cszqsv", "name": "otp/eunit", "status": "unreachable", "rows": 0,
    "error": {"reason": "econnrefused",
              "message": "the server did not accept the connection (econnrefused)",
              "hint": "Check that the remote node is up; to answer without it, attach and materialize or import a local copy.",
              "rows_received_before_failure": 0}}],
 "...": "..."}
```

## Save results locally

A working set is the list of contexts you work with. `context_attach` without
`working_set` creates one; keep its id. Attaching copies nothing.

```json
{"name": "context_attach", "arguments": {"context": "otp/tools"}}
{"name": "context_attach", "arguments": {"working_set": "ws_gbwtzhbgsbbzl7mpwrx2dnqq", "context": "otp/sasl"}}
```

```json
{"id": "ws_gbwtzhbgsbbzl7mpwrx2dnqq",
 "summary": "Working set ws_gbwtzhbgsbbzl7mpwrx2dnqq has 2 members. otp/tools: local database otp_tools, live. otp/sasl: remote, queried over the network (not offline). Offline, 1 of 2 can answer. Local copies use 0 of 1073741824 bytes.",
 "members": [
   {"context": "ctx_uuojp36sme7ym4pjb2faocuo", "name": "otp/tools", "mode": "local",
    "local_db": "otp_tools", "membership": "live", "answers_offline": true, "version": {"kind": "live"}},
   {"context": "ctx_5loieq7q3hdaxluhwkklz5px", "name": "otp/sasl", "mode": "remote",
    "location": {"endpoint": "http://127.0.0.1:18082", "db": "otp_sasl"},
    "membership": "live", "answers_offline": false, "version": {"kind": "live"}}],
 "usage": {"bytes": 0}, "...": "..."}
```

`context_materialize` saves what a query returns: one frozen slice per
context, holding exactly the documents whose ids the query returned. Without
`working_set` it creates one.

```json
{"name": "context_materialize",
 "arguments": {"working_set": "ws_gbwtzhbgsbbzl7mpwrx2dnqq",
               "from_query": {"query": "SELECT b.id FROM bm25_top_k('test', k => 5) AS b",
                              "contexts": ["otp/eunit"]},
               "include": {"embeddings": false}}}
```

```json
{"working_set": "ws_gbwtzhbgsbbzl7mpwrx2dnqq",
 "summary": "Working set ws_gbwtzhbgsbbzl7mpwrx2dnqq: saved 5 documents from otp/eunit (878 bytes) into wslice_gbwtzhbg_zuepupiv.",
 "slices": [{"context": "ctx_zuepupiv6vxtzjlqz2cszqsv", "name": "otp/eunit", "status": "complete",
             "docs": 5, "bytes": 878, "local_db": "wslice_gbwtzhbg_zuepupiv",
             "derived": {"from": "ctx_zuepupiv6vxtzjlqz2cszqsv", "selection": "ids",
                         "observed": {"instance_id": "980522d2a4ed1af5", "last_seq": "AAABoNO_WLoAAAAH"},
                         "...": "..."}}],
 "usage": {"bytes": 878, "budget_bytes": 1073741824}}
```

A context already in the working set must be detached first. The size is
checked against the budget before anything is written.

## Work offline

Switch the node offline with `context_offline` (REST:
`PUT /contexts/_offline`), or pass `"offline": true` on one query. Query the
working set by id instead of listing contexts:

```json
{"name": "context_offline", "arguments": {"offline": true}}
{"name": "context_query",
 "arguments": {"query": "SELECT id, path, lines FROM c ORDER BY lines DESC LIMIT 50",
               "working_set": "ws_gbwtzhbgsbbzl7mpwrx2dnqq"}}
```

```json
{"execution": "partial",
 "summary": "2 of 3 contexts answered; rows from the others are not included. otp/sasl: skipped: offline and no local copy. 21 rows are merged across contexts in ORDER BY order. otp/eunit answered from a saved slice: answers cover only the 5 saved documents, not the source context.",
 "sources": [
   {"name": "otp/tools", "status": "ok", "membership": "live", "rows": 16, "...": "..."},
   {"name": "otp/sasl", "status": "skipped_offline", "rows": 0,
    "error": {"reason": "no_local_copy", "message": "skipped: offline and no local copy",
              "hint": "Materialize a slice or import a snapshot while online to answer offline."}},
   {"name": "otp/eunit", "status": "ok", "membership": "retrieved_set", "rows": 5,
    "version": {"kind": "retrieved_set", "observed": {"instance_id": "980522d2a4ed1af5", "last_seq": "AAABoNO_WLoAAAAH"}},
    "location": {"kind": "local", "db": "wslice_gbwtzhbg_zuepupiv"},
    "note": "answers cover only the 5 saved documents, not the source context"}],
 "...": "..."}
```

Remote contexts are never contacted offline, and nothing is downloaded in
their place. Saving from a remote context while offline is refused:

```json
{"error": "offline",
 "message": "The node is offline and this operation needs a remote source.",
 "hint": "Switch offline mode off (context_offline or PUT /contexts/_offline {\"offline\": false}) and retry.",
 "details": {"operation": "materialize", "remote_contexts": ["ctx_5loieq7q3hdaxluhwkklz5px"]}}
```

## Import a snapshot

A publisher exports a database as a checksummed directory (an operator step
in Erlang; the database is closed during the copy):

```erlang
barrel_ctx_export:export(<<"otp_sasl">>, "/srv/export_sasl_g1",
                         #{owner => barrel_server,
                           context => <<"ctx_5loieq7q3hdaxluhwkklz5px">>,
                           generation => 1}).
```

On the querying node, `context_import` (REST: `POST /worksets/:ws/_import`)
verifies the copy, opens it read only, and adds it to a working set (a new
one when `working_set` is omitted):

```json
{"name": "context_import", "arguments": {"dir": "$WORK/export_sasl_g1"}}
```

```json
{"id": "ws_rtjdds5plowqawmuvsanzekg",
 "summary": "Working set ws_rtjdds5plowqawmuvsanzekg has 1 member. otp/sasl: imported snapshot, generation 1. Offline, 1 of 1 can answer. Local copies use 1899476 of 1073741824 bytes.",
 "members": [{"context": "ctx_5loieq7q3hdaxluhwkklz5px", "name": "otp/sasl", "mode": "snapshot",
              "generation": 1, "local_db": "wsnap_5loieq7q_1", "membership": "complete_generation",
              "answers_offline": true, "version": {"kind": "generation", "generation": 1},
              "bytes": 1899476}],
 "...": "..."}
```

Queried offline, it answers as that generation:

```json
{"summary": "otp/sasl answered. 17 rows in ORDER BY order. otp/sasl answered from an imported snapshot (generation 1).", "...": "..."}
```

## Clean up

```json
{"name": "context_detach", "arguments": {"working_set": "ws_gbwtzhbgsbbzl7mpwrx2dnqq", "context": "otp/eunit"}}
{"name": "context_working_sets", "arguments": {}}
{"name": "context_working_set_delete", "arguments": {"working_set": "ws_rtjdds5plowqawmuvsanzekg"}}
```

```json
{"working_sets": [
   {"id": "ws_gbwtzhbgsbbzl7mpwrx2dnqq", "members": [{"name": "otp/tools", "mode": "local", "...": "..."},
                                                   {"name": "otp/sasl", "mode": "remote", "...": "..."}], "...": "..."},
   {"id": "ws_rtjdds5plowqawmuvsanzekg", "members": [{"name": "otp/sasl", "mode": "snapshot", "...": "..."}], "...": "..."}]}
{"ok": true, "deleted": "ws_rtjdds5plowqawmuvsanzekg"}
```

Detaching a slice deletes it; deleting a working set deletes its slices.
Imported snapshots stay on disk.

## The same operations over REST and Erlang

Field names are the same on the three surfaces. MCP tools that take
`working_set` create one when you omit it; REST names it in the path, and
Erlang takes `new`.

| MCP tool | REST | Erlang (`barrel_ctx`) |
|---|---|---|
| `context_capabilities` | `GET /contexts/_capabilities` | `capabilities/0` |
| `context_list` | `GET /contexts` (`?prefix=`, `?unlisted=true`) | `list/1` |
| `context_discover` | `GET /contexts?q=` | `discover/2` |
| `context_inspect` | `GET /contexts/:id` (a name URL-encoded: `otp%2Fsasl`) | `inspect/1` |
| (register) | `POST /contexts`, `DELETE /contexts/:id` | `register/1`, `update/2`, `unregister/1` |
| `context_query` | `POST /contexts/_query` | `query/1` |
| `context_attach` | `POST /worksets`, `POST /worksets/:ws/members` | `create_ws/1`, `attach/3` |
| `context_detach` | `DELETE /worksets/:ws/members/:ctx` | `detach/2` |
| `context_materialize` | `POST /worksets/:ws/_materialize` | `materialize/2` |
| `context_import` | `POST /worksets/:ws/_import` | `import/3` |
| `context_working_sets` | `GET /worksets`, `GET /worksets/:ws` | `list_ws/0`, `get_ws/1` |
| `context_working_set_delete` | `DELETE /worksets/:ws` | `delete_ws/1` |
| `context_offline` | `GET` and `PUT /contexts/_offline` | `offline/0`, `set_offline/1` |

Erlang returns `{error, {Code, Details}}` with the codes below;
`barrel_ctx_error:to_map/1` gives the JSON body.

## Errors

Every error has one shape on REST and MCP:
`{"error": code, "message": one sentence, "hint": what to do next, "details": {...}}`.

| Code | HTTP | When | What to do |
|---|---|---|---|
| `invalid_argument` | 400 | A field is missing, unknown, of the wrong type or value; `details.field`, `expected`, `allowed` or `accepted`. | Fix that field. |
| `invalid_query` | 400 | The statement does not parse; `details.bql_error`. | Strings use single quotes. |
| `limit_required` | 400 | A row query has no LIMIT. | Add `LIMIT n` (n <= `details.max_limit`). |
| `limit_too_large` | 400 | LIMIT or k above 1000. | Lower it; narrow the WHERE clause. |
| `too_many_contexts` | 400 | More than `details.max_contexts` contexts. | Split the query. |
| `duplicate_context` | 400 | A context listed twice (a name and its id count as one). | List it once. |
| `unsupported_federated_query` | 400 | OFFSET, SUBSCRIBE, UNNEST, a continuation, ORDER BY a field not selected, ordered merge without ORDER BY; `details.reason`, `accepted_shapes`. | Rewrite in an accepted shape. |
| `merge_not_allowed` | 400 | A merge that does not fit the statement (`score` on rows or BM25, `ordered` on retrieval). | Leave `merge` unset. |
| `merge_not_supported` | 400 | `rrf` or `rerank`. | Leave `merge` unset or use `interleave`. |
| `scores_not_comparable` | 400 | `merge: score` over contexts with different embeddings; `details.context`. | Leave `merge` unset (grouped). |
| `unknown_context` | 404 | No context has that id or name; `details.suggestions`. | Use a suggested name, or `context_list`. |
| `ambiguous_context` | 409 | Several cards share the name; `details.candidates`. | Pass one of the ids. |
| `unknown_working_set` | 404 | No such working set; `details.known`. | `context_working_sets`, or omit `working_set`. |
| `not_attached` | 404 | Detaching a context that is not a member. | Read the working set. |
| `already_attached` | 409 | Attaching or materializing a context already in the working set. | Detach it first. |
| `already_exists` | 409 | A card id, or an import name, is taken. | Omit the id, or pass another `name`. |
| `invalid_card` | 400 | A card without name or locations, with a secret, or a bad location; `details.reason`. | Fix the card. |
| `no_location` | 400 | `mode` asks for a location the card does not have. | Omit `mode`. |
| `over_budget` | 413 | A working-set budget (`bytes`, `contexts`, `transfer_bytes`) or `max_bytes` is exceeded; `details.needed`, `available` or `limit`. | Detach, select less, or raise the budget. |
| `offline` | 409 | Materializing from a remote context while offline. | Go online first. |
| `invalid_snapshot` | 400 | The import directory has no readable manifest or a checksum fails. | Pass the directory an export wrote. |
| `source_unavailable` | 502 | A source did not deliver the documents to save (reported per slice). | Retry when the source is reachable. |
| `forbidden` | 403 | A capability token on working sets, imports or offline mode. | Use a global token. |
| `internal` | 500 | Anything else; `details.detail`. | Check the server log. |

A context that does not answer is not a request error: the query answers,
and the source's `error` has a `reason`, `message` and `hint` (`econnrefused`,
`deadline`, `no_local_copy`, `db_not_found`, `embedder_not_configured`,
`node_remote_limit`, ...).

## Notes

- **No pagination.** A context returns at most the LIMIT or k (cap 1000).
  `bound: limit_reached` means narrow the predicate or raise the limit.
- **Discover is a filter.** It ranks nothing; a context it does not return
  may still hold answers.
- **Scores.** BM25, hybrid and RRF scores never merge across contexts.
  `vector_top_k` merges by score when every context reports the same
  embedding fingerprint and cosine distance, otherwise it falls back to
  grouped and says why in `merge_fallback`.
- **Permissions.** Registration, working sets, imports and offline mode need
  global auth. A capability token can list cards and query contexts; each
  local context is checked as a `POST /db/:db/query` on that database. A
  `read` right on a source also allows copying (materialize uses
  `_bulk_get`).
- **Slices are frozen** and cover only the saved documents. A slice of a
  plain (non record-mode) source holds documents, not vectors: row queries
  work on it, BM25 and vector search find nothing.
- **Exports** refuse a record-mode database whose persisted policy holds an
  API key or another secret.
