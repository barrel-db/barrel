# Track 1 results: remote composition of contexts (M1, B1-B7)

Branch `exp/ctx-remote`, based on `docs/contexts-action-plan` (9bbb51d2).
Contract: `docs/architecture/contexts-action-plan.md` sections 3, 5, 6, 7, 8.

## What you can do with this branch

Register contexts on a node, then query several of them, local or on other
Barrel servers, with one BQL statement.

```sh
# register a local and a remote context (global auth only)
curl -XPOST localhost:8080/contexts -H 'content-type: application/json' -d '
  {"name": "otp/et", "locations": [{"kind": "local", "db": "otp_et"}]}'
curl -XPOST localhost:8080/contexts -H 'content-type: application/json' -d '
  {"name": "otp/ftp", "locations": [{"kind": "remote",
   "endpoint": "http://r1:8080", "db": "otp_ftp"}]}'

# list and inspect
curl localhost:8080/contexts
curl localhost:8080/contexts/ctx_...

# ordered merge over several contexts
curl -XPOST localhost:8080/contexts/_query -H 'content-type: application/json' -d '
  {"query": "SELECT id, path, lines FROM c WHERE lines > 50 ORDER BY lines DESC LIMIT 12",
   "contexts": ["ctx_...", "ctx_..."], "deadline_ms": 5000}'

# retrieval, grouped per context
curl -XPOST localhost:8080/contexts/_query -H 'content-type: application/json' -d '
  {"query": "SELECT b.id, b._score FROM bm25_top_k(\"event trace\", k => 3) AS b",
   "contexts": ["ctx_...", "ctx_..."]}'

# the single-db route now takes a row cap and a deadline
curl -XPOST localhost:8080/db/otp_et/query -H 'content-type: application/json' -d '
  {"query": "SELECT * FROM db", "max_rows": 5, "deadline_ms": 2000}'
```

Tokens for remote endpoints live in node config, never in cards:

```erlang
{barrel, [{ctx_credentials, #{<<"http://r1:8080">> => <<"token">>}}]}
```

MCP: `context_list`, `context_inspect`, `context_query` (same arguments as the
REST body, same result maps). Erlang: `barrel_ctx:register/1`, `inspect/1`,
`list/1`, `query/1`.

## Per backlog item

| Item | Status | Commits |
|---|---|---|
| B1 observed version | done | 00ee2dd7 |
| B2 HTTP route: max_rows, deadline_ms, meta | done, one deviation | 4c39b63b |
| B3 barrel-lite in-band errors | not in scope for this track | |
| B4 catalog | done | 1e2b3fc2 |
| B5 remote client | done | 1b81681c |
| B6 executor | done, two deviations | eb25454a, a1e27e79, 44aae486 |
| B7 REST and MCP | done | 9cc3a313 |
| S1 bench | script done, only a smoke run | 46e4f0ac |
| MCP resource template fix (found while testing) | done | 4d909a63 |

### B1
`barrel:'query'/3`, `query_fold/5` and `barrel_docdb:'query'/3` meta carry
`instance_id` and `last_seq`, read after the run (new
`barrel_docdb:db_observed_version/1`). `last_seq` stays the raw 12-byte HLC in
Erlang meta, as before for collection queries. Tests: `barrel_bql_facade_SUITE`
`observed_version` (collection and the three table functions, run and fold)
and `observed_version_after_write` (asserts `>=` semantics).

### B2
`POST /db/:db/query` (JSON body) and `GET ?q=` accept `max_rows` (clamped to
1000; zero or non-integers give 400 `invalid_max_rows`) and `deadline_ms`
(ceiling 300000; bad values give 400 `invalid_deadline_ms`). The meta line
adds `instance_id`, `last_seq` (urlsafe base64, same as `barrel:hlc_encode`)
and `bound` (`limit_reached` when the cap cut the stream, more rows exist, or
the statement's LIMIT/k was filled). A cap stop reports `has_more: true` and
drops the continuation. A passed deadline ends the stream with
`{"error":"deadline"}` and no meta.
Tests: `barrel_server_SUITE` `t_query_max_rows`, `t_query_meta_bound`,
`t_query_deadline`, `t_query_bad_bounds`.

### B4
`barrel_ctx_catalog`: `register/1`, `get/1`, `list/1` (`include_unlisted`,
`name_prefix`), `update/2`, `unregister/1`, `resolve_name/1` (`ambiguous` when
two cards share a name). Cards are docs in `_barrel_catalog` (app env
`ctx_catalog_db`). Minted ids are `ctx_` + 24 base32 chars (15 random bytes);
an explicit id must match `ctx_[a-z0-9_]{1,64}` (the walkthrough uses
`ctx_local_docs`). Validation rejects: any key naming a secret at any depth
(token, password, secret, api_key, authorization, credential(s), cookie, ...),
any `bsp_` value, an endpoint with userinfo, non http(s) endpoints, unknown or
incomplete location kinds, bad db names, cards over 64 KiB.
Tests: `barrel_ctx_catalog_SUITE` (8 cases).

### B5
`barrel_ctx_remote:query(#{endpoint, db}, Bql, Opts)`: hackney async request,
NDJSON parsed incrementally, `timeout` enforced by the caller's receive (also
sent as the server `deadline_ms`), `max_bytes` cap (16 MiB default), rows kept
only when a final meta line arrives. Failures are
`{error, #{status := timeout | unreachable | unauthorized | error, reason,
rows_received, bytes}}`: error line, missing meta, data after meta, bad line,
too large, HTTP 401/403 (unauthorized), other statuses. Credentials: explicit
`credential`, else `credential_ref` or the endpoint entry of `ctx_credentials`.
`hackney` added to barrel's `applications` and `rebar.config` deps.
Tests: `barrel_ctx_remote_SUITE` (11 cases, scripted HTTP server: chunk split
mid-line, closed port, stall timing, stall after rows, error mid-stream,
missing meta, data after meta, byte cap, 401/400, node slots, no connection
leak) and `barrel_server_contexts_SUITE` `t_remote_client_matches_local`
(live server, same rows as a local `barrel:'query'/3`).

### B6
`barrel_ctx:query/1` (`barrel_ctx_query`, `barrel_ctx_shape`):

- Shapes (5.1) from one local compile: ordered rows (LIMIT <= 1000, ORDER BY
  key selected or `SELECT *`), unordered rows (LIMIT required, grouped only),
  retrieval (k and LIMIT <= 1000, grouped only). Rejections, all before any
  member is contacted: `subscribe`, `offset`, `unnest`, `continuation`,
  `limit_required`, `limit_too_large`, `order_key_not_projected`,
  `order_by_required` (merge ordered without ORDER BY),
  `{merge_not_allowed, ordered}` (retrieval), `{merge_not_supported, M}`
  (score, rerank, interleave), `too_many_contexts`. Also `invalid_query`,
  `unknown_context`, `bad_request`.
- Budgets (6.1): contexts 8, parallel 4 (cap 16), member timeout 4000 ms,
  deadline 5000 ms (cap 60000), rows per member = statement bound (also sent
  as `max_rows`), bytes 16 MiB, node-wide remote requests 32
  (`skipped_budget`). All overridable by app env (`ctx_max_contexts`,
  `ctx_max_parallel`, `ctx_member_timeout_ms`, `ctx_deadline_ms`,
  `ctx_node_remote_max`, `ctx_max_response_bytes`) or per request.
- Fanout: a coordinator keeps `max_parallel` workers; each gets
  `min(member timeout, deadline left)`. Local workers take a counted
  `barrel_dbs` lease and run `barrel:'query'/3`; remote workers call B5. At
  the global deadline, running members and members not yet started are
  `timeout` (`deadline` or `deadline_before_start`).
- Merges: `grouped` (request order, answered members only) and `ordered`
  (term order on the ORDER BY value, then context id, then doc id; missing
  values sort as `null` like members do). `_ctx` on every row.
- Response: `execution`, `merge`, `groups` or `rows`, `sources` (context,
  location, status, rows, bound, retrieval, version with observation,
  elapsed_ms, bytes for remote, error block for failures), `coverage`
  (requested, answered, failed, skipped, missing, scope_origin), `elapsed_ms`.

Tests: `barrel_ctx_query_SUITE` (13 cases): union oracle over 4 fixed
statements plus 25 fixed-seed generated statements, tie determinism, grouped,
retrieval with observed versions, every rejected shape (and no member
contacted), remote member ok, closed/stall/error-mid-stream members, global
deadline, unauthorized member, node budget, snapshot-only card, concurrent
queries leave no lease and no slot. `barrel_dbs_SUITE` gains
`t_lease_counts`, `t_lease_released_on_exit`.

### B7
Routes (new `contexts` group, in `groups => all`): `POST /contexts`,
`GET /contexts` (`?prefix=`, `?unlisted=true`), `GET /contexts/:id`,
`POST /contexts/_query`. Auth classifier: capability tokens may read cards and
query; `POST /contexts` is refused to them (403), global auth only. Each local
member is checked with the existing classifier as `POST /db/:db/query`
(`barrel_server_auth:member_authorizer/1`); MCP uses
`barrel_server_mcp_auth:allow(Ctx, Db, read)`. Refused members are
`status: unauthorized`. Errors: 400 `unsupported_federated_query` with
`reason` (and `detail`), 400 `invalid_query`, 400 `bad_request` with `field`,
404 `unknown_context`, 400 `invalid_card`, 409 `already_exists`.
Tests: `barrel_server_contexts_SUITE` (10 cases, 1 local + 2 remote members,
remotes reached through a TCP delay proxy): cards over REST, B5 against a live
server, union oracle over REST vs `POST /db/union/query`, retrieval with
observed versions, closed port / stall / truncated stream / in-band error
after the 200, rejected shapes over REST, MCP results equal REST results,
no lease and no new database after queries, capability token over REST and
MCP (own space ok, other local db unauthorized, remote ok; without the
configured credential the remote is unauthorized), registration needs global
auth.

## Test results

| Command | Result |
|---|---|
| `rebar3 ct --dir=apps/barrel/test` | 138 passed |
| `rebar3 ct --suite=apps/barrel_docdb/test/barrel_bql_SUITE` | 12 passed |
| `rebar3 as server ct --suite=apps/barrel_server/test/barrel_server_contexts_SUITE` | 10 passed |
| `rebar3 as server ct --suite=apps/barrel_server/test/barrel_server_SUITE` | 28 passed |
| `rebar3 as server ct --dir=apps/barrel_server/test` | 157 passed (main snapshot baseline: 143) |
| `rebar3 xref`, `rebar3 as server xref` | only the pre-existing `barrel_faiss` warnings |
| `rebar3 dialyzer`, `rebar3 as server dialyzer` | 11 warnings, all pre-existing (`barrel_att_store_none`, `barrel_vectordb_index_faiss`); none in touched files |

## Deviations from the contract

1. **Leases instead of an executor refcount over boolean pins.** 3.6 says the
   executor holds a refcount because `barrel_dbs` pins are booleans. An
   executor-side count still has to call `pin/unpin`, and its `unpin` would
   clear a pin another owner set. `barrel_dbs` gains `lease/2`, `release/1`,
   `leases/0`: counted, monitored (a crashed holder releases), separate from
   `pin/1`. Idle close and eviction skip leased entries.
2. **Server deadline granularity (B2).** The deadline is checked per row and
   once before the meta line. Materializing plans (ORDER BY, LIMIT, table
   functions) build their whole result before the first row, so the server
   cannot stop a long sort; it reports `deadline` after it. Stopping inside
   the engine needs a deadline in `barrel_bql_exec` and `barrel_query`.
3. **Error block names.** 3.7 uses `rows_received_before_timeout`; the
   implementation reports `rows_received_before_failure` for every failed
   status (timeout, error mid-stream), with `reason`, optional `detail`,
   `after_ms`, `http_status`.
4. **Unknown context id rejects the request** (404 `unknown_context`) instead
   of a member status: the caller named it, and nothing has been contacted.
5. **Extra response fields**: top-level `elapsed_ms`; `bytes` on remote
   sources; `version.kind = unknown` when a remote meta has no observation
   (servers without B2).
6. **Default merge**: `ordered` for the ordered-rows shape, `grouped`
   otherwise; `grouped` is allowed for an ordered shape.
7. **Locations**: a card may hold several; M1 queries the first local one,
   else the first remote one. Snapshot-only cards give member status `error`,
   reason `no_queryable_location`. Remote locations may carry a
   `credential_ref` (a name, not a secret).

## Bugs and surprises in existing code

- **`barrel:open/2` creates missing databases**, so `POST /db/:db/query` on a
  name that does not exist creates it, and a local card with a typo creates an
  empty database the first time it is queried. Not changed here.
- **`barrel_bql_exec:fold_chunks/7`**: when the fold fun stops mid-chunk, the
  returned meta is the chunk's meta, whose continuation points past rows the
  caller never saw. No current caller resumes from it (the HTTP route now
  drops it on a cap stop), but a future one would skip rows.
- **hackney direct connections are owned by `hackney_conn_sup`**, not the
  caller (`hackney_conn:start_link/1` uses `self()` of the supervisor). Killing
  the caller mid-request leaves the connection open. The client closes on every
  path it controls; the executor gives remote workers a 100 ms grace before
  killing them.
- **MCP resource templates depend on registry order.** barrel_mcp matches
  resource templates first-come, in `maps:to_list` order of the whole handler
  registry, and the trailing `{db}` of `barrel://db/{db}` also matches
  `barrel://db/x/doc/y` and `barrel://db/x/live/s`. Adding three tools
  reshuffled that order, and 3 `barrel_server_mcp_live_SUITE` cases failed in
  the full server run (they pass alone; main passes because its order happens
  to be favourable). Worked around in `barrel_server_mcp_resources`
  (`db_resource` routes doc and live URIs itself, 4d909a63). The real fix
  belongs in barrel_mcp: prefer the most specific template, or stop a
  trailing variable at `/`.
- First full `apps/barrel/test` run: the executor suite left pinned databases
  in `barrel_dbs`, which broke `barrel_dbs_SUITE:t_idle_close` (suites share
  the VM). Fixed in the suite.

## Smoke numbers (not authoritative)

One run, `ITER=3`, other tracks building on the same machine, client and
server in one VM, loopback. The proxy delays data, not the TCP handshake, so a
real network adds one more RTT per member for connection setup (no pooling
yet).

| members | RTT ms | query | p50 ms | p95 ms | rows | bytes |
|---|---|---|---|---|---|---|
| 1 | 0 | ordered | 2.6 | 3.9 | 20 | 1734 |
| 1 | 100 | ordered | 102.9 | 103.2 | 20 | 1734 |
| 3 | 20 | ordered | 25.2 | 25.7 | 20 | 5316 |
| 8 | 0 | ordered | 7.1 | 7.9 | 20 | 15810 |
| 8 | 100 | ordered | 106.9 | 108.0 | 20 | 15810 |
| 8 | 100 | grouped | 109.9 | 110.1 | 160 | 13981 |

Shape of the result: latency is one RTT plus a few ms per member at this
size; bytes grow linearly with members for ordered merges (every member sends
its full LIMIT).

## Bench command (spike S1)

```sh
bench/ctx_remote/run.sh /path/to/corpus.jsonl
# knobs: MEMBERS=1,3,8 RTTS=0,20,100 ITER=30 DATA_DIR=/tmp/x OUT=results.json
```

It builds the server profile, starts a `barrel_server` in the VM with a
temporary data dir, loads the largest corpus applications (one database each;
id, app, path, line count, moduledoc), registers each as a remote context
behind a delay proxy (RTT/2 each way), and prints p50/p95 latency, rows,
bytes, success ratio and VM CPU per query for an ordered and a grouped
statement in every cell. Retrieval is not benchmarked (it needs an embedder).

## What remains before each item is PR-ready

- **B1**: CHANGELOG entries (barrel_docdb, barrel); docs of the new meta keys.
- **B2**: engine-level deadline (deviation 2); CHANGELOG and the HTTP API
  reference; barrel-lite (B3) should read `bound` and treat a missing meta as
  failure.
- **B4**: decide whether card writes should replicate (the catalog is a plain
  docdb db); decide listing visibility for capability principals (all listed
  cards are visible today).
- **B5**: connection reuse (pooling with safe reuse after aborted streams);
  TLS options; signed requests (`barrel_sync_sig`) as an alternative to bearer
  tokens.
- **B6**: slot and worker cleanup if the coordinator itself crashes (slots are
  released by the coordinator only); refuse to create a missing local
  database (see Bugs); the `ordered` merge sorts the union (fine at 8 x 1000
  rows) rather than a k-way merge.
- **B7**: HTTP API docs and a guide (B8); decide whether `context_register`
  belongs in MCP; the `contexts` group is not in the embedder default groups.
- **S1**: run the full campaign on a quiet machine and across two hosts.
