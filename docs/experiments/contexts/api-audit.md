# Contexts API: newcomer audit

Three `barrel_server` nodes on loopback (the `scripts/contexts-demo.sh`
setup: `otp/tools` local on L, `otp/sasl` on R1, `otp/eunit` on R2). Two
personas, neither reads the implementation:

- an agent that only sees MCP `tools/list` and tool results;
- a developer that only has the REST routes and their responses (and, for
  Erlang, the `barrel_ctx` exports and their edoc).

Tasks: find contexts about a topic, query two contexts, understand why a
source failed, save results locally, work offline, import a snapshot, clean
up. Branch `exp/contexts-integration` at `cf13fdae`.

## Before

### What the agent sees in `tools/list`

Seven tools: `context_list`, `context_discover`, `context_inspect`,
`context_query`, `context_attach`, `context_detach`, `context_materialize`.
Descriptions are one sentence (query has four). No input property has a
description, a default, an enum or an example: `merge` is a bare string,
`contexts` a bare array, `from_query` and `include` bare objects, `mode` a
bare string.

### Friction points

Numbered for the "after" section. Severity: B blocks a first try, C costs
a retry or a wrong conclusion, M is minor.

**Finding contexts**

1. (C) `context_discover {"q": "unit tests"}` returns `[]`: the filter is
   one substring, so two words never match. Nothing says so; the agent
   concludes there is no testing context (`q: "testing"` finds `otp/eunit`).
2. (C) Discover and list return whole cards (locations, format, type,
   `updated_at`); fine, but the id to use next is buried among them.

**Ids versus names**

3. (B) Every tool wants `ctx_...` ids. The agent knows `otp/eunit` from
   `context_list`, tries `context_inspect {"id": "otp/eunit"}` and gets
   `{"error": "not_found"}` with no hint that a name is not accepted.
4. (B) `context_query` with names in `contexts` returns
   `{"error": "unknown_context", "context": "otp/tools"}`: no candidate,
   no hint to use the id.
5. (C) Rows carry `_ctx: "ctx_..."`, groups and sources carry only the id:
   to know which dataset a row came from the agent must map ids back to
   names from an earlier `context_list`.
6. (C) Working-set ids (`ws_...`) must be copied from the `context_attach`
   answer. There is no MCP tool to list or read working sets, so an agent
   that loses the id cannot find it again.

**Query shapes and limits**

7. (B) `LIMIT` is required, discovered by failure:
   `{"error": "unsupported_federated_query", "reason": "limit_required"}`.
   The tool description says so, but the error does not say what to add.
8. (C) Validation runs before name resolution, so the first error on a
   request with names is `limit_required`, and the second is
   `unknown_context`: two round trips for two independent mistakes.
9. (C) `order_key_not_projected`, `limit_too_large`, `offset`,
   `too_many_contexts` come without the limit (1000, 8) or the fix (select
   the ORDER BY field). The maximum number of contexts (8) appears nowhere
   before the failure.
10. (C) Double-quoted strings are identifiers in BQL. `bm25_top_k("release",
    k => 2)` fails with `syntax error before: ','`. `INTEGRATION.md` shows
    exactly this wrong form for materialize.
11. (C) `merge` accepts `ordered`, `grouped`, `score`, `interleave`; an
    unknown value gives `{"error": "bad_request", "field": "merge"}` with no
    list. `merge: "score"` on BM25 gives `merge_not_allowed` with
    `detail: "score"`, not why.
12. (M) No way to learn the shapes, merges, limits and budgets without
    failing.
13. (C) An empty `contexts` array and a missing one both give
    `bad_request/contexts`; `contexts` together with `working_set` gives
    `bad_request/scope`: the field name `scope` is not in the input schema.
14. (M) Duplicate contexts give `bad_request` with `field:
    "duplicate_context"`: not a field.

**Reading an answer**

15. (B) A query where no source answered is `isError: false` with
    `execution: "failed"`. The agent sees a successful tool call with no
    rows; nothing in the text says "nothing answered". Same with only
    offline-skipped remote members.
16. (C) `execution`, `status`, `bound`, `membership`, `version`,
    `retrieval`, `relevance`, `coverage.scope_origin` are all needed to read
    an answer and none is explained in the result. A failed vector query
    across two contexts still reports `relevance: true`.
17. (C) Failure reasons are raw atoms: `econnrefused`, `deadline`,
    `no_local_copy`, `embedder_not_configured`, `remote_error` with
    `detail`. They do not say what to do (start the node, raise
    `per_context_timeout_ms`, materialize or import before going offline,
    configure an embedder).
18. (C) Local and remote failures of the same cause look different:
    local `{"reason": "embedder_not_configured"}`, remote
    `{"reason": "remote_error", "detail": "embedder_not_configured"}`.
19. (M) Local sources have no `bytes`; remote ones do.
20. (C) Working-set members say `coverage` (`live`, `retrieved_set`, ...)
    while query sources say `membership` for the same values; members also
    carry `available`, whose meaning is not stated (a remote member shows
    `available: false` while online).

**Saving results and working offline**

21. (B) `context_materialize` requires a `working_set`, and the only way to
    get one from MCP is to guess that `context_attach` without
    `working_set` creates one. An agent that wants "save these results"
    cannot do it in one call.
22. (C) `already_attached` on materialize comes back inside a slice as
    `{"reason": "already_attached", "detail": "<<\"ctx_...\">>"}` (an
    Erlang term printed as text), with no hint (detach first).
23. (C) A slice with no rows is `{"status": "empty"}` with no reason.
24. (C) `max_bytes: 10` fails as `source_unavailable` /
    `response_too_large`: reads as a network problem, not a budget.
25. (M) The description says the query must select the id; `SELECT b.path`
    also works (the id is always returned). The rule is stricter in text
    than in code.
26. (C) Offline has a write (`PUT /contexts/_offline`) but no read: `GET`
    returns 405. MCP has no way to see or set it except per request.

**Import and cleanup**

27. (B) Import exists only over REST (`POST /worksets/:ws/_import`); MCP
    has no tool. A bad directory returns HTTP 500 with
    `{"error": "{manifest_unreadable,enoent}"}` (an Erlang term).
28. (C) No MCP tool to delete a working set; REST has
    `DELETE /worksets/:ws`. No REST route to unregister a context
    (`DELETE /contexts/:id` is 405).
29. (M) `GET /worksets` returns ids only; the agent must read each one.
30. (M) `POST /worksets {"budget": {"bytes": "x"}}` is accepted and the
    bad budget ignored.

**Errors in general**

31. (B) Error shapes differ: `{"error": "not_found"}`,
    `{"error", "context"}`, `{"error", "reason", "detail"}`,
    `{"error", "field"}`, `{"error", "message"}`, a plain text
    `method not allowed`, and MCP schema errors printed as Erlang terms
    (`[{[],{missing_required,<<"id">>}}]`). No error carries a next step.

**Erlang developer**

32. (C) `barrel_ctx` has no edoc on `register/1`, `update/2`,
    `resolve_name/1`, `create_ws/1`, `get_ws/1`, `delete_ws/1`, `detach/2`;
    every spec returns `{error, term()}`, so the caller cannot match on a
    known set of reasons. Error terms mix `not_found`,
    `{unknown_context, Id}`, `{bad_request, from_query}` and
    `{unsupported_federated_query, Reason, Detail}`.
33. (M) No exported types on the facade; request and response maps are
    documented in `barrel_ctx_query` only.

**Naming across surfaces**

34. (M) Erlang `inspect/1` takes an id, MCP `context_inspect` takes `id`,
    other tools take `context`. REST `GET /contexts?q=` and MCP
    `context_discover {"q"}` agree; MCP `context_list {"prefix"}` has no
    REST filter of that name.

### Summary

The feature works, but an agent needs the guide to use it: ids that must
be copied, a working set it cannot guess, errors without next steps, and
answers whose `isError: false` hides a failed execution. The most costly
items for a first try are 3, 4, 7, 15, 21, 27 and 31.

## After

Same walk on `exp/contexts-api`, same three nodes, reading only
`tools/list`, tool results and REST responses. The demo
(`scripts/contexts-demo.sh`, 36 checks) now runs this walk by name.

### What `tools/list` shows now

Twelve tools. Each description says what the tool does, when to use it,
what it returns and gives one example; every input property has a type and
a description, defaults (`deadline_ms` 5000, `per_context_timeout_ms` 4000,
`max_parallel` 4, `include.embeddings` true) and enums (`merge`, `mode`)
are in the schema. New tools: `context_capabilities`, `context_import`,
`context_working_sets`, `context_working_set_delete`, `context_offline`.

### Friction points, resolved

| # | Before | After |
|---|---|---|
| 1 | `discover "unit tests"` found nothing | Every word must match, a plural matches its stem: `"unit tests"` finds `otp/eunit`. An empty result has a summary suggesting fewer words or `context_list`. |
| 2 | Discover buries the id | Discover answers a `summary` naming the matches; names work everywhere, so the id is rarely needed. |
| 3, 4 | Names refused, `not_found` without hint | Names accepted by every tool, REST path (`/contexts/otp%2Fsasl`) and Erlang function. A typo answers `unknown_context` with `suggestions` and "Did you mean: otp/sasl?"; a shared name answers `ambiguous_context` with the candidates. |
| 5 | Only ids in rows, groups, sources | `name` on sources, groups, working-set members and slices; `_ctx_name` on merged rows. |
| 6 | Lost working-set id is lost | `context_working_sets` (and `GET /worksets`) lists them with member names; an unknown id answers `details.known`. |
| 7 | `limit_required` without the fix | Own code, `details.max_limit`, hint "Add LIMIT n (n <= 1000), and ORDER BY a selected field...". Also stated in `context_capabilities` and the tool description. |
| 8 | Two round trips for two mistakes | Names are resolved first, then the statement. Still one error per call (see below). |
| 9 | Limits found by failure | `too_many_contexts` carries `requested` and `max_contexts`; `limit_too_large` carries `max_limit`; `order_key_not_projected` hints "Add the ORDER BY field to the SELECT list". All limits in `context_capabilities`. |
| 10 | Double quotes gave a bare syntax error | `invalid_query` hint: "Strings use single quotes ('text'); double quotes name fields." Also in capabilities (`bql`) and the `query` description. |
| 11 | Unknown merge without the list | `invalid_argument` with `details.allowed`; `rrf`/`rerank` answer `merge_not_supported` with why and what to use. |
| 12 | No way to learn shapes | `context_capabilities`, `GET /contexts/_capabilities`, `barrel_ctx:capabilities/0`. |
| 13, 14 | `field: scope`, `field: duplicate_context` | `invalid_argument` on `working_set` ("either contexts or working_set, not both"); `duplicate_context` is its own code, and a name plus its id count as a duplicate. |
| 15 | Failed execution looked like success | MCP returns `isError: true` when no context answered (the body is the full answer); `summary` starts "No context answered". REST keeps 200 and the same body. |
| 16 | Fields unexplained | `summary` on every query answer; the guide has a "Reading a response" table. |
| 17 | Raw reasons | Every source that did not answer has `error.message` and `error.hint` (start the node, raise the timeout, materialize or import, configure an embedder, fix the card). |
| 18 | Local and remote reasons differ | A remote reason sent as a code reads like the local one (`reason: embedder_not_configured`, `origin: remote`), same message and hint. |
| 20 | `coverage` vs `membership`, `available` | Members say `membership`, like sources, and `answers_offline` (true only when a local copy is present). |
| 21 | No way to save without guessing | `context_materialize` and `context_import` create a working set when `working_set` is omitted; `context_attach` already did. |
| 22 | Erlang term in slice errors | Slice errors use the catalog: `reason`, `message`, `hint`, `details`. |
| 23 | `empty` without reason | `reason: no_ids`, "the query returned no document ids from this context". |
| 24 | `max_bytes` looked like a network error | `over_budget` with `budget: max_bytes` and `limit`. |
| 26 | Offline was write-only | `GET /contexts/_offline`, `context_offline` reads or sets it; `offline` in capabilities. Materializing a remote context offline answers `offline`. |
| 27 | Import only over REST, 500 on a bad dir | `context_import`; a bad directory answers `invalid_snapshot` (400) with the reason. |
| 28 | No delete over MCP, no card removal | `context_working_set_delete`; `DELETE /contexts/:id` and `barrel_ctx:unregister/1`. |
| 29 | `GET /worksets` gave ids only | Id, owner, usage and members (name, mode) per working set. |
| 30 | Bad budget ignored | `invalid_argument` on `budget.<key>`. |
| 31 | Seven error shapes | One shape on REST and MCP (`error`, `message`, `hint`, `details`), MCP argument checks included; a catalog in the guide. Erlang returns `{error, {Code, Details}}` with the same codes. |
| 32, 33 | Undocumented facade | Every `barrel_ctx` export has a spec and edoc (what, returns, errors); `context_ref()`, `ws_ref()`, `error()`, `ws_view()`, `query_response()` exported. |
| 34 | `id` vs `context` | Every tool that takes a context calls the argument `context`. |

### What is still not obvious

- One error per call: a request with an unknown name and a missing LIMIT
  reports the name first, then the LIMIT.
- Export has no REST or MCP tool: an agent cannot produce the directory
  `context_import` needs; an operator runs `barrel_ctx_export:export/3`.
- Registering a card is REST or Erlang only (global auth); there is no
  `context_register` tool.
- The per-source "filled the LIMIT or k" sentence is noisy on ordered
  merges, where most sources fill the LIMIT by design.
- `relevance: true` is still set on a vector query where no context
  answered (no rows, so no harm, but it reads oddly).
- Hints name both the MCP tool and the REST route in one sentence; each
  reader skips half of it.
- A capability token refused on `/worksets` gets the auth middleware's
  403 body, not the contexts error shape.
- The budget still shows `open_dbs`, which is not enforced.
- Working sets have an `owner` field but any global principal sees and
  deletes all of them.
