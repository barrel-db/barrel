# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.8.0] - 2026-06-14

### Removed (breaking)
- The HTTP server and its REST/SSE/admin API are gone. barrel_docdb is now
  embedded-only: use it as an Erlang library via the `barrel_docdb` module.
  This drops the `livery` dependency, the API-key store, the signed-peer
  registry, and the HTTP replication transport. Replicate in-process with
  `barrel_rep_transport_local` or your own `barrel_rep_transport` behaviour
  (URL replication targets now raise `{http_transport_removed, _}`).

### Fixed
- `find/2,3`: an AND of two or more `{compare, ...}` conditions no longer
  returns the empty set.
- `find/2,3`: `{prefix, Path, Prefix}` works as a sole condition.
- Documents re-created over a tombstone are re-indexed, so `find` returns them.

### Added
- `find/2,3` id scans: `#{id_prefix => P}` and `#{id_range => {Start, End}}`
  ordered primary-key range scans over the document id.
- `find/2,3` `flat => true` returns flat documents (`Doc#{<<"id">>}`) instead of
  the `#{<<"id">>, <<"doc">>}` wrapper.

### Changed
- OTP 29: dropped the deprecated prefix-`catch` operator reliance; added
  `barrel_lib` with reusable `safe_*` helpers.
- Bumped `rocksdb` to 3.0.0 and `instrument` to 1.1.4.

### Notes
- Top-level fields whose key begins with `_` are reserved metadata: they are
  not stored or indexed, so they cannot be queried.

## [0.7.7] - 2026-06-11

### Changed (breaking)
- `/_replicate` now refuses to call out to anything not in the new
  peer registry. The request body must carry either `peer_id` (a
  registered peer's id, URL pulled from the registry) or a `target`
  URL that matches a registered peer's canonical URL. Anything else
  is rejected with **403 `unregistered_peer`**. Closes the SSRF
  surface (security-review #6): an authenticated key holder can no
  longer aim outbound replication at `127.0.0.1`,
  `169.254.169.254`, RFC1918, or any unregistered host.
- The inbound replication-receiving endpoints (`_revsdiff`,
  `_put_rev`, `_sync_hlc`) now require a valid Ed25519 signature
  from a registered peer (`X-Peer-Id`, `X-Peer-Timestamp`,
  `X-Peer-Signature` headers) in addition to the existing API key.
  Missing or invalid signatures return **401**
  `peer_signature_required` / `unregistered_peer` /
  `invalid_peer_signature`.
- Outbound replication calls now always include `peer_auth => true`,
  i.e. every request between registered peers is signed.

### Added
- New module `barrel_peer_registry` (DETS-backed, file
  `<data_dir>/peers.dets`). Schema: `peer_id`, `name`, canonical
  `url`, `public_key`, `databases`, `created_at`, `last_used`.
- Admin HTTP routes:
  - `GET /peers` — list registered peers.
  - `POST /peers` — register a peer. Body: `{"name": "...",
    "url": "...", "public_key": "<base64>", "peer_id": "..."}` for an
    explicit registration, or `{"name": "...", "url": "...",
    "discover": true}` to TOFU-fetch the remote's
    `/.well-known/barrel` pubkey.
  - `GET /peers/:peer_id` — fetch one entry.
  - `DELETE /peers/:peer_id` — revoke.
  All four routes require an admin API key.
- Config knob `replication_require_registered_peer` (default
  `true`), `BARREL_REPLICATION_REQUIRE_REGISTERED_PEER`. Setting it
  to `false` re-enables the legacy free-form target URL behaviour as
  a migration escape hatch — scheduled for removal in 0.8.0.

### Migration
- Existing replication clients that call `/_replicate` with a raw
  `target` URL must register the target first
  (`POST /peers {..., "discover": true}` is the easy path on
  trusted networks), then either continue calling `/_replicate`
  with the same URL or switch to `peer_id`.
- Inbound endpoints will start returning 401 to any caller that
  isn't a registered peer. Make sure both ends of every replication
  link register each other before deploying this version.

## [0.7.6] - 2026-06-05

### Changed (breaking)
- Query timeout now hard-fails instead of silently truncating.
  When the document-fetch pipeline (or the `barrel_parallel` worker
  pool) does not finish within `query_timeout_ms` (default 30000ms,
  override via `BARREL_QUERY_TIMEOUT_MS`), the call raises
  `error({query_timeout, Info})`. The HTTP `/_find` endpoint maps
  this to **504 Gateway Timeout** with a JSON/CBOR body of
  `{"error":"query_timeout","completed":N,"total":T,
  "missing_batches":M,"timeout_ms":Ms}`. Callers that previously
  received a partial `{ok, Results, Meta}` with no signal must now
  handle the 504.

### Added
- `barrel_query_timeouts` counter (Prometheus / OTel) records every
  pipeline or pool deadline expiry.
- Configurable global deadline `query_timeout_ms` shared by
  `barrel_query` and `barrel_parallel`. Replaces the previous
  hardcoded 30s and 60s splits.

### Fixed
- `barrel_parallel:pmap/2,3` spawn-fallback path used a bare
  `receive` with no deadline, so a stuck worker would hang the
  caller forever. Now bounded by the same `query_timeout_ms`.

## [0.7.5] - 2026-06-04

### Added
- HTTP request body size cap and in-flight request cap via livery's
  middleware stack. Buffered bodies above `http_max_body_bytes`
  (default 4 MiB) return 413; concurrent requests above
  `http_max_in_flight` (default 1000) shed with 503. Both tunable via
  `BARREL_HTTP_MAX_BODY_BYTES` / `BARREL_HTTP_MAX_IN_FLIGHT`.
  Streamed attachment uploads bypass the body cap by design.

### Changed
- `barrel_http_api_keys` now reads the canonical `data_dir` config
  key, with a deprecation-warning fallback to legacy `data_path`.
  The fallback is scheduled for removal in 0.8.0.

### Removed
- Dead cowboy-era config keys `http_acceptors`,
  `http_max_connections`, and `http_protocols`. Livery's defaults
  (60s request timeout, 300s idle timeout) replace them; per-request
  caps are enforced by middleware.

## [0.7.4] - 2026-06-04

### Changed
- The OpenAPI 3.1 document is now generated at boot from the route
  table in `barrel_http_server` and served live at
  `GET /openapi.json`. A Redoc UI is at `GET /docs`. Both are public
  (no API key required) so SDK generators and importers can pull
  the spec without credentials.
- Per-route OpenAPI metadata (`operation_id`, `summary`, `tags`,
  optional `parameters` / `request_body` / `responses`) lives next
  to each route in `barrel_http_server:routes/0`. To update the doc,
  edit the route metadata; the next service start picks it up.

### Removed
- `docs/api/openapi.yaml` (hand-maintained spec) and the stale
  `openapi.yaml` at the repo root (0.5.0, referenced the JWT
  scheme we removed in 0.6.2). Replaced by the live endpoint.
- `docs/api/openapi.md` rewritten to point at `/openapi.json` and
  `/docs` instead of the deleted YAML file.

## [0.7.3] - 2026-06-04

### Changed
- Authentication and authorisation moved into a livery middleware. New module `barrel_docdb_auth` is a service-level middleware (one entry in `barrel_http_server`'s middleware stack) that classifies every request by `(path, method)` into `public` / `admin` / `{data, DbName, ReadOrWrite}`, runs the API-key check, and 401/403s before the handler ever runs. Handlers no longer call `maybe_authenticate` / `authorize` / `required_perm` — those lived inside `barrel_http_handler` and have been removed.
- Routes stay 3-tuples; there's no per-route auth decoration. The middleware parses the database name and required permission from the raw path (service-level middleware runs before livery's router sets path bindings, so we can't rely on `livery_req:binding/2` there).
- `barrel_http_api_keys` (DETS-backed key store) and `barrel_peer_auth` (replication Ed25519 signing) are unchanged. The validated key map is attached to the request via `livery_req:set_meta(auth_ctx, _, _)` for handlers that want to inspect the caller.

## [0.7.2] - 2026-06-04

### Changed
- **HTTP observability moves into livery middleware.** `barrel_http_server` now wires `livery_request_id`, `livery_instrument_trace`, `livery_instrument_metrics`, and `livery_access_log` into the service stack. Every request gets a W3C `traceparent` propagation, an OpenTelemetry server span with semantic-convention attributes, the standard `http.server.active_requests` + `http.server.request.duration` metrics, and an access log line.
- `/metrics` is now served by `livery_metrics:handler/0`. The Prometheus exposition format is unchanged; both livery's HTTP middleware and `barrel_metrics`'s domain registrations feed the same `instrument` registry.
- `barrel_http_handler:handle/2` no longer wraps requests in `barrel_trace:with_extracted_context/2` + `with_http_span/3` — the middleware does it.

### Removed
- HTTP-shaped entries from `barrel_metrics`: `barrel_http_requests` (counter), `barrel_http_request_duration_seconds` (histogram), and the `inc_http_requests/3` + `observe_http_latency/3` helpers. Replaced by livery's OTel-semantic-convention names (`http.server.active_requests`, `http.server.request.duration`). **Dashboards or alerts referencing the old names need updating.**

### Kept
- Domain metrics (doc ops, query ops, replication, db gauges) and the `barrel_trace:*` helpers — they're used by domain code, not HTTP plumbing.

## [0.7.1] - 2026-06-04

### Changed
- Switch `livery` from a git pin against `main` to the hex package
  `0.2.0`.
- **Replace `hackney` with `livery_client` for outbound replication
  HTTP**. `barrel_rep_transport_http` now uses
  `livery_client:request/4` for the `_changes`/`_put_rev`/
  `_revsdiff`/`_sync_hlc` calls, giving outbound replication the
  same composable middleware stack the server side has. The
  underlying transport remains hackney (via livery's
  `livery_client_hackney` adapter), but is no longer a direct
  barrel dep.
- Drop `hackney` from `rebar.config` deps, `barrel_docdb.app.src`
  applications, and the relx release list. It still arrives as a
  transitive dependency of livery.

## [0.7.0] - 2026-06-04

### Changed
- **HTTP layer migrated from cowboy to [livery](https://github.com/benoitc/livery).** All HTTP endpoints, headers, and response shapes are unchanged; the framework swap is internal. The dep graph drops `cowboy` / `cowlib` / `ranch` and adds `livery` (and its transitive `h1` / `h2` / `quic` / `ws` / `webtransport` / `barrel_mcp`). Consumers building a release from source need `livery` in their build environment.
- HTTP/2 is supported out of the box (livery serves H1 and H2 from one listener via h2c upgrade or prior knowledge). HTTP/3 is available via livery and can be enabled in a follow-up.
- HTTP modules are now grouped under `src/web/` (rebar3 `src_dirs` extended).
- `instrument` is now consumed from hex (`1.1.3`) instead of the git pin, matching livery.

### Internal
- `barrel_http_server` calls `livery:start_service/1` + `livery_router:compile/1` instead of cowboy's listener / dispatch APIs.
- `barrel_http_handler` returns value-based responses via `livery_resp:new/3`, `livery_resp:stream/3`. Per-action `handle_action/3` clauses keep the same internal tuple shape; conversion is centralized in `handle/2`.
- `barrel_http_changes_stream` uses `livery_resp:sse/3` with a producer closure that owns the poll/heartbeat loop.

## [0.6.4] - 2026-06-02

### Security
- **API key permissions are now enforced.** Before 0.6.4 the `permissions` field on an `#api_key` record was stored and returned but discarded by the HTTP handler, so a key declared as `[<<"read">>]` could still write, delete, replicate, or bulk-update. A new `authorize/3` step in `barrel_http_handler` maps each `(action, method)` pair to a required permission (`read` / `write` / `admin`) and returns 403 when missing. `is_admin = true` and the "no keys configured yet" mode short-circuit. Admin-gated endpoints (`/keys/*`, `/admin/*`) are unchanged.
- **Chunked attachment metadata no longer goes through `binary_to_term`.** The writer now stores chunked metadata as `<<"BARREL_CHUNK_V1:", json:encode(Meta)/binary>>`; the reader pattern-matches on the tag and falls back to the legacy `term_to_binary` blob via `binary_to_term/2` with `[safe]` for one release (with a warning log). Removes a path that decoded user-controlled bytes as Erlang terms.
- **Query-op whitelist.** `convert_op/1` in the HTTP handler used to call `binary_to_atom/1` on any unrecognized binary, which could exhaust the atom table. The function now accepts only the documented set (`eq`/`==`, `ne`/`!=`/`=/=`, `gt`/`>`, `gte`/`>=`, `lt`/`<`, `lte`/`<=`/`=<`) and rejects everything else with HTTP 400.

### Deprecated
- Legacy `term_to_binary` chunked-attachment metadata. Attachments created on 0.6.3 or earlier are still readable in 0.6.4 with a warning; the fallback will be removed in 0.7.0.

## [0.6.3] - 2026-06-02

### Security
- **MVCC: stale or missing `_rev` on update is now rejected with `{error, conflict}` (HTTP 409).** Previously, `barrel_docdb:put_doc/2,3` and the bulk path silently overwrote the stored body and reported a different winning rev — a classic lost-update bug. The replication path (`put_rev`, bulk entries carrying explicit `history`) is unchanged: the revtree merge handles conflict detection there.
- **Database names are validated.** `barrel_docdb:create_db/1,2` and `delete_db/1` reject anything that does not match `[a-z0-9_-]{1,63}` (system databases keep their leading `_`). HTTP 400 is returned for invalid names at `PUT /db/:db` and `DELETE /db/:db`. Prevents filesystem traversal via the data directory.
- **`delete_db/1` no longer shells out.** Replaces `os:cmd("rm -rf " ++ DbPath)` with `file:del_dir_r/1`. Removes the shell-injection surface that depended on whatever ended up in `DbPath`.

## [0.6.2] - 2026-06-02

### Removed
- JWT authentication (`barrel_docdb_jwt`, `console_public_key` config, `bdb_` token branches in the HTTP handler) and the `jose` dependency. API key auth (`ak_*`) is the only bearer-token scheme.
- Dead VDB benchmark code (`bench/src/workloads/barrel_bench_vdb.erl`, `barrel_bench:run_vdb*`, VDB portions of the HTTP bench) referencing the long-removed `barrel_vdb` module.
- Stale `test/docker/CHECKPOINT.md` referencing removed `barrel_rep_policy` work.

### Changed
- Bumped `instrument` to v1.1.3 for Erlang/OTP 29 compatibility (replaces deprecated `catch Expr` with `try ... catch`).
- Replaced deprecated `catch Expr` patterns in `barrel_docdb`, `barrel_db_server`, `barrel_query`, `barrel_query_cursor`, and the affected test suites; added `barrel_store_rocksdb:safe_release_snapshot/1` helper.
- Updated docs to drop references to removed modules (`run_vdb` benchmark; "Sharding Strategy" renamed to "Bucketing Strategy" for the time-bucketed posting list).

## [0.6.1] - 2026-06-01

### Changed
- Moved to Erlang/OTP 28 (CI containers, Dockerfile builder, and docs).

### Fixed
- Docker image now boots on OTP 28: the runtime stage uses `debian:trixie-slim` to match the `erlang:28` builder's glibc (it was left on `bookworm-slim`, so ERTS failed with `GLIBC_2.38 not found`).
- Release CI job builds in the `erlang` container instead of installing Erlang from `packages.erlang-solutions.com`, which had failed the v0.6.0 release.
- The relx release version was hardcoded `0.4.2`; it now matches the application version.

## [0.6.0] - 2026-05-26

### Added
- `GET /.well-known/barrel` node identity endpoint returning `node_id`, `version`, and (when peer auth is initialized) `public_key`, with no dependency on discovery or federation
- Public `barrel_docdb:node_id/0` seam (persistent `_node_id` system document) for an external discovery or cluster layer

### Removed
- **Virtual Database (VDB) and sharding**: removed `barrel_vdb*`, `barrel_shard_map`, and `barrel_shard_rebalance` with the `/vdb` HTTP endpoints
- **Federation and peer discovery**: removed `barrel_federation` and `barrel_discovery` with the `/_federation`, `/_peers`, and `/.well-known/barrel` endpoints
- **Replication policies**: removed `barrel_rep_policy` (chain/group/fanout/tiered patterns) and the `/_policies` endpoints; the replication engine (`barrel_rep`, `barrel_rep_tasks`, transports) is unchanged
- **Tiered storage**: removed `barrel_tier` and the `/db/:db/_tier/*` endpoints; the `created_at`/`expires_at`/`tier` entity columns are kept as reserved fields so the on-disk format is unchanged
- **Materialized views**: removed `barrel_view`, `barrel_view_index`, `barrel_view_sup`, the `register_view`/`unregister_view`/`query_view`/`list_views`/`refresh_view` API, and the `/db/:db/_views/*` endpoints

### Changed
- Reduced scope to a document-database core: CRUD with MVCC, declarative queries, changes feed, attachments, pub/sub, and the replication engine. Sharding, tiering, and clustering can be built on top via the public API (changes feed, revision primitives, HLC, system and local documents, store/index introspection, and metrics).
- `barrel_peer_auth` is retained (used by the HTTP replication transport); its node id now persists in a `_node_id` system document.
- Upgraded the instrument dependency to v1.1.2.

### Fixed
- Metrics exemplar reservoir table is no longer lost on an application stop/start. It is initialized in `barrel_metrics:setup/0`, which previously caused `barrel_rep_tasks` to crash on restart when the first histogram value was recorded.

## [0.5.0] - 2026-04-04

### Added
- **Views with Map/Reduce**: Secondary indexes with map/reduce support
  - Module-based views with `map/1` and optional `reduce/3` callbacks
  - Query-based views with declarative key/value extraction
  - Built-in reduce functions: `_count`, `_sum`, `_stats`
  - Rereduce support for merging results from sharded queries via `merge_reduced_results/2`
  - Manual and automatic refresh modes
- **OpenAPI 3.0 Specification**: Full API documentation with Swagger UI at `/api-docs`
- `barrel_docdb:fold_docs/4` with options support (limit, skip, start_key, end_key)
- `barrel_query:matches/2` for simple condition matching outside of queries

### Changed
- Migrated from legacy `{Epoch, Counter}` sequence format to HLC timestamps throughout
- Removed `barrel_sequence` module (functionality consolidated in `barrel_hlc`)
- Upgraded hlc to 3.0.3 and match_trie to 1.0.0
- Upgraded rocksdb to 2.6.2
- Removed unused bitmap dependency

### Fixed
- Multiple dialyzer warnings across modules
- Dead code removal in fold_range functions
- Unmatched return values in shard rebalancing

## [0.4.1] - 2026-03-08

### Fixed
- Snapshot handle leak in chunked exists/prefix query paths where temporary
  snapshots were never released
- Streaming attachment uploads now clean up orphaned chunks on failure via
  new `abort_stream/1` function

### Added
- `abort_attachment_writer/1` API to clean up partial attachment uploads
- Snapshot support for pure compare queries ensuring read consistency
- `fold_compare_docids_with_snapshot/8` in barrel_ars_index
- `fold_range_posting_compare_with_snapshot/6` in barrel_store_rocksdb

## [0.4.0] - 2026-03-05

### Added
- Authentication support for federation queries with `bearer_token` and `basic_auth` formats
- Federation-level auth stored with config, per-query auth override in find options
- `doc_ids` filtering for changes feed to subscribe to specific documents
- Query-based filtering for changes feed with `where` conditions
- GitHub Actions CI/CD workflows for automated testing and releases

### Fixed
- VDB replication setup by adding `sync` option to peer registration
- JSON encoding of peer public keys in HTTP API responses

### Changed
- Upgraded hackney to 3.2.1

## [0.3.2] - 2026-02-13

### Fixed
- SSE changes stream now correctly includes document bodies when `include_docs=true`
- Fixed `get_changes_full_scan` to route to filtered path when `include_docs` is requested
- Fixed `get_changes_filtered` to set `NeedsDoc` flag when `include_docs=true`

### Added
- `changes_stream_include_docs` test to verify document bodies in SSE events

## [0.3.1] - 2026-02-13

### Verified
- SSE changes stream stability confirmed with all tests passing
- `since=now` parameter handling working correctly
- Heartbeat mechanism keeping connections alive

## [0.3.0] - 2026-02-12

### Added

#### JWT Authentication
- ES256 (ECDSA P-256) JWT token validation for API authentication
- Token format: `bdb_<base64-encoded-JWT>` prefix for barrel_docdb tokens
- Required claims validation: `sub`, `typ`, `oid`, `prm`, `exp`
- Workspace isolation via optional `wid` claim
- Permission-based access control with `is_admin` flag
- File-based or inline PEM key configuration

#### Usage Reporting
- New `barrel_docdb_usage` module for database statistics
- `GET /admin/usage` - Get usage stats for all databases
- `GET /admin/databases/:db/usage` - Get stats for specific database
- Stats include: document_count, storage_bytes, memtable_size, sst_files_size

#### Ed25519 Peer Authentication for P2P Replication
- `barrel_peer_auth` gen_server for Ed25519 key management
- Automatic keypair generation on startup
- Request signing with canonical format: `timestamp|peer_id|method|path|body_hash`
- 5-minute timestamp window for replay protection
- HTTP headers: `X-Peer-Id`, `X-Peer-Timestamp`, `X-Peer-Signature`
- Public key exposed via `/.well-known/barrel` discovery endpoint
- Optional `peer_auth` option in HTTP transport (disabled by default)

### Fixed

#### SSE Changes Stream Reliability
- Reduced heartbeat interval from 60s to 30s (below Cowboy's idle_timeout)
- Added `idle_timeout => 120000` to Cowboy protocol options
- Added `request_timeout => infinity` for long-running SSE streams
- Fixed `since=now` parameter crash (was calling `barrel_hlc:encode(now)`)
- Simplified `parse_since/1` to only accept valid base64-encoded HLC binaries

#### API Compatibility
- Fixed `barrel_docdb_usage` to use `db_pid/1` + `get_store_ref/1` API
- Updated all HTTP tests for hackney 3.x API (body in 4th tuple element)

### Changed
- Upgraded hackney to 3.0.2
- Default heartbeat for SSE streams changed from 60s to 30s

### Tests Added
- `barrel_docdb_usage_SUITE` - 6 tests for usage statistics module
- `barrel_docdb_jwt_SUITE` - 10 tests for JWT authentication
- `barrel_peer_auth_tests` - 11 EUnit tests for peer authentication
- HTTP usage endpoint tests (4 tests in barrel_http_SUITE)
- SSE stream tests: `changes_stream_since_now`, `changes_stream_heartbeat`

## [0.2.0] - 2025-01-12

### Added

#### Virtual Databases (VDB) - Automatic Sharding
- New VDB layer for horizontal scalability with automatic document sharding
- Consistent hashing of document IDs for even distribution across shards
- Scatter-gather queries across all shards with merged results
- HTTP API endpoints for VDB operations (`/vdb/:vdb/*`)
- Shard split and merge operations for rebalancing
- Multi-datacenter sharding with zone-aware placement
- Cross-node VDB configuration synchronization
- VDB shard replication with automatic failover
- Import from regular database to VDB (`POST /vdb/:vdb/_import`)
- VDB benchmarks and comprehensive documentation

#### HTTP/2 Support
- HTTP/2 cleartext (h2c) via HTTP Upgrade mechanism
- HTTP/2 over TLS (h2) with ALPN negotiation
- Graceful degradation to HTTP/1.1 for legacy clients
- Environment variable configuration for TLS/HTTP2 in releases

#### Docker & CI
- Multi-architecture Docker builds (amd64 + arm64)
- GitLab CI pipeline with buildx for container builds
- Docker multi-region VDB test suite

#### Documentation
- VDB overview and sharding guide
- Multi-datacenter deployment documentation
- Advanced features guide with curl examples
- HTTP API reference for VDB endpoints

### Fixed
- HTTP test suite isolation by unlinking server process
- VDB cross-node sync authentication
- Replication policy auth and checkpoint error handling
- HTTP benchmarks and documentation links

### Changed
- Upgraded hackney to 2.0.0-beta.1
- Added LZ4 and Snappy libraries for RocksDB native build

## [0.1.0] - 2024-12-01

### Added
- Initial release
- Document CRUD with MVCC revision trees
- Declarative queries with automatic path indexing
- Real-time subscriptions via MQTT-style path patterns
- HTTP API with REST endpoints
- Peer-to-peer replication with configurable patterns (chain, group, fanout)
- Federated queries across multiple databases
- Tiered storage with automatic TTL/capacity-based migration
- Prometheus metrics for monitoring
- HLC ordering for distributed event coordination
