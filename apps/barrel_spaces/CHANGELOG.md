# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [1.3.0] - 2026-10-01

### Added
- `create_space/1` and `open_space/2` take `embedding`, a record-mode policy. The space document records the policy's fields and the embedder's identity (provider, model, dimensions, distance, fingerprint), never the embedder config, which can hold paths and API keys while the registry replicates.
- A space created in record mode reopens in record mode: `open_space/2` without `embedding` rebuilds the policy from the recorded fields and the embedder of the `embedder` application env, on any node. The embedder must carry the recorded fingerprint: `{error, {embedder_mismatch, _}}` otherwise, `{error, {embedder_required, Fingerprint}}` when the node has none.

- `create_space/1` takes `vectordb => none` for a space without a vector store; the space document records it and the space reopens without one on every node.

### Changed
- `drop_space/1,2` opens the space as a plain database before deleting it, so dropping needs no embedder.
- Requires barrel `~> 1.11`.
- `barrel_caps:grant/2` stores `token_hash` as lowercase hex, so a grant document is valid JSON and the spaces registry replicates over HTTP. `verify/3` also accepts the raw 32-byte hash of grants minted before 1.3.0; they keep that form until reminted, and replicate over HTTP only between peers running barrel_docdb 1.8.0 and barrel_server 1.11.0.

## [1.2.2] - 2026-09-24

### Fixed
- A space's vector store path is resolved on the node that opens it, from the local `data_dir`. `create_space/1` records the default layout relative to `data_dir` (`vec_path` = `<id>_vec`), so a registry doc replicated to another node no longer opens, or creates, the store under the first node's directory.
- Docs written by 1.2.1 hold an absolute `vec_path`: kept when it lies under the local `data_dir`, otherwise a default-layout path opens at `<local data_dir>/<id>_vec`. A custom `db_path` is recorded absolute with `vec_custom` and kept on every node. An explicit `db_path` on open wins, as before.

## [1.2.1] - 2026-09-14

### Changed
- Requires `barrel ~> 1.3`. The old `barrel ~> 1.0` floor still resolved barrel 1.0 with barrel_vectordb 2.1 and its `gen_batch_server` dependency.

## [1.2.0] - 2026-08-26

### Added
- `pin_context/3` takes caller-supplied `id`, `pinned_at`, and `metadata` (defaults unchanged); a pin id already present in the session fails with `{error, conflict}` instead of quietly duplicating.
- `import_message/3` takes a caller-supplied `id` that keys the document (an existing id conflicts), closing the last place the API minted an id a consumer might already own.

### Changed
- `get_messages` orders by timestamp (id as tiebreak) instead of raw id order, so caller-supplied message ids that do not sort chronologically still list in order. Generated ids keep their exact previous order.

## [1.1.0] - 2026-08-25

### Added
- Sessions without expiry: `ttl => infinity` (or 0) creates a durable session the TTL machinery skips; `touch/2` and the other mutations return `{ok, 0}` for it.
- Indexed session listing: `barrel_session:list/2` takes `match` (field path to value, resolved through the space database's path indexes) and `limit`; the `agent` filter now uses the same indexed path.
- Session import: `create/2` accepts a caller-supplied `id`; `import_session/2` and `import_message/3` move an existing corpus in with its own ids and timestamps, without bypassing the schema.
- `barrel_handoff:accept/2` with `session => false` accepts on the token discipline alone: no space open, no session created, for consumers with their own session model.
- Token-to-handoff resolution reads a `handoff_token:` index doc written at create (scan fallback for pre-1.1 handoffs).
- The registry database name is configurable (`registry_db` app env, default `_barrel_spaces`); replicating the registry, and the logical (space-less) capability scopes, are now documented as intended.

### Fixed
- README described `chain/2` as walking a handoff backwards; it chains forward (completes the presented handoff, mints the next one with lineage).

## [1.0.1] - 2026-07-11

### Fixed
- Declare the sibling Hex dependencies (barrel, barrel_docdb, barrel_crypto). 1.0.0 shipped with no requirements because they were in a `hex` profile, which rebar3_hex drops from the package; a consumer got an undef at runtime.

## [1.0.0] - 2026-07-10

First tagged release of the agent layer: spaces (shared context databases),
capability tokens, sessions with TTL, and handoffs. See the umbrella
[CHANGELOG](../../CHANGELOG.md).
