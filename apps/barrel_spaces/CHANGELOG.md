# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

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
