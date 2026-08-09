# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [1.4.0] - 2026-08-09

### Added
- `PUT /db/:name` accepts an `att_opts` JSON body to select the
  attachment backend (e.g. S3) at creation time.
- The attachment `PUT` route wires `If-Match`/`If-None-Match` headers to
  `create_only`/`expected_etag` write-conflict detection.

## [1.3.0] - 2026-07-19

### Added
- `ngram_search` MCP tool: exact substring or regex (mode literal or regex)
  search over a database's documents via the barrel_ngram trigram index, each
  hit carrying the document id and match spans. Read-only; the corpus is opened
  lazily per database.

## [1.2.1] - 2026-07-18

### Fixed
- A signed attachment upload binds the body to the signed
  `x-barrel-content-sha256` header (not the unsigned `x-barrel-digest`), so an
  on-path body swap on a signed upload is rejected.
- mTLS is dropped from the accepted auth set unless the listener sets
  `verify_peer`, so a certless client is never authenticated by a config gap.
- The replication changes wire filter bounds regex work and nesting depth
  (ReDoS), and vector search `k` is clamped.
- The live-query bridge opens databases in the caller, so a cold open no longer
  blocks other subscribe/snapshot/unsubscribe calls; a snapshot reuses a cached
  id-sorted view instead of re-sorting in the loop.

## [1.2.0] - 2026-07-17

### Added
- Signed-request auth for the sync wire: Ed25519 signatures over
  `ts|keyId|method|path|sha256(body)`, with replay protection and a skew window.
  Enabled by `auth => #{accept => [..., signed], signers => #{KeyId => PubKey}}`.
- mTLS transport gate: TLS listeners with `verify => verify_peer` refuse a client
  without a CA-signed certificate (`accept => [mtls]`).
- Multi-protocol serving: `listeners => #{http, https, http3}` serves HTTP/1.1,
  HTTP/2, and HTTP/3; a shared `tls` config drives the TLS listeners. See the
  [synchronization guide](https://github.com/barrel-db/barrel/blob/main/docs/guides/synchronization.md).

### Changed
- The `auth` key is unchanged without an `accept` list (bearer-only, as before);
  the new methods are opt-in and additive. H3 is TLS-serving but not yet a
  client-cert gate; mapping a client cert to an identity needs a livery change.

## [1.1.0] - 2026-07-14

### Added
- `barrel_server_api`: exposes the REST/sync routes as a grouped livery route
  list (`routes/0,1`) and compiled router (`router/0,1`) so a host livery
  application can mount them, optionally under a sub-path, and own auth. Groups:
  `meta`, `db`, `sync`, `timeline`, `search`, `spaces`, `mcp`; the default is the
  DB surface (`db`, `sync`, `timeline`, `search`). See the
  [embedding guide](https://github.com/barrel-db/barrel/blob/main/docs/guides/embedding-barrel-server.md).

### Changed
- `barrel_server_http` assembles its route table from `barrel_server_api`
  (`groups => all`); the standalone service behaves as before.

## [1.0.1] - 2026-07-11

### Fixed
- Declare the sibling Hex dependencies (barrel, barrel_spaces). 1.0.0 omitted them (they were in a `hex` profile, which rebar3_hex drops).

## [1.0.0] - 2026-07-10

First tagged release of the network server: REST/JSON and MCP over `barrel`
and `barrel_spaces` using `livery`. The full server test suite set now runs in
CI. See the umbrella [CHANGELOG](../../CHANGELOG.md).
