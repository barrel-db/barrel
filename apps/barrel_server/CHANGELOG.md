# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [1.1.0] - 2026-07-14

### Added
- `barrel_server_api`: exposes the REST/sync routes as a grouped livery route
  list (`routes/0,1`) and compiled router (`router/0,1`) so a host livery
  application can mount them, optionally under a sub-path, and own auth. Groups:
  `meta`, `db`, `sync`, `timeline`, `search`, `spaces`, `mcp`; the default is the
  DB surface (`db`, `sync`, `timeline`, `search`). See the
  [embedding guide](../../docs/guides/embedding-barrel-server.md).

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
