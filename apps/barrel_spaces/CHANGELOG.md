# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [1.0.1] - 2026-07-11

### Fixed
- Declare the sibling Hex dependencies (barrel, barrel_docdb, barrel_crypto). 1.0.0 shipped with no requirements because they were in a `hex` profile, which rebar3_hex drops from the package; a consumer got an undef at runtime.

## [1.0.0] - 2026-07-10

First tagged release of the agent layer: spaces (shared context databases),
capability tokens, sessions with TTL, and handoffs. See the umbrella
[CHANGELOG](../../CHANGELOG.md).
