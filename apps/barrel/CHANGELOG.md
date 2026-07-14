# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [1.1.0] - 2026-07-14

Coordinated release with `barrel_server` 1.1.0 (the embeddable server API).
No changes to the `barrel` API.

## [1.0.1] - 2026-07-11

### Fixed
- Repoint the barrel_vectordb dependency to 2.1.2 (2.1.1 shipped without its barrel_embed requirement) and use `~>` pins so a sibling patch does not force a re-release here.

## [1.0.0] - 2026-07-10

First tagged release of the embeddable database. Composes `barrel_docdb`,
`barrel_vectordb`, and `barrel_crypto` under one id, adding record mode, the
timeline (branch/PITR/merge), and BQL. See the umbrella
[CHANGELOG](../../CHANGELOG.md) for the coordinated release notes.
