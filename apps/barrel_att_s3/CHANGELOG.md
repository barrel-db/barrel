# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Fixed
- Two writes to the same attachment at the same time could let an older replicated write replace a newer one and leave two change feed rows for it. Writes to one attachment now run one at a time, so the newest write wins and the feed keeps one row.
- After the clock went back, for example a node clock reset, new attachment changes could be placed before a replication checkpoint and never sync. Feed positions now always move forward.

## [0.1.3] - 2026-11-07

### Changed
- Requires barrel_docdb `~> 2.0`, whose replication keeps every concurrent write. See the barrel_docdb changelog for its breaking changes.

## [0.1.2] - 2026-10-26

### Changed
- Requires livery `~> 0.10.7` and livery_s3 `~> 0.2.1`. The old livery `~> 0.4.4` requirement could not be met together with livery_s3 0.2.1, so the backend could not be installed from Hex next to current livery.

## [0.1.1] - 2026-08-23

### Fixed
- `mimerl` and `livery` are declared in the application's `applications`:
  both are called directly but were only rebar deps, so a release that
  derives its app list from `applications` could ship without them.

## [0.1.0] - 2026-08-09

Initial release. `barrel_att_backend` implementation against S3-compatible
object stores (AWS S3, MinIO, Garage) via `livery_s3`.

### Added
- Whole-blob and streaming put/get/delete, multipart only once the buffer
  crosses `part_size`.
- Opt-in write-conflict detection (`create_only`/`expected_etag`), backed by
  a real per-store capability probe at `open/2` -- a store that can't
  verifiably enforce conditional writes (Garage, by design) fails fast
  rather than silently accepting an unprotected write.
- A local attachment feed (`att_changes/4`, `att_floor/2`, `sweep_att_feed/3`,
  `rebuild_feed/2`) makes S3-backed databases full bidirectional replication
  participants, not just replication sources.
- Non-blocking eager-copy branching: `checkpoint/2` returns immediately, the
  per-attachment S3 copy runs as a background sweep kicked off from the
  branch's own first `open/2`; reading an attachment the sweep hasn't
  reached yet returns a clean `att_sync_pending` error instead of blocking
  or serving stale data.
- `destroy/2`, wired into `delete_db`, erases the bucket objects under a
  store's own prefix.
- `barrel_att_s3_multipart_gc`: a background sweeper that garbage-collects
  multipart uploads abandoned by a crash mid-upload, configurable via
  `multipart_gc_interval`/`multipart_gc_max_age`.
- Both backend selection and write-conflict detection are reachable over
  `barrel_server`'s HTTP routes (`att_opts` on `PUT /db/:name`,
  `If-Match`/`If-None-Match` on the attachment `PUT` route).
