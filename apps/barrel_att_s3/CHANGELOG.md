# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

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
