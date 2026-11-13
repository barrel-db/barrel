# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed
- Requires livery_s3 0.3.3 and livery 0.13.

## [0.2.1] - 2026-11-09

### Added
- The attachment count of a database on S3, for barrel_docdb 2.4.0's `barrel_db_attachments_total` and `db_info`. Every attachment write updates it with the feed row it already writes. Needs barrel_docdb 2.4.0 or later.

## [0.2.0] - 2026-11-09

### Changed
- Attachments of an encrypted database are now encrypted in the bucket. They used to go to S3 in plaintext, with their SHA-256 readable in the object metadata, so anyone with access to the bucket could read them. Each object is now encrypted with AES-256 under a key derived from the database key; sizes are unchanged, and streaming, multipart uploads, resumed and range reads work as before. A full read is checked against the attachment's digest, so a changed object fails instead of returning altered bytes. Needs barrel_docdb 2.3.1 or later.
- The local attachment feed of an encrypted database, which holds doc ids, attachment names and digests, is now encrypted with the database.
- An encrypted database that already stored attachments in S3 keeps working: its plaintext objects stay readable and are rewritten encrypted in the background when it opens. Once every object is encrypted, a plaintext object is refused. Plaintext databases are unchanged.
- What stays visible in the bucket: object keys contain the document id and attachment name, and object sizes are the attachment sizes.

## [0.1.5] - 2026-11-08

### Fixed
- On a new branch, an attachment deleted while the branch was still copying its attachments in the background could come back with the parent's content, and a write could be replaced by the parent's version. The copy now takes the same per-attachment lock as writes and deletes, so whatever the branch does wins.
- A streamed attachment read failed when the store closed the connection partway through, which an overloaded S3 server does now and then. The read now asks again for the bytes it has not received yet. If the attachment was replaced in the meantime, the read fails instead of mixing two versions.
- A new branch copies its attachments in the background. One failed copy used to stop that copy until the database was opened again, and until then the branch answered that its attachments were still syncing. The copy now retries failed attachments itself for about a minute.

### Changed
- Requires rocksdb 3.2.1 (RocksDB 11.13). The build no longer downloads anything, so it works offline and behind a proxy, and it fixes rocksdb crashes, for example when a database handle is closed while another process still uses it, or when a handle is used after it was released.

## [0.1.4] - 2026-11-07

### Fixed
- Two writes to the same attachment at the same time could let an older replicated write replace a newer one and leave two change feed rows for it. Writes to one attachment now run one at a time, so the newest write wins and the feed keeps one row.
- After the clock went back, for example a node clock reset, new attachment changes could be placed before a replication checkpoint and never sync. Feed positions now always move forward.
- An S3 database could not be a replication target under the new default attachment durability, and its attachment timestamps were not compared by replication. Its local change feed is now synced before checkpoints and its timestamps are used.

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
