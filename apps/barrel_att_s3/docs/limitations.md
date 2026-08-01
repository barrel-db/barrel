# Limitations

What this backend does not do yet, and the one behavior difference between
S3-compatible stores you need to know before choosing one. Read this before
putting it into production.

## No change feed yet

`barrel_att_s3_store` implements only the required `barrel_att_backend`
callbacks -- `put`/`get`/`delete`/streaming. It does not implement
`att_changes/4`, `att_floor/2`, `sweep_att_feed/3`, or `rebuild_feed/2`: the
feed those callbacks provide is what replication reads to know what changed.
A full change feed (a local metadata-only index, so `att_changes/4` doesn't
need to list the whole bucket) is planned as a follow-on; until then, this
backend has no feed at all.

This has a real, asymmetric consequence for replication:

- **Source lacks a feed** (an S3-backed database as the replication
  *source*): detected automatically, reports `att_sync => skipped`. Nothing
  breaks, attachments just don't replicate out.
- **Target lacks a feed** (an S3-backed database as the replication
  *target*): **not** detected -- `put`/`delete` are required callbacks, so
  writes land normally, but there's no feed on the target to check
  `origin_hlc` against, so the last-write-wins guard the default backend
  enforces silently does not apply.

Until the feed lands, treat an S3-backed database as a replication *source*
only, not a target, if last-write-wins matters to you.

## No branching

`checkpoint/2` isn't implemented either (it needs the same feed). Calling
`barrel_docdb:branch_db/3` against an S3-backed database returns a clean
`{error, _}` rather than crashing, but branching itself isn't supported for
this backend yet.

## No HTTP surface

Two things are Erlang-API-only right now, not exposed over `barrel_server`'s
HTTP routes:

- **Backend selection.** `PUT /db/:name` reads only the database name from
  the URL and never parses a request body, so there is no way to pass
  `att_opts` over HTTP. Create S3-backed databases through
  `barrel_docdb:create_db/2` directly.
- **Write-conflict detection.** `create_only`/`expected_etag` have no
  `If-Match`/`If-None-Match` header wiring on the attachment PUT route.
  Drive them through `barrel_docdb:put_attachment/5` directly.

## Abandoned multipart uploads

A crash between starting and completing a multipart upload leaves orphaned
parts that S3 keeps billing until aborted or expired. `abort_stream/1`
covers an in-process failure, but not a hard crash mid-upload. Set a bucket
lifecycle rule to clean these up automatically:

```json
{
  "Rules": [{
    "ID": "abort-incomplete-multipart-uploads",
    "Status": "Enabled",
    "AbortIncompleteMultipartUpload": { "DaysAfterInitiation": 1 }
  }]
}
```

## Concurrent writers to the same key

Two uncoordinated processes writing the same attachment key -- the S3
equivalent of two RocksDB processes pointed at one data directory, which
RocksDB's file lock prevents structurally and S3 has no equivalent for --
can clobber each other. `create_only`/`expected_etag` (see [getting
started](getting-started.md)) turn this from a silent-corruption risk into
a loud `{error, {conflict, _}}` for a caller that opts in, but only on a
store that can actually enforce them -- see the table below.

Ordinary replication is not this risk: `barrel_rep` always checks a target's
actual stored digest via `get_info/4` before transferring anything, so two
S3-backed databases replicating into the same bucket don't redundantly
transfer or corrupt each other's data.

## Store compatibility

The one difference you need to know before choosing a store: whether it
actually enforces the conditional writes `create_only`/`expected_etag`
depend on. `open/2` runs a capability probe against the configured bucket
and records the verified result -- a store that didn't verifiably enforce it
gets `{error, conditional_writes_unsupported}` on every `create_only`/
`expected_etag` call, rather than a silent, unprotected overwrite.

| Store | Conditional writes (`create_only`/`expected_etag`) | Notes |
|-------|------------------------------------------------------|-------|
| AWS S3 | Supported (since 2024) | |
| MinIO | Supported (since 2023) | |
| Garage | **Not supported** | ["Structurally impossible to implement in Garage due to the lack of a consensus algorithm, which is one of Garage's core design choices which we cannot reconsider."](https://garagehq.deuxfleurs.fr/documentation/reference-manual/known-issues/) Not a bug, not planned. |

If you need real protection against concurrent writers to the same
attachment key, use AWS S3 or MinIO. On Garage, the one-writer-per-key
discipline is a purely operational rule -- there is no backstop.
