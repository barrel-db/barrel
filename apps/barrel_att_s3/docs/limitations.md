# Limitations

What this backend does not do yet, and the one behavior difference between
S3-compatible stores you need to know before choosing one. Read this before
putting it into production.

## Branching cost

`barrel_docdb:branch_db/3` works against an S3-backed database, but forking
isn't O(1) the way it is for the default RocksDB backend (a hard-link
checkpoint). S3 has no equivalent to a hard link, so a fork does a real
server-side copy: one `CopyObject` call per attachment the parent has.

That copy runs in the background, not inline in `branch_db/3` -- forking
returns as soon as the (cheap, local) part is done, regardless of how many
attachments the parent has. What's still catching up is the attachment
*bytes*: reading one the copy hasn't reached yet returns
`{error, {att_sync_pending, {DocId, AttName}}}` (HTTP 503) rather than
blocking or returning stale data. A write on the branch is never blocked by
this and is never overwritten by the catch-up copy once it lands.

For a parent with many or large attachments, expect a freshly forked
branch's attachments to become fully available over some real, non-zero
window after `branch_db/3` returns -- not instantly, and not bounded by
anything other than how fast the copy runs.

## Deleting a database

`barrel_docdb:delete_db/2` erases the bucket objects too, not just the local
directory -- but only automatically if the database is open when you delete
it. For a database that's already closed, pass the same `att_opts` you
created it with (`delete_db(Name, #{att_opts => ...})`): nothing else can
recover S3 credentials for a closed database, so without it the bucket
objects are left behind while the local directory is still removed.

Deleting an already-open database can still race a fork-copy sweep that's
in flight for it (see "Branching cost" above): the delete's object listing
is a one-time snapshot, and anything the sweep copies in after that
snapshot is never revisited. Closing this fully needs a live-process
registry to find and join an in-flight sweep for a given store, which is
out of scope today -- avoid deleting a database while a fork of it is
still catching up. This is narrower than it sounds: a freshly reopened
closed database never hits it (`delete_db` skips spawning a sweep it
would only abandon), so the gap is limited to a database still open from
the branch operation itself.

## HTTP surface

Both backend selection and write-conflict detection work over HTTP now --
see [getting started](getting-started.md). Two caveats worth knowing:

- **Re-`PUT`-ing an already-open database with different `att_opts` is a
  silent no-op.** `barrel_dbs`'s in-memory fast path returns the
  already-open handle without even looking at the new Opts. It only takes
  effect the next time the database is cold-opened (idle-swept or after a
  restart), with nothing validating the new config against what the
  database actually holds -- pointing an existing S3-backed database at a
  different bucket this way is a real footgun, not something this app
  protects you from. This is a property of `barrel_dbs:ensure/2` itself,
  not specific to `att_opts` or to going through HTTP.
- **`If-None-Match`/`If-Match` are accepted, but only enforced against an
  S3-backed database.** The default RocksDB (blob) backend's `put/6` never
  inspects `create_only`/`expected_etag` at all, so sending these headers
  against a non-S3-backed database is silently ignored -- same as the
  Erlang API already was; see "Concurrent writers to the same key" below.

## Abandoned multipart uploads

A crash between starting and completing a multipart upload leaves orphaned
parts that S3 keeps billing until aborted or expired. `abort_stream/1`
covers an in-process failure, but not a hard crash mid-upload.

A background sweeper (`barrel_att_s3_multipart_gc`, started with the app)
periodically aborts multipart uploads older than a configurable age --
see [getting started](getting-started.md#multipart-upload-garbage-collection)
for the two tunables. `delete_db` also triggers an immediate, targeted
sweep of the deleted database's own prefix, so its dangling uploads don't
wait for the next scheduled pass.

As a backstop -- it covers the window before the sweeper's first pass, or
a deployment that disables it (`multipart_gc_interval => 0`) -- also set a
bucket lifecycle rule:

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

Ordinary replication is not this risk in the usual case: `barrel_rep` always
checks a target's actual stored digest via `get_info/4` before transferring
anything, so two S3-backed databases replicating into the same bucket don't
redundantly transfer or corrupt each other's data. Replication's LWW
convergence is independent of `create_only`/`expected_etag` entirely --
those are opt-in, S3-level conditional writes you pass explicitly;
replication instead compares each write's HLC origin against the target's
own local feed (`barrel_att_feed:check/6`) before writing, and that S3 write
is always a plain, unconditional `PutObject`/`CopyObject` once the check
passes, on every store including Garage. The one residual gap: if two
replication streams push to the *same* target attachment at the same
instant, both can pass the local LWW check before either has written, and
whichever S3 write physically lands last wins -- not necessarily the one
with the newer origin. Narrow, store-independent, and not solved further
here (same class of check-then-act race accepted elsewhere in this design).

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
