# Getting started

`barrel_att_s3` stores a database's attachment bytes in an S3-compatible
bucket instead of `barrel_docdb`'s default embedded RocksDB instance. Use it
when attachments should live in object storage -- AWS S3, MinIO, or Garage --
rather than on the same disk as the database. This page takes you from an
empty bucket to your first attachment.

## Requirements

Build the umbrella (or `barrel_docdb`) with the `s3` rebar3 profile, which
adds `barrel_att_s3` and its dependency on `livery_s3`:

```console
$ rebar3 as s3 compile
```

A plain `rebar3 compile` never touches this app or `livery_s3` -- they stay
out of the default (embeddable) build.

## Point a database at S3

Pass `att_opts` when creating the database. `backend => s3` selects this
module; everything under `s3` except `bucket` and `part_size` passes
straight to `livery_s3:new/1`, so any option that client accepts (custom
`endpoint`, `region`, credentials, path-style addressing, and so on) works
here too.

```erlang
{ok, _} = barrel_docdb:create_db(<<"mydb">>, #{
    att_opts => #{
        backend => s3,
        s3 => #{
            bucket => <<"my-bucket">>,
            endpoint => <<"https://s3.eu-west-1.amazonaws.com">>,
            region => <<"eu-west-1">>,
            access_key_id => <<"AKIA...">>,
            secret_access_key => <<"...">>
        }
    }
}).
```

`bucket` is required; `open/2` returns `{error, missing_bucket}` without it.
The bucket itself is not created for you -- create it before pointing a
database at it.

Or over HTTP: `PUT /db/:name` accepts the same shape as a JSON body.

```console
$ curl -X PUT http://localhost:8080/db/mydb \
    -H 'content-type: application/json' \
    -d '{
      "att_opts": {
        "backend": "s3",
        "s3": {
          "bucket": "my-bucket",
          "endpoint": "https://s3.eu-west-1.amazonaws.com",
          "region": "eu-west-1",
          "access_key_id": "AKIA...",
          "secret_access_key": "..."
        }
      }
    }'
```

An empty body still works exactly as before, defaulting to the RocksDB
backend. A bad `backend` value or an `s3` key that isn't a real
`livery_s3`/`barrel_att_s3_store` option is a `400 bad_att_opts`, not a
crash or a silent fallback to the default backend.

## Use it

Once the database exists, every attachment function on `barrel_docdb` works
exactly as it does with the default backend -- the backend choice lives
entirely in `att_opts`, set once at creation:

```erlang
{ok, Info} = barrel_docdb:put_attachment(<<"mydb">>, <<"doc1">>,
    <<"greeting.txt">>, <<"Hello, World!">>),

{ok, Data} = barrel_docdb:get_attachment(<<"mydb">>, <<"doc1">>,
    <<"greeting.txt">>),

ok = barrel_docdb:delete_attachment(<<"mydb">>, <<"doc1">>,
    <<"greeting.txt">>).
```

Large attachments stream through `put_stream`/`write_chunk`/`finish_stream`
under the hood (this is also the path every *replicated* attachment goes
through) -- you don't call these directly through `barrel_docdb`'s API, but
it's worth knowing: a stream buffers in memory and only starts a multipart
upload once the buffer crosses `part_size` (default 8 MiB), so a small
attachment costs one `PutObject` call, not three.

## Tuning `part_size` for Garage

If you're backing a database with Garage, set `part_size` to match your
cluster's configured `block_size`. Garage's own documentation warns that
multipart parts not aligned to `block_size` cause quadratic-complexity
metadata overhead:

```erlang
s3 => #{
    bucket => <<"my-bucket">>,
    endpoint => <<"http://garage.internal:3900">>,
    region => <<"garage">>,
    access_key_id => <<"...">>,
    secret_access_key => <<"...">>,
    part_size => 8 * 1024 * 1024  %% match your garage.toml block_size
}
```

AWS S3 and MinIO don't need this tuning; the default is fine.

## Multipart upload garbage collection

A background sweeper aborts multipart uploads older than
`multipart_gc_max_age` seconds, checking every `multipart_gc_interval`
milliseconds. Every database `open/2`s registers its own bucket/prefix
with it automatically -- no extra wiring needed. Defaults: 24 hours / 1
hour. Set either via `sys.config` or `application:set_env/3`:

```erlang
{barrel_att_s3, [
    {multipart_gc_interval, 1800000},  %% 30 minutes
    {multipart_gc_max_age, 43200}      %% 12 hours
]}
```

Set `multipart_gc_interval` to `0` to disable the periodic sweep entirely
(a bucket lifecycle rule is still worth setting as a backstop -- see
[limitations](limitations.md#abandoned-multipart-uploads)).

## Write-conflict detection (optional)

Pass `create_only => true` or `expected_etag => Etag` to `put_attachment/5`
to guard against a concurrent write to the same attachment:

```erlang
%% only succeeds if nothing exists at this key yet
{ok, _} = barrel_docdb:put_attachment(<<"mydb">>, <<"doc1">>,
    <<"note.txt">>, <<"first">>, #{create_only => true}),

%% the same call again fails: something is already there
{error, {conflict, CurrentInfo}} = barrel_docdb:put_attachment(<<"mydb">>,
    <<"doc1">>, <<"note.txt">>, <<"second">>, #{create_only => true}).
```

`CurrentInfo` is what's actually stored now (digest, content type, length,
etag), enough to decide whether to retry, surface the conflict, or force an
overwrite by retrying without the option. Neither option is honored on every
store -- see [Limitations](limitations.md) for which ones, and what happens
on a store that can't.

Or over HTTP: `If-None-Match: *` for `create_only`, `If-Match: <etag>` for
`expected_etag`, on the attachment `PUT` route.

```console
$ curl -X PUT http://localhost:8080/db/mydb/doc/doc1/att/note.txt \
    -H 'If-None-Match: *' -d 'first'
# 201 -- nothing was there yet

$ curl -X PUT http://localhost:8080/db/mydb/doc/doc1/att/note.txt \
    -H 'If-None-Match: *' -d 'second'
# 409 {"error":"conflict","current":{...}} -- something already is
```

A store that can't enforce this at all (Garage) fails the request
immediately with `501 conditional_writes_unsupported`, the same way the
Erlang API does -- not a hang, not a silent unprotected write.

## Branching

`barrel_docdb:branch_db/3` (and `POST /db/:db/_timeline/branch` over HTTP)
works against an S3-backed database: the branch gets its own bucket prefix,
independent of its parent from the moment it exists.

```erlang
{ok, _} = barrel_docdb:branch_db(<<"mydb">>, <<"mydb-branch">>, #{}).
```

Forking isn't O(1) the way it is for the default backend, though -- see
[Limitations](limitations.md#branching-cost) for the cost and what a read
looks like for an attachment the background copy hasn't reached yet.
