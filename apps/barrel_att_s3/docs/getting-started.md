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

There is no HTTP surface for this yet: `barrel_server`'s `PUT /db/:name`
route only reads the database name from the URL and does not parse a
request body, so `att_opts` (and everything below) is Erlang-API-only for
now. Create S3-backed databases through `barrel_docdb:create_db/2` directly
(a release console, `barrel_server eval`, or your own supervision code).

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

Like `att_opts`, this is Erlang-API-only today: there's no
`If-Match`/`If-None-Match` HTTP header wiring on the attachment routes yet.
