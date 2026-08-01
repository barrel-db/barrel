# barrel_att_s3

S3-compatible attachment storage backend for `barrel_docdb`. Stores
attachment bytes in an S3 bucket (AWS S3, MinIO, Garage) instead of the
embedded RocksDB instance `barrel_docdb` uses by default.

[Documentation](https://barrel-db.eu/docs/lib/att-s3/) |
[HexDocs](https://hexdocs.pm/barrel_att_s3) |
[Repository](https://github.com/barrel-db/barrel)

Kept out of the default (embeddable) `barrel_docdb` build -- it pulls
`livery_s3` (and `livery`), so it opts in via its own `s3` rebar3 profile.

## Use

```erlang
{ok, _} = barrel_docdb:create_db(<<"mydb">>, #{
    att_opts => #{
        backend => s3,
        s3 => #{
            bucket => <<"my-bucket">>,
            endpoint => <<"https://s3.eu-west-1.amazonaws.com">>,
            region => <<"eu-west-1">>,
            access_key_id => <<"...">>,
            secret_access_key => <<"...">>
        }
    }
}),

{ok, _Info} = barrel_docdb:put_attachment(<<"mydb">>, <<"doc1">>,
    <<"greeting.txt">>, <<"Hello, World!">>).
```

Everything else -- `get_attachment/4`, `delete_attachment/4`, streaming --
works exactly as it does with the default backend; the backend choice is
entirely in `att_opts`.

## Documentation

- [Getting started](docs/getting-started.md) -- config shape, credentials, `part_size`.
- [Limitations](docs/limitations.md) -- what M1 doesn't do yet, and the store-compatibility table.
