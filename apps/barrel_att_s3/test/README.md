# barrel_att_s3 test setup

`barrel_att_s3_SUITE.erl` and `barrel_att_s3_rep_SUITE.erl` run against real
RustFS and Garage servers, not a mock. You need them when you change the S3
attachment backend. A group skips when its store is not configured or
reachable, so the suites are safe to run without either server; set
`BARREL_S3_REQUIRED=1` (as CI does) to make that a failure instead.

## Quick start

```console
$ docker compose -f test/e2e/docker-compose.attachments-s3.yml down -v
$ eval "$(test/e2e/attachments-s3-setup.sh)"
$ BARREL_S3_REQUIRED=1 rebar3 as s3 ct \
    --suite=apps/barrel_att_s3/test/barrel_att_s3_SUITE,apps/barrel_att_s3/test/barrel_att_s3_rep_SUITE
```

Run from the umbrella root. The setup script pulls and starts
`test/e2e/docker-compose.attachments-s3.yml` (RustFS + Garage, pinned tags),
creates the bucket on both, assigns the Garage layout and creates a Garage
key. It prints the connection settings as `export` lines only when every step
succeeded, and exits non-zero otherwise.

It is idempotent for RustFS but not for Garage: Garage never reveals a key's
secret again, so run the `down -v` above before each setup.

## Groups

| Group | Store | Covers |
|-------|-------|--------|
| `rustfs` | RustFS | the shared cases plus `create_only`/`expected_etag` against a store that enforces conditional writes |
| `garage` | Garage | the shared cases plus Garage specifics: small multipart parts, and `create_only`/`expected_etag` failing fast with `conditional_writes_unsupported` |
| `garage_conditional` | Garage behind a meck layer | the conflict cases again, with `If-None-Match`/`If-Match` emulated in `barrel_att_s3_test_support`, so the probe's supported branch does not depend on one server |

`multipart_gc_ignores_list_uploads_prefix` emulates the MinIO
`ListMultipartUploads` prefix bug, which the multipart GC works around.
MinIO itself is not run in the tests.

## Manual setup

If you already run your own servers, point the suites at them.

RustFS:

```console
$ docker run -d -p 19000:9000 \
    -e RUSTFS_ACCESS_KEY=s3testadmin -e RUSTFS_SECRET_KEY=s3testsecret \
    rustfs/rustfs:1.0.0
```

Create the bucket before running the suites (they never create one), for
example with a SigV4-signed `curl -X PUT http://127.0.0.1:19000/barrel-att-s3-test`.

Garage needs a config file (see `test/e2e/garage.toml`), then:

```console
$ docker exec <container> /garage layout assign -z dc1 -c 1G <node-id>
$ docker exec <container> /garage layout apply --version 1
$ docker exec <container> /garage bucket create <bucket>
$ docker exec <container> /garage key create <key-name>
$ docker exec <container> /garage bucket allow <bucket> --key <key-name> --read --write
```

`<node-id>` comes from `docker exec <container> /garage node id -q`.

## Env vars

```
BARREL_S3_REQUIRED          (1: fail instead of skip when a store is unusable)

RUSTFS_S3_TEST_ENDPOINT     (default http://127.0.0.1:19000)
RUSTFS_S3_TEST_ACCESS_KEY   (default s3testadmin)
RUSTFS_S3_TEST_SECRET_KEY   (default s3testsecret)
RUSTFS_S3_TEST_REGION       (default us-east-1)
RUSTFS_S3_TEST_BUCKET       (default barrel-att-s3-test)

GARAGE_S3_TEST_ENDPOINT     (default http://127.0.0.1:13900)
GARAGE_S3_TEST_ACCESS_KEY   (no default: the Garage groups skip without it)
GARAGE_S3_TEST_SECRET_KEY   (no default: the Garage groups skip without it)
GARAGE_S3_TEST_REGION       (default garage)
GARAGE_S3_TEST_BUCKET       (default barrel-att-s3-test)
```
