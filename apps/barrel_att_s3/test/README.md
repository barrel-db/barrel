# barrel_att_s3 test setup

`barrel_att_s3_SUITE.erl` runs against real MinIO and Garage -- there is no
mock. Each group skips cleanly if its store isn't reachable, so the suite is
safe to run without either running.

## Quick start

```console
$ eval "$(../../test/e2e/attachments-s3-setup.sh)"
$ rebar3 as s3 ct --suite apps/barrel_att_s3/test/barrel_att_s3_SUITE
```

(paths above assume you're in `apps/barrel_att_s3/`; from the umbrella root
use `test/e2e/attachments-s3-setup.sh` instead.)

The setup script starts `test/e2e/docker-compose.attachments-s3.yml` (MinIO +
Garage) and provisions both -- neither the suite nor
`barrel_att_s3_store:open/2` create a bucket on their own, and Garage
additionally needs a one-time layout assignment before it serves any S3
request. It prints the Garage credentials it created as `export` lines; the
`eval "$(...)"` above loads them into your shell so the `garage` group runs
instead of skipping.

Idempotent for MinIO. Not idempotent for Garage past the first run -- Garage
never reveals a key's secret again after creation. Run `docker compose -f
../../test/e2e/docker-compose.attachments-s3.yml down -v` first to start
clean.

## Manual setup

If you'd rather not use the script, or already have your own MinIO/Garage:

```console
$ docker run -d -p 19000:9000 -p 19001:9001 \
    -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
    minio/minio server /data --console-address :9001
```

MinIO needs a bucket created before the suite runs (it does not create one
for you): `barrel-att-s3-test`, matching `MINIO_S3_TEST_BUCKET`'s default
below.

Garage needs a config file (see `test/e2e/garage.toml` for a working
example), then, once the container is up:

```console
$ docker exec <container> /garage layout assign -z dc1 -c 1G <node-id>
$ docker exec <container> /garage layout apply --version 1
$ docker exec <container> /garage bucket create <bucket>
$ docker exec <container> /garage key create <key-name>
$ docker exec <container> /garage bucket allow <bucket> --key <key-name> --read --write
```

`<node-id>` comes from `docker exec <container> /garage node id -q`.

## Env vars

All optional for MinIO (defaults match the quick-start setup above);
`GARAGE_S3_TEST_ACCESS_KEY`/`_SECRET_KEY` have no default -- the `garage`
group skips without them.

```
MINIO_S3_TEST_ENDPOINT    (default http://127.0.0.1:19000)
MINIO_S3_TEST_ACCESS_KEY  (default minioadmin)
MINIO_S3_TEST_SECRET_KEY  (default minioadmin)
MINIO_S3_TEST_REGION      (default us-east-1)
MINIO_S3_TEST_BUCKET      (default barrel-att-s3-test)

GARAGE_S3_TEST_ENDPOINT    (default http://127.0.0.1:13900)
GARAGE_S3_TEST_ACCESS_KEY  (no default -- group skips without it)
GARAGE_S3_TEST_SECRET_KEY  (no default -- group skips without it)
GARAGE_S3_TEST_REGION      (default garage)
GARAGE_S3_TEST_BUCKET      (default barrel-test)
```
