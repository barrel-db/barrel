# Run barrel in Docker

The `barrel-server` image runs `barrel_server`, the barrel HTTP node, with the filesystem and S3 attachment backends. Use it when you want a barrel node on a server or in a cluster without installing Erlang. You configure it with environment variables, or you mount your own `sys.config` when you need more.

## Start a node

```console
$ docker run -d --name barrel -p 8080:8080 \
    -v barrel-data:/data \
    -e BARREL_AUTH_TOKENS=change-me \
    ghcr.io/barrel-db/barrel-server:latest
$ curl localhost:8080/ready
{"status":"ready"}
```

Create a database and write a document:

```console
$ curl -XPUT localhost:8080/db/notes -H 'authorization: Bearer change-me'
$ curl -XPUT localhost:8080/db/notes/doc/hello -H 'authorization: Bearer change-me' \
    -H 'content-type: application/json' -d '{"text": "hello"}'
```

The [REST server guide](rest-server.md) lists every route.

## Settings

| Variable | Default | What it sets |
|---|---|---|
| `BARREL_HTTP_PORT` | `8080` | HTTP port inside the container |
| `BARREL_DATA_DIR` | `/data` | Where databases and indexes live; keep it on a volume |
| `BARREL_AUTH_TOKENS` | none | Comma-separated bearer tokens |
| `BARREL_AUTH_TOKENS_FILE` | none | A file with one token per line, for Docker or Kubernetes secrets |
| `BARREL_LOG_LEVEL` | `info` | `debug`, `info`, `notice`, `warning` or `error` |
| `BARREL_NODE_NAME` | `barrel@127.0.0.1` | Erlang node name |
| `BARREL_COOKIE` | random | Erlang distribution cookie |

Without a token, the server accepts every request and logs a warning at start. Set at least one token on any node others can reach. `/health` and `/ready` never need a token.

## Keep tokens in a secret

```yaml
services:
  barrel:
    image: ghcr.io/barrel-db/barrel-server:latest
    ports: ["8080:8080"]
    volumes: ["barrel-data:/data"]
    environment:
      BARREL_AUTH_TOKENS_FILE: /run/secrets/barrel_tokens
    secrets: [barrel_tokens]
secrets:
  barrel_tokens:
    file: ./tokens.txt
volumes:
  barrel-data:
```

## Use your own configuration

For TLS, HTTP/2 and HTTP/3 listeners, signed requests, CORS, MCP or S3 attachments, write a `sys.config` and mount it at `/etc/barrel/sys.config`. The environment variables above are then ignored. Mount `/etc/barrel/vm.args` the same way to set Erlang flags.

```erlang
[
 {barrel_server, [
   {data_dir, "/data/docdb"},
   {auth, #{tokens => [<<"change-me">>]}},
   {listeners, #{https => #{port => 8443}}},
   {tls, #{certfile => "/etc/barrel/cert.pem", keyfile => "/etc/barrel/key.pem"}}
 ]},
 {barrel_docdb, [{data_dir, "/data/docdb"}]},
 {barrel_ngram, [{data_dir, "/data/ngram"}]}
].
```

```console
$ docker run -d -p 8443:8443 -v barrel-data:/data \
    -v $PWD/barrel:/etc/barrel:ro \
    ghcr.io/barrel-db/barrel-server:latest
```

The [synchronization guide](synchronization.md) describes the listener and TLS keys.

## Open a remote shell

```console
$ docker exec -it barrel barrel remote_console
```

## Build the image yourself

From a checkout of the repository:

```console
$ docker build -t barrel-server .
```

## Notes

- The container runs as user `barrel` (uid 1000). A bind-mounted data directory must be writable by that uid.
- The health check calls `/ready` on `BARREL_HTTP_PORT`, so `docker ps` shows the node as healthy once the database and replication processes run. If your own config serves HTTP on another port, set `BARREL_HTTP_PORT` to it.
- Images are published for `linux/amd64` and `linux/arm64` with each Barrel bundle, tagged with the bundle (`b<N>`) and `latest`. Pin a bundle tag in production.
