# Embed the barrel server in a livery app

`barrel_server_api` exposes barrel's REST/sync routes as a plain `livery` route
list so another Erlang application can serve them from its **own** livery service,
optionally under a sub-path, with its own authentication. Read this when you want
`barrel-lite` (or any HTTP client) to reach barrel through a host app's endpoint
instead of running the standalone `barrel_server` listener.

## When to use it

- You already run a `livery` service and want to add barrel's `/db` + `/_sync`
  surface to it, e.g. under `/barrel`.
- You want the host app to own auth/CORS rather than barrel's built-in bearer/CORS.
- For a turnkey standalone HTTP server instead, run `barrel_server` directly (see
  [rest-server](rest-server.md)).

## How

Start `barrel` and `barrel_spaces`, then mount barrel's router into yours. The DB
surface needs no `barrel_server` supervisor — the handlers are stateless.

```erlang
%% 1. Point barrel at a data dir and start it (not barrel_server).
application:set_env(barrel_docdb, data_dir, "/var/lib/myapp/barrel"),
{ok, _} = application:ensure_all_started(barrel),
{ok, _} = application:ensure_all_started(barrel_spaces),

%% 2. Mount barrel's default DB surface under /barrel, into your router.
BarrelRouter = barrel_server_api:router(),
Router = livery_router:nest(<<"/barrel">>, BarrelRouter, MyHostRouter),

%% 3. Serve it. Your middleware owns auth; barrel ships none here.
{ok, _} = livery:start_service(#{
    http       => #{port => 8080},
    router     => Router,
    middleware => MyAuthStack
}).
```

`barrel-lite` then points at the sub-path — no client change:

```ts
const db = openBarrel({ url: "https://host/barrel", db: "mydb" });
```

## Selecting routes

`routes/0` (and `router/0`) return the default DB surface: the `db`, `sync`,
`timeline`, and `search` groups — what barrel-lite drives. Pass `groups` to widen
or narrow it; `mcp` and the `spaces`/`handoffs` agent layer are opt-in.

```erlang
barrel_server_api:routes().                              %% DB surface (default)
barrel_server_api:routes(#{groups => [db, sync]}).       %% just docs + sync wire
barrel_server_api:routes(#{groups => all}).              %% everything
barrel_server_api:router(#{prefix => <<"/barrel">>}).    %% pre-nested router
```

Groups: `meta` (`/`, `/health`), `db`, `sync`, `timeline`, `search`, `spaces`,
`mcp`. Unknown names raise `{unknown_route_group, Name}`.

## Notes

- The host owns auth. `barrel_server_api` returns routes only; wrap them with your
  own middleware (`livery_router:layer/2` over the barrel subtree, or a
  service-wide stack). barrel's built-in bearer/CORS live in the standalone
  `barrel_server` service and are not applied here.
- Set `barrel_docdb`'s `data_dir` yourself (the `barrel_server` app's config
  mapping does not run when you don't start that app). Server-wide open options
  come from `{barrel_server, open_opts, Map}` if set.
- The `mcp` group needs the MCP registrar and live-query bridge running; start the
  `barrel_server` application with the HTTP listener disabled for those, and note
  that `/mcp` under a sub-path is not yet verified. The DB surface needs neither.
- Route paths, request/response shapes, and the changes/SSE behavior are identical
  to the standalone server — see [rest-server](rest-server.md).
