# barrel-lite (browser client)

`barrel-lite` is a TypeScript client that keeps an offline-first local
document store in the browser and syncs it to a `barrel_server` over the
`/db/:db/_sync/*` wire. It is a protocol client, not a WASM port: it
re-implements the HLC, version, and version-vector codecs byte-for-byte,
stamps local mutations with its own source id, and treats local data as a
cache (Safari evicts it) with sync as the durability story. Read this when you
want a web app to read and write a barrel database offline and converge with
the server. The package lives in the umbrella at `clients/barrel-lite`.

## When to use it

- A web app needs local reads and writes that keep working offline.
- You want changes to converge with the server (and other tabs) automatically.
- You are past a single tab: one writer per origin, the rest follow.

## Server setup

The browser needs two things the server does not enable by default: CORS, and
a token it can actually ship. Configure both in the `barrel_server` app env.

```erlang
{barrel_server, [
    {cors, #{origins => '*',              %% or a list of origins
             expose => [<<"x-barrel-hlc">>]}},   %% default; needed for the clock
    {auth, #{tokens => [<<"global-token">>]}}
]}
```

A browser cannot ship the global token. Issue a per-space capability token
instead (see [spaces](spaces.md)); it authenticates only that space's `/db`
and `/handoffs` routes, with `read` covering the pull leg and `write` the push
leg.

```console
$ curl -XPOST host:8080/spaces -H 'authorization: Bearer global-token' \
    -H 'content-type: application/json' -d '{"label":"app"}'
# {"space":"sp_...", ...}
$ curl -XPOST host:8080/spaces/sp_.../grants -H 'authorization: Bearer global-token' \
    -H 'content-type: application/json' -d '{"rights":["read","write"]}'
# {"token":"bsp_...", ...}
```

## How (open, read, write)

```ts
import { Database } from "barrel-lite";

const db = await Database.open("notes", {
  remote: { url: "https://edge.example:8080", db: "sp_...", token: "bsp_..." },
  multiTab: true,
});

await db.put({ id: "n1", title: "hello" });
const doc = await db.get("n1");     // { id: "n1", title: "hello" }
await db.remove("n1");
```

`db` uses OPFS storage and the space id as the database name. Writes are local
and immediate; they reach the server on the next push.

## How (sync)

```ts
await db.push();                    // send local writes
await db.pull();                    // apply server changes
await db.sync();                    // push then pull, one shot

const handle = db.liveSync();       // keep converging (adaptive polling)
handle.stop();
```

Pull with a filter to hold a subset (the filter joins the sync identity, so it
keeps its own cursor):

```ts
await db.pull({ filter: { query: { where: [["path", ["type"], "note"]] } } });
await db.pull({ filter: { channel: "mobile" } });   // a declared channel
```

## How (events)

```ts
db.onChange((c) => render(c.id, c.deleted, c.source));   // local | remote
db.onStatus((s) => setBadge(s.state));   // idle | syncing | live | error
```

Both fire on every tab, whether that tab is the leader or a follower, so UI
code is identical on either side.

## How (conflicts)

Concurrent edits resolve by last-write-wins on the HLC, the same rule the
server uses, so every replica converges on the same winner. To be told when a
local edit lost:

```ts
const db = await Database.open("notes", {
  remote,
  onConflict: (e) => console.warn("dropped local edit", e.id, "->", e.winner),
});
```

The losing body is not kept locally (the server retains it as a conflict
sibling); the winner arrives on the next pull.

## Notes

- Storage is a cache. Anything not yet flushed is lost on a crash, and the
  browser may evict the whole store; the server copy is the source of truth.
  A flush always precedes a push, so anything the server was told is also on
  disk locally.
- Multi-tab: one tab holds a Web Lock and owns the store and sync; other tabs
  proxy reads and writes to it over BroadcastChannel and receive change and
  status events. When the leader tab closes, the next tab is promoted and
  reloads the store.
- The client keeps its checkpoints itself, so a pull-only deployment needs
  only a `read` grant. It never writes server-side checkpoints.
- Live sync polls `_sync/changes` (500 ms after activity, backing off to 30 s
  when idle; a local write wakes it). The plain SSE changes feed is one-shot
  and cannot carry a bearer, so polling is the browser story for now.
- The client mints and persists its own 16-hex source id. Do not clear browser
  storage expecting a clean slate mid-sync: a new source id changes the
  authorship of future writes.
- Attachments, a local BQL subset, and vector search are later phases; the
  transport reserves the endpoint names.
