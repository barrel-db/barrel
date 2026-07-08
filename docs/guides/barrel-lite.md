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
await db.sync();                    // push then pull (docs + attachments)

const handle = db.liveSync();               // adaptive polling
const live = db.liveSync({ continuous: true }); // hold an SSE stream
handle.stop();
```

`continuous: true` opens a `GET /db/:db/changes?feed=continuous` stream and
re-pulls on each change, falling back to polling if the stream drops. The
stream needs the server's continuous mode (built into `barrel_server`).

Pull with a filter to hold a subset (the filter joins the sync identity, so it
keeps its own cursor):

```ts
await db.pull({ filter: { query: { where: [["path", ["type"], "note"]] } } });
await db.pull({ filter: { channel: "mobile" } });   // a declared channel
```

## How (query)

Query the synced set locally with BQL; the same text runs on the server. The
local executor matches the server's document subset (SELECT/WHERE/ORDER BY/
LIMIT/OFFSET, paths, UNNEST, IN/LIKE/IS NULL/IS MISSING/BETWEEN/CONTAINS).

```ts
const rows = await db.query(
  "SELECT name, price FROM db WHERE kind = 'fruit' ORDER BY price DESC LIMIT 10",
);
const scoped = await db.query("SELECT * FROM db WHERE org = $org", {
  params: { org: "acme" },
});
```

Vector and keyword search (`vector_top_k`, `bm25_top_k`, `hybrid_top_k`) and
`SUBSCRIBE` need the server; they throw `BqlServerOnlyError` locally. Send
those, or any heavy or global query, to the server:

```ts
const { rows, meta } = await db.queryRemote(
  "SELECT * FROM bm25_top_k('outage', k => 20) AS s",
);
```

## How (vector search)

Pull each document's embedding to the browser and rank the synced set by
cosine against a query vector you supply. Vectors ride a dedicated pass (they
never travel on the document feed), so pull them after a sync or fold them into
`liveSync`.

```ts
await db.syncEmbeddings();          // pull vectors for the docs you hold
db.liveSync({ vectors: true });     // or pull them each live cycle

const hits = await db.searchLocal(queryVector, { k: 10 });
//   [{ id, score, doc }], sorted by descending cosine

const filtered = await db.searchLocal(queryVector, {
  k: 10,
  filter: "kind = 'note'",          // a BQL WHERE narrows candidates first
});
```

`searchLocal` ranks the per-document `emb` vectors you have synced. That is a
different corpus from the server's ANN index, which is bound to the embedding
model's dimension; reach it with `db.searchVector` / `db.searchText`:

```ts
const vhits = await db.searchVector(queryVector, { k: 10 }); // server ANN
const thits = await db.searchText("outage", { k: 10, mode: "bm25" });
```

No model is bundled. To embed text in the browser, load a model yourself (for
example transformers.js) and pass the `Float32Array` to `searchLocal`; its
dimension must match the stored vectors. See `examples/vector-search.html`.

## How (attachments)

Attachments are content-addressed blobs synced on their own feed, correlated
to a document by name.

```ts
await db.putAttachment("doc1", "photo.jpg", bytes, { contentType: "image/jpeg" });
const got = await db.getAttachment("doc1", "photo.jpg"); // { bytes, info }
const info = await db.getAttachmentInfo("doc1", "photo.jpg"); // digest/length/type
await db.removeAttachment("doc1", "photo.jpg");
await db.gcAttachments(); // reclaim unreferenced blobs
```

Blobs stream over the `_sync/att*` endpoints, deduplicate by SHA-256 digest,
and resolve last-write-wins on an origin timestamp (no version vectors). A
server without an attachment feed degrades the phase to skipped.

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
  when idle; a local write wakes it). Polling is the floor; `continuous: true`
  adds a fetch-based SSE stream (which carries the bearer) that wakes the poller
  and degrades back to polling on drop.
- The client mints and persists its own 16-hex source id. Do not clear browser
  storage expecting a clean slate mid-sync: a new source id changes the
  authorship of future writes.
- Local BQL matches the server's document subset, with two documented
  divergences: JSON collapses `1` and `1.0` (the CBOR-backed server keeps them
  distinct), and ORDER BY across mixed types follows a fixed total order
  (number, then boolean/null, then object, then array, then string).
- Vector search runs in the browser: `searchLocal` ranks the synced per-doc
  vectors by brute-force cosine (pull them with `syncEmbeddings`). Text and ANN
  queries delegate to the server via `searchText` / `searchVector`. See the
  "How (vector search)" section above.
- Live sync polls by default; `continuous: true` holds one SSE stream per
  database (leader-only) and consumes one of the origin's HTTP connections.
