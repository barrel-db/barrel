# barrel-lite

A TypeScript protocol client for [barrel](../../). It keeps an offline-first
local document store and syncs it over the barrel_server wire
(`/db/:db/_sync/*`). It is not a WASM port of the engine: it re-implements the
wire codecs (HLC, version, version vector) byte-for-byte and treats local data
as a cache, with sync as the durability story.

## Install

Not published yet. In this repo:

```console
$ cd clients/barrel-lite
$ npm install
$ npm run build
```

## Use

```ts
import { Database } from "barrel-lite";

const db = await Database.open("notes", {
  remote: { url: "https://edge.example:8080", db: "notes", token: "bsp_..." },
  multiTab: true,
});

await db.put({ id: "n1", title: "hello" });
const doc = await db.get("n1");

// query the synced set locally (same BQL text runs on the server)
const rows = await db.query("SELECT title FROM notes WHERE pinned = true");

// attachments (content-addressed blobs on their own feed)
await db.putAttachment("n1", "cover.png", bytes, { contentType: "image/png" });

db.onChange((c) => console.log("changed", c.id, c.source));
db.liveSync({ continuous: true }); // SSE stream, falls back to polling

await db.sync(); // push then pull (documents + attachments)
```

In the browser, storage defaults to OPFS and `multiTab` uses Web Locks plus
BroadcastChannel (one tab per origin owns the store; others proxy to it). In
Node and tests, storage defaults to memory and there is a single instance.

`remote.url` is the base the client builds every route from, so it may include a
sub-path when the server is embedded under one (for example
`url: "https://host/barrel"` when barrel is mounted at `/barrel` in a host livery
app; see the [embedding guide](../../docs/guides/embedding-barrel-server.md)).

## Vector search

Pull each document's embedding to the browser and run brute-force cosine
top-k over the synced set. Use this when you already have a query vector
(precomputed, or from a model you load yourself) and want ranking without a
round trip.

```ts
// pull per-doc vectors for the docs you hold (a dedicated pass; vectors
// never ride the doc feed). Call it after a sync, or fold it into liveSync.
await db.syncEmbeddings();
db.liveSync({ vectors: true }); // pull vectors each cycle too

// rank the synced set by cosine against a query vector
const hits = await db.searchLocal(queryVector, { k: 10 });
//   [{ id, score, doc }], sorted by descending cosine

// narrow candidates first with a BQL WHERE filter
const recent = await db.searchLocal(queryVector, {
  k: 10,
  filter: "kind = 'note' AND pinned = true",
});
```

The vectors come from each document's per-document `emb` column, pulled
losslessly as float32. This is a different corpus from the server's ANN index
(`db.searchVector` / `db.searchText` delegate to the server `/search`
endpoints): the ANN store is bound to the embedding model's dimension and
rebuilds server-side, while `searchLocal` ranks exactly the vectors you have
synced. For text queries without a local vector, delegate to the server:

```ts
const vhits = await db.searchVector(queryVector, { k: 10 }); // server ANN
const thits = await db.searchText("quarterly revenue", { k: 10 }); // bm25
```

No model is bundled. To embed text in the browser, load a model yourself (for
example transformers.js) and pass the resulting `Float32Array` to
`searchLocal`; the dimension must match the stored vectors. See
`examples/vector-search.html`.

## Test

```console
$ npm test               # unit (codecs, store, sync, tabs) against fixtures
$ npm run test:integration   # boots a real barrel_server, needs rebar3 + Erlang
$ npm run test:browser       # Playwright chromium (OPFS, multi-tab, live)
```

## Regenerating the codec fixtures

The unit tests assert the TypeScript codecs against golden vectors generated
from the Erlang implementation. Regenerate after changing `barrel_hlc`,
`barrel_version`, or `barrel_vv`:

```console
$ cd ../..                    # umbrella root
$ rebar3 compile
$ ./clients/barrel-lite/scripts/gen_fixtures.escript \
    > clients/barrel-lite/test/fixtures/golden.json
```

The escript writes only to stdout; redirect it yourself. Never hand-edit
`golden.json`.

## Scope

Document sync, local store, leader election, live sync (polling and continuous
SSE), a local BQL subset matching the server (with `queryRemote` delegation for
table functions), attachment sync, and browser vector search (embedding pull
plus brute-force cosine top-k, with server `/search` delegation). No ANN index
runs in the browser; the server owns that.
