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

db.onChange((c) => console.log("changed", c.id, c.source));
db.liveSync(); // adaptive polling; local writes push on a short debounce

await db.sync(); // or one-shot: push then pull
```

In the browser, storage defaults to OPFS and `multiTab` uses Web Locks plus
BroadcastChannel (one tab per origin owns the store; others proxy to it). In
Node and tests, storage defaults to memory and there is a single instance.

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

This is phase 9a: document sync, local store, leader election, live pull.
Attachments, a local BQL subset, and vector search land in later phases.
