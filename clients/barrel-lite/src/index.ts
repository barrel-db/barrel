/**
 * barrel-lite: a TypeScript protocol client for the barrel database.
 *
 * Offline-first local store with HLC-stamped mutations, pushed and
 * pulled over the barrel_server sync wire (/db/:db/_sync/*). Not a
 * WASM port of the engine: the client re-implements the wire codecs
 * (HLC, version, version vector) byte-exactly and keeps local data as
 * a cache; sync is the durability story.
 */

export const VERSION = "0.1.0";
