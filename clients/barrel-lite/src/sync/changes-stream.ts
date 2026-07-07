/**
 * A continuous changes stream over GET /db/:db/changes?feed=continuous.
 * Uses fetch + a ReadableStream reader (EventSource cannot carry a
 * bearer; fetch can), parses the SSE frames, and calls onChange per
 * change while tracking the highest hlc cursor for resume. It is a wake
 * signal for live sync, not a replacement for the pull loop: the caller
 * re-pulls on each change and can reopen the stream with cursor() after
 * a drop.
 */
import { base64Decode } from "../codec/base64.js";
import { compareHlc, decodeHlc } from "../codec/hlc.js";
import type { JsonObject } from "../json.js";
import type { FetchLike } from "../wire/transport.js";
import { SseParser } from "./sse.js";

export interface ChangeRow {
  id: string;
  hlc: string; // urlsafe-base64 cursor
  rev: string;
  deleted: boolean;
}

export interface ChangesStreamOptions {
  url: string;
  db: string;
  token?: string;
  since?: string;
  fetch?: FetchLike;
  onChange: (change: ChangeRow) => void;
  onError: (err: Error) => void;
  onOpen?: () => void;
}

export interface ChangesStreamHandle {
  close(): void;
  /** The highest hlc seen so far (the resume cursor). */
  cursor(): string | undefined;
}

export function openChangesStream(opts: ChangesStreamOptions): ChangesStreamHandle {
  const controller = new AbortController();
  const fetchImpl = opts.fetch ?? ((i: string, init?: RequestInit) => fetch(i, init));
  const root = opts.url.replace(/\/+$/, "");
  let url = `${root}/db/${encodeURIComponent(opts.db)}/changes?feed=continuous`;
  if (opts.since && opts.since !== "first") {
    url += `&since=${encodeURIComponent(opts.since)}`;
  }
  let cursor: string | undefined = opts.since;
  let closed = false;

  const headers: Record<string, string> = { accept: "text/event-stream" };
  if (opts.token !== undefined) headers["authorization"] = `Bearer ${opts.token}`;

  void (async () => {
    let resp: Response;
    try {
      resp = await fetchImpl(url, { method: "GET", headers, signal: controller.signal });
    } catch (e) {
      if (!closed) opts.onError(asError(e));
      return;
    }
    if (!resp.ok) {
      opts.onError(new Error(`changes stream http ${resp.status}`));
      return;
    }
    if (!resp.body) {
      opts.onError(new Error("changes stream has no body"));
      return;
    }
    opts.onOpen?.();
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    const parser = new SseParser();
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        const chunk = decoder.decode(value, { stream: true });
        for (const event of parser.push(chunk)) {
          if (event.event === "ping") continue;
          if (event.event === "error") {
            opts.onError(new Error(errorText(event.data)));
            return;
          }
          if (event.data.length === 0) continue;
          const change = toChange(event.data);
          if (!change) continue;
          cursor = higher(cursor, change.hlc);
          opts.onChange(change);
        }
      }
      if (!closed) opts.onError(new Error("changes stream closed"));
    } catch (e) {
      if (!closed) opts.onError(asError(e));
    }
  })();

  return {
    close(): void {
      closed = true;
      controller.abort();
    },
    cursor(): string | undefined {
      return cursor;
    },
  };
}

function toChange(data: string): ChangeRow | undefined {
  let obj: JsonObject;
  try {
    obj = JSON.parse(data) as JsonObject;
  } catch {
    return undefined;
  }
  if (typeof obj["id"] !== "string" || typeof obj["hlc"] !== "string") {
    return undefined;
  }
  return {
    id: obj["id"],
    hlc: obj["hlc"],
    rev: typeof obj["rev"] === "string" ? obj["rev"] : "",
    deleted: obj["deleted"] === true,
  };
}

function higher(a: string | undefined, b: string): string {
  if (!a || a === "first") return b;
  try {
    return compareHlc(decodeHlc(base64Decode(b)), decodeHlc(base64Decode(a))) > 0 ? b : a;
  } catch {
    return b;
  }
}

function errorText(data: string): string {
  try {
    const obj = JSON.parse(data) as JsonObject;
    return typeof obj["error"] === "string" ? obj["error"] : data;
  } catch {
    return data;
  }
}

function asError(e: unknown): Error {
  return e instanceof Error ? e : new Error(String(e));
}
