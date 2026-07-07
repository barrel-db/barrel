import { describe, expect, it } from "vitest";
import { SseParser } from "../src/sync/sse.js";

describe("SSE parser", () => {
  it("parses a simple data frame", () => {
    const p = new SseParser();
    expect(p.push("data: hello\n\n")).toEqual([{ event: "message", data: "hello" }]);
  });

  it("parses event, data, and id fields", () => {
    const p = new SseParser();
    expect(p.push("event: change\ndata: {\"id\":\"a\"}\nid: 7\n\n")).toEqual([
      { event: "change", data: '{"id":"a"}', id: "7" },
    ]);
  });

  it("joins multiple data lines with newline", () => {
    const p = new SseParser();
    expect(p.push("data: line1\ndata: line2\n\n")).toEqual([
      { event: "message", data: "line1\nline2" },
    ]);
  });

  it("buffers a frame split across chunks", () => {
    const p = new SseParser();
    expect(p.push("data: par")).toEqual([]);
    expect(p.push("tial\n")).toEqual([]);
    expect(p.push("\n")).toEqual([{ event: "message", data: "partial" }]);
  });

  it("emits multiple frames from one chunk", () => {
    const p = new SseParser();
    expect(p.push("data: a\n\ndata: b\n\n")).toEqual([
      { event: "message", data: "a" },
      { event: "message", data: "b" },
    ]);
  });

  it("handles a ping event and CRLF line endings", () => {
    const p = new SseParser();
    expect(p.push("event: ping\r\ndata: {}\r\n\r\n")).toEqual([
      { event: "ping", data: "{}" },
    ]);
  });

  it("ignores comment lines and strips one leading space", () => {
    const p = new SseParser();
    expect(p.push(": keep-alive comment\ndata:nospace\n\n")).toEqual([
      { event: "message", data: "nospace" },
    ]);
  });

  it("skips an empty frame", () => {
    const p = new SseParser();
    expect(p.push("\n\ndata: real\n\n")).toEqual([
      { event: "message", data: "real" },
    ]);
  });
});
