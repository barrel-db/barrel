/**
 * A minimal text/event-stream frame parser. Feed it decoded string
 * chunks as they arrive from a stream reader; it buffers across reads,
 * splits on the blank-line frame boundary, and yields one SseEvent per
 * frame with its `event`, joined `data`, and `id` fields.
 *
 * Only the fields barrel-lite needs are parsed (event, data, id);
 * comment lines (starting ":") and unknown fields are ignored, per the
 * SSE spec's line grammar.
 */
export interface SseEvent {
  event: string; // defaults to "message" when no event: line
  data: string; // data: lines joined with "\n"
  id?: string;
}

export class SseParser {
  private buffer = "";

  /** Feed a chunk; returns the frames completed by it (may be empty). */
  push(chunk: string): SseEvent[] {
    this.buffer += chunk;
    const events: SseEvent[] = [];
    // Frames are separated by a blank line. Normalize CRLF to LF first.
    this.buffer = this.buffer.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
    let idx: number;
    while ((idx = this.buffer.indexOf("\n\n")) !== -1) {
      const frame = this.buffer.slice(0, idx);
      this.buffer = this.buffer.slice(idx + 2);
      const event = parseFrame(frame);
      if (event) events.push(event);
    }
    return events;
  }
}

function parseFrame(frame: string): SseEvent | undefined {
  let event = "message";
  let id: string | undefined;
  const dataLines: string[] = [];
  let sawField = false;
  for (const rawLine of frame.split("\n")) {
    if (rawLine === "" || rawLine.startsWith(":")) continue; // blank / comment
    const colon = rawLine.indexOf(":");
    const field = colon === -1 ? rawLine : rawLine.slice(0, colon);
    // one optional leading space after the colon is stripped
    let value = colon === -1 ? "" : rawLine.slice(colon + 1);
    if (value.startsWith(" ")) value = value.slice(1);
    switch (field) {
      case "event":
        event = value;
        sawField = true;
        break;
      case "data":
        dataLines.push(value);
        sawField = true;
        break;
      case "id":
        id = value;
        sawField = true;
        break;
      default:
        break; // ignore other fields (e.g. retry)
    }
  }
  if (!sawField) return undefined;
  const result: SseEvent = { event, data: dataLines.join("\n") };
  if (id !== undefined) result.id = id;
  return result;
}
