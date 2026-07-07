/**
 * Sync filters, serialized exactly as barrel_rep_filter:from_wire/1
 * expects. Filters apply at the source; a filtered pull keeps its own
 * checkpoint (the filter joins the replication identity).
 */
import type { JsonValue } from "../json.js";

/** Path components: strings or non-negative integers; "*" is a wildcard. */
export type WirePathComponent = string | number;

/** Query conditions are tagged JSON arrays, whitelisted by the server. */
export type WireCond =
  | ["path", WirePathComponent[], JsonValue]
  | ["compare", WirePathComponent[], CompareOp, JsonValue]
  | ["and", WireCond[]]
  | ["or", WireCond[]]
  | ["not", WireCond]
  | ["in", WirePathComponent[], JsonValue[]]
  | ["contains", WirePathComponent[], JsonValue]
  | ["exists", WirePathComponent[]]
  | ["missing", WirePathComponent[]]
  | ["regex", WirePathComponent[], string]
  | ["prefix", WirePathComponent[], string];

export type CompareOp = ">" | "<" | ">=" | "=<" | "==" | "=/=";

export interface SyncFilter {
  /** MQTT-style path patterns (`+` one segment, trailing `#` the rest). */
  paths?: string[];
  /** A channel declared at database creation. */
  channel?: string;
  /** A body query; conditions combine with AND. */
  query?: { where: WireCond[] };
}

/** Drop unset keys so the wire map carries only what was requested. */
export function filterToWire(filter: SyncFilter): Record<string, JsonValue> {
  const wire: Record<string, JsonValue> = {};
  if (filter.paths !== undefined) wire["paths"] = filter.paths;
  if (filter.channel !== undefined) wire["channel"] = filter.channel;
  if (filter.query !== undefined) {
    wire["query"] = { where: filter.query.where as unknown as JsonValue };
  }
  return wire;
}
