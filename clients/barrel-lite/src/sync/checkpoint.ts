/**
 * Checkpoint identity. A pull cursor is keyed by the replication
 * target and filter, mirroring the spirit of barrel_rep's rep_id_term
 * (credential-free, filter-sensitive) so that changing the token does
 * not reset progress but changing the filter does.
 *
 * Checkpoints live client-side (in meta.json), so the key just has to
 * be a stable string; a canonical concatenation is enough (no hash).
 */
import type { SyncFilter } from "../wire/filters.js";

export function checkpointKey(
  url: string,
  db: string,
  filter?: SyncFilter,
): string {
  const normalizedUrl = url.replace(/\/+$/, "").toLowerCase();
  return `${normalizedUrl}|${db}|${filter ? canonicalJson(filter) : ""}`;
}

/** JSON with object keys sorted, so equal filters map to equal keys. */
function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map(canonicalJson).join(",")}]`;
  }
  if (value && typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>)
      .filter(([, v]) => v !== undefined)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
    return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${canonicalJson(v)}`).join(",")}}`;
  }
  return JSON.stringify(value) ?? "null";
}
