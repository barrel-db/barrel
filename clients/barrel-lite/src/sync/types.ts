/** Shared sync-engine types. */
import type { DocRecord } from "../store/types.js";

/** Fired when a concurrent conflict drops a local change on pull/push. */
export interface ConflictEvent {
  id: string;
  /** The local record that lost. */
  losing: DocRecord;
  /** The winning version token (its body arrives on the next pull). */
  winner: string;
}

export type OnConflict = (event: ConflictEvent) => void;

export interface PullStats {
  scanned: number;
  applied: number;
}

export interface PushStats {
  pushed: number;
  skipped: number;
  conflicts: number;
}
