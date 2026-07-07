/** Sync status surfaced through Database.onStatus. */
export type SyncState = "idle" | "syncing" | "live" | "error" | "offline";

export interface SyncStatus {
  state: SyncState;
  error?: string;
}

/** A local or pulled document change surfaced through Database.onChange. */
export interface DocChange {
  id: string;
  deleted: boolean;
  source: "local" | "remote";
}
