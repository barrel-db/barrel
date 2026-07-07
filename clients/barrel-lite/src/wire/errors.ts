/**
 * Sync error taxonomy. Every failure the transport surfaces carries a
 * stable `code` so callers (the sync engine, status reporting) can
 * branch without string-matching messages.
 */
export type SyncErrorCode =
  | "network" // fetch itself threw (offline, DNS, TLS)
  | "unauthorized" // 401
  | "forbidden" // 403
  | "not_found" // 404
  | "clock_skew" // 409 on _sync/hlc
  | "bad_request" // other 4xx with an error body
  | "server_error"; // 5xx

export class SyncError extends Error {
  readonly code: SyncErrorCode;
  readonly status?: number;

  constructor(code: SyncErrorCode, message: string, status?: number) {
    super(message);
    this.name = "SyncError";
    this.code = code;
    this.status = status;
  }
}
