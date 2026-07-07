/**
 * Leader election over the Web Locks API. One tab per origin holds an
 * exclusive lock and does all store and sync work; the browser
 * releases the lock when that tab dies, and the next waiter is
 * promoted. The LockManager is injected so the election runs under
 * vitest with a double.
 */

/** The slice of the Web Locks API this needs (navigator.locks). */
export interface LockManager {
  request(
    name: string,
    options: { mode: "exclusive"; signal?: AbortSignal },
    callback: () => Promise<void>,
  ): Promise<void>;
}

export class LeaderLock {
  private leader = false;
  private releaseHeld: (() => void) | undefined;
  private readonly abort = new AbortController();

  constructor(
    private readonly locks: LockManager,
    private readonly lockName: string,
  ) {}

  get isLeader(): boolean {
    return this.leader;
  }

  /**
   * Queue for the lock. onPromote runs when this tab wins it; the lock
   * is then held until resign()/stop(). Resolves when the campaign
   * ends (won-then-released, or aborted before winning).
   */
  campaign(onPromote: () => void): Promise<void> {
    return this.locks
      .request(
        this.lockName,
        { mode: "exclusive", signal: this.abort.signal },
        () => {
          this.leader = true;
          onPromote();
          return new Promise<void>((resolve) => {
            this.releaseHeld = resolve;
          });
        },
      )
      .catch(() => {
        // AbortError before the lock was granted: not the leader
      });
  }

  /** Step down, releasing the lock so the next waiter is promoted. */
  resign(): void {
    this.leader = false;
    if (this.releaseHeld) {
      const release = this.releaseHeld;
      this.releaseHeld = undefined;
      release();
    }
  }

  /** Resign and stop waiting if still queued. */
  stop(): void {
    this.resign();
    this.abort.abort();
  }
}
