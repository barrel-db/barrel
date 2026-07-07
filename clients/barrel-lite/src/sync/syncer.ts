/**
 * Live sync by adaptive polling. The plain SSE changes feed is
 * one-shot and EventSource cannot carry a bearer, so live mode polls
 * _sync/changes on an interval that shrinks to minInterval after a
 * change and backs off ×1.5 to maxInterval while idle. A local
 * mutation wakes the loop on a short debounce. Network errors use a
 * separate 1s→60s backoff and surface through onStatus.
 *
 * The scheduler is a WakeSource seam: a future continuous-SSE enabler
 * can drive tick() from a stream instead of a timer, additively.
 */
import type { SyncStatus } from "./status.js";

export interface LiveTuning {
  minIntervalMs?: number;
  maxIntervalMs?: number;
  errorMinMs?: number;
  errorMaxMs?: number;
  wakeDebounceMs?: number;
}

interface ResolvedTuning {
  minIntervalMs: number;
  maxIntervalMs: number;
  errorMinMs: number;
  errorMaxMs: number;
  wakeDebounceMs: number;
}

const DEFAULTS: ResolvedTuning = {
  minIntervalMs: 500,
  maxIntervalMs: 30_000,
  errorMinMs: 1_000,
  errorMaxMs: 60_000,
  wakeDebounceMs: 50,
};

export interface LiveHandle {
  stop(): void;
}

export class LiveSync implements LiveHandle {
  private handle: ReturnType<typeof setTimeout> | undefined;
  private readonly tuning: ResolvedTuning;
  private interval: number;
  private errorDelay: number;
  private stopped = false;
  private running = false;

  constructor(
    private readonly runOnce: () => Promise<{ changed: boolean }>,
    tuning: LiveTuning,
    private readonly onStatus: (s: SyncStatus) => void,
  ) {
    this.tuning = { ...DEFAULTS, ...tuning };
    this.interval = this.tuning.minIntervalMs;
    this.errorDelay = this.tuning.errorMinMs;
  }

  start(): void {
    this.schedule(0);
  }

  /** A local mutation happened: sync soon (debounced). */
  wake(): void {
    if (this.stopped) return;
    this.reschedule(this.tuning.wakeDebounceMs);
  }

  stop(): void {
    this.stopped = true;
    if (this.handle !== undefined) {
      clearTimeout(this.handle);
      this.handle = undefined;
    }
  }

  private reschedule(delay: number): void {
    if (this.handle !== undefined) clearTimeout(this.handle);
    this.schedule(delay);
  }

  private schedule(delay: number): void {
    this.handle = setTimeout(() => {
      void this.tick();
    }, delay);
  }

  private async tick(): Promise<void> {
    if (this.stopped || this.running) return;
    this.running = true;
    try {
      this.onStatus({ state: "syncing" });
      const { changed } = await this.runOnce();
      this.interval = changed
        ? this.tuning.minIntervalMs
        : Math.min(this.interval * 1.5, this.tuning.maxIntervalMs);
      this.errorDelay = this.tuning.errorMinMs;
      this.onStatus({ state: "live" });
      if (!this.stopped) this.schedule(this.interval);
    } catch (e) {
      this.onStatus({ state: "error", error: String(e) });
      const delay = this.errorDelay;
      this.errorDelay = Math.min(this.errorDelay * 2, this.tuning.errorMaxMs);
      if (!this.stopped) this.schedule(delay);
    } finally {
      this.running = false;
    }
  }
}
