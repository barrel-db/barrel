/**
 * Hybrid Logical Clock: a byte-exact port of barrel_hlc / the hlc
 * library. A timestamp is a 64-bit wall time (milliseconds since the
 * Unix epoch) and a 32-bit logical counter. The 12-byte big-endian
 * encoding is chosen so byte order equals time order.
 *
 * wall is a bigint because the wire field is 64-bit; real wall times
 * sit well under 2^53, but the codec stays exact regardless.
 */

/** Maximum logical counter (2^31 - 1); incrementing past it throws. */
export const MAX_LOGICAL = 0x7fffffff;

export interface Hlc {
  readonly wall: bigint;
  readonly logical: number;
}

/** Raised by HlcClock.update when a remote clock exceeds maxOffset. */
export class ClockSkewError extends Error {
  constructor(message = "clock_skew") {
    super(message);
    this.name = "ClockSkewError";
  }
}

/** Encode a timestamp to 12 big-endian bytes (wall:64, logical:32). */
export function encodeHlc(hlc: Hlc): Uint8Array {
  const buf = new Uint8Array(12);
  const view = new DataView(buf.buffer);
  view.setBigUint64(0, hlc.wall, false);
  view.setUint32(8, hlc.logical, false);
  return buf;
}

/** Decode exactly 12 bytes into a timestamp. */
export function decodeHlc(bytes: Uint8Array): Hlc {
  if (bytes.length !== 12) {
    throw new Error(`hlc must be 12 bytes, got ${bytes.length}`);
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  return { wall: view.getBigUint64(0, false), logical: view.getUint32(8, false) };
}

/** Compare timestamps: wall first, then logical. Returns -1, 0, or 1. */
export function compareHlc(a: Hlc, b: Hlc): -1 | 0 | 1 {
  if (a.wall < b.wall) return -1;
  if (a.wall > b.wall) return 1;
  if (a.logical < b.logical) return -1;
  if (a.logical > b.logical) return 1;
  return 0;
}

function incrLogical(logical: number): number {
  if (logical >= MAX_LOGICAL) {
    throw new Error(`logical_overflow: ${logical}`);
  }
  return logical + 1;
}

export interface HlcClockOptions {
  /** Physical clock in integer milliseconds; defaults to Date.now(). */
  physClock?: () => number;
  /** Max wall-time offset in ms before an update rejects; 0 disables. */
  maxOffset?: number;
  /** Initial state; defaults to {wall: 0, logical: 0}. */
  initial?: Hlc;
}

/**
 * A stateful HLC. now() stamps a local event; update() folds a remote
 * timestamp (e.g. an x-barrel-hlc header) and advances causally. The
 * logic mirrors hlc.erl's now and update_clock exactly, including the
 * four update branches and the maxOffset rejection (which leaves the
 * state unchanged, matching the {timeahead, TS} return).
 */
export class HlcClock {
  private ts: Hlc;
  private readonly physClock: () => number;
  private readonly maxOffset: number;

  constructor(opts: HlcClockOptions = {}) {
    this.physClock = opts.physClock ?? (() => Date.now());
    this.maxOffset = opts.maxOffset ?? 0;
    this.ts = opts.initial ?? { wall: 0n, logical: 0 };
  }

  /** Current state without advancing. */
  peek(): Hlc {
    return this.ts;
  }

  /** Stamp a new local event, advancing the clock. */
  now(): Hlc {
    const now = BigInt(this.physClock());
    if (this.ts.wall >= now) {
      this.ts = { wall: this.ts.wall, logical: incrLogical(this.ts.logical) };
    } else {
      this.ts = { wall: now, logical: 0 };
    }
    return this.ts;
  }

  /** Fold a remote timestamp; throws ClockSkewError past maxOffset. */
  update(remote: Hlc): Hlc {
    const now = BigInt(this.physClock());
    const offset = remote.wall - now;
    const nowIsAhead = now > this.ts.wall && now > remote.wall;
    if (nowIsAhead) {
      this.ts = { wall: now, logical: 0 };
    } else if (remote.wall > this.ts.wall) {
      if (this.maxOffset > 0 && offset > BigInt(this.maxOffset)) {
        throw new ClockSkewError();
      }
      this.ts = { wall: remote.wall, logical: incrLogical(remote.logical) };
    } else if (this.ts.wall > remote.wall) {
      this.ts = { wall: this.ts.wall, logical: incrLogical(this.ts.logical) };
    } else {
      const l = Math.max(this.ts.logical, remote.logical);
      this.ts = { wall: this.ts.wall, logical: incrLogical(l) };
    }
    return this.ts;
  }
}
