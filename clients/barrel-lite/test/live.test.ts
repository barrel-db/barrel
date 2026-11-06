import { describe, expect, it } from "vitest";
import { fromHex, toHex } from "../src/codec/bytes.js";
import {
  decodeState,
  encodeState,
  liveAuthor,
  liveMerge,
  liveNew,
  liveReplaces,
  liveTokens,
  liveView,
  type LiveState,
} from "../src/codec/live.js";
import { versionToToken, type Version } from "../src/codec/version.js";

const v = (ms: number, w: string): Version => ({ hlc: { wall: BigInt(ms), logical: 0 }, author: w });
const tok = (ms: number, w: string) => versionToToken(v(ms, w));
const write = (s: LiveState, ms: number, w: string, replaces: string[] = []) =>
  liveAuthor(s, v(ms, w), false, replaces);
const tokens = (s: LiveState) => liveTokens(s).map(versionToToken);

// Encoded by barrel_live (Erlang): a two-writer conflict plus an old
// peer's version, and the same merged with an automatic resolution.
const ERLANG_M1 =
  "0100000003001c0000000000000005000000003031323334353637383961626364656600001f00011030313233343536373839616263646566000000000000000500000000000e000000000000000a0000000077610000110001027761000000000000000a00000000000e00000000000000140000000077620100110001027762000000000000001400000000010000000200027761000000000000000a000000000002776200000000000000140000000000000001001c0000000000000005000000003031323334353637383961626364656600000000";
const ERLANG_M3 =
  "0100000003001c0000000000000005000000003031323334353637383961626364656600001f000110303132333435363738396162636465660000000000000005000000000051000000000000000f000000026175746f3a383532316365306239356630643535336536333235386539383034613661333532396466666463346264636539363335313239663338323135616561313166660000720003456175746f3a38353231636530623935663064353533653633323538653938303461366133353239646666646334626463653936333531323966333832313561656131316666000000000000000f00000002027761000000000000000a00000000027763000000000000000f00000000000e00000000000000140000000077620100110001027762000000000000001400000000010000000300027761000000000000000a000000000002776200000000000000140000000000027763000000000000000f0000000000000004001c00000000000000050000000030313233343536373839616263646566000e000000000000000a000000007761000e000000000000000f0000000077630051000000000000000f000000026175746f3a38353231636530623935663064353533653633323538653938303461366133353239646666646334626463653936333531323966333832313561656131316666000000010051000000000000000f000000026175746f3a3835323163653062393566306435353365363332353865393830346136613335323964666664633462646365393633353132396633383231356165613131666600000002000e000000000000000a000000007761000e000000000000000f000000007763000172";

describe("document state (barrel_live port)", () => {
  it("decodes and re-encodes Erlang states byte for byte", () => {
    for (const hex of [ERLANG_M1, ERLANG_M3]) {
      const s = decodeState(fromHex(hex));
      expect(toHex(encodeState(s))).toBe(hex);
    }
    expect(tokens(decodeState(fromHex(ERLANG_M3)))).toEqual([
      "000000000000000500000000@0123456789abcdef",
      "000000000000000f00000002@auto:8521ce0b95f0d553e63258e9804a6a3529dffdc4bdce9635129f38215aea11ff",
      "000000000000001400000000@wb",
    ]);
  });

  it("keeps concurrent writes and drops what the other side replaced", () => {
    const a = write(liveNew(), 1, "wa");
    const b = write(liveNew(), 2, "wb");
    const ab = liveMerge(a, b);
    expect(tokens(ab)).toEqual([tok(1, "wa"), tok(2, "wb")]);
    const r = write(ab, 3, "wa", [tok(1, "wa"), tok(2, "wb")]);
    expect(tokens(liveMerge(r, b))).toEqual([tok(3, "wa")]);
    expect(tokens(liveMerge(b, r))).toEqual([tok(3, "wa")]);
  });

  it("merges commutatively, associatively and idempotently", () => {
    const a = write(liveNew(), 1, "wa");
    const b = write(liveNew(), 2, "wb");
    const c = write(liveMerge(a, b), 3, "wc", [tok(1, "wa")]);
    const enc = (s: LiveState) => toHex(encodeState(s));
    for (const [x, y] of [[a, b], [b, c], [a, c]] as const) {
      expect(enc(liveMerge(x, y))).toBe(enc(liveMerge(y, x)));
      expect(enc(liveMerge(x, x))).toBe(enc(x));
    }
    expect(enc(liveMerge(liveMerge(a, b), c))).toBe(enc(liveMerge(a, liveMerge(b, c))));
  });

  it("edits replace the winner, an observed set, or every delete", () => {
    const s = liveMerge(write(liveNew(), 1, "wa"), write(liveNew(), 2, "wb"));
    expect(liveReplaces(s, tok(2, "wb"), undefined)).toEqual([tok(2, "wb")]);
    expect(liveReplaces(s, tok(1, "wa"), undefined)).toBe("conflict");
    expect(liveReplaces(s, undefined, [tok(1, "wa"), tok(9, "zz")])).toEqual([tok(1, "wa")]);
    const d = liveMerge(liveAuthor(liveNew(), v(1, "wa"), true, []),
                        liveAuthor(liveNew(), v(2, "wb"), true, []));
    expect(liveView(d).deleted).toBe(true);
    expect(liveReplaces(d, undefined, undefined)?.length).toBe(2);
  });
});
