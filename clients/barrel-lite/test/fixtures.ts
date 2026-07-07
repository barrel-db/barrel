/**
 * Loader for the golden fixtures generated from the Erlang codecs by
 * scripts/gen_fixtures.escript. Wall times are decimal strings so
 * values above 2^53 survive JSON.
 */
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

export interface TsJson {
  wall: string;
  logical: number;
}

export interface HlcEncodeCase {
  wall: string;
  logical: number;
  hex: string;
}

export interface HlcCompareCase {
  a: TsJson;
  b: TsJson;
  result: "lt" | "eq" | "gt";
}

export interface TraceStep {
  op: "now" | "update";
  phys: string;
  remote?: TsJson;
  result: TsJson | "timeahead";
}

export interface HlcTrace {
  maxoffset: number;
  steps: TraceStep[];
}

export interface VersionCase {
  wall: string;
  logical: number;
  author: string;
  token: string;
}

export interface VersionCompareCase {
  a: { wall: string; logical: number; author: string };
  b: { wall: string; logical: number; author: string };
  result: "lt" | "eq" | "gt";
  winner: { wall: string; logical: number; author: string };
}

export interface VvEntry {
  node: string;
  wall: string;
  logical: number;
}

export interface VvEncodeCase {
  entries: VvEntry[];
  hex: string;
}

export interface VvRelateCase {
  a: VvEntry[];
  b: VvEntry[];
  result: "eq" | "dominates" | "dominated" | "concurrent";
}

export interface VvContainsCase {
  vv: VvEntry[];
  node: string;
  wall: string;
  logical: number;
  result: boolean;
}

export interface Golden {
  max_logical: number;
  hlc_encode: HlcEncodeCase[];
  hlc_compare: HlcCompareCase[];
  hlc_traces: HlcTrace[];
  version_tokens?: VersionCase[];
  version_compare?: VersionCompareCase[];
  vv_encode?: VvEncodeCase[];
  vv_relate?: VvRelateCase[];
  vv_contains?: VvContainsCase[];
}

const path = fileURLToPath(new URL("./fixtures/golden.json", import.meta.url));

export const golden: Golden = JSON.parse(readFileSync(path, "utf8")) as Golden;
