/**
 * The local BQL executor: compile a query, then run it over a set of
 * cached document bodies, reproducing the server's rows for the same
 * query text. Table functions and SUBSCRIBE cannot run locally (they
 * need the ANN/FTS index or a live subscription); those raise
 * BqlServerOnlyError so the caller can delegate to the server.
 */
import { compareBytes, utf8Encode } from "../codec/bytes.js";
import type { JsonObject } from "../json.js";
import { parse } from "./parser.js";
import { compilePlan, type LitValue, type Plan } from "./lower.js";
import { evalCond, finalize, framesFor, type Frame } from "./eval.js";
import { BqlServerOnlyError } from "./errors.js";

export interface CompileOptions {
  params?: Record<string, LitValue>;
}

export function compile(bql: string, opts: CompileOptions = {}): Plan {
  return compilePlan(parse(bql), opts.params ?? {});
}

/** Run a compiled plan over document bodies (each carrying its id). */
export function runLocal(plan: Plan, docs: Iterable<JsonObject>): JsonObject[] {
  if (plan.source.kind === "table_fn") {
    throw new BqlServerOnlyError("table function (vector/keyword search)");
  }
  if (plan.subscribe) {
    throw new BqlServerOnlyError("SUBSCRIBE live query");
  }
  if (plan.empty) return [];

  // primary-key scan order, so unordered/stable results match the server
  const sorted = [...docs].sort((a, b) => idCompare(a, b));
  const frames: Frame[] = [];
  for (const doc of sorted) {
    for (const frame of framesFor(doc, plan.unnest)) {
      if (plan.conds.every((c) => evalCond(c, frame))) frames.push(frame);
    }
  }
  return finalize(frames, plan);
}

function idCompare(a: JsonObject, b: JsonObject): number {
  return compareBytes(utf8Encode(String(a["id"] ?? "")), utf8Encode(String(b["id"] ?? "")));
}
