/**
 * BQL evaluator, a port of barrel_bql_exec's frame surface. A frame is
 * {doc, elem}: base (b) paths read the document, unnest (u) paths the
 * element. Leaves are exists-and-satisfies (an absent path is false for
 * every leaf except missing); ordered comparisons require both operands
 * the same primitive kind. ORDER BY sorts on a single key, stable, with
 * missing as null and a fixed cross-type total order.
 */
import { compareBytes, utf8Encode } from "../codec/bytes.js";
import type { JsonObject, JsonValue } from "../json.js";
import type { Cond, CompareOp, Plan, ProjItem, TaggedPath } from "./lower.js";

export interface Frame {
  doc: JsonObject;
  elem: JsonValue | undefined;
}

type Found = { ok: true; value: JsonValue } | { ok: false };

function isPlainObject(v: unknown): v is JsonObject {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

export function getPath(value: JsonValue, comps: readonly { key?: string; index?: number }[]): Found {
  let cur: JsonValue = value;
  for (const comp of comps) {
    if (comp.key !== undefined) {
      if (isPlainObject(cur) && Object.prototype.hasOwnProperty.call(cur, comp.key)) {
        cur = cur[comp.key] as JsonValue;
      } else {
        return { ok: false };
      }
    } else if (comp.index !== undefined) {
      if (Array.isArray(cur) && comp.index >= 0 && comp.index < cur.length) {
        cur = cur[comp.index] as JsonValue;
      } else {
        return { ok: false };
      }
    } else {
      return { ok: false }; // wildcard (rejected at lowering)
    }
  }
  return { ok: true, value: cur };
}

function frameValue(p: TaggedPath, frame: Frame): Found {
  if (p.tag === "b") return getPath(frame.doc, p.comps as { key?: string; index?: number }[]);
  if (frame.elem === undefined) return { ok: false };
  return getPath(frame.elem, p.comps as { key?: string; index?: number }[]);
}

/** Same-type-only comparison, mirroring barrel_query:compare_values/3. */
export function compareValues(a: JsonValue, op: CompareOp, b: JsonValue): boolean {
  if (op === "=/=") return !exactEqual(a, b);
  if (op === "==") return exactEqual(a, b);
  const bothNum = typeof a === "number" && typeof b === "number";
  const bothStr = typeof a === "string" && typeof b === "string";
  if (!bothNum && !bothStr) return false;
  switch (op) {
    case "<": return a < b;
    case "=<": return a <= b;
    case ">": return a > b;
    case ">=": return a >= b;
  }
}

function exactEqual(a: JsonValue, b: JsonValue): boolean {
  return a === b; // comparisons always have a scalar literal on one side
}

export function evalCond(cond: Cond, frame: Frame): boolean {
  switch (cond.t) {
    case "path": {
      const r = frameValue(cond.p, frame);
      return r.ok && exactEqual(r.value, cond.v);
    }
    case "compare": {
      const r = frameValue(cond.p, frame);
      return r.ok && compareValues(r.value, cond.op, cond.v);
    }
    case "exists":
      return frameValue(cond.p, frame).ok;
    case "missing":
      return !frameValue(cond.p, frame).ok;
    case "in": {
      const r = frameValue(cond.p, frame);
      return r.ok && cond.vs.some((v) => exactEqual(r.value, v));
    }
    case "contains": {
      const r = frameValue(cond.p, frame);
      return r.ok && Array.isArray(r.value) && r.value.some((x) => exactEqual(x as JsonValue, cond.v));
    }
    case "prefix": {
      const r = frameValue(cond.p, frame);
      return r.ok && typeof r.value === "string" && r.value.startsWith(cond.stem);
    }
    case "regex": {
      const r = frameValue(cond.p, frame);
      return r.ok && typeof r.value === "string" && cond.re.test(r.value);
    }
    case "and":
      return cond.cs.every((c) => evalCond(c, frame));
    case "or":
      return cond.cs.some((c) => evalCond(c, frame));
    case "not":
      return !evalCond(cond.c, frame);
  }
}

/** Expand a document into frames (inner-join UNNEST). */
export function framesFor(doc: JsonObject, unnest: Plan["unnest"]): Frame[] {
  if (!unnest) return [{ doc, elem: undefined }];
  const r = getPath(doc, unnest.comps as { key?: string; index?: number }[]);
  if (r.ok && Array.isArray(r.value)) {
    return r.value.map((elem) => ({ doc, elem: elem as JsonValue }));
  }
  return [];
}

//====================================================================
// Order, offset/limit, project
//====================================================================

/** Erlang term order over JSON: number < bool/null < object < array <
 * string, with false < null < true. Strings compare by UTF-8 bytes. */
export function termCompare(a: JsonValue, b: JsonValue): number {
  const ra = rank(a);
  const rb = rank(b);
  if (ra !== rb) return ra < rb ? -1 : 1;
  switch (ra) {
    case 0: // number
      return (a as number) < (b as number) ? -1 : (a as number) > (b as number) ? 1 : 0;
    case 1: // false < null < true
      return atomRank(a) - atomRank(b);
    case 4: // string, byte order
      return compareBytes(utf8Encode(a as string), utf8Encode(b as string));
    default: // object / array: not ordered here (stable fallback)
      return 0;
  }
}

function rank(v: JsonValue): number {
  if (typeof v === "number") return 0;
  if (typeof v === "boolean" || v === null) return 1;
  if (Array.isArray(v)) return 3;
  if (typeof v === "object") return 2;
  return 4; // string
}

function atomRank(v: JsonValue): number {
  if (v === false) return 0;
  if (v === null) return 1;
  return 2; // true
}

export function finalize(frames: Frame[], plan: Plan): JsonObject[] {
  let ordered = frames;
  if (plan.order) {
    const key = plan.order.path;
    const dir = plan.order.dir;
    const decorated = frames.map((frame, index) => {
      const r = frameValue(key, frame);
      return { sort: r.ok ? r.value : null, index, frame };
    });
    decorated.sort((x, y) => {
      const c = termCompare(x.sort, y.sort);
      if (c !== 0) return dir === "asc" ? c : -c;
      return x.index - y.index; // stable
    });
    ordered = decorated.map((d) => d.frame);
  }
  let sliced = plan.offset >= ordered.length ? [] : ordered.slice(plan.offset);
  if (plan.limit !== undefined) sliced = sliced.slice(0, plan.limit);
  return sliced.map((frame) => projectFrame(plan.project, frame, plan.unnest));
}

function projectFrame(
  project: Plan["project"],
  frame: Frame,
  unnest: Plan["unnest"],
): JsonObject {
  if (project === "star") {
    const row: JsonObject = {};
    for (const [k, v] of Object.entries(frame.doc)) {
      if (k !== "_rev") row[k] = v;
    }
    if (unnest && frame.elem !== undefined) row[unnest.alias] = frame.elem;
    return row;
  }
  const row: JsonObject = {};
  if (Object.prototype.hasOwnProperty.call(frame.doc, "id")) {
    row["id"] = frame.doc["id"] as JsonValue;
  }
  for (const item of project as ProjItem[]) {
    const r = frameValue(item.path, frame);
    if (r.ok) row[item.name] = r.value;
  }
  return row;
}
