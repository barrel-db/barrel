/**
 * BQL lowering, a port of barrel_bql_lower: canonicalize (substitute
 * params, tag paths base/unnest, normalize operand order, dualize NOT,
 * expand BETWEEN, flatten AND), validate (reserved fields, null
 * literals, constant predicates, id/order/select rules), and lower to a
 * tagged condition DSL the local evaluator runs. The client evaluates
 * every conjunct per frame, so there is no engine/residual split: all
 * conditions carry a base (b) or unnest (u) tag.
 */
import type {
  Ast,
  CmpOp,
  Expr,
  Lit,
  Operand,
  Path,
  PathComp,
  Projection,
  Source,
} from "./ast.js";
import { BqlError } from "./errors.js";

export type LitValue = string | number | boolean | null;

export interface TaggedPath {
  tag: "b" | "u";
  comps: PathComp[];
}

export type CompareOp = "==" | "=/=" | "<" | "=<" | ">" | ">=";

export type Cond =
  | { t: "path"; p: TaggedPath; v: LitValue }
  | { t: "compare"; p: TaggedPath; op: CompareOp; v: LitValue }
  | { t: "exists"; p: TaggedPath }
  | { t: "missing"; p: TaggedPath }
  | { t: "in"; p: TaggedPath; vs: LitValue[] }
  | { t: "contains"; p: TaggedPath; v: LitValue }
  | { t: "prefix"; p: TaggedPath; stem: string }
  | { t: "regex"; p: TaggedPath; re: RegExp }
  | { t: "and"; cs: Cond[] }
  | { t: "or"; cs: Cond[] }
  | { t: "not"; c: Cond };

export interface ProjItem {
  path: TaggedPath;
  name: string;
}

export interface Plan {
  source: Source;
  subscribe: boolean;
  unnest?: { comps: PathComp[]; alias: string };
  conds: Cond[];
  order?: { path: TaggedPath; dir: "asc" | "desc" };
  offset: number;
  limit?: number;
  project: "star" | ProjItem[];
  empty: boolean;
}

const TABLE_FNS = new Set(["vector_top_k", "bm25_top_k", "hybrid_top_k"]);

interface Ctx {
  alias: string;
  unnestAlias: string | undefined;
  params: Record<string, LitValue>;
}

// A canonicalized path with base/unnest tag.
interface CPath {
  tag: "b" | "u";
  comps: PathComp[];
}
interface CLit {
  value: LitValue;
}
type CExpr =
  | { k: "and"; a: CExpr; b: CExpr }
  | { k: "or"; a: CExpr; b: CExpr }
  | { k: "not"; e: CExpr }
  | { k: "cmp"; op: CmpOp; path: CPath; lit: CLit }
  | { k: "is_null"; path: CPath; neg: boolean }
  | { k: "is_missing"; path: CPath; neg: boolean }
  | { k: "in"; path: CPath; lits: CLit[]; neg: boolean }
  | { k: "like"; path: CPath; pattern: string; neg: boolean }
  | { k: "between"; path: CPath; lo: CLit; hi: CLit; neg: boolean }
  | { k: "contains"; path: CPath; lit: CLit; neg: boolean };

export function compilePlan(ast: Ast, params: Record<string, LitValue> = {}): Plan {
  const alias = resolveAlias(ast.from.source, ast.from.alias);
  const ctx: Ctx = { alias, unnestAlias: ast.from.unnest?.alias, params };

  const conjuncts = ast.where ? flattenAnd(pushNot(canonExpr(ast.where, ctx), false)) : [];

  validate(ast, ctx, conjuncts);

  const conds = conjuncts.map(lowerCond);
  const offset = ast.offset ?? 0;
  const limit = ast.limit;
  const plan: Plan = {
    source: ast.from.source,
    subscribe: ast.subscribe,
    conds,
    offset,
    project: lowerSelect(ast, ctx),
    empty: limit === 0,
  };
  if (ast.from.unnest) {
    plan.unnest = { comps: ast.from.unnest.path.comps, alias: ast.from.unnest.alias };
  }
  if (limit !== undefined) plan.limit = limit;
  if (ast.orderBy.length === 1) {
    const item = ast.orderBy[0] as { path: Path; dir: "asc" | "desc" };
    plan.order = { path: canonPath(item.path, ctx), dir: item.dir };
  }
  return plan;
}

function resolveAlias(source: Source, alias: string | undefined): string {
  if (alias !== undefined) return alias;
  if (source.kind === "collection") return source.name;
  throw new BqlError("alias_required", "a table function source needs an alias (AS v)");
}

//====================================================================
// Canonicalize
//====================================================================

function canonPath(path: Path, ctx: Ctx): CPath {
  if (path.head === ctx.unnestAlias) return { tag: "u", comps: path.comps };
  if (path.head === ctx.alias) return { tag: "b", comps: path.comps };
  return { tag: "b", comps: [{ key: path.head }, ...path.comps] };
}

function canonOperand(op: Operand, ctx: Ctx): CPath | CLit {
  if (op.kind === "path") return canonPath(op, ctx);
  if (op.kind === "lit") return { value: op.value };
  const v = ctx.params[op.name];
  if (v === undefined) throw new BqlError("unbound_param", `parameter $${op.name} is not bound`);
  if (!isScalar(v)) throw new BqlError("invalid_param", `parameter $${op.name} must be a scalar`);
  return { value: v };
}

function isScalar(v: unknown): v is LitValue {
  return v === null || ["string", "number", "boolean"].includes(typeof v);
}
function isPath(x: CPath | CLit): x is CPath {
  return "tag" in x;
}

function subjectPath(op: Operand, ctx: Ctx): CPath {
  const c = canonOperand(op, ctx);
  if (!isPath(c)) throw new BqlError("constant_predicate", "predicates must reference a field");
  return c;
}
function literalBound(op: Operand, ctx: Ctx): CLit {
  const c = canonOperand(op, ctx);
  if (isPath(c)) throw new BqlError("unsupported", "a BETWEEN bound must be a literal");
  return c;
}

function canonExpr(e: Expr, ctx: Ctx): CExpr {
  switch (e.kind) {
    case "and":
      return { k: "and", a: canonExpr(e.left, ctx), b: canonExpr(e.right, ctx) };
    case "or":
      return { k: "or", a: canonExpr(e.left, ctx), b: canonExpr(e.right, ctx) };
    case "not":
      return { k: "not", e: canonExpr(e.expr, ctx) };
    case "cmp": {
      const l = canonOperand(e.left, ctx);
      const r = canonOperand(e.right, ctx);
      if (isPath(l) && !isPath(r)) return { k: "cmp", op: e.op, path: l, lit: r };
      if (!isPath(l) && isPath(r)) return { k: "cmp", op: mirror(e.op), path: r, lit: l };
      if (!isPath(l) && !isPath(r))
        throw new BqlError("constant_predicate", "predicates must reference a field");
      throw new BqlError("unsupported", "path-to-path comparison is not supported");
    }
    case "is_null":
      return { k: "is_null", path: subjectPath(e.operand, ctx), neg: e.neg };
    case "is_missing":
      return { k: "is_missing", path: subjectPath(e.operand, ctx), neg: e.neg };
    case "in":
      return {
        k: "in",
        path: subjectPath(e.operand, ctx),
        lits: e.values.map((v) => canonLit(v, ctx)),
        neg: e.neg,
      };
    case "like":
      return { k: "like", path: subjectPath(e.operand, ctx), pattern: e.pattern, neg: e.neg };
    case "between":
      return {
        k: "between",
        path: subjectPath(e.operand, ctx),
        lo: literalBound(e.lo, ctx),
        hi: literalBound(e.hi, ctx),
        neg: e.neg,
      };
    case "contains":
      return { k: "contains", path: canonPath(e.path, ctx), lit: canonLit(e.value, ctx), neg: false };
  }
}

function canonLit(l: Lit, _ctx: Ctx): CLit {
  return { value: l.value };
}

function mirror(op: CmpOp): CmpOp {
  switch (op) {
    case "<": return ">";
    case "<=": return ">=";
    case ">": return "<";
    case ">=": return "<=";
    default: return op;
  }
}

function negate(op: CmpOp): CmpOp {
  switch (op) {
    case "=": return "!=";
    case "!=": return "=";
    case "<": return ">=";
    case "<=": return ">";
    case ">": return "<=";
    case ">=": return "<";
  }
}

function pushNot(e: CExpr, negated: boolean): CExpr {
  switch (e.k) {
    case "not":
      return pushNot(e.e, !negated);
    case "and":
      return negated
        ? { k: "or", a: pushNot(e.a, true), b: pushNot(e.b, true) }
        : { k: "and", a: pushNot(e.a, false), b: pushNot(e.b, false) };
    case "or":
      return negated
        ? { k: "and", a: pushNot(e.a, true), b: pushNot(e.b, true) }
        : { k: "or", a: pushNot(e.a, false), b: pushNot(e.b, false) };
    case "cmp":
      return negated ? { k: "cmp", op: negate(e.op), path: e.path, lit: e.lit } : e;
    case "is_null":
      return { k: "is_null", path: e.path, neg: e.neg !== negated };
    case "is_missing":
      return { k: "is_missing", path: e.path, neg: e.neg !== negated };
    case "in":
      return { k: "in", path: e.path, lits: e.lits, neg: e.neg !== negated };
    case "like":
      return { k: "like", path: e.path, pattern: e.pattern, neg: e.neg !== negated };
    case "contains":
      return { k: "contains", path: e.path, lit: e.lit, neg: e.neg !== negated };
    case "between":
      return (e.neg !== negated)
        ? {
            k: "or",
            a: { k: "cmp", op: "<", path: e.path, lit: e.lo },
            b: { k: "cmp", op: ">", path: e.path, lit: e.hi },
          }
        : {
            k: "and",
            a: { k: "cmp", op: ">=", path: e.path, lit: e.lo },
            b: { k: "cmp", op: "<=", path: e.path, lit: e.hi },
          };
  }
}

function flattenAnd(e: CExpr): CExpr[] {
  if (e.k === "and") return [...flattenAnd(e.a), ...flattenAnd(e.b)];
  return [e];
}

//====================================================================
// Validate
//====================================================================

function validate(ast: Ast, ctx: Ctx, conjuncts: CExpr[]): void {
  if (ast.from.unnest && ast.from.unnest.alias === ctx.alias) {
    throw new BqlError("duplicate_alias", `alias '${ctx.alias}' is used twice`);
  }
  checkSource(ast.from.source);
  checkSubscribe(ast);
  for (const p of collectPaths(ast, ctx, conjuncts)) checkPath(p, ast.from.source);
  for (const c of conjuncts) checkConjunct(c);
  checkOrder(ast);
  checkSelect(ast, ctx);
}

function checkSource(source: Source): void {
  if (source.kind === "table_fn" && !TABLE_FNS.has(source.name)) {
    throw new BqlError("unknown_table_function", `unknown table function ${source.name}`);
  }
}

function checkSubscribe(ast: Ast): void {
  if (!ast.subscribe) return;
  if (ast.from.source.kind === "table_fn")
    throw new BqlError("unsupported_with_subscribe", "table function is not supported with SUBSCRIBE");
  if (ast.from.unnest)
    throw new BqlError("unsupported_with_subscribe", "unnest is not supported with SUBSCRIBE");
  if (ast.orderBy.length > 0)
    throw new BqlError("unsupported_with_subscribe", "order_by is not supported with SUBSCRIBE");
  if (ast.offset !== undefined)
    throw new BqlError("unsupported_with_subscribe", "offset is not supported with SUBSCRIBE");
}

function collectPaths(ast: Ast, ctx: Ctx, conjuncts: CExpr[]): CPath[] {
  const paths: CPath[] = [];
  if (ast.select !== "star") {
    for (const p of ast.select) paths.push(canonPath(p.expr, ctx));
  }
  if (ast.from.unnest) paths.push(canonPath(ast.from.unnest.path, ctx));
  for (const c of conjuncts) collectExprPaths(c, paths);
  for (const o of ast.orderBy) paths.push(canonPath(o.path, ctx));
  return paths;
}

function collectExprPaths(e: CExpr, out: CPath[]): void {
  if (e.k === "and" || e.k === "or") {
    collectExprPaths(e.a, out);
    collectExprPaths(e.b, out);
  } else if (e.k === "not") {
    collectExprPaths(e.e, out);
  } else {
    out.push(e.path);
  }
}

function checkPath(p: CPath, source: Source): void {
  if (p.comps.some((c) => "wildcard" in c)) {
    throw new BqlError("unsupported", "wildcard [*] paths are not supported; use UNNEST");
  }
  if (p.tag === "b" && p.comps.length >= 1) {
    const first = p.comps[0];
    if (first && "key" in first && first.key.startsWith("_")) {
      const only = p.comps.length === 1;
      const fn = source.kind === "table_fn" ? source.name : undefined;
      if (only && first.key === "_score" && fn !== undefined) return;
      if (only && first.key === "_distance" && fn === "vector_top_k") return;
      throw new BqlError("reserved_field", `field '${first.key}' is reserved and not queryable`);
    }
  }
}

function checkConjunct(c: CExpr): void {
  checkNullLiterals(c);
  checkSourceRefs(c);
  checkScoreRefs(c);
  checkIdUse(c, "top");
}

function checkNullLiterals(c: CExpr): void {
  if (c.k === "and" || c.k === "or") {
    checkNullLiterals(c.a);
    checkNullLiterals(c.b);
  } else if (c.k === "cmp" && c.lit.value === null) {
    throw new BqlError("use_is_null", "comparisons with NULL never match; use IS NULL or IS MISSING");
  } else if (c.k === "in" && c.lits.some((l) => l.value === null)) {
    throw new BqlError("use_is_null", "comparisons with NULL never match; use IS NULL or IS MISSING");
  }
}

function leafPath(c: CExpr): CPath | undefined {
  switch (c.k) {
    case "cmp":
    case "is_null":
    case "is_missing":
    case "in":
    case "like":
    case "contains":
      return c.path;
    default:
      return undefined;
  }
}

function checkSourceRefs(c: CExpr): void {
  if (c.k === "and" || c.k === "or") {
    checkSourceRefs(c.a);
    checkSourceRefs(c.b);
    return;
  }
  const p = leafPath(c);
  if (p && p.tag === "b" && p.comps.length === 0) {
    throw new BqlError("unsupported", "bare source reference is not a predicate");
  }
}

function checkScoreRefs(c: CExpr): void {
  if (c.k === "and" || c.k === "or") {
    checkScoreRefs(c.a);
    checkScoreRefs(c.b);
    return;
  }
  const p = leafPath(c);
  if (p && p.tag === "b" && p.comps.length === 1) {
    const first = p.comps[0];
    if (first && "key" in first && (first.key === "_score" || first.key === "_distance")) {
      throw new BqlError("unsupported", "_score and _distance can be selected and ordered by, not filtered on");
    }
  }
}

function checkIdUse(c: CExpr, level: "top" | "nested"): void {
  if (c.k === "and" || c.k === "or") {
    checkIdUse(c.a, "nested");
    checkIdUse(c.b, "nested");
    return;
  }
  const p = leafPath(c);
  if (!p || p.tag !== "b" || p.comps.length !== 1) return;
  const first = p.comps[0];
  if (!first || !("key" in first) || first.key !== "id") return;
  if (level === "nested")
    throw new BqlError("unsupported", "document id conditions cannot appear inside OR or NOT");
  if (c.k === "cmp") {
    const rangeOp = ["=", "<", "<=", ">", ">="].includes(c.op);
    if (!(rangeOp && typeof c.lit.value === "string")) {
      throw new BqlError("unsupported", "the document id only supports =, range comparisons and LIKE 'prefix%'");
    }
    return;
  }
  if (c.k === "like" && !c.neg && likePrefix(c.pattern).kind === "prefix") return;
  throw new BqlError("unsupported", "the document id only supports =, range comparisons and LIKE 'prefix%'");
}

function checkOrder(ast: Ast): void {
  if (ast.orderBy.length <= 1) return;
  throw new BqlError("unsupported", "ORDER BY supports a single key");
}

function checkSelect(ast: Ast, ctx: Ctx): void {
  if (ast.select === "star") return;
  const names = ast.select.map((p) => projectionName(p, ctx));
  const seen = new Set<string>();
  for (const n of names) {
    if (seen.has(n)) throw new BqlError("duplicate_output_name", `duplicate output column '${n}'; add AS`);
    seen.add(n);
  }
}

//====================================================================
// Lower conditions and projections
//====================================================================

function lowerCond(c: CExpr): Cond {
  switch (c.k) {
    case "and":
      return { t: "and", cs: [lowerCond(c.a), lowerCond(c.b)] };
    case "or":
      return { t: "or", cs: [lowerCond(c.a), lowerCond(c.b)] };
    case "cmp":
      return lowerCmp(c.op, tagged(c.path), c.lit.value);
    case "is_null":
      return c.neg
        ? { t: "compare", p: tagged(c.path), op: "=/=", v: null }
        : { t: "or", cs: [{ t: "path", p: tagged(c.path), v: null }, { t: "missing", p: tagged(c.path) }] };
    case "is_missing":
      return c.neg ? { t: "exists", p: tagged(c.path) } : { t: "missing", p: tagged(c.path) };
    case "in": {
      const inner: Cond = { t: "in", p: tagged(c.path), vs: c.lits.map((l) => l.value) };
      return c.neg ? guarded(tagged(c.path), inner) : inner;
    }
    case "like": {
      const pre = likePrefix(c.pattern);
      const inner: Cond =
        pre.kind === "prefix"
          ? { t: "prefix", p: tagged(c.path), stem: pre.stem }
          : { t: "regex", p: tagged(c.path), re: likeRegex(c.pattern) };
      return c.neg ? guarded(tagged(c.path), inner) : inner;
    }
    case "contains": {
      const inner: Cond = { t: "contains", p: tagged(c.path), v: c.lit.value };
      return c.neg ? guarded(tagged(c.path), inner) : inner;
    }
    case "not":
      // NOT is eliminated by pushNot before lowering; any remaining
      // is a guarded negation built above, never a raw expr here.
      return { t: "not", c: lowerCond(c.e) };
    case "between":
      // pushNot always expands BETWEEN into >= AND <=; unreachable.
      throw new BqlError("unsupported", "internal: BETWEEN not expanded");
  }
}

function lowerCmp(op: CmpOp, p: TaggedPath, v: LitValue): Cond {
  switch (op) {
    case "=": return { t: "path", p, v };
    case "!=": return { t: "compare", p, op: "=/=", v };
    case "<": return { t: "compare", p, op: "<", v };
    case "<=": return { t: "compare", p, op: "=<", v };
    case ">": return { t: "compare", p, op: ">", v };
    case ">=": return { t: "compare", p, op: ">=", v };
  }
}

function guarded(p: TaggedPath, inner: Cond): Cond {
  return { t: "and", cs: [{ t: "exists", p }, { t: "not", c: inner }] };
}

function tagged(p: CPath): TaggedPath {
  return { tag: p.tag, comps: p.comps };
}

function lowerSelect(ast: Ast, ctx: Ctx): "star" | ProjItem[] {
  if (ast.select === "star") return "star";
  return ast.select.map((p) => ({ path: canonPath(p.expr, ctx), name: projectionName(p, ctx) }));
}

function projectionName(p: Projection, ctx: Ctx): string {
  if (p.as !== undefined) return p.as;
  const cp = canonPath(p.expr, ctx);
  if (cp.comps.length === 0) {
    return cp.tag === "u" ? (ctx.unnestAlias as string) : ctx.alias;
  }
  const last = cp.comps[cp.comps.length - 1];
  if (last && "key" in last) return last.key;
  throw new BqlError("alias_required", "a projection ending in an array index needs AS");
}

//====================================================================
// LIKE
//====================================================================

export function likePrefix(pattern: string): { kind: "prefix"; stem: string } | { kind: "regex" } {
  if (pattern.length < 2) return { kind: "regex" };
  if (pattern.endsWith("%")) {
    const stem = pattern.slice(0, -1);
    if (!stem.includes("%") && !stem.includes("_")) return { kind: "prefix", stem };
  }
  return { kind: "regex" };
}

const RE_SPECIAL = new Set([".", "^", "$", "*", "+", "?", "(", ")", "[", "]", "{", "}", "|", "\\"]);

export function likeRegex(pattern: string): RegExp {
  let body = "";
  for (const ch of pattern) {
    if (ch === "%") body += ".*";
    else if (ch === "_") body += ".";
    else body += RE_SPECIAL.has(ch) ? `\\${ch}` : ch;
  }
  return new RegExp(`^${body}$`);
}
