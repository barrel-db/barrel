/**
 * BQL abstract syntax, a TypeScript mirror of barrel_bql_ast. The exact
 * shape is the client's own (only the evaluated rows must match the
 * server, not the internal tree), but it carries the same information
 * the Erlang grammar produces.
 */
export interface Loc {
  line: number;
  column: number;
}

export type PathComp = { key: string } | { index: number } | { wildcard: true };

export interface Path {
  kind: "path";
  /** Leading identifier (may be an alias, resolved at lowering). */
  head: string;
  comps: PathComp[];
  loc: Loc;
}

export interface Lit {
  kind: "lit";
  value: string | number | boolean | null;
  loc: Loc;
}

export interface Param {
  kind: "param";
  name: string;
  loc: Loc;
}

export type Operand = Path | Lit | Param;

export interface Projection {
  expr: Path;
  as?: string;
  loc: Loc;
}

export type SelectList = "star" | Projection[];

export type FnArg =
  | { kind: "pos"; value: Operand }
  | { kind: "named"; name: string; value: Operand };

export type Source =
  | { kind: "collection"; name: string; loc: Loc }
  | { kind: "table_fn"; name: string; args: FnArg[]; loc: Loc };

export interface Unnest {
  path: Path;
  alias: string;
  loc: Loc;
}

export interface From {
  source: Source;
  alias?: string;
  unnest?: Unnest;
}

export type CmpOp = "=" | "!=" | "<" | "<=" | ">" | ">=";

export type Expr =
  | { kind: "and"; left: Expr; right: Expr }
  | { kind: "or"; left: Expr; right: Expr }
  | { kind: "not"; expr: Expr; loc: Loc }
  | { kind: "cmp"; op: CmpOp; left: Operand; right: Operand; loc: Loc }
  | { kind: "is_null"; operand: Operand; neg: boolean; loc: Loc }
  | { kind: "is_missing"; operand: Operand; neg: boolean; loc: Loc }
  | { kind: "in"; operand: Operand; values: Lit[]; neg: boolean; loc: Loc }
  | { kind: "like"; operand: Operand; pattern: string; neg: boolean; loc: Loc }
  | { kind: "between"; operand: Operand; lo: Operand; hi: Operand; neg: boolean; loc: Loc }
  | { kind: "contains"; path: Path; value: Lit; loc: Loc };

export interface OrderItem {
  path: Path;
  dir: "asc" | "desc";
}

export interface Ast {
  select: SelectList;
  from: From;
  where?: Expr;
  orderBy: OrderItem[];
  limit?: number;
  offset?: number;
  subscribe: boolean;
}
