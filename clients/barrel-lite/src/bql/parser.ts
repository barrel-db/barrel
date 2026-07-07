/**
 * BQL recursive-descent parser, a port of barrel_bql_parser.yrl. The
 * layered expression grammar is or > and > not > predicate; <> is
 * normalized to !=; negative numbers are built here; any keyword may
 * follow a dot in a path.
 */
import type {
  Ast,
  CmpOp,
  Expr,
  FnArg,
  From,
  Lit,
  Operand,
  OrderItem,
  Path,
  PathComp,
  Projection,
  SelectList,
  Source,
} from "./ast.js";
import { BqlError } from "./errors.js";
import { tokenize, type Token } from "./lexer.js";

const CMP_OPS: Record<string, CmpOp> = {
  "=": "=",
  "!=": "!=",
  "<>": "!=",
  "<": "<",
  "<=": "<=",
  ">": ">",
  ">=": ">=",
};

const KEYWORD_TYPES = new Set([
  "select", "from", "where", "order", "by", "limit", "offset", "as",
  "unnest", "asc", "desc", "and", "or", "not", "in", "like", "between",
  "is", "null", "missing", "true", "false", "subscribe", "contains",
]);

export function parse(source: string): Ast {
  return new Parser(tokenize(source)).parseStatement();
}

class Parser {
  private pos = 0;
  constructor(private readonly tokens: Token[]) {}

  private peek(off = 0): Token {
    return this.tokens[this.pos + off] as Token;
  }
  private at(type: string): boolean {
    return this.peek().type === type;
  }
  private next(): Token {
    return this.tokens[this.pos++] as Token;
  }
  private eat(type: string): Token {
    if (!this.at(type)) this.err(`expected '${type}' but got '${this.peek().type}'`);
    return this.next();
  }
  private err(msg: string): never {
    const loc = this.peek().loc;
    throw new BqlError("parse_error", `${msg} at line ${loc.line} column ${loc.column}`);
  }

  parseStatement(): Ast {
    this.eat("select");
    const select = this.parseSelectList();
    const from = this.parseFrom();
    const where = this.at("where") ? (this.next(), this.parseExpr()) : undefined;
    const orderBy = this.parseOrder();
    const limit = this.at("limit") ? (this.next(), Number(this.eat("integer").value)) : undefined;
    const offset = this.at("offset") ? (this.next(), Number(this.eat("integer").value)) : undefined;
    const subscribe = this.at("subscribe") ? (this.next(), true) : false;
    this.eat("eof");
    const ast: Ast = { select, from, orderBy, subscribe };
    if (where !== undefined) ast.where = where;
    if (limit !== undefined) ast.limit = limit;
    if (offset !== undefined) ast.offset = offset;
    return ast;
  }

  private parseSelectList(): SelectList {
    if (this.at("*")) {
      this.next();
      return "star";
    }
    const projs: Projection[] = [this.parseProjection()];
    while (this.at(",")) {
      this.next();
      projs.push(this.parseProjection());
    }
    return projs;
  }

  private parseProjection(): Projection {
    const expr = this.parsePath();
    const proj: Projection = { expr, loc: expr.loc };
    if (this.at("as")) {
      this.next();
      proj.as = String(this.eat("ident").value);
    }
    return proj;
  }

  private parseFrom(): From {
    this.eat("from");
    const source = this.parseSource();
    let alias: string | undefined;
    if (this.at("as")) {
      this.next();
      alias = String(this.eat("ident").value);
    } else if (this.at("ident")) {
      alias = String(this.next().value);
    }
    const from: From = { source };
    if (alias !== undefined) from.alias = alias;
    if (this.at(",")) {
      this.next();
      const loc = this.eat("unnest").loc;
      this.eat("(");
      const path = this.parsePath();
      this.eat(")");
      this.eat("as");
      const uAlias = String(this.eat("ident").value);
      from.unnest = { path, alias: uAlias, loc };
    }
    return from;
  }

  private parseSource(): Source {
    const tok = this.eat("ident");
    const name = String(tok.value);
    if (this.at("(")) {
      this.next();
      const args: FnArg[] = [this.parseFnArg()];
      while (this.at(",")) {
        this.next();
        args.push(this.parseFnArg());
      }
      this.eat(")");
      return { kind: "table_fn", name, args, loc: tok.loc };
    }
    return { kind: "collection", name, loc: tok.loc };
  }

  private parseFnArg(): FnArg {
    if (this.at("ident")) {
      const name = String(this.next().value);
      this.eat("=>");
      return { kind: "named", name, value: this.parseLiteralOrParam() };
    }
    return { kind: "pos", value: this.parseLiteralOrParam() };
  }

  private parseLiteralOrParam(): Operand {
    if (this.at("param")) {
      const t = this.next();
      return { kind: "param", name: String(t.value), loc: t.loc };
    }
    return this.parseLiteral();
  }

  //================================================================
  // Expression layer: or > and > not > predicate
  //================================================================

  private parseExpr(): Expr {
    let left = this.parseAnd();
    while (this.at("or")) {
      this.next();
      left = { kind: "or", left, right: this.parseAnd() };
    }
    return left;
  }

  private parseAnd(): Expr {
    let left = this.parseNot();
    while (this.at("and")) {
      this.next();
      left = { kind: "and", left, right: this.parseNot() };
    }
    return left;
  }

  private parseNot(): Expr {
    if (this.at("not")) {
      const loc = this.next().loc;
      return { kind: "not", expr: this.parseNot(), loc };
    }
    return this.parsePredicate();
  }

  private parsePredicate(): Expr {
    if (this.at("(")) {
      this.next();
      const e = this.parseExpr();
      this.eat(")");
      return e;
    }
    if (this.at("contains")) {
      const loc = this.next().loc;
      this.eat("(");
      const path = this.parsePath();
      this.eat(",");
      const value = this.parseLiteral();
      this.eat(")");
      return { kind: "contains", path, value, loc };
    }
    const left = this.parseOperand();
    const t = this.peek();
    if (t.type in CMP_OPS) {
      this.next();
      const op = CMP_OPS[t.type] as CmpOp;
      return { kind: "cmp", op, left, right: this.parseOperand(), loc: t.loc };
    }
    if (this.at("is")) {
      const loc = this.next().loc;
      const neg = this.at("not") ? (this.next(), true) : false;
      if (this.at("null")) {
        this.next();
        return { kind: "is_null", operand: left, neg, loc };
      }
      if (this.at("missing")) {
        this.next();
        return { kind: "is_missing", operand: left, neg, loc };
      }
      this.err("expected NULL or MISSING after IS");
    }
    if (this.at("not")) {
      this.next();
      return this.parseNegatable(left, true);
    }
    return this.parseNegatable(left, false);
  }

  private parseNegatable(operand: Operand, neg: boolean): Expr {
    if (this.at("in")) {
      const loc = this.next().loc;
      this.eat("(");
      const values: Lit[] = [this.parseLiteral()];
      while (this.at(",")) {
        this.next();
        values.push(this.parseLiteral());
      }
      this.eat(")");
      return { kind: "in", operand, values, neg, loc };
    }
    if (this.at("like")) {
      const loc = this.next().loc;
      const pattern = String(this.eat("string").value);
      return { kind: "like", operand, pattern, neg, loc };
    }
    if (this.at("between")) {
      const loc = this.next().loc;
      const lo = this.parseOperand();
      this.eat("and");
      const hi = this.parseOperand();
      return { kind: "between", operand, lo, hi, neg, loc };
    }
    this.err("expected a comparison, IN, LIKE, or BETWEEN");
  }

  private parseOperand(): Operand {
    if (this.at("ident")) return this.parsePath();
    if (this.at("param")) {
      const t = this.next();
      return { kind: "param", name: String(t.value), loc: t.loc };
    }
    return this.parseLiteral();
  }

  private parseLiteral(): Lit {
    const t = this.peek();
    switch (t.type) {
      case "string":
        this.next();
        return { kind: "lit", value: String(t.value), loc: t.loc };
      case "integer":
      case "float":
        this.next();
        return { kind: "lit", value: Number(t.value), loc: t.loc };
      case "-": {
        this.next();
        const num = this.peek();
        if (num.type !== "integer" && num.type !== "float") this.err("expected a number after '-'");
        this.next();
        return { kind: "lit", value: -Number(num.value), loc: t.loc };
      }
      case "true":
        this.next();
        return { kind: "lit", value: true, loc: t.loc };
      case "false":
        this.next();
        return { kind: "lit", value: false, loc: t.loc };
      case "null":
        this.next();
        return { kind: "lit", value: null, loc: t.loc };
      default:
        this.err(`expected a literal but got '${t.type}'`);
    }
  }

  private parsePath(): Path {
    const head = this.eat("ident");
    const comps: PathComp[] = [];
    for (;;) {
      if (this.at(".")) {
        this.next();
        comps.push({ key: this.parsePathKey() });
      } else if (this.at("[")) {
        this.next();
        if (this.at("*")) {
          this.next();
          comps.push({ wildcard: true });
        } else {
          comps.push({ index: Number(this.eat("integer").value) });
        }
        this.eat("]");
      } else {
        break;
      }
    }
    return { kind: "path", head: String(head.value), comps, loc: head.loc };
  }

  private parsePathKey(): string {
    const t = this.peek();
    if (t.type === "ident") {
      this.next();
      return String(t.value);
    }
    if (KEYWORD_TYPES.has(t.type)) {
      this.next();
      return t.type;
    }
    this.err(`expected a path key but got '${t.type}'`);
  }

  private parseOrder(): OrderItem[] {
    if (!this.at("order")) return [];
    this.next();
    this.eat("by");
    const items: OrderItem[] = [this.parseOrderItem()];
    while (this.at(",")) {
      this.next();
      items.push(this.parseOrderItem());
    }
    return items;
  }

  private parseOrderItem(): OrderItem {
    const path = this.parsePath();
    let dir: "asc" | "desc" = "asc";
    if (this.at("asc")) this.next();
    else if (this.at("desc")) {
      this.next();
      dir = "desc";
    }
    return { path, dir };
  }
}
