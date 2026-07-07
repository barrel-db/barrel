/**
 * BQL scanner, a port of barrel_bql_lexer.xrl. Keywords are
 * case-insensitive; bare identifiers are keyword-checked, double-quoted
 * identifiers are not ("" escapes a quote); strings are single-quoted
 * ('' escapes). $name is a parameter. -- starts a line comment. The
 * scanner emits '-' for negatives; the parser builds negative numbers.
 */
import type { Loc } from "./ast.js";
import { BqlError } from "./errors.js";

export interface Token {
  type: string; // keyword | operator | ident | string | integer | float | param
  value?: string | number;
  loc: Loc;
}

const KEYWORDS = new Set([
  "select", "from", "where", "order", "by", "limit", "offset", "as",
  "unnest", "asc", "desc", "and", "or", "not", "in", "like", "between",
  "is", "null", "missing", "true", "false", "subscribe", "contains",
]);

const TWO_CHAR = new Set(["=>", "<=", ">=", "<>", "!="]);
const ONE_CHAR = new Set(["=", "<", ">", "(", ")", ",", ".", "[", "]", "*", "-"]);

function isWordStart(c: string): boolean {
  return /[a-zA-Z_]/.test(c);
}
function isWord(c: string): boolean {
  return /[a-zA-Z0-9_]/.test(c);
}
function isDigit(c: string): boolean {
  return c >= "0" && c <= "9";
}

export function tokenize(src: string): Token[] {
  const tokens: Token[] = [];
  let i = 0;
  let line = 1;
  let col = 1;
  const n = src.length;

  const at = (o = 0): string => src[i + o] ?? "";
  const advance = (count = 1): void => {
    for (let k = 0; k < count; k++) {
      if (src[i] === "\n") {
        line++;
        col = 1;
      } else {
        col++;
      }
      i++;
    }
  };

  while (i < n) {
    const c = at();
    const loc: Loc = { line, column: col };

    if (c === " " || c === "\t" || c === "\r" || c === "\n") {
      advance();
      continue;
    }
    if (c === "-" && at(1) === "-") {
      while (i < n && at() !== "\n") advance();
      continue;
    }
    if (isWordStart(c)) {
      let s = "";
      while (i < n && isWord(at())) {
        s += at();
        advance();
      }
      const lower = s.toLowerCase();
      if (KEYWORDS.has(lower)) tokens.push({ type: lower, loc });
      else tokens.push({ type: "ident", value: s, loc });
      continue;
    }
    if (c === '"') {
      tokens.push({ type: "ident", value: readQuoted('"', loc, advance, at), loc });
      continue;
    }
    if (c === "'") {
      tokens.push({ type: "string", value: readQuoted("'", loc, advance, at), loc });
      continue;
    }
    if (isDigit(c)) {
      let s = "";
      while (i < n && isDigit(at())) {
        s += at();
        advance();
      }
      if (at() === "." && isDigit(at(1))) {
        s += ".";
        advance();
        while (i < n && isDigit(at())) {
          s += at();
          advance();
        }
        if (at() === "e" || at() === "E") {
          s += at();
          advance();
          if (at() === "+" || at() === "-") {
            s += at();
            advance();
          }
          while (i < n && isDigit(at())) {
            s += at();
            advance();
          }
        }
        tokens.push({ type: "float", value: Number.parseFloat(s), loc });
      } else {
        tokens.push({ type: "integer", value: Number.parseInt(s, 10), loc });
      }
      continue;
    }
    if (c === "$") {
      advance();
      let s = "";
      if (!isWordStart(at())) throw new BqlError("parse_error", "bad parameter name");
      while (i < n && isWord(at())) {
        s += at();
        advance();
      }
      tokens.push({ type: "param", value: s, loc });
      continue;
    }
    const two = c + at(1);
    if (TWO_CHAR.has(two)) {
      advance(2);
      tokens.push({ type: two, loc });
      continue;
    }
    if (ONE_CHAR.has(c)) {
      advance();
      tokens.push({ type: c, loc });
      continue;
    }
    throw new BqlError("parse_error", `unexpected character '${c}'`);
  }
  tokens.push({ type: "eof", loc: { line, column: col } });
  return tokens;
}

function readQuoted(
  quote: string,
  loc: Loc,
  advance: (n?: number) => void,
  at: (o?: number) => string,
): string {
  advance(); // opening quote
  let s = "";
  for (;;) {
    const c = at();
    if (c === "") throw new BqlError("parse_error", "unterminated quote");
    if (c === quote) {
      if (at(1) === quote) {
        s += quote; // "" or '' escape
        advance(2);
        continue;
      }
      advance(); // closing quote
      return s;
    }
    s += c;
    advance();
  }
}
