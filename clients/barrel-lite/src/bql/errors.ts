/**
 * BQL error taxonomy. The `tag` mirrors the Erlang compile-error family
 * (parse_error, use_is_null, reserved_field, constant_predicate,
 * unsupported, alias_required, ...) so a query that fails to compile
 * fails the same way locally and server-side.
 */
export class BqlError extends Error {
  readonly tag: string;
  constructor(tag: string, message: string) {
    super(message);
    this.name = "BqlError";
    this.tag = tag;
  }
}

/** A table function or SUBSCRIBE query the local executor cannot run. */
export class BqlServerOnlyError extends Error {
  readonly reason: string;
  constructor(reason: string) {
    super(`this query must run on the server: ${reason}`);
    this.name = "BqlServerOnlyError";
    this.reason = reason;
  }
}
