# Query with BQL

BQL is barrel's query language, a PartiQL (SQL for JSON) dialect: one
SELECT statement queries documents, vector search, and keyword search,
with live subscriptions. Read this when you want to query barrel with a
query string instead of the structured `find/2` spec maps, or when you
want vector and BM25 results joined back to their documents.

## When to use it

- You want one query surface: filters, projections, top-k vector or
  keyword retrieval, and live updates with the same syntax.
- You drive barrel over HTTP and want to send a query string.
- For programmatic query building, the structured `barrel:find/2` spec
  API remains first class; BQL compiles onto the same engine.

## Run a query

```erlang
{ok, Db} = barrel:open(mydb),

{ok, Rows, Meta} = barrel:query(Db,
    <<"SELECT title, author.name AS who "
      "FROM db "
      "WHERE type = 'post' AND rank >= 3 "
      "ORDER BY title LIMIT 10">>),

%% named parameters
{ok, Rows2, _} = barrel:query(Db,
    <<"SELECT * FROM db WHERE org = $org">>,
    #{params => #{<<"org">> => <<"acme">>}}).
```

Rows are maps: `<<"id">>` plus your projections (`SELECT *` returns the
whole document flattened in). A missing attribute leaves its key absent
from the row. Embedded docdb-only users get the same document queries
through `barrel_docdb:query/2,3`.

Queries without ORDER BY, UNNEST or LIMIT stream in chunks: pass
`chunk_size` and follow `Meta` (`has_more`, `continuation`), or fold
without materializing:

```erlang
{ok, Count, _} = barrel:query_fold(Db,
    <<"SELECT * FROM db WHERE type = 'post'">>, #{chunk_size => 100},
    fun(_Row, N) -> {ok, N + 1} end, 0).
```

## The language

```sql
SELECT d.title, d.author.name AS who
FROM db AS d
WHERE d.type = 'post' AND (d.rank > 3 OR d.pinned = true)
ORDER BY d.title DESC
LIMIT 10 OFFSET 20
```

- The FROM name is the range variable; the database comes from the API
  call. `AS d` is optional; without it the FROM name is the alias.
- Paths: `d.a.b`, array index `d.tags[0]`, quoted keys `d."a key"`.
  Keywords are fine after a dot (`d.order`). Top-level `_`-prefixed
  fields are reserved and rejected.
- Operators: `=`, `!=`, `<`, `<=`, `>`, `>=`, `IN (..)`, `LIKE`,
  `BETWEEN a AND b`, `IS [NOT] NULL`, `IS [NOT] MISSING`,
  `CONTAINS(path, value)`, `AND`, `OR`, `NOT`. Comparisons only match
  same-typed values (both numbers or both strings).
- Array membership: `d.tags CONTAINS 'erlang'` (infix) and
  `'erlang' IN d.tags` both test whether the list at `d.tags` contains
  the value; they are equivalent to `CONTAINS(d.tags, 'erlang')`. `IN`
  with a parenthesized list, `d.status IN ('a', 'b')`, is still
  scalar set membership.
- `$name` parameters bind scalars from the `params` map.
- `WHERE id = 'x'`, id ranges, and `id LIKE 'prefix%'` become
  primary-key scans.
- ORDER BY takes a single key; missing values sort after numbers and
  strings.

Equality, ranges, `LIKE 'prefix%'`, `IS NOT MISSING`, and their AND
combinations use the path indexes. `OR`, `IN`, `NOT`, regex-shaped
`LIKE`, `IS NULL`, `IS MISSING` and `CONTAINS` scan; `explain_query`
lists a `{full_scan, _}` warning for each:

```erlang
{ok, #{engine := #{strategy := Strategy}, warnings := Warnings}} =
    barrel:explain_query(Db, <<"SELECT * FROM db WHERE a = 1 OR b = 2">>).
```

### MISSING vs NULL

PartiQL distinguishes a stored `null` from an absent attribute:

- `a IS MISSING`: the attribute is absent.
- `a IS NULL`: the attribute is `null` OR absent.
- `a = NULL` never matches and is rejected; use the forms above.

## UNNEST arrays

`UNNEST` yields one row per array element; the element gets its own
alias, usable in WHERE, SELECT and ORDER BY. Empty, missing or
non-array values yield no rows.

```sql
SELECT d.title, t AS tag
FROM db AS d, UNNEST(d.tags) AS t
WHERE d.type = 'post' AND t = 'erlang'
```

Element predicates are evaluated per row; predicates on the document
itself still use the indexes.

## Search table functions

Vector and keyword search enter the language as FROM sources. The
alias is required; each function takes a query string (or `$param`)
and named options.

```sql
SELECT v._score, title FROM vector_top_k('rust orm', k => 10) AS v
SELECT m._score, title FROM bm25_top_k('rust', k => 10) AS m
SELECT h._score, title FROM hybrid_top_k('rust orm', k => 10) AS h
WHERE h.lang = 'en'
```

- `k` defaults to 10; `vector_top_k` also takes `ef_search`.
- `hybrid_top_k` fuses the vector and BM25 legs with RRF.
- Hits join back to their documents by id; hits whose document is gone
  are dropped. On record-mode databases the query text embeds through
  the database's embedder.
- `_score` is function specific (1 - distance, raw BM25, RRF fused)
  and not comparable across functions. `vector_top_k` rows also carry
  `_distance`. Both are SELECT and ORDER BY columns only.
- A WHERE filters the hits after retrieval: barrel over-fetches once
  (3x `k`, capped at 1000) and returns UP TO `k` rows; heavy filtering
  can return fewer. Default order is search rank.

## Live queries

Append `SUBSCRIBE` and subscribe instead of running: you get the
snapshot, then add/change/remove deltas as documents start or stop
matching.

```erlang
{ok, Sub} = barrel:subscribe_query(Db,
    <<"SELECT name FROM db WHERE status = 'active' SUBSCRIBE">>),
#{ref := Ref} = Sub,
receive {bql_rows, Ref, Rows} -> Rows end,
receive {bql_ready, Ref, #{count := N}} -> N end,
%% then, per matching write:
%% {bql_change, Ref, #{action := add | change, id, rev, row}}
%% {bql_change, Ref, #{action := remove, id}}
ok = barrel:unsubscribe_query(Sub).
```

- LIMIT caps the initial snapshot only; deltas are unbounded.
- Table functions, UNNEST, ORDER BY and OFFSET do not combine with
  SUBSCRIBE.
- The query stops when you unsubscribe or when the owner process dies.
  Remove detection rides the changes feed, so expect it within its
  poll interval (about 100 ms).

## Over HTTP

`POST /db/:db/query` takes the BQL text as the body (or JSON
`{"query", "params", "continuation"}`) and streams ndjson: one
`{"row": ...}` line per row, then one `{"meta": ...}` line with
`has_more` and a `continuation` token to POST back. Query errors are a
400 with `message`, `line` and `column`.

```sh
curl -s http://localhost:8080/db/mydb/query \
  -d "SELECT title FROM db WHERE type = 'post' LIMIT 3"
{"row":{"id":"post:1","title":"..."}}
{"row":{"id":"post:2","title":"..."}}
{"row":{"id":"post:3","title":"..."}}
{"meta":{"has_more":false}}
```

SUBSCRIBE statements need `Accept: text/event-stream` (or a browser
EventSource on `GET /db/:db/query?q=...`) and stream `row`, `ready`,
`change` and `error` events with a periodic `ping`.

## Notes

- v1 scope: SELECT, WHERE, ORDER BY (one key), LIMIT/OFFSET, UNNEST
  (one), the three table functions, SUBSCRIBE. No joins, no GROUP BY,
  no `[*]` wildcard paths (use UNNEST).
- ORDER BY, UNNEST and LIMIT/OFFSET materialize their (bounded) result
  before responding; plain filters stream.
- `SELECT` of narrow projections still reads full documents; the
  projection happens after fetch.
