# MCP tool

`barrel_server` exposes barrel_ngram as the read-only `ngram_search` MCP tool, so an MCP
client (for example an LLM agent) can search a database's documents lexically. Use this
page when wiring an agent to the server, or to understand the tool's shape.

## The tool

`ngram_search` runs a substring or regex search over one database.

| Parameter | Type | Notes |
|-----------|------|-------|
| `db` | string (required) | the database to search |
| `query` | string (required) | the literal or regex |
| `mode` | string | `literal` (default) or `regex` |
| `limit` | integer | max hits returned (default 50, cap 1000) |

It is annotated `readOnlyHint`. The result is `#{count, hits => [#{id, spans}]}` where each
span is `[Start, Length]`; a malformed regex returns an error result.

## Example calls

```json
{ "name": "ngram_search",
  "arguments": { "db": "mydb", "query": "connect_timeout" } }
```

```json
{ "name": "ngram_search",
  "arguments": { "db": "mydb", "mode": "regex", "query": "connect_\\w+" } }
```

## Behaviour

- The corpus is opened lazily the first time you search a database, keyed by the database
  name, and kept live by its feed subscription. The first call also catches the index up to
  the current database state.
- Access is checked with the caller's read grant on the database, like the other read
  tools.
- Segments are stored under the server's data directory (`<data_dir>/ngram/<db>`).

## Notes

- The tool uses a dense, single-shard corpus per database. For sparse or sharded corpora,
  drive `barrel_ngram` directly from Erlang (see [getting started](getting-started.md)).
