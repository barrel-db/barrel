# Audit and provenance

Every applied write leaves an entry in the retained history log, and a write
can carry provenance: who (actor), in what session, through which surface
(source). Together they answer "what did the agent know when" at the database
level. Read this when you need to attribute writes to agents or reconstruct
what a document looked like at a point in time.

## When to use it

- You run agents against a shared database and need to know which agent wrote
  what, and when.
- You need past versions of a document (bodies included) inside the retention
  window.
- You expose barrel over REST or MCP and want writes attributed without
  changing document bodies.

## Write with provenance

Pass `provenance` in the write options. Each value is a binary of at most
256 bytes; the encoded blob is capped at 1 KiB. Invalid provenance fails the
write with `{invalid_provenance, Reason}`.

```erlang
{ok, Db} = barrel:open(<<"mydb">>),
Prov = #{actor => <<"agent-9">>,
         session => <<"ses_abc">>,
         source => <<"planner">>},
{ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>, <<"v">> => 1},
                         #{provenance => Prov}),
{ok, _} = barrel:delete_doc(Db, <<"a">>, #{provenance => Prov}).
```

Over REST, set headers on any write; they apply batch-wide on `_bulk_docs`:

```console
$ curl -X PUT localhost:8080/db/mydb/doc/a \
    -H 'content-type: application/json' \
    -H 'x-barrel-actor: agent-9' \
    -H 'x-barrel-session: ses_abc' \
    -H 'x-barrel-source: planner' \
    -d '{"v":1}'
```

Writes over MCP carry provenance automatically: actor is the authenticated
subject, session is the MCP session (or the tool's `session` argument), source
is `mcp`. See the [MCP guide](mcp.md).

## Read the audit trail

The history log holds one entry per applied write (no bodies), ordered by HLC,
swept by retention. Superseded bodies are archived and readable by version
until swept.

```erlang
%% the whole retained trail, or a window
{ok, Entries} = barrel:history(Db),
{ok, Entries2} = barrel:history(Db, #{limit => 100}),
%% each entry: #{hlc, id, version, deleted, cause, provenance?}
%% cause: local | replicated | resolve; provenance only when the
%% write carried one

%% one document: current + archived versions, then a past body
{ok, Versions} = barrel:doc_versions(Db, <<"a">>),
[#{version := Rev} | _] = Versions,
{ok, OldBody} = barrel:version_body(Db, <<"a">>, Rev),

%% how far back the trail goes (undefined = nothing swept yet)
Floor = barrel:history_floor(Db).
```

`history/2` options: `from` and `to` (HLC cursors, decode with
`barrel:hlc_decode/1`), `limit`, and `id` (filters to one document; a
documented scan over the window).

## Over REST

```console
$ curl 'localhost:8080/db/mydb/_history?limit=50'
{"history":[{"id":"a","rev":"...","cause":"local","hlc":"...",
             "provenance":{"actor":"agent-9",...}}]}

$ curl 'localhost:8080/db/mydb/_history?since=<cursor>&until=<cursor>&id=a'
$ curl localhost:8080/db/mydb/doc/a/_versions
$ curl localhost:8080/db/mydb/doc/a/_versions/<rev>   # the archived body
```

`since`/`until` take the HLC cursors other endpoints return (a change's
`hlc`, a history entry's `hlc`). Bad cursors and limits answer 400.

## Notes

- Provenance is persisted twice: on the current winner (returned by
  `doc_versions` for the head) and in the history entry of every applied
  write (the durable audit record).
- A write without provenance clears the winner's provenance column: the
  current attribution never lies.
- Provenance does not travel on the replication wire in v1. Replicated
  arrivals carry `cause => replicated` and the origin database's identity;
  the origin's own trail holds the acting agent.
- Branches fork with the parent's history, so a branch carries the
  provenance of everything before the fork (see [timeline](timeline.md)).
- Retention sweeps history entries and archived bodies together
  (`retention_period`, default 30 days). `history_floor/1` tells you the
  oldest surviving point.
