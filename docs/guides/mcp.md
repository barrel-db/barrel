# MCP endpoint

`barrel_server` mounts an MCP endpoint (Streamable HTTP) at `/mcp`: tools for
databases, documents, BQL, search, the timeline, and the agent layer, plus
resources with live-query subscriptions. Read this when an MCP client (an
agent runtime, an IDE, `barrel_mcp_client`) should talk to barrel directly.

## When to use it

- Agents reach barrel through an MCP runtime instead of REST.
- You want live queries pushed to clients as resource-updated notifications.
- You hand agents capability tokens scoped to one space and want the same
  scoping on the MCP surface.

## Enable and configure

The endpoint is on by default in the server build. Configure it with the
`{barrel_server, mcp, #{...}}` app env:

```erlang
{barrel_server, [
    {mcp, #{
        enabled => true,               %% default
        allowed_origins => any,        %% tighten for browser clients
        allow_missing_origin => true,
        resources => full,             %% full | live_only
        live => #{max_per_session => 32, max_global => 1024,
                  sweep_interval_ms => 60000, debounce_ms => 100}
    }}
]}
```

## Auth

`/mcp` authenticates through its own provider, which accepts two bearers:

- a server token (the same `{barrel_server, auth, #{tokens => [...]}}` set
  the REST middleware checks): full access;
- a capability token (`bsp_...`, issued by `barrel_caps`): scoped to its
  space and rights; every tool checks the database it touches against the
  grant. Denials come back as tool errors the agent can read, not protocol
  failures.

With no auth configured the endpoint is open, like the rest of the server.

## Tools

Core (rights a capability needs in parens):

```
db_create (write)      db_list (read)          db_info (read)
doc_get (read)         doc_put (write)         doc_delete (write)
query (read)           search (read)           changes (read)
branch_create (admin)  branch_list (read)      merge (admin)
query_subscribe (write)                        query_unsubscribe
```

Agent layer:

```
space_create (management only)   space_info (read)
space_grant (admin)              space_revoke (admin)
session_create (write)           session_touch (write)
session_add_message (write)      session_get_messages (read)
handoff_create (admin)           handoff_list (read)
handoff_accept (token in args)   handoff_complete (token in args)
```

`query` compiles the statement first (parse errors are readable), bounds
rows with `max_rows` (default 100, cap 1000), and pages with a
`continuation`. SUBSCRIBE statements are rejected toward `query_subscribe`.
Every write carries provenance: actor = authenticated subject, session =
MCP session (or the tool's `session` argument), source = `mcp`. See
[audit-provenance](audit-provenance.md).

## Resources and live queries

Three templates:

```
barrel://db/{db}              database info
barrel://db/{db}/doc/{id}     a document body
barrel://db/{db}/live/{sub}   the materialized snapshot of a live query
```

`query_subscribe` starts a live query and returns its resource URI. Read the
URI for the current rows (`#{ready, count, rows}`, id-sorted); subscribe to
it (`resources/subscribe`) to get a `notifications/resources/updated` on
every change, debounced, delivered over the client's GET SSE stream. The
client re-reads on notification.

```erlang
{ok, C} = barrel_mcp_client:start(#{
    transport => {http, <<"http://localhost:8080/mcp">>},
    auth => {bearer, Token}}),
{ok, R} = barrel_mcp_client:call_tool(C, <<"query_subscribe">>, #{
    <<"db">> => <<"mydb">>,
    <<"query">> => <<"SELECT * FROM db WHERE kind = 'task' SUBSCRIBE">>}),
%% R's content carries {"sub": SubId, "uri": Uri}
{ok, _} = barrel_mcp_client:subscribe(C, Uri),
receive {mcp_resource_updated, Uri, _} -> refetch end,
{ok, Snapshot} = barrel_mcp_client:read_resource(C, Uri).
```

## Notes

- Resource reads carry no auth context in the MCP framework (arity-1
  handlers), so the db and doc templates answer any authenticated caller.
  Set `resources => live_only` when you hand out capability tokens: live
  URIs embed 16 random bytes, possession is the capability.
- The live bridge owns every subscription: databases with active live
  queries are pinned open, orphaned subscriptions (their MCP session
  expired) are swept, and per-session/global caps bound runaway agents.
- Rows of a live snapshot are id-sorted; ORDER BY is not maintained across
  deltas.
- For a capability principal, the database a tool touches must be the
  granted space itself; branch databases of a space are not reachable with
  a capability in v1.
- A stdio entry point (for local MCP hosts) is not wired; the engine
  supports it if you need one.
