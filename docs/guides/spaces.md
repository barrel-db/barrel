# Spaces, sessions, and handoffs

The agent layer: a space is a barrel database created through
`barrel_spaces`, shared by holding a capability token for it. Sessions give
agents a working context with a sliding TTL inside a space; a handoff
transfers work to another agent as a token whose possession is the right to
accept. Read this when you coordinate several agents over shared context.

## When to use it

- Several agents need to read and write the same context without copying it.
- You want revocable, per-space access instead of one global credential.
- Agents hand tasks to each other and the receiver must see the sender's
  context in place.

## Spaces and capability tokens

A space is a database with a generated name (`sp_` + 16 chars) plus a
registry document in the `_barrel_spaces` system database. Space databases
open through Barrel's database lifecycle manager, so idle spaces close themselves
and hundreds of ephemeral spaces stay cheap.

```erlang
{ok, _} = application:ensure_all_started(barrel_spaces),
{ok, #{id := SpaceId, db := Db}} = barrel_spaces:create_space(#{
    label => <<"joint research">>,
    session_ttl => 3600           %% default session TTL, seconds
}),
%% Db is a regular barrel handle: documents, search, timeline all work
{ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"notes">>}),

%% grant access: the token is shown once, only its hash is stored
{ok, Token, _Grant} = barrel_caps:grant(SpaceId, #{
    rights => [read, write],      %% read < write < admin
    subject => <<"agent-bob">>}),

%% the holder verifies (or resolves an auth context)
{ok, _} = barrel_caps:verify(Token, SpaceId, write),
{ok, #{space := SpaceId, rights := _}} = barrel_caps:auth_context(Token),

%% revoke one grant, or drop the space (revokes everything)
ok = barrel_caps:revoke(Token),
ok = barrel_spaces:drop_space(SpaceId).
```

`open_space/2` reopens an existing space; runtime options (an `encryption`
spec, extra store config) must be passed again on every open, exactly as for
any barrel database. Per-space encryption keys are the isolation story: see
[encryption](encryption.md).

## Sessions

A session is a document in the space database written with a TTL; every
mutation slides it. Messages are separate documents whose ids sort
chronologically. An expired session disappears from reads immediately and is
tombstoned by the space's TTL sweeper; a janitor then collects its orphaned
messages.

```erlang
{ok, Space} = barrel_spaces:open_space(SpaceId),
{ok, Sid} = barrel_session:create(Space, #{agent => <<"alice">>,
                                           ttl => 1800}),
{ok, _} = barrel_session:add_message(Space, Sid, #{
    role => <<"user">>, content => <<"draft is half done">>}),
{ok, Messages} = barrel_session:get_messages(Space, Sid,
                                             #{limit => 50, order => desc}),
{ok, _ExpiresAt} = barrel_session:touch(Space, Sid),

%% structured working state and pinned context
{ok, _} = barrel_session:set_data(Space, Sid, <<"cursor">>, 42),
{ok, PinId} = barrel_session:pin_context(Space, Sid,
                                         #{content => <<"key fact">>,
                                           priority => 0}),
{ok, Pinned} = barrel_session:list_pinned(Space, Sid),
ok = barrel_session:delete(Space, Sid).   %% cascades to messages
```

### Document TTL (the machinery underneath)

Sessions ride a general primitive that works on any barrel database: the
`expires_at` write option (unix ms). Expired documents read as not found
immediately; an opt-in per-database sweeper turns them into real tombstones
(so replication and branches stay correct).

```erlang
{ok, _} = barrel:put_doc(Db, Doc, #{expires_at => Now + 60000}),
%% absent = preserve the current expiry, 0 = clear it
{ok, Swept} = barrel_docdb:sweep_ttl(DbName).
```

Enable the sweeper with `ttl_sweep_interval` (ms) in the database's docdb
config; spaces set it to 60000 by default.

## Handoffs

A handoff is a shared space plus a capability. Creating one issues a grant
and returns the token once; whoever holds the token accepts. Accepting flips
the handoff document pending to accepted with a CAS (double accepts lose),
opens the space, and creates a session for the acceptor in it: context is
read in place, nothing is copied.

```erlang
{ok, #{handoff_id := Hid, token := HandoffToken}} =
    barrel_handoff:create(Space, #{
        task_name => <<"finish the draft">>,
        from_agent => <<"alice">>, to_agent => <<"bob">>,
        from_session => Sid,
        pending => [<<"polish conclusion">>]}),

%% bob, holding only the token:
{ok, #{handoff := H, space := BobSpace, session := BobSid}} =
    barrel_handoff:accept(HandoffToken, #{agent => <<"bob">>}),

%% work happens in the shared space, then:
{ok, _} = barrel_handoff:complete(HandoffToken,
                                  #{result => <<"shipped">>}),
%% completion revokes the token by default

%% or hand it on, keeping the lineage (parent/root/depth)
{ok, #{token := NextToken}} = barrel_handoff:chain(HandoffToken, #{
    task_name => <<"review the draft">>, to_agent => <<"carol">>}).
```

Handoff documents live in the registry database, so pending handoffs are
discoverable with `barrel_handoff:list/1` (filters: `space`, `status`,
`to_agent`, `from_agent`) or by subscribing to the registry's changes feed.

## Over REST and MCP

`barrel_server` exposes all of this under `/spaces` and `/handoffs`;
capability tokens work as bearers there (and only there). See
[rest-server](rest-server.md). The same operations exist as MCP tools
(`space_*`, `session_*`, `handoff_*`); see [mcp](mcp.md).

## Notes

- Rights ladder: `read < write < admin`. Admin covers granting, revoking,
  and handoff issuance. Verification is local (the registry is a database
  on the same node); tokens are random, stored hashed, compared in constant
  time.
- The registry (`_barrel_spaces`) holds space, grant, and handoff documents
  as regular docs: folds and the changes feed see them.
- An encrypted space needs its encryption spec on every open, including
  inside `barrel_handoff:accept/2` (pass `open_opts`). Over REST and MCP,
  spaces open with default options, so encrypted spaces are Erlang-API-only
  in v1.
- `barrel_spaces_janitor` sweeps orphaned session messages of open spaces
  periodically (`janitor_interval`, default 5 minutes);
  `barrel_spaces_janitor:sweep/0` runs one pass now.
