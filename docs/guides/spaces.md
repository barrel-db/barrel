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
any barrel database.

### A space in record mode

Give `create_space/1` an `embedding` policy and the space indexes the fields
you name, like any record-mode database (see [embedding](embedding.md)):

```erlang
Embedder = {ollama, #{model => <<"nomic-embed-text">>}},
{ok, #{id := SpaceId}} = barrel_spaces:create_space(#{
    embedding => #{fields => [<<"text">>], mode => sync,
                   embedder => Embedder}}).
```

The space document records the policy's fields and the embedder's identity
(provider, model, dimensions, distance, fingerprint), not the embedder config:
the registry replicates, and a config can hold paths and API keys. Each node
that opens the space supplies its own config for the same model:

```erlang
application:set_env(barrel_spaces, embedder, Embedder),
{ok, #{db := Db}} = barrel_spaces:open_space(SpaceId).
```

The open checks the fingerprint first. Another model answers
`{error, {embedder_mismatch, _}}`; no `embedder` env answers
`{error, {embedder_required, Fingerprint}}`. A space reached through
barrel_server (its `/db` and sync routes) opens the same way, so the first
route to touch it does not decide its mode. Per-space encryption keys are the isolation story: see
[encryption](encryption.md).

## Sessions

A session is a header document in the space database written with a TTL;
every mutation slides it. Each data key, pin, the summary and each replica's
counters are documents of their own, and messages are documents whose ids
sort chronologically. An expired session disappears from reads immediately
and its header is tombstoned by the space's TTL sweeper; a janitor then
collects its orphaned documents.

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
{ok, _} = barrel_session:delete_data(Space, Sid, <<"cursor">>),
{ok, PinId} = barrel_session:pin_context(Space, Sid,
                                         #{content => <<"key fact">>,
                                           priority => 0}),
{ok, Pinned} = barrel_session:list_pinned(Space, Sid),
{ok, _} = barrel_session:unpin_context(Space, Sid, PinId),

%% counters: no increment is lost when two nodes count at once
{ok, 21} = barrel_session:incr_counter(Space, Sid, <<"messages">>, 1),
{ok, #{<<"messages">> := 21}} = barrel_session:get_counters(Space, Sid),
ok = barrel_session:delete(Space, Sid).   %% cascades to every document
```

A counter only grows or shrinks by the amounts you add. To count "messages
since the last extraction", keep the message counter and store its total at
the last extraction as a data key: the difference is the count since.

### Durable sessions

Pass `ttl => infinity` (or 0) to create a session that never expires: a
record, not a cache. Mutations on it return `{ok, 0}` instead of an expiry
time, and the TTL sweeper skips it.

```erlang
{ok, Rec} = barrel_session:create(Space, #{agent => <<"alice">>,
                                           ttl => infinity}),
{ok, 0} = barrel_session:touch(Space, Rec).
```

### Filtered listing

`list/2` filters through the space database's path indexes, so a space
holding thousands of sessions does not pay a full fold:

```erlang
{ok, Mine} = barrel_session:list(Space, #{
    match => #{<<"data.user_id">> => <<"u1">>},   %% any field path
    agent => <<"alice">>,                         %% composes with match
    limit => 50}).
```

### Importing an existing corpus

Sessions and messages move in with their own ids and timestamps; the
module keeps owning the document schema:

```erlang
{ok, _} = barrel_session:create(Space, #{id => <<"legacy-1">>}),
{ok, _} = barrel_session:import_session(Space, #{
    id => <<"legacy-2">>, agent => <<"old-bot">>,
    data => #{<<"user_id">> => <<"u1">>},
    created_at => 1750000000000, updated_at => 1750000100000}),
    %% ttl defaults to never for imports; ttl > 0 arms from updated_at
{ok, _} = barrel_session:import_message(Space, <<"legacy-2">>, #{
    role => <<"user">>, content => <<"hello">>,
    ts => 1750000050000, id => <<"msg-legacy-1">>}),   %% no TTL slide
%% pins keep their id, timestamp, and metadata too
{ok, _} = barrel_session:pin_context(Space, <<"legacy-2">>, #{
    id => <<"pin-legacy-1">>, content => <<"key fact">>,
    pinned_at => 1750000060000, metadata => #{<<"src">> => <<"v1">>}}).
```

Duplicate caller ids fail with `{error, conflict}` (a session id on
create, a message id on import, a pin id on pin), never silently.

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
and returns the token once; whoever holds the token accepts. Accepting
records the acceptor (a second accept is refused), opens the space, and
creates a session for the acceptor in it: context is read in place, nothing
is copied.

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

%% or accept on the token discipline alone (no space opened, no
%% session created), for consumers with their own session model:
%% {ok, #{handoff := H2}} =
%%     barrel_handoff:accept(HandoffToken, #{agent => <<"bob">>,
%%                                           session => false}),

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
Each step is a document written once: the handoff at create, one accept per
agent, the completion. The status is the furthest step present.

## Replicating a space and its registry

Replicate the registry and a space database between nodes like any barrel
database. Every grant, session and handoff fact is a document written once
or replaced as a whole, so the version every node shows is already the
answer. barrel_spaces opens the registry and its spaces with
`barrel_spaces_resolver`, which closes each conflict on those documents by
choosing that version, the same way on every node:

- A revocation is a `revoke:` document of its own. A grant edited elsewhere
  cannot undo it, and two nodes revoking the same token agree.
- Two nodes writing different keys, pins or the summary of one session keep
  both writes. The same key written on two nodes keeps the later write.
- Removing a data key or a pin replicates as a delete: a node that still
  holds the old copy cannot bring it back.
- A handoff never goes back from completed to accepted, or from accepted to
  pending. When two agents accept it on nodes that cannot see each other,
  both accepts are kept and every node names the same owner: the earliest
  `accepted_at`, then the agent name.
- Counters are per replica and summed, so no increment is lost.

```erlang
%% node A
ok = barrel_caps:revoke(TokenId, #{by => <<"ops">>}),

%% node B, after replication
{error, revoked} = barrel_caps:verify(Token, SpaceId, read),
{ok, [#{<<"revoked_by">> := <<"ops">>}]} = barrel_caps:list(SpaceId).
```

### Your own conflict resolver

A space database also holds your documents. Pass your resolver as
`docdb => #{conflict_resolver => {M, F}}` to `create_space/1` or
`open_space/2`: barrel_spaces wraps it, handles the `session:` documents and
hands every other document to yours. If you open the registry or a space
database yourself, set `{barrel_spaces_resolver, registry}` on the registry,
and `barrel_spaces_resolver:with({M, F})` (or `{barrel_spaces_resolver,
space}`) on a space. As with any resolver, every node must run the same one.

### Upgrading from 1.x

2.0 stores grants, sessions and handoffs differently, and needs barrel_docdb
3.1. Nodes running both versions do not agree, so upgrade the whole mesh in
this order:

1. Upgrade barrel_docdb to 3.1 on every node. You can do it one node at a
   time: the replication protocol is unchanged, and a 3.1 node refuses the
   mixed-up versions a 3.0 node still sends. Each database converts its
   version history the first time 3.1 opens it, in one pass, and logs how
   many rows it moved and how many documents it repaired.
2. Stop writing through barrel_spaces, or stop replicating the registry and
   the space databases.
3. Upgrade barrel_spaces to 2.0 (and barrel_server to 1.15.1) on every node.
4. On one node, run `barrel_spaces:migrate/0`. It converts the registry and
   every space that opens without runtime options, and lists the others in
   `skipped`. Open each skipped space (an encrypted one with its key) and
   run `barrel_session:migrate/1`.
5. Resume replication. The other nodes receive the converted documents.
6. Resume writes.

```erlang
{ok, #{grants := G, handoffs := H, sessions := S, skipped := Skipped}} =
    barrel_spaces:migrate(),
[begin
     {ok, Space} = barrel_spaces:open_space(Id, #{encryption => Spec}),
     {ok, _} = barrel_session:migrate(Space)
 end || Id <- Skipped].
```

Documents you do not convert still read the same, and convert on their next
write.

What goes wrong if you skip step 2 and a 1.x node keeps writing:

| On a 1.x node | Effect |
|---|---|
| Verifying a token | It ignores `revoke:` documents and accepts a token revoked on a 2.0 node. |
| Reading a session | It finds no data, pins or summary written by 2.0. |
| `set_data/4` on an existing key | 2.0 keeps showing its own value; only new keys appear. |
| `unpin_context/3` | The pin comes back on 2.0 nodes. |
| Accepting a handoff | A handoff accepted on 2.0 looks pending, so a second accept succeeds and the owner may change. |

A 1.x revocation still holds on 2.0 nodes, which read both layouts.

There is no way back: 1.x cannot read converted sessions, and a database
opened by barrel_docdb 3.1 loses its conflicts and past versions under 3.0.

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
- The registry database name comes from the `registry_db` app env of
  `barrel_spaces` (default `_barrel_spaces`); set it before first use. The
  registry is a regular barrel database: opening it through `barrel_dbs`
  alongside this layer is supported.
- Replicating the registry is intended. Grant documents are authorization
  state, so replication is what makes a token minted on one node verify on
  another; the flip side is that revocation propagates at replication
  speed. Keep the registry's replication tight if revocation latency
  matters to you.
- A space's vector store is per node. It lives at
  `<data_dir>/<space id>_vec`, resolved from the local `barrel_docdb`
  `data_dir` on every open, so a replicated space doc opens on any node.
  Replicating the registry carries metadata and grants, not memory: the
  vectors stay on the node that wrote them. A custom
  `vectordb => #{db_path => ...}` given at create is kept as is and must
  exist on every node that opens the space.
- `barrel_caps:grant/2` does not require the space to exist. Granting on a
  purely logical scope (a tenant id, a handoff id) is supported: revocable,
  hash-at-rest, rights-laddered tokens with no space database behind them.
- The registry (`_barrel_spaces`) holds space, grant, revocation and
  handoff documents as regular docs: folds and the changes feed see them.
- An encrypted space needs its encryption spec on every open, including
  inside `barrel_handoff:accept/2` (pass `open_opts`). Over REST and MCP,
  spaces open with default options, so encrypted spaces are Erlang-API-only
  in v1.
- `barrel_spaces_janitor` sweeps orphaned session documents of open spaces
  periodically (`janitor_interval`, default 5 minutes);
  `barrel_spaces_janitor:sweep/0` runs one pass now.
