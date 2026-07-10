# barrel_spaces

The barrel agent layer: spaces, capability tokens, sessions, handoffs.

A space is a barrel database created through this layer. Sharing context
means holding a capability for the space; every barrel feature
(documents, search, channels, timeline, per-database encryption, sync)
works inside one unchanged. Space metadata lives in the `_barrel_spaces`
registry database; space names are generated so labels never constrain
database naming.

You need this when several agents work on the same data and you want to hand
context between them without copying it, or hand out scoped, revocable access
to one space.

[Documentation](https://barrel-db.eu/docs/lib/spaces/) |
[HexDocs](https://hexdocs.pm/barrel_spaces) |
[Repository](https://github.com/barrel-db/barrel)

## Spaces

`create_space/1` returns a space handle, `#{id := SpaceId, db := Db}`. The
lifecycle calls take the id; everything else takes the handle.

```erlang
{ok, #{id := SpaceId, db := Db} = Space} =
    barrel_spaces:create_space(#{label => <<"experiment-42">>}),
{ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"note">>, <<"v">> => 1}),
ok = barrel_spaces:close_space(SpaceId),
{ok, Space} = barrel_spaces:open_space(SpaceId).
```

Space databases open through Barrel's database lifecycle manager
(`barrel_dbs`), so idle spaces close automatically and hundreds of
ephemeral spaces stay cheap. Per-space `encryption` specs give each
space its own key (agent isolation); like all barrel runtime config,
the spec must be passed again on every open.

## Capability tokens

A capability is an opaque random token verified against a grant document in the
registry. Only a SHA-256 of the token is stored, so nothing on disk can mint
access, and revoking a grant takes effect immediately.

```erlang
%% The token is returned once, here. Only its SHA-256 is stored.
{ok, Token, _Grant} = barrel_caps:grant(SpaceId, #{rights => [read, write],
                                                   subject => <<"agent-b">>}),

%% The bearer proves it holds `write' on this space.
{ok, _Ctx} = barrel_caps:verify(Token, SpaceId, write),

{ok, _Grants} = barrel_caps:list(SpaceId),
ok = barrel_caps:revoke(Token).
```

The token is shown once, when you mint it. `verify/3` fails closed: unknown,
tampered, revoked, expired, wrong-space, and under-privileged tokens all return
an error, and none of them echo token material. `barrel_server` uses the same
tokens to scope its HTTP routes.

## Sessions

A session is a TTL-bounded conversation inside a space: messages, arbitrary
key/value data, a summary, and pinned context.

```erlang
{ok, Sid} = barrel_session:create(Space, #{agent => <<"agent-a">>,
                                           ttl => 3600}),
{ok, _Rev} = barrel_session:add_message(Space, Sid,
                                        #{role => <<"user">>,
                                          content => <<"index the corpus">>}),
{ok, _Messages} = barrel_session:get_messages(Space, Sid),

{ok, _} = barrel_session:set_data(Space, Sid, <<"cursor">>, 42),
{ok, 42} = barrel_session:get_data(Space, Sid, <<"cursor">>),

{ok, _ExpiresAt} = barrel_session:touch(Space, Sid),
ok = barrel_session:delete(Space, Sid).
```

`ttl` defaults to the space's `session_ttl`, or one hour. `touch/2` extends it.
`set_summary/3`, `pin_context/3`, `unpin_context/3`, and `list_pinned/2` keep the
long-lived context a session should not lose when its messages are compacted.

## Handoffs

A handoff passes a task and its space from one agent to another. Creating one
mints a capability for the space, so the acceptor gets access by presenting the
token, not by being granted anything up front.

```erlang
{ok, #{handoff_id := _Hid, token := HandoffToken}} =
    barrel_handoff:create(Space, #{task_name => <<"reindex">>,
                                   from_agent => <<"agent-a">>,
                                   to_agent => <<"agent-b">>,
                                   context => <<"corpus is loaded">>,
                                   rights => [read, write]}),

%% On the other side, with only the token:
{ok, #{space := SharedSpace, session := Sid, handoff := _H}} =
    barrel_handoff:accept(HandoffToken, #{agent => <<"agent-b">>}),

%% complete/2 takes the token, not the handoff id, and revokes its grant.
{ok, _} = barrel_handoff:complete(HandoffToken,
                                  #{result => <<"12k docs indexed">>}).
```

`accept/2` opens the shared space and creates a fresh session in it; the
from-agent's context is read in place, never copied. `chain/2` walks a handoff
back through the agents that touched it.

A runnable version of this whole flow, compiled and executed by
`barrel_agent_example_SUITE`, lives at
[examples/agent_layer.erl](https://github.com/barrel-db/barrel/blob/main/examples/agent_layer.erl).

## Over HTTP

`barrel_server` exposes all of this at `/spaces`, `/spaces/:space/grants`,
`/spaces/:space/sessions`, and `/handoffs`, authenticated by the same capability
tokens. See its README.
