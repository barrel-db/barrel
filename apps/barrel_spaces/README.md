# barrel_spaces

The barrel agent layer: spaces, capability tokens, sessions, handoffs.

A space is a barrel database created through this layer. Sharing context
means holding a capability for the space; every barrel feature
(documents, search, channels, timeline, per-database encryption, sync)
works inside one unchanged. Space metadata lives in the `_barrel_spaces`
registry database; space names are generated so labels never constrain
database naming.

```erlang
{ok, #{id := Space, db := Db}} =
    barrel_spaces:create_space(#{label => <<"experiment-42">>}),
{ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"note">>, <<"v">> => 1}),
ok = barrel_spaces:close_space(Space),
{ok, _} = barrel_spaces:open_space(Space).
```

Space databases open through the facade lifecycle manager
(`barrel_dbs`), so idle spaces close automatically and hundreds of
ephemeral spaces stay cheap. Per-space `encryption` specs give each
space its own key (agent isolation); like all barrel runtime config,
the spec must be passed again on every open.

Capability tokens (`barrel_caps`), sessions with TTL (`barrel_session`),
and handoffs (`barrel_handoff`) build on spaces; see the spaces guide in
the umbrella docs.
