# Examples

Cross-application examples for the Barrel umbrella.

- [agent_layer.erl](agent_layer.erl): spaces, capability tokens, sessions, and
  a handoff between two agents. Every example here is compiled and run by a test
  suite, so it cannot drift from the API.

Run one from the umbrella root:

```console
$ rebar3 shell
1> c("examples/agent_layer.erl").
2> agent_layer:run().
ok
```

Each application also ships its own examples and getting-started guides:

- `barrel_embed`: [apps/barrel_embed/examples/basic_usage.erl](../apps/barrel_embed/examples/basic_usage.erl)
- `barrel_vectordb`: [apps/barrel_vectordb/docs/getting-started.md](../apps/barrel_vectordb/docs/getting-started.md)
- `barrel_docdb`: [apps/barrel_docdb/docs/getting-started.md](../apps/barrel_docdb/docs/getting-started.md)
- `barrel_faiss`: [apps/barrel_faiss/guides/getting-started.md](../apps/barrel_faiss/guides/getting-started.md)
