# Contexts: portable datasets as the scale-out model

Status: proposal (2026-08-23). Nothing in this document is implemented; it
records the architecture, the API shapes, the risks, and the recommended
phasing for a contexts layer on top of the current codebase. Read
[vision.md](vision.md) first; this proposal extends it and does not replace
any shipped decision.

## The idea

Shards are implementation details: pieces of one large logical database,
hidden behind a routing layer that reconstructs the whole view. Barrel takes
the opposite bet. Local storage got big (tens of TB on one NVMe), so
datasets can stay bounded, and a bounded dataset can be a first-class unit:
moved, replicated, branched, and composed with other datasets. A **context**
is such a dataset made visible to agents: something an agent can discover,
inspect, attach to its session, query together with other contexts, and
detach.

The loop is a filesystem mount, not a query router:

    discover -> inspect -> attach -> query -> detach

The guiding constraint:

    A barrel should be small enough to move,
    large enough to matter,
    and independent enough to use alone.

A shard is system-owned, meaningless alone, and invisible to the
application. A context is domain-owned, useful alone, and chosen by the
agent. Composition replaces routing: the attached set of a session is the
query scope, and the agent decides what belongs in it.

This is not a proposal to build new machinery so much as to name what
barrel already is. The engine is already one database per bounded thing
(database-per-agent, spaces, branches), already replicates per database
with causal versions, already forks in O(1), and already searches each
database with vectors, BM25, and hybrid fusion. What is missing is the
layer that lets an agent see the datasets, reason about them, and query
several at once. That layer is small.

## What exists today (the load-bearing facts)

Every claim below was verified against the current code.

- One compiled BQL plan runs against any database. `FROM` is an alias that
  the executor discards; `barrel_bql:compile/1` returns a reusable plan and
  `barrel_bql_query:run(Db, Plan, Opts)` takes the target database as an
  argument. Federated query is therefore compile-once, run-N-times, merge.
- Live query messages (`{bql_rows | bql_change | bql_error, Ref, _}`) are
  tagged per subscription, so one process can multiplex subscriptions over
  many databases.
- Hybrid search fuses BM25 and vector legs with rank-based RRF inside each
  store (`barrel_vectordb_server`). Rank-based fusion is the only fusion
  that survives crossing databases with different embedders.
- `barrel_rerank` (cross-encoder sidecar) exists and nothing calls it. It
  is the model-agnostic comparator a cross-context result list needs.
- `barrel_dbs` opens databases lazily, coalesces concurrent opens, closes
  idle ones, bounds the open set with LRU, and supports pinning. Pinning is
  exactly an attachment lease.
- Spaces, capabilities, sessions, and handoffs exist. A handoff is already
  attach-by-reference: a space id plus a capability whose possession is the
  right to accept. Session documents carry free-form `data` and pins.
- Replication is per-database with checkpoints, continuous tasks, channels,
  and attachment sync, over local or HTTP transports. Timeline gives O(1)
  branches and merge-as-sync.
- Gaps this proposal has to close: there is no durable enumeration of
  databases (`list_dbs/0` sees open ones only), no dataset metadata
  anywhere (a space has only `label` and `purpose`), no export/import, no
  cross-database query surface, and `barrel:embedder_info/1` does not
  expose the embedding model id. One performance gap matters for attach:
  the vector store rebuilds its HNSW index from the vectors column family
  on every open, even when index metadata exists
  (`barrel_vectordb_server:load_or_create_index/5`); the fast
  serialization path in `barrel_vectordb_hnsw` is not wired in.

## Conceptual model

A **context** is one barrel database plus its catalog card. Kinds:

| kind | backing | example |
|---|---|---|
| `db` | a plain barrel database | `docs/python` |
| `space` | a space (agent workspace) | a shared task space |
| `branch` | a timeline branch | `projects/gunicorn@experiment` |
| `catalog` | another catalog (federation, one level up) | a region's catalog |

Contexts are not spaces. A space is an agent workspace with sessions, TTLs,
and handoffs; a context is a dataset descriptor. Every space gets a card
(projected from the space doc), but most contexts are not spaces. The
`_barrel_spaces` registry stays authoritative for space lifecycle.

Naming: cards carry a human path-like `name` (`projects/gunicorn`) distinct
from the physical database name; the card id is
`ctx:<node_id>:<db_name>`, where `node_id` comes from `_barrel_system`.
Ids are globally unique by construction, so catalogs from many nodes merge
without collisions.

## The catalog

A new database `_barrel_catalog`, managed by `barrel_catalog` (in
`apps/barrel`), opened in record mode so cards are searchable with hybrid
retrieval. One card per database.

### Card schema

```erlang
#{<<"id">> => <<"ctx:node-abc:gunicorn_src">>,
  <<"kind">> => <<"db">>,             %% db | space | branch | catalog
  <<"name">> => <<"projects/gunicorn">>,
  <<"db">> => <<"gunicorn_src">>,     %% physical database name
  <<"origin">> => <<"node-abc">>,     %% authoring node id

  %% authored (requires a capability over the target database)
  <<"title">> => <<"Gunicorn source">>,
  <<"description">> => <<"Source tree and commit history of gunicorn ...">>,
  <<"description_source">> => <<"authored">>,   %% authored | generated
  <<"topics">> => [<<"python">>, <<"http">>, <<"wsgi">>],
  <<"registered_by">> => <<"agent:indexer-7">>,
  <<"relationships">> => [
      #{<<"type">> => <<"documentation-for">>,
        <<"target">> => <<"ctx:node-abc:gunicorn_docs">>,
        <<"note">> => <<"rendered docs for this source">>}],

  %% computed (refresher-owned, agents cannot write these)
  <<"stats">> => #{<<"doc_count">> => 12400,       %% approximate
                   <<"storage_bytes">> => 480000000,
                   <<"vector_count">> => 11800,
                   <<"last_hlc">> => <<"...">>,     %% freshness, from the feed
                   <<"computed_at">> => 1787000000000},
  <<"schema_hints">> => #{<<"fields">> => [<<"path">>, <<"lang">>, <<"body">>],
                          <<"content_types">> => [<<"source_file">>]},
  <<"indexes">> => #{<<"vector">> => true, <<"bm25">> => true,
                     <<"ngram">> => false,
                     <<"embedding">> => #{<<"provider">> => <<"fastembed">>,
                                          <<"model">> => <<"bge-small-en">>,
                                          <<"dimensions">> => 384}},
  <<"placement">> => #{<<"role">> => <<"primary">>,   %% primary|replica|cached
                       <<"locations">> => [#{<<"node">> => <<"node-abc">>}],
                       <<"reachable">> => true},
  <<"permissions">> => #{<<"public">> => <<"read">>}, %% none | read
  <<"encrypted">> => false,

  %% derived: the text that gets embedded and BM25-indexed
  <<"search_text">> => <<"projects/gunicorn Gunicorn source ...">>}
```

Authored fields belong to whoever holds a capability on the target
database, never to arbitrary catalog writers: a card is only as
trustworthy as the right to the data it describes, and this rule is what
keeps a well-written card from steering agents into a database its author
has no rights over. Computed fields belong to the refresher and record
their own `computed_at`; agents must treat them as approximate
(`doc_count` is a RocksDB estimate already).

### Freshness without a thousand subscriptions

The refresher does not subscribe to every database's changes feed. Computed
fields update at three moments:

1. when `barrel_dbs` closes an idle database (the natural settle point; the
   close-hook seam is shared with the placement layer);
2. lazily on `context_inspect`, when `computed_at` is older than a staleness
   budget (default 5 minutes) and the database is already open;
3. a slow background sweep over open databases.

Cards are advisory. Truth is established at attach and query time; a card's
job is to let the agent decide whether attaching is worth it, without
opening anything.

### Enrollment and the missing enumeration

`barrel_catalog:enroll/2` runs on create and branch; a reconcile sweep scans
data directories and `_barrel_system` for databases that predate the
catalog. This makes the catalog the durable enumeration barrel currently
lacks, which is why it is phase 0: today a closed database is invisible to
every listing API, and an unregistered deletion would leave a phantom card
with nothing to reconcile against.

## Semantic discovery

Discovery is hybrid search over cards, never over member documents:

```erlang
{ok, Cards} = barrel_catalog:search(<<"Gunicorn HTTP/2 implementation">>,
                                    #{limit => 8}).
```

is `barrel:search_hybrid/3` on `_barrel_catalog` (BM25 + vector, in-store
RRF). `search_text` concatenates name, title, description, topics,
relationship notes, and sampled field names. An agent asking "which
datasets can help with this task" pays one search over a small database,
whatever the fleet holds.

Cold start: a card enrolled without a description gets a generated
`search_text` from the database name split on separators, sampled field
names from about 50 documents, and top BM25 terms when a BM25 index exists,
with `description_source => <<"generated">>`. The `context_describe` tool
lets an agent that used a dataset write the real description. Descriptions
improve with use, which is the right incentive loop; the
`description_source` flag keeps generated text from masquerading as
curation.

## Agent-facing API

### Erlang

```erlang
%% catalog
barrel_catalog:search(Query, Opts)      -> {ok, [Card]}.
barrel_catalog:get(CtxId)               -> {ok, Card} | {error, not_found}.
barrel_catalog:enroll(DbName, Card0)    -> {ok, CtxId}.
barrel_catalog:update_card(CtxId, Patch, Cap) -> ok | {error, _}.
barrel_catalog:sync()                   -> ok.   %% reconcile sweep

%% session-scoped composition
barrel_ctx:attach(Session, CtxId, Opts) -> {ok, Attachment} | {error, _}.
    %% Opts :: #{mode => auto | local | replica | cached,
    %%           cap => Token, ttl => Secs}
barrel_ctx:detach(Session, CtxId)       -> ok.
barrel_ctx:list_attached(Session)       -> {ok, [Attachment]}.
barrel_ctx:query(Session, Bql, Opts)    ->
    {ok, #{rows := [Row], errors := [#{context := CtxId, reason := term()}],
           complete := boolean()}}.
    %% Opts :: #{contexts => [CtxId], limit, timeout_per_ctx => 5000,
    %%           max_parallel => 8, fusion => rrf | none,
    %%           rerank => false | #{top_k => K}, dedup => false | by_id}
barrel_ctx:subscribe(Session, Bql, Opts) -> {ok, FedRef}.
    %% owner receives {ctx_rows | ctx_change | ctx_error, FedRef, CtxId, _}

%% portability
barrel_ctx_export:export(DbName, Dest, Opts) -> {ok, Manifest}.
barrel_ctx_export:import(Src, Opts)          -> {ok, DbName}.
```

### REST (`barrel_server`)

```
GET  /contexts                    list (paginated)
POST /contexts/_search            {"query": "...", "limit": 8}
GET  /contexts/:id                the card
POST /contexts/:id/_describe      authored fields (capability required)
POST /sessions/:sid/contexts      attach {"context": "...", "mode": "auto"}
DELETE /sessions/:sid/contexts/:id  detach
POST /contexts/_query             {"bql": "...", "contexts": [...], ...}
```

### MCP tools (same registry as the existing `db_*` and `space_*` tools)

| tool | args | effect |
|---|---|---|
| `context_search` | query, limit | hybrid search over cards |
| `context_list` | filter? | list cards (attached first) |
| `context_inspect` | context | full card, refreshed if stale and cheap |
| `context_describe` | context, description, topics, relationships | author the card |
| `context_attach` | context, mode?, capability? | attach to the MCP session |
| `context_detach` | context | detach |
| `context_query` | bql, contexts?, fusion?, rerank? | federated query |
| `context_subscribe` | bql, contexts? | live federated query, bridged to an MCP resource |

Cards also surface as MCP resources (`barrel://ctx/{id}`), so a client that
prefers resources over tools can browse the catalog natively.

### The loop, concretely

```
contexts.search("Gunicorn HTTP/2 implementation")
  -> projects/gunicorn      (source, 12k docs, fresh 2h ago)
     docs/python            (reference docs, public read)
     protocols/http2        (RFC corpus + notes)
     discussions/gunicorn   (mailing list, stale 40d)

context_attach("projects/gunicorn")
context_attach("protocols/http2")

context_query("SELECT h.id, h._score, h.path FROM
               hybrid_top_k('h2 frame parsing', k => 12) AS h")
  -> rows from both contexts, RRF-fused, each tagged _ctx

context_detach("protocols/http2")
```

## Attach: what it actually does

Attach is metadata plus placement. It never copies documents into the
agent's context window; it changes what the session's queries can see.

1. **Authorize.** `barrel_caps:verify(Token, Db, read)` unless the card says
   `public => read`. The grant's new `attach` scope (below) is checked.
2. **Place.** `barrel_ctx_placement:resolve(Card, Mode)` decides how the
   database becomes queryable here. `local`: it already is. `replica`:
   create or adopt a continuous pull task (`barrel_rep_tasks`) from the
   card's location. `cached`: restore the latest exported checkpoint from
   object storage and open it (readable immediately, refreshed by pull).
   `remote` is deferred: barrel has no BQL-over-HTTP execution wire, so
   until one exists `remote` resolves to `replica` or `cached`. The agent
   never needs to know which happened; `auto` picks by card placement,
   size, and local pressure.
3. **Mount.** `barrel_dbs:ensure/1` then `barrel_dbs:pin/1`. The pin is the
   attachment lease: it holds the database against LRU eviction while any
   session is attached.
4. **Record.** The attachment lands in the session document's `data`
   (`attachments => #{CtxId => #{mode, cap_id, attached_at}}`), so it
   expires with the session's sliding TTL and survives a reconnect. The
   spaces janitor learns one more duty: unpin attachments of expired
   sessions.

Detach removes the session entry and unpins when a per-node refcount (ETS
in `barrel_ctx`) drops to zero. A replica created for an attach stays warm
by default; the placement layer garbage-collects cold replicas on its own
schedule.

Attach has a budget. Pinned attachments per node are capped
(`{error, attach_capacity}`), and sessions get a soft attach limit, because
forty attached contexts means forty-way query fanout and a result set no
model window wants.

## Federated query

`barrel_ctx:query/3` compiles the BQL once and scatters it:

- bounded workers (default 8) each run the compiled plan against one
  attached database with the limit pushed down;
- a per-context deadline (default 5 s) turns stragglers into `errors`
  entries instead of stalling the answer; `complete => false` says the
  result is partial;
- every row gains `_ctx` (and keeps per-context `_score`), so provenance
  survives the merge;
- `ORDER BY` re-applies the plan's comparator over the union; order keys
  missing from the projection are added before scatter and stripped after;
- pagination is a cursor vector (one continuation per context) carried in
  session data, with the involved databases pinned for the pagination's
  lifetime.

**Fusion.** Raw vector scores are not comparable across contexts built with
different embedding models. The default is therefore rank-based RRF across
contexts (`rrf_k` 60, matching the in-store constant), which needs only
each context's ranking. When quality matters more than latency,
`rerank => #{top_k => K}` sends the fused candidates' text through
`barrel_rerank`: a cross-encoder scores query-document pairs directly, so
it is the one comparator that does not care which embedder produced the
candidates. This gives the currently unused rerank app its role in the
architecture.

**Dedup.** The same document can legitimately live in two attached contexts
(one replicated from the other). `dedup => by_id` keeps the copy with the
highest HLC when the version vectors are comparable; independently authored
documents that merely share an id are concurrent, stay distinct, and are
disambiguated by `_ctx`. Dedup is opt-in because identity across datasets
is a schema-level commitment, not something the engine can guess.

**Consistency.** There is no snapshot across databases. Each per-context
run reads its own RocksDB snapshot at its own instant; the merged result is
not a consistent cut, and the API says so: every result carries per-context
read HLCs. HLCs let an agent detect skew after the fact; nothing prevents
it. Cross-context atomicity is explicitly out of scope (see limits).

**Live.** `barrel_ctx:subscribe/3` opens one `subscribe_query` per context
and multiplexes the already-Ref-tagged messages into
`{ctx_rows | ctx_change | ctx_error, FedRef, CtxId, _}`. Each context's
stream is ordered; the merge is not, and no cross-context ordering is
promised.

## Permissions

The capability model extends, it does not change. Grants gain a `scopes`
list next to the `read < write < admin` ladder (the `auth_context/1` return
already has a `scopes` field waiting for this):

| scope | allows |
|---|---|
| `search` | catalog search and in-context search/query, no raw doc reads |
| `subscribe` | live queries |
| `attach` | attaching the context to a session |
| `branch` | creating branches of the context |
| `replicate` | pulling the context to another node (replica/cached attach) |

`read` implies `search` and `subscribe` for compatibility. Vector-only
search is `search` on a card whose database exposes only an embedding
index; there is no separate vector scope.

One grant per context, never multi-context tokens: revocation stays
per-dataset, and the session already holds the set. Handoffs compose
naturally: the handoff payload gains
`contexts => [#{context, capability}]`, so accepting a handoff attaches the
predecessor's working set in one step.

## Relationships

Cards carry typed edges:

    related | parent | derived-from | documentation-for |
    source-for | history | releases

Branch enrollment writes `derived-from` automatically; export/import writes
`derived-from` toward the source context. Navigation is card reads (an
agent follows `documentation-for` from source to docs), and
`context_search` can boost candidates connected to already-attached
contexts. Relationships are authored metadata with the same
capability-on-target rule as descriptions; they are hints for navigation,
not referential constraints the engine enforces.

## Placement: below the abstraction

Placement is a separate design (the big-barrel and read-replica
exploration) that this proposal treats as its lower layer:

- checkpoint upload and restore of whole databases to object storage, with
  epoch and generation manifests written with S3 conditional puts;
- per-database primary/replica roles with continuous pull replication;
- leases via object-store compare-and-swap for a unique writer.

The contexts layer consumes exactly three verbs from it: "make this
database queryable here" (attach modes), "this database is closed, settle
its card" (refresh hook), and "export/restore" (portability). An agent
never sees whether a context was local, restored, or replicated; the card's
`placement` block reports it for the curious.

## Portability

`barrel_ctx_export:export/3` produces one movable object:

```
projects-gunicorn.barrel/
  manifest.json
  docs/        rocksdb checkpoint of the document store
  att/         attachment store checkpoint (or a pointer to its S3 prefix)
  vector/      optional: vector store checkpoint
```

```json
{ "format": "barrel-export/1",
  "name": "projects/gunicorn", "db": "gunicorn_src",
  "source_node": "node-abc", "epoch": 3, "generation": 41,
  "exported_at": "...", "last_hlc": "...",
  "history_floor": "...", "retention_period": 2592000,
  "replication_checkpoints": {"peer-id": "vv..."},
  "encrypted": false, "keyspace": "gunicorn_src",
  "embedding": {"provider": "fastembed", "model": "bge-small-en",
                "dimensions": 384},
  "vector": "included",
  "checksums": {"docs/CURRENT": "sha256-...", "...": "..."},
  "card": { "...authored card snapshot..." } }
```

Facts that shape the format:

- `checkpoint_to/3` already checkpoints docs and attachments (today only
  the timeline calls it); the vector store needs the symmetric
  `barrel_vectordb:checkpoint_to/2`. The vectors column family is
  authoritative and HNSW state is serialized inside the store, so
  `vector` may also be `"rebuild_from_docs"` for record-mode databases,
  trading import time for export size.
- Epoch, generation, and conditional-put manifests are shared with the
  placement layer, so an export is a placement checkpoint and vice versa.
- `import/2` restores into a data dir, enrolls a card with a
  `derived-from` edge, and seeds replication checkpoints from the
  manifest, so a later sync against the source resumes from the export
  point instead of starting over.
- Encrypted databases export as ciphertext; the key travels through the
  keyprovider, never through the manifest.
- A format version gate refuses to open exports from a newer barrel.

## Failure model

- **Context unreachable at query time**: an `errors` entry, rows from the
  rest, `complete => false`, and a best-effort `reachable => false` on the
  card. Federated queries never fail as a whole because one member did.
- **Stale replica**: visible as the per-context read HLC on results and
  `last_hlc` on the card; agents that care compare before trusting.
- **Context disappears mid-session**: the next query reports it in
  `errors`; detach is idempotent; the janitor unpins for dead sessions.
- **Partially synchronized attach** (replica still catching up): the
  attachment records the replication task; queries run against what is
  there, and the attachment exposes `lag` so an agent can wait or accept.
- **Catalog lost**: rebuilt by the reconcile sweep from data directories
  plus manifests; authored fields recover from card snapshots inside
  exports where they exist, and are otherwise re-authored.

## Scale, honestly

- A node comfortably serves a few hundred **warm** databases, not tens of
  thousands: the block cache is shared (`barrel_cache`), but memtables,
  WAL, file descriptors, and compaction are per RocksDB instance.
  `barrel_dbs`' LRU and `dbs_max_open` are the control; attach pins fight
  it, which is why attach has a capacity bound. Cold databases are
  unbounded in number; they cost disk (or, evicted, object storage).
- Cold attach of a large vector set currently pays a full HNSW rebuild
  (verified: `load_or_create_index` rebuilds from the vectors column
  family even when index metadata exists). Wiring the existing HNSW
  serialization into store open is a scheduled enabler, not optional.
- Millions of datasets across a fleet: catalogs federate. Each node's
  catalog is authoritative for its own `origin`; aggregator catalogs pull
  many node catalogs through origin-filtered replication; a card with
  `kind => catalog` points one level up, and the same
  discover/attach loop applies to catalogs themselves. The catalog
  partitions by origin and namespace by construction. This is the one
  place the design happily admits a sharding-shaped answer, and it owns it
  instead of hiding it.

## Use cases

- **Software engineering agent**: attaches `projects/gunicorn`,
  `docs/python`, `protocols/http2`; federated hybrid search over all
  three; branches `projects/gunicorn` for an experiment (the branch gets
  its own card, `derived-from` the source); detaches the RFC corpus when
  the protocol work is done.
- **Support agent**: per-customer context plus shared product docs;
  customer context is `read`+`search` scoped; nothing the agent does can
  write the knowledge base; per-customer encryption comes free from
  per-keyspace keys.
- **Telecom incident**: branch the live telemetry context at the incident
  window (PITR gives "the network as it was at 03:12"), attach runbooks
  and past-incident contexts, investigate, then export the branch as the
  incident record.
- **Research agent**: a temporary task context `derived-from` a corpus
  context; intermediate findings written to the task context; handoff
  carries the attached set to the next agent; the task context expires
  with its space.
- **Per-customer isolation**: context = tenant. Movement between nodes is
  export/import or replication, not resharding.
- **Personal/local AI**: contexts on a laptop NVMe; `cached` attach
  restores from object storage when away from the home node; everything
  works offline because every context is independently usable.

## Comparison

| model | difference |
|---|---|
| Conventional sharding | Shards are invisible and system-owned; the router reconstructs one view. Contexts are visible and agent-owned; composition is explicit. Sharding wins for cross-partition transactions, global indexes, and hot logical datasets (see limits). |
| Vector DB / RAG store | One big index with namespace filters. Contexts are scoped corpora with lifecycle, provenance, versions, branches, and per-corpus permissions; retrieval quality also benefits (per-domain ranking beats one global space). |
| Data lake | Files queryable only through an external engine. A context is online, indexed, queryable alone, and versioned; an exported context in a bucket is close to a lake object, but it carries its own indexes and history. |
| Object store | Where exported contexts live. The catalog, attach, and query layers are exactly what an object store lacks. |
| Filesystem / mounts | The UX metaphor: discover/mount/umount, df is `context_list`. The differences: mounts have no semantic discovery, no capability tokens, and no federated queries. |
| Database federation (classic) | Admin-defined static views over heterogeneous engines, transparent to apps. Here federation is dynamic, agent-driven, capability-scoped, and homogeneous (every member is a barrel db), which is why one compiled plan can fan out. |
| MCP resources/tools | The delivery surface, not a competitor: cards are resources, the loop verbs are tools, and barrel is the backing store that makes the resources queryable. |

## Risks and explicit non-goals

Where this model is worse than a sharded database, it says so rather than
pretending:

- **Cross-context joins.** Only session-layer semi-joins (run side A, push
  the key set into side B as a `WHERE ... IN`). Large-large distributed
  joins: out of scope.
- **Cross-context atomicity.** No 2PC, no cross-db write batch. Sagas with
  idempotent replay (the outbox pattern already in record mode) are the
  ceiling. Out of scope.
- **Whole-corpus analytics.** `GROUP BY` over every context serializes
  through open/close cycles. Export to an OLAP engine instead. Out of
  scope.
- **Global uniqueness.** Unenforceable across contexts; uniqueness scope
  is one dataset, by contract.
- **A single hot dataset.** One writer process per database is a hard
  per-dataset write ceiling. The honest answer is a size and write-rate
  watchdog on cards that warns owners early, and O(1) branching as the
  splitting tool; a dataset that must be one logical database beyond one
  node's capacity needs a sharded system, and barrel should say so rather
  than reinvent shard routing ad hoc.
- **Catalog abuse.** Cards steer agents, so card authoring is gated by a
  capability on the target database and cards carry `registered_by`;
  computed fields cannot be authored. Signing cards end-to-end is an open
  question, not a phase-0 requirement.
- **Attach and fanout costs.** Tail latency is the slowest attached
  context; overfetch is N times k; both are bounded by deadlines, partial
  results, attach budgets, and the rerank stage being opt-in.

## Roadmap

Incremental, additive, no rewrite. Each phase is independently shippable.

| phase | delivers | touches |
|---|---|---|
| P0 | catalog core: `_barrel_catalog`, enroll on create/branch, reconcile sweep, refresher on the `barrel_dbs` close hook | `barrel_catalog` (new), `barrel_dbs`, `barrel_docdb_usage` |
| P1 | discovery: record-mode catalog, `search_text` synthesis, `context_search/list/inspect/describe` (MCP + REST), embedder model id in `embedder_info` | `barrel_server_mcp_tools`, REST, `barrel.erl` |
| P2 | attach: `barrel_ctx`, session attachments, pin refcounts, capability scopes, janitor unpin | `barrel_ctx` (new), `barrel_caps`, `barrel_session`, `barrel_spaces_janitor` |
| P3 | federated query: scatter-gather, RRF fusion, `barrel_rerank` stage, `context_query` | `barrel_ctx`, `barrel_server_mcp_tools` |
| P4 | live federation: `barrel_ctx_live`, MCP resource bridge | new module, MCP live bridge |
| P5 | portability and placement modes: export/import, `barrel_vectordb:checkpoint_to/2`, replica/cached attach, HNSW open-time deserialization | `barrel_ctx_export` (new), `barrel_vectordb` |
| P6 | catalog federation and handoff context sets | replication filters, `barrel_handoff` |

**Smallest useful prototype** (validates every load-bearing claim before
any permissions or placement work): P0 with sweep-only refresh; P1 with
generated descriptions only; attach as `ensure` + `pin` + refcount, public
databases, no capability integration; `context_query` restricted to
`hybrid_top_k` plans with RRF fusion, `_ctx` tags, and an errors list.
Four modules, one demo: three seeded databases, one search that finds two
of them, one federated query that returns fused, provenance-tagged rows
with one database deliberately offline.

## Open questions

- Remote query mode: is a BQL-over-HTTP execution wire worth building, or
  do replica and cached attach cover the need indefinitely?
- Identity across independently authored copies of "the same" document:
  leave to `_ctx` disambiguation, or introduce an optional global identity
  field replicated with the doc?
- Default attach budget per session and per node.
- Should space cards accept authored fields through the space API (label
  and purpose already project), or only through `context_describe`?
- Card signing beyond capability gating, for catalogs federated across
  trust domains.

## Recommendation

Pursue it, as the primary scale-out story, with the non-goals stated in
writing. The direction converts barrel's existing shape (many bounded
databases, per-database search and sync, spaces and capabilities, cheap
branches) into a coherent product surface for agents, and its new
primitives are small: a catalog database, an attach verb over the existing
lifecycle manager, a scatter-gather over an already db-independent query
plan, and an export manifest shared with the placement layer. The workloads
it serves badly (cross-dataset transactions, large joins, corpus OLAP, hot
single datasets) are workloads barrel serves badly today; naming them as
non-goals costs nothing and keeps the design honest. Start with the
prototype in the roadmap: it is four modules and answers the only open
question that matters early, whether hybrid search over cards plus
rank-fused federated retrieval actually feels like one database to an
agent.
