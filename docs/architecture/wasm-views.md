# Barrel design templates and WebAssembly views

**Installing a design should start indexing automatically.** The proposed `barrel_design` application builds its declared views over existing documents, then follows committed changes in the background. A design packages related views for one or more databases, an idea borrowed from Lotus Notes. Authors declare a selector, columns and totals, and can publish a WAT or compiled Wasm map for custom logic. Barrel reuses the loaded module and serves queries from RocksDB. A thousand reads of the same index need zero guest calls.

The first release should use fresh instances, finite fuel, a wall-clock deadline and memory limits. **Reusing a validated module works with all four.** Reusing generated BEAM code also works across fresh instances, but `erlang_wasm` 0.11.2 executes finite-fuel calls in the interpreter. A deployment that specifically needs generated code can use a worker deadline and memory quotas with unlimited fuel. Requiring both generated code and deterministic fuel metering would add an upstream runtime dependency. [Compiled tier](https://github.com/benoitc/erlang_wasm/blob/0.11.2/docs/compiled-tier.md)

Planning baseline, 26 November 2026. Barrel is at `b5bfbc5`. The example and measurements use the Hex package `erlang_wasm` 0.11.2, OTP application `wasm`. It requires OTP 29 or later. Barrel's Docker builder and most CI jobs use OTP 28. Give the optional profile an OTP 29 build and test job before offering it in a server image. Deployments that must stay on OTP 28 could host the worker pool in a separate OTP 29 process and measure the added IPC cost. [Runtime release](https://hex.pm/packages/erlang_wasm/0.11.2) · [Runtime requirements](https://github.com/benoitc/erlang_wasm/blob/0.11.2/docs/getting-started.md) · [Barrel CI](../../.github/workflows/ci.yml) · [Dockerfile](../../Dockerfile)

The implementation prompt specifies the public API, acceptance tests and measurements to publish for the first release. The invoice benchmark gives it a reproducible starting point. [Implementation prompt](barrel-design-implementation-prompt.md) · [Measured invoice indexing performance](#measured-invoice-indexing-performance)

**Implemented, 27 November 2026.** `barrel_design` 0.1.0 ships the first release this note proposes, behind the `design` profile on OTP 29: installed designs, automatically maintained native and Wasm views, category reads and exact totals, freshness waits, refresh, generations, and views as a BQL source. The sections below keep the design reasoning; what was built, how it differs and what it measured are in the last sections. [Implementation](#implementation) · [Measured public paths](#measured-public-paths) · [Guide](../guides/design-views.md)

## How databases use Wasm

| System | What users can deploy | What applies to Barrel |
|---|---|---|
| ScyllaDB | CQL functions supplied as WAT. The module exports a named function with a checked signature and a versioned ABI. ScyllaDB introduced reuse of compiled functions across calls in 5.1. UDFs are experimental and disabled by default. | Support handwritten WAT and compiler output through the same versioned interface. Check it at registration. [Wasm ABI](https://docs.scylladb.com/manual/stable/cql/wasm.html) · [Reuse](https://www.scylladb.com/product/release-notes/scylladb-open-source-5-1/) · [Feature status](https://docs.scylladb.com/manual/stable/cql/functions.html) |
| SingleStore Helios | Scalar, aggregate and table-valued Wasm functions. The code becomes part of the database and its backups. The engine constrains raw module size, compiled size and linear memory, and restricts host capabilities. | Give a module an explicit function kind. Budget installation separately from execution and back up the portable definition. [Code Engine](https://docs.singlestore.com/cloud/reference/code-engine-powered-by-wasm/) · [Limits](https://docs.singlestore.com/cloud/reference/code-engine-powered-by-wasm/configure-wasm/) |
| SpacetimeDB | Published modules can implement transactional reducers. Failure rolls back their database changes. A fresh instance may serve the next call, and the reducer has no filesystem or network access. | Keep durable state in the database. Stage a guest's emitted rows until the invocation succeeds. SpacetimeDB's reducer is a state-changing transaction, distinct from a CouchDB aggregate. [Reducer semantics](https://spacetimedb.com/docs/functions/reducers/) |
| Redpanda, a streaming analogue | A background transform reads a committed record and emits zero or more records. Recovery can replay input from the last committed offset. | Keep guest work off the source write path and make reprocessing a document idempotent. [Transform execution and replay](https://docs.redpanda.com/streaming/current/develop/data-transforms/how-transforms-work/) |

These five descriptions were checked against the linked pages on 27 November 2026 and still hold. ScyllaDB's page lists ABI versions 1 and 2, and Redpanda processes a record once it is written to disk on the input topic, resuming from the last committed offset with at-least-once delivery.

Across these systems the recurring pattern is a registered artifact, a narrow contract for inputs and outputs, and host control over effects. The contract varies with the job. RisingWave's published Rust/Wasm design exchanges Arrow record batches, an option worth measuring if per-document marshalling dominates. SQLite's browser build illustrates a separate use of Wasm, running the database engine on the client. Barrel's existing `barrel-lite` client can consume a server view over its protocol. [RisingWave design](https://risingwave.com/blog/user-defined-functions-rust-x-webassembly/) · [SQLite Wasm](https://sqlite.org/wasm/doc/trunk/index.md) · [Barrel client](../guides/barrel-lite.md)

**CouchDB supplies the view model.** A map sees one document and can emit zero, one or many key/value rows. Subsequent builds replace that document's previous emissions. Queries scan the sorted index. A reduction combines leaf values and a rereduction combines partial results. The portable object is the design definition. Each replica can rebuild its own index. These semantics work independently of the language used for the map. [View lifecycle](https://docs.couchdb.org/en/stable/intro/overview.html) · [Map and rereduce](https://docs.couchdb.org/en/stable/ddocs/views/intro.html)

## Lotus Notes design templates and categorized views

In Notes, a designer could give one collection several entry points, such as open invoices by customer and paid invoices by month. A view specified which documents belonged, what each column displayed and how rows were ordered. A categorized column let a reader expand one group. Domino design templates packaged reusable forms and views and could pass design changes to multiple databases. For Barrel, the two ideas to ship first are **named design templates** and **categorized views**. [Notes view design](https://help.hcl-software.com/dom_designer/14.5.1/basic/H_ABOUT_DESIGNING_VIEWS.html) · [Categories](https://help.hcl-software.com/dom_designer/14.5.0/basic/H_ABOUT_ADDING_CATEGORIES_TO_VIEWS.html) · [Domino templates](https://help.hcl-software.com/dom_designer/14.5.1/basic/H_CREATING_A_DATABASE_FROM_A_TEMPLATE_OVER.html)

A proposed `receivables` template could contain this view. The selector uses current BQL syntax. The surrounding JSON is a proposed manifest.

```json
{
  "name": "receivables",
  "version": 1,
  "views": {
    "open_by_customer": {
      "select": "SELECT * FROM db WHERE type = 'invoice' AND status = 'open'",
      "key": [
        {"path": ["customer"], "category": true},
        {"path": ["due_on"]}
      ],
      "value": {"path": ["cents"]},
      "reduce": "_sum"
    }
  }
}
```

If Ada has two open invoices for 1,250 and 750 cents, the collapsed view shows **Ada · 2,000 cents**. Expanding Ada reads the key prefix `["Ada"]` and lists her invoices by due date, stored as `YYYY-MM-DD`. Marking the first invoice paid removes its previous row at the next refresh and leaves 750 cents. The host can do all of this with a compiled document predicate, native field projection and an integer sum. Restrict stored selectors to deterministic conditions on one document. Evaluate the old row's removal even when the new body fails selection. Bound the work to enumerate categories and compute totals under the same scan and pagination rules as other reads. [BQL compiler](../../apps/barrel_docdb/src/barrel_bql.erl) · [Document predicate evaluator](../../apps/barrel_docdb/src/barrel_query.erl)

For databases that store individual line amounts, the [Zig example](#an-invoice-view-written-in-zig) emits the same `[customer, due_on]` key and a calculated balance. The host validates that row shape and applies the same category and reduction logic. Both designs support the same reads.

**Template updates.** Two databases can install the same `receivables@1` and enable it locally. Replicate the manifest and referenced portable module bytes. Each Barrel node builds against the documents it holds, including while offline once it has the design. Track the selected template version per database and keep an index signature per view. Changing a column caption can reuse the index. Changing selection, category order, row values or guest code builds a new generation before switching readers. A later design can also include a form with field labels, types and editing hints for clients. Existing invoice documents participate through their ordinary `type` field. [Notes forms and documents](https://help.hcl-software.com/dom_designer/14.5.1/basic/H_STORING_FORMS_OVER.html)

**Who can see a category.** Notes Readers fields narrow document visibility within database permissions. For Barrel v1, expose a shared view only to callers authorized to read the full indexed input. If document-level visibility is added later, apply it before exposing category names, counts or sums. An invoice hidden from a reader must contribute neither a customer heading nor a cent to that reader's result. [Notes document access](https://help.hcl-software.com/dom_designer/14.5.1/basic/H_ABOUT_RESTRICTING_ACCESS_TO_SPECIFIC_DOCUMENTS_USING_A_READERS_FIELD.html)

## Where the feature belongs in Barrel

I would name the OTP application and public Erlang module **`barrel_design`**. A database owner installs a versioned design such as `receivables@1`. A design can include declarative views and guest modules, with room for forms and agents. **Barrel Notes** (`barrel_notes`) fits a user-facing workspace for browsing and editing documents through those designs. **`barrel_views`** fits a package dedicated to materialized views. The guest import namespace `barrel_v1` identifies its own ABI version.

Barrel has already tried this shape. `barrel_docdb` 0.5.0 had Erlang `map/1`, optional `reduce/3`, declarative views, built-ins and manual or automatic refresh. Version 0.6.0 removed the view workers, public API and HTTP routes to narrow the document core. The old reverse index and tests are useful specifications. The old row key combined the emitted key and document ID, so two emissions of the same key by one document addressed the same entry. The surviving `encode_view_key/1` uses Erlang external-term bytes. Its byte order does not implement the ordering needed for text or signed integer ranges. [Barrel changelog](../../apps/barrel_docdb/CHANGELOG.md) · [Historical index](https://github.com/barrel-db/barrel_docdb/blob/v0.5.0/src/barrel_view_index.erl) · [Surviving key helpers](../../apps/barrel_docdb/src/barrel_store_keys.erl)

Use the same dependency direction as `barrel_ngram`. The optional app depends on `barrel_docdb` and `{wasm, "0.13.0", {pkg, erlang_wasm}}` (0.11.2 when this was planned). It owns registration, a node-local module holder, bounded workers, and one serialized index publisher per view and database instance. Add a small snapshot-reading API in docdb for consistent index builds. Keep the existing ARS path indexes and BQL execution available for native predicates. The record indexer is a precedent for background work, but its outbox is populated by tagged writes. A database-wide view must also see direct, bulk and replicated writes, deletes and conflict resolution. Follow the unfiltered document feed. [Ngram indexer](../../apps/barrel_ngram/src/barrel_ngram_shard.erl) · [Record indexer](../../apps/barrel/src/barrel_record_indexer.erl) · [Tagged outbox](../../apps/barrel_docdb/src/barrel_outbox.erl) · [Document writer](../../apps/barrel_docdb/src/barrel_db_server.erl)

## From WAT or source code to a reusable module

| Input or execution mode | Runtime behavior and integration |
|---|---|
| Inline WAT | `wasm:compile({wat, Source})` parses and validates the text directly in Erlang and returns an in-memory module that can instantiate multiple times. Use it for a console, tests or a preview. A module compiled this way maps documents at the same cost as one loaded from bytes (18.8–19.4 µs against 18.1–19.6 µs per document at p50 for `by_customer`), but has no shareable identity. [Core API](https://github.com/benoitc/erlang_wasm/blob/0.13.0/src/wasm.erl) |
| Published WAT | Since erlang_wasm 0.13.0, `wasm:encode({wat, Source})` writes the text as canonical `.wasm` bytes, and `wasm:load({wat, Source})` loads it under the same handle as its binary. `barrel_design:install/3` takes `{wat, Source}` and stores that encoding, so no tool is needed and comments, layout and identifiers leave the digest unchanged. `wasm-tools parse` or WABT's `wat2wasm` output installs too, under its own digest, since those tools add a name section. [Encoder](https://github.com/benoitc/erlang_wasm/blob/0.13.0/CHANGELOG.md) · [wasm-tools](https://github.com/bytecodealliance/wasm-tools#subcommands) · [WABT](https://github.com/WebAssembly/wabt) |
| Other languages | Zig can export a map for `wasm32-freestanding`, as in the [invoice example](#an-invoice-view-written-in-zig). Rust can build a `cdylib` with `cargo build --release --target wasm32-unknown-unknown`. C/C++ can build a freestanding `wasm32` module with exported functions and no entry point. Supply bindings for the same imports. Check the resulting imports and Wasm features during installation. The runtime can also host WASI commands and components with WIT interfaces, which can have their own ABI if a later extension needs them. [Guest toolchains](https://github.com/benoitc/erlang_wasm/blob/0.11.2/docs/guests.md) · [Rust target](https://doc.rust-lang.org/rustc/platform-support/wasm32-unknown-unknown.html) · [Components](https://github.com/benoitc/erlang_wasm/blob/0.11.2/docs/components.md) |
| Loaded binary | A supervised registry calls `wasm:load(Bytes)` at activation. Identical bytes share the node's SHA-256-keyed decoded and validated module and reusable execution preparation. Keep a holder alive across requests, drain in-flight workers before `wasm:unload/1`, and reload from stored bytes after restart. Separate authorization to use a module from reuse of its immutable code. [Module cache](https://github.com/benoitc/erlang_wasm/blob/0.11.2/src/wasm_module_cache.erl) |
| Generated BEAM | `compile => true` opts eligible hot functions into background compilation. Calls can fall back to interpretation, and the node has 16 compiled units available. The optional private `code_cache_dir` can reuse compatible artifacts across restarts. Warm with representative inputs and inspect `wasm_jit:counts/0` for actual compiled entries. Keep generated BEAM and runtime snapshots node-local. Compiled calls count recursive depth precisely but can exceed `max_depth` along nonrecursive chains. [Tier and disk cache](https://github.com/benoitc/erlang_wasm/blob/0.11.2/docs/compiled-tier.md) |

## A view a developer could publish

Consider a `by_customer` view over documents containing a text `customer` and signed integer `cents`. Document `a` with customer `Ada` and `cents = 1250` emits `("Ada", 1250)`. An edit moving it to `Bea` for `900` cents removes the Ada row and inserts the Bea row. A missing field yields zero rows. The same interface can emit a row for every line item in an order, including two items with the same key.

For the proposed `barrel_v1` ABI, the manifest lists the two typed field paths in slot order. `field(slot, dest, capacity)` copies one wire-CBOR value into guest memory and returns its byte length, or `-1` for a missing or nonmatching field. Insufficient capacity is a reported error. A declared root field can give a guest the whole bounded document body. `emit(key_ptr, key_len, value_ptr, value_len)` stages one row. The host attaches the document ID and emission ordinal. These two imports give WAT authors access to fields without requiring a JSON parser in every guest. Barrel already has a wire-CBOR encoder and a codec for indexed stored bodies. [Barrel codec](../../apps/barrel_docdb/src/barrel_docdb_codec_cbor.erl) · [Runtime host functions](https://github.com/benoitc/erlang_wasm/blob/0.11.2/docs/host-functions.md)

```wat
(module
  (import "barrel_v1" "field" (func $field (param i32 i32 i32) (result i32)))
  (import "barrel_v1" "emit" (func $emit (param i32 i32 i32 i32)))
  (memory (export "memory") 1 1)
  (func (export "map") (local $key i32) (local $value i32)
    (local.set $key (call $field (i32.const 0) (i32.const 0) (i32.const 1024)))
    (if (i32.lt_s (local.get $key) (i32.const 0)) (then return))
    (local.set $value (call $field (i32.const 1) (i32.const 1024) (i32.const 16)))
    (if (i32.ge_s (local.get $value) (i32.const 0))
      (then (call $emit (i32.const 0) (local.get $key)
                        (i32.const 1024) (local.get $value))))))
```

The proposed embedded API installs this WAT example in a design called `sales`. The map export and reducer are part of its versioned definition. A bounded `stale` read returns the published generation immediately, including its indexed-through HLC. A `wait_for` read names a source instance and HLC and has a deadline. HTTP can later wrap the same operations in `barrel_server`.

```erlang
Digest = crypto:hash(sha256, WasmBytes),
Map = #{abi => 1, module => Digest, export => <<"map">>,
        fields => [{[<<"customer">>], text}, {[<<"cents">>], i64}]},
Design = #{name => <<"sales">>, version => 1,
           views => #{<<"by_customer">> => #{map => Map, reduce => '_sum'}}},
{ok, _Version} = barrel_design:install(Db, Design, #{Digest => WasmBytes}),
{ok, Rows, Meta} = barrel_design:query(Db, {<<"sales">>, <<"by_customer">>},
    #{key => <<"Ada">>, reduce => true, freshness => stale}).
```

Begin with exact key, key range, a row limit, a continuation, and native `_count` and integer `_sum`. Start with versioned ordering for signed integers, UTF-8 text in byte order, and bounded arrays of those types for composite keys. Use an order-preserving, self-delimiting encoding with type tags and a documented type order. Canonical CBOR can encode a value for transport, but sorting its bytes puts some short strings ahead of lexically earlier long ones. CouchDB uses ICU for default string collation. Publish Barrel's ordering with golden vectors for negative numbers, prefix strings, composite keys and duplicate emissions. [CouchDB collation](https://docs.couchdb.org/en/stable/ddocs/views/collation.html)

## An invoice view written in Zig

Suppose an invoice stores signed line amounts in `line_cents`. For `open_by_customer`, Barrel evaluates the template's BQL selector first. The Zig map computes one balance for each open invoice. Its first key component groups invoices by customer and its second orders them by due date. Barrel's native `_sum` can then total the emitted balances for a customer.

```zig
const std = @import("std");
const invoice = @import("invoice.zig");

export fn map() void {
    var fields: invoice.Fields = undefined;
    const doc = fields.read() orelse return;

    var balance: i128 = 0;
    for (doc.line_cents) |cents| balance += cents;

    const cents = std.math.cast(i64, balance) orelse @trap();
    invoice.emit(.{ doc.customer, doc.due_on }, cents);
}
```

For example, Ada's open invoice due `2026-12-01` with charges of 1,250 and 750 cents and a credit of 200 cents emits `(["Ada", "2026-12-01"], 1800)`. A paid invoice skips the map. A missing or mistyped required field yields zero rows. The wider accumulator allows intermediate sums to exceed 64 bits. A final sum outside signed 64-bit range traps and the host discards the invocation.

The example includes a handwritten binding. Its three manifest slots, in order, are `customer` (text), `due_on` (text) and `line_cents` (array of signed 64-bit integers). It reads through `barrel_v1.field`, builds a CBOR key and value, and calls `barrel_v1.emit`. Fixed buffers allow 256 encoded bytes for the customer, 32 for the date and 64 line amounts per invoice. A future publisher could generate this binding from the manifest. [View source](examples/receivables/open_by_customer.zig) · [Invoice binding](examples/receivables/invoice.zig)

From the directory containing both files, build the portable module with Zig 0.17.1. The command reserves a 16 KiB stack and fixes linear memory at one 64 KiB page. Publish the resulting bytes with the design, and reuse the loaded module across invoices. [Zig freestanding WebAssembly](https://ziglang.org/documentation/0.17.1/#WebAssembly)

```sh
zig build-exe open_by_customer.zig -target wasm32-freestanding -O ReleaseSmall \
  -fno-entry --export=map --stack 16384 \
  --initial-memory=65536 --max-memory=65536 -femit-bin=open_by_customer.wasm
```

Run `make -C docs/architecture/examples/receivables run` from the repository root to follow invoices from insertion through the Zig map to a real BQL query. Ada's two open invoices total 2,550 cents. Paying the earlier invoice leaves 750 cents. Importing another invoice and explicitly refreshing the view gives a statement for 1,250 cents. [Complete example with queries and output](examples/receivables/README.md)

## When to update a view

**Use automatic, asynchronous indexing for every activated view in v1.** The database owner chooses which designs to install. In the proposed API, `install/3` returns after durably registering the design and scheduling its initial build. The database keeps each view current even if nobody opens it. Ordinary inserts, bulk imports, replicated writes and deletes all reach the same source feed. Closing an invoice removes its old row even though the new document fails the selector. An application can save a payment and query Ada's balance without issuing a refresh.

| Policy | Advantages | Costs |
|---|---|---|
| **Background after commit** | Readers usually find a warm index. One build serves every reader. Guest execution and a failing map stay outside the write transaction. Batching can combine repeated edits to one document. | A read can briefly show the previous balance. Every active view consumes CPU, I/O and disk even when unused. Backfills and bursts compete with foreground traffic and can extend lag. |
| **Synchronous with the write** | A successful local commit can make the document and all index rows visible together when both are in the same atomic transaction. | Each additional view adds work to writes and replication apply. A slow or trapping user map can delay or fail a valid document write. Atomicity would require a different storage arrangement from the proposed separate index store. |
| **On first use or on demand** | Rarely opened reports can defer their initial build and its storage cost. Updating only on demand can also save work between reads. | The first reader can wait through a large backlog. Concurrent readers need to share the build. Serving an older generation trades waiting time for stale results. |
| **Scheduled or manual as the sole policy** | An overnight report can spend its indexing budget during a known quiet period. | Freshness depends on the schedule or a person. A paid invoice can stay on the open list for an entire reporting interval. |

Domino exposes both always-on indexing and a first-use policy, as well as interval and manual refresh. CouchDB lets a read request an update before it returns or use an existing view generation. For Barrel, offer an explicit incremental refresh alongside automatic maintenance. A first-use option can follow if the measured cost of unused designs warrants it. [Domino index policies](https://help.hcl-software.com/dom_designer/14.5.1/basic/H_ABOUT_REFRESHING_VIEW_INDEXES.html) · [CouchDB view query options](https://docs.couchdb.org/en/stable/api/ddoc/views.html)

**Read freshness.** By default return the last complete generation and the source checkpoint it covers. During a first build, return `building` until a complete generation exists. After saving a payment, a caller can request `freshness => {wait_for, Target}` with a deadline. The read observes progress toward that fixed source instance and sequence while the worker continues independently. On timeout, report the checkpoint reached. A failed map leaves the source write committed and the last complete generation readable.

**Refresh on demand.** A caller preparing a statement can ask the serving node to catch up now. The proposed `barrel_design:refresh(Db, View)` captures the source instance and committed sequence once when the request is accepted. If the published checkpoint covers that target, it returns immediately. Otherwise it wakes the same bounded worker that follows the feed. Concurrent callers share its batches, and each finishes when its own target is covered. Success returns `{ok, #{instance_id => Id, last_seq => IndexedThrough}}`. The checkpoint can include later commits from the same batch. The default wait is one second. A third argument such as `#{timeout_ms => 5000}` sets a different deadline. On timeout, background indexing continues. Refresh advances the existing index incrementally. An administrative rebuild constructs a new generation from the source documents. [Runnable statement refresh](examples/receivables/README.md#refresh-before-exporting-a-statement)

**Keeping up.** Run bounded batches through the existing worker quotas. Drain full batches promptly, back off when idle and share a feed reader across views of a busy database when workloads justify it. A post-commit notification can wake the worker early. The durable feed and per-view checkpoint cover a lost notification or restart. Track publication lag, guest failures, CPU and index bytes per view so owners can see the cost. Barrel's ngram shard already follows changes and checks source identity, and its record indexer backs off between idle polls. [Ngram shard](../../apps/barrel_ngram/src/barrel_ngram_shard.erl) · [Record indexer](../../apps/barrel/src/barrel_record_indexer.erl)

## Maintaining and querying the index

1. **Install and authorize.** Put named designs in a reserved per-database catalog and exclude catalog records from map input. Give each view a signature covering its selector, projections, category and sort keys, reducer, collation and semantic options. For a Wasm map, include the module's binary digest, ABI version and export. Back up the definition and portable bytes together. Replication may deliver them in either order. Activate only a complete, digest-verified definition admitted by local policy.
2. **Build from a consistent source.** Open a RocksDB snapshot, capture the source instance and committed document-feed HLC in that snapshot, and enumerate winning bodies there. Tail the unfiltered feed after that HLC. On refresh, use a fixed source snapshot for the upper HLC, changed IDs and corresponding bodies. Barrel replaces a document's earlier feed key on rewrite. Reading a live body after reading a feed row can pair two versions. `db_observed_version/1` is a freshness hint. Implement a scoped docdb reader using the existing snapshot primitives and release it promptly, with time and space limits for backfills. [Changes](../../apps/barrel_docdb/src/barrel_changes.erl) · [Snapshot reads](../../apps/barrel_docdb/src/barrel_docdb_reader.erl) · [Observed version](../../apps/barrel_docdb/src/barrel_docdb.erl)
3. **Replace a document's contributions atomically.** Use a separate RocksDB view store under the database's encryption policy. Store forward rows keyed by `(generation, encoded key, doc ID, ordinal)` and a reverse entry listing that document's row keys. A single index writer deletes the old keys, installs validated new rows and updates reverse entries. Tombstones replace the row set with an empty set. A source HLC is local progress, distinct from the winning revision token, which may also change through replication. Apply a complete small refresh and its checkpoint in one durable batch. Stage larger refreshes in a private generation and atomically publish its pointer and checkpoint. A crash can discard staging and replay from the last complete generation. [Write batches](../../apps/barrel_docdb/src/barrel_store_rocksdb.erl) · [Encryption](../guides/encryption.md)
4. **Handle expiry, retention and branches.** Keep the source expiry on each row and exclude expired rows at a fixed query time, including before counting or summing. Barrel hides TTL-expired documents before the sweeper writes a tombstone. If a checkpoint predates recoverable deletes, rebuild into an empty generation. With configurable tombstone purging, check the purge horizon as well as the history floor, or persist an explicit purge watermark. Bind the index to the DB instance and branch lineage. A same-name recreation, point-in-time restore or unverified backup pairing needs a rebuild. [TTL reader](../../apps/barrel_docdb/src/barrel_docdb_reader.erl) · [Tombstone retention](../../apps/barrel_docdb/docs/replication.md) · [Timeline](../../apps/barrel_docdb/src/barrel_timeline.erl)
5. **Publish only complete results.** A trap, quota error or timeout discards that invocation's emissions. Record the view version, document ID, revision and error class, keep the last complete index readable, and expose its lag. Retry transient failures with backoff. Resume a repeatable document error on a corrected revision or definition. A freshness wait returns a bounded error if it cannot reach its target. Pin readers and pagination to a published snapshot or return a stale-cursor error. Publish a rebuilt generation only if its definition is still current, then retire the old one after readers drain.

For grouping, start with a bounded native fold over the selected row range and expose the scan limit. If large group totals are central, maintain per-key count and sum from the old and new contributions, with an expiry strategy. A later Wasm `reduce`/`rereduce` ABI should take bounded batches of values or partial states. Require bounded output, an identity for empty input, and an associative merge that gives the same result across permitted partitions. Test incremental and full rebuild results for equality. Integer cents make exact sums straightforward.

## Execution and memory limits

| Boundary | Proposed enforcement |
|---|---|
| Install | Cap WAT and binary bytes before parsing or hashing, and limit concurrent installs, validation time, resident modules and cache bytes. Validate the exact import **names and signatures**, exports, feature set and memory/table declarations before instantiation. An Erlang host fun inherits the signature declared by the guest, so linking alone is insufficient for the Barrel ABI. Start with one unshared 32-bit memory, bounded or absent tables, and a deterministic core subset. Exclude start functions, shared state, clocks, random input, WASI, external I/O and database handles from views. [Host linking](https://github.com/benoitc/erlang_wasm/blob/0.11.2/docs/host-functions.md) · [Runtime limits](https://github.com/benoitc/erlang_wasm/blob/0.11.2/docs/security.md) |
| Invoke | Dispatch to a supervised, killable owner. Start from `wasm_limits:untrusted()` and set finite `fuel`, `max_depth`, `max_host_calls`, `max_memory_pages` and `max_heap_words`. `wasm_instance_worker` takes these under `limits`, supports `isolation => fresh`, and `call/4` enforces a deadline that kills the worker. Raw `wasm:instantiate/3` takes flat limits. An inline call in the database writer has no independent timeout. For the two imports, give the worker a per-job input and private emission buffer and clear both on every exit. [Worker implementation](https://github.com/benoitc/erlang_wasm/blob/0.11.2/src/worker/wasm_instance_worker.erl) · [Limit definitions](https://github.com/benoitc/erlang_wasm/blob/0.11.2/src/wasm_limits.erl) |
| Copy and persist | Validate pointers, lengths and remaining byte budget **before** copying. Enforce CBOR types, nesting depth, item count, key/value lengths, emitted rows and total bytes per document. Cap query rows, bytes, scanned rows and view disk usage. Use integer or otherwise specified deterministic arithmetic for indexed results and exclude nondeterministic Wasm features such as relaxed SIMD from v1. |
| Share a node | Cap workers, queues and rate per database or tenant and globally. Set the engine's page budget and reserve capacity for normal queries and writes. A process heap limit misses off-heap linear memory and binaries. Wasm tables live in ETS and need separate bounds. If a hard total resident-memory ceiling or isolation between mutually distrustful tenants is required, run the same worker pool in a separate OS process or container with memory and CPU limits. Budget the installation/compiler process as well. [Runtime security model](https://github.com/benoitc/erlang_wasm/blob/0.11.2/docs/security.md) |

For performance, batch storage reads and writes first. Each map sees a fresh instance even when the worker process and module handle are reused. A module can retain values in globals, memory or tables, so a `reset` export supplied by its author is insufficient. If instantiation dominates real workloads, measure restoration of a pristine runtime snapshot captured before any tenant data. A batch ABI would need explicit independence guarantees before one guest invocation could span documents. [Runtime snapshots](https://github.com/benoitc/erlang_wasm/blob/0.11.2/docs/snapshots.md)

For a first integration test, the following is the actual runtime call shape. These limits fit the sample map. Set production budgets from the permitted document sizes and latency objective. The stock worker binds its imports at startup. A pooled Barrel adapter needs to install the current document and a fresh result buffer for each job, retain the independent deadline, and return staged rows only on success.

```erlang
Limits = (wasm_limits:untrusted())#{fuel => 1000, max_depth => 16,
    max_memory_pages => 1, max_heap_words => 262144, max_host_calls => 4,
    timeout => 100},
WorkerOpts = #{imports => ImportsForDocument, limits => Limits, isolation => fresh},
Job = make_ref(),
Child = #{id => Job, restart => temporary,
          start => {wasm_instance_worker, start_link, [Module, WorkerOpts]}},
{ok, Worker} = supervisor:start_child(ViewJobSup, Child),
try wasm_instance_worker:call(Worker, <<"map">>, [], 100)
after _ = supervisor:terminate_child(ViewJobSup, Job)
end.
```

## What ran locally

On arm64 macOS and OTP 29, the WAT above assembled to 192 bytes with `wasm-tools` 1.256.0. The checksum-verified Hex release ran the same four cases through direct WAT compilation and binary loading. Both produced the expected rows, including a negative amount and missing fields. Two binary loads returned the same handle. A two-document Barrel database drove the map from its changes feed. A key change, deletion and replay produced the expected final row set using an in-memory reverse map.

| Check | Observation |
|---|---|
| Invoice selector | Barrel compiled the BQL from the proposed template. Applying its predicate and projecting the example rows in memory gave the expected due-date order and a subtotal of 2,000 cents, then 750 after payment. Reassigning an open invoice produced the new customer key. Template installation and the category index are implementation milestones. |
| Zig invoice map | Zig 0.17.1 produced a 1,382-byte module. `wasm-tools validate` passed. Its only imports were `field` and `emit`. A temporary host used Barrel's BQL predicate and CBOR codec and ran the guest in `erlang_wasm` 0.11.2 with fresh instances, 50,000 fuel, at most four host calls, one memory page and a 100 ms deadline per call. All 37 cases passed, including credits, 64-line invoices, overflow, oversized fields and skipped documents. |
| Querying Zig-emitted rows | The [runnable receivables example](examples/receivables/README.md) wrote four invoices before starting its background indexer. A bounded query waited for a payment, then returned 750 cents for Ada. After a 500-cent import, an explicit refresh and an ordinary BQL query returned 1,250 cents. In a separate run, changes to the row store confirmed that the worker indexed 70 pre-existing invoices in three batches and processed a payment and a deletion before the first view query. |
| Concurrent refreshes | With the idle poll deferred for 60 seconds, 12 simultaneous requests woke the worker and indexed 40 imported invoices in two batches with 40 Zig map calls. A repeat refresh at the same checkpoint ran zero maps. In a separate check, one caller timed out while another reached its original target, and background indexing picked up a later write. |
| Fresh state and memory | A guest incrementing a mutable global returned `1` on each of two fresh requests. A zero-page budget rejected a module declaring one page. |
| Bounded work and elapsed time | A finite loop completed with fuel 100 and returned `out_of_fuel` with fuel 1. A separate 25 ms deadline returned `timeout` and a process monitor confirmed that the worker running a deliberately slow host callback died. |
| Reuse of generated code | With a warmed arithmetic module kept live, three fresh calls entered the same compiled unit and compiled zero additional units. Three finite-fuel calls on the same binary produced zero compiled entries. |
| Warm map cost | Three fresh VMs, two schedulers, one serial worker, 500 warm-up calls and 10,000 timed calls per run. The `by_customer` map used one 64 KiB page, finite fuel, two field reads and one emitted row. Median **20–21 µs**, p95 **32–37 µs**, p99 **45–57 µs** across runs. Timing includes the worker round trip, CBOR conversion and fresh instantiation. It excludes fetching a document, index I/O, and concurrent database traffic. |

The warm map timing above covers a single WAT invocation. The following benchmark measures the Zig invoice map through the example’s source and row databases.

## Measured invoice indexing performance

On 27 November 2026, the finite-fuel Zig map backfilled **3,076–4,456 source invoices/s** over three trials. The native Erlang map backfilled **6,365–7,421/s** through the same selector, worker and RocksDB row writes. Each Wasm run made **one module load and 1,600 guest calls** for 1,600 matching invoices. Checking all customer categories, repeating the query and refreshing the current index added **zero** calls. [Per-trial CSV](examples/receivables/benchmark-2026-11-27.csv) · [Benchmark source](examples/receivables/receivables_bench.erl)

| Measurement | Views disabled | Native Erlang | Zig/Wasm, finite fuel |
|---|---:|---:|---:|
| Initial backfill, source documents/s | — | 6,365–7,421 | 3,076–4,456 |
| Initial backfill, emitted rows/s | — | 5,092–5,937 | 2,461–3,565 |
| Foreground write p95, µs | 160–169 | 203–282 | 198–217 |
| Foreground write p99, µs | 200–211 | 294–592 | 253–327 |
| Catch-up after the write burst, ms | — | 37.9–57.5 | 137.6–155.0 |
| Warm category query p95, µs | — | 125–474 | 122–401 |
| Warm category query p99, µs | — | 161–663 | 173–458 |
| Already-current refresh p95, µs | — | 7–30 | 14–19 |

**Workload.** Each run seeded 1,000 invoices, then timed 1,000 more individual source writes from one producer. Every fifth invoice was paid. In the two indexed modes each phase yielded 800 rows distributed across 64 customers. The backfill timer started with worker creation after seeding and stopped when a passive wait observed the seed checkpoint. The catch-up timer started when the live write loop returned and included capturing and waiting for its fixed source checkpoint. The indexer polled every 25 ms when idle and processed batches of 32. Source and view used separate Barrel/RocksDB databases with default writes (`sync => false`). The disabled case measured the same source writes. [Indexer](examples/receivables/receivables_index.erl) · [Shared selector and row writer](examples/receivables/receivables.erl#L101)

**Reads and reuse.** After catch-up, the harness checked every customer’s invoice IDs, due-date order, balances and subtotal. It then warmed one customer query and timed 100 reads of its 25 saved rows, including the Erlang subtotal. It also timed 100 explicit refreshes of the current index. Erlang call counters recorded `wasm:load/1` and `wasm_instance_worker:call/4` before and after the reads and refreshes. The first load of the 1,382-byte binary into the running engine took 6.026–6.405 ms across the three Wasm runs. Guest compilation, file reading and first load were outside the backfill timer. Each map used fresh state, 50,000 fuel, at most four host calls, one 64 KiB guest page and a 100 ms deadline. [Guest and host limits](examples/receivables/receivables.erl#L148)

**Reproduction.** Apple M4 Pro, 14 cores, 48 GB RAM, macOS 27.2 arm64, Erlang/OTP 29.1.2 (ERTS 17.1.1), two online schedulers (`+S 2:2`), Zig 0.17.1 and `erlang_wasm` 0.11.2. The run used Barrel base commit `b5bfbc55ffa8a50bb5929c87fb7d5dfe702d42db` plus the example and benchmark linked here. The Wasm SHA-256 was `e0eed529a02f81ffb18e4f7c7f3c60a3459ca18eb1fb858167ad7f69ee23dbda`. Nine VMs ran sequentially with fresh databases, rotating the mode order across three trials. The OS file cache was shared. Table entries give the minimum and maximum of the three per-run values. Percentiles use nearest rank within each run. [Build and benchmark commands](examples/receivables/README.md#reproduce-the-measurements) · [Per-trial CSV in microseconds](examples/receivables/benchmark-2026-11-27.csv)

Both enabled modes raised foreground write p95 above the disabled control. The native index finished backfill and caught up sooner. Both modes read saved rows through the same BQL path and their warm query ranges overlap. This prototype publishes individual rows and keeps its checkpoint in memory. Sustained traffic, peak node memory, disk amplification, refresh with a backlog, durable generation publication and restart recovery are measured on the implementation below. [Implementation and measurement requirements](barrel-design-implementation-prompt.md#measure-the-implementation-and-publish-the-evidence) · [Measured public paths](#measured-public-paths)

## Implementation

The first release follows the delivery plan's first three steps on branch `feat/barrel-design`, one commit per step. Policy is **background indexing after commit, with explicit refresh**: a view follows the feed with no readers, a read answers at once from the last complete generation, a read can wait for a given write, and `refresh/2,3` catches up on demand. First-use, scheduled and synchronous indexing are not offered. [Guide](../guides/design-views.md) · [Indexing trade-offs](#when-to-update-a-view)

| App | Change |
|---|---|
| `barrel_docdb` | `read_feed/3` reads one feed page, each change with the body stored in its feed row and the document's expiry, plus the last-write watermark, under one snapshot; a short page ends at the watermark so a follower can tell it has caught up even when that write's row was rewritten or purged. `store_env/1` returns the database's encrypted environment. `barrel_bql_source` and `barrel_bql:register_source/2` let an application serve a BQL FROM source without docdb depending on it. [Feed reader](../../apps/barrel_docdb/src/barrel_changes.erl) · [Source registry](../../apps/barrel_docdb/src/barrel_bql_source.erl) · [Source execution](../../apps/barrel_docdb/src/barrel_bql_exec.erl) |
| `barrel` | `query/3`, `query_fold/5` and `explain_query/3` route registered sources to the same executor, so the HTTP and MCP query routes reach views with their existing read check. Federated context queries refuse them. [BQL facade](../../apps/barrel/src/barrel_bql_query.erl) |
| `barrel_design` | Manifests and view signatures, the key encoding, module admission, the node module holder, the guest runner, a derived RocksDB store per database instance, one indexer per view, the public API and the `view` BQL source. [Public API](../../apps/barrel_design/src/barrel_design.erl) · [Indexer](../../apps/barrel_design/src/barrel_design_view.erl) · [Index layout and reads](../../apps/barrel_design/src/barrel_design_index.erl) · [Guest runner](../../apps/barrel_design/src/barrel_design_guest.erl) · [Admission](../../apps/barrel_design/src/barrel_design_module.erl) |
| Build and CI | A `design` profile adds the app to the default set. A `design` CI job builds, cross-checks and tests it on `erlang:29`. The default build, the package job, the Dockerfile and the release workflow stay on OTP 28 without it. [Profile](../../rebar.config) · [CI](../../.github/workflows/ci.yml) |

**Choices where this note left room.**
- **Catalog.** Designs and module bytes are local documents of the source database: part of its backups, never replicated, never in the feed, so no map sees them. Each node installs the designs it serves.
- **Failed maps.** A map that fails on a revision (trap, fuel, deadline, a row or size limit) removes the document's previous rows and records the error with its revision and class; `status/2` counts them, and a corrected revision or design indexes again. Timeouts and dead jobs are retried twice first.
- **Deadline.** The default per-document deadline is 1,000 ms, not the prototype's 100 ms. Fuel already bounds the work; under CPU saturation a 100 ms deadline failed healthy maps in the tests.
- **Keys.** Version 1 orders null, false, true, signed 64-bit integers, UTF-8 texts in byte order and arrays, each self-delimiting; floats are not keys. Golden vectors fix the bytes. [Key encoding](../../apps/barrel_design/src/barrel_design_key.erl) · [Vectors](../../apps/barrel_design/test/barrel_design_key_tests.erl)
- **Pages and totals.** A continuation is bound to its generation and returns `stale_cursor` after a switch. Grouped totals fold at most `max_scan` rows per call and carry a partial group in the continuation, so a large category is totalled exactly across calls. A total over no rows returns no row, as in CouchDB.
- **Rebuilds.** A new generation builds while the current one keeps following the feed; one batch switches them. A rebuild also starts when an indexer resumes from a checkpoint older than the database's tombstone purge window.
- **Identity.** The store directory is named by the database instance id, so a recreation, restore or branch starts empty. A restored or branched database with designs in its catalog is attached on first use.
- **Guests.** Finite-fuel calls run in the interpreter; no generated BEAM code is used, and the measurements below record zero compiled entries.
- **WAT.** `install/3` takes `{wat, Source}` and stores the bytes erlang_wasm 0.13.0's encoder writes for it, which carry no names; the digest names those bytes, so editing comments, layout or `$identifiers` keeps the index. `barrel_design:module/1` returns the digest a design must reference. A compiler's binary keeps its own bytes and digest.

**Correctness tests.** 50 common test cases and 62 unit tests run on every change; the 48 cases of the first release were also run three times under full CPU saturation of all 14 cores without a failure. They cover the prompt's receivables sequence (with the Zig guest and the native projection, and again through BQL), indexing without readers, repeated keys, moves between categories, TTL before and after a sweep, bulk, replicated and winner changes, writes during a backfill, native and WAT maps agreeing, failed maps, caption and selector edits, an upgrade with readers paging through it, crashes just before and after a publication batch, an application restart, a same-name recreation, a restore and a branch, concurrent refreshes, a fixed refresh target, one caller timing out, guest limits, module admission, and a view installed from WAT matching its binary twin. Each recovered index is compared with a clean recomputation. [Tests](../../apps/barrel_design/test)

**Not in this release.** Replicating designs between nodes; HTTP routes to install designs and read status (views are readable over HTTP through the BQL query route; `install/3` already takes WAT text, so such a route needs no tool on the server); Rust bindings; Wasm reduce and rereduce; document-level read rules; a first-use indexing policy; a hard node memory ceiling or per-tenant OS isolation; the compiled tier.

**Later extensions.** BQL scalar or projection functions are the next candidate: they reuse the runner, admission and limits as they stand, applied to rows an index has already narrowed. Record-mode text extraction reuses them the same way. Notes-style folders need only membership rows per folder and document. Agents need a write-authorization policy and a durable event per transition. Export transforms need a checkpoint per transform version. Forms are client metadata a later design version can carry. Each is outside this release. [Candidates](#other-end-user-extensions)

## Measured public paths

On 28 November 2026, with erlang_wasm 0.13.0, the public paths backfilled **11,873–13,404 invoices/s with the native projection and 6,771–7,358/s with the Zig guest**, against 5,550–7,016 and 3,642–4,135 for the prototype rerun on the same machine. At a sustained 500 mixed writes/s with four concurrent readers, the Zig view covered a fixed write within **13–17 ms at p95** (native 5 ms). Reads and already-current refreshes made **zero** guest calls, every Wasm run made **one** module load, and no run entered generated code. The 27 November runs on erlang_wasm 0.11.2 overlap these ranges. [Per-trial views CSV](examples/receivables/design-benchmark-2026-11-28-views.csv) · [0.11.2 runs](examples/receivables/design-benchmark-2026-11-27-views.csv) · [Harness](../../apps/barrel_design/bench/barrel_design_bench.erl) · [Prototype rerun CSV](examples/receivables/benchmark-2026-11-27-rerun.csv)

| Measurement | Views disabled | Native projection | Zig/Wasm | WAT fan-out |
|---|---:|---:|---:|---:|
| Install call, ms | n/a | 57.5–58.9 | 63.4–67.0 | 57.3–62.3 |
| Install to first queryable generation, 1,000 invoices, ms | n/a | 74.6–84.2 | 135.9–147.7 | 111.8–122.4 |
| Backfill, source documents/s | n/a | 11,873–13,404 | 6,771–7,358 | 8,170–8,942 |
| Backfill, emitted rows/s | n/a | 9,498–10,723 | 5,417–5,886 | 19,608–21,462 |
| Foreground write p95 / p99, µs | 161–213 / 217–430 | 203–240 / 293–468 | 212–259 / 287–441 | 194–231 / 238–307 |
| Catch-up after 1,000 writes, ms | n/a | 7.4–10.0 | 4.3–23.9 | 6.8–24.7 |
| Category rows p50 / p95 / p99, µs | n/a | 59–216 / 145–379 / 184–463 | 60–199 / 141–363 / 182–398 | 48–228 / 125–276 / 145–300 |
| Category subtotal p95, µs | n/a | 132–369 | 316–378 | 50,138–55,269 (every group) |
| Already-current refresh p95, µs | n/a | 5–13 | 11–31 | 18–25 |
| Refresh after a 1,000-invoice burst, ms | n/a | 3.5–5.2 | 15.0–18.3 | 42.3–48.2 |
| Sustained writes p95 / p99, µs | 432–523 / 897–1,230 | 566–629 / 721–1,142 | 611–625 / 896–2,816 | 586–658 / 1,118–1,130 |
| Lag to a fixed write p50 / p95 / max, ms | n/a | 3 / 5 / 5–7 | 11 / 13–17 / 15–31 | 8–9 / 11–12 / 11–28 |
| Concurrent reader p50 / p95 / p99, µs | n/a | 615–667 / 850–965 / 992–1,235 | 611–644 / 853–1,039 / 1,087–1,369 | 491–564 / 698–930 / 867–1,246 |
| Module loads, guest calls, calls during reads, compiled entries | 0, 0, n/a, 0 | 0, 0, 0, 0 | 1, 3,734, 0, 0 | 1, 3,734, 0, 0 |
| Peak VM resident memory, MiB | 114–116 | 145–154 | 147–152 | 116–117 |
| Peak guest pages | 0 | 0 | 1 | 1 |
| Index bytes, share of the source database's bytes | n/a | 1.06 MB, 3.6% | 1.07 MB, 3.7% | 1.66 MB, 5.7% |

**Cold activation and warm calls.** Zig 0.17.1 built the guest from a cold cache in 0.93–1.27 s, always to SHA-256 `e0eed529…dbda`. erlang_wasm encoded the WAT example to its 152 canonical bytes in 7.0–8.7 ms on first use (most of it loading the encoder's code) and in 27–40 µs afterwards, so installing WAT needs no tool. In a fresh VM the first engine load of the 1,382-byte guest took 6.8–7.1 ms, including loading the runtime's code on first use. After 500 warm-up documents, a map in a fresh instance cost 46.1–48.3 µs per document at p50 and 50.3–56.1 µs at p95, in batches of 64, including field encoding, instantiation, the call, row checks and the job round trip. [Cold CSV](examples/receivables/design-benchmark-2026-11-28-cold.csv)

**Two databases.** A light database with a native view ran 300 interleaved writes and category reads alone, then again while a second database backfilled 5,000 invoices through the Zig view (done in 423–456 ms). Its write p95 went from 217–273 µs to 291–400 µs and p99 from 316–699 µs to 1,439–4,241 µs; its read p95 from 85–375 µs to 227–310 µs and p99 from 131–473 µs to 304–1,078 µs. Guest jobs were capped at two per database and one per scheduler on the node. [Tenants CSV](examples/receivables/design-benchmark-2026-11-28-tenants.csv)

**Limits and recovery.** A guest looping with 10,000,000 fuel against a 20 ms deadline returned `timeout` after 21.15–21.31 ms, its job process killed and its guest pages back to zero. 50,000 fuel ran out in 530–587 µs. With barrel_design's top supervisor killed after 763–822 of 3,000 backfill maps, the restarted app caught up with the database in 162–174 ms from its durable checkpoint, and the rows equalled a clean computation in every trial. [Limits CSV](examples/receivables/design-benchmark-2026-11-28-limits.csv)

**Workload and method.** Apple M4 Pro, 14 cores, 48 GB RAM, macOS 27.2 arm64, Erlang/OTP 29.1.2 (ERTS 17.1.1), two online schedulers (`+S 2:2`), `erlang_wasm` 0.13.0, Zig 0.17.1. Measured on the tree of the commit adding these figures, with no other changes to the measured code. Guests: Zig `open_by_customer.wasm` (SHA-256 `e0eed529a02f81ffb18e4f7c7f3c60a3459ca18eb1fb858167ad7f69ee23dbda`) and WAT `fanout.wasm` (`732e9414be1951085d60761a064e3cf698a4bfae68f01ef0a43f931c1574349a`), with the default limits: 50,000 fuel, a 1,000 ms deadline, one 64 KiB page, 16 rows per document. Indexers read 64 changes per batch, polled every 25 ms and backed off to 250 ms after a second idle. Each trial seeded 1,000 invoices (64 customers, every fifth paid), timed install and the first build, then 1,000 individual writes, a catch-up, 200 warm category reads and totals, 100 already-current refreshes, a 1,000-invoice burst written with `put_docs` and caught up with `refresh`, and 4 s of writes at 500/s (70% updates that move invoices between statuses and customers, 20% inserts, 10% deletes) with four readers and a lag sample every 100 ms. Before timings were kept, every row and per-customer total was compared with a computation over the live documents, after the first build and again after the sustained phase. Writes used Barrel's default `sync => false`. One fresh VM and data directory per trial and mode, the mode order rotated (off, native, wasm, fanout; native, wasm, fanout, off; wasm, fanout, off, native), each trial followed by its two-database, limits and cold runs. The OS file cache was shared. Ranges are the minimum and maximum of the three trials; percentiles use nearest rank. Two earlier runs on erlang_wasm 0.11.2 (27 November) are published as runs 1 and 2 of the 27 November files; in the first trial of run 1, write p95 rose to 3.3–4.9 ms in the three modes run during one stretch, views disabled included, so the host was slow there. [Command](examples/receivables/README.md#measure-the-public-paths)

**What this supports.** On this machine one view keeps up with a 500 writes/s mixed stream with ample room: the Zig view indexes about 7,000 documents/s and the native one about 12,000–13,000/s, so at that rate a single Zig view has about 14 times headroom and a native one 24–27 times, and the same budget divides between views of one database. The foreground cost is small at p95 (tens of µs per write) but shows at p99 under sustained load, and a second database's Wasm backfill raised its neighbour's write p99 2.5 to 13 times in this run. A view's rows took under 6% of the source database's bytes. Deployment objectives (peak write rate per database, views per database, latency objectives, memory per node) are not set; until they are, this is the measured envelope, and the open decision is how many views per database and what backfill share a node accepts before guest jobs need a separate OS process with its own CPU and memory limits.

## Other end user extensions

| Priority | Extension | Contract and integration cost |
|---|---|---|
| Next | BQL projection or scalar function | Run a named, pure function on rows already narrowed by native indexes. Bound total invocations and fuel per query. Start with projection, where an expensive function cannot silently turn an indexed predicate into a scan. [BQL](../guides/query-bql.md) |
| Next | Record-mode text or feature extraction | Transform declared document fields into bounded text, chunks or metadata before the existing embedding job. Include the module digest in the indexing policy fingerprint so an upgrade schedules a backfill. [Embedding policy](../../apps/barrel/src/barrel_embedding_policy.erl) · [Record mode](../guides/record-mode.md) |
| Later | Notes-style folders | Let a person file an invoice in “Review this week” and remove it independently of the invoice's status. Persist membership by folder ID and document ID. Removing membership leaves the source document intact. [Notes folder behavior](https://help.hcl-software.com/notes/14.0.0/client/fram_remove_doc_from_folder_t.html) |
| Later | Notes-style agents | Domino agents can act on new or changed documents. A Barrel agent could propose a review task for an invoice. Assign one runner per workflow, run the guest after commit in the bounded worker pool, and have the host authorize each proposed write. Key a review task by agent version, document ID, revision and action so replay is idempotent. The coalescing feed suits jobs interested in the latest state. Workflows that must observe every transition need a durable event for each transition. [Domino agent triggers](https://help.hcl-software.com/dom_designer/14.5.1/basic/H_TRIGGERING_AN_AGENT_ON_AN_EVENT_STEPS.html) · [Barrel outbox semantics](../../apps/barrel_docdb/src/barrel_outbox.erl) |
| Later | Changes-feed filter or export transform | Project/redact one outgoing record without editing the source. Replication filters also need durable membership and leave/delete semantics, and a separate checkpoint per filter version. An export-only filter is simpler to ship. [Replication filters](../../apps/barrel_docdb/src/barrel_rep_filter.erl) · [Channel behavior](../../apps/barrel_docdb/src/barrel_channel.erl) |
| Later | Write validation or conflict resolution | A validator needs a defined policy for local, bulk and replicated writes, plus a revision recheck if validation runs outside the writer. A resolver can consume the existing sorted sibling set and return `choose`, `merge`, `delete` or `keep`. Pin its digest across replicas and test convergence under different arrival orders. These extensions affect write availability or stored state. [Existing resolver contract](../../apps/barrel_docdb/docs/replication.md) |

## Delivery plan and acceptance criteria

Steps 1 and 2 are done and step 3 is measured; step 4 remains, apart from exercising the WAT and Zig examples through the public API. [Implementation](#implementation) · [Measured public paths](#measured-public-paths)

1. **Prove the runtime on the supported build.** Add the opt-in OTP 29 profile, manifest validation, module ownership, two imports and bounded worker. Compare native Erlang and Wasm maps over identical input, with fresh instances and production limits. Gate on equal results, zero redundant module loads in the hot path, discarded partial emissions on failure and released worker resources.
2. **Ship one automatically maintained, categorized view.** Implement the source snapshot reader, encrypted side index, forward/reverse rows, checkpoint, incremental refresh, rebuild, range queries and status. Install the design over existing invoices, then write and delete documents while there are no readers. Require the worker to catch up independently and to resume from its durable checkpoint after restart. Test an immediate read, a deadline-bounded read for a payment and concurrent explicit refreshes after an import. Repeating a refresh at the same checkpoint should run zero guest maps. Require native projection and an equivalent WAT map to yield the same rows, category prefix reads and subtotals as documents enter, leave or move categories. Verify that a caption edit reuses the index and a selector edit rebuilds it. Compare incremental results with a clean recomputation after repeated keys, deletes, TTL, bulk and replicated writes, winner changes, checkpoint replay, branch/PITR and a same-name database recreation.
3. **Set the performance budget before enabling the server.** Measure cold activation, documents and emitted rows per second, p95/p99 write and view-read latency, catch-up lag, peak node memory and disk amplification. Compare views disabled, a native Erlang map and the fuel-limited Wasm map against the same write stream. Include small projections, realistic compiled guest modules, fan-out and multiple tenants. Accept a target deployment when sustained indexing outpaces its peak change rate with agreed headroom, stays within its memory envelope and meets its foreground write/read latency objectives.
4. **Expose and extend.** Add authorized HTTP management and read routes for templates, category summaries, drill-down rows and freshness waits. Exercise the WAT and Zig examples through the public install and query API and publish Rust bindings for the same ABI. Use profiles from real workloads to decide between a bulk ABI, maintaining group summaries, or the deadline-bounded compiled tier. User-defined reductions and write-path hooks can then be evaluated against their own correctness and latency requirements.
