# Query open invoices by customer

The `receivables@2` design answers two questions for a billing clerk. Which of Ada's invoices are open, in due-date order? How many cents does she owe? A Zig map computes the balance of each invoice when it changes. Subsequent queries read the saved rows.

Two programs run this design. `make design-run` installs it through barrel_design's public API. `make run` is the prototype that came before barrel_design; it and its benchmark stay here as the baseline the public paths are measured against.

## Run it through barrel_design

From the Barrel repository root, with Erlang/OTP 29, Zig 0.17.1, Make and the usual Barrel build tools. The target builds the Zig guest, compiles the umbrella with the `design` profile and starts fresh databases. [Barrel build requirements](../../../../README.md#build) · [Program](receivables_design.erl)

```sh
make -C docs/architecture/examples/receivables design-run
```

```text
guest: _build/open_by_customer.wasm, 1382 bytes, sha256 e0eed529a02f81ffb18e4f7c7f3c60a3459ca18eb1fb858167ad7f69ee23dbda
installed receivables@2 over the four seed invoices
immediate read: building
Ada before payment
  inv-102  2026-12-01  1800
  inv-101  2026-12-15  750
  subtotal: Ada 2550 cents
immediate read after payment: Ada 2550 cents (payment not indexed yet)
Ada after payment (wait_for the payment)
  inv-101  2026-12-15  750
  subtotal: Ada 750 cents
Bea: Bea 900 cents
Ada after statement refresh
  inv-101  2026-12-15  750
  inv-103  2026-12-20  500
  subtotal: Ada 1250 cents
reads and an already-current refresh ran 0 guest maps
```

The first read finds the first build still running and answers `building` at once. The read right after the payment answers from the previous checkpoint; the read that waits for the payment's write sees 750. The program goes on with BQL reads, the equivalent native projection (same rows and totals) and a WAT map tried with `preview/3`.

## Query the same design through barrel_design

The view is addressed as `{<<"receivables">>, <<"open_by_customer">>}`. `key_prefix` matches whole leading key components and `group_level => 1` totals per customer. A freshness target is the source database instance and its last write; capture a new one after each write you want to see. [Statement program](receivables_design.erl#L58)

```erlang
{ok, Wasm} = file:read_file("_build/open_by_customer.wasm"),
Digest = crypto:hash(sha256, Wasm),
{ok, _} = barrel_design:install(Db, receivables_design:design(Digest), #{Digest => Wasm}),
View = {<<"receivables">>, <<"open_by_customer">>},
{ok, Seeded} = barrel_docdb:db_observed_version(Db),
Ada = #{key_prefix => [<<"Ada">>], freshness => {wait_for, Seeded}, timeout_ms => 1000},
{ok, [#{id := <<"inv-102">>, key := [<<"Ada">>, <<"2026-12-01">>], value := 1800},
      #{id := <<"inv-101">>, key := [<<"Ada">>, <<"2026-12-15">>], value := 750}], _} =
    barrel_design:query(Db, View, Ada#{reduce => false, limit => 20}),
{ok, [#{key := [<<"Ada">>], value := 2550}], _} =
    barrel_design:query(Db, View, Ada#{reduce => true, group_level => 1}),

{ok, Invoice} = barrel_docdb:get_doc(Db, <<"inv-102">>),
{ok, _} = barrel_docdb:put_doc(Db, Invoice#{<<"status">> => <<"paid">>}),
{ok, Paid} = barrel_docdb:db_observed_version(Db),
{ok, [#{key := [<<"Ada">>], value := 750}], _} =
    barrel_design:query(Db, View, Ada#{reduce => true, group_level => 1,
                                       freshness => {wait_for, Paid}}),

{ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"inv-103">>, <<"type">> => <<"invoice">>,
    <<"status">> => <<"open">>, <<"customer">> => <<"Ada">>,
    <<"due_on">> => <<"2026-12-20">>, <<"line_cents">> => [500]}),
{ok, _Checkpoint} = barrel_design:refresh(Db, View),
{ok, [#{key := [<<"Ada">>], value := 1250}], _} =
    barrel_design:query(Db, View, #{key_prefix => [<<"Ada">>], reduce => true, group_level => 1}).
```

The same rows through BQL, and so through the HTTP and MCP query routes. [BQL reads](receivables_design.erl#L123)

```erlang
{ok, Rows, _} = barrel_docdb:query(Db,
    <<"SELECT v.id, v.key, v.value FROM view('receivables', 'open_by_customer') AS v "
      "WHERE v.key[0] = 'Ada' ORDER BY v.key">>),
{ok, [#{<<"key">> := [<<"Ada">>], <<"value">> := 1250},
      #{<<"key">> := [<<"Bea">>], <<"value">> := 900}], _} = barrel_docdb:query(Db,
    <<"SELECT v.key, v.value FROM view('receivables', 'open_by_customer', group_level => 1) AS v">>).
```

The native projection reads the key from `customer` and `due_on` and sums `line_cents`, with no guest; it returns the same rows and totals. A minimal WAT map, [by_customer.wat](by_customer.wat), runs on sample documents with `preview/3`, then installs as text: `install/3` stores its canonical encoding, 152 bytes, with no toolchain. [Native design](receivables_design.erl#L21) · [WAT preview and install](receivables_design.erl#L143)

## Measure the public paths

The benchmark runs the same invoices through barrel_design with views disabled, the native projection, the Zig guest and a WAT guest emitting one row per invoice line, three trials in a fresh VM each with the mode order rotated. Each trial also runs two databases side by side, and the limits and crash-recovery checks. Rows and totals are compared with a clean computation before a timing is kept. [Results](../../wasm-views.md#measured-public-paths) · [Harness](../../../../apps/barrel_design/bench/barrel_design_bench.erl)

```sh
make -C docs/architecture/examples/receivables design-bench
```

## Source invoices and design

Both programs seed the `invoices` database with these four documents before any view exists. All four have `type = "invoice"`. Negative line amounts represent credits.

| Document ID | Customer | Status | Due date | Line amounts in cents |
|---|---|---|---|---|
| inv-101 | Ada | open | 2026-12-15 | 1,000, −250 |
| inv-099 | Ada | paid | 2026-12-02 | 9,900 |
| inv-200 | Bea | open | 2026-12-03 | 900 |
| inv-102 | Ada | open | 2026-12-01 | 1,250, 750, −200 |

The design selects documents with `SELECT * FROM db WHERE type = 'invoice' AND status = 'open'`. It binds three fields in order, `customer`, `due_on` and `line_cents`. The map emits `([customer, due_on], balance_cents)`. For `inv-102` the emitted pair is `(["Ada", "2026-12-01"], 1800)`. The first key component defines the category, the second orders its invoices and the design declares `_sum` as its reducer. [Design and seed documents](receivables_design.erl#L10) · [Prototype design](receivables.erl#L6) · [Zig map](open_by_customer.zig) · [Guest binding](invoice.zig)

## The prototype

The prototype indexed into a second Barrel database with an in-memory checkpoint. Its measurements are the baseline for the public paths.

### Run the example

From the Barrel repository root, run the following with Erlang/OTP 29, Zig 0.17.1, Make and the usual Barrel build tools. The target builds the guest, fetches the pinned `erlang_wasm` 0.11.2 dependency and starts a fresh local database for the example. [Barrel build requirements](../../../../README.md#build)

```sh
make -C docs/architecture/examples/receivables run
```

### Reproduce the measurements

Run the following target on the same checkout to measure writes with views disabled, a native Erlang map and the finite-fuel Zig map. It runs each mode three times with a fresh Erlang VM and fresh databases, rotates the order, and prints one CSV row per run. The benchmark checks each customer’s saved rows, their order, subtotal and guest-call counts before reporting timings. [Measurement method and results](../../wasm-views.md#measured-invoice-indexing-performance) · [Recorded CSV](benchmark-2026-11-27.csv) · [Benchmark source](receivables_bench.erl)

```sh
make -C docs/architecture/examples/receivables bench
```

### Query the saved rows with BQL

The example starts a background indexer after seeding the invoices. The worker builds the existing rows, then follows Barrel's changes feed even while nobody is querying. It writes one projected document per matching invoice into a second Barrel database named `open_by_customer`. The projected document keeps the invoice ID. Bind `$customer` to drill into Ada's category.

```erlang
ViewDb = <<"open_by_customer">>,
Bql = <<"SELECT id, due_on, balance_cents FROM db "
        "WHERE customer = $customer ORDER BY due_on">>,
{ok, Rows, #{has_more := false}} = barrel_docdb:query(ViewDb, Bql,
    #{params => #{<<"customer">> => <<"Ada">>}}),
Subtotal = lists:sum([Cents || #{<<"balance_cents">> := Cents} <- Rows]).
```

The subtotal covers the whole ordered result. After the first query, the program changes `inv-102` to `paid` and reads Ada's category again. The worker picks up the payment and removes the old row. For these two reads the program requests a one-second freshness wait against the source sequence it has just observed. Bea's invoice stays in her category. The final read prepares a statement after a new 500-cent invoice arrives. [Query and freshness option](receivables.erl#L26) · [Barrel query syntax](../../../guides/query-bql.md)

```text
Ada before payment
  inv-102  2026-12-01  1800
  inv-101  2026-12-15  750
  subtotal: 2550 cents
Ada after payment
  inv-101  2026-12-15  750
  subtotal: 750 cents
Ada after statement refresh
  inv-101  2026-12-15  750
  inv-103  2026-12-20  500
  subtotal: 1250 cents
```

### Refresh before exporting a statement

The clerk wants the statement to include the just-imported `inv-103`. The example explicitly refreshes the view once and then runs an ordinary query using the view handle from `receivables:start_view/5`.

```erlang
{ok, Checkpoint} = receivables:refresh(View),
{StatementRows, 1250} = receivables:query(View, <<"Ada">>).
```

Refresh captures the current source sequence, wakes the background worker if it is behind and returns the indexed checkpoint once it covers that sequence. A current view returns immediately. Concurrent refreshes share the worker. The default wait is one second. Pass `#{timeout_ms => 5000}` as a second argument for a five-second wait. If the deadline expires the caller gets `{error, timeout}` and automatic indexing continues. [Runnable refresh](receivables.erl#L41)

The example worker reads up to 32 changes at a time. A full batch triggers an immediate check for more work. After a shorter batch the worker checks again in 25 ms. It advances its in-memory checkpoint after applying the batch, including removals. The fixture uses one source writer and fresh databases on each run. barrel_design adds durable checkpoints and atomic publication for concurrent readers. [Background worker](receivables_index.erl) · [Maintenance trade-offs](../../wasm-views.md#when-to-update-a-view)

Matching changes get a fresh Wasm instance with 50,000 fuel, a 100 ms deadline, four host calls and one 64 KiB guest memory page. One module load and four guest calls cover the three initially open invoices and the imported invoice. All four BQL reads use the saved rows. [Guest invocation](receivables.erl#L148)

