# Maintained views with barrel_design

`barrel_design` keeps views of a database current as documents change. You install a named, versioned design; each of its views selects documents with a BQL predicate and builds ordered key/value rows, either from document paths or with a Wasm map you supply, and can total them per category with `_sum` or `_count`. Use it when an application lists and totals the same documents over and over: open invoices by customer with a balance per customer, sales by month, tasks by assignee. Reads come from the saved rows and never run your map.

## Build it

The app is optional and needs Erlang/OTP 29 (its Wasm runtime, `erlang_wasm` 0.13.0, requires it).

```sh
rebar3 as design compile
```

```erlang
{ok, _} = application:ensure_all_started(barrel_design).
```

## Install a design

A view takes its key and value from document paths. Leading key components marked `category` are the groups a reader expands.

```erlang
Db = <<"invoices">>,
Design = #{name => <<"receivables">>, version => 1,
           views => #{<<"open_by_customer">> => #{
               select => <<"SELECT * FROM db WHERE type = 'invoice' AND status = 'open'">>,
               key => [#{path => [<<"customer">>], type => text, category => true},
                       #{path => [<<"due_on">>], type => text}],
               value => #{sum => [<<"line_cents">>]},
               reduce => '_sum'}}},
{ok, #{outcome := installed}} = barrel_design:install(Db, Design, #{}).
```

The view builds over the documents already there, then follows every write: single, bulk and replicated writes, conflict resolutions and deletes. It keeps following with nobody reading it, and resumes from its checkpoint after a restart. Key types are `text`, `i64`, `bool` and `any`; a value is a `path` with a type, the `sum` of an integer array, or nothing.

## Read a category and its total

```erlang
View = {<<"receivables">>, <<"open_by_customer">>},
{ok, Rows, _} = barrel_design:query(Db, View,
    #{key_prefix => [<<"Ada">>], reduce => false, limit => 20}),
%% [#{id := <<"inv-102">>, key := [<<"Ada">>, <<"2026-12-01">>], value := 1800}, ...]
{ok, [#{key := [<<"Ada">>], value := 2550}], _} =
    barrel_design:query(Db, View, #{key_prefix => [<<"Ada">>], group_level => 1}).
```

- `key`, `key_prefix` (whole leading components), and `start_key`/`end_key` with `inclusive_end` select rows; `limit` and the returned `continuation` page through them.
- `reduce` totals the selected rows, per group of the first `group_level` components (`0` for one total, `exact` for whole keys). It is on by default when the view has a reducer.
- Every answer names the generation and the database checkpoint it covers.

## Wait for a write, or refresh

A read answers at once from the saved rows. To read your own write, wait for it:

```erlang
{ok, _} = barrel_docdb:put_doc(Db, Invoice#{<<"status">> => <<"paid">>}),
{ok, Paid} = barrel_docdb:db_observed_version(Db),
{ok, [#{value := 750}], _} = barrel_design:query(Db, View,
    #{key_prefix => [<<"Ada">>], group_level => 1,
      freshness => {wait_for, Paid}, timeout_ms => 1000}).
```

Before exporting a statement, catch the view up with everything written so far:

```erlang
{ok, #{instance_id := _, last_seq := _}} = barrel_design:refresh(Db, View),
{ok, #{last_seq := _}} = barrel_design:refresh(Db, View, #{timeout_ms => 5000}).
```

A refresh targets the last write at the time of the call and wakes the indexer; concurrent refreshes share its work. A view already current answers at once. On timeout you get `{error, {timeout, Reached}}` and indexing goes on.

## Query a view from BQL

Views are a FROM source, so `barrel_docdb:query/3`, `barrel:query/3` and the HTTP and MCP query routes read them:

```erlang
{ok, Rows, _} = barrel_docdb:query(Db,
    <<"SELECT v.id, v.key, v.value FROM view('receivables', 'open_by_customer') AS v "
      "WHERE v.key[0] = 'Ada' ORDER BY v.key">>),
{ok, Totals, _} = barrel_docdb:query(Db,
    <<"SELECT v.key, v.value FROM view('receivables', 'open_by_customer', group_level => 1) AS v">>).
```

Equalities on `v.key[0]`, `v.key[1]`, ... and one range on the next component narrow the scan; BQL still checks every condition. Pass `freshness` and `timeout_ms` in the query options.

## Map documents with Wasm

When paths are not enough, write a map. It imports two functions from `barrel_v1`: `field(slot, dest, capacity)` copies a declared field, as CBOR, into guest memory and returns its length or `-1` when the document lacks it; `emit(key, key_len, value, value_len)` stages one row (CBOR integers, UTF-8 texts, booleans, null and arrays). Write it in WAT, which installs as it is, or build it with any toolchain that produces a freestanding `wasm32` module:

```sh
zig build-exe open_by_customer.zig -target wasm32-freestanding -O ReleaseSmall \
  -fno-entry --export=map --stack 16384 \
  --initial-memory=65536 --max-memory=65536 -femit-bin=open_by_customer.wasm
```

Try it on sample documents first, from WAT or the binary:

```erlang
Sales = #{select => <<"SELECT * FROM db WHERE type = 'sale'">>,
          map => #{fields => [{[<<"customer">>], text}, {[<<"cents">>], i64}]},
          key => #{type => text}, value => #{type => i64}, reduce => '_sum'},
{ok, Wat} = file:read_file("by_customer.wat"),
{ok, [#{id := <<"a">>, rows := [#{key := <<"Ada">>, value := 1250}]}]} =
    barrel_design:preview({wat, Wat}, Sales,
        [#{<<"id">> => <<"a">>, <<"type">> => <<"sale">>,
           <<"customer">> => <<"Ada">>, <<"cents">> => 1250}]).
```

Then install it, named by the SHA-256 of its bytes. Text is installed through its canonical encoding, so comments, layout and `$identifiers` do not change the digest:

```erlang
{ok, Digest, _Bytes} = barrel_design:module({wat, Wat}),
{ok, _} = barrel_design:install(Db,
    #{name => <<"sales">>, version => 1,
      views => #{<<"by_customer">> => Sales#{map => #{module => Digest,
          fields => [{[<<"customer">>], text}, {[<<"cents">>], i64}]}}}},
    #{Digest => {wat, Wat}}).
```

A compiler's binary keeps its own digest:

```erlang
{ok, Wasm} = file:read_file("open_by_customer.wasm"),
Digest = crypto:hash(sha256, Wasm),
Map = #{abi => 1, module => Digest, export => <<"map">>,
        fields => [{[<<"customer">>], text}, {[<<"due_on">>], text},
                   {[<<"line_cents">>], {array, i64}}]},
Open = #{select => <<"SELECT * FROM db WHERE type = 'invoice' AND status = 'open'">>,
         map => Map, key => [#{type => text, category => true}, #{type => text}],
         value => #{type => i64}, reduce => '_sum'},
{ok, _} = barrel_design:install(Db, #{name => <<"receivables">>, version => 2,
                                      views => #{<<"open_by_customer">> => Open}},
                                #{Digest => Wasm}).
```

A module is admitted only with those two imports, one bounded 32-bit memory and no start function, SIMD, atomics, exceptions or GC. Each document runs in a fresh instance under these limits, which a view's `limits` map can change:

| Limit | Default | Range |
|---|---:|---:|
| `fuel` | 50,000 | 1 to 10,000,000 |
| `timeout_ms` | 1,000 | 1 to 5,000 |
| `max_memory_pages` (64 KiB) | 1 | 1 to 16 |
| `max_rows` per document | 16 | 1 to 1,024 |
| `max_key_bytes`, `max_value_bytes` | 512, 256 | up to 1,024, 4,096 |
| `max_field_bytes` | 4,096 | up to 65,536 |
| `max_host_calls`, `max_depth`, `max_heap_words` | 256, 64, 262,144 | |

Node-wide, `node_jobs` (default: one per scheduler) and `db_jobs` (default 2) cap concurrent guest batches, and `page_limit` sets the engine's page budget.

## Change a design

Install a higher version. A view whose selector, keys, value, reducer, map or limits are unchanged keeps its rows, so renaming a column (`title`) costs nothing; any other change builds a new generation while the current one keeps answering, and readers switch when it has caught up. A continuation from the old generation then returns `{error, stale_cursor}`. `barrel_design:rebuild/2` builds a view again from scratch, and `uninstall/2` removes a design and its rows.

## Check a view

```erlang
{ok, #{generation := 1, status := complete, checkpoint := _, errors := 0,
       guest := #{guest_calls := _}, store_bytes := _}} = barrel_design:status(Db, View).
```

## Notes

- Designs are stored as local documents of the database: they are part of its backups and are never replicated. Install them on each node that serves the views.
- A map that fails on a document (trap, fuel, deadline, a limit) removes that document's rows and counts in `errors` until the document or the design changes.
- Keys sort null, false, true, integers, texts, arrays; texts by their UTF-8 bytes, not a locale collation.
- An expired document leaves rows and totals as soon as it expires, before any TTL sweep.
- A view of an encrypted database stores its rows encrypted with the database key. A database deleted and recreated under the same name, restored or branched starts from an empty index.
- A view exposes data from every document it indexes. `query/3` and `refresh/3` take an `authorize` fun of the database name; a server must allow the caller to read the whole database.
