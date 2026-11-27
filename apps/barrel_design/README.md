# barrel_design

Versioned designs with automatically maintained views over barrel_docdb. A design packages named views for a database; each view selects documents with a BQL predicate and builds ordered key/value rows with a native projection or a Wasm map. Use it when an application needs categorized lists and totals (open invoices by customer, sales by month) that stay current as documents change.

The app is optional and needs Erlang/OTP 29 (its runtime, erlang_wasm 0.11.2, requires it). Build and test it from the umbrella:

```sh
rebar3 as design compile
rebar3 as design ct --dir apps/barrel_design/test
```

Try a map on sample documents without installing anything:

```erlang
{ok, _} = application:ensure_all_started(barrel_design),
View = #{select => <<"SELECT * FROM db WHERE type = 'sale'">>,
         map => #{fields => [{[<<"customer">>], text}, {[<<"cents">>], i64}]},
         key => #{type => text}, value => #{type => i64}, reduce => '_sum'},
{ok, Wat} = file:read_file("test/guests/by_customer.wat"),
{ok, Rows} = barrel_design:preview({wat, Wat}, View,
    [#{<<"id">> => <<"a">>, <<"type">> => <<"sale">>,
       <<"customer">> => <<"Ada">>, <<"cents">> => 1250}]).
```

The test guests in `test/guests` rebuild with `make -C test/guests` (wasm-tools 1.256.0, Zig 0.17.1).
