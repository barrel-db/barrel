%% The receivables design through barrel_design's public API: the Zig
%% guest, the equivalent native projection, BQL reads and a minimal WAT
%% map. Run with `make design-run' from this directory.
-module(receivables_design).
-export([run/0, design/1, native_design/0, seed/1]).

-define(VIEW, {<<"receivables">>, <<"open_by_customer">>}).
-define(SELECT, <<"SELECT * FROM db WHERE type = 'invoice' AND status = 'open'">>).

design(Digest) ->
    Map = #{abi => 1, module => Digest, export => <<"map">>,
            fields => [{[<<"customer">>], text}, {[<<"due_on">>], text},
                       {[<<"line_cents">>], {array, i64}}]},
    View = #{select => ?SELECT, map => Map,
             key => [#{type => text, category => true, title => <<"Customer">>},
                     #{type => text, title => <<"Due">>}],
             value => #{type => i64}, reduce => '_sum'},
    #{name => <<"receivables">>, version => 2, views => #{<<"open_by_customer">> => View}}.

%% The same rows without a guest: key and value read from paths.
native_design() ->
    View = #{select => ?SELECT,
             key => [#{path => [<<"customer">>], type => text, category => true},
                     #{path => [<<"due_on">>], type => text}],
             value => #{sum => [<<"line_cents">>]}, reduce => '_sum'},
    #{name => <<"receivables_native">>, version => 1,
      views => #{<<"open_by_customer">> => View}}.

seed(Db) ->
    Invoices = [{<<"inv-101">>, <<"Ada">>, <<"open">>, <<"2026-12-15">>, [1000, -250]},
                {<<"inv-099">>, <<"Ada">>, <<"paid">>, <<"2026-12-02">>, [9900]},
                {<<"inv-200">>, <<"Bea">>, <<"open">>, <<"2026-12-03">>, [900]},
                {<<"inv-102">>, <<"Ada">>, <<"open">>, <<"2026-12-01">>, [1250, 750, -200]}],
    lists:foreach(fun({Id, Customer, Status, Due, Lines}) ->
        {ok, _} = barrel_docdb:put_doc(Db, invoice(Id, Customer, Status, Due, Lines))
    end, Invoices).

invoice(Id, Customer, Status, Due, Lines) ->
    #{<<"id">> => Id, <<"type">> => <<"invoice">>, <<"customer">> => Customer,
      <<"status">> => Status, <<"due_on">> => Due, <<"line_cents">> => Lines}.

run() ->
    Suffix = binary_to_list(binary:encode_hex(crypto:strong_rand_bytes(8))),
    Dir = filename:absname(filename:join("_build", "receivables-design-" ++ Suffix)),
    ok = file:make_dir(Dir),
    try
        ok = application:load(barrel_docdb),
        ok = application:set_env(barrel_docdb, data_dir, filename:join(Dir, "docdb")),
        ok = application:load(barrel_design),
        ok = application:set_env(barrel_design, data_dir, filename:join(Dir, "design")),
        {ok, _} = application:ensure_all_started(barrel_design),
        try statement() after application:stop(barrel_design) end
    after
        _ = application:stop(barrel_docdb),
        ok = file:del_dir_r(Dir)
    end.

statement() ->
    Db = <<"invoices">>,
    {ok, _} = barrel_docdb:create_db(Db),
    ok = seed(Db),
    {ok, Wasm} = file:read_file("_build/open_by_customer.wasm"),
    Digest = crypto:hash(sha256, Wasm),
    io:format("guest: _build/open_by_customer.wasm, ~B bytes, sha256 ~s~n",
              [byte_size(Wasm), binary:encode_hex(Digest, lowercase)]),
    {ok, #{outcome := installed}} = barrel_design:install(Db, design(Digest), #{Digest => Wasm}),
    io:format("installed receivables@2 over the four seed invoices~n"),

    %% A read right after install may find the first build still running.
    case barrel_design:query(Db, ?VIEW, #{key_prefix => [<<"Ada">>], group_level => 1}) of
        {error, {building, _}} -> io:format("immediate read: building~n");
        {ok, Early, _} -> io:format("immediate read: ~p~n", [Early])
    end,
    {ok, Seeded} = barrel_docdb:db_observed_version(Db),
    Ada = #{key_prefix => [<<"Ada">>], freshness => {wait_for, Seeded}, timeout_ms => 1000},
    {ok, Rows, _} = barrel_design:query(Db, ?VIEW, Ada#{reduce => false, limit => 20}),
    [#{id := <<"inv-102">>, value := 1800}, #{id := <<"inv-101">>, value := 750}] = Rows,
    {ok, [#{key := [<<"Ada">>], value := 2550}] = Total, _} =
        barrel_design:query(Db, ?VIEW, Ada#{reduce => true, group_level => 1}),
    print("Ada before payment", Rows, Total),

    {ok, Invoice} = barrel_docdb:get_doc(Db, <<"inv-102">>),
    {ok, _} = barrel_docdb:put_doc(Db, Invoice#{<<"status">> => <<"paid">>}),
    {ok, Paid} = barrel_docdb:db_observed_version(Db),
    {ok, Stale, #{checkpoint := #{last_seq := StaleSeq}}} =
        barrel_design:query(Db, ?VIEW, #{key_prefix => [<<"Ada">>], group_level => 1}),
    io:format("immediate read after payment: ~s (~s)~n",
              [format_groups(Stale),
               case StaleSeq >= maps:get(last_seq, Paid) of
                   true -> <<"payment already indexed">>;
                   false -> <<"payment not indexed yet">>
               end]),
    {ok, AfterRows, _} = barrel_design:query(Db, ?VIEW,
        Ada#{reduce => false, freshness => {wait_for, Paid}}),
    {ok, [#{value := 750}] = After, _} = barrel_design:query(Db, ?VIEW,
        Ada#{reduce => true, group_level => 1, freshness => {wait_for, Paid}}),
    print("Ada after payment (wait_for the payment)", AfterRows, After),
    {ok, [#{value := 900}] = Bea, _} =
        barrel_design:query(Db, ?VIEW, #{key_prefix => [<<"Bea">>], group_level => 1}),
    io:format("Bea: ~s~n", [format_groups(Bea)]),

    {ok, _} = barrel_docdb:put_doc(Db, invoice(<<"inv-103">>, <<"Ada">>, <<"open">>,
                                               <<"2026-12-20">>, [500])),
    {ok, #{last_seq := _}} = barrel_design:refresh(Db, ?VIEW),
    {ok, Calls0} = guest_calls(Db),
    {ok, StatementRows, _} = barrel_design:query(Db, ?VIEW,
        #{key_prefix => [<<"Ada">>], reduce => false}),
    {ok, [#{value := 1250}] = Statement, _} = barrel_design:query(Db, ?VIEW,
        #{key_prefix => [<<"Ada">>], reduce => true, group_level => 1}),
    {ok, _} = barrel_design:refresh(Db, ?VIEW),
    {ok, Calls0} = guest_calls(Db),
    print("Ada after statement refresh", StatementRows, Statement),
    io:format("reads and an already-current refresh ran 0 guest maps~n"),

    bql(Db),
    native(Db),
    wat(),
    {ok, #{generation := G, errors := Errors, guest := Guest}} = barrel_design:status(Db, ?VIEW),
    io:format("status: generation ~B, ~B errors, ~B guest calls on this node, ~B module load(s)~n",
              [G, Errors, maps:get(guest_calls, Guest),
               maps:get(loads, barrel_design_modules:info())]).

bql(Db) ->
    Rows = <<"SELECT v.id, v.key, v.value FROM view('receivables', 'open_by_customer') AS v "
             "WHERE v.key[0] = 'Ada' ORDER BY v.key">>,
    Groups = <<"SELECT v.key, v.value FROM view('receivables', 'open_by_customer', "
               "group_level => 1) AS v">>,
    {ok, R, _} = barrel_docdb:query(Db, Rows),
    {ok, G, _} = barrel_docdb:query(Db, Groups),
    io:format("BQL: ~s~n  ~p~nBQL: ~s~n  ~p~n", [Rows, R, Groups, G]).

native(Db) ->
    {ok, _} = barrel_design:install(Db, native_design(), #{}),
    NView = {<<"receivables_native">>, <<"open_by_customer">>},
    {ok, _} = barrel_design:refresh(Db, NView),
    {ok, Wasm, _} = barrel_design:query(Db, ?VIEW, #{group_level => 1}),
    {ok, Wasm, _} = barrel_design:query(Db, NView, #{group_level => 1}),
    {ok, WRows, _} = barrel_design:query(Db, ?VIEW, #{reduce => false}),
    {ok, WRows, _} = barrel_design:query(Db, NView, #{reduce => false}),
    io:format("native projection: same ~B rows and totals ~s~n",
              [length(WRows), format_groups(Wasm)]).

%% A minimal WAT map: tried on sample documents, then installed as text.
wat() ->
    {ok, Wat} = file:read_file("by_customer.wat"),
    Fields = [{[<<"customer">>], text}, {[<<"cents">>], i64}],
    View = #{select => <<"SELECT * FROM db WHERE type = 'sale'">>,
             map => #{fields => Fields},
             key => #{type => text}, value => #{type => i64}, reduce => '_sum'},
    Docs = [#{<<"id">> => <<"a">>, <<"type">> => <<"sale">>, <<"customer">> => <<"Ada">>,
              <<"cents">> => 1250},
            #{<<"id">> => <<"b">>, <<"type">> => <<"sale">>, <<"customer">> => <<"Bea">>,
              <<"cents">> => -900},
            #{<<"id">> => <<"c">>, <<"type">> => <<"sale">>, <<"customer">> => <<"Cy">>}],
    {ok, Preview} = barrel_design:preview({wat, Wat}, View, Docs),
    io:format("WAT preview (by_customer.wat):~n"),
    [io:format("  ~s ~p~n", [Id, maps:get(rows, P)]) || #{id := Id} = P <- Preview],

    %% install the text itself: it is stored as its canonical encoding
    {ok, Digest, Bytes} = barrel_design:module({wat, Wat}),
    Db = <<"sales">>,
    {ok, _} = barrel_docdb:create_db(Db),
    [{ok, _} = barrel_docdb:put_doc(Db, D) || D <- Docs],
    Sales = #{name => <<"sales">>, version => 1, views => #{<<"by_customer">> =>
                  View#{map => #{module => Digest, fields => Fields}}}},
    {ok, _} = barrel_design:install(Db, Sales, #{Digest => {wat, Wat}}),
    {ok, _} = barrel_design:refresh(Db, {<<"sales">>, <<"by_customer">>}),
    {ok, Totals, _} = barrel_design:query(Db, {<<"sales">>, <<"by_customer">>}, #{group_level => exact}),
    io:format("WAT installed as ~B bytes, sha256 ~s~n  totals: ~s~n",
              [byte_size(Bytes), binary:encode_hex(Digest, lowercase),
               lists:join(", ", [io_lib:format("~s ~B", [K, V]) || #{key := K, value := V} <- Totals])]).

guest_calls(_Db) ->
    {ok, maps:get(guest_calls, barrel_design_stats:get())}.

print(Title, Rows, Groups) ->
    io:format("~s~n", [Title]),
    [io:format("  ~s  ~s  ~B~n", [Id, Due, V]) || #{id := Id, key := [_, Due], value := V} <- Rows],
    io:format("  subtotal: ~s~n", [format_groups(Groups)]).

format_groups(Groups) ->
    lists:join(", ", [io_lib:format("~s ~B cents", [C, V]) || #{key := [C], value := V} <- Groups]).
