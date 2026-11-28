%% A runnable host for the accompanying Zig invoice map.
-module(receivables).
-behaviour(supervisor).
-export([run/0, seed/1, design/1, start_view/5, query/2, query/3, refresh/1, refresh/2, init/1]).

design(Digest) ->
    Map = #{abi => 1, module => Digest, export => <<"map">>,
            fields => [{[<<"customer">>], text}, {[<<"due_on">>], text},
                       {[<<"line_cents">>], {array, i64}}]},
    View = #{select => <<"SELECT * FROM db WHERE type = 'invoice' AND status = 'open'">>,
             map => Map, key => [#{type => text, category => true}, #{type => text}],
             value => #{type => i64}, reduce => '_sum'},
    #{name => <<"receivables">>, version => 2, views => #{<<"open_by_customer">> => View}}.

seed(Db) ->
    Invoices = [{<<"inv-101">>, <<"Ada">>, <<"open">>, <<"2026-12-15">>, [1000, -250]},
                {<<"inv-099">>, <<"Ada">>, <<"paid">>, <<"2026-12-02">>, [9900]},
                {<<"inv-200">>, <<"Bea">>, <<"open">>, <<"2026-12-03">>, [900]},
                {<<"inv-102">>, <<"Ada">>, <<"open">>, <<"2026-12-01">>, [1250, 750, -200]}],
    lists:foreach(fun({Id, Customer, Status, Due, Lines}) ->
        {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => Id, <<"type">> => <<"invoice">>,
            <<"customer">> => Customer, <<"status">> => Status,
            <<"due_on">> => Due, <<"line_cents">> => Lines})
    end, Invoices).

query(#{rows := ViewDb}, Customer) -> query(ViewDb, Customer);
query(ViewDb, Customer) ->
    Bql = <<"SELECT id, due_on, balance_cents FROM db "
            "WHERE customer = $customer ORDER BY due_on">>,
    {ok, Rows, #{has_more := false}} = barrel_docdb:query(ViewDb, Bql,
        #{params => #{<<"customer">> => Customer}}),
    {Rows, lists:sum([Cents || #{<<"balance_cents">> := Cents} <- Rows])}.

query(#{indexer := Index, rows := ViewDb}, Customer,
      #{freshness := {wait_for, Target}, timeout_ms := Timeout}) ->
    case receivables_index:wait_for(Index, Target, Timeout) of
        ok -> query(ViewDb, Customer);
        {error, _} = Error -> Error
    end.

refresh(View) -> refresh(View, #{}).

refresh(#{indexer := Index}, Options) when is_map(Options) ->
    receivables_index:refresh(Index, maps:get(timeout_ms, Options, 1000)).

run() ->
    %% Each run owns its data directory, including Barrel's system database.
    Suffix = binary_to_list(binary:encode_hex(crypto:strong_rand_bytes(12))),
    Dir = filename:absname(filename:join("_build", "receivables-" ++ Suffix)),
    ok = file:make_dir(Dir),
    try
        ok = application:load(barrel_docdb),
        ok = application:set_env(barrel_docdb, data_dir, Dir),
        {ok, _} = application:ensure_all_started(barrel_docdb),
        {ok, _} = application:ensure_all_started(wasm),
        {ok, Bytes} = file:read_file("_build/open_by_customer.wasm"),
        #{views := #{<<"open_by_customer">> := View}} = design(crypto:hash(sha256, Bytes)),
        {ok, Module} = wasm:load(Bytes),
        try run_invoices(Module, View)
        after ok = wasm:unload(Module)
        end
    after
        _ = application:stop(barrel_docdb),
        ok = file:del_dir_r(Dir)
    end.

run_invoices(Module, Definition) ->
    Db = <<"invoices">>,
    ViewDb = <<"open_by_customer">>,
    {ok, Source} = barrel_docdb:create_db(Db),
    {ok, _} = barrel_docdb:create_db(ViewDb),
    ok = seed(Db),
    {ok, Sup} = supervisor:start_link(?MODULE, []),
    try
        View = start_view(Sup, Source, ViewDb, Module, Definition),
        {ok, Seeded} = barrel_docdb:db_observed_version(Db),
        Before = query(View, <<"Ada">>, #{freshness => {wait_for, Seeded}, timeout_ms => 1000}),
        {[#{<<"id">> := <<"inv-102">>, <<"balance_cents">> := 1800},
          #{<<"id">> := <<"inv-101">>, <<"balance_cents">> := 750}], 2550} = Before,
        print("Ada before payment", Before),

        {ok, Invoice} = barrel_docdb:get_doc(Db, <<"inv-102">>),
        {ok, _} = barrel_docdb:put_doc(Db, Invoice#{<<"status">> => <<"paid">>}),
        {ok, Paid} = barrel_docdb:db_observed_version(Db),
        After = query(View, <<"Ada">>, #{freshness => {wait_for, Paid}, timeout_ms => 1000}),
        {[#{<<"id">> := <<"inv-101">>}], 750} = After,
        {[#{<<"id">> := <<"inv-200">>}], 900} = query(View, <<"Bea">>),
        print("Ada after payment", After),

        {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"inv-103">>,
            <<"type">> => <<"invoice">>, <<"status">> => <<"open">>, <<"customer">> => <<"Ada">>,
            <<"due_on">> => <<"2026-12-20">>, <<"line_cents">> => [500]}),
        {ok, #{instance_id := _, last_seq := _}} = refresh(View),
        Statement = query(View, <<"Ada">>),
        {[#{<<"id">> := <<"inv-101">>}, #{<<"id">> := <<"inv-103">>}], 1250} = Statement,
        print("Ada after statement refresh", Statement)
    after
        ok = gen_server:stop(Sup)
    end.

start_view(Sup, Source, ViewDb, Mapper, #{select := Bql, map := Map}) ->
    Project = case Mapper of
        Native when is_function(Native, 1) -> Native;
        Module -> fun(Doc) -> map_invoice(Sup, Module, Map, Doc) end
    end,
    {ok, #{spec := Spec}} = barrel_bql:compile(Bql),
    {ok, Predicate} = barrel_query:compile(Spec),
    Apply = fun(Change = #{id := Id}) ->
        Row = case Change of
            #{deleted := true} -> none;
            #{doc := Doc} ->
                case barrel_query:match(Predicate, Doc) of
                    true -> Project(Doc);
                    false -> none
                end
        end,
        replace_row(ViewDb, Id, Row)
    end,
    Child = #{id => {indexer, ViewDb}, restart => temporary,
              start => {receivables_index, start_link, [Source, Apply]}},
    {ok, Index} = supervisor:start_child(Sup, Child),
    #{rows => ViewDb, indexer => Index}.

print(Title, {Rows, Total}) ->
    io:format("~s~n", [Title]),
    lists:foreach(fun(#{<<"id">> := Id, <<"due_on">> := Due, <<"balance_cents">> := Cents}) ->
        io:format("  ~s  ~s  ~B~n", [Id, Due, Cents])
    end, Rows),
    io:format("  subtotal: ~B cents~n", [Total]).

%% At most one row is emitted per invoice, so its source ID identifies the row.
replace_row(Db, Id, none) ->
    case barrel_docdb:get_doc(Db, Id) of
        {error, not_found} -> ok;
        {ok, #{<<"_rev">> := Rev}} ->
            {ok, _} = barrel_docdb:delete_doc(Db, Id, #{rev => Rev})
    end;
replace_row(Db, Id, Row) ->
    [Previous] = barrel_docdb:get_docs(Db, [Id], #{include_deleted => true}),
    Doc = case Previous of
        {error, not_found} -> Row;
        {ok, #{<<"_rev">> := Rev}} -> Row#{<<"_rev">> => Rev}
    end,
    {ok, _} = barrel_docdb:put_doc(Db, Doc#{<<"id">> => Id}).

init([]) -> {ok, {#{strategy => one_for_one}, []}}.

map_invoice(Sup, Module, #{export := Export, fields := Fields}, Doc) ->
    Stage = ets:new(emissions, [set, public]),
    Slots = list_to_tuple(Fields),
    Imports = #{{<<"barrel_v1">>, <<"field">>} =>
                    fun(Ctx, Args) -> field(Ctx, Args, Slots, Doc) end,
                {<<"barrel_v1">>, <<"emit">>} =>
                    fun(Ctx, Args) -> emit(Ctx, Args, Stage) end},
    Limits = (wasm_limits:untrusted())#{fuel => 50000, max_depth => 32,
        max_memory_pages => 1, max_host_calls => 4, max_heap_words => 262144, timeout => 100},
    Opts = #{imports => Imports, limits => Limits, isolation => fresh},
    Job = make_ref(),
    Child = #{id => Job, restart => temporary,
              start => {wasm_instance_worker, start_link, [Module, Opts]}},
    try
        {ok, Worker} = supervisor:start_child(Sup, Child),
        {ok, []} = wasm_instance_worker:call(Worker, Export, [], 100),
        case ets:lookup(Stage, row) of
            [] -> none;
            [{row, Row}] -> Row
        end
    after
        _ = supervisor:terminate_child(Sup, Job),
        ets:delete(Stage)
    end.

field(Ctx, [Slot, Ptr, Cap], Slots, Doc)
        when Slot >= 0, Slot < tuple_size(Slots), Ptr >= 0, Cap >= 0 ->
    {[Name], Type} = element(Slot + 1, Slots),
    case field_bytes(Type, maps:get(Name, Doc, missing)) of
        missing -> {ok, [-1]};
        Bytes when is_binary(Bytes), byte_size(Bytes) =< Cap ->
            ok = wasm:write_memory(Ctx, Ptr, Bytes),
            {ok, [byte_size(Bytes)]};
        _ -> {trap, field_too_large}
    end.

field_bytes(text, V) when is_binary(V), byte_size(V) =< 256 ->
    case barrel_docdb_codec_cbor:encode_wire(V) of
        Text = <<3:3, _/bitstring>> -> Text;
        _ -> missing
    end;
field_bytes(text, V) when is_binary(V) -> too_large;
field_bytes({array, i64}, V) when is_list(V) ->
    case lists:sublist(V, 65) of
        Lines when length(Lines) > 64 -> too_large;
        Lines ->
            case lists:all(fun is_i64/1, Lines) of
                true -> barrel_docdb_codec_cbor:encode_wire(Lines);
                false -> missing
            end
    end;
field_bytes(_, _) -> missing.

is_i64(N) -> is_integer(N) andalso N >= -(1 bsl 63) andalso N < (1 bsl 63).

emit(Ctx, [KP, KL, VP, VL], Stage)
        when KP >= 0, KL > 0, KL =< 289, VP >= 0, VL > 0, VL =< 9 ->
    {ok, K} = wasm:read_memory(Ctx, KP, KL),
    {ok, V} = wasm:read_memory(Ctx, VP, VL),
    [Customer, Due] = barrel_docdb_codec_cbor:decode_cbor(K),
    Cents = barrel_docdb_codec_cbor:decode_cbor(V),
    true = is_binary(Customer) andalso is_binary(Due) andalso is_i64(Cents),
    true = ets:insert_new(Stage, {row, #{<<"customer">> => Customer,
        <<"due_on">> => Due, <<"balance_cents">> => Cents}}),
    {ok, []}.
