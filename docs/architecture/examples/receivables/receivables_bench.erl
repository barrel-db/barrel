%% One fresh VM and data directory per mode and trial, driven by "make bench".
-module(receivables_bench).
-export([run/0]).

-define(PHASE_SIZE, 1000).
-define(READS, 100).
-define(CUSTOMERS, 64).

run() ->
    [Trial, ModeName] = init:get_plain_arguments(),
    Mode = case ModeName of "off" -> off; "native" -> native; "wasm" -> wasm end,
    Suffix = binary_to_list(binary:encode_hex(crypto:strong_rand_bytes(12))),
    Dir = filename:absname(filename:join("_build", "receivables-bench-" ++ Suffix)),
    ok = file:make_dir(Dir),
    try
        ok = application:load(barrel_docdb),
        ok = application:set_env(barrel_docdb, data_dir, Dir),
        {ok, _} = application:ensure_all_started(barrel_docdb),
        {ok, _} = application:ensure_all_started(wasm),
        Session = trace:session_create(?MODULE, self(), []),
        try
            count_calls(Session, wasm, load, 1),
            count_calls(Session, wasm_instance_worker, call, 4),
            run_trial(Trial, Mode, Session)
        after
            trace:session_destroy(Session)
        end
    after
        _ = application:stop(barrel_docdb),
        ok = file:del_dir_r(Dir)
    end.

count_calls(Session, M, F, A) ->
    {module, M} = code:ensure_loaded(M),
    1 = trace:function(Session, {M, F, A}, true, [call_count]).

counters(Session) ->
    {call_count, Loads} = trace:info(Session, {wasm, load, 1}, call_count),
    {call_count, Calls} = trace:info(Session, {wasm_instance_worker, call, 4}, call_count),
    [Loads, Calls].

run_trial(Trial, Mode, Session) ->
    Docs = [invoice(I) || I <- lists:seq(1, 2 * ?PHASE_SIZE)],
    {Seed, Live} = lists:split(?PHASE_SIZE, Docs),
    {ok, Db} = barrel_docdb:create_db(<<"invoices">>),
    lists:foreach(fun(Doc) -> {ok, _} = barrel_docdb:put_doc(Db, Doc) end, Seed),
    Measurements = case Mode of
        off ->
            Writes = write_times(Db, Live),
            [na, na] ++ percentiles(Writes) ++ [na, na, na, na, na];
        _ ->
            Expected = expected_rows(Docs),
            {ok, Bytes} = file:read_file("_build/open_by_customer.wasm"),
            #{views := #{<<"open_by_customer">> := Definition}} =
                receivables:design(crypto:hash(sha256, Bytes)),
            {LoadUs, Project} = case Mode of
                native -> {na, fun native_map/1};
                wasm ->
                    {Elapsed, {ok, Module}} = timer:tc(fun() -> wasm:load(Bytes) end),
                    {Elapsed, Module}
            end,
            try
                [LoadUs | measure_view(Db, Live, Expected, Project, Definition, Session)]
            after
                case Mode of wasm -> ok = wasm:unload(Project); native -> ok end
            end
    end,
    Counts = counters(Session),
    case Mode of
        wasm ->
            Matching = length([ok || #{<<"status">> := <<"open">>} <- Docs]),
            [1, Matching] = Counts;
        _ -> [0, 0] = Counts
    end,
    Fields = [Trial, atom_to_list(Mode) | [csv(V) || V <- Measurements ++ Counts]],
    io:format("~s~n", [lists:join($,, Fields)]).

measure_view(Db, Live, Expected, Project, Definition, Session) ->
    ViewDb = <<"open_by_customer">>,
    {ok, _} = barrel_docdb:create_db(ViewDb),
    {ok, Sup} = supervisor:start_link(receivables, []),
    try
        {ok, Seeded} = barrel_docdb:db_observed_version(Db),
        {Backfill, View = #{indexer := Index}} = timer:tc(fun() ->
            V = receivables:start_view(Sup, Db, ViewDb, Project, Definition),
            ok = receivables_index:wait_for(maps:get(indexer, V), Seeded, 60000),
            V
        end),
        Writes = write_times(Db, Live),
        {Catchup, ok} = timer:tc(fun() ->
            {ok, Target} = barrel_docdb:db_observed_version(Db),
            receivables_index:wait_for(Index, Target, 60000)
        end),
        Counts = counters(Session),
        check_rows(View, Expected),
        Customer = <<"0">>,
        {_, _} = receivables:query(View, Customer),
        Queries = samples(fun() -> receivables:query(View, Customer) end),
        Refreshes = samples(fun() -> {ok, _} = receivables:refresh(View) end),
        Counts = counters(Session),
        [Backfill] ++ percentiles(Writes) ++ [Catchup] ++ percentiles(Queries)
            ++ [percentile(95, lists:sort(Refreshes))]
    after
        ok = gen_server:stop(Sup)
    end.

write_times(Db, Docs) ->
    lists:map(fun(Doc) ->
        {Us, {ok, _}} = timer:tc(fun() -> barrel_docdb:put_doc(Db, Doc) end),
        Us
    end, Docs).

samples(Fun) ->
    lists:map(fun(_) ->
        {Us, _} = timer:tc(Fun),
        Us
    end, lists:seq(1, ?READS)).

percentiles(Times) ->
    Sorted = lists:sort(Times),
    [percentile(P, Sorted) || P <- [50, 95, 99]].

percentile(P, Sorted) ->
    lists:nth((P * length(Sorted) + 99) div 100, Sorted).

csv(na) -> "NA";
csv(N) -> integer_to_list(N).

invoice(I) ->
    Day = iolist_to_binary(io_lib:format("~2..0B", [1 + I rem 28])),
    Status = case I rem 5 of 0 -> <<"paid">>; _ -> <<"open">> end,
    #{<<"id">> => integer_to_binary(I), <<"type">> => <<"invoice">>, <<"status">> => Status,
      <<"customer">> => integer_to_binary(I rem ?CUSTOMERS), <<"due_on">> => <<"2026-12-", Day/binary>>,
      <<"line_cents">> => [1000 + I rem 700, -250, 25]}.

native_map(#{<<"customer">> := Customer, <<"due_on">> := Due, <<"line_cents">> := Lines}) ->
    #{<<"customer">> => Customer, <<"due_on">> => Due, <<"balance_cents">> => lists:sum(Lines)}.

expected_rows(Docs) ->
    Open = [Doc || #{<<"status">> := <<"open">>} = Doc <- Docs],
    maps:groups_from_list(fun(#{<<"customer">> := Customer}) -> Customer end,
        fun(#{<<"id">> := Id, <<"due_on">> := Due, <<"line_cents">> := Lines}) ->
            {Id, Due, lists:sum(Lines)}
        end, Open).

check_rows(View, Expected) ->
    ?CUSTOMERS = map_size(Expected),
    maps:foreach(fun(Customer, Want) ->
        {Rows, Total} = receivables:query(View, Customer),
        true = length(Rows) =:= length(Want),
        Actual = [{Id, Due, Cents} ||
            #{<<"id">> := Id, <<"due_on">> := Due, <<"balance_cents">> := Cents} <- Rows],
        Dates = [Due || {_, Due, _} <- Actual],
        Dates = lists:sort(Dates),
        true = lists:sort(Want) =:= lists:sort(Actual),
        Total = lists:sum([Cents || {_, _, Cents} <- Want])
    end, Expected).
