%% Benchmark of barrel_design's public install, write, refresh and query
%% paths. One fresh VM and data directory per trial and mode, driven by
%% `make -C docs/architecture/examples/receivables design-bench'.
%%
%% Modes: `off' (no view: the writes alone), `native' (the native
%% projection of the receivables view), `wasm' (the Zig guest), `fanout'
%% (a WAT guest emitting one row per invoice line), `tenants' (a light
%% database's latencies while another backfills a Wasm view), `limits'
%% (time to stop a runaway guest, recovery after a crash) and `cold'
%% (first engine load of the guest, warm cost of one fresh-instance map).
%%
%% Rows are checked against the expected rows and totals before any
%% timing is reported. Times are microseconds unless the column says
%% otherwise. Results append to CSV files in $BENCH_OUT (default
%% _build/bench): views.csv, tenants.csv, limits.csv, cold.csv.
-module(barrel_design_bench).
-export([header/0, run/0]).

-define(PHASE, 1000).
-define(CUSTOMERS, 64).
-define(READS, 200).
-define(BURST, 1000).
-define(RATE, 500).
-define(SUSTAIN_MS, 4000).
-define(READERS, 4).
-define(SELECT, <<"SELECT * FROM db WHERE type = 'invoice' AND status = 'open'">>).

-define(VIEWS_COLS, "trial,mode,seed_docs,seed_rows,install_us,first_generation_us,"
        "backfill_docs_per_s,backfill_rows_per_s,write_p50_us,write_p95_us,write_p99_us,"
        "write_samples,catchup_us,query_p50_us,query_p95_us,query_p99_us,group_p95_us,"
        "query_samples,refresh_current_p95_us,burst_docs,refresh_backlog_us,"
        "sustained_rate_per_s,sustained_writes,sustained_write_p95_us,sustained_write_p99_us,"
        "lag_p50_ms,lag_p95_ms,lag_max_ms,lag_samples,reader_p50_us,reader_p95_us,"
        "reader_p99_us,reader_samples,module_loads,guest_calls,read_guest_calls,"
        "compiled_entries,peak_rss_kb,peak_guest_pages,index_bytes,source_bytes").
-define(TENANT_COLS, "trial,b_write_p95_alone_us,b_write_p95_shared_us,b_write_p99_alone_us,"
        "b_write_p99_shared_us,b_read_p95_alone_us,b_read_p95_shared_us,b_read_p99_alone_us,"
        "b_read_p99_shared_us,b_samples,a_backfill_docs,a_backfill_us").
-define(LIMIT_COLS, "trial,deadline_ms,terminate_ms,fuel,out_of_fuel_us,pages_before,"
        "pages_after,recovery_docs,docs_before_crash,recovery_ms,recovery_valid").
-define(COLD_COLS, "trial,module_bytes,first_load_us,warm_map_p50_us,warm_map_p95_us,"
        "warm_map_p99_us,batches,batch_docs,wat_bytes,wat_encode_first_us,wat_encode_p50_us").

header() ->
    Out = out_dir(),
    ok = filelib:ensure_path(Out),
    [ok = file:write_file(filename:join(Out, F), [H, $\n]) || {F, H} <-
        [{"views.csv", ?VIEWS_COLS}, {"tenants.csv", ?TENANT_COLS}, {"limits.csv", ?LIMIT_COLS},
         {"cold.csv", ?COLD_COLS}]],
    io:format("writing ~s/{views,tenants,limits,cold}.csv~n", [Out]).

out_dir() -> os:getenv("BENCH_OUT", filename:absname("_build/bench")).

run() ->
    [Trial, ModeName] = init:get_plain_arguments(),
    Mode = list_to_atom(ModeName),
    Dir = filename:absname(filename:join("_build", "design-bench-" ++
              binary_to_list(binary:encode_hex(crypto:strong_rand_bytes(8))))),
    ok = file:make_dir(Dir),
    try
        ok = application:load(barrel_docdb),
        ok = application:set_env(barrel_docdb, data_dir, filename:join(Dir, "docdb")),
        ok = application:load(barrel_design),
        ok = application:set_env(barrel_design, data_dir, filename:join(Dir, "design")),
        {ok, _} = application:ensure_all_started(barrel_design),
        Sampler = start_sampler(),
        try
            Row = case Mode of
                tenants -> {"tenants.csv", tenants(Trial)};
                limits -> {"limits.csv", limits(Trial)};
                cold -> {"cold.csv", cold(Trial)};
                _ -> {"views.csv", views(Trial, Mode, Sampler, Dir)}
            end,
            {File, Fields} = Row,
            Line = [lists:join($,, [csv(F) || F <- Fields]), $\n],
            ok = file:write_file(filename:join(out_dir(), File), Line, [append]),
            io:format("~s: ~s", [File, Line])
        after
            Sampler ! stop
        end
    after
        _ = application:stop(barrel_design),
        _ = application:stop(barrel_docdb),
        ok = file:del_dir_r(Dir)
    end.

%%====================================================================
%% Views
%%====================================================================

views(Trial, Mode, Sampler, Dir) ->
    Db = <<"invoices">>,
    {ok, _} = barrel_docdb:create_db(Db),
    Docs = [invoice(I) || I <- lists:seq(1, 2 * ?PHASE)],
    {Seed, Live} = lists:split(?PHASE, Docs),
    [{ok, _} = barrel_docdb:put_doc(Db, D) || D <- Seed],
    View = {<<"bench">>, <<"open_by_customer">>},
    Calls0 = guest_calls(),
    {InstallUs, FirstGenUs} = case Mode of
        off -> {na, na};
        _ ->
            {ok, Seeded} = barrel_docdb:db_observed_version(Db),
            T0 = now_us(),
            {IUs, {ok, _}} = timer:tc(fun() -> install(Db, Mode) end),
            {ok, _, _} = barrel_design:query(Db, View, #{limit => 1, reduce => false,
                freshness => {wait_for, Seeded}, timeout_ms => 120000}),
            {IUs, now_us() - T0}
    end,
    SeedRows = case Mode of off -> na; _ -> length(all_rows(Db, View)) end,
    Writes = [timed(fun() -> {ok, _} = barrel_docdb:put_doc(Db, D) end) || D <- Live],
    CatchupUs = case Mode of
        off -> na;
        _ ->
            {ok, T} = barrel_docdb:db_observed_version(Db),
            element(1, timer:tc(fun() ->
                {ok, _, _} = barrel_design:query(Db, View, #{limit => 1, reduce => false,
                    freshness => {wait_for, T}, timeout_ms => 120000})
            end))
    end,
    ok = validate(Db, View, Mode, Docs),
    {Q, G, RefreshP95, ReadCalls} = case Mode of
        off -> {na, na, na, na};
        _ ->
            Customer = <<"7">>,
            _ = barrel_design:query(Db, View, drill(Mode, Customer)),
            C0 = guest_calls(),
            QS = [timed(fun() -> {ok, _, _} = barrel_design:query(Db, View, drill(Mode, Customer)) end)
                  || _ <- lists:seq(1, ?READS)],
            GS = [timed(fun() -> {ok, _, _} = barrel_design:query(Db, View, group(Mode, Customer)) end)
                  || _ <- lists:seq(1, ?READS)],
            RS = [timed(fun() -> {ok, _} = barrel_design:refresh(Db, View) end)
                  || _ <- lists:seq(1, 100)],
            {QS, GS, pct(95, RS), guest_calls() - C0}
    end,
    BurstUs = case Mode of
        off -> na;
        _ ->
            Burst = [invoice(I) || I <- lists:seq(10001, 10000 + ?BURST)],
            [_ | _] = lists:append([barrel_docdb:put_docs(Db, Chunk) || Chunk <- chunks(Burst, 100)]),
            element(1, timer:tc(fun() ->
                {ok, _} = barrel_design:refresh(Db, View, #{timeout_ms => 120000}) end))
    end,
    {SWrites, Lags, ReaderTimes} = sustained(Db, View, Mode),
    case Mode of
        off -> ok;
        _ ->
            {ok, _} = barrel_design:refresh(Db, View, #{timeout_ms => 120000}),
            ok = validate_rows(Db, View, Mode)
    end,
    Loads = maps:get(loads, barrel_design_modules:info()),
    Compiled = maps:get(entered, wasm_jit:counts(), 0),
    {Rss, Pages} = peaks(Sampler),
    IndexBytes = case Mode of
        off -> na;
        _ -> {ok, #{store_bytes := B}} = barrel_design:status(Db, View), B
    end,
    SourceBytes = dir_bytes(filename:join([Dir, "docdb", "invoices"])),
    Backfill = fun(N) -> case FirstGenUs of na -> na; Us -> round(N * 1.0e6 / Us) end end,
    [Trial, atom_to_list(Mode), ?PHASE, SeedRows, InstallUs, FirstGenUs,
     Backfill(?PHASE), case SeedRows of na -> na; R -> Backfill(R) end]
        ++ pcts(Writes) ++ [length(Writes), CatchupUs]
        ++ maybe_pcts(Q) ++ [maybe_pct(95, G), samples(Q), RefreshP95,
                             case BurstUs of na -> na; _ -> ?BURST end, BurstUs,
                             ?RATE, length(SWrites), pct(95, SWrites), pct(99, SWrites)]
        ++ lag_stats(Lags) ++ maybe_pcts(ReaderTimes) ++ [samples(ReaderTimes)]
        ++ [Loads, guest_calls() - Calls0, ReadCalls, Compiled, Rss, Pages, IndexBytes, SourceBytes].

install(Db, Mode) ->
    {Design, Modules} = design(Mode),
    barrel_design:install(Db, Design, Modules).

design(native) ->
    {#{name => <<"bench">>, version => 1, views => #{<<"open_by_customer">> => #{
        select => ?SELECT,
        key => [#{path => [<<"customer">>], type => text, category => true},
                #{path => [<<"due_on">>], type => text}],
        value => #{sum => [<<"line_cents">>]}, reduce => '_sum'}}}, #{}};
design(wasm) ->
    {ok, Wasm} = file:read_file("_build/open_by_customer.wasm"),
    D = crypto:hash(sha256, Wasm),
    {#{name => <<"bench">>, version => 1, views => #{<<"open_by_customer">> => #{
        select => ?SELECT,
        map => #{module => D, fields => [{[<<"customer">>], text}, {[<<"due_on">>], text},
                                         {[<<"line_cents">>], {array, i64}}]},
        key => [#{type => text, category => true}, #{type => text}],
        value => #{type => i64}, reduce => '_sum'}}}, #{D => Wasm}};
design(fanout) ->
    {ok, Wasm} = file:read_file(guest_path("fanout.wasm")),
    D = crypto:hash(sha256, Wasm),
    {#{name => <<"bench">>, version => 1, views => #{<<"open_by_customer">> => #{
        select => ?SELECT,
        map => #{module => D, fields => [{[<<"line_cents">>], {array, i64}}]},
        key => [#{type => i64, category => true}], value => #{type => i64},
        reduce => '_sum'}}}, #{D => Wasm}}.

guest_path(Name) ->
    filename:join(["..", "..", "..", "..", "apps", "barrel_design", "test", "guests", Name]).

drill(fanout, _C) -> #{key_prefix => [1], reduce => false, limit => 25};
drill(_, C) -> #{key_prefix => [C], reduce => false, limit => 100}.

group(fanout, _C) -> #{group_level => 1};
group(_, C) -> #{key_prefix => [C], group_level => 1}.

invoice(I) ->
    Day = iolist_to_binary(io_lib:format("~2..0B", [1 + I rem 28])),
    Status = case I rem 5 of 0 -> <<"paid">>; _ -> <<"open">> end,
    #{<<"id">> => integer_to_binary(I), <<"type">> => <<"invoice">>, <<"status">> => Status,
      <<"customer">> => integer_to_binary(I rem ?CUSTOMERS),
      <<"due_on">> => <<"2026-12-", Day/binary>>,
      <<"line_cents">> => [1000 + I rem 700, -250, 25]}.

%% Writes at a fixed rate (updates, inserts, deletes) with readers
%% running; a sampler measures, every 100 ms, how long the view takes
%% to cover the last write.
sustained(Db, View, Mode) ->
    Self = self(),
    Stop = erlang:monotonic_time(millisecond) + ?SUSTAIN_MS,
    Readers = case Mode of
        off -> [];
        _ -> [spawn_link(fun() -> Self ! {reader, self(), reader(Db, View, Mode, Stop, [])} end)
              || _ <- lists:seq(1, ?READERS)]
    end,
    Lagger = case Mode of
        off -> undefined;
        _ -> spawn_link(fun() -> Self ! {lags, self(), lagger(Db, View, Stop, [])} end)
    end,
    Writes = writer(Db, Stop, 0, []),
    ReaderTimes = lists:append([receive {reader, R, Ts} -> Ts end || R <- Readers]),
    Lags = case Lagger of undefined -> []; _ -> receive {lags, Lagger, L} -> L end end,
    {Writes, Lags, case Mode of off -> na; _ -> ReaderTimes end}.

writer(Db, Stop, N, Acc) ->
    Now = erlang:monotonic_time(microsecond),
    case Now div 1000 >= Stop of
        true -> Acc;
        false ->
            Due = N * 1000000 div ?RATE,
            Start = Stop * 1000 - ?SUSTAIN_MS * 1000,
            Wait = (Start + Due - Now) div 1000,
            _ = Wait > 0 andalso receive after Wait -> ok end,
            Us = timed(fun() -> mixed_write(Db, N) end),
            writer(Db, Stop, N + 1, [Us | Acc])
    end.

mixed_write(Db, N) ->
    I = 1 + (N * 7919) rem (2 * ?PHASE),
    Id = integer_to_binary(I),
    case N rem 10 of
        R when R < 7 ->
            case barrel_docdb:get_doc(Db, Id) of
                {ok, Doc} ->
                    {ok, _} = barrel_docdb:put_doc(Db, Doc#{
                        <<"status">> => case N rem 3 of 0 -> <<"paid">>; _ -> <<"open">> end,
                        <<"customer">> => integer_to_binary((I + N) rem ?CUSTOMERS)});
                {error, not_found} ->
                    {ok, _} = barrel_docdb:put_doc(Db, invoice(I))
            end;
        R when R < 9 ->
            {ok, _} = barrel_docdb:put_doc(Db, invoice(20000 + N));
        _ ->
            case barrel_docdb:get_doc(Db, Id) of
                {ok, #{<<"_rev">> := Rev}} -> {ok, _} = barrel_docdb:delete_doc(Db, Id, #{rev => Rev});
                {error, not_found} -> ok
            end
    end.

reader(Db, View, Mode, Stop, Acc) ->
    case erlang:monotonic_time(millisecond) >= Stop of
        true -> Acc;
        false ->
            C = integer_to_binary(erlang:unique_integer([positive]) rem ?CUSTOMERS),
            Us = timed(fun() -> {ok, _, _} = barrel_design:query(Db, View, drill(Mode, C)) end),
            reader(Db, View, Mode, Stop, [Us | Acc])
    end.

lagger(Db, View, Stop, Acc) ->
    case erlang:monotonic_time(millisecond) >= Stop of
        true -> Acc;
        false ->
            {ok, T} = barrel_docdb:db_observed_version(Db),
            {Us, {ok, _, _}} = timer:tc(fun() ->
                barrel_design:query(Db, View, #{limit => 1, reduce => false,
                    freshness => {wait_for, T}, timeout_ms => 60000})
            end),
            receive after 100 -> ok end,
            lagger(Db, View, Stop, [Us div 1000 | Acc])
    end.

%%====================================================================
%% Validation
%%====================================================================

validate(_Db, _View, off, _Docs) -> ok;
validate(Db, View, Mode, _Docs) -> validate_rows(Db, View, Mode).

%% The index equals a clean computation over the live documents.
validate_rows(Db, View, Mode) ->
    {ok, Live} = barrel_docdb:fold_docs(Db, fun(D, Acc) -> {ok, [D | Acc]} end, []),
    Open = [D || #{<<"type">> := <<"invoice">>, <<"status">> := <<"open">>} = D <- Live],
    Want = lists:sort(expected(Mode, Open)),
    Got = lists:sort([{K, Id, V} || #{id := Id, key := K, value := V} <- all_rows(Db, View)]),
    case Want =:= Got of
        true -> ok;
        false -> error({rows_differ, Mode, length(Want), length(Got)})
    end,
    {ok, Groups, _} = barrel_design:query(Db, View, #{group_level => 1, limit => 10000}),
    WantGroups = lists:foldl(fun({[G | _], _, V}, Acc) -> maps:update_with([G], fun(X) -> X + V end, V, Acc) end,
                             #{}, Want),
    case lists:sort([{K, V} || #{key := K, value := V} <- Groups]) =:= lists:sort(maps:to_list(WantGroups)) of
        true -> ok;
        false -> error({totals_differ, Mode})
    end.

expected(fanout, Open) ->
    [{[I], Id, L} || #{<<"id">> := Id, <<"line_cents">> := Lines} <- Open,
                     {I, L} <- lists:enumerate(0, Lines)];
expected(_, Open) ->
    [{[C, Due], Id, lists:sum(Lines)}
     || #{<<"id">> := Id, <<"customer">> := C, <<"due_on">> := Due, <<"line_cents">> := Lines} <- Open].

all_rows(Db, View) -> page(Db, View, #{reduce => false, limit => 10000}, []).

page(Db, View, Opts, Acc) ->
    case barrel_design:query(Db, View, Opts) of
        {ok, Rows, #{continuation := C}} -> page(Db, View, Opts#{continuation => C}, Acc ++ Rows);
        {ok, Rows, _} -> Acc ++ Rows
    end.

%%====================================================================
%% Two tenants
%%====================================================================

tenants(Trial) ->
    A = <<"tenant_a">>,
    B = <<"tenant_b">>,
    {ok, _} = barrel_docdb:create_db(A),
    {ok, _} = barrel_docdb:create_db(B),
    [{ok, _} | _] = lists:append([barrel_docdb:put_docs(A, C) ||
                                  C <- chunks([invoice(I) || I <- lists:seq(1, 5 * ?PHASE)], 200)]),
    [{ok, _} = barrel_docdb:put_doc(B, invoice(I)) || I <- lists:seq(1, 500)],
    {ok, _} = install(B, native),
    BView = {<<"bench">>, <<"open_by_customer">>},
    {ok, _} = barrel_design:refresh(B, BView, #{timeout_ms => 60000}),
    {WAlone, RAlone} = tenant_b(B, BView, 1000),
    {ok, Seeded} = barrel_docdb:db_observed_version(A),
    Self = self(),
    T0 = now_us(),
    {ok, _} = install(A, wasm),
    Waiter = spawn_link(fun() ->
        {ok, _, _} = barrel_design:query(A, BView, #{limit => 1, reduce => false,
            freshness => {wait_for, Seeded}, timeout_ms => 120000}),
        Self ! {backfilled, self(), now_us() - T0}
    end),
    {WShared, RShared} = tenant_b(B, BView, 2000),
    ABackfill = receive {backfilled, Waiter, Us} -> Us end,
    [Trial, pct(95, WAlone), pct(95, WShared), pct(99, WAlone), pct(99, WShared),
     pct(95, RAlone), pct(95, RShared), pct(99, RAlone), pct(99, RShared),
     length(WAlone), 5 * ?PHASE, ABackfill].

tenant_b(B, View, Base) ->
    lists:unzip([begin
        W = timed(fun() -> {ok, _} = barrel_docdb:put_doc(B, invoice(Base + I)) end),
        R = timed(fun() -> {ok, _, _} = barrel_design:query(B, View, drill(native, <<"3">>)) end),
        {W, R}
    end || I <- lists:seq(1, 300)]).

%%====================================================================
%% Limits and recovery
%%====================================================================

limits(Trial) ->
    {ok, Loop} = wasm:compile(element(2, file:read_file(guest_path("loop.wasm")))),
    Spec = #{export => <<"map">>, fields => [], key => {scalar, #{type => i64, category => false}},
             value => #{type => i64}, limits => barrel_design_manifest:default_limits()},
    Deadline = 20,
    Before = pages(),
    {TermUs, [{error, timeout}]} = timer:tc(fun() ->
        barrel_design_guest:run(Loop, with_limits(Spec, #{fuel => 10000000, timeout_ms => Deadline}),
                                [#{}], #{retries => 0})
    end),
    ok = wait(fun() -> pages() =:= Before end, 2000),
    After = pages(),
    Fuel = 50000,
    {FuelUs, [{error, out_of_fuel}]} = timer:tc(fun() ->
        barrel_design_guest:run(Loop, with_limits(Spec, #{fuel => Fuel}), [#{}], #{retries => 0})
    end),
    {Docs, AtCrash, RecoveryMs, Valid} = recovery(),
    [Trial, Deadline, TermUs / 1000, Fuel, FuelUs, Before, After, Docs, AtCrash, RecoveryMs, Valid].

%% Crash barrel_design during a Wasm backfill, start it again and time
%% how long the view takes to cover the database again.
recovery() ->
    Db = <<"recovery">>,
    N = 3 * ?PHASE,
    {ok, _} = barrel_docdb:create_db(Db),
    [{ok, _} | _] = lists:append([barrel_docdb:put_docs(Db, C) ||
                                  C <- chunks([invoice(I) || I <- lists:seq(1, N)], 200)]),
    {ok, _} = install(Db, wasm),
    View = {<<"bench">>, <<"open_by_customer">>},
    ok = wait(fun() -> guest_calls() >= N div 4 end, 60000),
    AtCrash = guest_calls(),
    Sup = whereis(barrel_design_sup),
    Ref = erlang:monitor(process, Sup),
    exit(Sup, kill),
    receive {'DOWN', Ref, _, _, _} -> ok end,
    _ = application:stop(barrel_design),
    T0 = erlang:monotonic_time(millisecond),
    {ok, _} = application:ensure_all_started(barrel_design),
    {ok, Target} = barrel_docdb:db_observed_version(Db),
    ok = wait(fun() -> barrel_design_dbs:lookup(Db) =/= {error, not_found} end, 30000),
    {ok, _, _} = barrel_design:query(Db, View, #{limit => 1, reduce => false,
        freshness => {wait_for, Target}, timeout_ms => 120000}),
    Ms = erlang:monotonic_time(millisecond) - T0,
    Valid = try validate_rows(Db, View, wasm) of ok -> true catch _:_ -> false end,
    {N, AtCrash, Ms, Valid}.

with_limits(#{limits := L} = Spec, More) -> Spec#{limits := maps:merge(L, More)}.

%% First engine load of the Zig guest in a fresh VM (this includes
%% loading the runtime's own code on first use), then the per-document
%% cost of maps in batches of 64, each in a fresh instance, after 500
%% warm-up documents.
cold(Trial) ->
    {ok, Bytes} = file:read_file("_build/open_by_customer.wasm"),
    {LoadUs, {ok, H}} = timer:tc(fun() -> wasm:load(Bytes) end),
    Spec = #{export => <<"map">>,
             fields => [{[<<"customer">>], text}, {[<<"due_on">>], text},
                        {[<<"line_cents">>], {array, i64}}],
             key => {array, [#{type => text, category => true}, #{type => text, category => false}]},
             value => #{type => i64}, limits => barrel_design_manifest:default_limits()},
    Doc = fun(I) -> #{<<"customer">> => integer_to_binary(I rem ?CUSTOMERS),
                      <<"due_on">> => <<"2026-12-01">>, <<"line_cents">> => [1000 + I, -250, 25]} end,
    _ = barrel_design_guest:run(H, Spec, [Doc(I) || I <- lists:seq(1, 500)], #{}),
    PerDoc = [begin
        {Us, R} = timer:tc(fun() -> barrel_design_guest:run(H, Spec, [Doc(I) || I <- lists:seq(1, 64)], #{}) end),
        64 = length([ok || {ok, [_]} <- R]),
        Us / 64
    end || _ <- lists:seq(1, 200)],
    %% the WAT example encoded to its canonical bytes: first call, then 100
    {ok, Wat} = file:read_file("by_customer.wat"),
    {FirstEnc, {ok, _, WatBytes}} = timer:tc(fun() -> barrel_design:module({wat, Wat}) end),
    Encodes = [timed(fun() -> {ok, _, _} = barrel_design:module({wat, Wat}) end)
               || _ <- lists:seq(1, 100)],
    [Trial, byte_size(Bytes), LoadUs, pct(50, PerDoc), pct(95, PerDoc), pct(99, PerDoc), 200, 64,
     byte_size(WatBytes), FirstEnc, pct(50, Encodes)].

pages() -> maps:get(pages_in_use, wasm_engine:stats()).

wait(Fun, Ms) when Ms =< 0 -> case Fun() of true -> ok; false -> {error, timeout} end;
wait(Fun, Ms) ->
    case Fun() of
        true -> ok;
        false -> receive after 10 -> wait(Fun, Ms - 10) end
    end.

%%====================================================================
%% Sampling and statistics
%%====================================================================

%% Peak OS resident memory and guest pages, sampled every 100 ms.
start_sampler() ->
    Self = self(),
    spawn_link(fun() -> sampler(Self, 0, 0) end).

sampler(Owner, Rss, Pages) ->
    R = rss_kb(),
    P = try pages() catch _:_ -> 0 end,
    receive
        stop -> ok;
        {peaks, From} -> From ! {peaks, max(Rss, R), max(Pages, P)}, sampler(Owner, max(Rss, R), max(Pages, P))
    after 100 -> sampler(Owner, max(Rss, R), max(Pages, P))
    end.

peaks(Sampler) ->
    Sampler ! {peaks, self()},
    receive {peaks, R, P} -> {R, P} end.

rss_kb() ->
    case string:trim(os:cmd("ps -o rss= -p " ++ os:getpid())) of
        "" -> 0;
        S -> list_to_integer(S)
    end.

dir_bytes(Dir) ->
    filelib:fold_files(Dir, ".*", true, fun(F, A) -> A + filelib:file_size(F) end, 0).

guest_calls() -> maps:get(guest_calls, barrel_design_stats:get()).

timed(Fun) ->
    {Us, _} = timer:tc(Fun),
    Us.

now_us() -> erlang:monotonic_time(microsecond).

chunks([], _N) -> [];
chunks(L, N) when length(L) =< N -> [L];
chunks(L, N) -> {H, T} = lists:split(N, L), [H | chunks(T, N)].

pcts(L) -> [pct(P, L) || P <- [50, 95, 99]].

maybe_pcts(na) -> [na, na, na];
maybe_pcts(L) -> pcts(L).

maybe_pct(_P, na) -> na;
maybe_pct(P, L) -> pct(P, L).

samples(na) -> na;
samples(L) -> length(L).

%% Nearest rank.
pct(_P, []) -> na;
pct(P, L) ->
    Sorted = lists:sort(L),
    lists:nth(max(1, (P * length(Sorted) + 99) div 100), Sorted).

lag_stats([]) -> [na, na, na, 0];
lag_stats(L) -> [pct(50, L), pct(95, L), lists:max(L), length(L)].

csv(na) -> "NA";
csv(N) when is_integer(N) -> integer_to_list(N);
csv(F) when is_float(F) -> float_to_list(F, [{decimals, 3}]);
csv(B) when is_boolean(B) -> atom_to_list(B);
csv(S) when is_list(S) -> S.
