%% @doc Read-only opens: RocksDB read-write open (baseline) against
%% OpenForReadOnly (prototype) on imported generations. Driven by run.sh;
%% each command runs in its own BEAM.
-module(rocksdb_ro_bench).

-export([main/1]).

-define(DIM, 128).
-define(TEXT_CHARS, 6000).
-define(BATCH, 100).
-define(GETS, 1000).
-define(FINDS, 20).
-define(TOPK, 50).
-define(BQ, [<<"gen_server">>, <<"supervisor child">>, <<"ets table">>,
             <<"socket">>, <<"binary">>, <<"compile">>, <<"certificate">>,
             <<"release">>, <<"test case">>, <<"alarm">>]).

%%====================================================================
%% Commands
%%====================================================================

main(["prepare", Corpus, Work]) -> prepare(Corpus, Work);
main(["import", Work, Side]) -> import(Work, Side);
main(["run", Work, Side, Case, Iter, Uptime]) -> run(Work, Side, Case, Iter, Uptime);
main(["hold", Work, Side]) -> hold(Work, Side);
main(["second", Work, Side]) -> second(Work, Side);
main(["legacy", Work, Side]) -> legacy(Work, Side);
main(["report", Work]) -> report(Work).

%% Sources with the baseline code: clean (closed, then reopened once so
%% RocksDB flushes the WAL at recovery) and wal (copied while open).
prepare(Corpus, Work) ->
    Src = filename:join(Work, "src"),
    ok = setup(filename:join(Src, "node"), filename:join(Src, "ctx")),
    ok = check_side("baseline"),
    Docs = load(Corpus),
    log("~b docs", [length(Docs)]),
    ok = file:write_file(filename:join(Work, "ids.term"),
                         io_lib:format("~p.~n", [[{Id, App} || #{<<"id">> := Id,
                                                                <<"app">> := App}
                                                                   <- Docs]])),
    CleanOpts = src_opts(Src, "clean"),
    {ok, C} = barrel:open(<<"src_clean">>, CleanOpts),
    ok = fill(C, Docs),
    ok = barrel:close(C),
    {ok, C2} = barrel:open(<<"src_clean">>, CleanOpts),
    ok = barrel:close(C2),
    ExpClean = filename:join(Work, "exp_clean"),
    {ok, _} = barrel_ctx_export:export(<<"src_clean">>, ExpClean,
                                       #{open_opts => CleanOpts}),
    WalOpts = src_opts(Src, "wal"),
    {ok, W} = barrel:open(<<"src_wal">>, WalOpts),
    ok = fill(W, Docs),
    ok = sync_wals(<<"src_wal">>),
    ExpWal = filename:join(Work, "exp_wal"),
    ok = filelib:ensure_path(ExpWal),
    cp(filename:join([Src, "docdb", "src_wal"]), filename:join(ExpWal, "docdb")),
    cp(filename:join(Src, "vec_wal"), filename:join(ExpWal, "vectordb")),
    ok = barrel:close(W),
    ok = wal_manifest(ExpClean, ExpWal),
    [log("~s: ~s", [E, os:cmd("du -sh " ++ filename:join(Work, E) ++
                              " | cut -f1") -- "\n"])
     || E <- ["exp_clean", "exp_wal"]],
    ok.

src_opts(Src, Case) ->
    #{docdb => #{data_dir => filename:join(Src, "docdb")},
      vectordb => #{dimension => ?DIM,
                    db_path => filename:join(Src, "vec_" ++ Case),
                    bm25_backend => disk}}.

fill(Db, Docs) ->
    lists:foreach(
      fun(Chunk) ->
              [{ok, _} = R || R <- barrel:put_docs(Db, Chunk)],
              Batch = [{Id, T, #{<<"app">> => App}, vec(Id)}
                       || #{<<"id">> := Id, <<"app">> := App} = D <- Chunk,
                          T <- [text(D)]],
              {ok, _} = barrel:vector_add_batch(Db, Batch)
      end, chunks(Docs, ?BATCH)).

%% docdb store + attachments, vector store and bm25.ids WALs to disk.
sync_wals(Name) ->
    #{ref := DocRef} = persistent_term:get({barrel_store, Name}),
    {ok, Pid} = barrel_docdb:db_pid(Name),
    {ok, #{ref := AttRef}} = barrel_db_server:get_att_ref(Pid),
    State = sys:get_state({via, barrel_vectordb_registry, {vstore, Name}}),
    VecDb = element(4, State),
    IdsDb = element(8, element(14, State)),
    [ok = rocksdb:sync_wal(R) || R <- [DocRef, AttRef, VecDb, IdsDb]],
    ok.

%% Same layout as the clean export, keyed by the wal source.
wal_manifest(ExpClean, ExpWal) ->
    {ok, Root0, _} = barrel_ctx_manifest:read(ExpClean),
    [Source | _] = maps:get(<<"sources">>, Root0),
    Root = (maps:without([<<"type">>, <<"format">>, <<"parts">>], Root0))#{
             <<"context">> => barrel_ctx_manifest:new_id(<<"ctx_">>),
             <<"sources">> => [Source#{<<"db">> => <<"src_wal">>,
                                       <<"keyspace">> => <<"src_wal">>,
                                       <<"quiesced">> => false}]},
    {ok, _} = barrel_ctx_manifest:write(ExpWal, Root,
                                        barrel_ctx_manifest:scan(ExpWal)),
    ok.

%% Import both generations with one side's code (the prototype mints
%% the source id in the sidecar), plus a legacy copy of the clean one.
import(Work, Side) ->
    Dir = filename:join(Work, "pristine_" ++ Side),
    ok = setup(filename:join(Dir, "node"), Dir),
    ok = check_side(Side),
    [{ok, _} = barrel_ctx_export:import(filename:join(Work, "exp_" ++ Case),
                                        #{name => name(Case), open => false})
     || Case <- ["clean", "wal"]],
    {ok, _} = barrel_ctx_export:import(filename:join(Work, "exp_clean"),
                                       #{name => <<"imp_legacy">>,
                                         open => false}),
    ok = make_legacy(filename:join([import_dir(Dir, <<"imp_legacy">>),
                                    "vectordb", "bm25", "bm25.ids"])),
    ok.

%% What 2.4.1 added to bm25.ids is dropped (as the #47 tests do).
make_legacy(IdsPath) ->
    Cfs = ["default", "terms_fwd", "terms_rev", "docs_fwd", "docs_rev",
           "doc_terms", "term_df", "pending"],
    {ok, Db, [CfD, _, _, _, _ | New]} =
        rocksdb:open(IdsPath, [], [{N, []} || N <- Cfs]),
    [ok = rocksdb:drop_column_family(Db, Cf) || Cf <- New],
    [ok = rocksdb:delete(Db, CfD, K, [])
     || K <- [<<"format">>, <<"stats">>, <<"segment">>]],
    rocksdb:close(Db).

%%====================================================================
%% Measured run
%%====================================================================

run(Work, Side, Case, Iter, Uptime) ->
    Name = name(Case),
    RunDir = filename:join([Work, "runs", Side ++ "_" ++ Case ++ "_" ++ Iter]),
    DbDir = fresh_copy(Work, Side, Name, RunDir),
    ok = setup(filename:join(RunDir, "node"), filename:join(RunDir, "ctx")),
    ok = check_side(Side),
    Before = tree(DbDir),
    {ok, Opts} = barrel_ctx_export:open_opts(Name),
    erlang:garbage_collect(),
    Mem0 = erlang:memory(total),
    Rss0 = rss(),
    T0 = erlang:monotonic_time(microsecond),
    {ok, Db} = barrel:open(Name, Opts),
    OpenUs = erlang:monotonic_time(microsecond) - T0,
    Mem1 = erlang:memory(total),
    Rss1 = rss(),
    Lat = workload(Db, Work),
    ok = barrel:close(Db),
    After = tree(DbDir),
    Result = #{side => Side, 'case' => Case, iter => list_to_integer(Iter),
               uptime => string:trim(Uptime),
               open_ms => OpenUs / 1000,
               mem_total_delta => Mem1 - Mem0, mem_total => Mem1,
               rss_kb => Rss1, rss_delta_kb => Rss1 - Rss0,
               changes => diff(Before, After),
               manifest_ok => manifest_ok(Work, Case, Name, DbDir),
               latency_us => Lat},
    save(Work, Side ++ "_" ++ Case ++ "_" ++ Iter, Result),
    log("~s ~s #~s open ~.1f ms, ~p changed files",
        [Side, Case, Iter, OpenUs / 1000,
         length(maps:get(files, maps:get(changes, Result)))]).

workload(Db, Work) ->
    Ids = ids(Work),
    Apps = lists:usort([A || {_, A} <- Ids]),
    Find = [time(fun() -> {ok, _, _} = barrel:find(
                                         Db, #{where => [{path, [<<"app">>], A}]})
                 end)
            || A <- take(?FINDS, Apps)],
    Vec = [time(fun() -> {ok, [_ | _]} = barrel:search_vector(
                                            Db, vec(Id), #{k => 10})
                end)
           || {Id, _} <- take(?TOPK, Ids)],
    Bm25 = [time(fun() -> {ok, _} = barrel:search_bm25(Db, Q, #{k => 10}) end)
            || Q <- take(?TOPK, ?BQ)],
    Gets = [time(fun() -> {ok, _} = barrel:get_doc(Db, Id) end)
            || {Id, _} <- take(?GETS, Ids)],
    #{find => pct(Find), vector_top_k => pct(Vec), bm25_top_k => pct(Bm25),
      get_doc => pct(Gets)}.

%% Doc ids and apps of the corpus, saved by prepare's first load.
ids(Work) ->
    {ok, [Ids]} = file:consult(filename:join(Work, "ids.term")),
    Ids.

%%====================================================================
%% Concurrent readers: hold opens and waits, second opens meanwhile
%%====================================================================

hold(Work, Side) ->
    Name = name("clean"),
    RunDir = filename:join([Work, "runs", Side ++ "_concurrent"]),
    DbDir = fresh_copy(Work, Side, Name, RunDir),
    ok = setup(filename:join(RunDir, "node1"), filename:join(RunDir, "ctx")),
    ok = check_side(Side),
    Before = tree(DbDir),
    {ok, Opts} = barrel_ctx_export:open_opts(Name),
    {ok, Db} = barrel:open(Name, Opts),
    ok = file:write_file(filename:join(RunDir, "ready"), <<>>),
    ok = wait_file(filename:join(RunDir, "done"), 120),
    {ok, _} = barrel:get_doc(Db, element(1, hd(ids(Work)))),
    ok = barrel:close(Db),
    save(Work, Side ++ "_hold", #{side => Side, changes => diff(Before, tree(DbDir))}).

second(Work, Side) ->
    process_flag(trap_exit, true),
    Name = name("clean"),
    RunDir = filename:join([Work, "runs", Side ++ "_concurrent"]),
    ok = wait_file(filename:join(RunDir, "ready"), 600),
    ok = setup(filename:join(RunDir, "node2"), filename:join(RunDir, "ctx")),
    ok = check_side(Side),
    {ok, Opts} = barrel_ctx_export:open_opts(Name),
    T0 = erlang:monotonic_time(microsecond),
    Res = try barrel:open(Name, Opts) of
              {ok, Db} ->
                  Ms = (erlang:monotonic_time(microsecond) - T0) / 1000,
                  Found = length([ok || {Id, _} <- ids(Work),
                                        {ok, _} <- [barrel:get_doc(Db, Id)]]),
                  {ok, Hits} = barrel:search_bm25(Db, <<"gen_server">>, #{k => 10}),
                  ok = barrel:close(Db),
                  #{open => ok, open_ms => Ms, docs_read => Found,
                    bm25_hits => length(Hits)};
              Other ->
                  #{open => Other}
          catch C:E ->
              #{open => {C, E}}
          end,
    ok = file:write_file(filename:join(RunDir, "done"), <<>>),
    save(Work, Side ++ "_second", Res#{side => Side}),
    log("~s second reader: ~p", [Side, Res]).

%%====================================================================
%% Legacy store (bm25.ids without the 2.4.1 column families)
%%====================================================================

legacy(Work, Side) ->
    Name = <<"imp_legacy">>,
    RunDir = filename:join([Work, "runs", Side ++ "_legacy"]),
    DbDir = fresh_copy(Work, Side, Name, RunDir),
    ok = setup(filename:join(RunDir, "node"), filename:join(RunDir, "ctx")),
    ok = check_side(Side),
    process_flag(trap_exit, true),
    Before = tree(DbDir),
    {ok, Opts} = barrel_ctx_export:open_opts(Name),
    T0 = erlang:monotonic_time(microsecond),
    Res = case barrel:open(Name, Opts) of
              {ok, Db} ->
                  Ms = (erlang:monotonic_time(microsecond) - T0) / 1000,
                  {ok, Hits} = barrel:search_bm25(Db, <<"gen_server">>, #{k => 10}),
                  ok = barrel:close(Db),
                  #{open => ok, open_ms => Ms, bm25_hits => length(Hits)};
              Other ->
                  #{open => Other}
          end,
    Result = Res#{side => Side, changes => diff(Before, tree(DbDir))},
    save(Work, Side ++ "_legacy", Result),
    log("~s legacy: ~p", [Side, maps:get(open, Res)]).

%%====================================================================
%% Report
%%====================================================================

report(Work) ->
    Rs = [R || F <- filelib:wildcard(filename:join([Work, "results", "*.term"])),
               {ok, [R]} <- [file:consult(F)]],
    Runs = [R || #{iter := _} = R <- Rs],
    Rows = [row(Case, Side, [R || #{side := S, 'case' := C} = R <- Runs,
                                 S =:= Side, C =:= Case])
            || Case <- ["clean", "wal"], Side <- ["baseline", "prototype"]],
    Loads = lists:usort([U || #{uptime := U} <- Runs]),
    Out = [<<"| case | side | open ms | bytes written | files +/~/- | "
             "manifest ok | find p50/p95 us | vector p50/p95 us | "
             "bm25 p50/p95 us | get_doc p50/p95 us | erlang mem MB | "
             "RSS delta MB |\n"
             "|---|---|---|---|---|---|---|---|---|---|---|---|\n">>,
           Rows,
           io_lib:format("~nruns: ~b; load (uptime) per run:~n~s~n",
                         [length(Runs), [["  ", L, "\n"] || L <- Loads]]),
           [io_lib:format("~n~s: ~p~n", [K, maps:without([side], R)])
            || K <- ["baseline_second", "prototype_second",
                     "baseline_hold", "prototype_hold",
                     "baseline_legacy", "prototype_legacy"],
               R <- [maps:get(K, maps:from_list([{key(X), X} || X <- Rs]),
                              #{missing => true})]],
           [io_lib:format("~n~s ~s #1 changed files:~n~p~n",
                          [S, C, maps:get(files, Ch)])
            || #{side := S, 'case' := C, iter := 1, changes := Ch} <- Runs]],
    ok = file:write_file(filename:join(Work, "results.md"), Out),
    io:put_chars(Out).

key(#{iter := _, side := S, 'case' := C}) -> S ++ "_" ++ C;
key(#{side := S, open := _, changes := _}) -> S ++ "_legacy";
key(#{side := S, open := _}) -> S ++ "_second";
key(#{side := S}) -> S ++ "_hold".

row(_Case, _Side, []) ->
    [];
row(Case, Side, Rs) ->
    Med = fun(F) -> median([F(R) || R <- Rs]) end,
    Lat = fun(Op) ->
                  io_lib:format("~b/~b",
                                [round(Med(fun(R) -> p(R, Op, p50) end)),
                                 round(Med(fun(R) -> p(R, Op, p95) end))])
          end,
    Ch = fun(K) -> round(Med(fun(#{changes := C}) -> maps:get(K, C) end)) end,
    io_lib:format("| ~s | ~s | ~.1f | ~b | ~b/~b/~b | ~s | ~s | ~s | ~s | ~s "
                  "| ~.1f | ~.1f |~n",
                  [Case, Side, Med(fun(#{open_ms := M}) -> M end),
                   Ch(bytes_written), Ch(created), Ch(modified), Ch(deleted),
                   manifest_summary(Rs),
                   Lat(find), Lat(vector_top_k), Lat(bm25_top_k), Lat(get_doc),
                   Med(fun(#{mem_total := M}) -> M end) / 1048576,
                   Med(fun(#{rss_delta_kb := K}) -> K end) / 1024]).

p(#{latency_us := L}, Op, K) -> maps:get(K, maps:get(Op, L)).

manifest_summary(Rs) ->
    Ok = length([ok || #{manifest_ok := true} <- Rs]),
    io_lib:format("~b/~b", [Ok, length(Rs)]).

%%====================================================================
%% Helpers
%%====================================================================

setup(NodeDir, CtxDir) ->
    [ok = application:load(A) || A <- [barrel_embed, barrel_docdb,
                                       barrel_vectordb, barrel]],
    ok = application:set_env(barrel_embed, managed_venv, false),
    os:putenv("HF_HUB_OFFLINE", "1"),
    ok = application:set_env(barrel_docdb, data_dir, NodeDir),
    ok = application:set_env(barrel, ctx_dir, CtxDir),
    ok = filelib:ensure_path(NodeDir),
    {ok, _} = application:ensure_all_started(barrel),
    ok.

%% The prototype side has barrel_rocksdb_ro; the baseline beams shadow
%% the changed modules and do not call it.
check_side("prototype") ->
    true = lists:member({open_store, 4},
                        barrel_store_rocksdb:module_info(functions)),
    ok;
check_side("baseline") ->
    false = lists:member({open_store, 4},
                         barrel_store_rocksdb:module_info(functions)),
    ok.

name("clean") -> <<"imp_clean">>;
name("wal") -> <<"imp_wal">>.

import_dir(CtxDir, Name) ->
    filename:join([CtxDir, "imports", binary_to_list(Name)]).

%% A fresh copy of the side's pristine import; returns the import dir.
fresh_copy(Work, Side, Name, RunDir) ->
    _ = file:del_dir_r(RunDir),
    Dst = import_dir(filename:join(RunDir, "ctx"), Name),
    ok = filelib:ensure_dir(Dst),
    cp(import_dir(filename:join(Work, "pristine_" ++ Side), Name), Dst),
    Dst.

cp(Src, Dst) ->
    "" = os:cmd("cp -Rc '" ++ Src ++ "' '" ++ Dst ++ "' 2>&1"),
    ok.

load(Corpus) ->
    {ok, Bin} = file:read_file(Corpus),
    Docs = [#{<<"id">> => Id, <<"app">> => App, <<"path">> => Path,
              <<"body">> => Body}
            || L <- binary:split(Bin, <<"\n">>, [global]), L =/= <<>>,
               #{<<"id">> := Id, <<"app">> := App, <<"path">> := Path,
                 <<"body">> := Body} <- [json:decode(L)]],
    Docs.

text(#{<<"path">> := P, <<"body">> := B}) ->
    <<P/binary, "\n", (unicode:characters_to_binary(
                         string:slice(B, 0, ?TEXT_CHARS)))/binary>>.

%% Deterministic unit vector from the id.
vec(Key) ->
    Bytes = << <<(crypto:hash(sha256, <<I:8, Key/binary>>))/binary>>
               || I <- lists:seq(0, ?DIM div 32 - 1) >>,
    Fs = [(B - 127.5) / 127.5 || <<B:8>> <= Bytes],
    Norm = math:sqrt(lists:sum([F * F || F <- Fs])),
    [F / Norm || F <- Fs].

chunks([], _N) -> [];
chunks(L, N) when length(L) =< N -> [L];
chunks(L, N) -> {H, T} = lists:split(N, L), [H | chunks(T, N)].

take(N, L) -> take(N, L, L).
take(0, _, _) -> [];
take(N, [], All) -> take(N, All, All);
take(N, [H | T], All) -> [H | take(N - 1, T, All)].

time(Fun) ->
    T0 = erlang:monotonic_time(microsecond),
    _ = Fun(),
    erlang:monotonic_time(microsecond) - T0.

pct(Samples) ->
    S = lists:sort(Samples),
    #{p50 => nth(S, 0.50), p95 => nth(S, 0.95), n => length(S)}.

nth(Sorted, Q) ->
    lists:nth(max(1, round(Q * length(Sorted))), Sorted).

median(L) ->
    S = lists:sort(L),
    N = length(S),
    case N rem 2 of
        1 -> lists:nth(N div 2 + 1, S);
        0 -> (lists:nth(N div 2, S) + lists:nth(N div 2 + 1, S)) / 2
    end.

rss() ->
    list_to_integer(string:trim(os:cmd("ps -o rss= -p " ++ os:getpid()))).

%% {RelPath => {Size, Sha256}} of every file under Dir.
tree(Dir) ->
    Base = length(filename:split(Dir)),
    maps:from_list(
      [{filename:join(lists:nthtail(Base, filename:split(F))),
        {filelib:file_size(F), element(2, barrel_ctx_manifest:sha256_file(F))}}
       || F <- filelib:fold_files(Dir, ".*", true, fun(F, A) -> [F | A] end, [])]).

diff(Before, After) ->
    Created = [P || P <- maps:keys(After), not maps:is_key(P, Before)],
    Deleted = [P || P <- maps:keys(Before), not maps:is_key(P, After)],
    Modified = [P || {P, V} <- maps:to_list(After),
                     maps:is_key(P, Before), maps:get(P, Before) =/= V],
    Bytes = lists:sum([element(1, maps:get(P, After))
                       || P <- Created ++ Modified]),
    #{created => length(Created), modified => length(Modified),
      deleted => length(Deleted), bytes_written => Bytes,
      files => lists:sort([{created, P} || P <- Created] ++
                          [{modified, P} || P <- Modified] ++
                          [{deleted, P} || P <- Deleted])}.

%% Every artifact of the export still matches its recorded sha256.
manifest_ok(Work, Case, Name, DbDir) ->
    {ok, _, Arts} = barrel_ctx_manifest:read(filename:join(Work, "exp_" ++ Case)),
    Local = fun(<<"docdb/", Rest/binary>>) ->
                    filename:join([DbDir, "docdb", Name, Rest]);
               (P) -> filename:join(DbDir, P)
            end,
    lists:all(fun(#{path := P} = A) ->
                      barrel_ctx_manifest:verify_file(Local(P), A)
              end, Arts).

wait_file(_File, 0) ->
    {error, timeout};
wait_file(File, N) ->
    case filelib:is_regular(File) of
        true -> ok;
        false -> receive after 1000 -> wait_file(File, N - 1) end
    end.

save(Work, Tag, Term) ->
    File = filename:join([Work, "results", Tag ++ ".term"]),
    ok = filelib:ensure_dir(File),
    ok = file:write_file(File, io_lib:format("~p.~n", [Term])).

log(Fmt, Args) ->
    io:format(standard_error, "[rocksdb_ro] " ++ Fmt ++ "~n", Args).
