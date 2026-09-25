%%%-------------------------------------------------------------------
%%% @doc Writer benchmark: concurrent writers against one database,
%%% and a tprof profile of the database server process.
%%% Uses only the public API so it runs against any release.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_bench_writer).

-export([run/1, run/2, profile/2, profile/3, phases/1, sample/2, cases/0]).

-define(TAG, <<"hb.task">>).

%% Case => {Writers, Kind, Opts}
cases() ->
    #{new_unsynced_64 => {64, new, #{}},
      new_synced_64 => {64, new, #{outbox => [?TAG], sync => true}},
      update_unsynced_64 => {64, update, #{}},
      update_synced_64 => {64, update, #{outbox => [?TAG], sync => true}},
      new_unsynced_1 => {1, new, #{}},
      new_synced_1 => {1, new, #{outbox => [?TAG], sync => true}},
      put_docs_64 => {64, {put_docs, 8}, #{}},
      put_docs_synced_64 => {64, {put_docs, 8}, #{outbox => [?TAG], sync => true}}}.

%% @doc Run a case for the default duration (3 s).
run(Case) ->
    run(Case, #{}).

%% @doc Run a case; prints one result line and returns it as a map.
run(Case, Opts) ->
    {Writers, Kind, WOpts} = maps:get(Case, cases()),
    Duration = maps:get(duration, Opts, 3000),
    Db = setup(Case, Writers, Kind),
    Lats = drive(Db, Writers, Kind, WOpts, Duration),
    Res = summary(Case, Lats, Duration, Kind),
    teardown(Db),
    io:format("RESULT ~p~n", [Res]),
    Res.

%% @doc Profile the database server while a case runs: `call_time' or
%% `call_memory', totals per function over the server process.
profile(Case, Type) ->
    profile(Case, Type, #{}).

profile(Case, Type, Opts) ->
    {Writers, Kind, WOpts} = maps:get(Case, cases()),
    Duration = maps:get(duration, Opts, 3000),
    Db = setup(Case, Writers, Kind),
    {ok, Pid} = barrel_docdb:db_pid(Db),
    %% server: the database server and the processes it spawned (its
    %% committer, when it has one), each on its own; clients: the writers
    {Targets, Inspect} = case maps:get(target, Opts, server) of
        server -> {[Pid | spawned(Pid)], process};
        clients -> {[new], total}
    end,
    {ok, _} = tprof:start(#{type => Type}),
    _ = [tprof:enable_trace(T) || T <- Targets],
    _ = tprof:set_pattern('_', '_', '_'),
    Lats = drive(Db, Writers, Kind, WOpts, Duration),
    _ = [tprof:disable_trace(T) || T <- Targets],
    Sample = tprof:collect(),
    ok = tprof:stop(),
    Docs = docs_written(Lats, Kind),
    Profiles = maps:to_list(tprof:inspect(Sample, Inspect, measurement)),
    lists:foreach(fun({Who, Profile}) ->
        io:format("PROFILE ~p ~p ~p docs=~p~n", [Case, Type, Who, Docs]),
        tprof:format(Profile),
        print_buckets(Profile, Docs)
    end, Profiles),
    teardown(Db),
    {Docs, Profiles}.

%% Processes linked to the server that it started (not its supervisor
%% nor the compaction filter handler, which runs no write path).
spawned(Pid) ->
    {links, Links} = erlang:process_info(Pid, links),
    [L || L <- Links, is_pid(L), not is_supervisor(L),
          element(2, erlang:process_info(L, initial_call)) =/=
              {proc_lib, init_p, 5}].

%% @doc Sample the current function and queue of the server and of the
%% processes it links to, every millisecond while a case runs
%% (untraced): where the writer's wall time goes.
sample(Case, Opts) ->
    {Writers, Kind, WOpts} = maps:get(Case, cases()),
    Duration = maps:get(duration, Opts, 3000),
    Db = setup(Case, Writers, Kind),
    {ok, Pid} = barrel_docdb:db_pid(Db),
    {links, Links} = erlang:process_info(Pid, links),
    Self = self(),
    Sampled = [Pid | [L || L <- Links, is_pid(L), not is_supervisor(L)]],
    Samplers0 = [{P, spawn_link(fun() -> sample_loop(P, Self, #{}, 0, 0) end)}
                 || P <- Sampled],
    %% and two of the writers, once they run
    Finder = spawn_link(fun() ->
        timer:sleep(200),
        Clients = lists:sublist([P || P <- erlang:processes(), is_client(P)], 2),
        Self ! {clients, [{P, spawn_link(fun() -> sample_loop(P, Self, #{}, 0, 0) end)}
                          || P <- Clients]}
    end),
    Lats = drive(Db, Writers, Kind, WOpts, Duration),
    Samplers = Samplers0 ++ receive {clients, Cs} -> Cs after 1000 -> [] end,
    unlink(Finder),
    io:format("SAMPLES ~p docs=~p~n", [Case, docs_written(Lats, Kind)]),
    lists:foreach(fun({P, S}) ->
        S ! stop,
        {Counts, N, QSum} = receive {samples, C, SN, Q} -> {C, SN, Q} end,
        io:format(" ~p samples=~p mean_queue=~.1f~n", [P, N, QSum / max(N, 1)]),
        _ = [io:format("  ~5.1f%  ~p~n", [100 * V / N, K])
             || {K, V} <- lists:sublist(
                            lists:reverse(lists:keysort(2, maps:to_list(Counts))), 8)]
    end, Samplers),
    teardown(Db),
    ok.

is_client(P) ->
    case erlang:process_info(P, current_stacktrace) of
        {current_stacktrace, St} ->
            lists:any(fun({barrel_bench_writer, loop, _, _}) -> true;
                         (_) -> false
                      end, St);
        undefined ->
            false
    end.

is_supervisor(P) ->
    case erlang:process_info(P, dictionary) of
        {dictionary, D} ->
            case proplists:get_value('$initial_call', D) of
                {supervisor, _, _} -> true;
                _ -> false
            end;
        undefined ->
            true
    end.

sample_loop(Pid, Parent, Acc, N, QSum) ->
    receive
        stop -> Parent ! {samples, Acc, N, QSum}
    after 0 ->
        case erlang:process_info(Pid, [current_function, status, message_queue_len]) of
            [{current_function, F}, {status, St}, {message_queue_len, Q}] ->
                K = {St, F},
                timer:sleep(1),
                sample_loop(Pid, Parent, maps:update_with(K, fun(X) -> X + 1 end, 1, Acc),
                            N + 1, QSum + Q);
            undefined ->
                Parent ! {samples, Acc, N, QSum}
        end
    end.

%% Exclusive totals per bucket, per document (traced, so inflated).
print_buckets({_Type, Total, Rows}, Docs) ->
    Sums = lists:foldl(
        fun({M, {F, A}, _Count, V, _PerCall, _Pct}, Acc) ->
            maps:update_with(bucket(M, F, A), fun(X) -> X + V end, V, Acc)
        end, #{}, Rows),
    io:format("~nBUCKETS (per doc, share of ~p)~n", [Total]),
    lists:foreach(
        fun({B, V}) ->
            io:format("  ~-14s ~10.2f ~6.1f%~n",
                      [B, V / max(Docs, 1), 100 * V / max(Total, 1)])
        end, lists:reverse(lists:keysort(2, maps:to_list(Sums)))).

bucket(rocksdb, write_batch, 3) -> rocksdb_write;
bucket(rocksdb, F, _) ->
    case atom_to_list(F) of
        "batch" ++ _ -> batch_build;
        "encode_merge_value" -> batch_build;
        "release_batch" -> batch_build;
        _ -> reads
    end;
bucket(barrel_store_rocksdb, F, _) ->
    case atom_to_list(F) of
        "-write_batch" ++ _ -> batch_build;
        "write_batch" -> batch_build;
        "encode_entity" ++ _ -> batch_build;
        _ -> reads
    end;
bucket(barrel_store_keys, _, _) -> keys;
bucket(barrel_docdb_codec_cbor, _, _) -> cbor;
bucket(barrel_ars, _, _) -> paths;
bucket(barrel_ars_index, _, _) -> paths;
bucket(barrel_changes, _, _) -> paths;
bucket(gen, _, _) -> calls;
bucket(gen_server, _, _) -> calls;
bucket(hlc, _, _) -> calls;
bucket(barrel_sub, _, _) -> calls;
bucket(barrel_query_sub, _, _) -> calls;
bucket(erlang, send, _) -> calls;
bucket(erlang, monitor, _) -> calls;
bucket(erlang, demonitor, _) -> calls;
bucket(barrel_db_server, _, _) -> writer_own;
bucket(M, _, _) when M =:= lists; M =:= maps; M =:= erlang; M =:= binary;
                     M =:= proplists; M =:= erts_internal -> stdlib_bifs;
bucket(_, _, _) -> other.

%% @doc Untraced cost of each writer step, one process, per document (us).
phases(N) ->
    Db = <<"bench_phases_", (integer_to_binary(erlang:unique_integer([positive])))/binary>>,
    {ok, _} = barrel_docdb:create_db(Db, #{data_dir => data_dir()}),
    StoreRef = persistent_term:get({barrel_store, Db}),
    Docs = [doc(id(1, I), I) || I <- lists:seq(1, N)],
    Recs = [barrel_doc:make_doc_record(barrel_doc:to_map(D)) || D <- Docs],
    Bodies = [maps:get(doc, R) || R <- Recs],
    Hlc = barrel_hlc:new_hlc(),
    Info = fun(#{id := Id} = R) ->
        #{id => Id, rev => <<"1-abcdef0123456789">>, deleted => false,
          num_conflicts => 0, hlc => Hlc, doc => maps:get(doc, R)}
    end,
    Steps = [
        {make_doc_record, fun() ->
            [barrel_doc:make_doc_record(barrel_doc:to_map(D)) || D <- Docs] end},
        {encode_cbor, fun() ->
            [barrel_docdb_codec_cbor:encode_cbor(B) || B <- Bodies] end},
        {analyze_topics, fun() ->
            [barrel_ars:paths_to_topics(barrel_ars:analyze(B)) || B <- Bodies] end},
        {index_doc_ops, fun() ->
            [barrel_ars_index:index_doc_ops(Db, maps:get(id, R), maps:get(doc, R))
             || R <- Recs] end},
        {change_ops, fun() ->
            [barrel_changes:write_change_ops(Db, Hlc, Info(R)) || R <- Recs] end},
        {path_hlc_ops, fun() ->
            [barrel_changes:write_path_index_ops(Db, Hlc, Info(R)) || R <- Recs] end},
        {update_path_ops, fun() ->
            [barrel_ars_index:update_doc_ops(Db, maps:get(id, R), B, maps:get(doc, R))
             || {R, B} <- lists:zip(Recs, lists:reverse(Bodies))] end},
        {hlc_now, fun() -> [barrel_hlc:new_hlc() || _ <- Recs] end},
        {sub_match, fun() ->
            [barrel_sub:match(Db, [<<"type/step">>]) || _ <- Recs] end}
    ],
    Timed = [{Name, time_per(Fun, N)} || {Name, Fun} <- Steps],
    %% seed the docs, then time the reads of an update and the batch write
    Ops = lists:append([element(1, build_ops(Db, R, Hlc)) || R <- Recs]),
    ok = barrel_store_rocksdb:write_batch(StoreRef, Ops, #{}),
    Ks = barrel_keyspace:resolve(Db),
    ReadCurrent = time_per(fun() ->
        [begin
             {ok, _} = barrel_store_rocksdb:get_entity(
                         StoreRef, barrel_store_keys:doc_entity(Ks, Id)),
             {ok, Cbor} = barrel_store_rocksdb:body_get(
                            StoreRef, barrel_store_keys:doc_body(Ks, Id)),
             barrel_docdb_codec_cbor:decode_any(Cbor)
         end || #{id := Id} <- Recs]
    end, N),
    Built = [build_ops(Db, R, barrel_hlc:new_hlc()) || R <- Recs],
    {BatchT, ok} = timer:tc(fun() -> write_in_groups(StoreRef, Built, 64) end),
    Batch64 = BatchT / N,
    NOps = length(Ops) div N,
    _ = barrel_docdb:delete_db(Db),
    All = Timed ++ [{read_current_update, ReadCurrent},
                    {write_batch_64_per_doc, Batch64}],
    io:format("PHASES (us per doc, ~p ops per new doc)~n", [NOps]),
    _ = [io:format("  ~-24s ~8.2f~n", [K, V]) || {K, V} <- All],
    All.

build_ops(Db, #{id := Id} = R, Hlc) ->
    Info = #{id => Id, rev => <<"1-abcdef0123456789">>, deleted => false,
             num_conflicts => 0, hlc => Hlc, doc => maps:get(doc, R)},
    Ks = barrel_keyspace:resolve(Db),
    {barrel_ars_index:index_doc_ops(Db, Id, maps:get(doc, R))
     ++ barrel_changes:write_change_ops(Db, Hlc, Info)
     ++ barrel_changes:write_path_index_ops(Db, Hlc, Info)
     ++ [{entity_put, barrel_store_keys:doc_entity(Ks, Id),
          [{<<"v">>, <<"x">>}]},
         {body_put, barrel_store_keys:doc_body(Ks, Id),
          barrel_docdb_codec_cbor:encode_cbor(maps:get(doc, R))}],
     Info}.

write_in_groups(_StoreRef, [], _G) ->
    ok;
write_in_groups(StoreRef, Built, G) ->
    {Grp, Rest} = lists:split(min(G, length(Built)), Built),
    ok = barrel_store_rocksdb:write_batch(
           StoreRef, lists:append([O || {O, _} <- Grp]), #{}),
    write_in_groups(StoreRef, Rest, G).

time_per(Fun, N) ->
    _ = Fun(),
    {T, _} = timer:tc(Fun),
    T / N.

%%====================================================================
%% Internals
%%====================================================================

setup(Case, Writers, Kind) ->
    Db = <<"bench_", (atom_to_binary(Case))/binary, "_",
           (integer_to_binary(erlang:unique_integer([positive])))/binary>>,
    {ok, _} = barrel_docdb:create_db(Db, #{data_dir => data_dir()}),
    ok = seed(Db, Writers, Kind),
    Db.

data_dir() ->
    {ok, Dir} = application:get_env(barrel_docdb, data_dir),
    Dir.

teardown(Db) ->
    _ = barrel_docdb:delete_db(Db),
    ok.

%% Updates rewrite one doc per writer; seed it first.
seed(Db, Writers, update) ->
    lists:foreach(fun(W) ->
        {ok, _} = barrel_docdb:put_doc(Db, doc(id(W, 0), 0))
    end, lists:seq(1, Writers));
seed(_Db, _Writers, _Kind) ->
    ok.

drive(Db, Writers, Kind, WOpts, Duration) ->
    Parent = self(),
    Deadline = erlang:monotonic_time(millisecond) + Duration,
    Pids = [spawn_link(fun() ->
                receive go -> ok end,
                Parent ! {self(), loop(Db, W, Kind, WOpts, Deadline, 1,
                                        undefined, [])}
            end) || W <- lists:seq(1, Writers)],
    _ = [P ! go || P <- Pids],
    lists:append([receive {P, L} -> L end || P <- Pids]).

loop(Db, W, Kind, WOpts, Deadline, N, Rev, Acc) ->
    case erlang:monotonic_time(millisecond) >= Deadline of
        true ->
            Acc;
        false ->
            T0 = erlang:monotonic_time(microsecond),
            Rev1 = op(Db, W, Kind, WOpts, N, Rev),
            T1 = erlang:monotonic_time(microsecond),
            loop(Db, W, Kind, WOpts, Deadline, N + 1, Rev1, [T1 - T0 | Acc])
    end.

op(Db, W, new, WOpts, N, _Rev) ->
    {ok, _} = barrel_docdb:put_doc(Db, doc(id(W, N), N), WOpts),
    undefined;
op(Db, W, update, WOpts, N, Rev0) ->
    Rev = case Rev0 of
        undefined ->
            {ok, #{<<"_rev">> := R}} = barrel_docdb:get_doc(Db, id(W, 0)),
            R;
        R ->
            R
    end,
    Doc = (doc(id(W, 0), N))#{<<"_rev">> => Rev},
    {ok, #{<<"rev">> := NewRev}} = barrel_docdb:put_doc(Db, Doc, WOpts),
    NewRev;
op(Db, W, {put_docs, B}, WOpts, N, _Rev) ->
    Docs = [doc(id(W, N * 1000 + I), N) || I <- lists:seq(1, B)],
    Results = barrel_docdb:put_docs(Db, Docs, WOpts),
    B = length([ok || {ok, _} <- Results]),
    undefined.

id(W, N) ->
    <<"w", (integer_to_binary(W))/binary, "-",
      (integer_to_binary(N))/binary>>.

%% A small execution-record-like document.
doc(Id, N) ->
    #{<<"id">> => Id,
      <<"type">> => <<"step">>,
      <<"n">> => N,
      <<"status">> => <<"done">>,
      <<"input">> => #{<<"name">> => <<"fetch">>, <<"args">> => [1, 2, 3]},
      <<"output">> => binary:copy(<<"x">>, 200)}.

docs_written(Lats, {put_docs, B}) -> length(Lats) * B;
docs_written(Lats, _Kind) -> length(Lats).

summary(Case, Lats, Duration, Kind) ->
    Sorted = lists:sort(Lats),
    N = length(Sorted),
    #{case_name => Case,
      ops => N,
      docs_per_s => round(docs_written(Lats, Kind) * 1000 / Duration),
      ops_per_s => round(N * 1000 / Duration),
      p50_us => pct(Sorted, N, 0.50),
      p99_us => pct(Sorted, N, 0.99)}.

pct(_Sorted, 0, _P) -> 0;
pct(Sorted, N, P) -> lists:nth(max(1, round(N * P)), Sorted).
