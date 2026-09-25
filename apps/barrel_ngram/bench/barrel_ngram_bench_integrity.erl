%%%-------------------------------------------------------------------
%%% @doc Integrity and lease cost on a real corpus: freeze throughput,
%%% fsyncs per freeze, open time, query latency, queries during a
%%% background compaction. Run through `integrity_bench.sh'.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_bench_integrity).

-export([main/1]).

-define(CHUNK, 128).
-define(REPS, 30).
-define(QUERIES, [{rare_literal, search, <<"inet_tls_dist">>},
                  {common_literal, search, <<"gen_server:call">>},
                  {regex, regex, <<"handle_info\\(\\{tcp,">>}]).
-define(RACE_QUERY, <<"gen_server:call">>).

main([CorpusPath, OutDir, RunsStr]) ->
    Runs = list_to_integer(RunsStr),
    ok = application:set_env(barrel_docdb, data_dir, filename:join(OutDir, "docdb")),
    {ok, _} = application:ensure_all_started(barrel_ngram),
    Docs = load_corpus(CorpusPath),
    Integrity = code:ensure_loaded(barrel_ngram_fs) =:= {module, barrel_ngram_fs},
    io:format("docs ~p, body ~.1f MB, integrity build ~p~n",
              [length(Docs), lists:sum([byte_size(B) || {_, B} <- Docs]) / 1.0e6, Integrity]),
    Results = [run_once(I, Docs, OutDir, Integrity) || I <- lists:seq(1, Runs)],
    io:format("~n== median of ~p runs ==~n", [Runs]),
    [io:format("~-28s ~p~n", [K, median([maps:get(K, R) || R <- Results])])
     || K <- lists:sort(maps:keys(hd(Results)))],
    ok = file:write_file(filename:join(OutDir, "results.term"),
                         io_lib:format("~p.~n", [Results])),
    init:stop().

run_once(I, Docs, OutDir, Integrity) ->
    Db = iolist_to_binary(["bench_integ_", integer_to_list(I)]),
    Dir = filename:join(OutDir, "ngram"),
    _ = barrel_ngram:delete_corpus(Db, #{data_dir => Dir}),
    _ = barrel_docdb:delete_db(Db),
    {ok, _} = barrel_docdb:create_db(Db),
    Base = #{db => Db, data_dir => Dir, fields => [<<"body">>]},
    ok = barrel_ngram:open(Db, Base#{compact_threshold => infinity}),
    %% build: chunks of ?CHUNK docs, one refresh (one freeze) each
    start_counts(),
    {PutUs, RefreshUs} = build(Db, Docs, 0, 0),
    {Syncs, DirSyncs} = read_counts(),
    SegBytes = segment_bytes(Dir, Db),
    NSegs = length(segment_files(Dir, Db)),
    BodyBytes = lists:sum([byte_size(B) || {_, B} <- Docs]),
    %% queries on the built corpus
    QRes = lists:foldl(fun(Q, Acc) -> maps:merge(Acc, query_stats(Db, Q)) end, #{}, ?QUERIES),
    %% open time, default verification, then layout only
    ok = barrel_ngram:close(Db),
    {OpenUs, ok} = timer:tc(fun() -> barrel_ngram:open(Db, Base) end),
    ok = barrel_ngram:close(Db),
    OpenLayout = case Integrity of
        true ->
            {UsL, ok} = timer:tc(fun() -> barrel_ngram:open(Db, Base#{verify_segments => layout}) end),
            ok = barrel_ngram:close(Db),
            #{open_layout_ms => UsL / 1000};
        false ->
            #{}
    end,
    %% queries racing a background compaction of every segment
    ok = barrel_ngram:open(Db, Base#{compact_threshold => NSegs + 1}),
    Race = race(Db),
    ok = barrel_ngram:close(Db),
    ok = barrel_ngram:delete_corpus(Db, #{data_dir => Dir}),
    ok = barrel_docdb:delete_db(Db),
    R = maps:merge(maps:merge(QRes, OpenLayout), Race#{
        build_put_s => PutUs / 1.0e6,
        build_refresh_s => RefreshUs / 1.0e6,
        build_mb_per_s => BodyBytes / RefreshUs,
        build_docs_per_s => length(Docs) / (RefreshUs / 1.0e6),
        segments => NSegs,
        segment_mb_per_freeze => SegBytes / NSegs / 1.0e6,
        file_syncs_per_freeze => Syncs / NSegs,
        dir_syncs_per_freeze => DirSyncs / NSegs,
        open_default_ms => OpenUs / 1000}),
    io:format("run ~p: ~p~n", [I, R]),
    R.

build(_Db, [], PutUs, RefreshUs) ->
    {PutUs, RefreshUs};
build(Db, Docs, PutUs, RefreshUs) ->
    {Chunk, Rest} = lists:split(min(?CHUNK, length(Docs)), Docs),
    {Pu, _} = timer:tc(fun() ->
        [{ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => Id, <<"body">> => Body})
         || {Id, Body} <- Chunk]
    end),
    {Ru, {ok, _}} = timer:tc(fun() -> barrel_ngram:refresh(Db) end),
    build(Db, Rest, PutUs + Pu, RefreshUs + Ru).

query_stats(Db, {Name, Fun, Q}) ->
    {ok, Hits} = barrel_ngram:Fun(Db, Q),
    Lat = [begin
               {Us, {ok, _}} = timer:tc(fun() -> barrel_ngram:Fun(Db, Q) end),
               Us / 1000
           end || _ <- lists:seq(1, ?REPS)],
    #{key(Name, "_p50_ms") => pct(Lat, 0.50),
      key(Name, "_p95_ms") => pct(Lat, 0.95),
      key(Name, "_hits") => length(Hits)}.

%% One more doc and a refresh add the segment that crosses the compact
%% threshold; query until the background merge has swapped the manifest.
race(Db) ->
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"race">>,
                                         <<"body">> => <<"gen_server:call race">>}),
    T0 = erlang:monotonic_time(microsecond),
    {ok, _} = barrel_ngram:refresh(Db),
    {Lat, Errors} = race_loop(Db, [], 0, T0 + 300000000),
    DoneUs = erlang:monotonic_time(microsecond) - T0,
    #{race_p50_ms => pct(Lat, 0.50), race_p95_ms => pct(Lat, 0.95),
      race_queries => length(Lat) + Errors, race_errors => Errors,
      race_compaction_s => DoneUs / 1.0e6}.

race_loop(Db, Lat, Errors, Deadline) ->
    case compacted(Db) orelse erlang:monotonic_time(microsecond) > Deadline of
        true ->
            {Lat, Errors};
        false ->
            case timer:tc(fun() -> barrel_ngram:search(Db, ?RACE_QUERY) end) of
                {Us, {ok, _}} -> race_loop(Db, [Us / 1000 | Lat], Errors, Deadline);
                {_Us, {error, _}} -> race_loop(Db, Lat, Errors + 1, Deadline)
            end
    end.

compacted(Db) ->
    {ok, Segs} = barrel_ngram_shard:get_manifest(Db),
    length(Segs) =:= 1.

%% fsync counters: file:sync/1 and, on the integrity build, directory fsyncs
start_counts() ->
    _ = erlang:trace_pattern({file, sync, 1}, false, [call_count]),
    1 = erlang:trace_pattern({file, sync, 1}, true, [call_count]),
    case code:is_loaded(barrel_ngram_fs) of
        {file, _} ->
            _ = erlang:trace_pattern({barrel_ngram_fs, fsync_dir, 1}, false, [local, call_count]),
            1 = erlang:trace_pattern({barrel_ngram_fs, fsync_dir, 1}, true, [local, call_count]);
        false ->
            ok
    end,
    ok.

read_counts() ->
    {call_count, S} = erlang:trace_info({file, sync, 1}, call_count),
    D = case code:is_loaded(barrel_ngram_fs) of
        {file, _} ->
            {call_count, N} = erlang:trace_info({barrel_ngram_fs, fsync_dir, 1}, call_count),
            N;
        false ->
            0
    end,
    {S, D}.

load_corpus(Path) ->
    {ok, Bin} = file:read_file(Path),
    [begin
         #{<<"id">> := Id, <<"body">> := Body} = json:decode(L),
         {Id, Body}
     end || L <- binary:split(Bin, <<"\n">>, [global]), L =/= <<>>].

segment_files(Dir, Db) ->
    filelib:wildcard(unicode:characters_to_list(filename:join([Dir, Db, "**", "*.ngseg"]))).

segment_bytes(Dir, Db) ->
    lists:sum([filelib:file_size(F) || F <- segment_files(Dir, Db)]).

pct([], _P) -> 0;
pct(L, P) ->
    S = lists:sort(L),
    lists:nth(max(1, ceil(P * length(S))), S).

median(L) ->
    S = lists:sort(L),
    lists:nth((length(S) + 1) div 2, S).

key(Name, Suffix) ->
    list_to_atom(atom_to_list(Name) ++ Suffix).
