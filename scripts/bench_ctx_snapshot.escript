#!/usr/bin/env escript
%%! -noshell
%%% Spike S4: snapshot size and preparation cost.
%%%
%%% For corpora of 1, 10 and all apps of the OTP module corpus, with and
%%% without vectors, and a disk or memory BM25 index: build a plain
%%% composed database, export it (barrel_ctx_export:export/3), import it
%%% under a new name, then time the cold read-only open and report the ANN
%%% graph's index_origin. A disk BM25 index is copied as written; a memory
%%% one is rebuilt from the stored text at open.
%%%
%%% Usage (from the umbrella root, after `rebar3 compile'):
%%%   escript scripts/bench_ctx_snapshot.escript \
%%%       [--corpus corpus.jsonl] [--apps 1,10,all] [--vectors both|yes|no] \
%%%       [--bm25 both|disk|memory] [--runs 1] [--dir _build/bench_ctx] \
%%%       [--out results.json]
%%%
%%% Vectors are 64-dim feature-hashed token counts (no embedding
%%% service). Numbers depend on the machine and on concurrent load.

-mode(compile).

-define(DIM, 64).

main(Args) ->
    Opts = parse(Args, #{corpus => default_corpus(), apps => ["1", "10", "all"],
                         vectors => "both", bm25 => "both", runs => 1,
                         dir => "_build/bench_ctx", out => undefined}),
    add_paths(),
    Dir = filename:absname(maps:get(dir, Opts)),
    _ = file:del_dir_r(Dir),
    ok = filelib:ensure_path(Dir),
    ok = application:load(barrel_docdb),
    ok = application:set_env(barrel_docdb, data_dir,
                             filename:join(Dir, "docdb_default")),
    ok = application:load(barrel),
    ok = application:set_env(barrel, ctx_dir, filename:join(Dir, "ctx")),
    ok = logger:set_primary_config(level, warning),
    {ok, _} = application:ensure_all_started(barrel),
    Corpus = load(maps:get(corpus, Opts)),
    Ranked = rank_apps(Corpus),
    io:format("corpus: ~b docs, ~b apps~n", [length(Corpus), length(Ranked)]),
    Rows = [run(Dir, Corpus, Ranked, N, V, Bm, R)
            || N <- maps:get(apps, Opts),
               V <- vector_modes(maps:get(vectors, Opts)),
               Bm <- bm25_modes(maps:get(bm25, Opts)),
               R <- lists:seq(1, maps:get(runs, Opts))],
    print(Rows),
    case maps:get(out, Opts) of
        undefined -> ok;
        Out -> ok = file:write_file(Out, json:encode(Rows))
    end,
    halt(0).

run(Dir, Corpus, Ranked, NApps, Vectors, Bm25, Run) ->
    Apps = select(Ranked, NApps),
    Docs = [D || #{<<"app">> := A} = D <- Corpus, lists:member(A, Apps)],
    Tag = io_lib:format("~s_~s_~s_~b", [NApps, vtag(Vectors), Bm25, Run]),
    Src = iolist_to_binary(["bsrc_", Tag]),
    Base = filename:join(Dir, lists:flatten(Tag)),
    OpenOpts = #{docdb => #{data_dir => filename:join(Base, "docdb")},
                 vectordb => #{dimension => ?DIM,
                               db_path => filename:join(Base, "vec"),
                               bm25_backend => list_to_atom(Bm25)}},
    {ok, Db} = barrel_dbs:ensure(Src, OpenOpts),
    {LoadMs, _} = timed(fun() -> load_db(Db, Docs, Vectors) end),
    Export = filename:join(Base, "export"),
    {ok, Exp} = barrel_ctx_export:export(Src, Export, #{}),
    ok = barrel_dbs:close(Src),
    Name = iolist_to_binary(["bimp_", Tag]),
    {ImportMs, {ok, _}} = timed(fun() ->
        barrel_ctx_export:import(Export, #{name => Name, open => false})
    end),
    {OpenMs, {ok, Imp}} = timed(fun() -> barrel_ctx_export:open(Name) end),
    {ok, Stats} = barrel:vector_stats(Imp),
    Q = vec(<<"process supervisor restart">>),
    {QueryMs, {ok, _}} = timed(fun() ->
        barrel:search_vector(Imp, Q, #{k => 10})
    end),
    {Bm25Ms, {ok, _}} = timed(fun() ->
        barrel:search_bm25(Imp, <<"supervisor">>, #{k => 10})
    end),
    %% a second cold open (page cache warm)
    ok = barrel_dbs:close(Name),
    {ReopenMs, {ok, _}} = timed(fun() -> barrel_ctx_export:open(Name) end),
    ok = barrel_ctx_export:remove_import(Name),
    _ = barrel_dbs:destroy(Src),
    Row = #{apps => NApps, vectors => Vectors, bm25 => Bm25, run => Run,
            docs => length(Docs), load_ms => LoadMs,
            export_ms => maps:get(elapsed_ms, Exp),
            artifacts => maps:get(artifacts, Exp),
            bytes => maps:get(bytes, Exp),
            import_ms => ImportMs, cold_open_ms => OpenMs,
            reopen_ms => ReopenMs,
            index_origin => maps:get(index_origin, Stats),
            vectors_loaded => maps:get(count, Stats),
            first_vector_query_ms => QueryMs,
            first_bm25_query_ms => Bm25Ms},
    io:format("~p~n", [Row]),
    Row.

load_db(Db, Docs, Vectors) ->
    lists:foreach(fun(Chunk) ->
        [{ok, _} = R || R <- barrel:put_docs(Db, Chunk)],
        case Vectors of
            true ->
                Batch = [{Id, B, #{}, vec(B)}
                         || #{<<"id">> := Id, <<"body">> := B} <- Chunk],
                {ok, _} = barrel:vector_add_batch(Db, Batch);
            false ->
                ok
        end
    end, chunks(Docs, 200)).

%%--------------------------------------------------------------------

parse([], Acc) -> Acc;
parse(["--corpus", V | T], Acc) -> parse(T, Acc#{corpus => V});
parse(["--apps", V | T], Acc) ->
    parse(T, Acc#{apps => string:lexemes(V, ",")});
parse(["--vectors", V | T], Acc) -> parse(T, Acc#{vectors => V});
parse(["--bm25", V | T], Acc) -> parse(T, Acc#{bm25 => V});
parse(["--runs", V | T], Acc) -> parse(T, Acc#{runs => list_to_integer(V)});
parse(["--dir", V | T], Acc) -> parse(T, Acc#{dir => V});
parse(["--out", V | T], Acc) -> parse(T, Acc#{out => V});
parse([Other | _], _Acc) ->
    io:format(standard_error, "unknown argument ~s~n", [Other]),
    halt(2).

default_corpus() ->
    os:getenv("BARREL_CTX_CORPUS", "corpus.jsonl").

add_paths() ->
    Root = filename:dirname(filename:dirname(filename:absname(
                                               escript:script_name()))),
    Paths = filelib:wildcard(filename:join(Root,
                                           "_build/default/lib/*/ebin")),
    ok = code:add_pathsa(Paths).

vector_modes("both") -> [false, true];
vector_modes("yes") -> [true];
vector_modes("no") -> [false].

bm25_modes("both") -> ["disk", "memory"];
bm25_modes(M) -> [M].

vtag(true) -> "vec";
vtag(false) -> "novec".

load(File) ->
    {ok, Bin} = file:read_file(File),
    [json:decode(L) || L <- binary:split(Bin, <<"\n">>, [global]), L =/= <<>>].

%% Apps by descending document count, ties by name.
rank_apps(Corpus) ->
    Counts = lists:foldl(fun(#{<<"app">> := A}, M) ->
                             maps:update_with(A, fun(C) -> C + 1 end, 1, M)
                         end, #{}, Corpus),
    [A || {_, A} <- lists:sort([{-C, A} || {A, C} <- maps:to_list(Counts)])].

select(Ranked, "all") -> Ranked;
select(Ranked, N) -> lists:sublist(Ranked, list_to_integer(N)).

vec(Text) ->
    Tokens = [T || T <- re:split(string:lowercase(Text), "[^a-z0-9_]+",
                                 [{return, binary}]), T =/= <<>>],
    Counts = lists:foldl(fun(T, Acc) ->
                             I = erlang:phash2(T, ?DIM) + 1,
                             setelement(I, Acc, element(I, Acc) + 1.0)
                         end, erlang:make_tuple(?DIM, 0.0), Tokens),
    L = tuple_to_list(Counts),
    case math:sqrt(lists:sum([X * X || X <- L])) of
        +0.0 -> [1.0 | lists:duplicate(?DIM - 1, 0.0)];
        Norm -> [X / Norm || X <- L]
    end.

chunks([], _N) -> [];
chunks(L, N) when length(L) =< N -> [L];
chunks(L, N) -> {A, B} = lists:split(N, L), [A | chunks(B, N)].

timed(Fun) ->
    T0 = erlang:monotonic_time(microsecond),
    R = Fun(),
    {(erlang:monotonic_time(microsecond) - T0) / 1000, R}.

print(Rows) ->
    io:format("~n| apps | vectors | bm25 | docs | export ms | bytes | "
              "artifacts | import ms | cold open ms | reopen ms | "
              "index_origin | 1st vec q ms | 1st bm25 q ms |~n"
              "|---|---|---|---|---|---|---|---|---|---|---|---|---|~n"),
    [io:format("| ~s | ~p | ~s | ~b | ~b | ~b | ~b | ~.1f | ~.1f | ~.1f | "
               "~p | ~.2f | ~.2f |~n",
               [A, V, Bm, D, E, B, Ar, I, O, O2, Or, Q, Bq])
     || #{apps := A, vectors := V, bm25 := Bm, docs := D, export_ms := E,
          bytes := B, artifacts := Ar, import_ms := I, cold_open_ms := O,
          reopen_ms := O2,
          index_origin := Or, first_vector_query_ms := Q,
          first_bm25_query_ms := Bq} <- Rows],
    ok.
