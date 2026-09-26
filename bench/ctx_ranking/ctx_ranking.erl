%% @doc Spike S2: cross-context ranking quality on one corpus split
%% into per-app contexts, against a single index over the union.
-module(ctx_ranking).

-export([main/1]).

-define(K, 10).
-define(TEXT_CHARS, 6000).
-define(CARD_SENTENCES, 10).
-define(RERANK_POOL, 100).
%% the cross-encoder truncates at 512 tokens anyway
-define(RERANK_CHARS, 2000).
-define(DIM, 768).
-define(URL, <<"http://localhost:11434">>).
-define(MODEL, <<"nomic-embed-text">>).
-define(RERANK_MODEL, "cross-encoder/ms-marco-MiniLM-L-6-v2").
-define(SCOPES, [1, 3, 5, 10, 33]).

%%====================================================================
%% Entry point
%%====================================================================

main([CorpusDir, WorkDir, "embed"]) ->
    ok = setup(WorkDir),
    Docs = load_docs(filename:join(CorpusDir, "corpus.jsonl")),
    Queries = load_queries(filename:join(CorpusDir, "queries.jsonl")),
    Apps = lists:usort([maps:get(app, D) || D <- Docs]),
    _ = ensure_vectors(WorkDir, [plain, prefix], Docs, Queries,
                       build_cards(Docs, Queries, Apps)),
    ok;
main([CorpusDir, WorkDir | Rest]) ->
    RerankPython = case Rest of
        [P | _] when P =/= "" -> P;
        _ -> undefined
    end,
    ok = setup(WorkDir),
    Docs = load_docs(filename:join(CorpusDir, "corpus.jsonl")),
    Queries = load_queries(filename:join(CorpusDir, "queries.jsonl")),
    Apps = lists:usort([maps:get(app, D) || D <- Docs]),
    log("~b docs, ~b queries, ~b apps", [length(Docs), length(Queries),
                                         length(Apps)]),
    Cards = build_cards(Docs, Queries, Apps),
    Variants = [plain, prefix],
    Vecs = ensure_vectors(WorkDir, Variants, Docs, Queries, Cards),
    Rerank = start_rerank(RerankPython),
    Out = [run_variant(V, WorkDir, Docs, Queries, Apps, Cards, Vecs, Rerank)
           || V <- Variants],
    Report = report(Docs, Apps, Queries, Out, Rerank),
    ok = file:write_file(filename:join(WorkDir, "results.md"), Report),
    io:put_chars(Report),
    ok.

setup(WorkDir) ->
    %% no managed venvs, no model downloads
    ok = application:load(barrel_embed),
    ok = application:set_env(barrel_embed, managed_venv, false),
    os:putenv("HF_HUB_OFFLINE", "1"),
    os:putenv("TRANSFORMERS_OFFLINE", "1"),
    {ok, _} = application:ensure_all_started(barrel),
    Data = filename:join(WorkDir, "data"),
    _ = file:del_dir_r(Data),
    ok = filelib:ensure_path(Data).

log(Fmt, Args) ->
    io:format(standard_error, "[ctx_ranking] " ++ Fmt ++ "~n", Args).

%%====================================================================
%% Corpus
%%====================================================================

load_jsonl(File) ->
    {ok, Bin} = file:read_file(File),
    [json:decode(L) || L <- binary:split(Bin, <<"\n">>, [global]), L =/= <<>>].

load_docs(File) ->
    [#{id => Id, app => App, path => Path,
       text => slice(Body, ?TEXT_CHARS), moduledoc => Md}
     || #{<<"id">> := Id, <<"app">> := App, <<"path">> := Path,
          <<"body_nodoc">> := Body, <<"moduledoc">> := Md}
            <- load_jsonl(File)].

load_queries(File) ->
    Qs = load_jsonl(File),
    [#{i => I, query => Q, target => T, app => A}
     || {I, #{<<"query">> := Q, <<"target">> := T, <<"app">> := A}}
            <- lists:zip(lists:seq(1, length(Qs)), Qs)].

slice(Bin, N) ->
    unicode:characters_to_binary(string:slice(Bin, 0, N)).

%% Policy text: fields [path, text] joined with "\n".
doc_text(#{path := P, text := T}) ->
    <<P/binary, "\n", T/binary>>.

%%====================================================================
%% Catalog cards (leave-one-out for the query's own target)
%%====================================================================

build_cards(Docs, Queries, Apps) ->
    Sentences = fun(App, Exclude) ->
        Mods = lists:sort([{Id, Md} || #{app := A, id := Id, moduledoc := Md}
                                           <- Docs, A =:= App, Md =/= <<>>,
                                       Id =/= Exclude]),
        [Md || {_, Md} <- lists:sublist(Mods, ?CARD_SENTENCES)]
    end,
    Base = [#{id => <<"card:", App/binary>>, app => App,
              body => join(Sentences(App, none))} || App <- Apps],
    BaseBody = maps:from_list([{A, B} || #{app := A, body := B} <- Base]),
    Loo = lists:usort(
            [#{id => <<"loo:", T/binary>>, app => A, body => Body}
             || #{target := T, app := A} <- Queries,
                Body <- [join(Sentences(A, T))],
                Body =/= maps:get(A, BaseBody)]),
    LooIds = maps:from_list([{binary:part(Id, 4, byte_size(Id) - 4), Id}
                             || #{id := Id} <- Loo]),
    #{cards => Base ++ Loo, loo => LooIds}.

join(Parts) ->
    iolist_to_binary(lists:join(<<"\n">>, Parts)).

card_text(#{app := App, body := Body}) ->
    <<App/binary, "\n", Body/binary>>.

%% The card ids a query may see: base cards, except the target's app
%% uses the card without the target's own sentence.
allowed_cards(#{target := T, app := A}, #{loo := Loo}) ->
    fun(<<"card:", App/binary>>) -> App =/= A orelse not maps:is_key(T, Loo);
       (<<"loo:", T2/binary>>) -> T2 =:= T
    end.

card_app(<<"card:", App/binary>>, _Cards) -> App;
card_app(<<"loo:", _/binary>> = Id, #{cards := Cards}) ->
    hd([A || #{id := I, app := A} <- Cards, I =:= Id]).

%%====================================================================
%% Embeddings (cached by input hash)
%%====================================================================

doc_prefix(plain) -> <<>>;
doc_prefix(prefix) -> <<"search_document: ">>.

query_prefix(plain) -> <<>>;
query_prefix(prefix) -> <<"search_query: ">>.

doc_input(V, D) -> <<(doc_prefix(V))/binary, (doc_text(D))/binary>>.
card_input(V, C) -> <<(doc_prefix(V))/binary, (card_text(C))/binary>>.
query_input(V, #{query := Q}) -> <<(query_prefix(V))/binary, Q/binary>>.

ensure_vectors(WorkDir, Variants, Docs, Queries, #{cards := Cards}) ->
    File = filename:join(WorkDir, "embeddings.etf"),
    Cache0 = case file:read_file(File) of
        {ok, Bin} -> binary_to_term(Bin);
        {error, enoent} -> #{}
    end,
    Inputs = lists:usort(
               [doc_input(V, D) || V <- Variants, D <- Docs] ++
               [card_input(V, C) || V <- Variants, C <- Cards] ++
               [query_input(V, Q) || V <- Variants, Q <- Queries]),
    Missing = [I || I <- Inputs, not maps:is_key(hash(I), Cache0)],
    log("embeddings: ~b inputs, ~b cached, ~b to embed",
        [length(Inputs), length(Inputs) - length(Missing), length(Missing)]),
    Cache = embed_missing(Missing, Cache0, File),
    Cache.

embed_missing([], Cache, _File) ->
    Cache;
embed_missing(Missing, Cache0, File) ->
    {ok, Embed} = barrel_embed:init(#{
        embedder => {ollama, #{url => ?URL, model => ?MODEL,
                               timeout => 300000}},
        dimensions => ?DIM, batch_size => 8}),
    Cache = embed_chunks(chunks(Missing, 64), Embed, Cache0, File, 0,
                         length(Missing)),
    ok = file:write_file(File, term_to_binary(Cache)),
    Cache.

embed_chunks([], _Embed, Cache, _File, _Done, _Total) ->
    Cache;
embed_chunks([Chunk | Rest], Embed, Cache0, File, Done, Total) ->
    {ok, Vs} = barrel_embed:embed_batch(Chunk, Embed),
    ?DIM = length(hd(Vs)),
    Cache = lists:foldl(fun({I, V}, Acc) -> Acc#{hash(I) => V} end,
                        Cache0, lists:zip(Chunk, Vs)),
    ok = file:write_file(File, term_to_binary(Cache)),
    log("embedded ~b/~b", [Done + length(Chunk), Total]),
    embed_chunks(Rest, Embed, Cache, File, Done + length(Chunk), Total).

hash(Input) -> crypto:hash(sha256, Input).

vec(Vecs, Input) -> maps:get(hash(Input), Vecs).

chunks([], _N) -> [];
chunks(L, N) when length(L) =< N -> [L];
chunks(L, N) -> {H, T} = lists:split(N, L), [H | chunks(T, N)].

%%====================================================================
%% Databases
%%====================================================================

policy() ->
    #{fields => [<<"path">>, <<"text">>], join => <<"\n">>, mode => sync,
      dimensions => ?DIM,
      embedder => {ollama, #{url => ?URL, model => ?MODEL}}}.

open_db(Name, WorkDir) ->
    Data = filename:join(WorkDir, "data"),
    {ok, Db} = barrel:open(Name, #{
        embedding => policy(),
        docdb => #{data_dir => Data},
        vectordb => #{db_path => binary_to_list(filename:join(Data, <<Name/binary, "_vec">>)),
                      bm25_backend => disk}}),
    Db.

load(Db, Rows) ->
    lists:foreach(
      fun(Chunk) ->
              Results = barrel:put_docs(Db, Chunk, #{}),
              [] = [R || R <- Results, element(1, R) =/= ok]
      end, chunks(Rows, 100)).

db_name(V, Suffix) ->
    <<"ctxr_", (atom_to_binary(V))/binary, "_", Suffix/binary>>.

build_dbs(V, WorkDir, Docs, Apps, #{cards := Cards}, Vecs) ->
    Row = fun(D) ->
        #{<<"id">> => maps:get(id, D), <<"app">> => maps:get(app, D),
          <<"path">> => maps:get(path, D), <<"text">> => maps:get(text, D),
          <<"_embedding">> => vec(Vecs, doc_input(V, D))}
    end,
    Ctx = maps:from_list(
            [begin
                 Db = open_db(db_name(V, App), WorkDir),
                 ok = load(Db, [Row(D) || #{app := A} = D <- Docs, A =:= App]),
                 {App, Db}
             end || App <- Apps]),
    Union = open_db(db_name(V, <<"_union">>), WorkDir),
    ok = load(Union, [Row(D) || D <- Docs]),
    Catalog = open_db(db_name(V, <<"_catalog">>), WorkDir),
    ok = load(Catalog,
              [#{<<"id">> => Id, <<"app">> => App, <<"path">> => App,
                 <<"text">> => Body, <<"_embedding">> => vec(Vecs, card_input(V, C))}
               || #{id := Id, app := App, body := Body} = C <- Cards]),
    {Ctx, Union, Catalog}.

%%====================================================================
%% Searches
%%====================================================================

search_vec(Db, QV, K) ->
    {ok, Rs} = barrel:search_vector(Db, QV, #{k => K, ef_search => max(K, 50),
                                               include_text => false,
                                               include_metadata => false}),
    [{Id, S} || #{key := Id, score := S} <- Rs].

search_bm25(Db, Q, K) ->
    {ok, Rs} = barrel:search_bm25(Db, Q, #{k => K}),
    Rs.

%% What hybrid_top_k runs, with the cached query vector.
search_hybrid(#{vstore := Store}, Q, QV, K) ->
    {ok, Rs} = barrel_vectordb:search_hybrid(
                 Store, Q, #{k => K, fusion => rrf, query_vector => QV,
                             include_text => false, include_metadata => false}),
    [{Id, S} || #{key := Id, score := S} <- Rs].

%% Exact cosine over the union: the brute-force oracle.
exact_union(Norm, QV, K) ->
    Q = normalize(QV),
    Scored = [{-dot(Q, V, 0.0), Id} || {Id, V} <- Norm],
    [{Id, -S} || {S, Id} <- lists:sublist(lists:sort(Scored), K)].

normalize(V) ->
    N = math:sqrt(dot(V, V, 0.0)),
    [X / N || X <- V].

dot([A | As], [B | Bs], Acc) -> dot(As, Bs, Acc + A * B);
dot([], [], Acc) -> Acc.

%%====================================================================
%% One variant
%%====================================================================

run_variant(V, WorkDir, Docs, Queries, Apps, Cards, Vecs, Rerank) ->
    log("variant ~p: building dbs", [V]),
    {Ctx, Union, Catalog} = build_dbs(V, WorkDir, Docs, Apps, Cards, Vecs),
    Info = fingerprints(Ctx, Union),
    Norm = [{Id, normalize(vec(Vecs, doc_input(V, D)))} || #{id := Id} = D <- Docs],
    NCards = length(maps:get(cards, Cards)),
    Texts = maps:from_list([{Id, doc_text(D)} || #{id := Id} = D <- Docs]),
    log("variant ~p: searching", [V]),
    PerQuery = [one_query(V, Q, Ctx, Union, Catalog, Apps, Cards, NCards,
                          Norm, Vecs, Info, Texts, Rerank)
                || Q <- Queries],
    [ok = barrel:close(Db) || Db <- [Union, Catalog | maps:values(Ctx)]],
    #{variant => V, queries => PerQuery, info => Info}.

fingerprints(Ctx, Union) ->
    Infos = [{App, element(2, barrel:embedder_info(Db))}
             || {App, Db} <- maps:to_list(Ctx)],
    {ok, UInfo} = barrel:embedder_info(Union),
    Fps = lists:usort([maps:get(fingerprint, I, undefined) || {_, I} <- Infos]),
    #{union => UInfo, distinct_fingerprints => Fps,
      per_ctx => maps:from_list(Infos)}.

one_query(V, #{query := Q} = Query, Ctx, Union, Catalog, Apps, Cards, NCards,
          Norm, Vecs, Info, Texts, Rerank) ->
    QV = vec(Vecs, query_input(V, Query)),
    Per = maps:from_list(
            [{App, #{vector => search_vec(Db, QV, ?K),
                     bm25 => search_bm25(Db, Q, ?K),
                     hybrid => search_hybrid(Db, Q, QV, ?K)}}
             || {App, Db} <- maps:to_list(Ctx)]),
    UnionRes = #{vector => search_vec(Union, QV, ?K),
                 bm25 => search_bm25(Union, Q, ?K),
                 hybrid => search_hybrid(Union, Q, QV, ?K),
                 exact => exact_union(Norm, QV, ?K),
                 hybrid_pool => search_hybrid(Union, Q, QV, ?RERANK_POOL)},
    Allowed = allowed_cards(Query, Cards),
    CardRank = fun(Hits) ->
        Seen = dedup([card_app(Id, Cards) || {Id, _} <- Hits, Allowed(Id)]),
        Seen ++ (Apps -- Seen)
    end,
    Scope = #{card_vector => CardRank(search_vec(Catalog, QV, NCards)),
              card_bm25 => CardRank(search_bm25(Catalog, Q, NCards)),
              card_hybrid => CardRank(search_hybrid(Catalog, Q, QV, NCards))},
    Fp = fun(App) -> maps:get(App, maps:get(per_ctx, Info)) end,
    Merged = merges(Per, Apps, Fp, Scope, Query, Texts, Rerank, UnionRes),
    #{query => Query, per => Per, union => UnionRes, scope => Scope,
      merged => Merged}.

dedup(L) -> dedup(L, #{}, []).
dedup([], _, Acc) -> lists:reverse(Acc);
dedup([H | T], S, Acc) ->
    case maps:is_key(H, S) of
        true -> dedup(T, S, Acc);
        false -> dedup(T, S#{H => true}, [H | Acc])
    end.

%%====================================================================
%% Merges (through barrel_ctx_merge)
%%====================================================================

members(Per, Apps, Method, Fp) ->
    [begin
         I = Fp(App),
         #{ctx => App,
           rows => [#{<<"id">> => Id, <<"_score">> => S}
                    || {Id, S} <- maps:get(Method, maps:get(App, Per))],
           fingerprint => maps:get(fingerprint, I, undefined),
           distance => maps:get(distance, I, undefined)}
     end || App <- Apps].

ids(Rows) -> [maps:get(<<"id">>, R) || R <- Rows].

merges(Per, Apps, Fp, Scope, #{query := Q, i := QI}, Texts, Rerank, UnionRes) ->
    M = fun(Method, Order) -> members(Per, Order, Method, Fp) end,
    Ok = fun({ok, Rows, _Meta}) -> ids(Rows) end,
    CardOrder = maps:get(card_hybrid, Scope),
    Base = #{
      {interleave, vector} => Ok(barrel_ctx_merge:interleave(M(vector, Apps), ?K)),
      {interleave, bm25} => Ok(barrel_ctx_merge:interleave(M(bm25, Apps), ?K)),
      {interleave, hybrid} => Ok(barrel_ctx_merge:interleave(M(hybrid, Apps), ?K)),
      {interleave_card, hybrid} =>
          Ok(barrel_ctx_merge:interleave(M(hybrid, CardOrder), ?K)),
      {score, vector} => Ok(barrel_ctx_merge:score(M(vector, Apps), ?K, #{})),
      {rrf, hybrid} => Ok(barrel_ctx_merge:rrf(M(hybrid, Apps), ?K, #{})),
      {raw_score, bm25} => raw_sort(M(bm25, Apps)),
      {raw_score, hybrid} => raw_sort(M(hybrid, Apps)),
      score_refused_bm25 => refused(M(bm25, Apps))
     },
    Scoped = maps:from_list(
               [{{scoped, SM, N}, scoped(Per, Fp, maps:get(SM, Scope), N)}
                || SM <- [card_vector, card_bm25, card_hybrid], N <- ?SCOPES]),
    maps:merge(maps:merge(Base, Scoped),
               rerank_merges(Rerank, QI, Q, M(hybrid, Apps), Texts, UnionRes)).

%% The naive global sort by raw member scores (not offered by the plan).
raw_sort(Members) ->
    Rows = [R#{<<"_ctx">> => C} || #{ctx := C, rows := Rs} <- Members, R <- Rs],
    Sorted = lists:sort(fun(A, B) ->
        {-maps:get(<<"_score">>, A), maps:get(<<"_ctx">>, A), maps:get(<<"id">>, A)}
            =< {-maps:get(<<"_score">>, B), maps:get(<<"_ctx">>, B), maps:get(<<"id">>, B)}
    end, Rows),
    ids(lists:sublist(Sorted, ?K)).

%% score merge must refuse members without a fingerprint.
refused(Members) ->
    Stripped = [maps:remove(fingerprint, Mb) || Mb <- Members],
    case barrel_ctx_merge:score(Stripped, ?K, #{}) of
        {error, _} -> true;
        {ok, _, _} -> false
    end.

scoped(Per, Fp, Order, N) ->
    Sel = lists:sublist(Order, N),
    {ok, Score, _} = barrel_ctx_merge:score(members(Per, Sel, vector, Fp), ?K, #{}),
    {ok, Inter, _} = barrel_ctx_merge:interleave(members(Per, Sel, hybrid, Fp), ?K),
    {ok, Groups, _} = barrel_ctx_merge:grouped(members(Per, Sel, hybrid, Fp)),
    #{selected => Sel, score => ids(Score), interleave => ids(Inter),
      grouped => [{C, ids(Rs)} || #{ctx := C, rows := Rs} <- Groups]}.

rerank_merges(undefined, _QI, _Q, _Members, _Texts, _UnionRes) ->
    #{};
rerank_merges({Server, Cache}, QI, Q, Members, Texts, UnionRes) ->
    Pool = barrel_ctx_merge:rerank_pool(Members, ?RERANK_POOL),
    UPool = [#{<<"id">> => Id, <<"_ctx">> => union, <<"_score">> => S}
             || {Id, S} <- maps:get(hybrid_pool, UnionRes)],
    #{{rerank, hybrid} => rerank_ids(Server, Cache, QI, Q, Pool, Texts),
      {rerank_union, hybrid} => rerank_ids(Server, Cache, QI, Q, UPool, Texts)}.

rerank_ids(Server, Cache, QI, Q, Pool, Texts) ->
    Ids = ids(Pool),
    Scores = rerank_scores(Server, Cache, QI, Q, Ids, Texts),
    Indexed = [{I - 1, S} || {I, S} <- lists:zip(lists:seq(1, length(Ids)), Scores)],
    {ok, Rows, _} = barrel_ctx_merge:rerank(Pool, Indexed, ?K),
    ids(Rows).

%% Cross-encoder scores, cached per (query, doc) in an ets table.
rerank_scores(Server, Cache, QI, Q, Ids, Texts) ->
    Missing = [Id || Id <- Ids, ets:lookup(Cache, {QI, Id}) =:= []],
    case Missing of
        [] -> ok;
        _ ->
            %% the sidecar reads one JSON line of at most 64 KiB
            [begin
                 {ok, Res} = barrel_rerank:rerank(
                               Server, Q,
                               [slice(maps:get(Id, Texts), ?RERANK_CHARS)
                                || Id <- Chunk],
                               #{timeout => 600000}),
                 Arr = list_to_tuple(Chunk),
                 [ets:insert(Cache, {{QI, element(I + 1, Arr)}, S})
                  || {I, S} <- Res]
             end || Chunk <- chunks(Missing, 8)]
    end,
    [ets:lookup_element(Cache, {QI, Id}, 2) || Id <- Ids].

start_rerank(undefined) ->
    undefined;
start_rerank(Python) ->
    case barrel_rerank:start_link(#{python => Python, model => ?RERANK_MODEL,
                                    timeout => 600000}) of
        {ok, Pid} ->
            log("rerank: started ~s", [?RERANK_MODEL]),
            {Pid, ets:new(rerank_cache, [public, set])};
        {error, Reason} ->
            log("rerank: skipped (~p)", [Reason]),
            undefined
    end.

%%====================================================================
%% Metrics and report
%%====================================================================

rank(Target, Ids) -> rank(Target, Ids, 1).
rank(_T, [], _N) -> none;
rank(T, [T | _], N) -> N;
rank(T, [_ | R], N) -> rank(T, R, N + 1).

metrics(Ranks) ->
    N = length(Ranks),
    In = [R || R <- Ranks, R =/= none, R =< ?K],
    #{r1 => length([1 || 1 <- In]) / N,
      r10 => length(In) / N,
      mrr => lists:sum([1 / R || R <- In]) / N,
      ndcg => lists:sum([1 / math:log2(R + 1) || R <- In]) / N}.

row(Name, Ranks) ->
    #{r1 := R1, r10 := R10, mrr := Mrr, ndcg := Nd} = metrics(Ranks),
    io_lib:format("| ~s | ~.3f | ~.3f | ~.3f | ~.3f |~n",
                  [Name, R1, R10, Mrr, Nd]).

header(First) ->
    ["| ", First, " | recall@1 | recall@10 | MRR@10 | nDCG@10 |\n",
     "|---|---|---|---|---|\n"].

report(Docs, Apps, Queries, Outs, Rerank) ->
    [io_lib:format("# S2 results (generated)~n~n~b docs, ~b contexts, ~b queries, k=~b, "
                   "model ~s, rerank ~s~n~n",
                   [length(Docs), length(Apps), length(Queries), ?K, ?MODEL,
                    case Rerank of undefined -> "skipped"; _ -> ?RERANK_MODEL end]),
     [variant_report(O, Docs, Apps) || O <- Outs]].

variant_report(#{variant := V, queries := PQ, info := Info}, Docs, Apps) ->
    T = fun(#{query := #{target := Tg}}) -> Tg end,
    A = fun(#{query := #{app := Ap}}) -> Ap end,
    Union = fun(Method) -> [rank(T(P), ids_of(maps:get(Method, maps:get(union, P)))) || P <- PQ] end,
    Own = fun(Method) -> [rank(T(P), ids_of(maps:get(Method, maps:get(A(P), maps:get(per, P))))) || P <- PQ] end,
    Mg = fun(Key) -> [rank(T(P), maps:get(Key, maps:get(merged, P))) || P <- PQ] end,
    HasRerank = maps:is_key({rerank, hybrid}, maps:get(merged, hd(PQ))),
    Refused = lists:all(fun(P) -> maps:get(score_refused_bm25, maps:get(merged, P)) end, PQ),
    [io_lib:format("## Variant `~p`~n~n", [V]),
     io_lib:format("Fingerprints across the 33 contexts: ~p distinct; union: ~p~n"
                   "score merge refused when fingerprints are missing: ~p~n~n",
                   [length(maps:get(distinct_fingerprints, Info)),
                    maps:get(fingerprint, maps:get(union, Info), undefined), Refused]),
     "### Retrieval quality\n\n", header(<<"method">>),
     row("union vector (HNSW)", Union(vector)),
     row("union exact vector (brute force)", Union(exact)),
     row("union bm25", Union(bm25)),
     row("union hybrid (RRF)", Union(hybrid)),
     row("grouped vector (target's group)", Own(vector)),
     row("grouped bm25 (target's group)", Own(bm25)),
     row("grouped hybrid (target's group)", Own(hybrid)),
     row("score vector (fingerprint checked)", Mg({score, vector})),
     row("interleave vector (alphabetical)", Mg({interleave, vector})),
     row("interleave bm25 (alphabetical)", Mg({interleave, bm25})),
     row("interleave hybrid (alphabetical)", Mg({interleave, hybrid})),
     row("interleave hybrid (card order)", Mg({interleave_card, hybrid})),
     row("cross-context RRF (hybrid ranks)", Mg({rrf, hybrid})),
     row("raw hybrid RRF score sort", Mg({raw_score, hybrid})),
     row("raw BM25 score sort (invalid)", Mg({raw_score, bm25})),
     case HasRerank of
         true -> [row("rerank, pool 100 from 33 contexts", Mg({rerank, hybrid})),
                  row("rerank, union hybrid top 100", Mg({rerank_union, hybrid}))];
         false -> []
     end,
     "\n", lost_report(PQ, T, A),
     "\n", scope_report(PQ, T, A),
     "\n", dist_report(PQ, Docs, Apps)].

ids_of(Pairs) -> [Id || {Id, _} <- Pairs].

%% Queries whose target is rank 1 in its own context but not after merge.
lost_report(PQ, T, A) ->
    Own1 = [P || P <- PQ,
                 rank(T(P), ids_of(maps:get(vector, maps:get(A(P), maps:get(per, P))))) =:= 1],
    Lost = fun(Key) -> length([P || P <- Own1, rank(T(P), maps:get(Key, maps:get(merged, P))) =/= 1]) end,
    OwnB = [P || P <- PQ,
                 rank(T(P), ids_of(maps:get(bm25, maps:get(A(P), maps:get(per, P))))) =:= 1],
    LostB = length([P || P <- OwnB, rank(T(P), maps:get({raw_score, bm25}, maps:get(merged, P))) =/= 1]),
    io_lib:format("Target rank 1 in its own context (vector): ~b queries; no longer rank 1 after "
                  "score merge: ~b, after RRF: ~b, after interleave: ~b.~n"
                  "Target rank 1 in its own context (bm25): ~b; no longer rank 1 after raw BM25 sort: ~b.~n",
                  [length(Own1), Lost({score, vector}), Lost({rrf, hybrid}),
                   Lost({interleave, vector}), length(OwnB), LostB]).

scope_report(PQ, T, A) ->
    ["### Scope selection by card search\n\n",
     "| card search | contexts queried | true context selected | score vector recall@10 | score vector MRR@10 | grouped hybrid recall@10 | interleave hybrid recall@10 |\n",
     "|---|---|---|---|---|---|---|\n",
     [begin
          Ss = [maps:get({scoped, SM, N}, maps:get(merged, P)) || P <- PQ],
          Hit = [lists:member(A(P), maps:get(selected, S)) || {P, S} <- lists:zip(PQ, Ss)],
          ScoreR = [rank(T(P), maps:get(score, S)) || {P, S} <- lists:zip(PQ, Ss)],
          InterR = [rank(T(P), maps:get(interleave, S)) || {P, S} <- lists:zip(PQ, Ss)],
          GroupR = [case lists:keyfind(A(P), 1, maps:get(grouped, S)) of
                        false -> none;
                        {_, Ids} -> rank(T(P), Ids)
                    end || {P, S} <- lists:zip(PQ, Ss)],
          #{r10 := SR10, mrr := SMrr} = metrics(ScoreR),
          #{r10 := GR10} = metrics(GroupR),
          #{r10 := IR10} = metrics(InterR),
          io_lib:format("| ~s | ~b | ~.3f | ~.3f | ~.3f | ~.3f | ~.3f |~n",
                        [SM, N, length([1 || true <- Hit]) / length(PQ),
                         SR10, SMrr, GR10, IR10])
      end || SM <- [card_vector, card_bm25, card_hybrid], N <- ?SCOPES]].

%% Top-1 score per context and method: BM25 depends on the corpus.
dist_report(PQ, Docs, Apps) ->
    Size = fun(App) -> length([1 || #{app := X} <- Docs, X =:= App]) end,
    Top1 = fun(App, Method) ->
        [S || P <- PQ, [{_, S} | _] <- [maps:get(Method, maps:get(App, maps:get(per, P)))]]
    end,
    Stat = fun([]) -> {0.0, 0.0, 0.0};
              (L) -> S = lists:sort(L),
                     {lists:sum(S) / length(S), lists:nth((length(S) + 1) div 2, S), lists:last(S)}
           end,
    Rows = [{App, Size(App), Stat(Top1(App, bm25)), Stat(Top1(App, vector)), Stat(Top1(App, hybrid))}
            || App <- Apps],
    SizesB = [{float(Sz), Mean} || {_, Sz, {Mean, _, _}, _, _} <- Rows],
    SizesV = [{float(Sz), Mean} || {_, Sz, _, {Mean, _, _}, _} <- Rows],
    ["### Top-1 score per context over all queries (mean / median / max)\n\n",
     io_lib:format("Spearman(context size, mean top-1 BM25) = ~.3f; "
                   "Spearman(context size, mean top-1 vector) = ~.3f~n~n",
                   [spearman(SizesB), spearman(SizesV)]),
     "| context | docs | bm25 top-1 | vector top-1 | hybrid top-1 |\n|---|---|---|---|---|\n",
     [io_lib:format("| ~s | ~b | ~.2f / ~.2f / ~.2f | ~.3f / ~.3f / ~.3f | ~.4f / ~.4f / ~.4f |~n",
                    [App, Sz, B1, B2, B3, V1, V2, V3, H1, H2, H3])
      || {App, Sz, {B1, B2, B3}, {V1, V2, V3}, {H1, H2, H3}} <- Rows]].

spearman(Pairs) ->
    {Xs, Ys} = lists:unzip(Pairs),
    pearson(ranks(Xs), ranks(Ys)).

ranks(L) ->
    Sorted = lists:sort(L),
    [avg_rank(X, Sorted) || X <- L].

avg_rank(X, Sorted) ->
    Pos = [I || {I, Y} <- lists:zip(lists:seq(1, length(Sorted)), Sorted), Y == X],
    lists:sum(Pos) / length(Pos).

pearson(Xs, Ys) ->
    N = length(Xs),
    Mx = lists:sum(Xs) / N, My = lists:sum(Ys) / N,
    Cov = lists:sum([(X - Mx) * (Y - My) || {X, Y} <- lists:zip(Xs, Ys)]),
    Sx = math:sqrt(lists:sum([(X - Mx) * (X - Mx) || X <- Xs])),
    Sy = math:sqrt(lists:sum([(Y - My) * (Y - My) || Y <- Ys])),
    case Sx * Sy of
        +0.0 -> 0.0;
        D -> Cov / D
    end.
