%% @doc BQL execution at the facade: collection sources delegate to the
%% docdb executor; table-function sources (vector_top_k, bm25_top_k,
%% hybrid_top_k) run here, where the vector store and the embedder
%% live.
%%
%% Table-function pipeline: search top KFetch hits (single over-fetch
%% when a WHERE must filter them, no re-query loop: the contract is UP
%% TO k rows) -> batch-join hits to their documents (hits without a
%% document are dropped) -> residual WHERE via barrel_query:match/2 ->
%% truncate to k -> unnest/order/offset/limit/project through the
%% shared barrel_bql_exec post stages.
-module(barrel_bql_query).

-export([
    run/3,
    fold/5,
    explain/2,
    subscribe_plan/3
]).

-define(MAX_FETCH_K, 1000).

%%====================================================================
%% API
%%====================================================================

-spec run(barrel:db(), barrel_bql_lower:plan(), map()) ->
    {ok, [map()], map()} | {error, term()}.
run(_Db, #{subscribe := true}, _Opts) ->
    {error, {unsupported, subscribe}};
run(#{docdb := DbBin}, #{source := {collection, _}} = Plan, Opts) ->
    barrel_bql_exec:run(DbBin, Plan, Opts);
run(Db, #{source := {table_fn, Fn, Args}} = Plan, Opts) ->
    run_table_fn(Db, Fn, Args, Plan, Opts).

-spec fold(barrel:db(), barrel_bql_lower:plan(), map(),
           fun((map(), term()) -> {ok, term()} | {stop, term()}),
           term()) ->
    {ok, term(), map()} | {error, term()}.
fold(_Db, #{subscribe := true}, _Opts, _Fun, _Acc) ->
    {error, {unsupported, subscribe}};
fold(#{docdb := DbBin}, #{source := {collection, _}} = Plan, Opts, Fun,
     Acc) ->
    barrel_bql_exec:fold(DbBin, Plan, Opts, Fun, Acc);
fold(Db, Plan, Opts, Fun, Acc0) ->
    case run(Db, Plan, Opts) of
        {ok, Rows, Meta} ->
            {ok, fold_rows(Rows, Fun, Acc0), Meta};
        {error, _} = Error ->
            Error
    end.

%% @doc Start a live query for a compiled SUBSCRIBE plan. Deltas go to
%% the owner; see barrel_bql_live for the message shapes.
-spec subscribe_plan(barrel:db(), barrel_bql_lower:plan(), map()) ->
    {ok, #{ref := reference(), pid := pid()}} | {error, term()}.
subscribe_plan(_Db, #{subscribe := false}, _Opts) ->
    {error, missing_subscribe};
subscribe_plan(Db, #{subscribe := true} = Plan, Opts) ->
    Owner = maps:get(owner, Opts, self()),
    Ref = make_ref(),
    case barrel_bql_live_sup:start_child(
             #{db => Db, plan => Plan, owner => Owner, ref => Ref}) of
        {ok, Pid} -> {ok, #{ref => Ref, pid => Pid}};
        {error, _} = Error -> Error
    end.

-spec explain(barrel:db(), barrel_bql_lower:plan()) ->
    {ok, map()} | {error, term()}.
explain(Db, Plan) ->
    #{source := Source, streamable := Streamable,
      subscribe := Subscribe, warnings := Warnings,
      post := #{doc_where := DocWhere, residual := Residual}} = Plan,
    Base = #{streamable => Streamable, subscribe => Subscribe,
             warnings => Warnings},
    case Source of
        {collection, _Name} ->
            #{docdb := DbBin} = Db,
            case barrel_docdb:explain(DbBin, maps:get(spec, Plan)) of
                {ok, Engine} ->
                    {ok, Base#{source => collection, engine => Engine}};
                {error, _} = Error ->
                    Error
            end;
        {table_fn, Fn, Args} ->
            {ok, Base#{source => Fn, args => Args,
                       residual_conditions =>
                           length(DocWhere) + length(Residual)}}
    end.

%%====================================================================
%% Table functions
%%====================================================================

run_table_fn(_Db, _Fn, _Args, #{post := #{empty := true}}, _Opts) ->
    {ok, [], #{has_more => false, count => 0}};
run_table_fn(Db, Fn, Args, Plan, Opts) ->
    case maps:is_key(continuation, Opts) of
        true -> {error, {unsupported, continuation}};
        false -> run_table_fn1(Db, Fn, Args, Plan, Opts)
    end.

run_table_fn1(Db, Fn, Args, Plan, Opts) ->
    #{post := Post, unnest := Unnest} = Plan,
    #{doc_where := DocWhere, residual := Residual} = Post,
    #{k := K} = Args,
    KFetch = fetch_k(K, DocWhere, Opts),
    case search_hits(Db, Fn, Args, KFetch) of
        {ok, Hits} ->
            MatchPlan = match_plan(DocWhere),
            Pairs = join_docs(Db, Hits),
            Filtered = [Pair || {Doc, _Score} = Pair <- Pairs,
                                doc_matches(MatchPlan, Doc)],
            Top = lists:sublist(Filtered, K),
            Frames = [Frame#{score => score_columns(Fn, Score)}
                      || {Doc, Score} <- Top,
                         Frame <- barrel_bql_exec:frames_for(Doc, Unnest)],
            Kept = [Frame || Frame <- Frames,
                             barrel_bql_exec:residual_match(Residual,
                                                            Frame)],
            Rows = barrel_bql_exec:finalize(Kept, Post, Unnest),
            {ok, Rows, #{has_more => false, count => length(Rows)}};
        {error, _} = Error ->
            Error
    end.

%% Single-shot over-fetch when hits must survive a WHERE filter.
fetch_k(K, [], _Opts) ->
    K;
fetch_k(K, _DocWhere, Opts) ->
    OverFetch = maps:get(overfetch, Opts, 3),
    max(K, min(K * OverFetch, ?MAX_FETCH_K)).

search_hits(Db, vector_top_k, Args, KFetch) ->
    SearchOpts0 = #{k => KFetch, include_text => false,
                    include_metadata => false},
    SearchOpts = case Args of
        #{ef_search := EfSearch} -> SearchOpts0#{ef_search => EfSearch};
        _ -> SearchOpts0
    end,
    case barrel:search(Db, maps:get(query, Args), SearchOpts) of
        {ok, Results} -> {ok, hit_pairs(Results)};
        {error, _} = Error -> Error
    end;
search_hits(Db, bm25_top_k, Args, KFetch) ->
    %% bm25 returns bare {Id, Score} tuples; the shape difference stops
    %% at this boundary
    barrel:search_bm25(Db, maps:get(query, Args), #{k => KFetch});
search_hits(Db, hybrid_top_k, Args, KFetch) ->
    SearchOpts = #{k => KFetch, fusion => rrf, include_text => false,
                   include_metadata => false},
    case barrel:search_hybrid(Db, maps:get(query, Args), SearchOpts) of
        {ok, Results} -> {ok, hit_pairs(Results)};
        {error, _} = Error -> Error
    end.

hit_pairs(Results) ->
    [{Key, Score} || #{key := Key, score := Score} <- Results].

join_docs(Db, Hits) ->
    Ids = [Id || {Id, _Score} <- Hits],
    Docs = barrel:get_docs(Db, Ids),
    lists:filtermap(
        fun({{Id, Score}, {ok, Doc}}) ->
                {true, {Doc#{<<"id">> => Id}, Score}};
           ({{_Id, _Score}, {error, _}}) ->
                false
        end,
        lists:zip(Hits, Docs)).

match_plan([]) ->
    none;
match_plan(Conds) ->
    {ok, Plan} = barrel_query:compile(#{where => Conds}),
    Plan.

doc_matches(none, _Doc) -> true;
doc_matches(Plan, Doc) -> barrel_query:match(Plan, Doc).

%% score is function-specific: 1 - distance for vectors, raw BM25, RRF
%% fused for hybrid. Only vector_top_k has a distance interpretation.
score_columns(vector_top_k, Score) ->
    #{<<"_score">> => Score, <<"_distance">> => 1.0 - Score};
score_columns(_Fn, Score) ->
    #{<<"_score">> => Score}.

fold_rows([], _Fun, Acc) ->
    Acc;
fold_rows([Row | Rest], Fun, Acc0) ->
    case Fun(Row, Acc0) of
        {ok, Acc} -> fold_rows(Rest, Fun, Acc);
        {stop, Acc} -> Acc
    end.
