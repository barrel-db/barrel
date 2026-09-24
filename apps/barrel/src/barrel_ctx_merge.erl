%% @doc Merge retrieval rows returned by several contexts (pure).
%% Only `score' and `rerank' claim a cross-context relevance order.
-module(barrel_ctx_merge).

-export([grouped/1, interleave/2, score/3, rrf/3,
         rerank_pool/2, rerank/3]).

-type row() :: map().
-type member() :: #{ctx := binary(), rows := [row()],
                    fingerprint => binary() | undefined,
                    distance => atom()}.
-type meta() :: #{merge := atom(), relevance := boolean()}.
-export_type([member/0, meta/0]).

-define(SCORE, <<"_score">>).
-define(CTX, <<"_ctx">>).
-define(RANK, <<"_rank">>).
-define(RRF_K, 60).

%% @doc Rows per context, in each context's own order.
-spec grouped([member()]) -> {ok, [#{ctx := binary(), rows := [row()]}], meta()}.
grouped(Members) ->
    Groups = [#{ctx => Ctx, rows => tag(Ctx, Rows)}
              || #{ctx := Ctx, rows := Rows} <- Members],
    {ok, Groups, #{merge => grouped, relevance => false}}.

%% @doc Round-robin by member rank, in member order (presentation only).
-spec interleave([member()], pos_integer()) -> {ok, [row()], meta()}.
interleave(Members, Limit) ->
    Lists = [tag(Ctx, Rows) || #{ctx := Ctx, rows := Rows} <- Members],
    {ok, round_robin(Lists, Limit, []),
     #{merge => interleave, relevance => false}}.

%% @doc Global order by `_score' (vector retrieval only). Refused
%% unless every member reports the same fingerprint and a cosine metric.
-spec score([member()], pos_integer(), map()) ->
    {ok, [row()], meta()} | {error, term()}.
score(Members, Limit, _Opts) ->
    case comparable(Members) of
        ok ->
            Rows = [R || #{ctx := Ctx, rows := Rs} <- Members,
                         R <- tag(Ctx, Rs)],
            {ok, lists:sublist(sort_desc(?SCORE, Rows), Limit),
             #{merge => score, relevance => true}};
        {error, _} = Err ->
            Err
    end.

%% @doc Reciprocal rank fusion over member ranks. With disjoint corpora
%% every member's rank 1 gets the same weight: not a relevance order.
-spec rrf([member()], pos_integer(), map()) -> {ok, [row()], meta()}.
rrf(Members, Limit, Opts) ->
    K = maps:get(rrf_k, Opts, ?RRF_K),
    Rows = [R#{<<"_rrf">> => 1 / (K + maps:get(?RANK, R))}
            || #{ctx := Ctx, rows := Rs} <- Members, R <- tag(Ctx, Rs)],
    {ok, lists:sublist(sort_desc(<<"_rrf">>, Rows), Limit),
     #{merge => rrf, relevance => false}}.

%% @doc Candidates for a cross-encoder: round-robin by rank, capped, so
%% every member contributes its best rows first.
-spec rerank_pool([member()], pos_integer()) -> [row()].
rerank_pool(Members, Cap) ->
    {ok, Rows, _} = interleave(Members, Cap),
    Rows.

%% @doc Order a pool by cross-encoder scores `[{Index0, Score}]'
%% (0-based pool positions, as barrel_rerank returns them).
-spec rerank([row()], [{non_neg_integer(), number()}], pos_integer()) ->
    {ok, [row()], meta()} | {error, term()}.
rerank(Pool, Scores, Limit) ->
    Indexed = maps:from_list(lists:zip(lists:seq(0, length(Pool) - 1), Pool)),
    case [I || {I, _} <- Scores, not maps:is_key(I, Indexed)] of
        [] ->
            Rows = [(maps:get(I, Indexed))#{<<"_rerank">> => S}
                    || {I, S} <- Scores],
            {ok, lists:sublist(sort_desc(<<"_rerank">>, Rows), Limit),
             #{merge => rerank, relevance => true}};
        [Bad | _] ->
            {error, {rerank_index_out_of_pool, Bad}}
    end.

%%====================================================================
%% Internal
%%====================================================================

tag(Ctx, Rows) ->
    [R#{?CTX => Ctx, ?RANK => N}
     || {N, R} <- lists:zip(lists:seq(1, length(Rows)), Rows)].

round_robin(_Lists, 0, Acc) ->
    lists:reverse(Acc);
round_robin(Lists, Left, Acc) ->
    case [L || L <- Lists, L =/= []] of
        [] ->
            lists:reverse(Acc);
        NonEmpty ->
            Heads = [H || [H | _] <- NonEmpty],
            Tails = [T || [_ | T] <- NonEmpty],
            Take = lists:sublist(Heads, Left),
            round_robin(Tails, Left - length(Take),
                        lists:reverse(Take, Acc))
    end.

%% Descending by Key; ties by context, then id, for a deterministic order.
sort_desc(Key, Rows) ->
    Keyed = [{-maps:get(Key, R), maps:get(?CTX, R),
              maps:get(<<"id">>, R, undefined), R} || R <- Rows],
    [R || {_, _, _, R} <- lists:sort(fun le/2, Keyed)].

le({S1, C1, I1, _}, {S2, C2, I2, _}) ->
    {S1, C1, I1} =< {S2, C2, I2}.

comparable([]) ->
    ok;
comparable([#{fingerprint := F, distance := cosine} | Rest])
  when is_binary(F) ->
    same_fingerprint(F, Rest);
comparable([#{ctx := Ctx} | _]) ->
    {error, {not_score_comparable, Ctx}}.

same_fingerprint(_F, []) ->
    ok;
same_fingerprint(F, [#{fingerprint := F, distance := cosine} | Rest]) ->
    same_fingerprint(F, Rest);
same_fingerprint(_F, [#{ctx := Ctx} | _]) ->
    {error, {fingerprint_mismatch, Ctx}}.
