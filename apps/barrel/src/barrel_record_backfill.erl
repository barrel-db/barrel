%%%-------------------------------------------------------------------
%%% @doc Vector backfill for a freshly-branched record database.
%%%
%%% A branch forks the docdb instantly but starts with an EMPTY vector
%%% store. This walks every current document once (chunked changes
%%% feed) and rebuilds the index: documents carrying an `_embedding'
%%% (client or computed) index directly with zero embedder calls
%%% (stored computed vectors are trusted: a stale one implies a
%%% pending outbox entry, which the fork copied and the branch's own
%%% indexer heals); documents without one whose policy matches embed
%%% their policy text (batch, per-item fallback) and write the vector
%%% back; everything else is skipped. Documents rewound by PITR lost
%%% their embedding column (archived bodies do not carry it), so the
%%% embed leg re-derives their vectors from the restored text.
%%%
%%% Runs synchronously in barrel:branch/3; a vector-store write error
%%% aborts (the caller tears the branch down), per-document embed
%%% failures only count in the report.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_record_backfill).

-export([run/1]).

-define(DEFAULT_BATCH, 500).

-type report() :: #{indexed := non_neg_integer(),
                    embedded := non_neg_integer(),
                    skipped := non_neg_integer(),
                    failed := non_neg_integer()}.

-spec run(#{db := binary(), vstore := atom(),
            policy := barrel_embedding_policy:policy(),
            embed := term(), dimensions := pos_integer(),
            batch_size => pos_integer()}) ->
    {ok, report()} | {error, term()}.
run(#{db := DbBin, vstore := VStore, policy := Policy,
      embed := Embed, dimensions := Dim} = Opts) ->
    Batch = maps:get(batch_size, Opts, ?DEFAULT_BATCH),
    Zero = #{indexed => 0, embedded => 0, skipped => 0, failed => 0},
    try
        {ok, loop(DbBin, VStore, Policy, Embed, Dim, Batch, first,
                  Zero)}
    catch
        throw:{backfill_error, Reason} ->
            {error, Reason}
    end.

loop(DbBin, VStore, Policy, Embed, Dim, Batch, Since, Stats0) ->
    case barrel_docdb:get_changes(DbBin, Since, #{limit => Batch}) of
        {ok, [], _LastSeq} ->
            Stats0;
        {ok, Changes, LastSeq} ->
            Stats = process(DbBin, VStore, Policy, Embed, Dim, Changes,
                            Stats0),
            case length(Changes) < Batch of
                true ->
                    Stats;
                false ->
                    loop(DbBin, VStore, Policy, Embed, Dim, Batch,
                         LastSeq, Stats)
            end;
        {error, Reason} ->
            throw({backfill_error, Reason})
    end.

process(DbBin, VStore, Policy, Embed, Dim, Changes, Stats0) ->
    LiveIds = [maps:get(id, C) || C <- Changes,
               not maps:get(deleted, C, false)],
    Tombstones = length(Changes) - length(LiveIds),
    Docs = case LiveIds of
        [] -> [];
        _ -> barrel_docdb:get_docs(DbBin, LiveIds,
                                   #{include_embedding => true})
    end,
    {Given, ToEmbed, Skipped, Failed} = lists:foldl(
        fun({Id, DocRes}, {G, E, S, F}) ->
            case DocRes of
                {ok, Doc} ->
                    case plan(Policy, Dim, Doc) of
                        {given, Text, Vector} ->
                            {[{Id, Text, Vector} | G], E, S, F};
                        {embed, Text, Rev} ->
                            {G, [{Id, Text, Rev} | E], S, F};
                        skip ->
                            {G, E, S + 1, F};
                        bad_dimension ->
                            {G, E, S, F + 1}
                    end;
                {error, _} ->
                    %% deleted or unreadable between feed and read
                    {G, E, S + 1, F}
            end
        end,
        {[], [], 0, 0},
        lists:zip(LiveIds, Docs)),
    {Embedded, WriteBacks, EmbedFailed} = embed_all(Embed,
                                                    lists:reverse(ToEmbed)),
    Entries = lists:reverse(Given) ++ Embedded,
    ok = index_all(VStore, Entries),
    ok = write_back(DbBin, WriteBacks),
    Stats0#{
        indexed := maps:get(indexed, Stats0) + length(Entries),
        embedded := maps:get(embedded, Stats0) + length(Embedded),
        skipped := maps:get(skipped, Stats0) + Skipped + Tombstones,
        failed := maps:get(failed, Stats0) + Failed + EmbedFailed
    }.

%% Unlike the indexer, a stored COMPUTED vector is trusted here (see
%% moduledoc); only its dimension is checked.
plan(Policy, Dim, Doc) ->
    case maps:get(<<"_embedding">>, Doc, undefined) of
        #{<<"vector">> := Vector} when is_list(Vector),
                                       length(Vector) =:= Dim ->
            {given, barrel_embedding_policy:text(Policy, Doc), Vector};
        #{<<"vector">> := Vector} when is_list(Vector) ->
            bad_dimension;
        _ ->
            case barrel_embedding_policy:matches(Policy, Doc) of
                true ->
                    {embed, barrel_embedding_policy:text(Policy, Doc),
                     maps:get(<<"_rev">>, Doc, undefined)};
                false ->
                    skip
            end
    end.

embed_all(_Embed, []) ->
    {[], [], 0};
embed_all(Embed, Items) ->
    Texts = [Text || {_Id, Text, _Rev} <- Items],
    case safe(fun() -> barrel_embed:embed_batch(Texts, Embed) end) of
        {ok, Vectors} when length(Vectors) =:= length(Items) ->
            {[{Id, Text, V}
              || {{Id, Text, _Rev}, V} <- lists:zip(Items, Vectors)],
             [{Id, Rev, V}
              || {{Id, _Text, Rev}, V} <- lists:zip(Items, Vectors)],
             0};
        _BatchFailure ->
            embed_one_by_one(Embed, Items)
    end.

embed_one_by_one(Embed, Items) ->
    lists:foldr(
        fun({Id, Text, Rev}, {Entries, WriteBacks, Failed}) ->
            case safe(fun() -> barrel_embed:embed(Text, Embed) end) of
                {ok, Vector} ->
                    {[{Id, Text, Vector} | Entries],
                     [{Id, Rev, Vector} | WriteBacks],
                     Failed};
                Failure ->
                    logger:warning(
                        "barrel record backfill: embed failed for ~s: "
                        "~p", [Id, Failure]),
                    {Entries, WriteBacks, Failed + 1}
            end
        end,
        {[], [], 0},
        Items).

index_all(_VStore, []) ->
    ok;
index_all(VStore, Entries) ->
    case safe(fun() ->
             barrel_vectordb:add_index_only_batch(VStore, Entries)
         end) of
        {ok, _Stats} -> ok;
        Failure -> throw({backfill_error, {index_failed, Failure}})
    end.

write_back(_DbBin, []) ->
    ok;
write_back(DbBin, [{Id, Rev, Vector} | Rest]) ->
    _ = case Rev of
        undefined -> ok;
        _ -> safe(fun() ->
                 barrel_docdb:set_doc_embedding(DbBin, Id, Rev, Vector)
             end)
    end,
    write_back(DbBin, Rest).

safe(Fun) ->
    try Fun()
    catch Class:Reason -> {error, {Class, Reason}}
    end.
