%%%-------------------------------------------------------------------
%%% @doc Segment compaction: merge several immutable segments into one.
%%%
%%% Collapses each document key to its newest occurrence by change HLC
%%% (the recency sequence number carried per ordinal), dropping superseded
%%% versions. A key whose newest occurrence is a tombstone is either
%%% retained as a tombstone (so an older, un-merged segment does not
%%% resurrect it) or dropped entirely when `DropTombstones' says no older
%%% segment can hold it (a full compaction).
%%%
%%% Recency is by HLC, not by file/generation order: a merge gives its
%%% output a fresh generation while its content may be older than a
%%% segment that was not part of the merge, so generation order cannot be
%%% trusted. Correctness never depends on getting eviction exactly right,
%%% the query confirm pass drops any resurfaced stale or deleted candidate;
%%% this only governs how compactly things are stored.
%%%
%%% Runs in a worker process; the caller renames the temp segment into
%%% place and swaps the manifest.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_merge).

-export([merge/2]).

%% @doc Merge `InputPaths' into one temp segment. Returns its path, the
%% document count, and the max input watermark.
-spec merge([file:name_all()], boolean()) ->
    {ok, binary(), non_neg_integer(), binary()} | {error, term()}.
merge([], _DropTombstones) ->
    {error, no_inputs};
merge(InputPaths, DropTombstones) ->
    case open_all(InputPaths, []) of
        {ok, Handles} ->
            try
                do_merge(Handles, InputPaths, DropTombstones)
            after
                [barrel_ngram_segment:close(H) || H <- Handles]
            end;
        {error, _} = Err ->
            Err
    end.

%%====================================================================
%% Internal
%%====================================================================

open_all([], Acc) ->
    {ok, lists:reverse(Acc)};
open_all([P | Ps], Acc) ->
    case barrel_ngram_segment:open(P) of
        {ok, H} ->
            open_all(Ps, [H | Acc]);
        {error, Reason} ->
            [barrel_ngram_segment:close(H) || H <- Acc],
            {error, {open_failed, P, Reason}}
    end.

do_merge(Handles, InputPaths, DropTombstones) ->
    KeyState = collect(Handles),
    {Entries, Postings, DocCount} = build_output(KeyState, DropTombstones),
    Wm = max_watermark(Handles),
    %% preserve the inputs' codec on the merged output
    Codec = barrel_ngram_segment:codec(hd(Handles)),
    Dir = filename:dirname(hd(InputPaths)),
    Name = "segment-merge-" ++ integer_to_list(erlang:unique_integer([positive]))
           ++ ".ngseg",
    Temp = filename:join(Dir, Name),
    Spec = #{doc_count => DocCount, watermark => Wm,
             postings => Postings, entries => Entries, codec => Codec},
    case barrel_ngram_segment:write(Temp, Spec) of
        ok -> {ok, iolist_to_binary(Temp), DocCount, Wm};
        {error, _} = Err -> Err
    end.

%% @private Fold inputs into Key -> {MaxHlc, Deleted, Grams}, keeping the
%% max-HLC occurrence of each key.
collect(Handles) ->
    lists:foldl(
        fun(H, Acc) ->
            OrdGrams = invert(barrel_ngram_segment:all_postings(H)),
            lists:foldl(
                fun({Ord, Key, Hlc, Deleted}, A) ->
                    case maps:find(Key, A) of
                        {ok, {PrevHlc, _, _}} when PrevHlc >= Hlc ->
                            A;
                        _ ->
                            Grams = maps:get(Ord, OrdGrams, []),
                            A#{Key => {Hlc, Deleted, Grams}}
                    end
                end, Acc, barrel_ngram_segment:entries(H))
        end, #{}, Handles).

%% @private gram -> [ordinal] into ordinal -> [gram].
invert(GramOrds) ->
    lists:foldl(
        fun({Gram, Ords}, Acc) ->
            lists:foldl(
                fun(O, A) -> maps:update_with(O, fun(L) -> [Gram | L] end, [Gram], A) end,
                Acc, Ords)
        end, #{}, GramOrds).

%% @private Assign dense ordinals to the surviving keys and build the
%% output entries + postings.
build_output(KeyState, DropTombstones) ->
    Kept = maps:fold(
        fun(Key, {Hlc, Deleted, Grams}, Acc) ->
            case Deleted andalso DropTombstones of
                true -> Acc;
                false -> [{Key, Hlc, Deleted, Grams} | Acc]
            end
        end, [], KeyState),
    Sorted = lists:sort(fun({A, _, _, _}, {B, _, _, _}) -> A =< B end, Kept),
    {EntriesRev, GramMap, _Ord} =
        lists:foldl(
            fun({Key, Hlc, Deleted, Grams}, {Es, GM, Ord}) ->
                E = #{key => Key, hlc => Hlc, deleted => Deleted},
                GM1 = case Deleted of
                    true -> GM;   %% tombstones carry no grams
                    false ->
                        lists:foldl(
                            fun(G, M) ->
                                maps:update_with(G, fun(L) -> [Ord | L] end, [Ord], M)
                            end, GM, Grams)
                end,
                {[E | Es], GM1, Ord + 1}
            end, {[], #{}, 0}, Sorted),
    Postings = [{G, lists:usort(Os)} || {G, Os} <- maps:to_list(GramMap)],
    {lists:reverse(EntriesRev), Postings, length(Sorted)}.

max_watermark(Handles) ->
    lists:foldl(
        fun(H, Max) ->
            Wm = barrel_ngram_segment:watermark(H),
            case Wm > Max of true -> Wm; false -> Max end
        end, <<0:96>>, Handles).
