%%%-------------------------------------------------------------------
%%% @doc Path index storage for automatic document indexing
%%%
%%% Provides storage operations for the path index:
%%% - Index all paths extracted from a document
%%% - Update paths when a document changes
%%% - Remove all paths when a document is deleted
%%% - Query paths by prefix
%%%
%%% Uses barrel_ars for path extraction and barrel_store_keys for encoding.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ars_index).

-export([
    index_doc/4,
    update_doc/5,
    remove_doc/3,
    fold_path/5, fold_path/6,
    fold_path_reverse/5, fold_path_reverse/6,
    fold_path_chunked/6, fold_path_chunked/7,
    fold_path_range/6, fold_path_range/7,
    fold_path_values/5, fold_path_values/6,
    fold_path_values_reverse/5, fold_path_values_reverse/6,
    fold_path_values_compare/7,
    fold_compare_docids/7,
    fold_compare_docids_with_snapshot/8,
    fold_prefix/6, fold_prefix/7,
    fold_posting/5, fold_posting/6,
    fold_prefix_posting/6,
    fold_posting_with_snapshot/6,
    fold_prefix_posting_with_snapshot/7,
    get_posting_list/3,
    get_posting_list_sorted/3,
    get_bucketed_posting_list/4,
    fold_value_index/6, fold_value_index/7
]).

%% Operations-only variants (for batching with other ops)
-export([
    index_doc_ops/3,
    update_doc_ops/4,
    remove_doc_ops/3
]).

%% Utility for reading stored paths
-export([
    get_doc_paths/3,
    get_path_cardinality/3,
    get_posting_cardinality/3
]).

%% Point lookup for condition verification
-export([
    docid_has_value/5,
    docid_get_value/4,
    docid_satisfies_compare/6,
    filter_docids_by_value/5
]).

%% Native postings API (V2 posting lists)
-export([
    get_posting_list_binary/3,
    get_postings_resource/3,
    intersect_posting_lists/1,
    posting_contains/2,
    posting_bitmap_contains/2
]).

-include("barrel_docdb.hrl").

-type store_ref() :: barrel_store_rocksdb:db_ref().
-type read_profile() :: barrel_store_rocksdb:read_profile().

%%====================================================================
%% API
%%====================================================================

%% @doc Index all paths from a document.
%% Extracts paths using barrel_ars:analyze/1 and stores them.
%% Also stores the reverse index (doc_id -> paths) for later updates.
%% Updates posting lists for O(1) equality query lookups.
-spec index_doc(store_ref(), db_name(), docid(), doc()) ->
    ok | {error, term()}.
index_doc(StoreRef, DbName, DocId, Doc) ->
    Paths = barrel_ars:analyze(Doc),
    Operations = make_index_ops(DbName, DocId, Paths),
    barrel_store_rocksdb:write_batch(StoreRef, Operations).

%% @doc Return batch operations to index all paths from a document.
%% Use this to combine with other operations in a single write_batch.
-spec index_doc_ops(db_name(), docid(), doc()) -> [{put, binary(), binary()}].
index_doc_ops(DbName, DocId, Doc) ->
    Paths = barrel_ars:analyze(Doc),
    make_index_ops(DbName, DocId, Paths).

%% @doc Update paths when a document changes.
%% Computes the diff between old and new paths and applies changes.
-spec update_doc(store_ref(), db_name(), docid(), doc(), doc()) ->
    ok | {error, term()}.
update_doc(StoreRef, DbName, DocId, OldDoc, NewDoc) ->
    OldPaths = barrel_ars:analyze(OldDoc),
    NewPaths = barrel_ars:analyze(NewDoc),
    {Added, Removed} = barrel_ars:diff(OldPaths, NewPaths),

    case {Added, Removed} of
        {[], []} ->
            ok;
        _ ->
            Operations = make_update_ops(DbName, DocId, Added, Removed, NewPaths),
            barrel_store_rocksdb:write_batch(StoreRef, Operations)
    end.

%% @doc Return batch operations to update paths when a document changes.
%% Use this to combine with other operations in a single write_batch.
%% Note: Does not include posting list updates - use update_doc for full updates.
-spec update_doc_ops(db_name(), docid(), doc(), doc()) ->
    [{put | delete, binary()} | {put, binary(), binary()}].
update_doc_ops(DbName, DocId, OldDoc, NewDoc) ->
    OldPaths = barrel_ars:analyze(OldDoc),
    NewPaths = barrel_ars:analyze(NewDoc),
    {Added, Removed} = barrel_ars:diff(OldPaths, NewPaths),

    case {Added, Removed} of
        {[], []} ->
            [];
        _ ->
            make_update_ops(DbName, DocId, Added, Removed, NewPaths)
    end.

%% @doc Remove all paths for a document.
%% Reads the reverse index to find all paths and deletes them.
-spec remove_doc(store_ref(), db_name(), docid()) ->
    ok | {error, term()}.
remove_doc(StoreRef, DbName, DocId) ->
    %% Get stored paths from reverse index
    ReverseKey = barrel_store_keys:doc_paths_key(DbName, DocId),
    case barrel_store_rocksdb:get(StoreRef, ReverseKey) of
        {ok, PathsBin} ->
            Paths = binary_to_term(PathsBin),
            Operations = remove_doc_ops(DbName, DocId, Paths),
            barrel_store_rocksdb:write_batch(StoreRef, Operations);
        not_found ->
            %% No paths indexed
            ok;
        {error, _} = Error ->
            Error
    end.

%% @doc Return batch operations to remove all paths for a document.
%% Takes the stored paths (from reverse index) as parameter.
%% Use this to combine with other operations in a single write_batch.
-spec remove_doc_ops(db_name(), docid(), [{[term()], term()}]) ->
    [{posting_remove, binary(), binary()} | {delete, binary()} | {merge, binary(), integer()}].
remove_doc_ops(DbName, DocId, Paths) ->
    %% Remove DocId from all posting lists
    PostingOps = [{posting_remove,
                   barrel_store_keys:path_posting_key(DbName, Path),
                   DocId}
                  || {Path, _} <- Paths],
    %% Remove DocId from value-first index
    ValueIndexOps = make_value_index_remove_ops(DbName, DocId, Paths),
    %% Remove DocId from bucketed posting lists
    BucketedOps = make_bucketed_posting_remove_ops(DbName, DocId, Paths),
    %% Decrement counters for each path
    DecrOps = [{merge, barrel_store_keys:path_stats_key(DbName, Path), -1}
               || {Path, _} <- Paths],
    %% Delete reverse index
    ReverseKey = barrel_store_keys:doc_paths_key(DbName, DocId),
    PostingOps ++ ValueIndexOps ++ BucketedOps ++ DecrOps ++ [{delete, ReverseKey}].

%% @private Create value-first index remove operations (individual key deletes)
make_value_index_remove_ops(_DbName, _DocId, []) ->
    [];
make_value_index_remove_ops(DbName, DocId, [{Path, _Type} | Rest]) ->
    case Path of
        [] ->
            make_value_index_remove_ops(DbName, DocId, Rest);
        [_Single] ->
            make_value_index_remove_ops(DbName, DocId, Rest);
        _ ->
            {FieldPath, Value} = split_path_value(Path),
            %% Individual key with DocId - just delete it
            Key = barrel_store_keys:value_index_key(DbName, Value, FieldPath, DocId),
            [{delete, Key} | make_value_index_remove_ops(DbName, DocId, Rest)]
    end.

%% @doc Get stored paths for a document from the reverse index.
%% Returns {ok, Paths} or not_found.
-spec get_doc_paths(store_ref(), db_name(), docid()) ->
    {ok, [{[term()], term()}]} | not_found | {error, term()}.
get_doc_paths(StoreRef, DbName, DocId) ->
    ReverseKey = barrel_store_keys:doc_paths_key(DbName, DocId),
    case barrel_store_rocksdb:get(StoreRef, ReverseKey) of
        {ok, PathsBin} ->
            {ok, binary_to_term(PathsBin)};
        not_found ->
            not_found;
        {error, _} = Error ->
            Error
    end.

%% @doc Get the cardinality (document count) for a path+value.
%% Returns 0 if the path has never been indexed.
%% Uses counter stored in path_stats_key (O(1) lookup).
-spec get_path_cardinality(store_ref(), db_name(), [term()]) ->
    {ok, non_neg_integer()} | {error, term()}.
get_path_cardinality(StoreRef, DbName, Path) ->
    Key = barrel_store_keys:path_stats_key(DbName, Path),
    case barrel_store_rocksdb:get(StoreRef, Key) of
        {ok, CountBin} ->
            Count = binary_to_integer(CountBin),
            {ok, max(0, Count)};
        not_found ->
            {ok, 0};
        {error, _} = Error ->
            Error
    end.

%% @doc Get exact cardinality from posting list using postings_count.
%% This reads the posting list binary and counts keys directly.
%% More accurate than counter (no drift), but requires reading posting list.
-spec get_posting_cardinality(store_ref(), db_name(), [term()]) ->
    {ok, non_neg_integer()} | not_found | {error, term()}.
get_posting_cardinality(StoreRef, DbName, Path) ->
    case get_posting_list_binary(StoreRef, DbName, Path) of
        {ok, Binary} ->
            {ok, Postings} = barrel_postings:open(Binary),
            {ok, barrel_postings:count(Postings)};
        not_found ->
            {ok, 0};
        {error, _} = Error ->
            Error
    end.

%% @doc Check if a docid has a specific value at a path via point lookup.
%% Uses the value-first index for O(1) verification instead of collecting
%% the entire posting list. Uses key_exists which doesn't load the value.
-spec docid_has_value(store_ref(), db_name(), [term()], term(), docid()) -> boolean().
docid_has_value(StoreRef, DbName, Path, Value, DocId) ->
    Key = barrel_store_keys:value_index_key(DbName, Value, Path, DocId),
    barrel_store_rocksdb:key_exists(StoreRef, Key).

%% @doc Get the value at a specific path for a DocId.
%% Uses the reverse index (doc_paths) which stores all paths for a document.
%% Returns {ok, Value} if found, not_found if path doesn't exist.
-spec docid_get_value(store_ref(), db_name(), docid(), [term()]) ->
    {ok, term()} | not_found | {error, term()}.
docid_get_value(StoreRef, DbName, DocId, Path) ->
    case get_doc_paths(StoreRef, DbName, DocId) of
        {ok, PathValues} ->
            %% Stored entries are {FieldPath ++ [Value], <<>>} (the analyzer
            %% appends the value as the last path element), so the value at
            %% Path is the last element of the entry whose field path == Path.
            find_path_value(Path, PathValues);
        not_found ->
            not_found;
        {error, _} = Error ->
            Error
    end.

%% @private Find the value stored at a field path in the reverse index.
%% Entries are {FieldPath ++ [Value], _}; match on the field path and return
%% the trailing value. Returns the first match (arrays use the set-based path).
find_path_value(_Path, []) ->
    not_found;
find_path_value(Path, [{[_ | _] = Full, _} | Rest]) ->
    case lists:droplast(Full) of
        Path -> {ok, lists:last(Full)};
        _ -> find_path_value(Path, Rest)
    end;
find_path_value(Path, [_ | Rest]) ->
    find_path_value(Path, Rest).

%% @doc Check if a DocId's value at a path satisfies a comparison.
%% Op is one of: '&gt;' | '&lt;' | '&gt;=' | '=&lt;' | '=/=' | '=='
%% Returns true if the condition is satisfied, false otherwise.
-spec docid_satisfies_compare(store_ref(), db_name(), docid(), [term()], atom(), term()) ->
    boolean().
docid_satisfies_compare(StoreRef, DbName, DocId, Path, Op, CompareValue) ->
    case docid_get_value(StoreRef, DbName, DocId, Path) of
        {ok, DocValue} ->
            compare_values(DocValue, Op, CompareValue);
        _ ->
            false
    end.

%% @private Compare two values with the given operator
compare_values(A, '>', B) -> A > B;
compare_values(A, '<', B) -> A < B;
compare_values(A, '>=', B) -> A >= B;
compare_values(A, '=<', B) -> A =< B;
compare_values(A, '=/=', B) -> A =/= B;
compare_values(A, '==', B) -> A == B.

%% @doc Filter a list of DocIds to only those that have a specific value at a path.
%% Uses batch multi_get for efficiency instead of individual key lookups.
%% Much faster than calling docid_has_value for each DocId individually.
-spec filter_docids_by_value(store_ref(), db_name(), [term()], term(), [docid()]) -> [docid()].
filter_docids_by_value(_StoreRef, _DbName, _Path, _Value, []) ->
    [];
filter_docids_by_value(StoreRef, DbName, Path, Value, DocIds) ->
    %% Build keys for batch lookup
    Keys = [barrel_store_keys:value_index_key(DbName, Value, Path, DocId)
            || DocId <- DocIds],
    %% Use batch key existence check
    Results = barrel_store_rocksdb:multi_key_exists(StoreRef, Keys),
    %% Filter to only DocIds that exist
    filter_by_exists(DocIds, Results, []).

%% @private Filter DocIds based on existence results
filter_by_exists([], [], Acc) ->
    lists:reverse(Acc);
filter_by_exists([DocId | DocIds], [true | Results], Acc) ->
    filter_by_exists(DocIds, Results, [DocId | Acc]);
filter_by_exists([_DocId | DocIds], [false | Results], Acc) ->
    filter_by_exists(DocIds, Results, Acc).

%% @doc Fold over path index entries matching a path prefix.
%% The callback receives {Path, DocId} for each match.
%% Uses posting lists internally - each posting list key contains multiple DocIds.
-spec fold_path(store_ref(), db_name(), [term()], fun(), term()) -> term().
fold_path(StoreRef, DbName, PathPrefix, Fun, Acc0) ->
    fold_path(StoreRef, DbName, PathPrefix, Fun, Acc0, short_range).

%% @doc Fold over path index entries with explicit read profile.
%% Uses posting lists: iterates posting keys and expands each to {Path, DocId} tuples.
-spec fold_path(store_ref(), db_name(), [term()], fun(), term(), read_profile()) -> term().
fold_path(StoreRef, DbName, PathPrefix, Fun, Acc0, _Profile) ->
    StartKey = barrel_store_keys:path_posting_prefix(DbName, PathPrefix),
    EndKey = barrel_store_keys:path_posting_end(DbName, PathPrefix),
    %% Fold over posting lists and expand each to {Path, DocId} tuples
    FoldFun = fun(Key, DocIds, Acc) ->
        {ok, Path} = decode_posting_key(Key),
        %% Call Fun for each DocId in the posting list
        NewAcc = lists:foldl(
            fun(DocId, InnerAcc) ->
                case Fun({Path, DocId}, InnerAcc) of
                    {ok, Updated} -> Updated;
                    {stop, StopAcc} -> throw({stop, StopAcc})
                end
            end,
            Acc,
            DocIds
        ),
        {ok, NewAcc}
    end,
    barrel_lib:safe_fold(fun() ->
        barrel_store_rocksdb:fold_range_posting(StoreRef, StartKey, EndKey, FoldFun, Acc0)
    end).

%% @doc Fold over path index entries in reverse order.
%% Iterates from last to first; useful for building sorted lists with prepend.
%% Uses posting lists internally.
-spec fold_path_reverse(store_ref(), db_name(), [term()], fun(), term()) -> term().
fold_path_reverse(StoreRef, DbName, PathPrefix, Fun, Acc0) ->
    fold_path_reverse(StoreRef, DbName, PathPrefix, Fun, Acc0, short_range).

%% @doc Fold over path index entries in reverse order with explicit read profile.
%% Uses posting lists: iterates in reverse and expands to {Path, DocId} tuples.
-spec fold_path_reverse(store_ref(), db_name(), [term()], fun(), term(), read_profile()) -> term().
fold_path_reverse(StoreRef, DbName, PathPrefix, Fun, Acc0, _Profile) ->
    StartKey = barrel_store_keys:path_posting_prefix(DbName, PathPrefix),
    EndKey = barrel_store_keys:path_posting_end(DbName, PathPrefix),
    FoldFun = fun(Key, DocIds, Acc) ->
        {ok, Path} = decode_posting_key(Key),
        %% Reverse the DocIds to maintain reverse order within each posting list
        NewAcc = lists:foldl(
            fun(DocId, InnerAcc) ->
                case Fun({Path, DocId}, InnerAcc) of
                    {ok, Updated} -> Updated;
                    {stop, StopAcc} -> throw({stop, StopAcc})
                end
            end,
            Acc,
            lists:reverse(DocIds)
        ),
        {ok, NewAcc}
    end,
    barrel_lib:safe_fold(fun() ->
        barrel_store_rocksdb:fold_range_posting_reverse(StoreRef, StartKey, EndKey, FoldFun, Acc0)
    end).

%% @doc Fold over path index entries in chunks for efficient batch processing.
%% Collects DocIds into chunks of ChunkSize, calling Fun with each chunk.
%% The callback can return:
%%   - {ok, Acc} to continue with next chunk
%%   - {ok, Acc, NewChunkSize} to adjust chunk size adaptively
%%   - {stop, Acc} to terminate early
%% Returns the final accumulated value after processing all chunks.
%% Uses posting lists internally.
-spec fold_path_chunked(store_ref(), db_name(), [term()], pos_integer(),
                        fun(([docid()], Acc) -> {ok, Acc} | {ok, Acc, pos_integer()} | {stop, Acc}), Acc) -> Acc
    when Acc :: term().
fold_path_chunked(StoreRef, DbName, PathPrefix, InitialChunkSize, Fun, Acc0) ->
    fold_path_chunked(StoreRef, DbName, PathPrefix, InitialChunkSize, Fun, Acc0, short_range).

%% @doc Fold over path index entries in chunks with explicit read profile.
%% Uses posting lists: iterates posting keys and chunks the DocIds.
-spec fold_path_chunked(store_ref(), db_name(), [term()], pos_integer(),
                        fun(([docid()], Acc) -> {ok, Acc} | {ok, Acc, pos_integer()} | {stop, Acc}), Acc, read_profile()) -> Acc
    when Acc :: term().
fold_path_chunked(StoreRef, DbName, PathPrefix, InitialChunkSize, Fun, Acc0, _Profile) ->
    StartKey = barrel_store_keys:path_posting_prefix(DbName, PathPrefix),
    EndKey = barrel_store_keys:path_posting_end(DbName, PathPrefix),

    {FinalChunk, FinalAcc, _} = barrel_store_rocksdb:fold_range_posting_reverse(
        StoreRef, StartKey, EndKey,
        fun(_Key, DocIds, {CurrentChunk, ChunkAcc, ChunkSize}) ->
            %% Add all DocIds from this posting list to the current chunk
            process_docids_chunked(lists:reverse(DocIds), CurrentChunk, ChunkAcc, ChunkSize, Fun)
        end,
        {[], Acc0, InitialChunkSize}
    ),
    %% Process remaining items in final partial chunk
    case FinalChunk of
        [] -> FinalAcc;
        _ ->
            case Fun(lists:reverse(FinalChunk), FinalAcc) of
                {ok, Result} -> Result;
                {ok, Result, _} -> Result;
                {stop, Result} -> Result
            end
    end.

%% @private Process DocIds in chunks, handling chunk boundaries
process_docids_chunked([], Chunk, Acc, ChunkSize, _Fun) ->
    {Chunk, Acc, ChunkSize};
process_docids_chunked([DocId | Rest], Chunk, Acc, ChunkSize, Fun) ->
    NewChunk = [DocId | Chunk],
    case length(NewChunk) >= ChunkSize of
        true ->
            case Fun(lists:reverse(NewChunk), Acc) of
                {ok, NewAcc} ->
                    process_docids_chunked(Rest, [], NewAcc, ChunkSize, Fun);
                {ok, NewAcc, NewChunkSize} ->
                    process_docids_chunked(Rest, [], NewAcc, NewChunkSize, Fun);
                {stop, StopAcc} ->
                    throw({stop, {[], StopAcc, ChunkSize}})
            end;
        false ->
            process_docids_chunked(Rest, NewChunk, Acc, ChunkSize, Fun)
    end.

%% @doc Fold over path index entries in a key range.
%% Lower-level function for range queries.
%% Note: StartKey and EndKey should be posting key format.
-spec fold_path_range(store_ref(), db_name(), binary(), binary(), fun(), term()) ->
    term().
fold_path_range(StoreRef, DbName, StartKey, EndKey, Fun, Acc0) ->
    fold_path_range(StoreRef, DbName, StartKey, EndKey, Fun, Acc0, short_range).

%% @doc Fold over path index entries in a key range with explicit read profile.
%% Uses posting lists: iterates posting keys and expands to {Path, DocId} tuples.
-spec fold_path_range(store_ref(), db_name(), binary(), binary(), fun(), term(), read_profile()) ->
    term().
fold_path_range(StoreRef, _DbName, StartKey, EndKey, Fun, Acc0, _Profile) ->
    FoldFun = fun(Key, DocIds, Acc) ->
        {ok, Path} = decode_posting_key(Key),
        NewAcc = lists:foldl(
            fun(DocId, InnerAcc) ->
                case Fun({Path, DocId}, InnerAcc) of
                    {ok, Updated} -> Updated;
                    {stop, StopAcc} -> throw({stop, StopAcc})
                end
            end,
            Acc,
            DocIds
        ),
        {ok, NewAcc}
    end,
    barrel_lib:safe_fold(fun() ->
        barrel_store_rocksdb:fold_range_posting(StoreRef, StartKey, EndKey, FoldFun, Acc0)
    end).

%% @doc Fold over all values for a path in ascending order.
%% Useful for ORDER BY path ASC with early termination.
%% The callback receives {FullPath, DocId} where FullPath includes the value.
%% Uses posting lists internally.
-spec fold_path_values(store_ref(), db_name(), [term()], fun(), term()) -> term().
fold_path_values(StoreRef, DbName, PathPrefix, Fun, Acc0) ->
    fold_path_values(StoreRef, DbName, PathPrefix, Fun, Acc0, short_range).

%% @doc Fold over all values for a path in ascending order with explicit read profile.
%% Uses posting lists: iterates posting keys and expands to {Path, DocId} tuples.
-spec fold_path_values(store_ref(), db_name(), [term()], fun(), term(), read_profile()) -> term().
fold_path_values(StoreRef, DbName, PathPrefix, Fun, Acc0, _Profile) ->
    StartKey = barrel_store_keys:path_posting_prefix(DbName, PathPrefix),
    EndKey = barrel_store_keys:path_posting_end(DbName, PathPrefix),
    FoldFun = fun(Key, DocIds, Acc) ->
        {ok, Path} = decode_posting_key(Key),
        NewAcc = lists:foldl(
            fun(DocId, InnerAcc) ->
                case Fun({Path, DocId}, InnerAcc) of
                    {ok, Updated} -> Updated;
                    {stop, StopAcc} -> throw({stop, StopAcc})
                end
            end,
            Acc,
            DocIds
        ),
        {ok, NewAcc}
    end,
    barrel_lib:safe_fold(fun() ->
        barrel_store_rocksdb:fold_range_posting(StoreRef, StartKey, EndKey, FoldFun, Acc0)
    end).

%% @doc Fold over all values for a path in descending order.
%% Useful for ORDER BY path DESC with early termination.
%% Iterates from highest value to lowest.
%% Uses posting lists internally.
-spec fold_path_values_reverse(store_ref(), db_name(), [term()], fun(), term()) -> term().
fold_path_values_reverse(StoreRef, DbName, PathPrefix, Fun, Acc0) ->
    fold_path_values_reverse(StoreRef, DbName, PathPrefix, Fun, Acc0, short_range).

%% @doc Fold over all values for a path in descending order with explicit read profile.
%% Uses posting lists: iterates in reverse and expands to {Path, DocId} tuples.
-spec fold_path_values_reverse(store_ref(), db_name(), [term()], fun(), term(), read_profile()) -> term().
fold_path_values_reverse(StoreRef, DbName, PathPrefix, Fun, Acc0, _Profile) ->
    StartKey = barrel_store_keys:path_posting_prefix(DbName, PathPrefix),
    EndKey = barrel_store_keys:path_posting_end(DbName, PathPrefix),
    FoldFun = fun(Key, DocIds, Acc) ->
        {ok, Path} = decode_posting_key(Key),
        NewAcc = lists:foldl(
            fun(DocId, InnerAcc) ->
                case Fun({Path, DocId}, InnerAcc) of
                    {ok, Updated} -> Updated;
                    {stop, StopAcc} -> throw({stop, StopAcc})
                end
            end,
            Acc,
            lists:reverse(DocIds)
        ),
        {ok, NewAcc}
    end,
    barrel_lib:safe_fold(fun() ->
        barrel_store_rocksdb:fold_range_posting_reverse(StoreRef, StartKey, EndKey, FoldFun, Acc0)
    end).

%% @doc Fold over path values matching a comparison operator.
%% Supports: '&gt;' | '&lt;' | '&gt;=' | '=&lt;' for range queries.
%% Uses posting lists with proper range bounds.
%% Example: `fold_path_values_compare(S, Db, [&lt;&lt;"age"&gt;&gt;], '&gt;', 50, Fun, Acc)'
%%   finds all docs where age &gt; 50
-spec fold_path_values_compare(store_ref(), db_name(), [term()],
                                '>' | '<' | '>=' | '=<', term(), fun(), term()) -> term().
fold_path_values_compare(StoreRef, DbName, Path, Op, Value, Fun, Acc0) ->
    %% Compute start/end keys based on operator
    {StartKey, EndKey} = compare_range_keys(DbName, Path, Op, Value),
    FoldFun = fun(Key, DocIds, Acc) ->
        {ok, FullPath} = decode_posting_key(Key),
        NewAcc = lists:foldl(
            fun(DocId, InnerAcc) ->
                case Fun({FullPath, DocId}, InnerAcc) of
                    {ok, Updated} -> Updated;
                    {stop, StopAcc} -> throw({stop, StopAcc})
                end
            end,
            Acc,
            DocIds
        ),
        {ok, NewAcc}
    end,
    barrel_lib:safe_fold(fun() ->
        %% Use compare-optimized fold with bloom filter
        barrel_store_rocksdb:fold_range_posting_compare(StoreRef, StartKey, EndKey, FoldFun, Acc0)
    end).

%% @doc Fold over DocIds matching a comparison operator (no path decoding).
%% Optimized for pure range queries where only DocIds are needed.
%% Callback receives (DocId, Acc) instead of ({Path, DocId}, Acc).
-spec fold_compare_docids(store_ref(), db_name(), [term()],
                          '>' | '<' | '>=' | '=<', term(), fun(), term()) -> term().
fold_compare_docids(StoreRef, DbName, Path, Op, Value, Fun, Acc0) ->
    {StartKey, EndKey} = compare_range_keys(DbName, Path, Op, Value),
    FoldFun = fun(_Key, DocIds, Acc) ->
        NewAcc = lists:foldl(
            fun(DocId, InnerAcc) ->
                case Fun(DocId, InnerAcc) of
                    {ok, Updated} -> Updated;
                    {stop, StopAcc} -> throw({stop, StopAcc})
                end
            end,
            Acc,
            DocIds
        ),
        {ok, NewAcc}
    end,
    barrel_lib:safe_fold(fun() ->
        barrel_store_rocksdb:fold_range_posting_compare(StoreRef, StartKey, EndKey, FoldFun, Acc0)
    end).

%% @doc Fold over DocIds matching a comparison operator with snapshot.
%% Same as fold_compare_docids but uses a snapshot for consistent reads.
-spec fold_compare_docids_with_snapshot(store_ref(), db_name(), [term()],
                          '>' | '<' | '>=' | '=<', term(), fun(), term(),
                          barrel_store_rocksdb:snapshot()) -> term().
fold_compare_docids_with_snapshot(StoreRef, DbName, Path, Op, Value, Fun, Acc0, Snapshot) ->
    {StartKey, EndKey} = compare_range_keys(DbName, Path, Op, Value),
    FoldFun = fun(_Key, DocIds, Acc) ->
        NewAcc = lists:foldl(
            fun(DocId, InnerAcc) ->
                case Fun(DocId, InnerAcc) of
                    {ok, Updated} -> Updated;
                    {stop, StopAcc} -> throw({stop, StopAcc})
                end
            end,
            Acc,
            DocIds
        ),
        {ok, NewAcc}
    end,
    barrel_lib:safe_fold(fun() ->
        barrel_store_rocksdb:fold_range_posting_compare_with_snapshot(
            StoreRef, StartKey, EndKey, FoldFun, Acc0, Snapshot)
    end).

%% @private Compute range keys for compare operators
compare_range_keys(DbName, Path, '>', Value) ->
    %% > Value: start AFTER value, end at path end
    StartKey = barrel_store_keys:path_posting_end(DbName, Path ++ [Value]),
    EndKey = barrel_store_keys:path_posting_end(DbName, Path),
    {StartKey, EndKey};
compare_range_keys(DbName, Path, '>=', Value) ->
    %% >= Value: start AT value, end at path end
    StartKey = barrel_store_keys:path_posting_prefix(DbName, Path ++ [Value]),
    EndKey = barrel_store_keys:path_posting_end(DbName, Path),
    {StartKey, EndKey};
compare_range_keys(DbName, Path, '<', Value) ->
    %% < Value: start at path start, end BEFORE value
    StartKey = barrel_store_keys:path_posting_prefix(DbName, Path),
    EndKey = barrel_store_keys:path_posting_prefix(DbName, Path ++ [Value]),
    {StartKey, EndKey};
compare_range_keys(DbName, Path, '=<', Value) ->
    %% =< Value: start at path start, end AFTER value
    StartKey = barrel_store_keys:path_posting_prefix(DbName, Path),
    EndKey = barrel_store_keys:path_posting_end(DbName, Path ++ [Value]),
    {StartKey, EndKey}.

%% @doc Fold over path index entries matching a value prefix.
%% Uses interval scan: [path, prefix] to [path, prefix ++ 0xFF]
%% Much faster than collecting all values and filtering.
%% Example: `fold_prefix(S, Db, [&lt;&lt;"name"&gt;&gt;], &lt;&lt;"John"&gt;&gt;, Fun, Acc)'
%%   matches: John, Johnny, Johnson, etc.
%% Uses posting lists internally.
-spec fold_prefix(store_ref(), db_name(), [term()], binary(), fun(), term()) -> term().
fold_prefix(StoreRef, DbName, Path, Prefix, Fun, Acc0) when is_binary(Prefix) ->
    fold_prefix(StoreRef, DbName, Path, Prefix, Fun, Acc0, short_range).

%% @doc Fold over path index entries matching a value prefix with explicit read profile.
%% Uses posting lists: iterates posting keys and expands to {Path, DocId} tuples.
-spec fold_prefix(store_ref(), db_name(), [term()], binary(), fun(), term(), read_profile()) -> term().
fold_prefix(StoreRef, DbName, Path, Prefix, Fun, Acc0, _Profile) when is_binary(Prefix) ->
    %% Start key: path + prefix value
    StartPath = Path ++ [Prefix],
    StartKey = barrel_store_keys:path_posting_prefix(DbName, StartPath),
    %% End key: path + prefix + 0xFF (exclusive upper bound)
    EndPrefix = <<Prefix/binary, 16#FF>>,
    EndPath = Path ++ [EndPrefix],
    EndKey = barrel_store_keys:path_posting_prefix(DbName, EndPath),
    FoldFun = fun(Key, DocIds, Acc) ->
        {ok, FullPath} = decode_posting_key(Key),
        NewAcc = lists:foldl(
            fun(DocId, InnerAcc) ->
                case Fun({FullPath, DocId}, InnerAcc) of
                    {ok, Updated} -> Updated;
                    {stop, StopAcc} -> throw({stop, StopAcc})
                end
            end,
            Acc,
            DocIds
        ),
        {ok, NewAcc}
    end,
    barrel_lib:safe_fold(fun() ->
        barrel_store_rocksdb:fold_range_posting(StoreRef, StartKey, EndKey, FoldFun, Acc0)
    end).

%% @doc Fold over posting lists matching a path prefix.
%% The callback receives a list of DocIds from each posting list entry.
%% Much more efficient than fold_path for exists queries since each key
%% contains multiple DocIds instead of one.
%% The callback can return:
%%   - {ok, Acc} to continue
%%   - {stop, Acc} to terminate early
-spec fold_posting(store_ref(), db_name(), [term()], fun(), term()) -> term().
fold_posting(StoreRef, DbName, PathPrefix, Fun, Acc0) ->
    fold_posting(StoreRef, DbName, PathPrefix, Fun, Acc0, short_range).

%% @doc Fold over posting lists matching a value prefix.
%% Like fold_prefix but returns raw DocId lists instead of expanding to tuples.
%% Much more efficient for pure prefix queries with early termination.
%% Uses prefix bloom optimization for faster key lookup.
%% The callback receives (Key, DocIds, Acc) for each posting list.
%% Example: `fold_prefix_posting(S, Db, [&lt;&lt;"name"&gt;&gt;], &lt;&lt;"John"&gt;&gt;, Fun, Acc)'
%%   matches: John, Johnny, Johnson, etc.
-spec fold_prefix_posting(store_ref(), db_name(), [term()], binary(), fun(), term()) -> term().
fold_prefix_posting(StoreRef, DbName, Path, Prefix, Fun, Acc0) when is_binary(Prefix) ->
    StartPath = Path ++ [Prefix],
    StartKey = barrel_store_keys:path_posting_prefix(DbName, StartPath),
    EndPrefix = <<Prefix/binary, 16#FF>>,
    EndPath = Path ++ [EndPrefix],
    EndKey = barrel_store_keys:path_posting_prefix(DbName, EndPath),
    %% Use prefix-optimized fold with bloom filter
    barrel_store_rocksdb:fold_range_posting_prefix(StoreRef, StartKey, EndKey, Fun, Acc0).

%% @doc Fold over posting lists with snapshot for consistent reads.
%% Same as fold_posting but uses a snapshot for read consistency.
-spec fold_posting_with_snapshot(store_ref(), db_name(), [term()], fun(), term(),
                                  barrel_store_rocksdb:snapshot()) -> term().
fold_posting_with_snapshot(StoreRef, DbName, PathPrefix, Fun, Acc0, Snapshot) ->
    StartKey = barrel_store_keys:path_posting_prefix(DbName, PathPrefix),
    EndKey = barrel_store_keys:path_posting_end(DbName, PathPrefix),
    barrel_store_rocksdb:fold_range_posting_with_snapshot(StoreRef, StartKey, EndKey, Fun, Acc0, Snapshot).

%% @doc Fold over prefix posting lists with snapshot for consistent reads.
%% Same as fold_prefix_posting but uses a snapshot for read consistency.
-spec fold_prefix_posting_with_snapshot(store_ref(), db_name(), [term()], binary(), fun(), term(),
                                         barrel_store_rocksdb:snapshot()) -> term().
fold_prefix_posting_with_snapshot(StoreRef, DbName, Path, Prefix, Fun, Acc0, Snapshot) when is_binary(Prefix) ->
    StartPath = Path ++ [Prefix],
    StartKey = barrel_store_keys:path_posting_prefix(DbName, StartPath),
    EndPrefix = <<Prefix/binary, 16#FF>>,
    EndPath = Path ++ [EndPrefix],
    EndKey = barrel_store_keys:path_posting_prefix(DbName, EndPath),
    %% Use prefix-optimized fold with bloom filter
    barrel_store_rocksdb:fold_range_posting_prefix_with_snapshot(StoreRef, StartKey, EndKey, Fun, Acc0, Snapshot).

%% @doc Fold over posting lists with explicit read profile.
-spec fold_posting(store_ref(), db_name(), [term()], fun(), term(), read_profile()) -> term().
fold_posting(StoreRef, DbName, PathPrefix, Fun, Acc0, _Profile) ->
    StartKey = barrel_store_keys:path_posting_prefix(DbName, PathPrefix),
    EndKey = barrel_store_keys:path_posting_end(DbName, PathPrefix),
    barrel_store_rocksdb:fold_range_posting(StoreRef, StartKey, EndKey, Fun, Acc0).

%% @doc Get posting list for an exact path+value.
%% This is O(1) lookup - no iteration needed.
%% Returns list of DocIds or empty list if path not found.
-spec get_posting_list(store_ref(), db_name(), [term()]) -> [binary()].
get_posting_list(StoreRef, DbName, FullPath) ->
    Key = barrel_store_keys:path_posting_key(DbName, FullPath),
    case barrel_store_rocksdb:posting_get(StoreRef, Key) of
        {ok, DocIds} -> DocIds;
        not_found -> [];
        {error, _} -> []
    end.

%% @doc Get posting list for an exact path+value, returning SORTED DocIds.
%% V2 posting lists store keys in lexicographic order, so no sorting needed.
%% Uses postings_keys/1 which returns already-sorted keys from V2 format.
%% FullPath format: [field1, field2, value] where value is the last element.
-spec get_posting_list_sorted(store_ref(), db_name(), [term()]) -> [binary()].
get_posting_list_sorted(StoreRef, DbName, FullPath) ->
    %% V2 posting lists have keys pre-sorted in lexicographic order
    case get_posting_list_binary(StoreRef, DbName, FullPath) of
        {ok, Binary} ->
            {ok, Postings} = barrel_postings:open(Binary),
            barrel_postings:keys(Postings);  %% Already sorted in V2!
        not_found ->
            [];
        {error, _} ->
            []
    end.

%% @doc Get DocIds from bucketed posting lists in sorted order.
%% Iterates buckets (each ~20-100 DocIds) in sorted bucket order.
%% Within each bucket, DocIds are merged and sorted.
%% FullPath format: [field1, field2, value] where value is the last element.
-spec get_bucketed_posting_list(store_ref(), db_name(), term(), [term()]) -> [binary()].
get_bucketed_posting_list(StoreRef, DbName, Value, FieldPath) ->
    StartKey = barrel_store_keys:value_posting_bucket_prefix(DbName, Value, FieldPath),
    EndKey = barrel_store_keys:value_posting_bucket_end(DbName, Value, FieldPath),
    %% Iterate buckets in sorted order (by bucket prefix = first 2 bytes of DocId)
    %% fold_range_posting already decodes posting lists, so BucketDocIds is a list
    FoldFun = fun(_Key, BucketDocIds, Acc) ->
        %% BucketDocIds is already decoded by fold_range_posting
        %% Merge into accumulator
        {ok, Acc ++ BucketDocIds}
    end,
    AllDocIds = barrel_store_rocksdb:fold_range_posting(StoreRef, StartKey, EndKey, FoldFun, []),
    %% Sort the collected DocIds
    lists:sort(AllDocIds).

%%====================================================================
%% Native Postings API (V2 Posting Lists)
%%====================================================================

%% @doc Get posting list binary for an exact path+value.
%% Returns the raw binary from storage without decoding.
%% Use with barrel_postings:open/1 for repeated lookups.
-spec get_posting_list_binary(store_ref(), db_name(), [term()]) ->
    {ok, binary()} | not_found | {error, term()}.
get_posting_list_binary(StoreRef, DbName, FullPath) ->
    Key = barrel_store_keys:path_posting_key(DbName, FullPath),
    barrel_store_rocksdb:posting_get_binary(StoreRef, Key).

%% @doc Get posting list as a postings resource for fast repeated lookups.
%% Parse once, lookup many times with O(1) or O(log n) performance.
%% Returns an opaque postings resource for use with barrel_postings functions.
-spec get_postings_resource(store_ref(), db_name(), [term()]) ->
    {ok, barrel_postings:postings()} | not_found | {error, term()}.
get_postings_resource(StoreRef, DbName, FullPath) ->
    case get_posting_list_binary(StoreRef, DbName, FullPath) of
        {ok, Binary} ->
            barrel_postings:open(Binary);
        not_found ->
            not_found;
        {error, _} = Error ->
            Error
    end.

%% @doc Intersect multiple posting list binaries using native roaring bitmap.
%% Keys in V2 posting lists are pre-sorted in lexicographic order.
%% Returns {ok, Postings} with the intersection result.
-spec intersect_posting_lists([binary()]) ->
    {ok, barrel_postings:postings()} | {error, term()}.
intersect_posting_lists(PostingBinaries) when is_list(PostingBinaries) ->
    barrel_postings:intersect_all(PostingBinaries).

%% @doc Check if a postings resource contains a DocId using exact lookup.
%% O(log n) binary search on sorted keys.
-spec posting_contains(barrel_postings:postings(), binary()) -> boolean().
posting_contains(Postings, DocId) ->
    barrel_postings:contains(Postings, DocId).

%% @doc Check if a postings resource contains a DocId using bitmap lookup.
%% O(1) hash-based lookup, may have rare false positives.
%% Use posting_contains/2 for exact checks when false positives are unacceptable.
-spec posting_bitmap_contains(barrel_postings:postings(), binary()) -> boolean().
posting_bitmap_contains(Postings, DocId) ->
    barrel_postings:bitmap_contains(Postings, DocId).

%% @doc Fold over value-first index with early termination support.
%% Iterates over individual keys (one per DocId) allowing bounded queries
%% to stop after collecting enough results.
%%
%% Fun receives (DocId, Acc) and should return:
%%   {ok, NewAcc} - continue iteration
%%   {stop, FinalAcc} - stop iteration early
%%
%% Example: `fold_value_index(S, Db, &lt;&lt;"user"&gt;&gt;, [&lt;&lt;"type"&gt;&gt;], Fun, Acc)'
%%   finds all docs where type = "user"
-spec fold_value_index(store_ref(), db_name(), term(), [term()], fun(), term()) -> term().
fold_value_index(StoreRef, DbName, Value, FieldPath, Fun, Acc0) ->
    StartKey = barrel_store_keys:value_index_prefix(DbName, Value, FieldPath),
    EndKey = barrel_store_keys:value_index_end(DbName, Value, FieldPath),
    PrefixLen = byte_size(StartKey),
    FoldFun = fun(Key, _Value, Acc) ->
        %% Extract DocId from key (everything after the prefix)
        <<_:PrefixLen/binary, DocId/binary>> = Key,
        Fun(DocId, Acc)
    end,
    barrel_store_rocksdb:fold_range(StoreRef, StartKey, EndKey, FoldFun, Acc0).

%% @doc Fold over value-first index with snapshot for consistent reads.
-spec fold_value_index(store_ref(), db_name(), term(), [term()], fun(), term(),
                       barrel_store_rocksdb:snapshot()) -> term().
fold_value_index(StoreRef, DbName, Value, FieldPath, Fun, Acc0, Snapshot) ->
    StartKey = barrel_store_keys:value_index_prefix(DbName, Value, FieldPath),
    EndKey = barrel_store_keys:value_index_end(DbName, Value, FieldPath),
    PrefixLen = byte_size(StartKey),
    FoldFun = fun(Key, _Value, Acc) ->
        %% Extract DocId from key (everything after the prefix)
        <<_:PrefixLen/binary, DocId/binary>> = Key,
        Fun(DocId, Acc)
    end,
    %% Use range fold with snapshot - prefix optimization requires prefix extractor config
    barrel_store_rocksdb:fold_range_with_snapshot(
        StoreRef, StartKey, EndKey, FoldFun, Acc0, Snapshot).

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Create batch operations for indexing paths
make_index_ops(DbName, DocId, Paths) ->
    %% Create posting list append operations for all paths
    %% Key: [prefix, db, path] -> Value: list of DocIds
    PostingOps = [{posting_append,
                   barrel_store_keys:path_posting_key(DbName, Path),
                   DocId}
                  || {Path, _} <- Paths],

    %% Create value-first index operations for fast equality queries
    %% Key: [prefix, db, value_prefix, field_path, DocId] -> Value: empty
    ValueIndexOps = make_value_index_ops(DbName, DocId, Paths),

    %% Create bucketed posting list operations for sorted bulk iteration
    %% Key: [prefix, db, value_prefix, field_path, bucket] -> posting list
    BucketedPostingOps = make_bucketed_posting_ops(DbName, DocId, Paths),

    %% Increment counters for each path
    StatsOps = [{merge, barrel_store_keys:path_stats_key(DbName, Path), 1}
                || {Path, _} <- Paths],

    %% NOTE: Bitmap operations removed - V2 posting lists have built-in roaring bitmaps
    %% Use barrel_postings:bitmap_contains/2 for O(1) existence checks

    %% Store reverse index (doc_id -> paths) for later updates/deletes
    ReverseOp = {put,
                 barrel_store_keys:doc_paths_key(DbName, DocId),
                 term_to_binary(Paths)},

    PostingOps ++ ValueIndexOps ++ BucketedPostingOps ++ StatsOps ++ [ReverseOp].

%% @private Create value-first index operations for equality queries
%% Path format: [field1, field2, value] -> value_index_key(DbName, value, [field1, field2], DocId)
%% Uses individual keys (one per DocId) for iterable scans with early termination
make_value_index_ops(_DbName, _DocId, []) ->
    [];
make_value_index_ops(DbName, DocId, [{Path, _Type} | Rest]) ->
    case Path of
        [] ->
            make_value_index_ops(DbName, DocId, Rest);
        [_Single] ->
            %% Single element path (just value, no field) - skip value-first
            make_value_index_ops(DbName, DocId, Rest);
        _ ->
            %% Extract value (last element) and field path (rest)
            {FieldPath, Value} = split_path_value(Path),
            %% Individual key with DocId in the key, empty value
            Key = barrel_store_keys:value_index_key(DbName, Value, FieldPath, DocId),
            Op = {put, Key, <<>>},
            [Op | make_value_index_ops(DbName, DocId, Rest)]
    end.

%% @private Split path into field path and value
%% [field1, field2, value] -> {[field1, field2], value}
split_path_value(Path) ->
    ReversedPath = lists:reverse(Path),
    [Value | ReversedFieldPath] = ReversedPath,
    {lists:reverse(ReversedFieldPath), Value}.

%% @private Create bucketed posting list operations
%% Each path+value combination gets a posting append to its bucket key.
%% Buckets are based on DocId prefix for sorted iteration.
make_bucketed_posting_ops(_DbName, _DocId, []) ->
    [];
make_bucketed_posting_ops(DbName, DocId, [{Path, _Type} | Rest]) ->
    case Path of
        [] ->
            make_bucketed_posting_ops(DbName, DocId, Rest);
        [_Single] ->
            %% Single element path - skip bucketed posting
            make_bucketed_posting_ops(DbName, DocId, Rest);
        _ ->
            %% Extract value (last element) and field path (rest)
            {FieldPath, Value} = split_path_value(Path),
            %% Bucketed posting key includes DocId bucket for sorted iteration
            Key = barrel_store_keys:value_posting_bucket_key(DbName, Value, FieldPath, DocId),
            Op = {posting_append, Key, DocId},
            [Op | make_bucketed_posting_ops(DbName, DocId, Rest)]
    end.

%% @private Create bucketed posting list remove operations
make_bucketed_posting_remove_ops(_DbName, _DocId, []) ->
    [];
make_bucketed_posting_remove_ops(DbName, DocId, [{Path, _Type} | Rest]) ->
    case Path of
        [] ->
            make_bucketed_posting_remove_ops(DbName, DocId, Rest);
        [_Single] ->
            make_bucketed_posting_remove_ops(DbName, DocId, Rest);
        _ ->
            {FieldPath, Value} = split_path_value(Path),
            Key = barrel_store_keys:value_posting_bucket_key(DbName, Value, FieldPath, DocId),
            Op = {posting_remove, Key, DocId},
            [Op | make_bucketed_posting_remove_ops(DbName, DocId, Rest)]
    end.

%% @private Decode a posting key to extract path (no docid - docids are in value)
%% Key format: 0x14 + len:16 + dbname + encoded_path
decode_posting_key(Key) ->
    %% Skip prefix byte (0x14) and extract db name length
    <<16#14, DbNameLen:16, _DbName:DbNameLen/binary, Rest/binary>> = Key,
    %% The rest is just the encoded_path (no docid)
    Path = decode_path_only(Rest, []),
    {ok, Path}.

%% @private Decode path components only (no trailing docid)
decode_path_only(<<>>, Acc) ->
    lists:reverse(Acc);
decode_path_only(<<16#01, Rest/binary>>, Acc) ->  %% null
    decode_path_only(Rest, [null | Acc]);
decode_path_only(<<16#02, Rest/binary>>, Acc) ->  %% false
    decode_path_only(Rest, [false | Acc]);
decode_path_only(<<16#03, Rest/binary>>, Acc) ->  %% true
    decode_path_only(Rest, [true | Acc]);
decode_path_only(<<16#20, Rest/binary>>, Acc) ->  %% zero
    decode_path_only(Rest, [0 | Acc]);
decode_path_only(<<16#30, Len:8, IntBin:Len/binary, Rest/binary>>, Acc) when Len > 0 ->  %% positive int
    N = binary_to_integer(IntBin),
    decode_path_only(Rest, [N | Acc]);
decode_path_only(<<16#10, InvLen:8, Rest/binary>>, Acc) when InvLen < 255 ->  %% negative int
    Len = 255 - InvLen,
    <<InvBytes:Len/binary, Rest2/binary>> = Rest,
    Bin2 = << <<(255 - B)>> || <<B>> <= InvBytes >>,
    N = -binary_to_integer(Bin2),
    decode_path_only(Rest2, [N | Acc]);
decode_path_only(<<16#40, Encoded:8/binary, Rest/binary>>, Acc) ->  %% float
    F = decode_float(Encoded),
    decode_path_only(Rest, [F | Acc]);
decode_path_only(<<16#50, Rest/binary>>, Acc) ->  %% binary (null-escaped)
    {BinVal, Rest2} = unescape_binary(Rest),
    decode_path_only(Rest2, [BinVal | Acc]).

%% @private Unescape binary (same as in barrel_store_keys)
unescape_binary(Bin) ->
    unescape_binary(Bin, <<>>).

unescape_binary(<<0, 0, Rest/binary>>, Acc) ->
    {Acc, Rest};
unescape_binary(<<0, 16#FF, Rest/binary>>, Acc) ->
    unescape_binary(Rest, <<Acc/binary, 0>>);
unescape_binary(<<B, Rest/binary>>, Acc) ->
    unescape_binary(Rest, <<Acc/binary, B>>);
unescape_binary(<<>>, Acc) ->
    {Acc, <<>>}.

%% @private Decode float (same as in barrel_store_keys)
decode_float(<<Encoded:64/big-unsigned>>) ->
    Bits = case Encoded band 16#8000000000000000 of
        0 -> bnot Encoded;
        _ -> Encoded bxor 16#8000000000000000
    end,
    <<F:64/float>> = <<Bits:64/big-unsigned>>,
    F.

%% @private Create batch operations for updating paths (add/remove)
make_update_ops(DbName, DocId, Added, Removed, NewPaths) ->
    %% Build posting list operations
    RemoveOps = [{posting_remove,
                  barrel_store_keys:path_posting_key(DbName, Path),
                  DocId}
                 || {Path, _} <- Removed],
    AddOps = [{posting_append,
               barrel_store_keys:path_posting_key(DbName, Path),
               DocId}
              || {Path, _} <- Added],

    %% Value-first index operations
    ValueRemoveOps = make_value_index_remove_ops(DbName, DocId, Removed),
    ValueAddOps = make_value_index_ops(DbName, DocId, Added),

    %% Update counters: decrement removed, increment added
    DecrOps = [{merge, barrel_store_keys:path_stats_key(DbName, Path), -1}
               || {Path, _} <- Removed],
    IncrOps = [{merge, barrel_store_keys:path_stats_key(DbName, Path), 1}
               || {Path, _} <- Added],

    %% NOTE: Bitmap operations removed - V2 posting lists have built-in roaring bitmaps

    %% Update reverse index with new paths
    ReverseOp = {put,
                 barrel_store_keys:doc_paths_key(DbName, DocId),
                 term_to_binary(NewPaths)},

    RemoveOps ++ AddOps ++ ValueRemoveOps ++ ValueAddOps ++
        DecrOps ++ IncrOps ++ [ReverseOp].
