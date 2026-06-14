%%%-------------------------------------------------------------------
%%% @doc Query compiler and executor for barrel_docdb
%%%
%%% Provides functions to compile Datalog-style query specifications
%%% into query plans, and execute them against the path index.
%%%
%%% Query Syntax:
%%% ```
%%% #{
%%%     where => [
%%%         {path, [<<"type">>], <<"user">>},           % equality
%%%         {path, [<<"org_id">>], '?Org'},              % bind variable
%%%         {compare, [<<"age">>], '>', 18},            % comparison
%%%         {'and', [...]},                              % conjunction
%%%         {'or', [...]}                                % disjunction
%%%     ],
%%%     select => ['?Org', '?Name'],   % fields/variables to return
%%%     order_by => '?Name',           % ordering
%%%     limit => 100,                   % max results
%%%     offset => 0                     % skip first N
%%% }
%%% '''
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_query).

-include("barrel_docdb.hrl").

%% API
-export([
    compile/1,
    validate_spec/1,
    execute/3,
    execute/4,
    match/2,
    matches/2,
    explain/1,
    extract_paths/1
]).

%% Internal exports for testing
-export([
    is_logic_var/1,
    normalize_condition/1
]).

%% Profiling (temporary)
-export([get_profile/0, reset_profile/0, dump_profile/0]).

%%====================================================================
%% Types
%%====================================================================

-type logic_var() :: atom().  % Atoms starting with '?'

-type path() :: [binary() | integer()].

-type value() :: binary() | number() | boolean() | null | logic_var().

-type compare_op() :: '>' | '<' | '>=' | '=<' | '=/=' | '=='.

-type condition() ::
    {path, path(), value()} |
    {compare, path(), compare_op(), value()} |
    {'and', [condition()]} |
    {'or', [condition()]} |
    {'not', condition()} |
    {in, path(), [value()]} |
    {contains, path(), value()} |
    {exists, path()} |
    {missing, path()} |
    {regex, path(), binary()} |
    {prefix, path(), binary()}.

-type projection() :: logic_var() | path() | '*'.

-type order_spec() :: logic_var() | path() | {logic_var() | path(), asc | desc}.

-type query_spec() :: #{
    where => [condition()],
    select => [projection()],
    order_by => order_spec() | [order_spec()],
    limit => pos_integer(),
    offset => non_neg_integer(),
    include_docs => boolean(),
    flat => boolean(),
    id_prefix => binary(),
    id_range => {binary() | undefined, binary() | undefined},
    doc_format => binary | map | json,
    decoder_fun => fun((binary()) -> term())
}.

-record(query_plan, {
    %% Normalized conditions
    conditions :: [condition()],
    %% Variables bound in conditions
    bindings :: #{logic_var() => path()},
    %% Fields/variables to project
    projections :: [projection()],
    %% Ordering specification
    order :: [{path() | logic_var(), asc | desc}],
    %% Result limit
    limit :: pos_integer() | undefined,
    %% Result offset
    offset :: non_neg_integer(),
    %% Include full documents
    include_docs :: boolean(),
    %% Document format for include_docs: map (default) | binary | json
    doc_format :: binary | map | json,
    %% Custom decoder function for documents
    decoder_fun :: undefined | fun((binary()) -> term()),
    %% Index strategy hint
    strategy :: index_seek | index_scan | multi_index | full_scan,
    %% Return flat documents (Doc#{<<"id">>}) instead of #{id, doc} wrappers
    flat = false :: boolean(),
    %% Primary-key scan over the entity keyspace by document id:
    %% undefined | {prefix, binary()} | {range, Start, End}
    %% (Start/End are binary() | undefined). Bypasses the ARS index.
    id_scan = undefined :: undefined | {prefix, binary()}
                         | {range, binary() | undefined, binary() | undefined}
}).

-type query_plan() :: #query_plan{}.

%% Chunked execution options
-type chunk_opts() :: #{
    chunk_size => pos_integer(),          %% Max results per chunk (default 1000)
    continuation => binary() | undefined, %% Resume from previous chunk
    eventual_consistency => boolean()     %% Skip snapshot for fresh reads (default false)
}.

%% Chunked execution result metadata
-type result_meta() :: #{
    last_seq := seq(),
    has_more := boolean(),
    continuation => binary()  %% Only present if has_more = true
}.

-export_type([query_spec/0, query_plan/0, condition/0, logic_var/0, chunk_opts/0, result_meta/0]).

%% Read profile type from storage layer
-type read_profile() :: barrel_store_rocksdb:read_profile().

%%====================================================================
%% Chunked Execution Constants
%%====================================================================

%% Target: ~1MB of doc data per chunk for good cache utilization
-define(TARGET_CHUNK_BYTES, 1048576).
-define(MIN_CHUNK_SIZE, 50).
-define(MAX_CHUNK_SIZE, 1000).
-define(INITIAL_CHUNK_SIZE, 200).

%%====================================================================
%% Adaptive Strategy Constants
%%====================================================================

%% Threshold for choosing between scan vs posting list strategies:
%% - Below threshold: use value_index scan (per-doc keys, early termination)
%% - Above threshold: use posting list decode (one seek, bulk decode)
-define(ADAPTIVE_CARDINALITY_THRESHOLD, 1000).

%% Maximum unbounded results before forcing pagination
%% For include_docs=true queries without limit, auto-paginate at this size
-define(MAX_UNBOUNDED_RESULTS, 1000).

%%====================================================================
%% Streaming Batch Fetch Constants
%%====================================================================

%% Minimum batch size for streaming (too small = overhead from many batches)
-define(STREAM_MIN_BATCH, 50).
%% Maximum batch size for streaming (too large = defeats the purpose)
-define(STREAM_MAX_BATCH, 500).
%% Initial assumed selectivity (50% of docs pass filter)
-define(STREAM_INITIAL_SELECTIVITY, 0.5).
%% Threshold ratio: use streaming when we have Nx more docs than needed
-define(STREAM_THRESHOLD_RATIO, 3).

%% Streaming statistics for adaptive batch sizing
-record(stream_stats, {
    fetched = 0 :: non_neg_integer(),      %% Total DocIds fetched
    decoded = 0 :: non_neg_integer(),      %% Total docs decoded
    matched = 0 :: non_neg_integer(),      %% Docs passing filter
    selectivity = ?STREAM_INITIAL_SELECTIVITY :: float()  %% Estimated filter pass rate
}).

%%====================================================================
%% Pipelined Execution Constants
%%====================================================================

%% Batch size for pipelined index iteration
-define(PIPELINE_BATCH_SIZE, 200).
%% Maximum in-flight body fetches (limits memory usage)
-define(PIPELINE_MAX_INFLIGHT, 4).
%% Minimum DocIds to use pipelining (below this, sequential is faster)
-define(PIPELINE_MIN_DOCIDS, 500).

%%====================================================================
%% API
%%====================================================================

%% @doc Compile a query specification into a query plan.
%% Returns {ok, QueryPlan} or {error, Reason}.
-spec compile(query_spec()) -> {ok, query_plan()} | {error, term()}.
compile(Spec) when is_map(Spec) ->
    barrel_trace:with_db_span(query_compile, undefined, fun() ->
        case validate_spec(Spec) of
            ok ->
                do_compile(Spec);
            {error, _} = Error ->
                barrel_trace:record_error(Error),
                Error
        end
    end);
compile(_) ->
    {error, {invalid_spec, not_a_map}}.

%% @doc Validate a query specification.
%% Returns ok or {error, Reason}.
-spec validate_spec(query_spec()) -> ok | {error, term()}.
validate_spec(Spec) when is_map(Spec) ->
    HasIdScan = maps:is_key(id_prefix, Spec) orelse maps:is_key(id_range, Spec),
    case maps:get(where, Spec, undefined) of
        undefined when HasIdScan ->
            %% Standalone id_prefix / id_range scan - no where clause required.
            case compile_id_scan(Spec) of
                {error, _} = Error -> Error;
                _ -> ok
            end;
        undefined ->
            {error, {missing_clause, where}};
        Where when is_list(Where) ->
            validate_conditions(Where);
        _ ->
            {error, {invalid_clause, where, must_be_list}}
    end;
validate_spec(_) ->
    {error, {invalid_spec, not_a_map}}.

%% @doc Execute a compiled query plan against a database.
%% Returns {ok, Results, LastSeq} or {error, Reason}.
%% Uses a snapshot for read consistency across all index and document reads.
%%
%% For unbounded include_docs queries, automatically paginates internally
%% to avoid excessive memory usage from large MultiGet operations.
-spec execute(barrel_store_rocksdb:db_ref(), db_name(), query_plan()) ->
    {ok, [map()], seq()} | {error, term()}.
execute(StoreRef, DbName, #query_plan{include_docs = true, limit = undefined} = Plan) ->
    %% Unbounded include_docs query - auto-paginate to avoid memory issues
    Result = barrel_trace:with_db_span(query_execute, DbName, fun() ->
        execute_with_auto_pagination(StoreRef, DbName, Plan)
    end),
    maybe_flatten_result(Result, Plan);
execute(StoreRef, DbName, #query_plan{} = Plan) ->
    %% Bounded query or pure index query - execute directly
    Result = barrel_trace:with_db_span(query_execute, DbName, fun() ->
        {ok, Snapshot} = barrel_store_rocksdb:snapshot(StoreRef),
        try
            execute_with_snapshot(StoreRef, DbName, Plan, Snapshot)
        after
            barrel_store_rocksdb:release_snapshot(Snapshot)
        end
    end),
    maybe_flatten_result(Result, Plan).

%% @private Execute unbounded include_docs query with automatic pagination
%% Collects results in chunks to avoid memory pressure from large MultiGet
execute_with_auto_pagination(StoreRef, DbName, Plan) ->
    collect_all_chunks(StoreRef, DbName, Plan, undefined, []).

%% @private Direct execution bypassing auto-pagination
%% Used when chunked execution falls back to non-chunked for needs_body queries
execute_direct(StoreRef, DbName, Plan) ->
    {ok, Snapshot} = barrel_store_rocksdb:snapshot(StoreRef),
    try
        execute_with_snapshot(StoreRef, DbName, Plan, Snapshot)
    after
        barrel_store_rocksdb:release_snapshot(Snapshot)
    end.

%% @private Collect all chunks from paginated query
-spec collect_all_chunks(barrel_store_rocksdb:db_ref(), db_name(), query_plan(),
                         binary() | undefined, [map()]) ->
    {ok, [map()], seq()} | {error, term()}.
collect_all_chunks(StoreRef, DbName, Plan, undefined, AccResults) ->
    %% Initial call - no continuation token
    %% execute_chunked_fresh always succeeds (no cursor lookup)
    Opts = #{chunk_size => ?MAX_UNBOUNDED_RESULTS},
    {ok, Results, Meta} = execute_chunked_fresh(StoreRef, DbName, Plan, Opts),
    case Meta of
        #{has_more := false, last_seq := LastSeq} ->
            {ok, AccResults ++ Results, LastSeq};
        #{has_more := true, continuation := NextToken} ->
            collect_all_chunks(StoreRef, DbName, Plan, NextToken, AccResults ++ Results)
    end;
collect_all_chunks(StoreRef, DbName, Plan, Token, AccResults) when is_binary(Token) ->
    %% Continuation call - cursor lookup may fail
    Opts = #{chunk_size => ?MAX_UNBOUNDED_RESULTS},
    case execute_chunked_resume(StoreRef, DbName, Plan, Token, Opts) of
        {ok, Results, #{has_more := false, last_seq := LastSeq}} ->
            {ok, AccResults ++ Results, LastSeq};
        {ok, Results, #{has_more := true, continuation := NextToken}} ->
            collect_all_chunks(StoreRef, DbName, Plan, NextToken, AccResults ++ Results);
        {error, _} = Error ->
            Error
    end.

%% @doc Execute a compiled query plan with chunked execution options.
%% Returns {ok, Results, ResultMeta} where ResultMeta includes:
%%   - last_seq: sequence number for change tracking
%%   - has_more: true if more results available
%%   - continuation: opaque token to resume (only if has_more = true)
%%
%% Options:
%%   - chunk_size: max results per chunk (default 1000)
%%   - continuation: resume token from previous call
%%   - eventual_consistency: if true, each chunk sees current data (default false)
%%
%% Example:
%% ```
%% %% First chunk
%% {ok, R1, #{has_more := true, continuation := Token}} =
%%     barrel_query:execute(Store, Db, Plan, #{chunk_size => 100}),
%%
%% %% Next chunk
%% {ok, R2, #{has_more := false}} =
%%     barrel_query:execute(Store, Db, Plan, #{continuation => Token}).
%% '''
-spec execute(barrel_store_rocksdb:db_ref(), db_name(), query_plan(), chunk_opts()) ->
    {ok, [map()], result_meta()} | {error, term()}.
execute(StoreRef, DbName, #query_plan{} = Plan, Opts) when is_map(Opts) ->
    Result = barrel_trace:with_db_span(query_execute, DbName, fun() ->
        case maps:get(continuation, Opts, undefined) of
            undefined ->
                %% Fresh query - create new snapshot
                execute_chunked_fresh(StoreRef, DbName, Plan, Opts);
            Token ->
                %% Resume from cursor
                execute_chunked_resume(StoreRef, DbName, Plan, Token, Opts)
        end
    end),
    maybe_flatten_result(Result, Plan).

%% @private Apply `flat => true' to a query result: replace #{id, doc} wrappers
%% with the flat document Doc#{<<"id">>}. Other shapes pass through.
maybe_flatten_result({ok, Results, Meta}, #query_plan{flat = true, include_docs = true}) ->
    {ok, [flatten_one(R) || R <- Results], Meta};
maybe_flatten_result(Result, _Plan) ->
    Result.

flatten_one(#{<<"doc">> := Doc, <<"id">> := Id}) when is_map(Doc) ->
    Doc#{<<"id">> => Id};
flatten_one(Other) ->
    Other.

%%====================================================================
%% Chunked Execution Implementation
%%====================================================================

%% @private Execute fresh chunked query (no continuation token)
execute_chunked_fresh(StoreRef, DbName, Plan, Opts) ->
    ChunkSize = maps:get(chunk_size, Opts, 1000),
    EventualConsistency = maps:get(eventual_consistency, Opts, false),

    %% Create snapshot for consistent reads (unless eventual consistency)
    Snapshot = case EventualConsistency of
        true -> undefined;
        false ->
            {ok, S} = barrel_store_rocksdb:snapshot(StoreRef),
            S
    end,

    try
        execute_chunked_with_snapshot(StoreRef, DbName, Plan, ChunkSize, undefined, Snapshot)
    catch
        Class:Reason:Stack ->
            %% Release snapshot on error
            maybe_release_snapshot(Snapshot),
            erlang:raise(Class, Reason, Stack)
    end.

%% @private Resume chunked query from cursor
execute_chunked_resume(StoreRef, DbName, Plan, Token, Opts) ->
    case barrel_query_cursor:lookup(Token) of
        {ok, Cursor} ->
            %% Get cursor fields
            #{snapshot := Snapshot, last_key := LastKey, query_type := QueryType} = cursor_to_map(Cursor),
            ChunkSize = maps:get(chunk_size, Opts, 1000),

            %% Validate cursor matches current query
            %% Currently validate_cursor_for_plan always returns ok
            ok = validate_cursor_for_plan(QueryType, Plan),
            execute_chunked_with_snapshot(StoreRef, DbName, Plan, ChunkSize, LastKey, Snapshot);
        {error, expired} ->
            {error, cursor_expired};
        {error, not_found} ->
            {error, cursor_not_found}
    end.

%% @private Execute chunked query with snapshot
execute_chunked_with_snapshot(StoreRef, DbName, #query_plan{id_scan = IdScan} = Plan,
                              ChunkSize, StartKey, Snapshot)
  when IdScan =/= undefined ->
    %% Primary-key scan over the entity keyspace by document id.
    execute_id_scan_chunked(StoreRef, DbName, IdScan, Plan, ChunkSize, StartKey, Snapshot);
execute_chunked_with_snapshot(StoreRef, DbName, Plan, ChunkSize, StartKey, Snapshot) ->
    #query_plan{conditions = Conditions} = Plan,

    %% Classify query type for pure index execution
    QueryType = classify_scan_query(Conditions, Plan),

    case QueryType of
        {pure_equality, Path, Value} ->
            execute_pure_equality_chunked(StoreRef, DbName, Path, Value, Plan, ChunkSize, StartKey, Snapshot);
        {pure_exists, Path} ->
            execute_pure_exists_chunked(StoreRef, DbName, Path, Plan, ChunkSize, StartKey, Snapshot);
        {pure_prefix, Path, Prefix} ->
            execute_pure_prefix_chunked(StoreRef, DbName, Path, Prefix, Plan, ChunkSize, StartKey, Snapshot);
        {pure_compare, Path, Op, Value} ->
            execute_pure_compare_chunked(StoreRef, DbName, Path, Op, Value, Plan, ChunkSize, StartKey, Snapshot);
        {multi_index, IndexConditions} ->
            execute_multi_index_chunked(StoreRef, DbName, IndexConditions, Plan, ChunkSize, StartKey, Snapshot);
        needs_body ->
            %% Body-fetch queries require loading full documents, which doesn't
            %% support efficient chunked iteration. Fall back to direct execution.
            %% This may increase memory usage for large result sets.
            maybe_release_snapshot(Snapshot),
            {ok, Results, LastSeq} = execute_direct(StoreRef, DbName, Plan),
            {ok, Results, #{last_seq => LastSeq, has_more => false}}
    end.

%% @private Execute chunked pure equality query
execute_pure_equality_chunked(StoreRef, DbName, Path, Value, Plan, ChunkSize, StartKey, Snapshot) ->
    #query_plan{offset = Offset, include_docs = IncludeDocs, decoder_fun = DecoderFun} = Plan,

    %% Determine start key for iteration
    ActualStartKey = case StartKey of
        undefined ->
            barrel_store_keys:value_index_prefix(DbName, Value, Path);
        Key ->
            %% Resume after the last key
            <<Key/binary, 0>>
    end,
    EndKey = barrel_store_keys:value_index_end(DbName, Value, Path),
    PrefixLen = byte_size(barrel_store_keys:value_index_prefix(DbName, Value, Path)),

    %% Collect ChunkSize + 1 to detect if there are more results
    %% Track the last key of the ChunkSize-th result separately
    MaxCollect = ChunkSize + 1,
    FoldFun = fun(Key, _Val, {Count, ChunkLastK, Acc}) ->
        <<_:PrefixLen/binary, DocId/binary>> = Key,
        NewCount = Count + 1,
        if
            NewCount > MaxCollect ->
                %% Already collected enough, stop
                {stop, {Count, ChunkLastK, Acc}};
            NewCount =:= ChunkSize ->
                %% This is the ChunkSize-th result - save its key for cursor
                {ok, {NewCount, Key, [DocId | Acc]}};
            NewCount > ChunkSize ->
                %% Extra result to detect has_more - don't update ChunkLastK
                {ok, {NewCount, ChunkLastK, [DocId | Acc]}};
            true ->
                {ok, {NewCount, ChunkLastK, [DocId | Acc]}}
        end
    end,

    {CollectedCount, ChunkLastKey, DocIds} =
        case Snapshot of
            undefined ->
                barrel_store_rocksdb:fold_range(
                    StoreRef, ActualStartKey, EndKey, FoldFun, {0, undefined, []});
            _ ->
                barrel_store_rocksdb:fold_range_with_snapshot(
                    StoreRef, ActualStartKey, EndKey, FoldFun, {0, undefined, []}, Snapshot)
        end,

    %% Check if we have more results
    HasMore = CollectedCount > ChunkSize,

    %% Take only ChunkSize doc IDs (drop the extra one if present)
    FinalDocIds0 = case HasMore of
        true -> tl(DocIds);  %% Drop the extra one (results are reversed)
        false -> DocIds
    end,
    FinalDocIds = apply_offset_limit(lists:reverse(FinalDocIds0), Offset, ChunkSize),

    %% Fetch docs if include_docs is true, otherwise just return IDs
    FinalResults = maybe_fetch_docs_chunked(DbName, FinalDocIds, IncludeDocs, DecoderFun),

    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    case HasMore of
        true ->
            %% Create cursor with the key of the last result we're returning
            Token = barrel_query_cursor:create(
                StoreRef, DbName, pure_equality, ChunkLastKey, Snapshot),
            {ok, FinalResults, #{last_seq => LastSeq, has_more => true, continuation => Token}};
        false ->
            %% No more results - release snapshot
            maybe_release_snapshot(Snapshot),
            {ok, FinalResults, #{last_seq => LastSeq, has_more => false}}
    end.

%% @private Execute chunked pure exists query
execute_pure_exists_chunked(StoreRef, DbName, Path, Plan, ChunkSize, StartKey, Snapshot) ->
    #query_plan{offset = Offset, include_docs = IncludeDocs, decoder_fun = DecoderFun} = Plan,

    %% Determine start key for iteration
    ActualStartKey = case StartKey of
        undefined ->
            barrel_store_keys:path_posting_prefix(DbName, Path);
        Key ->
            <<Key/binary, 0>>
    end,
    EndKey = barrel_store_keys:path_posting_end(DbName, Path),

    %% Collect ChunkSize + 1 to detect if there are more
    MaxCollect = ChunkSize + 1,

    %% Use provided snapshot or create a temporary one
    ActualSnapshot = case Snapshot of
        undefined ->
            {ok, S} = barrel_store_rocksdb:snapshot(StoreRef),
            S;
        S -> S
    end,
    %% Fold over posting lists
    FoldResult = barrel_store_rocksdb:fold_range_posting_with_snapshot(
        StoreRef, ActualStartKey, EndKey,
        fun(Key, DocIds0, {Seen, Count, LastK, Acc}) ->
            process_exists_docids_chunked(DocIds0, Key, Seen, Count, LastK, Acc, MaxCollect)
        end,
        {#{}, 0, undefined, []},
        ActualSnapshot
    ),

    {_, CollectedCount, LastCollectedKey, DocIds} = FoldResult,

    HasMore = CollectedCount > ChunkSize,
    FinalDocIds0 = case HasMore of
        true -> tl(DocIds);
        false -> DocIds
    end,
    FinalDocIds = apply_offset_limit(lists:reverse(FinalDocIds0), Offset, ChunkSize),

    %% Fetch docs if include_docs is true
    FinalResults = maybe_fetch_docs_chunked(DbName, FinalDocIds, IncludeDocs, DecoderFun),

    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    case HasMore of
        true ->
            Token = barrel_query_cursor:create(
                StoreRef, DbName, pure_exists, LastCollectedKey, ActualSnapshot),
            {ok, FinalResults, #{last_seq => LastSeq, has_more => true, continuation => Token}};
        false ->
            maybe_release_snapshot(ActualSnapshot),
            {ok, FinalResults, #{last_seq => LastSeq, has_more => false}}
    end.

%% @private Execute chunked pure prefix query
execute_pure_prefix_chunked(StoreRef, DbName, Path, Prefix, Plan, ChunkSize, StartKey, Snapshot) ->
    #query_plan{offset = Offset, include_docs = IncludeDocs, decoder_fun = DecoderFun} = Plan,

    %% Build prefix range
    FullPath = Path ++ [Prefix],
    ActualStartKey = case StartKey of
        undefined ->
            barrel_store_keys:path_posting_prefix(DbName, FullPath);
        Key ->
            <<Key/binary, 0>>
    end,

    %% End key for prefix scan. The value is the last path component and is
    %% encoded with a 0x00 0x00 terminator, so we must append 0xFF *inside*
    %% the value component (before the terminator) to bracket all values that
    %% start with Prefix. Using path_posting_end(FullPath) would place 0xFF
    %% after the terminator and wrongly exclude longer values like
    %% "Hello World" (see fold_prefix/6).
    EndKey = barrel_store_keys:path_posting_prefix(
               DbName, Path ++ [<<Prefix/binary, 16#FF>>]),

    MaxCollect = ChunkSize + 1,

    %% Use provided snapshot or create a temporary one
    ActualSnapshot = case Snapshot of
        undefined ->
            {ok, S} = barrel_store_rocksdb:snapshot(StoreRef),
            S;
        S -> S
    end,
    FoldResult = barrel_store_rocksdb:fold_range_posting_with_snapshot(
        StoreRef, ActualStartKey, EndKey,
        fun(Key, DocIds0, {Seen, Count, LastK, Acc}) ->
            process_prefix_docids_chunked(DocIds0, Key, Seen, Count, LastK, Acc, MaxCollect)
        end,
        {#{}, 0, undefined, []},
        ActualSnapshot
    ),

    {_, CollectedCount, LastCollectedKey, DocIds} = FoldResult,

    HasMore = CollectedCount > ChunkSize,
    FinalDocIds0 = case HasMore of
        true -> tl(DocIds);
        false -> DocIds
    end,
    FinalDocIds = apply_offset_limit(lists:reverse(FinalDocIds0), Offset, ChunkSize),

    %% Fetch docs if include_docs is true
    FinalResults = maybe_fetch_docs_chunked(DbName, FinalDocIds, IncludeDocs, DecoderFun),

    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    case HasMore of
        true ->
            Token = barrel_query_cursor:create(
                StoreRef, DbName, pure_prefix, LastCollectedKey, ActualSnapshot),
            {ok, FinalResults, #{last_seq => LastSeq, has_more => true, continuation => Token}};
        false ->
            maybe_release_snapshot(ActualSnapshot),
            {ok, FinalResults, #{last_seq => LastSeq, has_more => false}}
    end.

%% @private Execute a primary-key (document id) range scan over the entity
%% keyspace. Skips tombstones; supports include_docs/flat/limit/offset and
%% cursor continuation. O(matches in range), ordered by id.
execute_id_scan_chunked(StoreRef, DbName, IdScan, Plan, ChunkSize, StartKey, Snapshot) ->
    #query_plan{offset = Offset, include_docs = IncludeDocs, decoder_fun = DecoderFun} = Plan,
    EntityPrefix = barrel_store_keys:doc_entity_prefix(DbName),
    PrefixLen = byte_size(EntityPrefix),
    {RangeStart, RangeEnd} = id_scan_range(DbName, IdScan),
    ActualStartKey = case StartKey of
        undefined -> RangeStart;
        Key -> <<Key/binary, 0>>
    end,
    ActualSnapshot = case Snapshot of
        undefined ->
            {ok, S} = barrel_store_rocksdb:snapshot(StoreRef),
            S;
        S -> S
    end,
    MaxCollect = ChunkSize + 1,
    FoldFun = fun(Key, _Value, {Count, Acc}) ->
        case entity_live(StoreRef, Key) of
            true ->
                DocId = binary:part(Key, PrefixLen, byte_size(Key) - PrefixLen),
                NewCount = Count + 1,
                NewAcc = [DocId | Acc],
                case NewCount >= MaxCollect of
                    true -> {stop, {NewCount, NewAcc}};
                    false -> {ok, {NewCount, NewAcc}}
                end;
            false ->
                {ok, {Count, Acc}}
        end
    end,
    {CollectedCount, DocIdsRev} = barrel_store_rocksdb:fold_range_with_snapshot(
        StoreRef, ActualStartKey, RangeEnd, FoldFun, {0, []}, ActualSnapshot),

    HasMore = CollectedCount > ChunkSize,
    AllDocIds = lists:reverse(DocIdsRev),
    ReturnedDocIds = lists:sublist(AllDocIds, ChunkSize),
    FinalDocIds = apply_offset_limit(ReturnedDocIds, Offset, ChunkSize),

    FinalResults = maybe_fetch_docs_chunked(DbName, FinalDocIds, IncludeDocs, DecoderFun),
    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    case HasMore andalso ReturnedDocIds =/= [] of
        true ->
            LastDocId = lists:last(ReturnedDocIds),
            LastKey = barrel_store_keys:doc_entity(DbName, LastDocId),
            Token = barrel_query_cursor:create(StoreRef, DbName, id_scan, LastKey, ActualSnapshot),
            {ok, FinalResults, #{last_seq => LastSeq, has_more => true, continuation => Token}};
        false ->
            maybe_release_snapshot(ActualSnapshot),
            {ok, FinalResults, #{last_seq => LastSeq, has_more => false}}
    end.

%% @private Compute the entity-keyspace [Start, End) range for an id_scan.
id_scan_range(DbName, {prefix, P}) ->
    Prefix = barrel_store_keys:doc_entity_prefix(DbName),
    Start = <<Prefix/binary, P/binary>>,
    End = case bin_increment(P) of
        overflow -> barrel_store_keys:doc_entity_end(DbName);
        NextP -> <<Prefix/binary, NextP/binary>>
    end,
    {Start, End};
id_scan_range(DbName, {range, StartId, EndId}) ->
    Prefix = barrel_store_keys:doc_entity_prefix(DbName),
    Start = case StartId of
        undefined -> Prefix;
        _ -> <<Prefix/binary, StartId/binary>>
    end,
    End = case EndId of
        undefined -> barrel_store_keys:doc_entity_end(DbName);
        _ -> <<Prefix/binary, EndId/binary>>
    end,
    {Start, End}.

%% @private Smallest binary greater than all binaries having Bin as a prefix.
%% Returns `overflow' when Bin is empty or all 0xFF (scan to keyspace end).
bin_increment(Bin) ->
    case bin_increment_rev(lists:reverse(binary_to_list(Bin))) of
        overflow -> overflow;
        Rev -> list_to_binary(lists:reverse(Rev))
    end.

bin_increment_rev([]) -> overflow;
bin_increment_rev([16#FF | Rest]) -> bin_increment_rev(Rest);
bin_increment_rev([B | Rest]) -> [B + 1 | Rest].

%% @private A document entity is live (not a tombstone) when its `del' column
%% is not <<"true">>.
entity_live(StoreRef, EntityKey) ->
    case barrel_store_rocksdb:get_entity(StoreRef, EntityKey) of
        {ok, Columns} ->
            proplists:get_value(<<"del">>, Columns, <<"false">>) =/= <<"true">>;
        _ ->
            false
    end.

%% @private Process DocIds for chunked exists query
process_exists_docids_chunked([], _Key, Seen, Count, LastKey, Acc, _MaxCollect) ->
    {ok, {Seen, Count, LastKey, Acc}};
process_exists_docids_chunked([DocId | Rest], Key, Seen, Count, _LastKey, Acc, MaxCollect) ->
    case maps:is_key(DocId, Seen) of
        true ->
            process_exists_docids_chunked(Rest, Key, Seen, Count, Key, Acc, MaxCollect);
        false ->
            NewSeen = Seen#{DocId => true},
            NewCount = Count + 1,
            NewAcc = [DocId | Acc],
            case NewCount >= MaxCollect of
                true ->
                    {stop, {NewSeen, NewCount, Key, NewAcc}};
                false ->
                    process_exists_docids_chunked(Rest, Key, NewSeen, NewCount, Key, NewAcc, MaxCollect)
            end
    end.

%% @private Process DocIds for chunked prefix query
process_prefix_docids_chunked([], _Key, Seen, Count, LastKey, Acc, _MaxCollect) ->
    {ok, {Seen, Count, LastKey, Acc}};
process_prefix_docids_chunked([DocId | Rest], Key, Seen, Count, _LastKey, Acc, MaxCollect) ->
    case maps:is_key(DocId, Seen) of
        true ->
            process_prefix_docids_chunked(Rest, Key, Seen, Count, Key, Acc, MaxCollect);
        false ->
            NewSeen = Seen#{DocId => true},
            NewCount = Count + 1,
            NewAcc = [DocId | Acc],
            case NewCount >= MaxCollect of
                true ->
                    {stop, {NewSeen, NewCount, Key, NewAcc}};
                false ->
                    process_prefix_docids_chunked(Rest, Key, NewSeen, NewCount, Key, NewAcc, MaxCollect)
            end
    end.

%% @private Execute chunked pure compare query (range scan)
execute_pure_compare_chunked(StoreRef, DbName, Path, Op, Value, Plan, ChunkSize, StartKey, Snapshot) ->
    #query_plan{offset = Offset, include_docs = IncludeDocs, decoder_fun = DecoderFun} = Plan,

    %% Compute start/end keys for the range based on operator
    {RangeStart, RangeEnd} = compare_range_keys(DbName, Path, Op, Value),

    %% Use provided StartKey for pagination, otherwise use range start
    ActualStartKey = case StartKey of
        undefined -> RangeStart;
        Key -> <<Key/binary, 0>>
    end,

    MaxCollect = ChunkSize + 1,

    %% Get snapshot if not already present
    ActualSnapshot = case Snapshot of
        undefined ->
            {ok, S} = barrel_store_rocksdb:snapshot(StoreRef),
            S;
        S -> S
    end,

    FoldResult = barrel_store_rocksdb:fold_range_posting_with_snapshot(
        StoreRef, ActualStartKey, RangeEnd,
        fun(Key, DocIds0, {Seen, Count, LastK, Acc}) ->
            process_compare_docids_chunked(DocIds0, Key, Seen, Count, LastK, Acc, MaxCollect)
        end,
        {#{}, 0, undefined, []},
        ActualSnapshot
    ),

    {_, CollectedCount, LastCollectedKey, DocIds} = FoldResult,

    HasMore = CollectedCount > ChunkSize,
    FinalDocIds0 = case HasMore of
        true -> tl(DocIds);
        false -> DocIds
    end,
    FinalDocIds = apply_offset_limit(lists:reverse(FinalDocIds0), Offset, ChunkSize),

    %% Fetch docs if include_docs is true
    FinalResults = maybe_fetch_docs_chunked(DbName, FinalDocIds, IncludeDocs, DecoderFun),

    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    case HasMore of
        true ->
            Token = barrel_query_cursor:create(
                StoreRef, DbName, pure_compare, LastCollectedKey, ActualSnapshot),
            {ok, FinalResults, #{last_seq => LastSeq, has_more => true, continuation => Token}};
        false ->
            maybe_release_snapshot(ActualSnapshot),
            {ok, FinalResults, #{last_seq => LastSeq, has_more => false}}
    end.

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

%% @private Process DocIds for chunked compare query
process_compare_docids_chunked([], _Key, Seen, Count, LastKey, Acc, _MaxCollect) ->
    {ok, {Seen, Count, LastKey, Acc}};
process_compare_docids_chunked([DocId | Rest], Key, Seen, Count, _LastKey, Acc, MaxCollect) ->
    case maps:is_key(DocId, Seen) of
        true ->
            process_compare_docids_chunked(Rest, Key, Seen, Count, Key, Acc, MaxCollect);
        false ->
            NewSeen = Seen#{DocId => true},
            NewCount = Count + 1,
            NewAcc = [DocId | Acc],
            case NewCount >= MaxCollect of
                true ->
                    {stop, {NewSeen, NewCount, Key, NewAcc}};
                false ->
                    process_compare_docids_chunked(Rest, Key, NewSeen, NewCount, Key, NewAcc, MaxCollect)
            end
    end.

%% @private Execute chunked multi-index query (intersection of posting lists)
execute_multi_index_chunked(StoreRef, DbName, Conditions, Plan, ChunkSize, StartKey, Snapshot) ->
    #query_plan{offset = Offset, include_docs = IncludeDocs, decoder_fun = DecoderFun} = Plan,

    %% Get snapshot if not already present
    ActualSnapshot = case Snapshot of
        undefined ->
            {ok, S} = barrel_store_rocksdb:snapshot(StoreRef),
            S;
        S -> S
    end,

    %% Collect DocIds - use lazy intersection when starting from scratch
    %% and have multiple conditions (lazy is O(result_count) vs O(total_matching))
    MaxToCollect = ChunkSize + Offset + 1,  %% ChunkSize + 1 for has_more detection + offset
    IntersectedDocIds = case {StartKey, length(Conditions) > 1} of
        {undefined, true} ->
            %% Fresh query with multiple conditions - use lazy intersection
            %% This is much faster when result set is larger than chunk size
            intersect_lazy_with_limit(StoreRef, DbName, Conditions, MaxToCollect);
        _ ->
            %% Continuation - need full sorted list for proper cursor position
            %% Or single condition - no benefit from lazy
            intersect_docid_sets_v2(StoreRef, DbName, Conditions)
    end,

    %% Apply pagination using StartKey
    PaginatedDocIds = case StartKey of
        undefined -> IntersectedDocIds;
        Key ->
            %% Skip until we pass the start key
            lists:dropwhile(fun(DocId) -> DocId =< Key end, IntersectedDocIds)
    end,

    %% Collect ChunkSize + 1 for has_more detection
    MaxCollect = ChunkSize + 1,
    {CollectedDocIds, HasMore} = case length(PaginatedDocIds) > MaxCollect of
        true ->
            {lists:sublist(PaginatedDocIds, MaxCollect), true};
        false ->
            {PaginatedDocIds, false}
    end,

    %% Build results
    FinalDocIds0 = case HasMore of
        true -> lists:droplast(CollectedDocIds);
        false -> CollectedDocIds
    end,
    FinalDocIds = apply_offset_limit(FinalDocIds0, Offset, ChunkSize),

    %% Fetch docs if include_docs is true
    FinalResults = maybe_fetch_docs_chunked(DbName, FinalDocIds, IncludeDocs, DecoderFun),

    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    case HasMore of
        true ->
            LastDocId = lists:last(CollectedDocIds),
            Token = barrel_query_cursor:create(
                StoreRef, DbName, multi_index, LastDocId, ActualSnapshot),
            {ok, FinalResults, #{last_seq => LastSeq, has_more => true, continuation => Token}};
        false ->
            maybe_release_snapshot(ActualSnapshot),
            {ok, FinalResults, #{last_seq => LastSeq, has_more => false}}
    end.

%% @private Collect DocIds for a single condition using index
collect_condition_docids(StoreRef, DbName, {path, Path, Value}) ->
    FullPath = Path ++ [Value],
    barrel_ars_index:get_posting_list(StoreRef, DbName, FullPath);
collect_condition_docids(StoreRef, DbName, {exists, Path}) ->
    %% Collect all DocIds that have this path
    barrel_ars_index:fold_path(StoreRef, DbName, Path,
        fun({_P, DocId}, Acc) -> {ok, [DocId | Acc]} end, [], short_range);
collect_condition_docids(StoreRef, DbName, {prefix, Path, Prefix}) ->
    barrel_ars_index:fold_prefix(StoreRef, DbName, Path, Prefix,
        fun({_P, DocId}, Acc) -> {ok, [DocId | Acc]} end, [], short_range);
collect_condition_docids(StoreRef, DbName, {compare, Path, Op, Value}) ->
    barrel_ars_index:fold_path_values_compare(StoreRef, DbName, Path, Op, Value,
        fun({_P, DocId}, Acc) -> {ok, [DocId | Acc]} end, []).

%% @private Intersect multiple DocId sets using optimized intersection
%% First collects from most selective condition, then filters against others
intersect_docid_sets(StoreRef, DbName, Conditions) ->
    %% Find the most selective condition (smallest posting list)
    {SmallestCond, _SmallestPath} = find_most_selective_condition(StoreRef, DbName, Conditions),

    %% Get docids from the smallest posting list
    DocIds = collect_condition_docids(StoreRef, DbName, SmallestCond),

    %% Verify remaining conditions using posting lists
    OtherConditions = Conditions -- [SmallestCond],
    verify_conditions(StoreRef, DbName, DocIds, OtherConditions).

%% @private Find the most selective condition based on cardinality
find_most_selective_condition(StoreRef, DbName, Conditions) ->
    ConditionsWithCard = lists:map(fun(Cond) ->
        {Card, Path} = case Cond of
            {path, P, V} ->
                FullPath = P ++ [V],
                case barrel_ars_index:get_path_cardinality(StoreRef, DbName, FullPath) of
                    {ok, C} -> {C, FullPath};
                    _ -> {999999999, FullPath}
                end;
            {exists, P} ->
                case barrel_ars_index:get_path_cardinality(StoreRef, DbName, P) of
                    {ok, C} -> {C, P};
                    _ -> {999999999, P}
                end;
            {compare, P, Op, Value} ->
                %% Estimate compare cardinality by sampling the index
                %% For '>' or '<', estimate roughly half the values match
                %% This is a heuristic - actual count would require full scan
                case barrel_ars_index:get_path_cardinality(StoreRef, DbName, P) of
                    {ok, TotalCard} ->
                        %% Assume 50% selectivity for range queries
                        EstCard = case Op of
                            '>' -> TotalCard div 2;
                            '<' -> TotalCard div 2;
                            '>=' -> (TotalCard div 2) + 1;
                            '=<' -> (TotalCard div 2) + 1
                        end,
                        {EstCard, P ++ [Value]};
                    _ ->
                        {999999998, P}
                end;
            {prefix, P, Prefix} ->
                %% Estimate prefix matches ~10% of path values
                case barrel_ars_index:get_path_cardinality(StoreRef, DbName, P) of
                    {ok, TotalCard} -> {TotalCard div 10, P ++ [Prefix]};
                    _ -> {999999997, P}
                end;
            _ ->
                {999999999, []}
        end,
        {Card, Path, Cond}
    end, Conditions),
    [{_, BestPath, BestCond} | _] = lists:sort(ConditionsWithCard),
    {BestCond, BestPath}.

%% @private Verify that docids match remaining conditions using posting lists
%% Uses point lookups for equality conditions (O(1) per docid) and set membership
%% for other condition types (O(m) to collect, then O(1) lookups).
verify_conditions(_StoreRef, _DbName, DocIds, []) ->
    lists:usort(DocIds);
verify_conditions(StoreRef, DbName, DocIds, [{path, Path, Value} | Rest]) ->
    %% Use point lookup for equality - O(1) per docid via value-first index
    FilteredDocIds = lists:filter(fun(DocId) ->
        barrel_ars_index:docid_has_value(StoreRef, DbName, Path, Value, DocId)
    end, DocIds),
    verify_conditions(StoreRef, DbName, FilteredDocIds, Rest);
verify_conditions(StoreRef, DbName, DocIds, [Cond | Rest]) ->
    %% For other conditions (compare, exists, prefix), use set-based approach
    CondDocIds = sets:from_list(collect_condition_docids(StoreRef, DbName, Cond)),
    FilteredDocIds = lists:filter(fun(DocId) ->
        sets:is_element(DocId, CondDocIds)
    end, DocIds),
    verify_conditions(StoreRef, DbName, FilteredDocIds, Rest).

%%====================================================================
%% Native Postings V2 Query Execution (using rocksdb 2.5.0 postings API)
%%====================================================================

%% @private Intersect DocId sets using native posting list intersection.
%% For equality conditions, uses native postings_intersect_all for O(min(n,m)) performance.
%% For non-equality conditions, uses cardinality-based selection for optimal iteration order.
intersect_docid_sets_v2(StoreRef, DbName, Conditions) ->
    %% Separate equality conditions (can use native intersection) from others
    {EqConds, OtherConds} = lists:partition(fun
        ({path, _, _}) -> true;
        (_) -> false
    end, Conditions),

    case {EqConds, OtherConds} of
        {[], _} ->
            %% No equality conditions - fall back to original approach
            intersect_docid_sets(StoreRef, DbName, Conditions);
        {_, []} ->
            %% Only equality conditions - use native posting intersection
            intersect_equality_conditions(StoreRef, DbName, EqConds);
        {_, _} ->
            %% Mixed conditions - choose best starting condition based on cardinality
            intersect_mixed_conditions_v2(StoreRef, DbName, EqConds, OtherConds)
    end.

%% @private Intersect multiple equality conditions using native posting list API.
intersect_equality_conditions(StoreRef, DbName, EqConds) ->
    PostingBins = lists:filtermap(fun({path, Path, Value}) ->
        FullPath = Path ++ [Value],
        case barrel_ars_index:get_posting_list_binary(StoreRef, DbName, FullPath) of
            {ok, Bin} -> {true, Bin};
            not_found -> false;
            {error, _} -> false
        end
    end, EqConds),

    case PostingBins of
        [] ->
            [];
        [Single] ->
            {ok, Postings} = barrel_postings:open(Single),
            barrel_postings:keys(Postings);
        _ ->
            case barrel_postings:intersect_all(PostingBins) of
                {ok, ResultPostings} ->
                    barrel_postings:keys(ResultPostings);
                {error, _} ->
                    []
            end
    end.

%% @private Intersect mixed equality + non-equality conditions.
%% Uses cardinality estimation to choose the most selective starting condition.
intersect_mixed_conditions_v2(StoreRef, DbName, EqConds, OtherConds) ->
    %% Estimate cardinality for all conditions
    AllConds = EqConds ++ OtherConds,
    CondsWithCard = estimate_all_cardinalities(StoreRef, DbName, AllConds),

    %% Sort by estimated cardinality (smallest first)
    Sorted = lists:sort(CondsWithCard),
    %% Debug: io:format("Sorted conditions: ~p~n", [Sorted]),
    [{_BestCard, BestCond} | RestWithCard] = Sorted,
    RestConds = [C || {_, C} <- RestWithCard],

    %% Collect DocIds from the most selective condition
    DocIds = collect_condition_docids(StoreRef, DbName, BestCond),
    %% Verify remaining conditions
    verify_conditions_v2(StoreRef, DbName, DocIds, RestConds).

%% @private Estimate cardinality for all conditions.
estimate_all_cardinalities(StoreRef, DbName, Conditions) ->
    lists:map(fun(Cond) ->
        Card = estimate_condition_cardinality(StoreRef, DbName, Cond),
        {Card, Cond}
    end, Conditions).

%% @private Estimate cardinality for a single condition.
estimate_condition_cardinality(StoreRef, DbName, {path, Path, Value}) ->
    FullPath = Path ++ [Value],
    case barrel_ars_index:get_path_cardinality(StoreRef, DbName, FullPath) of
        {ok, C} -> C;
        _ -> 999999999
    end;
estimate_condition_cardinality(StoreRef, DbName, {compare, Path, Op, _Value}) ->
    %% Estimate compare cardinality - assume 50% selectivity for range queries
    case barrel_ars_index:get_path_cardinality(StoreRef, DbName, Path) of
        {ok, TotalCard} ->
            case Op of
                '>' -> TotalCard div 2;
                '<' -> TotalCard div 2;
                '>=' -> (TotalCard div 2) + 1;
                '=<' -> (TotalCard div 2) + 1;
                _ -> TotalCard
            end;
        _ -> 999999998
    end;
estimate_condition_cardinality(StoreRef, DbName, {exists, Path}) ->
    case barrel_ars_index:get_path_cardinality(StoreRef, DbName, Path) of
        {ok, C} -> C;
        _ -> 999999999
    end;
estimate_condition_cardinality(StoreRef, DbName, {prefix, Path, _Prefix}) ->
    %% Estimate prefix matches ~10% of path values
    case barrel_ars_index:get_path_cardinality(StoreRef, DbName, Path) of
        {ok, TotalCard} -> TotalCard div 10;
        _ -> 999999997
    end;
estimate_condition_cardinality(_, _, _) ->
    999999999.

%% @private Verify conditions using batch lookups for efficiency.
%% Uses multi_key_exists for batch verification instead of individual lookups.
verify_conditions_v2(_StoreRef, _DbName, DocIds, []) ->
    lists:usort(DocIds);
verify_conditions_v2(StoreRef, DbName, DocIds, [{path, Path, Value} | Rest]) ->
    %% Use batch key existence check - much faster than individual lookups
    %% First deduplicate to avoid redundant checks
    UniqueDocIds = lists:usort(DocIds),
    FilteredDocIds = barrel_ars_index:filter_docids_by_value(
        StoreRef, DbName, Path, Value, UniqueDocIds),
    verify_conditions_v2(StoreRef, DbName, FilteredDocIds, Rest);
verify_conditions_v2(StoreRef, DbName, DocIds, [Cond | Rest]) ->
    %% For non-equality conditions (compare, exists, prefix), use set-based approach
    %% These conditions need to iterate over ranges, so we can't use single posting list lookup
    CondDocIds = sets:from_list(collect_condition_docids(StoreRef, DbName, Cond)),
    FilteredDocIds = lists:filter(fun(DocId) ->
        sets:is_element(DocId, CondDocIds)
    end, DocIds),
    verify_conditions_v2(StoreRef, DbName, FilteredDocIds, Rest).

%% @private Release snapshot if present
maybe_release_snapshot(Snapshot) ->
    barrel_store_rocksdb:safe_release_snapshot(Snapshot).

%% @private Convert cursor record to map for pattern matching
cursor_to_map(Cursor) ->
    %% Cursor is a record, extract fields
    #{
        snapshot => element(5, Cursor),    %% #cursor.snapshot
        last_key => element(6, Cursor),    %% #cursor.last_key
        query_type => element(7, Cursor)   %% #cursor.query_type
    }.

%% @private Validate cursor matches current query plan
validate_cursor_for_plan(_QueryType, _Plan) ->
    %% For now, just accept - could add stricter validation
    ok.

%% @doc Execute query with a snapshot for read consistency
execute_with_snapshot(StoreRef, DbName, Plan, Snapshot) ->
    #query_plan{order = Order, limit = Limit, conditions = Conditions} = Plan,

    %% Check if we can use indexed order for ORDER BY + LIMIT optimization
    %% Most beneficial when: no filter conditions, small limit, large dataset
    case can_use_indexed_order(Order, Limit) of
        {true, OrderPath, Dir} ->
            case should_use_indexed_order(OrderPath, Conditions) of
                true ->
                    execute_with_indexed_order(StoreRef, DbName, OrderPath, Dir, Plan, Snapshot);
                false ->
                    execute_by_strategy(StoreRef, DbName, Plan, Snapshot)
            end;
        false ->
            execute_by_strategy(StoreRef, DbName, Plan, Snapshot)
    end.

%% @doc Execute based on query strategy
execute_by_strategy(StoreRef, DbName, Plan, Snapshot) ->
    case Plan#query_plan.strategy of
        index_seek ->
            execute_index_seek(StoreRef, DbName, Plan, Snapshot);
        index_scan ->
            execute_index_scan(StoreRef, DbName, Plan, Snapshot);
        multi_index ->
            execute_multi_index(StoreRef, DbName, Plan, Snapshot);
        full_scan ->
            execute_full_scan(StoreRef, DbName, Plan, Snapshot)
    end.

%% @doc Decide if indexed order should be used
%% The Top-K optimization is most beneficial when:
%% - No filter conditions (pure ORDER BY + LIMIT)
%% - Conditions can be checked via index (lazy evaluation)
%% This changes complexity from O(matching_docs) to O(limit)
should_use_indexed_order(_OrderPath, []) ->
    %% No conditions - pure ORDER BY + LIMIT, definitely use indexed order
    %% This is the main use case: "get latest N" without filtering
    true;
should_use_indexed_order(_OrderPath, Conditions) ->
    %% With filter conditions, use indexed order if all conditions
    %% can be checked via index lookups (lazy evaluation)
    can_lazy_check_conditions(Conditions).

%% @doc Check if all conditions can be lazily evaluated via index.
%% Returns true if we can avoid body fetch for condition checking.
can_lazy_check_conditions([]) ->
    true;
can_lazy_check_conditions([Condition | Rest]) ->
    case can_lazy_check_condition(Condition) of
        true -> can_lazy_check_conditions(Rest);
        false -> false
    end.

%% @private Check if a single condition can be lazily evaluated via O(1) index lookup.
%% Only allow conditions that use docid_has_value (point lookup), not doc_paths load.
can_lazy_check_condition({path, _Path, Value}) when not is_atom(Value) ->
    %% Equality check - uses docid_has_value (O(1) point lookup)
    %% Logic variables need body fetch for binding
    true;
can_lazy_check_condition({path, _Path, Value}) ->
    %% Logic variables like '?Var' need body fetch
    not is_logic_var(Value);
can_lazy_check_condition({compare, _Path, _Op, _Value}) ->
    %% Range check - docid_satisfies_compare loads doc_paths which is expensive
    %% Disable for now until we have a more efficient range index lookup
    false;
can_lazy_check_condition({exists, _Path}) ->
    %% Existence check - requires doc_paths load, expensive
    false;
can_lazy_check_condition({prefix, _Path, _Prefix}) ->
    %% Prefix check - requires doc_paths load, expensive
    false;
can_lazy_check_condition({'and', Conditions}) ->
    can_lazy_check_conditions(Conditions);
can_lazy_check_condition({'or', Conditions}) ->
    can_lazy_check_conditions(Conditions);
can_lazy_check_condition(_) ->
    %% Other conditions (in, contains, regex, not, etc.) need body fetch
    false.

%% @doc Check if a document matches a compiled query plan.
%% This is useful for filtering documents in-memory without index access.
-spec match(query_plan(), map()) -> boolean().
match(#query_plan{conditions = Conditions, bindings = Bindings}, Doc)
  when is_map(Doc) ->
    case matches_conditions(Doc, Conditions, Bindings) of
        {true, _BoundVars} -> true;
        false -> false
    end.

%% @doc Check if a document matches a list of conditions.
%% Simpler API for filtering without needing a compiled query plan.
-spec matches(map(), list()) -> boolean().
matches(Doc, Conditions) when is_map(Doc), is_list(Conditions) ->
    case matches_conditions(Doc, Conditions, #{}) of
        {true, _BoundVars} -> true;
        false -> false
    end.

%% @doc Explain a query plan (for debugging/optimization).
%% Returns a map describing the execution strategy.
-spec explain(query_plan()) -> map().
explain(#query_plan{} = Plan) ->
    #{
        strategy => Plan#query_plan.strategy,
        conditions => Plan#query_plan.conditions,
        bindings => Plan#query_plan.bindings,
        projections => Plan#query_plan.projections,
        order => Plan#query_plan.order,
        limit => Plan#query_plan.limit,
        offset => Plan#query_plan.offset,
        include_docs => Plan#query_plan.include_docs
    }.

%% @doc Extract all paths referenced in a query plan.
%% Used for subscription optimization - only evaluate query when
%% a change affects one of these paths.
%% Returns MQTT-style path patterns for use with barrel_sub.
-spec extract_paths(query_plan()) -> [binary()].
extract_paths(#query_plan{conditions = Conditions}) ->
    Paths = extract_paths_from_conditions(Conditions, []),
    UniquePathPatterns = lists:usort([path_to_pattern(P) || P <- Paths]),
    UniquePathPatterns.

%% @private Extract paths from conditions recursively
extract_paths_from_conditions([], Acc) ->
    Acc;
extract_paths_from_conditions([{path, Path, _} | Rest], Acc) ->
    extract_paths_from_conditions(Rest, [Path | Acc]);
extract_paths_from_conditions([{compare, Path, _, _} | Rest], Acc) ->
    extract_paths_from_conditions(Rest, [Path | Acc]);
extract_paths_from_conditions([{'and', Nested} | Rest], Acc) ->
    NestedPaths = extract_paths_from_conditions(Nested, []),
    extract_paths_from_conditions(Rest, NestedPaths ++ Acc);
extract_paths_from_conditions([{'or', Nested} | Rest], Acc) ->
    NestedPaths = extract_paths_from_conditions(Nested, []),
    extract_paths_from_conditions(Rest, NestedPaths ++ Acc);
extract_paths_from_conditions([{'not', Condition} | Rest], Acc) ->
    NestedPaths = extract_paths_from_conditions([Condition], []),
    extract_paths_from_conditions(Rest, NestedPaths ++ Acc);
extract_paths_from_conditions([{in, Path, _} | Rest], Acc) ->
    extract_paths_from_conditions(Rest, [Path | Acc]);
extract_paths_from_conditions([{contains, Path, _} | Rest], Acc) ->
    extract_paths_from_conditions(Rest, [Path | Acc]);
extract_paths_from_conditions([{exists, Path} | Rest], Acc) ->
    extract_paths_from_conditions(Rest, [Path | Acc]);
extract_paths_from_conditions([{missing, Path} | Rest], Acc) ->
    extract_paths_from_conditions(Rest, [Path | Acc]);
extract_paths_from_conditions([{regex, Path, _} | Rest], Acc) ->
    extract_paths_from_conditions(Rest, [Path | Acc]);
extract_paths_from_conditions([{prefix, Path, _} | Rest], Acc) ->
    extract_paths_from_conditions(Rest, [Path | Acc]);
extract_paths_from_conditions([_ | Rest], Acc) ->
    extract_paths_from_conditions(Rest, Acc).

%% @private Convert a path list to an MQTT-style pattern with # wildcard.
%% This allows matching any value at the path.
%% Example: `[&lt;&lt;"type"&gt;&gt;]' to `&lt;&lt;"type/#"&gt;&gt;'
path_to_pattern([]) ->
    <<"#">>;
path_to_pattern(Path) ->
    Parts = [to_bin(P) || P <- Path],
    BasePath = iolist_to_binary(lists:join(<<"/">>, Parts)),
    <<BasePath/binary, "/#">>.

%%====================================================================
%% Internal - Compilation
%%====================================================================

do_compile(Spec) ->
    case compile_id_scan(Spec) of
        {error, _} = Err -> Err;
        IdScan -> do_compile(Spec, IdScan)
    end.

%% @private Parse id_prefix / id_range spec options into an id_scan plan field.
compile_id_scan(Spec) ->
    case {maps:get(id_prefix, Spec, undefined), maps:get(id_range, Spec, undefined)} of
        {undefined, undefined} ->
            undefined;
        {P, undefined} when is_binary(P) ->
            {prefix, P};
        {undefined, {S, E}} when (is_binary(S) orelse S =:= undefined),
                                 (is_binary(E) orelse E =:= undefined) ->
            {range, S, E};
        _ ->
            {error, {invalid_id_scan, maps:with([id_prefix, id_range], Spec)}}
    end.

do_compile(Spec, IdScan) ->
    Where = maps:get(where, Spec, []),
    %% id_scan is a standalone primary-key scan; it does not combine with where.
    case {IdScan, Where} of
        {Scan, [_ | _]} when Scan =/= undefined ->
            {error, {id_scan_with_where, Where}};
        _ ->
            do_compile(Spec, IdScan, Where)
    end.

do_compile(Spec, IdScan, Where) ->
    Flat = maps:get(flat, Spec, false),
    Select = maps:get(select, Spec, ['*']),
    OrderBy = maps:get(order_by, Spec, undefined),
    Limit = maps:get(limit, Spec, undefined),
    Offset = maps:get(offset, Spec, 0),
    IncludeDocs = maps:get(include_docs, Spec, false),
    %% Default doc_format is 'map' for backwards compatibility
    DocFormat = maps:get(doc_format, Spec, map),

    %% Convert doc_format to decoder_fun at compile time for simpler execution
    DecoderFun = case maps:get(decoder_fun, Spec, undefined) of
        undefined -> make_decoder_fun(DocFormat);
        CustomFun -> CustomFun
    end,

    %% Normalize conditions
    NormalizedConditions = [normalize_condition(C) || C <- Where],

    %% Extract variable bindings from conditions
    Bindings = extract_bindings(NormalizedConditions),

    %% Normalize projections
    Projections = normalize_projections(Select),

    %% Normalize order specification
    Order = normalize_order(OrderBy),

    %% Determine execution strategy
    Strategy = determine_strategy(NormalizedConditions),

    Plan = #query_plan{
        conditions = NormalizedConditions,
        bindings = Bindings,
        projections = Projections,
        order = Order,
        limit = Limit,
        offset = Offset,
        include_docs = IncludeDocs,
        doc_format = DocFormat,
        decoder_fun = DecoderFun,
        strategy = Strategy,
        flat = Flat,
        id_scan = IdScan
    },
    {ok, Plan}.

%% @doc Create a decoder function from doc_format option
%% Uses decode_any/1 which handles both indexed CBOR and plain CBOR
-spec make_decoder_fun(binary | map | json) -> fun((binary()) -> term()).
make_decoder_fun(binary) -> fun(Bin) -> Bin end;
make_decoder_fun(map) -> fun barrel_docdb_codec_cbor:decode_any/1;
make_decoder_fun(json) -> fun(Bin) ->
    Map = barrel_docdb_codec_cbor:decode_any(Bin),
    json:encode(Map)
end.

%% @doc Normalize a condition to canonical form
normalize_condition({path, Path, Value}) when is_list(Path) ->
    {path, Path, Value};
normalize_condition({compare, Path, Op, Value}) when is_list(Path) ->
    case lists:member(Op, ['>', '<', '>=', '=<', '=/=', '==']) of
        true -> {compare, Path, Op, Value};
        false -> {error, {invalid_operator, Op}}
    end;
normalize_condition({'and', Conditions}) when is_list(Conditions) ->
    {'and', [normalize_condition(C) || C <- Conditions]};
normalize_condition({'or', Conditions}) when is_list(Conditions) ->
    {'or', [normalize_condition(C) || C <- Conditions]};
normalize_condition({'not', Condition}) ->
    {'not', normalize_condition(Condition)};
normalize_condition({in, Path, Values}) when is_list(Path), is_list(Values) ->
    {in, Path, Values};
normalize_condition({contains, Path, Value}) when is_list(Path) ->
    {contains, Path, Value};
normalize_condition({exists, Path}) when is_list(Path) ->
    {exists, Path};
normalize_condition({missing, Path}) when is_list(Path) ->
    {missing, Path};
normalize_condition({regex, Path, Pattern}) when is_list(Path), is_binary(Pattern) ->
    {regex, Path, Pattern};
normalize_condition({prefix, Path, Prefix}) when is_list(Path), is_binary(Prefix) ->
    {prefix, Path, Prefix};
normalize_condition(Other) ->
    {error, {invalid_condition, Other}}.

%% @doc Extract variable bindings from conditions
extract_bindings(Conditions) ->
    extract_bindings(Conditions, #{}).

extract_bindings([], Acc) ->
    Acc;
extract_bindings([{path, Path, Value} | Rest], Acc) ->
    case is_logic_var(Value) of
        true ->
            extract_bindings(Rest, Acc#{Value => Path});
        false ->
            extract_bindings(Rest, Acc)
    end;
extract_bindings([{compare, Path, _Op, Value} | Rest], Acc) ->
    case is_logic_var(Value) of
        true ->
            extract_bindings(Rest, Acc#{Value => Path});
        false ->
            extract_bindings(Rest, Acc)
    end;
extract_bindings([{'and', Nested} | Rest], Acc) ->
    NestedBindings = extract_bindings(Nested, Acc),
    extract_bindings(Rest, NestedBindings);
extract_bindings([{'or', Branches} | Rest], Acc) ->
    %% For OR, we can only safely use bindings that appear in ALL branches
    %% Extract bindings from each branch and intersect them
    case Branches of
        [] ->
            extract_bindings(Rest, Acc);
        [First | RestBranches] ->
            FirstBindings = extract_bindings([First], #{}),
            CommonBindings = lists:foldl(
                fun(Branch, CommonAcc) ->
                    BranchBindings = extract_bindings([Branch], #{}),
                    maps:filter(
                        fun(Var, Path) ->
                            maps:get(Var, BranchBindings, undefined) =:= Path
                        end,
                        CommonAcc
                    )
                end,
                FirstBindings,
                RestBranches
            ),
            extract_bindings(Rest, maps:merge(Acc, CommonBindings))
    end;
extract_bindings([_ | Rest], Acc) ->
    extract_bindings(Rest, Acc).

%% @doc Check if a value is a logic variable (atom starting with '?')
-spec is_logic_var(term()) -> boolean().
is_logic_var(Atom) when is_atom(Atom) ->
    case atom_to_list(Atom) of
        [$? | _] -> true;
        _ -> false
    end;
is_logic_var(_) ->
    false.

%% @doc Normalize projections
normalize_projections(Select) when is_list(Select) ->
    Select;
normalize_projections(Single) ->
    [Single].

%% @doc Normalize order specification
normalize_order(undefined) ->
    [];
normalize_order(Spec) when is_atom(Spec); is_list(Spec), is_binary(hd(Spec)) ->
    [{Spec, asc}];
normalize_order({Spec, Dir}) when Dir =:= asc; Dir =:= desc ->
    [{Spec, Dir}];
normalize_order(Specs) when is_list(Specs) ->
    [normalize_order_item(S) || S <- Specs].

normalize_order_item({Spec, Dir}) when Dir =:= asc; Dir =:= desc ->
    {Spec, Dir};
normalize_order_item(Spec) ->
    {Spec, asc}.

%% @doc Determine the best execution strategy for the query
determine_strategy(Conditions) ->
    %% Analyze conditions to find indexed access paths
    case find_index_conditions(Conditions) of
        [] ->
            %% No index-friendly conditions - full scan
            full_scan;
        [_Single] ->
            %% Single index condition
            case has_range_condition(Conditions) of
                true -> index_scan;
                false -> index_seek
            end;
        _Multiple ->
            %% Multiple index conditions - intersection
            multi_index
    end.

%% @doc Find conditions that can use the path index
find_index_conditions(Conditions) ->
    find_index_conditions(Conditions, []).

find_index_conditions([], Acc) ->
    lists:reverse(Acc);
find_index_conditions([{path, Path, Value} | Rest], Acc) ->
    case is_logic_var(Value) of
        true ->
            %% Variable binding - can't use for initial index lookup
            find_index_conditions(Rest, Acc);
        false ->
            %% Concrete value - good for index
            find_index_conditions(Rest, [{path, Path, Value} | Acc])
    end;
find_index_conditions([{compare, Path, _Op, _Value} | Rest], Acc) ->
    %% Range comparison - can use index scan
    find_index_conditions(Rest, [{compare, Path} | Acc]);
find_index_conditions([{prefix, Path, _Prefix} | Rest], Acc) ->
    %% Prefix match - can use index scan
    find_index_conditions(Rest, [{prefix, Path} | Acc]);
find_index_conditions([{exists, Path} | Rest], Acc) ->
    %% Exists check - can use index scan on path prefix
    find_index_conditions(Rest, [{exists, Path} | Acc]);
find_index_conditions([{'and', Nested} | Rest], Acc) ->
    NestedIndexable = find_index_conditions(Nested),
    find_index_conditions(Rest, NestedIndexable ++ Acc);
find_index_conditions([_ | Rest], Acc) ->
    find_index_conditions(Rest, Acc).

%% @doc Check if conditions include range comparisons
has_range_condition([]) -> false;
has_range_condition([{compare, _, Op, _} | _]) when Op =/= '==' -> true;
has_range_condition([{prefix, _, _} | _]) -> true;
has_range_condition([{exists, _} | _]) -> true;
has_range_condition([{'and', Nested} | Rest]) ->
    has_range_condition(Nested) orelse has_range_condition(Rest);
has_range_condition([{'or', Nested} | Rest]) ->
    has_range_condition(Nested) orelse has_range_condition(Rest);
has_range_condition([_ | Rest]) ->
    has_range_condition(Rest).

%%====================================================================
%% Internal - Validation
%%====================================================================

validate_conditions([]) ->
    ok;
validate_conditions([Condition | Rest]) ->
    case validate_condition(Condition) of
        ok -> validate_conditions(Rest);
        {error, _} = Error -> Error
    end.

validate_condition({path, Path, _Value}) ->
    validate_path(Path);
validate_condition({compare, Path, Op, _Value}) ->
    case lists:member(Op, ['>', '<', '>=', '=<', '=/=', '==']) of
        true -> validate_path(Path);
        false -> {error, {invalid_operator, Op}}
    end;
validate_condition({'and', Conditions}) when is_list(Conditions) ->
    validate_conditions(Conditions);
validate_condition({'or', Conditions}) when is_list(Conditions) ->
    validate_conditions(Conditions);
validate_condition({'not', Condition}) ->
    validate_condition(Condition);
validate_condition({in, Path, Values}) when is_list(Values) ->
    validate_path(Path);
validate_condition({in, _Path, _}) ->
    {error, {invalid_in_values, must_be_list}};
validate_condition({contains, Path, _Value}) ->
    validate_path(Path);
validate_condition({exists, Path}) ->
    validate_path(Path);
validate_condition({missing, Path}) ->
    validate_path(Path);
validate_condition({regex, Path, Pattern}) when is_binary(Pattern) ->
    case re:compile(Pattern) of
        {ok, _} -> validate_path(Path);
        {error, Reason} -> {error, {invalid_regex, Reason}}
    end;
validate_condition({regex, _Path, _}) ->
    {error, {invalid_regex, must_be_binary}};
validate_condition({prefix, Path, Prefix}) when is_binary(Prefix) ->
    validate_path(Path);
validate_condition({prefix, _Path, _}) ->
    {error, {invalid_prefix, must_be_binary}};
validate_condition(Other) ->
    {error, {invalid_condition, Other}}.

validate_path(Path) when is_list(Path) ->
    case lists:all(fun is_valid_path_component/1, Path) of
        true -> ok;
        false -> {error, {invalid_path, Path}}
    end;
validate_path(Path) ->
    {error, {invalid_path, Path, must_be_list}}.

is_valid_path_component(C) when is_binary(C) -> true;
is_valid_path_component(C) when is_integer(C), C >= 0 -> true;
is_valid_path_component('*') -> true;  % Wildcard for array any
is_valid_path_component(_) -> false.

%%====================================================================
%% Internal - Execution
%%====================================================================

%% @doc Select read profile based on limit and cardinality.
%% - point: Small result sets (limit =&lt; 10), keep blocks in cache
%% - short_range: Medium scans (limit =&lt; 200 or cardinality =&lt; 200), auto-readahead
%% - long_scan: Large/unbounded scans, prefetch aggressively, avoid cache pollution
-spec select_read_profile(undefined | pos_integer(), non_neg_integer()) -> read_profile().
select_read_profile(Limit, _Cardinality) when is_integer(Limit), Limit =< 10 ->
    point;
select_read_profile(Limit, _Cardinality) when is_integer(Limit), Limit =< 200 ->
    short_range;
select_read_profile(_Limit, Cardinality) when Cardinality =< 200 ->
    short_range;
select_read_profile(_, _) ->
    long_scan.

%% @doc Select read profile based on limit only (for collectors)
select_read_profile(MaxCount) when is_integer(MaxCount), MaxCount =< 10 ->
    point;
select_read_profile(MaxCount) when is_integer(MaxCount), MaxCount =< 200 ->
    short_range;
select_read_profile(_) ->
    long_scan.

%% @doc Execute using direct index key lookup (fastest)
%% Uses prefix bloom filters for O(1) skip of non-matching SST blocks.
execute_index_seek(StoreRef, DbName, Plan, Snapshot) ->
    #query_plan{conditions = Conditions, include_docs = IncludeDocs,
                projections = Projections} = Plan,

    %% Find the first equality condition to use for index lookup
    case find_first_equality(Conditions) of
        {ok, {path, Path, Value} = IndexCond} ->
            FullPath = Path ++ [Value],

            %% Compute remaining conditions (index condition already satisfied by iteration)
            RemainingConds = Conditions -- [IndexCond],

            %% Check for pure equality: single condition, no doc body needed
            NeedsBody = IncludeDocs orelse needs_body_for_projection(Projections),
            case {RemainingConds, NeedsBody} of
                {[], false} ->
                    %% Pure equality - return DocIds directly from posting list
                    execute_pure_equality(StoreRef, DbName, Path, Value, Plan, Snapshot);
                _ ->
                    %% Need doc fetch - original path
                    execute_index_seek_with_fetch(StoreRef, DbName, FullPath, RemainingConds, Plan, Snapshot)
            end;
        not_found ->
            %% Fallback to scan
            execute_index_scan(StoreRef, DbName, Plan, Snapshot)
    end.

%% @doc Execute index seek that requires document fetching
execute_index_seek_with_fetch(StoreRef, DbName, FullPath, RemainingConds, Plan, Snapshot) ->
    #query_plan{order = Order, limit = Limit, offset = Offset} = Plan,

    %% O(1) cardinality check - skip iteration if no matches
    Cardinality = case barrel_ars_index:get_path_cardinality(StoreRef, DbName, FullPath) of
        {ok, C} -> C;
        {error, _} -> 1  %% Assume at least 1 if error
    end,
    case Cardinality of
        0 ->
            %% No matches - skip iteration entirely
            filter_and_project(StoreRef, DbName, [], Plan, Snapshot);
        _ ->
            %% Index condition already satisfied - use remaining conditions only
            FilterPlan = Plan#query_plan{conditions = RemainingConds},
            case Limit of
                undefined ->
                    %% Unbounded query - use adaptive chunked execution
                    execute_index_seek_chunked(StoreRef, DbName, FullPath, FilterPlan, Snapshot);
                _ when is_integer(Limit) ->
                    %% Limited query - check streaming vs batch
                    case should_use_streaming(Limit, Cardinality) of
                        true ->
                            %% Streaming: fetch/match documents one-by-one
                            execute_index_seek_streaming(StoreRef, DbName, FullPath, FilterPlan, Snapshot);
                        false ->
                            %% Batch: collect DocIds then fetch/filter
                            %% PROFILING: Index iteration
                            T0 = erlang:monotonic_time(microsecond),
                            EarlyLimitResult = can_use_early_limit(Order, RemainingConds, Limit),
                            DocIds = case EarlyLimitResult of
                                {true, MaxCollect} ->
                                    collect_docids_for_path_limited(StoreRef, DbName, FullPath, MaxCollect + Offset);
                                false ->
                                    collect_docids_for_path(StoreRef, DbName, FullPath)
                            end,
                            T1 = erlang:monotonic_time(microsecond),
                            put(profile_index_iter, pdict_get(profile_index_iter, 0) + (T1 - T0)),
                            put(profile_doc_count, length(DocIds)),
                            filter_and_project(StoreRef, DbName, DocIds, FilterPlan, Snapshot)
                    end
            end
    end.

%% @doc Execute index seek using streaming approach
%% Iterates index entries and fetches/matches documents one-by-one
%% Stops early when enough results are collected
%% Much faster than batch-fetch for small limits on high-cardinality indexes
execute_index_seek_streaming(StoreRef, DbName, FullPath, Plan, _Snapshot) ->
    #query_plan{
        conditions = Conditions,
        bindings = Bindings,
        projections = Projections,
        limit = Limit,
        offset = Offset,
        include_docs = IncludeDocs,
        decoder_fun = DecoderFun
    } = Plan,

    %% Calculate how many results we need
    %% Use 1.5x to account for deleted docs (less than batch approach)
    MaxCollect = round((Limit + Offset) * 1.5),

    %% Iterate index entries and fetch/match documents one-by-one
    %% Use point profile since streaming is for small limits
    {_Count, Results} = barrel_ars_index:fold_path_reverse(
        StoreRef, DbName, FullPath,
        fun({_Path, DocId}, {Count, Acc}) ->
            case Count >= MaxCollect of
                true ->
                    {stop, {Count, Acc}};
                false ->
                    %% Fetch and filter document
                    case fetch_and_match_doc(StoreRef, DbName, DocId, Conditions, Bindings, IncludeDocs) of
                        {ok, CborBin, Doc, BoundVars} ->
                            Result = project_result(CborBin, Doc, DocId, Projections, BoundVars, IncludeDocs, DecoderFun),
                            {ok, {Count + 1, [Result | Acc]}};
                        skip ->
                            {ok, {Count, Acc}}
                    end
            end
        end,
        {0, []},
        point
    ),

    %% Results are in reverse order due to prepend - reverse them
    OrderedResults = lists:reverse(Results),

    %% Apply offset and limit
    FinalResults = apply_offset_limit(OrderedResults, Offset, Limit),

    %% Get last sequence
    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    {ok, FinalResults, LastSeq}.

%% @doc Execute index seek with adaptive chunked processing for unbounded queries.
%% Processes index entries in chunks, batch-fetching each chunk for efficiency.
%% Chunk size adapts based on average document size to target ~1MB per batch.
execute_index_seek_chunked(StoreRef, DbName, FullPath, Plan, Snapshot) ->
    #query_plan{
        conditions = Conditions,
        bindings = Bindings,
        projections = Projections,
        order = Order,
        offset = Offset,
        include_docs = IncludeDocs,
        decoder_fun = DecoderFun
    } = Plan,

    %% Fold index in chunks, batch-fetch and filter each chunk
    %% Use long_scan profile for unbounded queries (prefetch, avoid cache pollution)
    {ResultChunks, _TotalBytes, _ChunkCount} = barrel_ars_index:fold_path_chunked(
        StoreRef, DbName, FullPath, ?INITIAL_CHUNK_SIZE,
        fun(DocIdChunk, {Acc, TotalBytes, ChunkCount}) ->
            {ChunkResults, ChunkBytes} = batch_fetch_and_filter_with_size(
                StoreRef, DbName, DocIdChunk,
                Conditions, Bindings, Projections, IncludeDocs, DecoderFun, Snapshot),

            %% Calculate new chunk size based on average doc size
            NewTotalBytes = TotalBytes + ChunkBytes,
            NewChunkCount = ChunkCount + 1,
            TotalDocs = length(DocIdChunk) * NewChunkCount,
            AvgDocSize = case TotalDocs > 0 of
                true -> NewTotalBytes div TotalDocs;
                false -> 0
            end,
            NewChunkSize = calculate_chunk_size(AvgDocSize),

            {ok, {[ChunkResults | Acc], NewTotalBytes, NewChunkCount}, NewChunkSize}
        end,
        {[], 0, 0},
        long_scan
    ),

    %% Flatten result chunks (they're in reverse order)
    FlatResults = lists:append(lists:reverse(ResultChunks)),

    %% Apply ordering if specified
    OrderedResults = apply_order(FlatResults, Order),

    %% Apply offset (no limit for unbounded queries)
    FinalResults = apply_offset_limit(OrderedResults, Offset, undefined),

    %% Get last sequence
    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    {ok, FinalResults, LastSeq}.

%% @doc Calculate optimal chunk size based on average doc size
%% Targets ~1MB of doc data per chunk for good cache utilization
calculate_chunk_size(AvgDocSize) when AvgDocSize > 0 ->
    Optimal = ?TARGET_CHUNK_BYTES div AvgDocSize,
    max(?MIN_CHUNK_SIZE, min(?MAX_CHUNK_SIZE, Optimal));
calculate_chunk_size(_) ->
    ?INITIAL_CHUNK_SIZE.

%% @doc Batch fetch with size tracking for adaptive chunking
batch_fetch_and_filter_with_size(StoreRef, DbName, DocIds, Conditions, Bindings, Projections, IncludeDocs, DecoderFun, Snapshot) ->
    Results = batch_fetch_and_filter(StoreRef, DbName, DocIds, Conditions, Bindings, Projections, IncludeDocs, DecoderFun, Snapshot),
    %% Estimate bytes from doc count (conservative ~500 bytes avg per doc)
    EstimatedBytes = length(DocIds) * 500,
    {Results, EstimatedBytes}.

%% @private Helper for process dictionary get with default
pdict_get(Key, Default) ->
    case erlang:get(Key) of
        undefined -> Default;
        Value -> Value
    end.

%% @doc Check if early limit optimization can be used
%% Returns {true, MaxToCollect} or false
can_use_early_limit([], [], Limit) when is_integer(Limit), Limit > 0 ->
    %% No ORDER BY and no remaining conditions - safe to limit early
    %% Collect limit + small buffer to account for deleted docs (reduced from 2x)
    {true, max(Limit + 5, round(Limit * 1.2))};
can_use_early_limit([], RemainingConds, Limit) when is_integer(Limit), Limit > 0, RemainingConds =/= [] ->
    %% No ORDER BY but has remaining conditions - over-collect to handle filter losses
    %% Use 3x multiplier to account for filtering (conservative estimate)
    {true, Limit * 3};
can_use_early_limit(_, _, _) ->
    %% Has ORDER BY - need full scan to sort candidates
    false.

%% @doc Check if streaming execution should be used for index seek
%% Streaming is better when: small limit + high cardinality index
%% Returns true if streaming should be used
should_use_streaming(Limit, Cardinality) when is_integer(Limit), Limit > 0, Limit =< 100 ->
    %% Use streaming when cardinality is much higher than limit
    %% This avoids batch-fetching many documents we won't use
    Cardinality > Limit * 10;
should_use_streaming(_, _) ->
    false.

%% @doc Check if we can use indexed order for ORDER BY + LIMIT
%% Returns {true, Path, Dir} if ORDER BY can use index iteration order
can_use_indexed_order([], _Limit) ->
    false;
can_use_indexed_order(_Order, undefined) ->
    false;
can_use_indexed_order([{Path, Dir}], Limit) when is_list(Path), is_integer(Limit), Limit > 0 ->
    %% Single ORDER BY on a path - can use index order
    {true, Path, Dir};
can_use_indexed_order([{Var, _Dir}], Limit) when is_atom(Var), is_integer(Limit), Limit > 0 ->
    %% ORDER BY on a variable - check if it's bound to a path
    %% For now, we only support direct path ordering
    case is_logic_var(Var) of
        true -> false;  %% Variable - can't use directly
        false -> false
    end;
can_use_indexed_order(_, _) ->
    false.

%% @doc Execute query using indexed order for ORDER BY + LIMIT
%% Iterates the index in the requested order and stops early
%% Uses CBOR iterator for condition matching and projection
execute_with_indexed_order(StoreRef, DbName, OrderPath, Dir, Plan, _Snapshot) ->
    #query_plan{
        conditions = Conditions,
        bindings = Bindings,
        projections = Projections,
        limit = Limit,
        offset = Offset,
        include_docs = IncludeDocs,
        decoder_fun = DecoderFun
    } = Plan,

    %% Calculate how many results we need (accounting for filtering)
    %% Collect 3x to handle filtering and deleted docs
    MaxCollect = (Limit + Offset) * 3,

    %% Select read profile based on limit (ORDER BY + LIMIT is typically small)
    Profile = select_read_profile(Limit, MaxCollect),

    %% Choose iteration direction based on ORDER BY
    FoldFun = case Dir of
        desc -> fun barrel_ars_index:fold_path_values_reverse/6;
        asc -> fun barrel_ars_index:fold_path_values/6
    end,

    %% Determine if we should use lazy checking (index-based filtering)
    UseLazy = can_lazy_check_conditions(Conditions),

    %% Collect matching documents with early termination
    {_Count, Results} = FoldFun(
        StoreRef, DbName, OrderPath,
        fun({_Path, DocId}, {Count, Acc}) ->
            case Count >= MaxCollect of
                true ->
                    {stop, {Count, Acc}};
                false ->
                    %% Filter and fetch document
                    case UseLazy of
                        true ->
                            %% Lazy path: check conditions via index BEFORE body fetch
                            lazy_fetch_and_match_doc(StoreRef, DbName, DocId, Conditions,
                                                      Bindings, Projections, IncludeDocs, DecoderFun,
                                                      Count, Acc);
                        false ->
                            %% Standard path: fetch body first, then check conditions
                            case fetch_and_match_doc(StoreRef, DbName, DocId, Conditions, Bindings, IncludeDocs) of
                                {ok, CborBin, Doc, BoundVars} ->
                                    Result = project_result(CborBin, Doc, DocId, Projections, BoundVars, IncludeDocs, DecoderFun),
                                    {ok, {Count + 1, [Result | Acc]}};
                                skip ->
                                    {ok, {Count, Acc}}
                            end
                    end
            end
        end,
        {0, []},
        Profile
    ),

    %% Results are in reverse order due to prepend - reverse them
    %% Note: For DESC we iterate high-to-low, prepend gives low-to-high, so reverse gives high-to-low
    %% For ASC we iterate low-to-high, prepend gives high-to-low, so reverse gives low-to-high
    OrderedResults = lists:reverse(Results),

    %% Apply offset and limit (no sorting needed - already in order)
    FinalResults = apply_offset_limit(OrderedResults, Offset, Limit),

    %% Get last sequence
    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    {ok, FinalResults, LastSeq}.

%% @doc Lazy fetch and match - check conditions via index BEFORE body fetch.
%% This avoids fetching bodies for documents that don't match filters.
%% Complexity: O(limit) instead of O(matching_docs)
lazy_fetch_and_match_doc(StoreRef, DbName, DocId, Conditions, _Bindings, Projections,
                          IncludeDocs, DecoderFun, Count, Acc) ->
    case lazy_check_conditions(StoreRef, DbName, DocId, Conditions) of
        true ->
            %% Conditions passed via index lookup - now fetch body if needed
            case IncludeDocs of
                true ->
                    case barrel_doc_body_store:get_current_body(DbName, DocId) of
                        {ok, CborBin} ->
                            Doc = barrel_docdb_codec_cbor:decode_any(CborBin),
                            Result = project_result(CborBin, Doc, DocId, Projections, #{}, IncludeDocs, DecoderFun),
                            {ok, {Count + 1, [Result | Acc]}};
                        _ ->
                            %% Doc deleted
                            {ok, {Count, Acc}}
                    end;
                false ->
                    %% No body needed - just return DocId
                    Result = #{<<"_id">> => DocId},
                    {ok, {Count + 1, [Result | Acc]}}
            end;
        false ->
            %% Conditions failed - skip without body fetch
            {ok, {Count, Acc}}
    end.

%% @doc Check conditions via index lookups (lazy evaluation).
%% Returns true if all conditions pass, false otherwise.
%% Uses index point lookups instead of body fetch.
lazy_check_conditions(_StoreRef, _DbName, _DocId, []) ->
    true;
lazy_check_conditions(StoreRef, DbName, DocId, [Condition | Rest]) ->
    case lazy_check_condition(StoreRef, DbName, DocId, Condition) of
        true -> lazy_check_conditions(StoreRef, DbName, DocId, Rest);
        false -> false
    end.

%% @private Check a single condition via index lookup
lazy_check_condition(StoreRef, DbName, DocId, {path, Path, Value}) ->
    %% O(1) point lookup in value index
    barrel_ars_index:docid_has_value(StoreRef, DbName, Path, Value, DocId);

lazy_check_condition(StoreRef, DbName, DocId, {compare, Path, Op, Value}) ->
    %% Load doc_paths (once per DocId) and compare
    barrel_ars_index:docid_satisfies_compare(StoreRef, DbName, DocId, Path, Op, Value);

lazy_check_condition(StoreRef, DbName, DocId, {exists, Path}) ->
    %% Check if path exists via doc_paths
    case barrel_ars_index:docid_get_value(StoreRef, DbName, DocId, Path) of
        {ok, _} -> true;
        _ -> false
    end;

lazy_check_condition(StoreRef, DbName, DocId, {prefix, Path, Prefix}) ->
    %% Get value and check prefix
    case barrel_ars_index:docid_get_value(StoreRef, DbName, DocId, Path) of
        {ok, Value} when is_binary(Value) ->
            PrefixLen = byte_size(Prefix),
            case Value of
                <<Prefix:PrefixLen/binary, _/binary>> -> true;
                _ -> false
            end;
        _ ->
            false
    end;

lazy_check_condition(StoreRef, DbName, DocId, {'and', Conditions}) ->
    lazy_check_conditions(StoreRef, DbName, DocId, Conditions);

lazy_check_condition(StoreRef, DbName, DocId, {'or', Conditions}) ->
    lazy_check_any(StoreRef, DbName, DocId, Conditions);

lazy_check_condition(_StoreRef, _DbName, _DocId, _Condition) ->
    %% Unsupported condition type - should not happen if can_lazy_check_condition is correct
    false.

%% @private Check if any condition passes (for OR)
lazy_check_any(_StoreRef, _DbName, _DocId, []) ->
    false;
lazy_check_any(StoreRef, DbName, DocId, [Condition | Rest]) ->
    case lazy_check_condition(StoreRef, DbName, DocId, Condition) of
        true -> true;
        false -> lazy_check_any(StoreRef, DbName, DocId, Rest)
    end.

%% @doc Fetch a document and check if it matches conditions (direct body fetch)
%% Returns {ok, CborBin, Doc, BoundVars} or skip
%% Returns both raw binary (for doc_format=binary) and decoded doc (for matching/projections)
%% No entity lookup needed - body is stored at doc_body(DbName, DocId)
fetch_and_match_doc(_StoreRef, DbName, DocId, Conditions, Bindings, _IncludeDocs) ->
    %% Direct body fetch - no entity lookup needed!
    case barrel_doc_body_store:get_current_body(DbName, DocId) of
        {ok, CborBin} ->
            %% Decode to map for condition matching
            Doc = barrel_docdb_codec_cbor:decode_any(CborBin),
            case matches_conditions(Doc, Conditions, Bindings) of
                {true, BoundVars} ->
                    {ok, CborBin, Doc, BoundVars};
                false ->
                    skip
            end;
        _ ->
            %% Doc deleted or not found
            skip
    end.

%% @doc Execute using index prefix scan with adaptive strategy selection
execute_index_scan(StoreRef, DbName, Plan, Snapshot) ->
    #query_plan{conditions = Conditions, limit = Limit} = Plan,
    %% Use adaptive classification for equality queries
    %% For limited queries, always prefer scan (early termination)
    %% For unlimited high-cardinality, prefer bulk decode
    case classify_scan_query_adaptive(StoreRef, DbName, Conditions, Plan) of
        {scan_equality, Path, Value} ->
            %% Low cardinality or has limit: iterate value_index keys
            execute_pure_equality(StoreRef, DbName, Path, Value, Plan, Snapshot);
        {bulk_equality, Path, Value} ->
            %% High cardinality without limit: single posting list decode
            case Limit of
                undefined ->
                    execute_bulk_equality(StoreRef, DbName, Path, Value, Plan, Snapshot);
                _ ->
                    %% Has limit, prefer scan for early termination
                    execute_pure_equality(StoreRef, DbName, Path, Value, Plan, Snapshot)
            end;
        {pure_exists, Path} ->
            execute_pure_exists(StoreRef, DbName, Path, Plan, Snapshot);
        {pure_prefix, Path, Prefix} ->
            execute_pure_prefix(StoreRef, DbName, Path, Prefix, Plan, Snapshot);
        {pure_compare, Path, Op, Value} ->
            %% Range query using index scan
            execute_pure_compare(StoreRef, DbName, Path, Op, Value, Plan, Snapshot);
        {multi_index, IndexConditions} ->
            %% Multi-condition with all index-friendly conditions
            execute_multi_index(StoreRef, DbName, IndexConditions, Plan, Snapshot);
        needs_body ->
            %% Extract limit from Plan for early termination
            #query_plan{limit = Limit, offset = Offset} = Plan,
            MaxCount = case Limit of
                undefined -> infinity;
                L when is_integer(L) -> (L + Offset) * 3  %% Over-collect to handle filtering
            end,
            DocIds = collect_scan_docids(StoreRef, DbName, Conditions, Snapshot, MaxCount),
            filter_and_project(StoreRef, DbName, DocIds, Plan, Snapshot)
    end.

%% @doc Classify scan query for pure index execution.
%% Returns:
%%   {pure_equality, Path, Value} - exact match, return DocIds from posting list
%%   {pure_exists, Path} - exists check, return DocIds from posting lists
%%   {pure_prefix, Path, Prefix} - prefix match, return DocIds from posting lists
%%   needs_body - requires doc body fetch for filtering/projection
-spec classify_scan_query([condition()], query_plan()) ->
    {pure_equality, [term()], term()} |
    {pure_exists, [term()]} |
    {pure_prefix, [term()], binary()} |
    {pure_compare, [term()], compare_op(), term()} |
    {multi_index, [condition()]} |
    needs_body.
classify_scan_query(Conditions, Plan) ->
    #query_plan{projections = Projections} = Plan,
    %% Note: include_docs doesn't affect classification - we can still use
    %% pure index scans and fetch docs afterward. Only projections that
    %% reference paths require body fetch during query.
    NeedsBodyForQuery = needs_body_for_projection(Projections),
    case {Conditions, NeedsBodyForQuery} of
        {[{path, Path, Value}], false} ->
            %% Pure equality - only if value is concrete (not a logic var)
            case is_logic_var(Value) of
                true -> needs_body;
                false -> {pure_equality, Path, Value}
            end;
        {[{exists, Path}], false} ->
            {pure_exists, Path};
        {[{prefix, Path, Prefix}], false} ->
            {pure_prefix, Path, Prefix};
        {[{compare, Path, Op, Value}], false} when Op =:= '>' orelse Op =:= '<'
                                                   orelse Op =:= '>=' orelse Op =:= '=<' ->
            %% Pure compare - use index range scan
            case is_logic_var(Value) of
                true -> needs_body;
                false -> {pure_compare, Path, Op, Value}
            end;
        {MultiConds, false} when length(MultiConds) > 1 ->
            %% Multiple conditions - check if all are index-friendly
            case all_index_conditions(MultiConds) of
                true -> {multi_index, MultiConds};
                false -> needs_body
            end;
        _ ->
            needs_body
    end.

%% @doc Check if all conditions can be evaluated using index
all_index_conditions([]) -> true;
all_index_conditions([{path, _, Value} | Rest]) ->
    case is_logic_var(Value) of
        true -> false;
        false -> all_index_conditions(Rest)
    end;
all_index_conditions([{exists, _} | Rest]) ->
    all_index_conditions(Rest);
all_index_conditions([{prefix, _, _} | Rest]) ->
    all_index_conditions(Rest);
all_index_conditions([{compare, _, Op, Value} | Rest])
  when Op =:= '>' orelse Op =:= '<' orelse Op =:= '>=' orelse Op =:= '=<' ->
    case is_logic_var(Value) of
        true -> false;
        false -> all_index_conditions(Rest)
    end;
all_index_conditions(_) -> false.

%% @doc Check if projections require document body
needs_body_for_projection(['*']) -> false;
needs_body_for_projection([]) -> false;
needs_body_for_projection(Projections) ->
    %% Projections require body if they include path references
    lists:any(fun(P) -> is_list(P) end, Projections).

%% @doc Adaptive query classification with cardinality-based strategy selection
%% For equality queries, chooses between:
%% - scan_equality: iterate value_index keys (good for selective queries, early termination)
%% - bulk_equality: decode posting list (good for high-cardinality, one seek)
-spec classify_scan_query_adaptive(barrel_store_rocksdb:db_ref(), db_name(), [condition()], query_plan()) ->
    {scan_equality, [term()], term()} |
    {bulk_equality, [term()], term()} |
    {pure_exists, [term()]} |
    {pure_prefix, [term()], binary()} |
    {pure_compare, [term()], compare_op(), term()} |
    {multi_index, [condition()]} |
    needs_body.
classify_scan_query_adaptive(StoreRef, DbName, Conditions, Plan) ->
    #query_plan{projections = Projections} = Plan,
    %% Note: include_docs doesn't affect classification - we can still use
    %% pure index scans and fetch docs afterward.
    NeedsBodyForQuery = needs_body_for_projection(Projections),
    case {Conditions, NeedsBodyForQuery} of
        {[{path, Path, Value}], false} ->
            case is_logic_var(Value) of
                true ->
                    needs_body;
                false ->
                    %% Check cardinality to choose strategy
                    choose_equality_strategy(StoreRef, DbName, Path, Value)
            end;
        {[{exists, Path}], false} ->
            {pure_exists, Path};
        {[{prefix, Path, Prefix}], false} ->
            {pure_prefix, Path, Prefix};
        {[{compare, Path, Op, Value}], false} when Op =:= '>' orelse Op =:= '<'
                                                   orelse Op =:= '>=' orelse Op =:= '=<' ->
            case is_logic_var(Value) of
                true -> needs_body;
                false -> {pure_compare, Path, Op, Value}
            end;
        {MultiConds, false} when length(MultiConds) > 1 ->
            %% Multiple conditions - check if all are index-friendly
            case all_index_conditions(MultiConds) of
                true -> {multi_index, MultiConds};
                false -> needs_body
            end;
        _ ->
            needs_body
    end.

%% @doc Choose between scan vs bulk strategy based on estimated cardinality
-spec choose_equality_strategy(barrel_store_rocksdb:db_ref(), db_name(), [term()], term()) ->
    {scan_equality, [term()], term()} | {bulk_equality, [term()], term()}.
choose_equality_strategy(StoreRef, DbName, Path, Value) ->
    case estimate_cardinality(StoreRef, DbName, Path, Value) of
        {ok, Count} when Count =< ?ADAPTIVE_CARDINALITY_THRESHOLD ->
            %% Low cardinality: use value_index scan with early termination
            {scan_equality, Path, Value};
        {ok, _HighCount} ->
            %% High cardinality: use posting list bulk decode
            {bulk_equality, Path, Value};
        {error, _} ->
            %% Error getting stats, fall back to scan (safer default)
            {scan_equality, Path, Value}
    end.

%% @doc Estimate cardinality for a path+value combination
%% Uses stored path statistics for O(1) lookup
-spec estimate_cardinality(barrel_store_rocksdb:db_ref(), db_name(), [term()], term()) ->
    {ok, non_neg_integer()} | {error, term()}.
estimate_cardinality(StoreRef, DbName, Path, Value) ->
    FullPath = Path ++ [Value],
    barrel_ars_index:get_path_cardinality(StoreRef, DbName, FullPath).

%% @doc Execute pure exists query - iterate posting lists directly
%% Path index only contains non-deleted docs, so no doc_current check needed
%% Uses fold_posting which gets list of DocIds per key - much more efficient
execute_pure_exists(StoreRef, DbName, Path, Plan, Snapshot) ->
    #query_plan{
        limit = Limit,
        offset = Offset
    } = Plan,

    %% Calculate how many unique docs we need
    MaxCollect = case Limit of
        undefined -> undefined;
        L -> L + Offset
    end,

    %% Iterate posting lists directly with snapshot for consistency
    %% Each key contains a list of DocIds
    {_, _, Results} = barrel_ars_index:fold_posting_with_snapshot(
        StoreRef, DbName, Path,
        fun(_Key, DocIds, {Seen, Count, Acc}) ->
            %% Process all DocIds from this posting list (ignore Key)
            process_exists_docids(DocIds, Seen, Count, Acc, MaxCollect)
        end,
        {#{}, 0, []},
        Snapshot
    ),

    %% Apply offset and limit (results are in reverse order)
    Results1 = lists:reverse(Results),
    Results2 = apply_offset_limit(Results1, Offset, Limit),

    %% Get last sequence
    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    {ok, Results2, LastSeq}.

%% @private Process DocIds from a posting list for exists query
process_exists_docids([], Seen, Count, Acc, _MaxCollect) ->
    {ok, {Seen, Count, Acc}};
process_exists_docids([DocId | Rest], Seen, Count, Acc, MaxCollect) ->
    case maps:is_key(DocId, Seen) of
        true ->
            %% Already seen this doc, skip
            process_exists_docids(Rest, Seen, Count, Acc, MaxCollect);
        false ->
            %% New doc
            NewSeen = Seen#{DocId => true},
            NewCount = Count + 1,
            NewAcc = [#{<<"id">> => DocId} | Acc],
            case MaxCollect =/= undefined andalso NewCount >= MaxCollect of
                true ->
                    {stop, {NewSeen, NewCount, NewAcc}};
                false ->
                    process_exists_docids(Rest, NewSeen, NewCount, NewAcc, MaxCollect)
            end
    end.

%% @doc Execute pure prefix query - iterate posting lists directly
%% No doc_current check needed since path index only contains non-deleted docs
%% Uses fold_prefix_posting which gets raw DocId lists per posting list key
execute_pure_prefix(StoreRef, DbName, Path, Prefix, Plan, Snapshot) ->
    #query_plan{
        limit = Limit,
        offset = Offset
    } = Plan,

    %% Calculate how many unique docs we need
    MaxCollect = case Limit of
        undefined -> undefined;
        L -> L + Offset
    end,

    %% Iterate posting lists directly with snapshot for consistency
    %% Each key contains a list of DocIds
    {_, _, Results} = barrel_ars_index:fold_prefix_posting_with_snapshot(
        StoreRef, DbName, Path, Prefix,
        fun(_Key, DocIds, {Seen, Count, Acc}) ->
            %% Process all DocIds from this posting list
            process_prefix_docids(DocIds, Seen, Count, Acc, MaxCollect)
        end,
        {#{}, 0, []},
        Snapshot
    ),

    %% Apply offset and limit (results are in reverse order)
    Results1 = lists:reverse(Results),
    Results2 = apply_offset_limit(Results1, Offset, Limit),

    %% Get last sequence
    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    {ok, Results2, LastSeq}.

%% @doc Execute pure compare query - iterates posting lists for range matches
%% Uses optimized fold_compare_docids which skips key decoding overhead
execute_pure_compare(StoreRef, DbName, Path, Op, Value, Plan, Snapshot) ->
    #query_plan{
        limit = Limit,
        offset = Offset
    } = Plan,

    %% Calculate how many unique docs we need
    MaxCollect = case Limit of
        undefined -> undefined;
        L -> L + Offset
    end,

    %% Iterate posting lists for values matching the compare condition
    %% Uses optimized fold that doesn't decode keys (we don't need paths)
    FoldFun = fun(DocId, {Seen, Count, Acc}) ->
        case maps:is_key(DocId, Seen) of
            true ->
                {ok, {Seen, Count, Acc}};
            false ->
                NewSeen = Seen#{DocId => true},
                NewCount = Count + 1,
                NewAcc = [#{<<"id">> => DocId} | Acc],
                case MaxCollect =/= undefined andalso NewCount >= MaxCollect of
                    true ->
                        {stop, {NewSeen, NewCount, NewAcc}};
                    false ->
                        {ok, {NewSeen, NewCount, NewAcc}}
                end
        end
    end,

    %% Use provided snapshot for consistency
    {_, _, Results} = barrel_ars_index:fold_compare_docids_with_snapshot(
        StoreRef, DbName, Path, Op, Value, FoldFun, {#{}, 0, []}, Snapshot
    ),

    %% Apply offset and limit (results are in reverse order)
    Results1 = lists:reverse(Results),
    Results2 = apply_offset_limit(Results1, Offset, Limit),

    %% Get last sequence
    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    {ok, Results2, LastSeq}.

%% @doc Execute multi-index query - intersects posting lists from all conditions
execute_multi_index(StoreRef, DbName, Conditions, Plan, Snapshot) ->
    #query_plan{
        limit = Limit,
        offset = Offset
    } = Plan,

    %% Collect DocIds using native postings API intersection (V2)
    IntersectedDocIds = intersect_docid_sets_v2(StoreRef, DbName, Conditions),

    %% Build results with limit applied
    LimitedDocIds = case Limit of
        undefined -> IntersectedDocIds;
        L -> lists:sublist(IntersectedDocIds, L + Offset)
    end,
    Results0 = [#{<<"id">> => DocId} || DocId <- LimitedDocIds],
    Results = apply_offset_limit(Results0, Offset, Limit),

    %% Get last sequence
    _ = Snapshot,  %% Snapshot not used yet for this path
    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    {ok, Results, LastSeq}.

%% @doc Execute pure equality query - iterates value-first index with early termination
%% Uses prefix scan on individual keys, stops after collecting enough results
execute_pure_equality(StoreRef, DbName, Path, Value, Plan, Snapshot) ->
    #query_plan{
        limit = Limit,
        offset = Offset
    } = Plan,

    %% Calculate how many docs we need to collect
    MaxCollect = case Limit of
        undefined -> undefined;
        L -> L + Offset
    end,

    %% Fold over value-first index with early termination
    %% Path is the field path, Value is what we're searching for
    FoldFun = fun(DocId, {Count, Acc}) ->
        NewCount = Count + 1,
        NewAcc = [#{<<"id">> => DocId} | Acc],
        case MaxCollect =/= undefined andalso NewCount >= MaxCollect of
            true ->
                %% Collected enough, stop iteration
                {stop, {NewCount, NewAcc}};
            false ->
                {ok, {NewCount, NewAcc}}
        end
    end,
    {_, Results} = barrel_ars_index:fold_value_index(
        StoreRef, DbName, Value, Path, FoldFun, {0, []}, Snapshot),

    %% Results are in reverse order, fix and apply offset
    FinalResults = apply_offset_limit(lists:reverse(Results), Offset, Limit),

    %% Get last sequence
    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    {ok, FinalResults, LastSeq}.

%% @doc Execute bulk equality query - single seek + posting list decode
%% Used for high-cardinality queries where iterating individual keys is slower
%% than decoding a posting list in one go.
execute_bulk_equality(StoreRef, DbName, Path, Value, Plan, _Snapshot) ->
    #query_plan{
        limit = Limit,
        offset = Offset
    } = Plan,

    %% Single O(1) posting list lookup
    FullPath = Path ++ [Value],
    DocIds = barrel_ars_index:get_posting_list(StoreRef, DbName, FullPath),

    %% Build results and apply offset/limit
    Results = [#{<<"id">> => Id} || Id <- DocIds],
    FinalResults = apply_offset_limit(Results, Offset, Limit),

    %% Get last sequence
    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    {ok, FinalResults, LastSeq}.

%% @private Process DocIds from a posting list for prefix query
process_prefix_docids([], Seen, Count, Acc, _MaxCollect) ->
    {ok, {Seen, Count, Acc}};
process_prefix_docids([DocId | Rest], Seen, Count, Acc, MaxCollect) ->
    case maps:is_key(DocId, Seen) of
        true ->
            %% Already seen this doc, skip
            process_prefix_docids(Rest, Seen, Count, Acc, MaxCollect);
        false ->
            %% New doc
            NewSeen = Seen#{DocId => true},
            NewCount = Count + 1,
            NewAcc = [#{<<"id">> => DocId} | Acc],
            case MaxCollect =/= undefined andalso NewCount >= MaxCollect of
                true ->
                    {stop, {NewSeen, NewCount, NewAcc}};
                false ->
                    process_prefix_docids(Rest, NewSeen, NewCount, NewAcc, MaxCollect)
            end
    end.

%% @doc Collect DocIds for scan-based execution
%% Tries optimized paths: multi-index intersection, exists/prefix, then path prefix scan
%% MaxCount limits collection for early termination (use infinity for no limit)
collect_scan_docids(StoreRef, DbName, Conditions, _Snapshot, MaxCount) ->
    %% First check if we have multiple index-friendly conditions for intersection
    IndexConditions = [C || C <- Conditions, is_index_condition(C)],
    case IndexConditions of
        [_, _ | _] ->
            %% Multiple index conditions - use native postings intersection (V2)
            intersect_docid_sets_v2(StoreRef, DbName, IndexConditions);
        _ ->
            %% Single or no index conditions - use existing path
            case find_exists_condition(Conditions) of
                {ok, Path} ->
                    %% Exists check: collect docs that have any value at this path
                    collect_docids_for_path_exists(StoreRef, DbName, Path, MaxCount);
                not_found ->
                    case find_prefix_condition(Conditions) of
                        {ok, Path, Prefix} ->
                            %% Optimized interval scan for prefix queries
                            collect_docids_for_value_prefix(StoreRef, DbName, Path, Prefix, MaxCount);
                        not_found ->
                            case find_best_scan_path(Conditions) of
                                {ok, Path} ->
                                    collect_docids_for_prefix(StoreRef, DbName, Path, MaxCount);
                                not_found ->
                                    collect_all_docids(StoreRef, DbName, MaxCount)
                            end
                    end
            end
    end.

%% @private Check if a condition can use index for intersection
is_index_condition({path, _Path, Value}) ->
    not is_logic_var(Value);
is_index_condition({compare, _Path, _Op, Value}) ->
    not is_logic_var(Value);
is_index_condition({prefix, _Path, _Prefix}) ->
    true;
is_index_condition({exists, _Path}) ->
    true;
is_index_condition(_) ->
    false.

%% @doc Find an exists condition for optimized path scan
find_exists_condition([]) ->
    not_found;
find_exists_condition([{exists, Path} | _]) ->
    {ok, Path};
find_exists_condition([{'and', Nested} | Rest]) ->
    case find_exists_condition(Nested) of
        {ok, _} = Found -> Found;
        not_found -> find_exists_condition(Rest)
    end;
find_exists_condition([_ | Rest]) ->
    find_exists_condition(Rest).

%% @doc Collect all DocIds that have any value at the given path
%% Uses the path index to find docs with the path without fetching full docs
%% MaxCount limits collection for early termination (use infinity for no limit)
collect_docids_for_path_exists(StoreRef, DbName, Path, infinity) ->
    %% Use long_scan profile since exists queries typically return many docs
    barrel_ars_index:fold_path_values(
        StoreRef, DbName, Path,
        fun({_FullPath, DocId}, Acc) -> {ok, [DocId | Acc]} end,
        [],
        long_scan
    );
collect_docids_for_path_exists(StoreRef, DbName, Path, MaxCount) when is_integer(MaxCount) ->
    %% Limited collection with early termination
    Profile = select_read_profile(MaxCount),
    {_, DocIds} = barrel_ars_index:fold_path_values(
        StoreRef, DbName, Path,
        fun({_FullPath, DocId}, {Count, Acc}) ->
            case Count >= MaxCount of
                true -> {stop, {Count, Acc}};
                false -> {ok, {Count + 1, [DocId | Acc]}}
            end
        end,
        {0, []},
        Profile
    ),
    DocIds.

%% @doc Collect all document IDs (for full scan fallback)
%% MaxCount limits collection for early termination (use infinity for no limit)
collect_all_docids(StoreRef, DbName, infinity) ->
    %% Use long_scan profile for full table scans (prefetch, avoid cache pollution)
    barrel_store_rocksdb:fold_range(
        StoreRef,
        barrel_store_keys:doc_info_prefix(DbName),
        barrel_store_keys:doc_info_end(DbName),
        fun(Key, _Value, Acc) ->
            DocId = barrel_store_keys:decode_doc_info_key(DbName, Key),
            {ok, [DocId | Acc]}
        end,
        [],
        long_scan
    );
collect_all_docids(StoreRef, DbName, MaxCount) when is_integer(MaxCount) ->
    %% Limited full scan with early termination
    Profile = select_read_profile(MaxCount),
    {_, DocIds} = barrel_store_rocksdb:fold_range(
        StoreRef,
        barrel_store_keys:doc_info_prefix(DbName),
        barrel_store_keys:doc_info_end(DbName),
        fun(Key, _Value, {Count, Acc}) ->
            case Count >= MaxCount of
                true -> {stop, {Count, Acc}};
                false ->
                    DocId = barrel_store_keys:decode_doc_info_key(DbName, Key),
                    {ok, {Count + 1, [DocId | Acc]}}
            end
        end,
        {0, []},
        Profile
    ),
    DocIds.

%% @doc Find a prefix condition for optimized interval scan
find_prefix_condition([]) ->
    not_found;
find_prefix_condition([{prefix, Path, Prefix} | _]) ->
    {ok, Path, Prefix};
find_prefix_condition([{'and', Nested} | Rest]) ->
    case find_prefix_condition(Nested) of
        {ok, _, _} = Found -> Found;
        not_found -> find_prefix_condition(Rest)
    end;
find_prefix_condition([_ | Rest]) ->
    find_prefix_condition(Rest).

%% @doc Collect DocIds using optimized prefix interval scan
%% MaxCount limits collection for early termination (use infinity for no limit)
collect_docids_for_value_prefix(StoreRef, DbName, Path, Prefix, infinity) ->
    %% Use short_range profile - prefix queries typically have moderate selectivity
    barrel_ars_index:fold_prefix(
        StoreRef, DbName, Path, Prefix,
        fun({_FullPath, DocId}, Acc) -> {ok, [DocId | Acc]} end,
        [],
        short_range
    );
collect_docids_for_value_prefix(StoreRef, DbName, Path, Prefix, MaxCount) when is_integer(MaxCount) ->
    %% Limited prefix scan with early termination
    Profile = select_read_profile(MaxCount),
    {_, DocIds} = barrel_ars_index:fold_prefix(
        StoreRef, DbName, Path, Prefix,
        fun({_FullPath, DocId}, {Count, Acc}) ->
            case Count >= MaxCount of
                true -> {stop, {Count, Acc}};
                false -> {ok, {Count + 1, [DocId | Acc]}}
            end
        end,
        {0, []},
        Profile
    ),
    DocIds.

%% @doc Execute using multiple index lookups with intersection
%% Uses cardinality-ordered intersection for efficient multi-condition queries.
execute_multi_index(StoreRef, DbName, Plan, Snapshot) ->
    #query_plan{conditions = Conditions, limit = Limit, offset = Offset,
                include_docs = IncludeDocs, projections = Projections} = Plan,

    %% Find all indexable conditions (including compare, exists, prefix)
    IndexConditions = find_all_index_conditions(Conditions),

    case IndexConditions of
        [] ->
            execute_full_scan(StoreRef, DbName, Plan, Snapshot);
        _ ->
            %% Check if all conditions are equality (can use bitmap optimization)
            AllEquality = lists:all(fun({path, _, _}) -> true; (_) -> false end, IndexConditions),

            case AllEquality of
                true ->
                    %% All equality conditions
                    case order_by_cardinality(StoreRef, DbName, IndexConditions) of
                        [] ->
                            filter_and_project(StoreRef, DbName, [], Plan, Snapshot);
                        OrderedConditions ->
                            %% Check if LIMIT allows lazy iteration
                            RemainingConds = Conditions -- IndexConditions,
                            NeedsBody = IncludeDocs orelse needs_body_for_projection(Projections)
                                        orelse RemainingConds =/= [],
                            case {Limit, NeedsBody, length(OrderedConditions)} of
                                {L, false, N} when is_integer(L), L > 0, N > 1 ->
                                    %% Multiple equality conditions with LIMIT, no body fetch
                                    %% Use lazy iteration for early termination
                                    execute_multi_index_lazy(StoreRef, DbName, OrderedConditions, Plan, Snapshot);
                                {L, true, N} when is_integer(L), L > 0, N > 1 ->
                                    %% Multiple equality conditions with LIMIT, needs body
                                    %% Use lazy intersection then stream body
                                    DocIds = intersect_lazy_with_limit(StoreRef, DbName, OrderedConditions, (L + Offset) * 3),
                                    FilterPlan = Plan#query_plan{conditions = RemainingConds},
                                    filter_and_project(StoreRef, DbName, DocIds, FilterPlan, Snapshot);
                                _ ->
                                    %% Single condition or no LIMIT - use full intersection
                                    execute_with_postings(StoreRef, DbName, OrderedConditions, Plan, Snapshot)
                            end
                    end;
                false ->
                    %% Mixed conditions (equality + compare/exists/prefix)
                    %% Check if remaining conditions need body fetch
                    RemainingConds = Conditions -- IndexConditions,
                    NeedsBody = IncludeDocs orelse needs_body_for_projection(Projections)
                                orelse RemainingConds =/= [],

                    %% Optimize: for LIMIT queries with mixed conditions,
                    %% use lazy iteration with O(1) equality checks
                    case {Limit, NeedsBody} of
                        {L, false} when is_integer(L), L > 0 ->
                            %% Pure index query with LIMIT - use lazy intersection
                            execute_multi_index_lazy(StoreRef, DbName, IndexConditions, Plan, Snapshot);
                        {L, true} when is_integer(L), L > 0 ->
                            %% Body fetch with LIMIT - use lazy intersection then stream body
                            DocIds = intersect_lazy_with_limit(StoreRef, DbName, IndexConditions, (L + Offset) * 3),
                            FilterPlan = Plan#query_plan{conditions = RemainingConds},
                            filter_and_project(StoreRef, DbName, DocIds, FilterPlan, Snapshot);
                        _ ->
                            %% No limit - use standard intersection
                            IntersectedDocIds = intersect_docid_sets_v2(StoreRef, DbName, IndexConditions),
                            case NeedsBody of
                                false ->
                                    Results0 = [#{<<"id">> => DocId} || DocId <- IntersectedDocIds],
                                    Results = apply_offset_limit(Results0, Offset, Limit),
                                    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),
                                    {ok, Results, LastSeq};
                                true ->
                                    FilterPlan = Plan#query_plan{conditions = RemainingConds},
                                    filter_and_project(StoreRef, DbName, IntersectedDocIds, FilterPlan, Snapshot)
                            end
                    end
            end
    end.

%% @doc Execute multi-condition query using V2 posting list intersection.
%% Uses native postings_intersect_all for O(min(n,m)) intersection performance.
execute_with_postings(StoreRef, DbName, IndexConditions, Plan, Snapshot) ->
    %% Remaining conditions not verified by index (e.g., OR, logic vars)
    #query_plan{conditions = AllConditions} = Plan,
    RemainingConds = AllConditions -- IndexConditions,
    FilterPlan = Plan#query_plan{conditions = RemainingConds},

    %% Use V2 native posting list intersection
    DocIds = intersect_docid_sets_v2(StoreRef, DbName, IndexConditions),
    filter_and_project(StoreRef, DbName, DocIds, FilterPlan, Snapshot).

%% @doc Execute multi-index with lazy iteration for LIMIT queries.
%% Iterates the smallest condition and verifies others with O(1) lookups.
%% Complexity: O(limit / selectivity) instead of O(matching_docs)
execute_multi_index_lazy(StoreRef, DbName, Conditions, Plan, Snapshot) ->
    #query_plan{limit = Limit, offset = Offset} = Plan,
    MaxCollect = Limit + Offset,

    DocIds = intersect_lazy_with_limit(StoreRef, DbName, Conditions, MaxCollect),

    Results0 = [#{<<"id">> => DocId} || DocId <- DocIds],
    Results = apply_offset_limit(Results0, Offset, Limit),

    _ = Snapshot,
    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),
    {ok, Results, LastSeq}.

%% @doc Lazy intersection with limit - iterates one condition and verifies others.
%% Strategy: ALWAYS prefer iterating non-equality conditions because:
%% - Non-equality (compare/exists/prefix) iteration uses fast fold with early termination
%% - Equality verification uses O(1) docid_has_value lookup
%% - Non-equality verification requires expensive doc_paths load
%%
%% Among non-equality conditions, choose the smallest estimated cardinality.
%% Among equality conditions (if no non-equality), choose smallest cardinality.
intersect_lazy_with_limit(StoreRef, DbName, Conditions, MaxCollect) ->
    %% Separate: equality (O(1) verify) vs non-equality (fast iterate, slow verify)
    {EqConds, NonEqConds} = lists:partition(fun
        ({path, _, _}) -> true;
        (_) -> false
    end, Conditions),

    case NonEqConds of
        [] when length(EqConds) > 1 ->
            %% All equality with multiple conditions - iterate smallest posting list
            %% and verify others with O(1) lookups, using lazy iterator (not loading full list)
            {IterCond, RestEq} = pick_smallest_equality(StoreRef, DbName, EqConds),
            iterate_with_lazy_verify(StoreRef, DbName, IterCond, RestEq, MaxCollect);
        [] ->
            %% Single equality - just get posting list with limit
            [{path, Path, Value}] = EqConds,
            collect_docids_for_path_limited(StoreRef, DbName, Path ++ [Value], MaxCollect);
        [SingleNonEq] ->
            %% One non-equality condition - iterate it, verify equalities
            iterate_with_lazy_verify(StoreRef, DbName, SingleNonEq, EqConds, MaxCollect);
        _MultipleNonEq ->
            %% Two or more non-equality conditions. Per-docid verification of a
            %% second range/compare via doc_paths is unreliable, so collect each
            %% condition's docid set and intersect (the proven non-lazy path),
            %% then truncate to the limit. Returns a sorted list, consistent with
            %% the continuation path.
            lists:sublist(
                intersect_docid_sets_v2(StoreRef, DbName, Conditions), MaxCollect)
    end.


%% @private Pick the equality condition with smallest cardinality to iterate
pick_smallest_equality(StoreRef, DbName, EqConds) ->
    WithCardinality = lists:map(fun({path, Path, Value} = Cond) ->
        FullPath = Path ++ [Value],
        Card = case barrel_ars_index:get_path_cardinality(StoreRef, DbName, FullPath) of
            {ok, C} -> C;
            _ -> 500000
        end,
        {Card, Cond}
    end, EqConds),
    Sorted = lists:keysort(1, WithCardinality),
    [{_, Smallest} | Rest] = Sorted,
    {Smallest, [C || {_, C} <- Rest]}.

%% @private Iterate a condition and lazily verify others with early termination
iterate_with_lazy_verify(StoreRef, DbName, IterCond, VerifyConds, MaxCollect) ->
    %% Callback for path iteration (receives {Path, DocId})
    PathFoldFun = fun({_Path, DocId}, {Count, Seen, Acc}) ->
        process_docid_for_verify(StoreRef, DbName, DocId, VerifyConds, MaxCollect,
                                  Count, Seen, Acc)
    end,

    %% Callback for value_index iteration (receives DocId directly)
    ValueIndexFoldFun = fun(DocId, {Count, Seen, Acc}) ->
        process_docid_for_verify(StoreRef, DbName, DocId, VerifyConds, MaxCollect,
                                  Count, Seen, Acc)
    end,

    {_, _, DocIds} = case IterCond of
        {compare, Path, Op, Value} ->
            barrel_ars_index:fold_path_values_compare(
                StoreRef, DbName, Path, Op, Value, PathFoldFun, {0, #{}, []});
        {exists, Path} ->
            barrel_ars_index:fold_path(
                StoreRef, DbName, Path, PathFoldFun, {0, #{}, []}, short_range);
        {prefix, Path, Prefix} ->
            barrel_ars_index:fold_prefix(
                StoreRef, DbName, Path, Prefix, PathFoldFun, {0, #{}, []}, short_range);
        {path, Path, Value} ->
            %% Equality - iterate value_index keys (individual DocId per key)
            %% Much faster than fold_posting for LIMIT queries since we read
            %% one key at a time instead of loading the entire posting list
            barrel_ars_index:fold_value_index(
                StoreRef, DbName, Value, Path, ValueIndexFoldFun, {0, #{}, []})
    end,
    lists:reverse(DocIds).

%% @private Process a single DocId for verification
process_docid_for_verify(StoreRef, DbName, DocId, VerifyConds, MaxCollect, Count, Seen, Acc) ->
    case maps:is_key(DocId, Seen) of
        true ->
            {ok, {Count, Seen, Acc}};
        false ->
            NewSeen = Seen#{DocId => true},
            case lazy_verify_all(StoreRef, DbName, DocId, VerifyConds) of
                true ->
                    NewCount = Count + 1,
                    NewAcc = [DocId | Acc],
                    case NewCount >= MaxCollect of
                        true -> {stop, {NewCount, NewSeen, NewAcc}};
                        false -> {ok, {NewCount, NewSeen, NewAcc}}
                    end;
                false ->
                    {ok, {Count, NewSeen, Acc}}
            end
    end.

%% @private Verify all conditions for a DocId using O(1) index lookups
lazy_verify_all(_StoreRef, _DbName, _DocId, []) ->
    true;
lazy_verify_all(StoreRef, DbName, DocId, [{path, Path, Value} | Rest]) ->
    case barrel_ars_index:docid_has_value(StoreRef, DbName, Path, Value, DocId) of
        true -> lazy_verify_all(StoreRef, DbName, DocId, Rest);
        false -> false
    end;
lazy_verify_all(StoreRef, DbName, DocId, [{compare, Path, Op, Value} | Rest]) ->
    %% For compare in verification, we need to check doc_paths (slower)
    %% But this is rare - usually compare is the iterator
    case barrel_ars_index:docid_satisfies_compare(StoreRef, DbName, DocId, Path, Op, Value) of
        true -> lazy_verify_all(StoreRef, DbName, DocId, Rest);
        false -> false
    end;
lazy_verify_all(StoreRef, DbName, DocId, [{exists, Path} | Rest]) ->
    case barrel_ars_index:docid_get_value(StoreRef, DbName, DocId, Path) of
        {ok, _} -> lazy_verify_all(StoreRef, DbName, DocId, Rest);
        _ -> false
    end;
lazy_verify_all(StoreRef, DbName, DocId, [{prefix, Path, Prefix} | Rest]) ->
    case barrel_ars_index:docid_get_value(StoreRef, DbName, DocId, Path) of
        {ok, Val} when is_binary(Val) ->
            PrefixLen = byte_size(Prefix),
            case Val of
                <<Prefix:PrefixLen/binary, _/binary>> ->
                    lazy_verify_all(StoreRef, DbName, DocId, Rest);
                _ -> false
            end;
        _ -> false
    end;
lazy_verify_all(_StoreRef, _DbName, _DocId, [_ | _]) ->
    %% Unknown condition type - fail safe
    false.

%% @doc Order conditions by cardinality (smallest first) for optimal intersection.
%% Returns empty list if any condition has 0 cardinality (short-circuit).
order_by_cardinality(StoreRef, DbName, Conditions) ->
    %% Build keys for all conditions
    Keys = [barrel_store_keys:path_stats_key(DbName, Path ++ [Value])
            || {path, Path, Value} <- Conditions],

    %% Batch fetch all cardinalities with multi_get
    Results = barrel_store_rocksdb:multi_get(StoreRef, Keys),

    %% Parse results and associate with conditions
    WithCardinality = lists:zipwith(
        fun(Cond, Result) ->
            Count = case Result of
                {ok, CountBin} -> max(0, binary_to_integer(CountBin));
                not_found -> 0
            end,
            {Count, Cond}
        end,
        Conditions, Results
    ),

    %% Check for any zero cardinality (short-circuit)
    case lists:any(fun({0, _}) -> true; (_) -> false end, WithCardinality) of
        true ->
            [];
        false ->
            %% Sort by cardinality ascending and extract conditions
            Sorted = lists:keysort(1, WithCardinality),
            [Cond || {_, Cond} <- Sorted]
    end.

%% @doc Execute full document scan (slowest, last resort, using wide column entity)
execute_full_scan(StoreRef, DbName, Plan, Snapshot) ->
    %% Collect all doc IDs by scanning doc_entity keys
    %% Use long_scan profile for full table scans (prefetch, avoid cache pollution)
    StartKey = barrel_store_keys:doc_entity_prefix(DbName),
    EndKey = barrel_store_keys:doc_entity_end(DbName),
    PrefixLen = byte_size(StartKey),
    DocIds = barrel_store_rocksdb:fold_range(
        StoreRef,
        StartKey,
        EndKey,
        fun(Key, _Value, Acc) ->
            %% Extract DocId from key (after prefix)
            DocId = binary:part(Key, PrefixLen, byte_size(Key) - PrefixLen),
            {ok, [DocId | Acc]}
        end,
        [],
        long_scan
    ),

    filter_and_project(StoreRef, DbName, DocIds, Plan, Snapshot).

%% @doc Collect document IDs matching an exact path+value
%% Returns SORTED list for intersection operations.
%% Uses bucketed posting lists for efficient sorted iteration.
collect_docids_for_path(StoreRef, DbName, FullPath) ->
    %% Extract value (last element) and field path (rest)
    case split_path_value(FullPath) of
        {[], _} ->
            %% Single element path - fall back to regular posting list
            lists:sort(barrel_ars_index:get_posting_list(StoreRef, DbName, FullPath));
        {FieldPath, Value} ->
            %% Use bucketed posting list for sorted iteration
            barrel_ars_index:get_bucketed_posting_list(StoreRef, DbName, Value, FieldPath)
    end.

%% @private Split path into field path and value
split_path_value(Path) ->
    ReversedPath = lists:reverse(Path),
    [Value | ReversedFieldPath] = ReversedPath,
    {lists:reverse(ReversedFieldPath), Value}.

%% @doc Collect document IDs with early termination at MaxCount
%% For LIMIT pushdown optimization
%% Uses direct posting list lookup with sublist - O(1) fetch + truncate
collect_docids_for_path_limited(StoreRef, DbName, FullPath, MaxCount) ->
    AllDocIds = barrel_ars_index:get_posting_list(StoreRef, DbName, FullPath),
    lists:sublist(AllDocIds, MaxCount).

%% @doc Collect document IDs matching a path prefix
%% MaxCount limits collection for early termination (use infinity for no limit)
collect_docids_for_prefix(StoreRef, DbName, PathPrefix, infinity) ->
    %% Use short_range profile - prefix scans have unknown but typically moderate size
    barrel_ars_index:fold_path(
        StoreRef, DbName, PathPrefix,
        fun({_Path, DocId}, Acc) -> {ok, [DocId | Acc]} end,
        [],
        short_range
    );
collect_docids_for_prefix(StoreRef, DbName, PathPrefix, MaxCount) when is_integer(MaxCount) ->
    %% Limited prefix scan with early termination
    Profile = select_read_profile(MaxCount),
    {_, DocIds} = barrel_ars_index:fold_path(
        StoreRef, DbName, PathPrefix,
        fun({_Path, DocId}, {Count, Acc}) ->
            case Count >= MaxCount of
                true -> {stop, {Count, Acc}};
                false -> {ok, {Count + 1, [DocId | Acc]}}
            end
        end,
        {0, []},
        Profile
    ),
    DocIds.

%% @doc Filter results by remaining conditions and apply projections.
%% Automatically chooses between streaming, pipelined, or batch fetch based on:
%% - Number of DocIds vs limit (streaming when we have 3x+ more than needed)
%% - Presence of ORDER BY (no streaming with ORDER BY, need all results for sorting)
%% - Large unbounded queries use pipelined execution for better throughput
filter_and_project(StoreRef, DbName, DocIds, Plan, Snapshot) ->
    %% Remove duplicates
    UniqueDocIds = lists:usort(DocIds),
    DocCount = length(UniqueDocIds),

    %% Choose strategy: streaming > pipelined > batch
    StreamingCheck = should_use_streaming_fetch(DocCount, Plan),
    PipelineCheck = should_use_pipelining(DocCount, Plan),
    case StreamingCheck of
        true ->
            %% Streaming: fetch in batches with early termination (for limited queries)
            filter_and_project_streamed(StoreRef, DbName, UniqueDocIds, Plan, Snapshot);
        false ->
            case PipelineCheck of
                true ->
                    %% Pipelined: parallel body fetches for large unbounded queries
                    filter_and_project_pipelined(StoreRef, DbName, UniqueDocIds, Plan, Snapshot);
                false ->
                    %% Batch: fetch all at once (original behavior)
                    filter_and_project_batch(StoreRef, DbName, UniqueDocIds, Plan, Snapshot)
            end
    end.

%% @doc Decide whether to use streaming fetch.
%% Returns true when:
%% 1. Query has a limit (unbounded queries need all results)
%% 2. No ORDER BY (need all results for sorting)
%% 3. DocIds count is 3x+ more than limit+offset
should_use_streaming_fetch(DocCount, #query_plan{limit = Limit, offset = Offset, order = Order}) ->
    case {Limit, Order} of
        {undefined, _} ->
            %% No limit - need all results
            false;
        {_, [_ | _]} ->
            %% Has ORDER BY - need all candidates for sorting
            false;
        {L, []} when is_integer(L), DocCount > (L + Offset) * ?STREAM_THRESHOLD_RATIO ->
            %% Has limit, no ORDER BY, many more docs than needed - use streaming
            true;
        _ ->
            false
    end.

%% @doc Original batch fetch strategy - fetches all DocIds at once.
filter_and_project_batch(StoreRef, DbName, UniqueDocIds, Plan, Snapshot) ->
    #query_plan{
        conditions = Conditions,
        bindings = Bindings,
        projections = Projections,
        order = Order,
        limit = Limit,
        offset = Offset,
        include_docs = IncludeDocs,
        decoder_fun = DecoderFun
    } = Plan,

    %% Batch fetch documents using multi_get with snapshot for consistency
    Results0 = batch_fetch_and_filter(StoreRef, DbName, UniqueDocIds,
                                       Conditions, Bindings, Projections, IncludeDocs, DecoderFun, Snapshot),

    %% Apply ordering
    Results1 = apply_order(Results0, Order),

    %% Apply offset and limit
    Results2 = apply_offset_limit(Results1, Offset, Limit),

    %% Get last sequence (for consistency tracking)
    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    {ok, Results2, LastSeq}.

%% @doc Streaming batch fetch - fetches DocIds in small batches with early termination.
%% Stops as soon as we have limit + offset valid results.
%% Uses adaptive batch sizing based on observed filter selectivity.
filter_and_project_streamed(StoreRef, DbName, DocIds, Plan, Snapshot) ->
    #query_plan{
        conditions = Conditions,
        bindings = Bindings,
        projections = Projections,
        limit = Limit,
        offset = Offset,
        include_docs = IncludeDocs,
        decoder_fun = DecoderFun
    } = Plan,

    %% Calculate target: how many valid results we need
    Target = Limit + Offset,

    %% Initial batch size with over-collect factor for filter conditions
    InitialBatchSize = calculate_initial_batch_size(Target, Conditions),

    %% Stream batches until we have enough
    {Results, _Stats} = stream_batches(
        DbName, DocIds,
        Conditions, Bindings, Projections, IncludeDocs, DecoderFun, Snapshot,
        Target, InitialBatchSize, [], #stream_stats{}
    ),

    %% Apply offset and limit (no ORDER BY in streaming path)
    FinalResults = apply_offset_limit(Results, Offset, Limit),

    %% Get last sequence
    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    {ok, FinalResults, LastSeq}.

%% @doc Stream batches with early termination.
stream_batches(_DbName, [], _Conds, _Bindings, _Projs, _IncludeDocs, _DecoderFun,
               _Snapshot, _Target, _BatchSize, Acc, Stats) ->
    %% Exhausted all DocIds
    {lists:reverse(Acc), Stats};
stream_batches(_DbName, _DocIds, _Conds, _Bindings, _Projs, _IncludeDocs, _DecoderFun,
               _Snapshot, Target, _BatchSize, Acc, Stats) when length(Acc) >= Target ->
    %% Reached target - stop early (this is the key optimization!)
    {lists:reverse(Acc), Stats};
stream_batches(DbName, DocIds, Conds, Bindings, Projs, IncludeDocs, DecoderFun,
               Snapshot, Target, BatchSize, Acc, Stats) ->
    %% Take next batch
    {Batch, Remaining} = safe_split(BatchSize, DocIds),

    %% Select read profile based on batch size
    Profile = select_body_read_profile(length(Batch)),

    %% Fetch and filter this batch
    {BatchResults, DecodedCount} = fetch_and_filter_batch_streaming(
        DbName, Batch, Conds, Bindings, Projs, IncludeDocs, DecoderFun, Snapshot, Profile
    ),

    %% Update running stats
    MatchedCount = length(BatchResults),
    NewStats = update_stream_stats(Stats, length(Batch), DecodedCount, MatchedCount),

    %% Calculate next batch size based on observed selectivity
    Needed = Target - length(Acc) - MatchedCount,
    NextBatchSize = calculate_next_batch_size(Needed, NewStats),

    %% Continue with remaining DocIds
    stream_batches(DbName, Remaining, Conds, Bindings, Projs, IncludeDocs, DecoderFun,
                   Snapshot, Target, NextBatchSize, BatchResults ++ Acc, NewStats).

%% @doc Fetch and filter a single batch for streaming.
%% Returns {Results, DecodedCount}.
fetch_and_filter_batch_streaming(DbName, DocIds, Conds, Bindings, Projs,
                                  IncludeDocs, DecoderFun, Snapshot, Profile) ->
    case DocIds of
        [] ->
            {[], 0};
        _ ->
            %% Use snapshot-aware fetch with read profile
            %% Snapshot is always valid in this code path (created by execute/3)
            DocBodyResults = barrel_doc_body_store:multi_get_current_bodies_with_snapshot(
                DbName, DocIds, Snapshot, Profile
            ),

            %% Process each doc
            Pairs = lists:zip(DocIds, DocBodyResults),
            FilterFun = fun({DocId, BodyResult}) ->
                process_doc_body(DocId, BodyResult, Conds, Bindings, Projs, IncludeDocs, DecoderFun)
            end,
            Results = lists:filtermap(FilterFun, Pairs),

            %% Count successful decodes (not not_found or errors)
            DecodedCount = length([ok || {ok, _} <- DocBodyResults]),

            {Results, DecodedCount}
    end.

%% @doc Calculate initial batch size based on target and conditions.
calculate_initial_batch_size(Target, []) ->
    %% No filter conditions - all docs will match
    min(?STREAM_MAX_BATCH, max(?STREAM_MIN_BATCH, Target + 10));
calculate_initial_batch_size(Target, _Conditions) ->
    %% Has filter conditions - over-collect assuming 50% selectivity
    EstimatedNeeded = round(Target / ?STREAM_INITIAL_SELECTIVITY),
    min(?STREAM_MAX_BATCH, max(?STREAM_MIN_BATCH, EstimatedNeeded)).

%% @doc Calculate next batch size based on observed selectivity.
calculate_next_batch_size(Needed, _Stats) when Needed =< 0 ->
    0;
calculate_next_batch_size(Needed, #stream_stats{selectivity = Selectivity}) ->
    %% Adjust for observed selectivity with 20% safety margin
    SafeSelectivity = max(0.1, Selectivity),  %% Floor at 10%
    EstimatedNeeded = round(Needed / SafeSelectivity * 1.2),
    min(?STREAM_MAX_BATCH, max(?STREAM_MIN_BATCH, EstimatedNeeded)).

%% @doc Update streaming stats after processing a batch.
update_stream_stats(Stats, FetchedCount, DecodedCount, MatchedCount) ->
    #stream_stats{fetched = F, decoded = D, matched = M} = Stats,
    TotalDecoded = D + DecodedCount,
    TotalMatched = M + MatchedCount,
    NewSelectivity = case TotalDecoded > 0 of
        true -> TotalMatched / TotalDecoded;
        false -> ?STREAM_INITIAL_SELECTIVITY
    end,
    #stream_stats{
        fetched = F + FetchedCount,
        decoded = TotalDecoded,
        matched = TotalMatched,
        selectivity = NewSelectivity
    }.

%% @doc Select read profile based on batch size.
select_body_read_profile(BatchSize) when BatchSize =< 50 ->
    point;
select_body_read_profile(BatchSize) when BatchSize =< 200 ->
    short_range;
select_body_read_profile(_) ->
    long_scan.

%% @doc Safe split that handles lists shorter than N.
safe_split(N, List) when length(List) =< N ->
    {List, []};
safe_split(N, List) ->
    lists:split(N, List).

%% @doc Fetch documents for chunked query results when include_docs is true.
%% Takes a list of doc IDs and returns results with doc bodies included.
maybe_fetch_docs_chunked(_DbName, DocIds, false, _DecoderFun) ->
    %% No include_docs - just return ID-only results
    [#{<<"id">> => DocId} || DocId <- DocIds];
maybe_fetch_docs_chunked(DbName, DocIds, true, DecoderFun) ->
    case DocIds of
        [] -> [];
        _ ->
            %% Fetch document bodies
            DocBodyResults = barrel_doc_body_store:multi_get_current_bodies(DbName, DocIds),
            Pairs = lists:zip(DocIds, DocBodyResults),
            lists:filtermap(
                fun({DocId, {ok, CborBin}}) ->
                    Doc = case DecoderFun of
                        undefined -> barrel_docdb_codec_cbor:decode_any(CborBin);
                        Fun -> Fun(CborBin)
                    end,
                    {true, #{<<"id">> => DocId, <<"doc">> => Doc}};
                   ({DocId, not_found}) ->
                    %% Doc deleted - return ID only with deleted flag
                    {true, #{<<"id">> => DocId, <<"deleted">> => true}};
                   ({_DocId, {error, _}}) ->
                    false
                end,
                Pairs)
    end.

%% @doc Batch fetch documents using direct body fetch (no entity lookup needed).
%% With current body stored without revision key, we can skip the entity fetch entirely
%% for include_docs queries, providing a major performance improvement.
%%
%% For large result sets (>50 docs), uses parallel decode + condition matching.
batch_fetch_and_filter(_StoreRef, DbName, DocIds, Conditions, Bindings, Projections, IncludeDocs, DecoderFun, _Snapshot) ->
    case DocIds of
        [] -> [];
        _ ->
            %% Direct body fetch - no entity lookup needed!
            %% Current body is stored at doc_body(DbName, DocId) without revision
            T0 = erlang:monotonic_time(microsecond),
            DocBodyResults = barrel_doc_body_store:multi_get_current_bodies(DbName, DocIds),
            T1 = erlang:monotonic_time(microsecond),
            put(profile_docbody_fetch, pdict_get(profile_docbody_fetch, 0) + (T1 - T0)),

            %% Decode and condition matching + projection
            %% Use parallel processing for large result sets
            T2 = erlang:monotonic_time(microsecond),
            Pairs = lists:zip(DocIds, DocBodyResults),
            FilterFun = fun({DocId, BodyResult}) ->
                process_doc_body(DocId, BodyResult, Conditions, Bindings, Projections, IncludeDocs, DecoderFun)
            end,
            Results = case length(DocIds) of
                N when N > 100 ->
                    %% Parallel decode + filter for batches > 100 docs
                    %% Pool-based execution gives 1.9-6x speedup
                    %% Use max 4 workers (like PostgreSQL's conservative default)
                    barrel_parallel:pfiltermap(FilterFun, Pairs, 4);
                _ ->
                    %% Sequential for small batches
                    lists:filtermap(FilterFun, Pairs)
            end,
            T3 = erlang:monotonic_time(microsecond),
            put(profile_deser_match, pdict_get(profile_deser_match, 0) + (T3 - T2)),
            Results
    end.

%% @private Process a single document body for filtermap
process_doc_body(DocId, {ok, CborBin}, Conditions, Bindings, Projections, IncludeDocs, DecoderFun) ->
    %% Decode to map (handles both indexed and plain CBOR)
    Doc = barrel_docdb_codec_cbor:decode_any(CborBin),
    case matches_conditions(Doc, Conditions, Bindings) of
        {true, BoundVars} ->
            Result = project_result(CborBin, Doc, DocId, Projections, BoundVars, IncludeDocs, DecoderFun),
            {true, Result};
        false ->
            false
    end;
process_doc_body(_DocId, not_found, _Conditions, _Bindings, _Projections, _IncludeDocs, _DecoderFun) ->
    %% Doc was deleted or doesn't exist
    false;
process_doc_body(_DocId, {error, _}, _Conditions, _Bindings, _Projections, _IncludeDocs, _DecoderFun) ->
    false.

%% @doc Check if a document matches all conditions
matches_conditions(Doc, Conditions, InitialBindings) ->
    matches_conditions(Doc, Conditions, InitialBindings, #{}).

matches_conditions(_Doc, [], _Bindings, BoundVars) ->
    {true, BoundVars};
matches_conditions(Doc, [Condition | Rest], Bindings, BoundVars) ->
    case match_condition(Doc, Condition, Bindings, BoundVars) of
        {true, NewBoundVars} ->
            matches_conditions(Doc, Rest, Bindings, NewBoundVars);
        false ->
            false
    end.

match_condition(Doc, {path, Path, Value}, _Bindings, BoundVars) ->
    case get_path_value(Doc, Path) of
        {ok, DocValue} ->
            case is_logic_var(Value) of
                true ->
                    %% Bind the variable
                    {true, BoundVars#{Value => DocValue}};
                false ->
                    case DocValue =:= Value of
                        true -> {true, BoundVars};
                        false -> false
                    end
            end;
        not_found ->
            false
    end;

match_condition(Doc, {compare, Path, Op, Value}, _Bindings, BoundVars) ->
    case get_path_value(Doc, Path) of
        {ok, DocValue} ->
            CompareValue = case is_logic_var(Value) of
                true -> maps:get(Value, BoundVars, undefined);
                false -> Value
            end,
            case compare_values(DocValue, Op, CompareValue) of
                true -> {true, BoundVars};
                false -> false
            end;
        not_found ->
            false
    end;

match_condition(Doc, {'and', Conditions}, Bindings, BoundVars) ->
    matches_conditions(Doc, Conditions, Bindings, BoundVars);

match_condition(Doc, {'or', Conditions}, Bindings, BoundVars) ->
    match_any(Doc, Conditions, Bindings, BoundVars);

match_condition(Doc, {'not', Condition}, Bindings, BoundVars) ->
    case match_condition(Doc, Condition, Bindings, BoundVars) of
        {true, _} -> false;
        false -> {true, BoundVars}
    end;

match_condition(Doc, {in, Path, Values}, _Bindings, BoundVars) ->
    case get_path_value(Doc, Path) of
        {ok, DocValue} ->
            case lists:member(DocValue, Values) of
                true -> {true, BoundVars};
                false -> false
            end;
        not_found ->
            false
    end;

match_condition(Doc, {contains, Path, Value}, _Bindings, BoundVars) ->
    case get_path_value(Doc, Path) of
        {ok, DocValue} when is_list(DocValue) ->
            case lists:member(Value, DocValue) of
                true -> {true, BoundVars};
                false -> false
            end;
        _ ->
            false
    end;

match_condition(Doc, {exists, Path}, _Bindings, BoundVars) ->
    case get_path_value(Doc, Path) of
        {ok, _} -> {true, BoundVars};
        not_found -> false
    end;

match_condition(Doc, {missing, Path}, _Bindings, BoundVars) ->
    case get_path_value(Doc, Path) of
        {ok, _} -> false;
        not_found -> {true, BoundVars}
    end;

match_condition(Doc, {regex, Path, Pattern}, _Bindings, BoundVars) ->
    case get_path_value(Doc, Path) of
        {ok, DocValue} when is_binary(DocValue) ->
            case re:run(DocValue, Pattern) of
                {match, _} -> {true, BoundVars};
                nomatch -> false
            end;
        _ ->
            false
    end;

match_condition(Doc, {prefix, Path, Prefix}, _Bindings, BoundVars) ->
    case get_path_value(Doc, Path) of
        {ok, DocValue} when is_binary(DocValue) ->
            PrefixLen = byte_size(Prefix),
            case DocValue of
                <<Prefix:PrefixLen/binary, _/binary>> -> {true, BoundVars};
                _ -> false
            end;
        _ ->
            false
    end;

match_condition(_Doc, {error, _}, _Bindings, _BoundVars) ->
    false.

match_any(_Doc, [], _Bindings, _BoundVars) ->
    false;
match_any(Doc, [Condition | Rest], Bindings, BoundVars) ->
    case match_condition(Doc, Condition, Bindings, BoundVars) of
        {true, NewBoundVars} -> {true, NewBoundVars};
        false -> match_any(Doc, Rest, Bindings, BoundVars)
    end.

%% @doc Get a value from a document at the given path
get_path_value(Doc, []) ->
    {ok, Doc};
get_path_value(Doc, [Key | Rest]) when is_map(Doc), is_binary(Key) ->
    case maps:find(Key, Doc) of
        {ok, Value} -> get_path_value(Value, Rest);
        error -> not_found
    end;
get_path_value(Doc, [Index | Rest]) when is_list(Doc), is_integer(Index) ->
    case Index < length(Doc) of
        true ->
            Value = lists:nth(Index + 1, Doc),  % 0-based to 1-based
            get_path_value(Value, Rest);
        false ->
            not_found
    end;
get_path_value(_, _) ->
    not_found.

%% @doc Compare two values with an operator
compare_values(A, '>', B) when is_number(A), is_number(B) -> A > B;
compare_values(A, '<', B) when is_number(A), is_number(B) -> A < B;
compare_values(A, '>=', B) when is_number(A), is_number(B) -> A >= B;
compare_values(A, '=<', B) when is_number(A), is_number(B) -> A =< B;
compare_values(A, '=/=', B) -> A =/= B;
compare_values(A, '==', B) -> A =:= B;
compare_values(A, '>', B) when is_binary(A), is_binary(B) -> A > B;
compare_values(A, '<', B) when is_binary(A), is_binary(B) -> A < B;
compare_values(A, '>=', B) when is_binary(A), is_binary(B) -> A >= B;
compare_values(A, '=<', B) when is_binary(A), is_binary(B) -> A =< B;
compare_values(_, _, _) -> false.

%% @doc Project result fields from document
%% CborBin is the raw binary, Doc is decoded map (for projections)
%% DecoderFun formats the doc for include_docs output
project_result(CborBin, Doc, DocId, Projections, BoundVars, IncludeDocs, DecoderFun) ->
    Result0 = #{<<"id">> => DocId},

    Result1 = case IncludeDocs of
        true ->
            FormattedDoc = DecoderFun(CborBin),
            Result0#{<<"doc">> => FormattedDoc};
        false -> Result0
    end,

    %% Add projected fields/variables
    lists:foldl(
        fun('*', Acc) ->
            %% Include all bound variables
            maps:fold(fun(Var, Val, A) ->
                VarName = atom_to_binary(Var, utf8),
                A#{VarName => Val}
            end, Acc, BoundVars);
           (Var, Acc) when is_atom(Var) ->
            case is_logic_var(Var) of
                true ->
                    VarName = atom_to_binary(Var, utf8),
                    case maps:find(Var, BoundVars) of
                        {ok, Val} -> Acc#{VarName => Val};
                        error -> Acc
                    end;
                false ->
                    Acc
            end;
           (Path, Acc) when is_list(Path) ->
            case get_path_value(Doc, Path) of
                {ok, Val} ->
                    PathKey = path_to_key(Path),
                    Acc#{PathKey => Val};
                not_found ->
                    Acc
            end
        end,
        Result1,
        Projections
    ).

path_to_key(Path) ->
    iolist_to_binary(lists:join(<<"/">>, [to_bin(P) || P <- Path])).

to_bin(B) when is_binary(B) -> B;
to_bin(N) when is_integer(N) -> integer_to_binary(N).

%% @doc Apply ordering to results
apply_order(Results, []) ->
    Results;
apply_order(Results, [{Field, Dir} | _Rest]) ->
    %% Simple single-field ordering for now
    Sorted = lists:sort(
        fun(A, B) ->
            ValA = get_sort_value(A, Field),
            ValB = get_sort_value(B, Field),
            case Dir of
                asc -> ValA =< ValB;
                desc -> ValA >= ValB
            end
        end,
        Results
    ),
    Sorted.

get_sort_value(Result, Field) when is_atom(Field) ->
    FieldKey = atom_to_binary(Field, utf8),
    maps:get(FieldKey, Result, null);
get_sort_value(Result, Path) when is_list(Path) ->
    PathKey = path_to_key(Path),
    maps:get(PathKey, Result, null).

%% @doc Apply offset and limit
apply_offset_limit(Results, Offset, Limit) ->
    Results1 = case Offset > 0 of
        true -> lists:nthtail(min(Offset, length(Results)), Results);
        false -> Results
    end,
    case Limit of
        undefined -> Results1;
        N -> lists:sublist(Results1, N)
    end.

%% @doc Find first equality condition
find_first_equality([]) ->
    not_found;
find_first_equality([{path, Path, Value} | _]) when not is_atom(Value) ->
    {ok, {path, Path, Value}};
find_first_equality([{path, Path, Value} | Rest]) ->
    case is_logic_var(Value) of
        false -> {ok, {path, Path, Value}};
        true -> find_first_equality(Rest)
    end;
find_first_equality([{'and', Nested} | Rest]) ->
    case find_first_equality(Nested) of
        {ok, _} = Result -> Result;
        not_found -> find_first_equality(Rest)
    end;
find_first_equality([_ | Rest]) ->
    find_first_equality(Rest).

%% @doc Find all index-friendly conditions (equality, compare, exists, prefix)
find_all_index_conditions(Conditions) ->
    find_all_index_conditions(Conditions, []).

find_all_index_conditions([], Acc) ->
    lists:reverse(Acc);
find_all_index_conditions([{path, Path, Value} | Rest], Acc) ->
    case is_logic_var(Value) of
        false ->
            find_all_index_conditions(Rest, [{path, Path, Value} | Acc]);
        true ->
            find_all_index_conditions(Rest, Acc)
    end;
find_all_index_conditions([{compare, Path, Op, Value} | Rest], Acc)
  when Op =:= '>' orelse Op =:= '<' orelse Op =:= '>=' orelse Op =:= '=<' ->
    case is_logic_var(Value) of
        false ->
            find_all_index_conditions(Rest, [{compare, Path, Op, Value} | Acc]);
        true ->
            find_all_index_conditions(Rest, Acc)
    end;
find_all_index_conditions([{exists, Path} | Rest], Acc) ->
    find_all_index_conditions(Rest, [{exists, Path} | Acc]);
find_all_index_conditions([{prefix, Path, Prefix} | Rest], Acc) ->
    find_all_index_conditions(Rest, [{prefix, Path, Prefix} | Acc]);
find_all_index_conditions([{'and', Nested} | Rest], Acc) ->
    NestedConds = find_all_index_conditions(Nested),
    find_all_index_conditions(Rest, NestedConds ++ Acc);
find_all_index_conditions([_ | Rest], Acc) ->
    find_all_index_conditions(Rest, Acc).

%% @doc Find best path for scanning
find_best_scan_path([]) ->
    not_found;
find_best_scan_path([{path, Path, _} | _]) ->
    {ok, Path};
find_best_scan_path([{compare, Path, _, _} | _]) ->
    {ok, Path};
find_best_scan_path([{prefix, Path, _} | _]) ->
    {ok, Path};
find_best_scan_path([{'and', Nested} | Rest]) ->
    case find_best_scan_path(Nested) of
        {ok, _} = Result -> Result;
        not_found -> find_best_scan_path(Rest)
    end;
find_best_scan_path([_ | Rest]) ->
    find_best_scan_path(Rest).


%%====================================================================
%% Pipelined Execution
%%====================================================================

%% @doc Execute query with pipelined index iteration and body fetching.
%% Overlaps index iteration with body fetching for better throughput.
%%
%% Architecture:
%% - Main process iterates index in batches
%% - For each batch, spawns async body fetch process
%% - Continues iterating while body fetches are in progress
%% - Collects results at the end
%%
%% Benefits:
%% - Overlaps I/O: index iteration and body fetch happen concurrently
%% - Better resource utilization: multiple RocksDB reads in flight
%% - Reduced total latency for large result sets
%% - Bounded concurrency: limits memory usage with concurrent queries
filter_and_project_pipelined(StoreRef, DbName, DocIds, Plan, Snapshot) ->
    #query_plan{
        conditions = Conditions,
        bindings = Bindings,
        projections = Projections,
        order = Order,
        limit = Limit,
        offset = Offset,
        include_docs = IncludeDocs,
        decoder_fun = DecoderFun
    } = Plan,

    %% Split DocIds into batches
    UniqueDocIds = lists:usort(DocIds),
    Batches = split_into_batches(UniqueDocIds, ?PIPELINE_BATCH_SIZE),
    NumBatches = length(Batches),

    %% Use bounded concurrency to limit memory with concurrent queries
    %% Spawn initial batch of fetchers (up to MAX_INFLIGHT)
    Parent = self(),
    FetchCtx = #{
        db_name => DbName,
        conditions => Conditions,
        bindings => Bindings,
        projections => Projections,
        include_docs => IncludeDocs,
        decoder_fun => DecoderFun,
        snapshot => Snapshot,
        parent => Parent
    },

    %% Execute with bounded concurrency
    Results0 = execute_bounded_pipeline(Batches, FetchCtx, NumBatches),

    %% Results are already in order (collected in batch order)

    %% Apply ordering if needed
    Results1 = apply_order(Results0, Order),

    %% Apply offset and limit
    Results2 = apply_offset_limit(Results1, Offset, Limit),

    %% Get last sequence
    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    {ok, Results2, LastSeq}.

%% @doc Execute pipeline with bounded concurrency.
%% Only spawns up to MAX_INFLIGHT fetchers at a time.
%% As each completes, spawns the next batch if available.
execute_bounded_pipeline(Batches, FetchCtx, NumBatches) ->
    %% Create indexed batch list
    IndexedBatches = lists:zip(lists:seq(1, NumBatches), Batches),

    %% Spawn initial fetchers (up to MAX_INFLIGHT)
    {InitialBatches, RemainingBatches} = safe_split(?PIPELINE_MAX_INFLIGHT, IndexedBatches),
    InitialRefs = spawn_fetchers(InitialBatches, FetchCtx),

    %% Collect results with bounded spawning
    collect_bounded_results(InitialRefs, RemainingBatches, FetchCtx, #{}, NumBatches).

%% @doc Spawn fetchers for a list of indexed batches.
spawn_fetchers(IndexedBatches, FetchCtx) ->
    #{parent := Parent} = FetchCtx,
    lists:map(fun({BatchNum, Batch}) ->
        Ref = make_ref(),
        spawn_link(fun() ->
            Results = fetch_batch_for_pipeline_ctx(Batch, FetchCtx),
            Parent ! {pipeline_result, Ref, BatchNum, Results}
        end),
        {Ref, BatchNum}
    end, IndexedBatches).

%% @doc Fetch using context map.
fetch_batch_for_pipeline_ctx(DocIds, FetchCtx) ->
    #{db_name := DbName, conditions := Conditions, bindings := Bindings,
      projections := Projections, include_docs := IncludeDocs,
      decoder_fun := DecoderFun, snapshot := Snapshot} = FetchCtx,
    fetch_batch_for_pipeline(DbName, DocIds, Conditions, Bindings,
                             Projections, IncludeDocs, DecoderFun, Snapshot).

%% @doc Collect results with bounded concurrency.
%% When a fetcher completes, spawn next batch if available.
collect_bounded_results([], [], _FetchCtx, Results, NumBatches) ->
    %% All done - flatten in order
    lists:flatmap(fun(N) ->
        maps:get(N, Results, [])
    end, lists:seq(1, NumBatches));
collect_bounded_results(InFlight, Remaining, FetchCtx, Results, NumBatches) ->
    Timeout = application:get_env(barrel_docdb, query_timeout_ms, 30000),
    receive
        {pipeline_result, Ref, BatchNum, BatchResults} ->
            case lists:keytake(Ref, 1, InFlight) of
                {value, _, RestInFlight} ->
                    NewResults = Results#{BatchNum => BatchResults},
                    case Remaining of
                        [] ->
                            collect_bounded_results(RestInFlight, [], FetchCtx, NewResults, NumBatches);
                        [NextBatch | RestRemaining] ->
                            NewRefs = spawn_fetchers([NextBatch], FetchCtx),
                            collect_bounded_results(NewRefs ++ RestInFlight, RestRemaining, FetchCtx, NewResults, NumBatches)
                    end;
                false ->
                    collect_bounded_results(InFlight, Remaining, FetchCtx, Results, NumBatches)
            end
    after Timeout ->
        Completed = maps:size(Results),
        Missing = NumBatches - Completed,
        logger:warning("barrel_query: pipeline timed out after ~pms; "
                       "~p/~p batches completed, ~p still in flight",
                       [Timeout, Completed, NumBatches, length(InFlight) + length(Remaining)]),
        barrel_metrics:inc_query_timeouts(),
        error({query_timeout, #{completed => Completed,
                                total => NumBatches,
                                missing_batches => Missing,
                                timeout_ms => Timeout}})
    end.

%% @doc Split list into batches of specified size.
split_into_batches(List, BatchSize) ->
    split_into_batches(List, BatchSize, []).

split_into_batches([], _BatchSize, Acc) ->
    lists:reverse(Acc);
split_into_batches(List, BatchSize, Acc) ->
    case length(List) =< BatchSize of
        true ->
            lists:reverse([List | Acc]);
        false ->
            {Batch, Rest} = lists:split(BatchSize, List),
            split_into_batches(Rest, BatchSize, [Batch | Acc])
    end.

%% @doc Fetch and filter a batch for pipelined execution.
fetch_batch_for_pipeline(DbName, DocIds, Conditions, Bindings,
                         Projections, IncludeDocs, DecoderFun, Snapshot) ->
    Profile = select_body_read_profile(length(DocIds)),

    %% Fetch bodies with snapshot (always valid in this code path)
    DocBodyResults = barrel_doc_body_store:multi_get_current_bodies_with_snapshot(
        DbName, DocIds, Snapshot, Profile
    ),

    %% Process each doc
    Pairs = lists:zip(DocIds, DocBodyResults),
    lists:filtermap(fun({DocId, BodyResult}) ->
        process_doc_body(DocId, BodyResult, Conditions, Bindings,
                         Projections, IncludeDocs, DecoderFun)
    end, Pairs).

%% @doc Decide whether to use pipelined execution.
%% Use pipelining when:
%% - Many DocIds to process (>500)
%% - Include docs is true (need body fetch)
%% - No limit or large limit (for small limits, streaming is better)
should_use_pipelining(DocCount, #query_plan{include_docs = IncludeDocs, limit = Limit}) ->
    case IncludeDocs of
        false ->
            %% No body fetch needed, pipelining doesn't help
            false;
        true ->
            case Limit of
                undefined ->
                    %% Unbounded query with many docs
                    DocCount >= ?PIPELINE_MIN_DOCIDS;
                L when is_integer(L), L > 100 ->
                    %% Large limit with many docs
                    DocCount >= ?PIPELINE_MIN_DOCIDS;
                _ ->
                    %% Small limit - streaming is better
                    false
            end
    end.


%%====================================================================
%% Profiling Functions (temporary)
%%====================================================================

%% @doc Get current profiling counters
get_profile() ->
    #{
        index_iter_us => pdict_get(profile_index_iter, 0),
        verify_conds_us => pdict_get(profile_verify_conds, 0),
        docinfo_fetch_us => pdict_get(profile_docinfo_fetch, 0),
        docbody_fetch_us => pdict_get(profile_docbody_fetch, 0),
        deser_match_us => pdict_get(profile_deser_match, 0),
        doc_count => pdict_get(profile_doc_count, 0)
    }.

%% @doc Reset profiling counters
reset_profile() ->
    erase(profile_index_iter),
    erase(profile_verify_conds),
    erase(profile_docinfo_fetch),
    erase(profile_docbody_fetch),
    erase(profile_deser_match),
    erase(profile_doc_count),
    ok.

%% @doc Dump profiling data to console
dump_profile() ->
    Profile = get_profile(),
    Total = maps:get(index_iter_us, Profile) +
            maps:get(verify_conds_us, Profile) +
            maps:get(docinfo_fetch_us, Profile) +
            maps:get(docbody_fetch_us, Profile) +
            maps:get(deser_match_us, Profile),
    io:format("~n=== Query Profile ===~n"),
    io:format("  Index iteration:     ~8.B us (~5.1f%)~n",
              [maps:get(index_iter_us, Profile),
               pct(maps:get(index_iter_us, Profile), Total)]),
    io:format("  Verify conditions:   ~8.B us (~5.1f%)~n",
              [maps:get(verify_conds_us, Profile),
               pct(maps:get(verify_conds_us, Profile), Total)]),
    io:format("  Doc info fetch:      ~8.B us (~5.1f%)~n",
              [maps:get(docinfo_fetch_us, Profile),
               pct(maps:get(docinfo_fetch_us, Profile), Total)]),
    io:format("  Doc body fetch:      ~8.B us (~5.1f%)~n",
              [maps:get(docbody_fetch_us, Profile),
               pct(maps:get(docbody_fetch_us, Profile), Total)]),
    io:format("  Deser + matching:    ~8.B us (~5.1f%)~n",
              [maps:get(deser_match_us, Profile),
               pct(maps:get(deser_match_us, Profile), Total)]),
    io:format("  --------------------------~n"),
    io:format("  Total:               ~8.B us~n", [Total]),
    io:format("  Docs processed:      ~8.B~n", [maps:get(doc_count, Profile)]),
    ok.

pct(_, 0) -> 0.0;
pct(Part, Total) -> (Part / Total) * 100.

