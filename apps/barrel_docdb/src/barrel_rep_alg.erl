%%%-------------------------------------------------------------------
%%% @doc barrel_rep_alg - Replication algorithm
%%%
%%% Implements the core replication algorithm for synchronizing
%%% documents between source and target databases.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_rep_alg).

-include("barrel_docdb.hrl").

%% API
-export([
    replicate/5,
    replicate_batch/5
]).

%%====================================================================
%% API
%%====================================================================

%% @doc Replicate a list of changes from source to target
-spec replicate(term(), term(), module(), module(), [map()]) ->
    {ok, map()}.
replicate(Source, Target, SourceTransport, TargetTransport, Changes) ->
    ExtraAttrs = #{
        <<"replication.changes_count">> => length(Changes),
        <<"replication.source_transport">> => atom_to_binary(SourceTransport, utf8),
        <<"replication.target_transport">> => atom_to_binary(TargetTransport, utf8)
    },
    barrel_trace:with_db_span(replication, undefined, ExtraAttrs, fun() ->
        Stats = new_stats(),
        {ok, Stats2} = lists:foldl(
            fun(Change, {ok, Acc}) ->
                %% Sync HLC from change to target (for distributed ordering)
                _ = maybe_sync_hlc(Target, TargetTransport, Change),
                sync_change(Source, Target, SourceTransport, TargetTransport, Change, Acc)
            end,
            {ok, Stats},
            Changes
        ),
        {ok, Stats2}
    end).

%% @doc Replicate changes in batches with checkpoint callback
-spec replicate_batch(term(), term(), module(), module(), map()) ->
    {ok, map()}.
replicate_batch(Source, Target, SourceTransport, TargetTransport, Opts) ->
    Since = maps:get(since, Opts, first),
    Limit = maps:get(batch_size, Opts, 100),
    CheckpointFun = maps:get(checkpoint_fun, Opts, fun(_) -> ok end),

    Stats = new_stats(),
    replicate_loop(Source, Target, SourceTransport, TargetTransport, Since, Limit, CheckpointFun, Stats).

%%====================================================================
%% Internal functions
%%====================================================================

%% @doc Main replication loop
replicate_loop(Source, Target, SourceTransport, TargetTransport, Since, Limit, CheckpointFun, Stats) ->
    GetOpts = #{limit => Limit},
    case SourceTransport:get_changes(Source, Since, GetOpts) of
        {ok, [], _LastSeq} ->
            %% No more changes
            {ok, Stats};
        {ok, Changes, LastSeq} ->
            %% Process this batch
            {ok, Stats2} = replicate(Source, Target, SourceTransport, TargetTransport, Changes),
            MergedStats = merge_stats(Stats, Stats2),

            %% Call checkpoint function
            ok = CheckpointFun(LastSeq),

            %% Continue with next batch
            replicate_loop(Source, Target, SourceTransport, TargetTransport, LastSeq, Limit, CheckpointFun, MergedStats);
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Sync a single change
sync_change(Source, Target, SourceTransport, TargetTransport, Change, Stats) ->
    DocId = get_value(id, Change),
    ChangeRevs = get_value(changes, Change, []),

    %% Extract revision IDs from changes
    RevIds = [get_value(rev, R) || R <- ChangeRevs],

    %% Find which revisions target is missing
    case TargetTransport:revsdiff(Target, DocId, RevIds) of
        {ok, [], _Ancestors} ->
            %% Target has all revisions
            {ok, Stats};
        {ok, MissingRevisions, _Ancestors} ->
            %% Sync each missing revision
            Stats2 = lists:foldl(
                fun(Revision, Acc) ->
                    sync_revision(Source, Target, SourceTransport, TargetTransport, DocId, Revision, Acc)
                end,
                Stats,
                MissingRevisions
            ),
            {ok, Stats2};
        {error, _} = Error ->
            logger:warning("revsdiff failed for ~s: ~p", [DocId, Error]),
            {ok, inc_stat(doc_read_failures, Stats, 1)}
    end.

%% @doc Sync a single revision
sync_revision(Source, Target, SourceTransport, TargetTransport, DocId, Revision, Stats) ->
    case read_doc_with_history(Source, SourceTransport, DocId, Revision, Stats) of
        {undefined, undefined, Stats2} ->
            Stats2;
        {Doc, Meta, Stats2} ->
            History = parse_revisions(Meta),
            Deleted = maps:get(<<"deleted">>, Meta, false),
            write_doc(Target, TargetTransport, Doc, History, Deleted, Stats2)
    end.

%% @doc Read document with revision history from source
read_doc_with_history(Source, SourceTransport, DocId, Rev, Stats) ->
    StartTime = erlang:monotonic_time(microsecond),
    case SourceTransport:get_doc(Source, DocId, #{rev => Rev, history => true}) of
        {ok, Doc, Meta} ->
            Time = erlang:monotonic_time(microsecond) - StartTime,
            Stats2 = inc_stat(docs_read, Stats, 1),
            Stats3 = update_time_stat(doc_read_time_us, Time, Stats2),
            {Doc, Meta, Stats3};
        {error, _} ->
            Stats2 = inc_stat(doc_read_failures, Stats, 1),
            {undefined, undefined, Stats2}
    end.

%% @doc Write document with history to target
write_doc(_Target, _TargetTransport, undefined, _History, _Deleted, Stats) ->
    Stats;
write_doc(Target, TargetTransport, Doc, History, Deleted, Stats) ->
    StartTime = erlang:monotonic_time(microsecond),
    case TargetTransport:put_rev(Target, Doc, History, Deleted) of
        {ok, _DocId, _Rev} ->
            Time = erlang:monotonic_time(microsecond) - StartTime,
            Stats2 = inc_stat(docs_written, Stats, 1),
            update_time_stat(doc_write_time_us, Time, Stats2);
        {error, Reason} ->
            DocId = maps:get(<<"id">>, Doc, undefined),
            logger:error("replicate write error for ~p: ~p", [DocId, Reason]),
            inc_stat(doc_write_failures, Stats, 1)
    end.

%% @doc Parse revisions from metadata
parse_revisions(#{<<"revisions">> := Revisions}) ->
    Start = maps:get(<<"start">>, Revisions),
    Ids = maps:get(<<"ids">>, Revisions),
    lists:zipwith(
        fun(Gen, Hash) ->
            iolist_to_binary([integer_to_binary(Gen), "-", Hash])
        end,
        lists:seq(Start, Start - length(Ids) + 1, -1),
        Ids
    );
parse_revisions(#{<<"rev">> := Rev}) ->
    [Rev];
parse_revisions(_) ->
    [].

%%====================================================================
%% Stats management
%%====================================================================

new_stats() ->
    #{
        docs_read => 0,
        doc_read_failures => 0,
        docs_written => 0,
        doc_write_failures => 0,
        doc_read_time_us => 0,
        doc_write_time_us => 0
    }.

inc_stat(Key, Stats, N) ->
    maps:update_with(Key, fun(V) -> V + N end, Stats).

update_time_stat(Key, Time, Stats) ->
    maps:update_with(Key, fun(V) -> V + Time end, Stats).

merge_stats(Stats1, Stats2) ->
    maps:merge_with(fun(_K, V1, V2) -> V1 + V2 end, Stats1, Stats2).

%% @doc Get value from map, trying atom and binary keys
get_value(Key, Map) ->
    get_value(Key, Map, undefined).

get_value(Key, Map, Default) when is_atom(Key) ->
    BinKey = atom_to_binary(Key, utf8),
    case maps:get(Key, Map, undefined) of
        undefined -> maps:get(BinKey, Map, Default);
        Value -> Value
    end.

%% @doc Sync HLC from change to target if present
%% This ensures the target database clock reflects causality with source events.
maybe_sync_hlc(Target, TargetTransport, Change) ->
    case get_value(hlc, Change) of
        undefined ->
            ok;
        Hlc ->
            %% Try to sync - ignore errors (transport may not support it)
            try
                _ = TargetTransport:sync_hlc(Target, Hlc),
                ok
            catch
                error:undef -> ok;
                _:_ -> ok
            end
    end.
