%%%-------------------------------------------------------------------
%%% @doc barrel_rep_alg - Replication algorithm
%%%
%%% Implements the core version-vector replication algorithm: one
%%% `diff_versions' round-trip per batch of changes, then a
%%% `get_doc' + `put_version' per missing document. Conflict handling
%%% lives entirely in the target's `put_version' (fast-forward, ignore,
%%% or record a conflict sibling), so the algorithm itself needs no
%%% ancestor negotiation.
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

%% @doc Replicate a list of changes from source to target.
-spec replicate(term(), term(), module(), module(), [map()]) ->
    {ok, map()} | {error, term()}.
replicate(_Source, _Target, _SourceTransport, _TargetTransport, []) ->
    {ok, new_stats()};
replicate(Source, Target, SourceTransport, TargetTransport, Changes) ->
    ExtraAttrs = #{
        <<"replication.changes_count">> => length(Changes),
        <<"replication.source_transport">> => atom_to_binary(SourceTransport, utf8),
        <<"replication.target_transport">> => atom_to_binary(TargetTransport, utf8)
    },
    barrel_trace:with_db_span(replication, undefined, ExtraAttrs, fun() ->
        %% Advance the target clock past the newest source change so its
        %% change sequence stays strictly increasing across the sync
        _ = maybe_sync_hlc(Target, TargetTransport, lists:last(Changes)),
        %% One diff round-trip for the whole batch
        TokenMap = maps:from_list(
            [{get_value(id, Change), get_value(rev, Change)}
             || Change <- Changes]),
        case TargetTransport:diff_versions(Target, TokenMap) of
            {ok, DiffMap} ->
                Missing = [DocId || {DocId, missing} <- maps:to_list(DiffMap)],
                Stats = lists:foldl(
                    fun(DocId, Acc) ->
                        sync_doc(Source, Target, SourceTransport,
                                 TargetTransport, DocId, Acc)
                    end,
                    new_stats(),
                    Missing),
                {ok, Stats};
            {error, _} = Error ->
                Error
        end
    end).

%% @doc Replicate changes in batches with checkpoint callback
-spec replicate_batch(term(), term(), module(), module(), map()) ->
    {ok, map()} | {error, term()}.
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
            case replicate(Source, Target, SourceTransport, TargetTransport, Changes) of
                {ok, Stats2} ->
                    MergedStats = merge_stats(Stats, Stats2),
                    %% Call checkpoint function
                    ok = CheckpointFun(LastSeq),
                    %% Continue with next batch
                    replicate_loop(Source, Target, SourceTransport, TargetTransport,
                                   LastSeq, Limit, CheckpointFun, MergedStats);
                {error, _} = Error ->
                    Error
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Ship one missing document: read it with its version metadata
%% from the source, apply it on the target.
sync_doc(Source, Target, SourceTransport, TargetTransport, DocId, Stats) ->
    ReadStart = erlang:monotonic_time(microsecond),
    case SourceTransport:get_doc(Source, DocId, #{}) of
        {ok, Doc, #{version := Token, vv := VVBin, deleted := Deleted}} ->
            ReadTime = erlang:monotonic_time(microsecond) - ReadStart,
            Stats2 = update_time_stat(doc_read_time_us, ReadTime,
                                      inc_stat(docs_read, Stats, 1)),
            WriteStart = erlang:monotonic_time(microsecond),
            case TargetTransport:put_version(Target, Doc, Token, VVBin, Deleted) of
                {ok, _DocId, _WinnerToken} ->
                    WriteTime = erlang:monotonic_time(microsecond) - WriteStart,
                    update_time_stat(doc_write_time_us, WriteTime,
                                     inc_stat(docs_written, Stats2, 1));
                {error, Reason} ->
                    logger:error("replicate write error for ~p: ~p",
                                 [DocId, Reason]),
                    inc_stat(doc_write_failures, Stats2, 1)
            end;
        {error, Reason} ->
            %% The doc may have been purged between changes and read
            logger:warning("replicate read error for ~p: ~p", [DocId, Reason]),
            inc_stat(doc_read_failures, Stats, 1)
    end.

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
