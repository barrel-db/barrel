%% @doc The attachment phase of a replication run. Runs AFTER the doc
%% loop (independent lifecycles, independent checkpoint): fold the
%% source's attachment feed since the attachment checkpoint, diff each
%% batch against the target in one round trip (origin, operation and
%% digest under last-write-wins), then apply the entries in feed order:
%% stream the missing blobs (digest-verified at the target's commit
%% point), touch the ones whose bytes the target already holds, apply
%% the deletes.
%%
%% The checkpoint moves only to the end of the entries applied without
%% a failure, and only once the target synced them (att_durability
%% `required', the default; `best_effort' skips the sync and a power
%% loss can then lose a checkpointed batch). A filter applies to the
%% attachment feed as it does to documents.
%%
%% Degrades to `skipped' when either transport (or the underlying
%% attachment backend) has no feed support. Redelivery is safe
%% end-to-end: the diff answers `have', and the origin-HLC guard turns
%% replays into `{ok, ignored}'.
-module(barrel_rep_att).

-include("barrel_docdb.hrl").

-export([sync/5, sync_from/7, prepare/5, sync_docs/2, reconcile/5]).

-define(DEFAULT_BATCH, 100).

-spec sync(term(), term(), module(), module(), map()) ->
    {ok, map()} | skipped | {error, term()}.
sync(Source, Target, SourceTransport, TargetTransport, Opts) ->
    case supports(SourceTransport) andalso supports(TargetTransport) of
        false ->
            skipped;
        true ->
            do_sync(Source, Target, SourceTransport, TargetTransport, Opts)
    end.

supports(Transport) ->
    exports(Transport, att_changes, 3).

exports(Transport, Fun, Arity) ->
    _ = code:ensure_loaded(Transport),
    erlang:function_exported(Transport, Fun, Arity).

do_sync(Source, Target, SourceTransport, TargetTransport, Opts) ->
    RepId = maps:get(rep_id, Opts),
    Checkpoint = barrel_rep_checkpoint:new(#{
        id => <<"att-", RepId/binary>>,
        source => Source,
        target => Target,
        source_transport => SourceTransport,
        target_transport => TargetTransport,
        options => Opts,
        %% 2.0 checkpointed past failed transfers
        reset_legacy => true
    }),
    case barrel_rep_checkpoint:read_error(Checkpoint) of
        {Side, Reason} ->
            %% an unknown position must not restart the feed from first
            {error, {checkpoint_unreadable, Side, Reason}};
        undefined ->
            StartSeq0 = barrel_rep_checkpoint:get_start_seq(Checkpoint),
            StartSeq = maybe_reset_on_att_floor(StartSeq0, Source,
                                                SourceTransport, RepId),
            %% each call persists the given seq under the run's session
            CheckpointFun = fun(Seq) ->
                barrel_rep_checkpoint:write_checkpoint(
                    barrel_rep_checkpoint:set_last_seq(Seq, Checkpoint))
            end,
            run(Source, Target, SourceTransport, TargetTransport, StartSeq,
                CheckpointFun, Opts)
    end.

%% @doc Run the attachment phase from an explicit since with a
%% caller-supplied checkpoint fun (invoked with the seq reached, after
%% the target synced it; it returns ok or an error that ends the run).
%% This is the timeline merge and task entry point; sync/5 wraps it
%% with the replication-checkpoint machinery.
-spec sync_from(term(), term(), module(), module(),
                barrel_hlc:timestamp() | first,
                fun((barrel_hlc:timestamp() | first) -> ok | {error, term()}),
                map()) ->
    {ok, map()} | skipped | {error, term()}.
sync_from(Source, Target, SourceTransport, TargetTransport, Since,
          CheckpointFun, Opts) ->
    case supports(SourceTransport) andalso supports(TargetTransport) of
        false ->
            skipped;
        true ->
            run(Source, Target, SourceTransport, TargetTransport,
                Since, CheckpointFun, Opts)
    end.

%% @doc Check options and endpoint capabilities once, before any
%% mutation, for sync_docs/2.
-spec prepare(term(), term(), module(), module(), map()) ->
    {ok, map()} | skipped | {error, term()}.
prepare(Source, Target, SourceTransport, TargetTransport, Opts) ->
    case supports(SourceTransport) andalso supports(TargetTransport) of
        false -> skipped;
        true -> context(Source, Target, SourceTransport, TargetTransport, Opts)
    end.

%% @doc Diff and send the current attachments of documents the doc
%% phase copied (a filtered run): their feed rows may have been passed
%% while the documents were out of scope. Synced before it returns.
-spec sync_docs(map() | skipped, [docid()]) ->
    {ok, map()} | {error, term()}.
sync_docs(skipped, _DocIds) ->
    {ok, #{}};
sync_docs(_Ctx, []) ->
    {ok, #{}};
sync_docs(#{st := ST} = Ctx, DocIds) ->
    case exports(ST, att_list, 2) of
        false ->
            {error, att_list_unsupported};
        true ->
            #{source := Source} = Ctx,
            case ST:att_list(Source, DocIds) of
                {ok, []} ->
                    {ok, new_stats()};
                {ok, Entries} ->
                    case apply_entries(Ctx, Entries, new_stats()) of
                        {ok, Stats} ->
                            case flush(Ctx) of
                                ok -> {ok, Stats};
                                {error, _} = Error -> Error
                            end;
                        {failed, _Last, Stats} ->
                            _ = flush(Ctx),
                            {error, {att_transfer_failed, Stats}};
                        {error, _} = Error ->
                            Error
                    end;
                {error, _} = Error ->
                    Error
            end
    end.

%% @doc Walk the whole source feed (no checkpoint), sending what the target
%% lacks or holds with other bytes; failures go to `failed', the walk goes on.
-spec reconcile(term(), term(), module(), module(), map()) ->
    {ok, map()} | skipped | {error, term()}.
reconcile(Source, Target, SourceTransport, TargetTransport, Opts) ->
    case supports(SourceTransport) andalso supports(TargetTransport) of
        false ->
            skipped;
        true ->
            case context(Source, Target, SourceTransport, TargetTransport,
                         Opts) of
                {ok, Ctx} -> reconcile_loop(Ctx, first,
                                            (new_stats())#{failed => []});
                {error, _} = Error -> Error
            end
    end.

reconcile_loop(Ctx, Since, Stats) ->
    #{source := Source, st := ST, batch := BatchSize,
      filter := Filter} = Ctx,
    ChangesOpts = case map_size(Filter) of
        0 -> #{limit => BatchSize};
        _ -> #{limit => BatchSize, filter => Filter}
    end,
    case ST:att_changes(Source, Since, ChangesOpts) of
        {error, att_sync_unsupported} ->
            skipped;
        {error, _} = Error ->
            Error;
        {ok, [], LastSeq} ->
            case barrel_rep_checkpoint:empty_page_advanced(Since, LastSeq) of
                true ->
                    reconcile_loop(Ctx, LastSeq, Stats);
                false ->
                    case flush(Ctx) of
                        ok -> {ok, Stats};
                        {error, _} = Error -> Error
                    end
            end;
        {ok, Entries, LastSeq} ->
            case barrel_rep_checkpoint:seq_advanced(Since, LastSeq) of
                false ->
                    {error, {no_progress, Since, LastSeq}};
                true ->
                    Puts = [E || #{op := put} = E <- Entries],
                    case diff(Ctx, Puts) of
                        {ok, Status} ->
                            reconcile_loop(Ctx, LastSeq,
                                           reconcile_entries(Ctx, Entries,
                                                             Status, Stats));
                        {error, _} = Error ->
                            Error
                    end
            end
    end.

reconcile_entries(Ctx, Entries, Status, Stats) ->
    lists:foldl(
        fun(#{id := Id, name := Name} = Entry, Acc) ->
            case apply_entry(Ctx, Entry, Status, Acc) of
                {ok, Acc1} -> Acc1;
                {error, #{failed := F} = Acc1} ->
                    Acc1#{failed => [{Id, Name} | F]}
            end
        end, Stats, Entries).

run(Source, Target, SourceTransport, TargetTransport, Since,
    CheckpointFun, Opts) ->
    case context(Source, Target, SourceTransport, TargetTransport, Opts) of
        {ok, Ctx} ->
            loop(Ctx#{checkpoint => CheckpointFun, start => Since}, Since,
                 new_stats());
        {error, _} = Error ->
            Error
    end.

new_stats() ->
    #{atts_written => 0, atts_skipped => 0, atts_ignored => 0,
      atts_deleted => 0, atts_touched => 0, att_read_failures => 0,
      att_write_failures => 0, att_bytes_written => 0}.

%% Capabilities first: a filter the source cannot apply would widen the
%% run, a target that cannot sync would checkpoint volatile writes.
context(Source, Target, ST, TT, Opts) ->
    Filter = maps:get(filter, Opts, #{}),
    Ctx = #{source => Source, target => Target, st => ST, tt => TT,
            filter => Filter,
            batch => maps:get(att_batch_size, Opts, ?DEFAULT_BATCH)},
    case maps:get(att_durability, Opts, required) of
        D when D =:= required; D =:= best_effort ->
            case check_filter(Ctx) of
                ok ->
                    case check_flush(D, Ctx) of
                        ok -> {ok, Ctx#{flush => D =:= required}};
                        {error, _} = Error -> Error
                    end;
                {error, _} = Error ->
                    Error
            end;
        Bad ->
            {error, {invalid_option, {att_durability, Bad}}}
    end.

check_filter(#{filter := Filter}) when map_size(Filter) =:= 0 ->
    ok;
check_filter(#{source := Source, st := ST}) ->
    case capabilities(ST, Source) of
        {ok, #{filter := true}} -> ok;
        {ok, _} -> {error, att_filter_unsupported};
        {error, _} = Error -> Error
    end.

check_flush(best_effort, _Ctx) ->
    ok;
check_flush(required, #{target := Target, tt := TT}) ->
    case capabilities(TT, Target) of
        {ok, #{flush := true}} -> ok;
        {ok, _} -> {error, att_flush_unsupported};
        {error, _} = Error -> Error
    end.

capabilities(Transport, Endpoint) ->
    case exports(Transport, att_capabilities, 1) of
        true -> Transport:att_capabilities(Endpoint);
        false -> {ok, #{}}
    end.

loop(Ctx, Since, Stats) ->
    #{source := Source, st := ST, batch := BatchSize,
      filter := Filter} = Ctx,
    ChangesOpts = case map_size(Filter) of
        0 -> #{limit => BatchSize};
        _ -> #{limit => BatchSize, filter => Filter}
    end,
    case ST:att_changes(Source, Since, ChangesOpts) of
        {error, att_sync_unsupported} ->
            %% the transport speaks the protocol but the backend has
            %% no feed
            skipped;
        {error, _} = Error ->
            Error;
        {ok, [], LastSeq} ->
            %% a filtered page can be empty while the feed moved on
            case barrel_rep_checkpoint:empty_page_advanced(Since, LastSeq) of
                true ->
                    case advance(Ctx, LastSeq) of
                        ok -> loop(Ctx, LastSeq, Stats);
                        {error, _} = Error -> Error
                    end;
                false when Since =:= map_get(start, Ctx) ->
                    %% nothing moved: record the position as is
                    case checkpoint(Ctx, Since) of
                        ok -> {ok, Stats};
                        {error, _} = Error -> Error
                    end;
                false ->
                    %% the last page already checkpointed Since
                    {ok, Stats}
            end;
        {ok, Entries, LastSeq} ->
            case barrel_rep_checkpoint:seq_advanced(Since, LastSeq) of
                false ->
                    {error, {no_progress, Since, LastSeq}};
                true ->
                    batch(Ctx, Entries, LastSeq, Stats)
            end
    end.

batch(Ctx, Entries, LastSeq, Stats) ->
    case apply_entries(Ctx, Entries, Stats) of
        {ok, Stats2} ->
            case advance(Ctx, LastSeq) of
                ok -> loop(Ctx, LastSeq, Stats2);
                {error, _} = Error -> Error
            end;
        {failed, Last, Stats2} ->
            %% keep what was applied before the failure, nothing after
            Moved = case Last of
                none -> ok;
                _ -> advance(Ctx, Last)
            end,
            case Moved of
                ok -> {error, {att_transfer_failed, Stats2}};
                {error, _} = Error -> Error
            end;
        {error, _} = Error ->
            Error
    end.

%% The checkpoint moves past the run's start only once the target
%% synced what it acknowledged.
advance(#{start := Start} = Ctx, Seq) ->
    Synced = case Seq =:= Start of
        true -> ok;
        false -> flush(Ctx)
    end,
    case Synced of
        ok -> checkpoint(Ctx, Seq);
        {error, _} = Error -> Error
    end.

flush(#{flush := false}) ->
    ok;
flush(#{target := Target, tt := TT}) ->
    case TT:flush_attachments(Target) of
        ok -> ok;
        {error, unsupported} -> {error, att_flush_unsupported};
        {error, Reason} -> {error, {att_flush_failed, Reason}}
    end.

checkpoint(#{checkpoint := Fun}, Seq) ->
    case Fun(Seq) of
        ok -> ok;
        {error, {checkpoint_failed, _, _}} = Error -> Error;
        {error, Reason} -> {error, {checkpoint_failed, local, Reason}};
        _ -> ok
    end.

%% Entries apply in feed order; the first failure stops the batch and
%% reports the seq of the last entry applied before it.
apply_entries(Ctx, Entries, Stats) ->
    Puts = [E || #{op := put} = E <- Entries],
    case diff(Ctx, Puts) of
        {ok, Status} -> walk(Ctx, Entries, Status, none, Stats);
        {error, _} = Error -> Error
    end.

walk(_Ctx, [], _Status, _Last, Stats) ->
    {ok, Stats};
walk(Ctx, [#{seq := Seq} = Entry | Rest], Status, Last, Stats) ->
    case apply_entry(Ctx, Entry, Status, Stats) of
        {ok, Stats1} -> walk(Ctx, Rest, Status, Seq, Stats1);
        {error, Stats1} -> {failed, Last, Stats1}
    end.

diff(_Ctx, []) ->
    {ok, #{}};
diff(#{target := Target, tt := TT}, Puts) ->
    Input = [maps:with([id, name, digest, origin], E) || E <- Puts],
    case TT:diff_attachments(Target, Input) of
        {ok, Diff} ->
            {ok, maps:from_list([{{Id, Name}, St}
                                 || #{id := Id, name := Name,
                                      status := St} <- Diff])};
        {error, _} = Error ->
            Error
    end.

apply_entry(Ctx, #{op := delete, id := Id, name := Name,
                   origin := Origin}, _Status, Stats) ->
    #{target := Target, tt := TT} = Ctx,
    case TT:delete_attachment(Target, Id, Name, #{origin_hlc => Origin}) of
        ok ->
            {ok, bump(atts_deleted, Stats)};
        {error, Reason} ->
            logger:warning("attachment delete ~s/~s failed: ~p",
                           [Id, Name, Reason]),
            {error, bump(att_write_failures, Stats)}
    end;
apply_entry(Ctx, #{id := Id, name := Name} = Entry, Status, Stats) ->
    case maps:get({Id, Name}, Status, missing) of
        have -> {ok, bump(atts_skipped, Stats)};
        touch -> touch(Ctx, Entry, Stats);
        missing -> transfer(Ctx, Entry, Stats)
    end.

%% Same bytes, older origin on the target: send the origin alone. A
%% target that cannot (or whose bytes changed meanwhile) gets them all.
touch(#{target := Target, tt := TT} = Ctx, #{id := Id, name := Name} = Entry,
      Stats) ->
    case exports(TT, touch_attachment, 4) of
        false ->
            transfer(Ctx, Entry, Stats);
        true ->
            case TT:touch_attachment(Target, Id, Name, Entry) of
                {ok, touched} -> {ok, bump(atts_touched, Stats)};
                {ok, ignored} -> {ok, bump(atts_ignored, Stats)};
                {error, bytes_needed} -> transfer(Ctx, Entry, Stats);
                {error, Reason} ->
                    logger:warning("attachment touch ~s/~s failed: ~p",
                                   [Id, Name, Reason]),
                    {error, bump(att_write_failures, Stats)}
            end
    end.

transfer(Ctx, Entry, Stats) ->
    #{source := Source, target := Target, st := ST, tt := TT} = Ctx,
    #{id := Id, name := Name, digest := Digest, length := Length,
      content_type := ContentType, origin := Origin} = Entry,
    case ST:get_attachment_stream(Source, Id, Name) of
        {ok, Info, ReadFun} ->
            Meta = #{content_type => ContentType, digest => Digest,
                     length => Length, origin_hlc => Origin},
            Put = TT:put_attachment(Target, Id, Name, Meta, ReadFun),
            %% a target that stopped early leaves the stream open
            ok = close_stream(Info),
            case Put of
                {ok, ignored} ->
                    {ok, bump(atts_ignored, Stats)};
                {ok, _} ->
                    {ok, bump(att_bytes_written, Length,
                              bump(atts_written, Stats))};
                {error, Reason} ->
                    logger:warning("attachment put ~s/~s failed: ~p",
                                   [Id, Name, Reason]),
                    {error, bump(att_write_failures, Stats)}
            end;
        {error, not_found} ->
            case superseded(Ctx, Entry) of
                true -> {ok, bump(atts_skipped, Stats)};
                false -> {error, bump(att_read_failures, Stats)}
            end;
        {error, Reason} ->
            logger:warning("attachment read ~s/~s failed: ~p",
                           [Id, Name, Reason]),
            {error, bump(att_read_failures, Stats)}
    end.

%% Gone at the source since the feed read: skipping is safe only when a
%% later delete or a newer write took its place (its row comes later).
superseded(#{source := Source, st := ST}, #{id := Id, name := Name,
                                            origin := Origin}) ->
    case exports(ST, att_state, 3) andalso ST:att_state(Source, Id, Name) of
        {ok, #{op := delete}} -> true;
        {ok, #{origin := Current}} -> barrel_hlc:less(Origin, Current);
        _ -> false
    end.

close_stream(#{close := Close}) when is_function(Close, 0) ->
    _ = Close(),
    ok;
close_stream(_Info) ->
    ok.

%% A checkpoint below the source's attachment floor may predate
%% forgotten tombstones: restart from the beginning (put rows are never
%% swept, so a full feed walk re-enumerates everything; the diff keeps
%% the re-transfer cost at metadata only).
maybe_reset_on_att_floor(first, _Source, _SourceTransport, _RepId) ->
    first;
maybe_reset_on_att_floor(StartSeq, Source, SourceTransport, RepId) ->
    Floor = try SourceTransport:db_info(Source) of
        {ok, #{att_floor := F}} -> F;
        _ -> undefined
    catch
        _:_ -> undefined
    end,
    case Floor of
        undefined ->
            StartSeq;
        _ ->
            case barrel_hlc:less(StartSeq, Floor) of
                true ->
                    logger:warning(
                        "attachment checkpoint for ~s is below the "
                        "source floor; forcing full resync", [RepId]),
                    first;
                false ->
                    StartSeq
            end
    end.

bump(Key, Stats) ->
    bump(Key, 1, Stats).

bump(Key, N, Stats) ->
    maps:update_with(Key, fun(V) -> V + N end, N, Stats).
