%% @doc The attachment phase of a replication run. Runs AFTER the doc
%% loop (independent lifecycles, independent checkpoint): fold the
%% source's attachment feed since the attachment checkpoint, digest-diff
%% each batch against the target in one round trip, stream the missing
%% blobs (digest-verified at the target's commit point), apply
%% LWW-guarded deletes, checkpoint per batch.
%%
%% Degrades to `skipped' when either transport (or the underlying
%% attachment backend) has no feed support. Redelivery is safe
%% end-to-end: digest diff answers `have', and the origin-HLC guard
%% turns replays into `{ok, ignored}'.
-module(barrel_rep_att).

-include("barrel_docdb.hrl").

-export([sync/5]).

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
    _ = code:ensure_loaded(Transport),
    erlang:function_exported(Transport, att_changes, 3).

do_sync(Source, Target, SourceTransport, TargetTransport, Opts) ->
    RepId = maps:get(rep_id, Opts),
    Checkpoint = barrel_rep_checkpoint:new(#{
        id => <<"att-", RepId/binary>>,
        source => Source,
        target => Target,
        source_transport => SourceTransport,
        target_transport => TargetTransport,
        options => Opts
    }),
    StartSeq0 = barrel_rep_checkpoint:get_start_seq(Checkpoint),
    StartSeq = maybe_reset_on_att_floor(StartSeq0, Source, SourceTransport,
                                        RepId),
    BatchSize = maps:get(att_batch_size, Opts, ?DEFAULT_BATCH),
    Stats0 = #{atts_written => 0, atts_skipped => 0, atts_ignored => 0,
               atts_deleted => 0, att_read_failures => 0,
               att_write_failures => 0, att_bytes_written => 0},
    loop(Source, Target, SourceTransport, TargetTransport, StartSeq,
         BatchSize, Checkpoint, Stats0).

loop(Source, Target, SourceTransport, TargetTransport, Since, BatchSize,
     Checkpoint, Stats) ->
    case SourceTransport:att_changes(Source, Since, #{limit => BatchSize}) of
        {error, att_sync_unsupported} ->
            %% the transport speaks the protocol but the backend has
            %% no feed
            skipped;
        {error, _} = Error ->
            Error;
        {ok, [], _LastSeq} ->
            ok = barrel_rep_checkpoint:write_checkpoint(Checkpoint),
            {ok, Stats};
        {ok, Entries, LastSeq} ->
            case process_batch(Source, Target, SourceTransport,
                               TargetTransport, Entries, Stats) of
                {ok, Stats2} ->
                    Checkpoint2 = barrel_rep_checkpoint:set_last_seq(
                        LastSeq, Checkpoint),
                    ok = barrel_rep_checkpoint:write_checkpoint(Checkpoint2),
                    loop(Source, Target, SourceTransport, TargetTransport,
                         LastSeq, BatchSize, Checkpoint2, Stats2);
                {error, _} = Error ->
                    Error
            end
    end.

process_batch(Source, Target, SourceTransport, TargetTransport, Entries,
              Stats0) ->
    {Puts, Deletes} = lists:partition(
        fun(#{op := Op}) -> Op =:= put end, Entries),
    DiffInput = [maps:with([id, name, digest], E) || E <- Puts],
    case diff(TargetTransport, Target, DiffInput) of
        {ok, Diff} ->
            Missing = sets:from_list(
                [{Id, Name} || #{id := Id, name := Name,
                                 status := missing} <- Diff]),
            Stats1 = lists:foldl(
                fun(Entry, Acc) ->
                    #{id := Id, name := Name} = Entry,
                    case sets:is_element({Id, Name}, Missing) of
                        true ->
                            transfer(Source, Target, SourceTransport,
                                     TargetTransport, Entry, Acc);
                        false ->
                            bump(atts_skipped, Acc)
                    end
                end,
                Stats0,
                Puts),
            Stats2 = lists:foldl(
                fun(#{id := Id, name := Name, origin := Origin}, Acc) ->
                    case TargetTransport:delete_attachment(
                             Target, Id, Name, #{origin_hlc => Origin}) of
                        ok ->
                            bump(atts_deleted, Acc);
                        {error, Reason} ->
                            logger:warning(
                                "attachment delete ~s/~s failed: ~p",
                                [Id, Name, Reason]),
                            bump(att_write_failures, Acc)
                    end
                end,
                Stats1,
                Deletes),
            {ok, Stats2};
        {error, _} = Error ->
            Error
    end.

diff(_TargetTransport, _Target, []) ->
    {ok, []};
diff(TargetTransport, Target, DiffInput) ->
    TargetTransport:diff_attachments(Target, DiffInput).

transfer(Source, Target, SourceTransport, TargetTransport, Entry, Stats) ->
    #{id := Id, name := Name, digest := Digest, length := Length,
      content_type := ContentType, origin := Origin} = Entry,
    case SourceTransport:get_attachment_stream(Source, Id, Name) of
        {ok, _Info, ReadFun} ->
            Meta = #{content_type => ContentType, digest => Digest,
                     length => Length, origin_hlc => Origin},
            case TargetTransport:put_attachment(Target, Id, Name, Meta,
                                                ReadFun) of
                {ok, ignored} ->
                    bump(atts_ignored, Stats);
                {ok, _} ->
                    bump(att_bytes_written, Length,
                         bump(atts_written, Stats));
                {error, Reason} ->
                    logger:warning("attachment put ~s/~s failed: ~p",
                                   [Id, Name, Reason]),
                    bump(att_write_failures, Stats)
            end;
        {error, not_found} ->
            %% deleted at the source since the feed read; its delete
            %% entry sits later in the feed
            bump(att_read_failures, Stats);
        {error, Reason} ->
            logger:warning("attachment read ~s/~s failed: ~p",
                           [Id, Name, Reason]),
            bump(att_read_failures, Stats)
    end.

%% A checkpoint below the source's attachment floor may predate
%% forgotten tombstones: restart from the beginning (put rows are never
%% swept, so a full feed walk re-enumerates everything; digest diff
%% keeps the re-transfer cost at metadata only).
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
