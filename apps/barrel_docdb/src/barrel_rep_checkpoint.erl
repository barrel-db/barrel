%%%-------------------------------------------------------------------
%%% @doc barrel_rep_checkpoint - Checkpoint management for replication
%%%
%%% Manages replication checkpoints, which track the progress of
%%% replication between source and target databases. Checkpoints
%%% are stored as local documents (not replicated).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_rep_checkpoint).

-include("barrel_docdb.hrl").

%% API
-export([
    new/1,
    get_start_seq/1,
    get_last_seq/1,
    set_last_seq/2,
    get_pending/1,
    set_pending/2,
    read_error/1,
    max_pending/1,
    maybe_write_checkpoint/1,
    write_checkpoint/1,
    delete/1
]).

%% Seq wire codec (base64 of the 12-byte HLC; <<"first">> sentinel)
-export([encode_seq/1, decode_seq/1]).

%% Sequence ordering
-export([seq_advanced/2]).

%% Internal API for reading checkpoints
-export([
    read_checkpoint_doc/3
]).

-record(checkpoint, {
    rep_id :: binary(),
    session_id :: binary(),
    source :: term(),
    target :: term(),
    source_transport :: module(),
    target_transport :: module(),
    start_seq :: barrel_hlc:timestamp() | first,
    last_seq :: barrel_hlc:timestamp() | first,
    docs_processed = 0 :: non_neg_integer(),
    %% ids that failed and are retried by id; saved with last_seq, so a
    %% failure is never behind the saved position unless it is listed
    pending = [] :: [binary()],
    %% a side whose checkpoint could not be read: the run must not start
    read_error :: undefined | {source | target, term()},
    options :: map()
}).

-opaque checkpoint() :: #checkpoint{}.
-export_type([checkpoint/0]).

-define(CHECKPOINT_SIZE, 10).
-define(MAX_CHECKPOINT_HISTORY, 20).

%%====================================================================
%% API
%%====================================================================

%% @doc Create a new checkpoint state
-spec new(map()) -> checkpoint().
new(RepConfig) ->
    #{
        id := RepId,
        source := Source,
        target := Target,
        source_transport := SourceTransport,
        target_transport := TargetTransport
    } = RepConfig,

    Options = maps:get(options, RepConfig, #{}),

    %% Get start sequence from existing checkpoints
    %% Start from the lower position of both sides and the union of
    %% their pending ids: the two writes are not atomic, and reading
    %% lower only ships again
    {StartSeq, Pending, ReadError} =
        case {read_entry(Source, SourceTransport, RepId),
              read_entry(Target, TargetTransport, RepId)} of
            {{error, R}, _} -> {first, [], {source, R}};
            {_, {error, R}} -> {first, [], {target, R}};
            {{S1, P1}, {S2, P2}} ->
                {min_seq(S1, S2), lists:usort(P1 ++ P2), undefined}
        end,

    #checkpoint{
        rep_id = RepId,
        session_id = generate_session_id(),
        source = Source,
        target = Target,
        source_transport = SourceTransport,
        target_transport = TargetTransport,
        start_seq = StartSeq,
        last_seq = StartSeq,
        docs_processed = 0,
        pending = Pending,
        read_error = ReadError,
        options = Options
    }.

-spec get_pending(checkpoint()) -> [binary()].
get_pending(#checkpoint{pending = Pending}) ->
    Pending.

-spec set_pending([binary()], checkpoint()) -> checkpoint().
set_pending(Pending, Checkpoint) ->
    Checkpoint#checkpoint{pending = lists:usort(Pending)}.

%% @doc How many failed ids a run may carry before it stops (default
%% 1000, the `max_pending' option).
-spec max_pending(checkpoint()) -> pos_integer().
max_pending(#checkpoint{options = Options}) ->
    maps:get(max_pending, Options, 1000).

-spec read_error(checkpoint()) -> undefined | {source | target, term()}.
read_error(#checkpoint{read_error = ReadError}) ->
    ReadError.

%% @doc Get the starting HLC for this replication session
-spec get_start_seq(checkpoint()) -> barrel_hlc:timestamp() | first.
get_start_seq(#checkpoint{start_seq = Seq}) ->
    Seq.

%% @doc Get the last processed HLC
-spec get_last_seq(checkpoint()) -> barrel_hlc:timestamp() | first.
get_last_seq(#checkpoint{last_seq = Seq}) ->
    Seq.

%% @doc Set the last processed sequence (HLC timestamp)
-spec set_last_seq(barrel_hlc:timestamp() | first, checkpoint()) -> checkpoint().
set_last_seq(Seq, #checkpoint{docs_processed = DocsProcessed} = Checkpoint) ->
    Checkpoint#checkpoint{last_seq = Seq, docs_processed = DocsProcessed + 1}.

%% @doc True if `New' is strictly after `Old' in sequence order. `first'
%% precedes every real sequence. A drain loop uses this to detect a source
%% that returns a non-empty batch without advancing the sequence, which
%% would otherwise loop forever re-requesting the same point.
-spec seq_advanced(barrel_hlc:timestamp() | first,
                   barrel_hlc:timestamp() | first) -> boolean().
seq_advanced(_Old, first) -> false;
seq_advanced(first, _New) -> true;
seq_advanced(Old, New) -> barrel_hlc:compare(New, Old) =:= gt.

%% @doc Check if checkpoint should be written and write it if needed.
-spec maybe_write_checkpoint(checkpoint()) ->
    {ok, checkpoint()} | {error, term()}.
maybe_write_checkpoint(#checkpoint{docs_processed = DocsProcessed, options = Options} = Checkpoint) ->
    CheckpointSize = maps:get(checkpoint_size, Options, ?CHECKPOINT_SIZE),
    case DocsProcessed >= CheckpointSize of
        true ->
            case write_checkpoint(Checkpoint) of
                ok -> {ok, Checkpoint#checkpoint{docs_processed = 0}};
                {error, _} = Err -> Err
            end;
        false ->
            {ok, Checkpoint}
    end.

%% @doc Write checkpoint to both source and target databases. A failed
%% write is reported (with the side that failed) instead of swallowed:
%% the docs are already committed, the next run just resumes earlier.
-spec write_checkpoint(checkpoint()) ->
    ok | {error, {checkpoint_failed, source | target, term()}}.
write_checkpoint(#checkpoint{
    rep_id = RepId,
    session_id = SessionId,
    source = Source,
    target = Target,
    source_transport = SourceTransport,
    target_transport = TargetTransport,
    start_seq = StartSeq,
    last_seq = LastSeq,
    pending = Pending,
    options = Options
}) ->
    HistorySize = maps:get(checkpoint_max_history, Options, ?MAX_CHECKPOINT_HISTORY),

    Checkpoint = #{
        <<"source_last_seq">> => encode_seq(LastSeq),
        <<"source_start_seq">> => encode_seq(StartSeq),
        <<"pending">> => Pending,
        <<"session_id">> => SessionId,
        <<"end_time">> => timestamp(),
        <<"end_time_microsec">> => erlang:system_time(microsecond)
    },

    %% Write both sides before reporting, so one failure never skips
    %% the other side's checkpoint.
    SourceRes = add_checkpoint(Source, SourceTransport, RepId, SessionId,
                               HistorySize, Checkpoint),
    TargetRes = add_checkpoint(Target, TargetTransport, RepId, SessionId,
                               HistorySize, Checkpoint),
    checkpoint_result([{source, SourceRes}, {target, TargetRes}]).

checkpoint_result([]) ->
    ok;
checkpoint_result([{_Side, ok} | Rest]) ->
    checkpoint_result(Rest);
checkpoint_result([{Side, {error, Reason}} | _Rest]) ->
    {error, {checkpoint_failed, Side, Reason}}.

%% @doc Delete checkpoints from both databases
-spec delete(checkpoint()) -> ok.
delete(#checkpoint{
    rep_id = RepId,
    source = Source,
    target = Target,
    source_transport = SourceTransport,
    target_transport = TargetTransport
}) ->
    DocId = checkpoint_docid(RepId),
    _ = SourceTransport:delete_local_doc(Source, DocId),
    _ = TargetTransport:delete_local_doc(Target, DocId),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @doc Add a checkpoint entry to history: ok | {error, Reason}. A
%% transport error reading the previous doc (unauthorized, network) is
%% returned, not raised.
add_checkpoint(Db, Transport, RepId, SessionId, HistorySize, Checkpoint) ->
    DocId = checkpoint_docid(RepId),
    case Transport:get_local_doc(Db, DocId) of
        {ok, #{<<"history">> := H} = PreviousDoc} ->
            H2 = update_history(H, SessionId, HistorySize, Checkpoint),
            Transport:put_local_doc(Db, DocId,
                                    PreviousDoc#{<<"history">> => H2});
        {ok, _NoHistory} ->
            Transport:put_local_doc(Db, DocId,
                                    #{<<"history">> => [Checkpoint]});
        {error, not_found} ->
            Transport:put_local_doc(Db, DocId,
                                    #{<<"history">> => [Checkpoint]});
        {error, _} = Err ->
            Err
    end.

%% @doc Update checkpoint history
update_history(History, SessionId, HistorySize, Checkpoint) ->
    case History of
        [#{<<"session_id">> := SessionId} | Rest] ->
            %% Same session - replace last entry
            [Checkpoint | Rest];
        _ ->
            %% New session - add to history, trim if needed
            NewHistory = [Checkpoint | History],
            case length(NewHistory) > HistorySize of
                true ->
                    lists:sublist(NewHistory, HistorySize);
                false ->
                    NewHistory
            end
    end.

%% The newest entry of one side: {Seq, Pending}, or the read error.
read_entry(Db, Transport, RepId) ->
    case read_checkpoint_doc(Db, Transport, RepId) of
        {ok, Doc} ->
            case newest(maps:get(<<"history">>, Doc, [])) of
                undefined ->
                    {first, []};
                Entry ->
                    {decode_seq(maps:get(<<"source_last_seq">>, Entry)),
                     [Id || Id <- maps:get(<<"pending">>, Entry, []),
                            is_binary(Id)]}
            end;
        {error, not_found} ->
            {first, []};
        {error, _} = Err ->
            Err
    end.

newest([]) ->
    undefined;
newest(History) ->
    hd(lists:sort(fun(H1, H2) ->
                      maps:get(<<"end_time_microsec">>, H1, 0) >
                          maps:get(<<"end_time_microsec">>, H2, 0)
                  end, History)).

%% @doc Read checkpoint document
-spec read_checkpoint_doc(term(), module(), binary()) ->
    {ok, map()} | {error, term()}.
read_checkpoint_doc(Db, Transport, RepId) ->
    Transport:get_local_doc(Db, checkpoint_docid(RepId)).

%% @doc Generate checkpoint document ID
checkpoint_docid(RepId) ->
    <<"replication-checkpoint-", RepId/binary>>.

%% @doc Generate unique session ID
generate_session_id() ->
    Rand = crypto:strong_rand_bytes(16),
    base64:encode(Rand).

%% @doc RFC3339 timestamp
timestamp() ->
    {{Year, Month, Day}, {Hour, Min, Sec}} = calendar:local_time(),
    iolist_to_binary(
        io_lib:format("~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0wZ",
                      [Year, Month, Day, Hour, Min, Sec])
    ).

%% @doc Encode HLC timestamp for storage in checkpoint
encode_seq(first) -> <<"first">>;
encode_seq(Hlc) ->
    %% Store HLC as base64-encoded binary
    base64:encode(barrel_hlc:encode(Hlc)).

%% @doc Decode HLC timestamp from storage
decode_seq(<<"first">>) -> first;
decode_seq(Bin) when is_binary(Bin) ->
    try
        barrel_hlc:decode(base64:decode(Bin))
    catch
        _:_ ->
            %% Handle legacy format or corrupted data
            first
    end.

%% @doc Get minimum of two HLC timestamps (earliest)
min_seq(first, _) -> first;
min_seq(_, first) -> first;
min_seq(Hlc1, Hlc2) ->
    case barrel_hlc:less(Hlc1, Hlc2) of
        true -> Hlc1;
        false -> Hlc2
    end.
