%%%-------------------------------------------------------------------
%%% @author Benoit Chesneau
%%% @copyright (C) 2024, Benoit Chesneau
%%% @doc barrel_rep - Replication API for barrel_docdb
%%%
%%% This module provides the public API for replicating documents
%%% between barrel_docdb databases. It implements a CouchDB-style
%%% replication protocol with:
%%%
%%% <ul>
%%%   <li>Incremental replication using revision comparison</li>
%%%   <li>Checkpoint-based resumption</li>
%%%   <li>Pluggable transport layer for local or remote databases</li>
%%%   <li>Conflict-aware document merging</li>
%%% </ul>
%%%
%%% == Quick Start ==
%%%
%%% ```
%%% %% Create source and target databases
%%% {ok, _} = barrel_docdb:create_db(<<"source">>),
%%% {ok, _} = barrel_docdb:create_db(<<"target">>),
%%%
%%% %% Add documents to source
%%% {ok, _} = barrel_docdb:put_doc(<<"source">>, #{
%%%     <<"id">> => <<"doc1">>,
%%%     <<"value">> => <<"hello">>
%%% }),
%%%
%%% %% Replicate source to target
%%% {ok, Result} = barrel_rep:replicate(<<"source">>, <<"target">>),
%%% io:format("Replicated ~p documents~n", [maps:get(docs_written, Result)]).
%%% '''
%%%
%%% == How Replication Works ==
%%%
%%% Replication follows these steps:
%%%
%%% <ol>
%%%   <li>Read checkpoint to find last replicated sequence</li>
%%%   <li>Fetch changes from source since that sequence</li>
%%%   <li>For each batch, diff versions by vector containment to find what
%%%       the target is missing</li>
%%%   <li>Fetch and transfer the missing versions with history</li>
%%%   <li>Write checkpoint after each batch</li>
%%% </ol>
%%%
%%% == Transport Abstraction ==
%%%
%%% Replication uses a transport behaviour (`barrel_rep_transport') to
%%% communicate with databases. The default `barrel_rep_transport_local'
%%% works with databases in the same Erlang VM.
%%%
%%% Custom transports can be implemented for HTTP, TCP, or other protocols.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_rep).

-include("barrel_docdb.hrl").

%% API
-export([
    replicate/2,
    replicate/3,
    replicate_one_shot/1,
    replicate_one_shot/2,
    reconcile/3,
    rep_id/5
]).

%% Types
-type filter_opts() :: #{
    paths => [binary()],           % MQTT-style path patterns
    query => barrel_query:query_spec(),  % Query to filter by
    channel => binary()            % Configured channel (write-time feed)
}.

-type rep_config() :: #{
    source := term(),
    target := term(),
    source_transport => module(),
    target_transport => module(),
    batch_size => pos_integer(),
    checkpoint_size => pos_integer(),
    filter => filter_opts()        % Optional filter for selective replication
}.

-type rep_result() :: #{
    ok := boolean(),
    docs_read := non_neg_integer(),
    docs_written := non_neg_integer(),
    doc_read_failures := non_neg_integer(),
    doc_write_failures := non_neg_integer(),
    start_seq := seq() | first,
    last_seq := seq() | first,
    att_sync := disabled | skipped | map()
}.

-export_type([rep_config/0, rep_result/0, filter_opts/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Replicate from source to target database.
%%
%% Performs a one-shot replication from source to target, copying all
%% documents that don't exist in the target or have newer revisions.
%%
%% Uses the local transport (`barrel_rep_transport_local') for both
%% endpoints, suitable for replicating between databases in the same VM.
%%
%% == Example ==
%% ```
%% {ok, Result} = barrel_rep:replicate(<<"source">>, <<"target">>),
%% DocsWritten = maps:get(docs_written, Result).
%% '''
%%
%% @param Source Source database name
%% @param Target Target database name
%% @returns `{ok, rep_result()}' with replication statistics
%% @see replicate/3
-spec replicate(binary(), binary()) -> {ok, rep_result()} | {error, term()}.
replicate(Source, Target) ->
    replicate(Source, Target, #{}).

%% @doc Replicate from source to target with options.
%%
%% == Options ==
%% <ul>
%%   <li>`batch_size' - Number of changes to process per batch (default: 100)</li>
%%   <li>`checkpoint_size' - Write checkpoint after this many documents (default: 10)</li>
%%   <li>`source_transport' - Transport module for source</li>
%%   <li>`target_transport' - Transport module for target</li>
%%   <li>`filter' - Filter options for selective replication (see below);
%%       it applies to attachments too</li>
%%   <li>`attachments' - Replicate attachments (default: true)</li>
%%   <li>`att_durability' - `required' (default): the target syncs its
%%       attachment writes before each attachment checkpoint, and a target
%%       that cannot fails the run; `best_effort': no sync, a power loss on
%%       the target can lose checkpointed attachments</li>
%% </ul>
%%
%% == Filter Options ==
%% The `filter' option allows selective replication. Both filters use AND logic:
%% documents must match ALL specified filters to be replicated.
%% <ul>
%%   <li>`paths' - List of MQTT-style path patterns (e.g., `[<<"users/#">>]')</li>
%%   <li>`query' - Query specification (e.g., `#{where => [{path, [<<"type">>], <<"user">>}]}')</li>
%% </ul>
%%
%% == Example ==
%% ```
%% %% Replicate only user type documents
%% {ok, Result} = barrel_rep:replicate(<<"source">>, <<"target">>, #{
%%     filter => #{
%%         query => #{where => [{path, [<<"type">>], <<"user">>}]}
%%     }
%% }),
%%
%% %% Replicate users with status=active (path AND query)
%% {ok, Result} = barrel_rep:replicate(<<"source">>, <<"target">>, #{
%%     filter => #{
%%         paths => [<<"type/#">>],
%%         query => #{where => [{path, [<<"status">>], <<"active">>}]}
%%     }
%% }).
%% '''
%%
%% @param Source Source database name
%% @param Target Target database name
%% @param Opts Replication options
%% @returns `{ok, rep_result()}' with replication statistics
-spec replicate(binary(), binary(), map()) -> {ok, rep_result()} | {error, term()}.
replicate(Source, Target, Opts) ->
    Config = #{
        source => Source,
        target => Target,
        source_transport => maps:get(source_transport, Opts, barrel_rep_transport_local),
        target_transport => maps:get(target_transport, Opts, barrel_rep_transport_local)
    },
    replicate_one_shot(Config, Opts).

%% @doc Perform one-shot replication with full configuration.
%%
%% This is the lower-level API that accepts a complete configuration map.
%% Use this when you need custom transports or advanced configuration.
%%
%% == Example ==
%% ```
%% Config = #{
%%     source => <<"source_db">>,
%%     target => <<"target_db">>,
%%     source_transport => barrel_rep_transport_local,
%%     target_transport => barrel_rep_transport_local
%% },
%% {ok, Result} = barrel_rep:replicate_one_shot(Config, #{}).
%% '''
%%
%% @param Config Replication configuration map
%% @param Opts Additional options
%% @returns `{ok, rep_result()}' with replication statistics
%% @see replicate/2
-spec replicate_one_shot(rep_config(), map()) -> {ok, rep_result()} | {error, term()}.
replicate_one_shot(Config, Opts) ->
    case {barrel_rep_filter:check(maps:get(filter, Opts, #{})),
          maps:get(att_durability, Opts, required)} of
        {ok, D} when D =:= required; D =:= best_effort ->
            replicate_urls_checked(Config, Opts);
        {ok, Bad} -> {error, {invalid_option, {att_durability, Bad}}};
        {{error, _} = Invalid, _} -> Invalid
    end.

%% Inline credentials are fine here (nothing is persisted), but not in
%% a URL: it would reach logs and the replication id.
replicate_urls_checked(#{source := Source, target := Target} = Config, Opts) ->
    case lists:any(fun has_userinfo/1, [Source, Target]) of
        true -> {error, {invalid_sync_url, userinfo}};
        false -> replicate_checked(Config, Opts)
    end.

has_userinfo(#{url := Url}) -> barrel_rep_transport_http:has_userinfo(Url);
has_userinfo(Url) when is_binary(Url) ->
    barrel_rep_transport_http:has_userinfo(Url);
has_userinfo(_Endpoint) -> false.

replicate_checked(Config, Opts) ->
    #{
        source := Source,
        target := Target,
        source_transport := SourceTransport,
        target_transport := TargetTransport
    } = Config,

    %% Generate replication ID (a filtered replication is a different
    %% stream: it must not share the full replication's checkpoint)
    RepId = rep_id(Source, SourceTransport, Target, TargetTransport,
                   maps:get(filter, Opts, #{})),

    %% Create checkpoint state
    %% a pre-2.1 filtered checkpoint may sit past skipped matches
    CheckpointConfig = Config#{
        id => RepId,
        options => Opts,
        reset_legacy => map_size(maps:get(filter, Opts, #{})) > 0
    },
    Checkpoint0 = barrel_rep_checkpoint:new(CheckpointConfig),
    case barrel_rep_checkpoint:read_error(Checkpoint0) of
        undefined ->
            run_one_shot(Config, Opts, RepId, Checkpoint0);
        {Side, Reason} ->
            barrel_metrics:inc_rep_errors(RepId),
            {error, {checkpoint_unreadable, Side, Reason}}
    end.

run_one_shot(Config, Opts, RepId, Checkpoint0) ->
    #{
        source := Source,
        target := Target,
        source_transport := SourceTransport,
        target_transport := TargetTransport
    } = Config,

    %% Get starting sequence. A checkpoint older than the source's
    %% history floor may predate forgotten tombstones: force a full
    %% resync so current state converges.
    StartSeq0 = barrel_rep_checkpoint:get_start_seq(Checkpoint0),
    StartSeq = maybe_reset_on_floor(StartSeq0, Source, SourceTransport, RepId),

    %% Run replication
    BatchSize = maps:get(batch_size, Opts, 100),
    CheckpointSize = maps:get(checkpoint_size, Opts, 10),
    Filter = maps:get(filter, Opts, #{}),
    %% a filtered run copies the attachments of the documents it copies
    Scan = case doc_atts(Config, Opts, RepId) of
        {ok, AttCtx} -> {ok, #{filter => Filter, att => AttCtx}};
        {error, _} = AttErr -> AttErr
    end,

    %% The final checkpoint write is part of the run: a failure ends it
    %% with an error (docs are committed; the next run resumes earlier).
    %% Pending ids are retried first, by id; a success leaves the set.
    Run = case Scan of
        {error, _} = ScanSetupErr ->
            ScanSetupErr;
        {ok, ScanCtx} ->
            run_scan(Source, Target, SourceTransport, TargetTransport,
                     Checkpoint0, StartSeq, BatchSize, CheckpointSize,
                     ScanCtx)
    end,
    finish_one_shot(Run, Config, Opts, RepId, StartSeq).

run_scan(Source, Target, SourceTransport, TargetTransport, Checkpoint0,
         StartSeq, BatchSize, CheckpointSize, Scan) ->
    Retried = case retry_pending(Source, Target, SourceTransport,
                                 TargetTransport, Checkpoint0) of
        {ok, _, Cp1} = RetryOk ->
            Copied = barrel_rep_checkpoint:get_pending(Checkpoint0)
                -- barrel_rep_checkpoint:get_pending(Cp1),
            case barrel_rep_att:sync_docs(maps:get(att, Scan), Copied) of
                {ok, _} -> RetryOk;
                {error, AttReason} -> {error, {att_sync_failed, AttReason}}
            end;
        RetryErr0 ->
            RetryErr0
    end,
    case Retried of
        {ok, RetryStats, Checkpoint} ->
            case do_replicate(Source, Target, SourceTransport,
                              TargetTransport, StartSeq, BatchSize,
                              CheckpointSize, Checkpoint, Scan) of
                {ok, ScanStats, Cp} ->
                    {ok, merge_stats(RetryStats, ScanStats), Cp};
                ScanErr ->
                    ScanErr
            end;
        RetryErr ->
            RetryErr
    end.

finish_one_shot(Run, Config, Opts, RepId, StartSeq) ->
    #{
        source := Source,
        target := Target,
        source_transport := SourceTransport,
        target_transport := TargetTransport
    } = Config,
    Run2 = case Run of
        {ok, Stats0, FinalCheckpoint0} ->
            case barrel_rep_checkpoint:write_checkpoint(FinalCheckpoint0) of
                ok -> {ok, Stats0, FinalCheckpoint0};
                {error, _} = CpError -> CpError
            end;
        NotOk ->
            NotOk
    end,
    case Run2 of
        {ok, Stats, FinalCheckpoint} ->

            %% Record replication metrics
            DocsWritten = maps:get(docs_written, Stats, 0),
            DocWriteFailures = maps:get(doc_write_failures, Stats, 0),
            barrel_metrics:inc_rep_docs(push, DocsWritten),
            case DocWriteFailures > 0 of
                true -> barrel_metrics:inc_rep_errors(RepId);
                false -> ok
            end,

            %% Attachment phase (independent lifecycle + checkpoint;
            %% degrades to skipped without transport/backend support)
            AttSync = case maps:get(attachments, Opts, true) of
                false ->
                    disabled;
                true ->
                    barrel_rep_att:sync(Source, Target, SourceTransport,
                                        TargetTransport,
                                        Opts#{rep_id => RepId})
            end,
            case AttSync of
                {error, AttReason} ->
                    barrel_metrics:inc_rep_errors(RepId),
                    {error, {att_sync_failed, AttReason}};
                _ ->
                    %% Build result
                    Result = (maps:remove(failed, Stats))#{
                        ok => true,
                        pending => barrel_rep_checkpoint:get_pending(
                                     FinalCheckpoint),
                        start_seq => StartSeq,
                        last_seq => barrel_rep_checkpoint:get_last_seq(
                            FinalCheckpoint),
                        att_sync => case AttSync of
                            {ok, AttStats} -> AttStats;
                            Other -> Other
                        end
                    },
                    {ok, Result}
            end;
        {error, _} = Error ->
            %% Record error metric
            barrel_metrics:inc_rep_errors(RepId),
            Error
    end.

%% @doc Repair a target from a healthy source, once (after upgrading
%% from 2.0, see the replication guide). Walks the source's whole feed
%% without checkpoints: for each document, a version both sides hold
%% but the target stores with another body (such as `#{}') gets the
%% source's body, then the source state is merged as usual. Then every
%% source attachment the target lacks or holds with other bytes is sent
%% again. Failures are listed, the walk goes on.
%%
%% Options: `source_transport', `target_transport', `filter',
%% `batch_size', `attachments' (default true), `att_batch_size',
%% `att_durability', as for replicate/3.
%%
%% ```
%% {ok, #{docs_checked := _, bodies_repaired := _, atts_resent := _,
%%        doc_failures := [], att_failures := []}} =
%%     barrel_rep:reconcile(<<"healthy">>, <<"damaged">>, #{}).
%% '''
-spec reconcile(term(), term(), map()) -> {ok, map()} | {error, term()}.
reconcile(Source, Target, Opts) ->
    ST = maps:get(source_transport, Opts, barrel_rep_transport_local),
    TT = maps:get(target_transport, Opts, barrel_rep_transport_local),
    Filter = maps:get(filter, Opts, #{}),
    Checks = [fun() -> barrel_rep_filter:check(Filter) end,
              fun() -> check_durability(Opts) end,
              fun() ->
                  case lists:any(fun has_userinfo/1, [Source, Target]) of
                      true -> {error, {invalid_sync_url, userinfo}};
                      false -> ok
                  end
              end,
              fun() -> reconcile_capable(Source, Target, ST, TT) end,
              fun() -> writable(Target, TT) end],
    case run_checks(Checks) of
        ok ->
            Ctx = #{source => Source, target => Target, st => ST, tt => TT,
                    opts => build_changes_opts(maps:get(batch_size, Opts, 100),
                                               Filter)},
            case reconcile_docs(Ctx, first, reconcile_stats()) of
                {ok, DocStats} -> reconcile_atts(Ctx, Opts, DocStats);
                {error, _} = Error -> Error
            end;
        {error, _} = Error ->
            Error
    end.

run_checks([]) -> ok;
run_checks([Check | Rest]) ->
    case Check() of
        ok -> run_checks(Rest);
        {error, _} = Error -> Error
    end.

check_durability(Opts) ->
    case maps:get(att_durability, Opts, required) of
        D when D =:= required; D =:= best_effort -> ok;
        Bad -> {error, {invalid_option, {att_durability, Bad}}}
    end.

%% Body repair needs whole states on both sides.
reconcile_capable(Source, Target, ST, TT) ->
    Has = fun(M, F, A) ->
              _ = code:ensure_loaded(M),
              erlang:function_exported(M, F, A)
          end,
    Full = fun(M, E) ->
               not Has(M, full_state, 1) orelse M:full_state(E)
           end,
    case Has(ST, get_doc_state, 2) andalso Has(TT, get_doc_state, 2)
            andalso Has(TT, put_doc_state, 4)
            andalso Has(TT, repair_doc_bodies, 3)
            andalso Full(ST, Source) andalso Full(TT, Target) of
        true -> ok;
        false -> {error, full_state_unsupported}
    end.

writable(Target, TT) ->
    case TT:db_info(Target) of
        {ok, #{read_only := true}} -> {error, read_only};
        {ok, _} -> ok;
        {error, _} = Error -> Error
    end.

reconcile_stats() ->
    #{docs_checked => 0, bodies_repaired => 0, docs_written => 0,
      doc_failures => []}.

reconcile_docs(#{source := Source, st := ST, opts := ChangesOpts} = Ctx,
               Since, Stats) ->
    case ST:get_changes(Source, Since, ChangesOpts) of
        {ok, [], LastSeq} ->
            case barrel_rep_checkpoint:empty_page_advanced(Since, LastSeq) of
                true -> reconcile_docs(Ctx, LastSeq, Stats);
                false -> {ok, Stats}
            end;
        {ok, Changes, LastSeq} ->
            case barrel_rep_checkpoint:seq_advanced(Since, LastSeq) of
                false ->
                    {error, {no_progress, Since, LastSeq}};
                true ->
                    Ids = lists:usort([Id || #{id := Id} <- Changes]),
                    reconcile_docs(Ctx, LastSeq,
                                   lists:foldl(fun(Id, Acc) ->
                                                   reconcile_doc(Ctx, Id, Acc)
                                               end, Stats, Ids))
            end;
        {error, _} = Error ->
            Error
    end.

reconcile_doc(#{source := Source, st := ST} = Ctx, DocId, Stats) ->
    case ST:get_doc_state(Source, DocId) of
        {ok, #{state := State, bodies := Bodies} = R} ->
            Stats1 = bump(docs_checked, 1, Stats),
            case repair_bodies(Ctx, DocId, Bodies) of
                {ok, N} ->
                    merge_state(Ctx, DocId, State, Bodies,
                                maps:get(vectors, R, #{}),
                                bump(bodies_repaired, N, Stats1));
                {error, Reason} ->
                    doc_failed(DocId, Reason, Stats1)
            end;
        {error, not_found} ->
            Stats;
        {error, Reason} ->
            doc_failed(DocId, Reason, Stats)
    end.

%% A token names one body: one the target stores differently is damage.
repair_bodies(#{target := Target, tt := TT}, DocId, Bodies) ->
    case TT:get_doc_state(Target, DocId) of
        {ok, #{bodies := Have}} ->
            Differ = maps:filter(fun(Tok, B) ->
                                     case Have of
                                         #{Tok := B} -> false;
                                         #{Tok := _} -> true;
                                         _ -> false
                                     end
                                 end, Bodies),
            case map_size(Differ) of
                0 ->
                    {ok, 0};
                _ ->
                    case TT:repair_doc_bodies(Target, DocId, Differ) of
                        {ok, _, Tokens} -> {ok, length(Tokens)};
                        {error, _} = Error -> Error
                    end
            end;
        {error, not_found} ->
            {ok, 0};
        {error, _} = Error ->
            Error
    end.

merge_state(#{target := Target, tt := TT}, DocId, State, Bodies, Vectors,
            Stats) ->
    case barrel_rep_alg:put_state(TT, Target, DocId, State, Bodies, Vectors) of
        {ok, _, _} -> bump(docs_written, 1, Stats);
        {unchanged, _, _} -> Stats;
        {error, Reason} -> doc_failed(DocId, Reason, Stats)
    end.

doc_failed(DocId, Reason, #{doc_failures := F} = Stats) ->
    logger:warning("reconcile: ~p failed: ~p", [DocId, Reason]),
    Stats#{doc_failures => [DocId | F]}.

reconcile_atts(#{source := Source, target := Target, st := ST, tt := TT},
               Opts, DocStats) ->
    Done = DocStats#{doc_failures => lists:reverse(
                                       maps:get(doc_failures, DocStats))},
    Att = case maps:get(attachments, Opts, true) of
        true -> barrel_rep_att:reconcile(Source, Target, ST, TT, Opts);
        false -> disabled
    end,
    case Att of
        {ok, #{failed := Failed} = AttStats} ->
            {ok, Done#{atts_resent => maps:get(atts_written, AttStats)
                                          + maps:get(atts_touched, AttStats),
                       att_failures => lists:reverse(Failed),
                       att_sync => maps:remove(failed, AttStats)}};
        {error, _} = Error ->
            Error;
        Other ->
            {ok, Done#{atts_resent => 0, att_failures => [],
                       att_sync => Other}}
    end.

bump(Key, N, Stats) ->
    maps:update_with(Key, fun(V) -> V + N end, N, Stats).

%% @doc Perform one-shot replication with config only.
%%
%% Convenience function that uses default options.
%%
%% @param Config Replication configuration map
%% @returns `{ok, rep_result()}' with replication statistics
-spec replicate_one_shot(rep_config()) -> {ok, rep_result()} | {error, term()}.
replicate_one_shot(Config) ->
    replicate_one_shot(Config, #{}).

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Run replication loop
do_replicate(Source, Target, SourceTransport, TargetTransport, Since,
             BatchSize, CheckpointSize, Checkpoint, Filter) ->
    do_replicate(Source, Target, SourceTransport, TargetTransport, Since,
                 BatchSize, CheckpointSize, Checkpoint, Filter, new_stats(), 0).

do_replicate(Source, Target, SourceTransport, TargetTransport, Since,
             BatchSize, CheckpointSize, Checkpoint, Filter, AccStats, DocsProcessed) ->
    %% Build changes options with limit and optional filters
    ChangesOpts = build_changes_opts(BatchSize, maps:get(filter, Filter)),
    %% Get next batch of changes
    case SourceTransport:get_changes(Source, Since, ChangesOpts) of
        {ok, [], LastSeq} ->
            %% A filtered page can be empty while the feed moved on:
            %% continue from there; no progress means the end
            case barrel_rep_checkpoint:empty_page_advanced(Since, LastSeq) of
                true ->
                    do_replicate(Source, Target, SourceTransport,
                                 TargetTransport, LastSeq, BatchSize,
                                 CheckpointSize,
                                 barrel_rep_checkpoint:set_last_seq(
                                   LastSeq, Checkpoint),
                                 Filter, AccStats, DocsProcessed);
                false ->
                    {ok, AccStats, Checkpoint}
            end;

        {ok, Changes, LastSeq} ->
            case barrel_rep_checkpoint:seq_advanced(Since, LastSeq) of
                false ->
                    %% A non-empty batch that did not advance the sequence
                    %% means a non-conforming source. Abort instead of
                    %% re-requesting the same point forever.
                    {error, {no_progress, Since, LastSeq}};
                true ->
                    %% Replicate this batch
                    case replicate_batch(
                             Source, Target, SourceTransport, TargetTransport,
                             Changes, Filter) of
                        {ok, BatchStats} ->
                            do_replicate_batch_done(
                                Source, Target, SourceTransport, TargetTransport,
                                BatchSize, CheckpointSize, Checkpoint, Filter,
                                AccStats, DocsProcessed, Changes, LastSeq,
                                BatchStats);
                        {error, _} = BatchError ->
                            %% a target-side batch failure (network, auth)
                            %% aborts the run with the error
                            BatchError
                    end
            end;

        {error, _} = Error ->
            Error
    end.

do_replicate_batch_done(Source, Target, SourceTransport, TargetTransport,
                        BatchSize, CheckpointSize, Checkpoint, Filter,
                        AccStats, DocsProcessed, Changes, LastSeq,
                        BatchStats) ->
    %% Merge stats
    MergedStats = merge_stats(AccStats, BatchStats),

    %% Failed docs join the pending set saved with the new position
    Pending = barrel_rep_checkpoint:get_pending(Checkpoint)
        ++ maps:get(failed, BatchStats, []),
    case length(lists:usort(Pending))
            > barrel_rep_checkpoint:max_pending(Checkpoint) of
        true ->
            %% the last saved entry stands; nothing is skipped
            {error, too_many_failed_docs};
        false ->
            do_replicate_continue(
                Source, Target, SourceTransport, TargetTransport, BatchSize,
                CheckpointSize,
                barrel_rep_checkpoint:set_pending(Pending, Checkpoint),
                Filter, MergedStats, DocsProcessed, Changes, LastSeq)
    end.

do_replicate_continue(Source, Target, SourceTransport, TargetTransport,
                      BatchSize, CheckpointSize, Checkpoint, Filter,
                      MergedStats, DocsProcessed, Changes, LastSeq) ->
    %% Update checkpoint
    Checkpoint2 = barrel_rep_checkpoint:set_last_seq(LastSeq, Checkpoint),
    NewDocsProcessed = DocsProcessed + length(Changes),

    %% Maybe write checkpoint; a failed write ends the run
    MaybeWritten = case NewDocsProcessed >= CheckpointSize of
        true ->
            barrel_rep_checkpoint:maybe_write_checkpoint(Checkpoint2);
        false ->
            {ok, Checkpoint2}
    end,
    case MaybeWritten of
        {error, _} = CpError ->
            CpError;
        {ok, Checkpoint3} ->
            %% Continue with next batch
            do_replicate(Source, Target, SourceTransport, TargetTransport,
                         LastSeq, BatchSize, CheckpointSize, Checkpoint3,
                         Filter, MergedStats,
                         NewDocsProcessed rem CheckpointSize)
    end.

%% @private One batch of the doc phase, then (filtered runs) the
%% attachments of its documents, before the position moves past them.
replicate_batch(Source, Target, SourceTransport, TargetTransport, Changes,
                #{att := AttCtx}) ->
    case barrel_rep_alg:replicate(Source, Target, SourceTransport,
                                  TargetTransport, Changes) of
        {ok, _} = Ok ->
            Ids = [Id || #{id := Id} = C <- Changes,
                         not maps:get(deleted, C, false)],
            case barrel_rep_att:sync_docs(AttCtx, Ids) of
                {ok, _} -> Ok;
                {error, Reason} -> {error, {att_sync_failed, Reason}}
            end;
        Other ->
            Other
    end.

%% @private The attachment context for a filtered run's doc phase.
doc_atts(Config, Opts, RepId) ->
    case maps:get(filter, Opts, #{}) of
        Filter when map_size(Filter) > 0 ->
            case maps:get(attachments, Opts, true) of
                true ->
                    #{source := Source, target := Target,
                      source_transport := ST, target_transport := TT} = Config,
                    case barrel_rep_att:prepare(Source, Target, ST, TT,
                                                Opts#{rep_id => RepId}) of
                        {error, Reason} -> {error, {att_sync_failed, Reason}};
                        Prepared -> {ok, unwrap(Prepared)}
                    end;
                false ->
                    {ok, skipped}
            end;
        _ ->
            {ok, skipped}
    end.

unwrap({ok, Ctx}) -> Ctx;
unwrap(skipped) -> skipped.

%% @private Retry the pending ids by id, before the scan.
retry_pending(Source, Target, SourceTransport, TargetTransport, Checkpoint) ->
    case barrel_rep_checkpoint:get_pending(Checkpoint) of
        [] ->
            {ok, new_stats(), Checkpoint};
        Pending ->
            case barrel_rep_alg:replicate_ids(Source, Target, SourceTransport,
                                              TargetTransport, Pending,
                                              #{retry => true}) of
                {ok, #{failed := Failed} = Stats} ->
                    {ok, Stats#{failed => []},
                     barrel_rep_checkpoint:set_pending(Failed, Checkpoint)};
                {error, _} = Err ->
                    Err
            end
    end.

%% @doc Deterministic replication ID. Each endpoint contributes its
%% rep_id_term/1 when its transport exports one (network transports:
%% the credential-free URL), else the endpoint term itself; a filter
%% joins the hash when set (filtered streams keep their own
%% checkpoints). The unfiltered local form hashes the legacy 2-tuple
%% so existing checkpoints stay valid.
-spec rep_id(term(), module(), term(), module(), map()) -> binary().
rep_id(Source, SourceTransport, Target, TargetTransport, Filter) ->
    SourceId = endpoint_id(SourceTransport, Source),
    TargetId = endpoint_id(TargetTransport, Target),
    Data = case map_size(Filter) of
        0 -> term_to_binary({SourceId, TargetId});
        _ -> term_to_binary({SourceId, TargetId, Filter})
    end,
    Hash = crypto:hash(md5, Data),
    binary:encode_hex(Hash, lowercase).

endpoint_id(Transport, Endpoint) ->
    _ = code:ensure_loaded(Transport),
    case erlang:function_exported(Transport, rep_id_term, 1) of
        true -> Transport:rep_id_term(Endpoint);
        false -> Endpoint
    end.

%% @private A checkpoint below the source's history floor predates the
%% retention window: tombstones forgotten in that gap would never
%% replicate. Restart from the beginning (current state re-syncs).
maybe_reset_on_floor(first, _Source, _SourceTransport, _RepId) ->
    first;
maybe_reset_on_floor(StartSeq, Source, SourceTransport, RepId) ->
    Floor = try SourceTransport:db_info(Source) of
        {ok, #{history_floor := F}} -> F;
        _ -> undefined
    catch
        _:_ -> undefined
    end,
    case Floor =/= undefined andalso barrel_hlc:less(StartSeq, Floor) of
        true ->
            logger:warning(
                "replication ~s: checkpoint predates source history floor, "
                "forcing full resync", [RepId]),
            first;
        false ->
            StartSeq
    end.

%% @private Create new stats map
new_stats() ->
    #{
        docs_read => 0,
        doc_read_failures => 0,
        docs_written => 0,
        doc_write_failures => 0,
        failed => []
    }.

%% @private Merge two stats maps
merge_stats(Stats1, Stats2) ->
    maps:merge_with(fun(failed, V1, V2) -> V2 ++ V1;
                       (_K, V1, V2) -> V1 + V2
                    end, Stats1, Stats2).

%% @private Build changes options from batch size and filter
%% Filter options are AND-ed: document must match both path pattern AND query
build_changes_opts(BatchSize, Filter) ->
    %% replication: deletes pass every filter, a document passes when any
    %% live version matches
    BaseOpts = #{limit => BatchSize, replication => true},
    Opts1 = case maps:get(paths, Filter, undefined) of
        undefined -> BaseOpts;
        Paths when is_list(Paths) -> BaseOpts#{paths => Paths}
    end,
    Opts2 = case maps:get(query, Filter, undefined) of
        undefined -> Opts1;
        Query when is_map(Query) -> Opts1#{query => Query}
    end,
    case maps:get(channel, Filter, undefined) of
        undefined -> Opts2;
        Channel when is_binary(Channel) -> Opts2#{channel => Channel}
    end.
