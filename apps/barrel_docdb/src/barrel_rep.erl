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
%%   <li>`filter' - Filter options for selective replication (see below)</li>
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
    CheckpointConfig = Config#{
        id => RepId,
        options => Opts
    },
    Checkpoint = barrel_rep_checkpoint:new(CheckpointConfig),

    %% Get starting sequence. A checkpoint older than the source's
    %% history floor may predate forgotten tombstones: force a full
    %% resync so current state converges.
    StartSeq0 = barrel_rep_checkpoint:get_start_seq(Checkpoint),
    StartSeq = maybe_reset_on_floor(StartSeq0, Source, SourceTransport, RepId),

    %% Run replication
    BatchSize = maps:get(batch_size, Opts, 100),
    CheckpointSize = maps:get(checkpoint_size, Opts, 10),
    Filter = maps:get(filter, Opts, #{}),

    case do_replicate(Source, Target, SourceTransport, TargetTransport,
                      StartSeq, BatchSize, CheckpointSize, Checkpoint, Filter) of
        {ok, Stats, FinalCheckpoint} ->
            %% Write final checkpoint
            ok = barrel_rep_checkpoint:write_checkpoint(FinalCheckpoint),

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
                    Result = Stats#{
                        ok => true,
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
    ChangesOpts = build_changes_opts(BatchSize, Filter),
    %% Get next batch of changes
    case SourceTransport:get_changes(Source, Since, ChangesOpts) of
        {ok, [], _LastSeq} ->
            %% No more changes
            {ok, AccStats, Checkpoint};

        {ok, Changes, LastSeq} ->
            case barrel_rep_checkpoint:seq_advanced(Since, LastSeq) of
                false ->
                    %% A non-empty batch that did not advance the sequence
                    %% means a non-conforming source. Abort instead of
                    %% re-requesting the same point forever.
                    {error, {no_progress, Since, LastSeq}};
                true ->
                    %% Replicate this batch
                    case barrel_rep_alg:replicate(
                             Source, Target, SourceTransport, TargetTransport,
                             Changes) of
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

    %% Update checkpoint
    Checkpoint2 = barrel_rep_checkpoint:set_last_seq(LastSeq, Checkpoint),
    NewDocsProcessed = DocsProcessed + length(Changes),

    %% Maybe write checkpoint
    Checkpoint3 = case NewDocsProcessed >= CheckpointSize of
        true ->
            barrel_rep_checkpoint:maybe_write_checkpoint(Checkpoint2);
        false ->
            Checkpoint2
    end,

    %% Continue with next batch
    do_replicate(Source, Target, SourceTransport, TargetTransport, LastSeq,
                 BatchSize, CheckpointSize, Checkpoint3, Filter, MergedStats,
                 NewDocsProcessed rem CheckpointSize).

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
        doc_write_failures => 0
    }.

%% @private Merge two stats maps
merge_stats(Stats1, Stats2) ->
    maps:merge_with(fun(_K, V1, V2) -> V1 + V2 end, Stats1, Stats2).

%% @private Build changes options from batch size and filter
%% Filter options are AND-ed: document must match both path pattern AND query
build_changes_opts(BatchSize, Filter) ->
    BaseOpts = #{limit => BatchSize},
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
