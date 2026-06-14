%%%-------------------------------------------------------------------
%%% @author Benoit Chesneau
%%% @copyright (C) 2024, Benoit Chesneau
%%% @doc barrel_docdb - Public API for barrel_docdb
%%%
%%% This module provides the main public API for interacting with
%%% barrel_docdb databases. It supports:
%%%
%%% <ul>
%%%   <li>Database lifecycle management (create, open, close, delete)</li>
%%%   <li>Document CRUD operations with MVCC revision control</li>
%%%   <li>Binary attachments stored efficiently with BlobDB</li>
%%%   <li>Secondary indexes (views) with automatic updates</li>
%%%   <li>Changes feed for tracking document modifications</li>
%%%   <li>Replication primitives for syncing databases</li>
%%% </ul>
%%%
%%% == Quick Start ==
%%%
%%% ```
%%% %% Create a database
%%% {ok, _} = barrel_docdb:create_db(<<"mydb">>),
%%%
%%% %% Store a document
%%% {ok, #{<<"id">> := DocId, <<"rev">> := Rev}} =
%%%     barrel_docdb:put_doc(<<"mydb">>, #{
%%%         <<"id">> => <<"doc1">>,
%%%         <<"type">> => <<"user">>,
%%%         <<"name">> => <<"Alice">>
%%%     }),
%%%
%%% %% Retrieve the document
%%% {ok, Doc} = barrel_docdb:get_doc(<<"mydb">>, <<"doc1">>),
%%%
%%% %% Update the document (must include _rev)
%%% {ok, _} = barrel_docdb:put_doc(<<"mydb">>, Doc#{<<"name">> => <<"Bob">>}),
%%%
%%% %% Delete the document
%%% {ok, _} = barrel_docdb:delete_doc(<<"mydb">>, DocId).
%%% '''
%%%
%%% == Document Structure ==
%%%
%%% Documents are Erlang maps with the following special keys:
%%%
%%% <ul>
%%%   <li>`<<"id">>' - Document identifier (auto-generated if not provided)</li>
%%%   <li>`<<"_rev">>' - Revision identifier (managed by the system)</li>
%%%   <li>`<<"_deleted">>' - Set to `true' for deleted documents</li>
%%% </ul>
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_docdb).

-include("barrel_docdb.hrl").

%% Database lifecycle
-export([
    create_db/1,
    create_db/2,
    open_db/1,
    close_db/1,
    delete_db/1,
    db_info/1,
    db_pid/1,
    list_dbs/0
]).

%% Document CRUD
-export([
    put_doc/2,
    put_doc/3,
    put_docs/2,
    put_docs/3,
    get_doc/2,
    get_doc/3,
    get_docs/2,
    get_docs/3,
    delete_doc/2,
    delete_doc/3,
    fold_docs/3,
    fold_docs/4,
    get_conflicts/2,
    resolve_conflict/4
]).

%% Attachments
-export([
    put_attachment/4,
    get_attachment/3,
    delete_attachment/3,
    list_attachments/2,
    get_attachment_info/3
]).

%% Attachment Streaming API
-export([
    open_attachment_stream/3,
    read_attachment_chunk/1,
    close_attachment_stream/1,
    open_attachment_writer/4,
    write_attachment_chunk/2,
    finish_attachment_writer/1,
    abort_attachment_writer/1
]).

%% Query (declarative queries using path index)
-export([
    find/2,
    find/3,
    explain/2
]).

%% Changes
-export([
    get_changes/2,
    get_changes/3,
    subscribe_changes/2,
    subscribe_changes/3
]).

%% Replication primitives
-export([
    put_rev/4,
    revsdiff/3,
    revsdiff_batch/2
]).

%% Local documents (for checkpoints, not replicated)
-export([
    put_local_doc/3,
    get_local_doc/2,
    delete_local_doc/2
]).

%% System documents (global, not replicated, stored in _barrel_system db)
-export([
    put_system_doc/2,
    get_system_doc/1,
    delete_system_doc/1,
    fold_system_docs/3,
    ensure_system_db/0,
    node_id/0
]).

%% HLC (Hybrid Logical Clock) for distributed time synchronization
-export([
    get_hlc/0,
    sync_hlc/1,
    new_hlc/0
]).

%% Path Subscriptions (real-time document change notifications)
-export([
    subscribe/2,
    subscribe/3,
    unsubscribe/1
]).

%% Query Subscriptions (real-time query-based change notifications)
-export([
    subscribe_query/2,
    subscribe_query/3,
    unsubscribe_query/1
]).

%%====================================================================
%% Database Lifecycle
%%====================================================================

%% @doc Create a new database with default options.
%%
%% Creates a new database with the given name using default settings.
%% The database will be stored in the default data directory.
%%
%% == Example ==
%% ```
%% {ok, Pid} = barrel_docdb:create_db(<<"mydb">>).
%% '''
%%
%% @param Name The database name as a binary
%% @returns `{ok, Pid}' on success, `{error, already_exists}' if database exists
%% @see create_db/2
-spec create_db(binary()) -> {ok, pid()} | {error, term()}.
create_db(Name) ->
    create_db(Name, #{}).

%% @doc Create a new database with options.
%%
%% Creates a new database with custom configuration options.
%%
%% == Options ==
%% <ul>
%%   <li>`data_dir' - Directory to store database files (default: `/tmp/barrel_data')</li>
%%   <li>`store_opts' - RocksDB options for document store</li>
%%   <li>`att_opts' - RocksDB options for attachment store</li>
%% </ul>
%%
%% == Example ==
%% ```
%% {ok, Pid} = barrel_docdb:create_db(<<"mydb">>, #{
%%     data_dir => "/var/lib/barrel"
%% }).
%% '''
%%
%% @param Name The database name as a binary
%% @param Opts Configuration options map
%% @returns `{ok, Pid}' on success, `{error, already_exists}' if database exists
-spec create_db(binary(), map()) -> {ok, pid()} | {error, term()}.
create_db(Name, Opts) when is_binary(Name) ->
    case validate_db_name(Name) of
        ok ->
            case get_db(Name) of
                {ok, _Pid} ->
                    {error, already_exists};
                {error, not_found} ->
                    barrel_db_sup:start_db(Name, Opts)
            end;
        {error, _} = Err ->
            Err
    end.

%% @doc Validate a database name.
%% Accepts: lowercase alphanumerics, underscore, hyphen. First char must
%% be alphanumeric. Length 1..63. Internal system databases (prefix `_')
%% are accepted to support `_barrel_system' and similar.
-spec validate_db_name(binary()) -> ok | {error, invalid_db_name}.
validate_db_name(<<"_", Rest/binary>>) ->
    validate_db_name_chars(Rest);
validate_db_name(Name) when is_binary(Name) ->
    Size = byte_size(Name),
    if
        Size < 1; Size > 63 -> {error, invalid_db_name};
        true -> validate_db_name_chars(Name)
    end.

validate_db_name_chars(<<>>) -> ok;
validate_db_name_chars(<<C, Rest/binary>>) when
        (C >= $a andalso C =< $z);
        (C >= $0 andalso C =< $9);
        C =:= $_; C =:= $- ->
    validate_db_name_chars(Rest);
validate_db_name_chars(_) ->
    {error, invalid_db_name}.

%% @doc Open an existing database.
%%
%% Returns the pid of an already running database. Databases are
%% automatically opened when created and remain open until explicitly
%% closed or the application stops.
%%
%% == Example ==
%% ```
%% {ok, Pid} = barrel_docdb:open_db(<<"mydb">>).
%% '''
%%
%% @param Name The database name as a binary
%% @returns `{ok, Pid}' if database is open, `{error, not_found}' otherwise
-spec open_db(binary()) -> {ok, pid()} | {error, term()}.
open_db(Name) when is_binary(Name) ->
    get_db(Name).

%% @doc Close a database.
%%
%% Stops the database process and releases resources. The database
%% can be reopened by calling `create_db/1' again.
%%
%% == Example ==
%% ```
%% ok = barrel_docdb:close_db(<<"mydb">>).
%% '''
%%
%% @param Db Database name or pid
%% @returns `ok' on success, `{error, not_found}' if database doesn't exist
-spec close_db(binary() | pid()) -> ok | {error, term()}.
close_db(Name) when is_binary(Name) ->
    case get_db(Name) of
        {ok, Pid} ->
            barrel_db_server:stop(Pid),
            ok;
        {error, _} = Error ->
            Error
    end;
close_db(Pid) when is_pid(Pid) ->
    barrel_db_server:stop(Pid),
    ok.

%% @doc Delete a database and all its data.
%%
%% Permanently removes the database, including all documents, attachments,
%% and indexes. This operation cannot be undone.
%%
%% == Example ==
%% ```
%% ok = barrel_docdb:delete_db(<<"mydb">>).
%% '''
%%
%% @param Name The database name as a binary
%% @returns `ok' on success (also returns `ok' if database doesn't exist)
-spec delete_db(binary()) -> ok | {error, term()}.
delete_db(Name) when is_binary(Name) ->
    case validate_db_name(Name) of
        ok ->
            do_delete_db(Name);
        {error, _} = Err ->
            Err
    end.

do_delete_db(Name) ->
    case get_db(Name) of
        {ok, Pid} ->
            {ok, Info} = barrel_db_server:info(Pid),
            DbPath = maps:get(db_path, Info),
            barrel_db_server:stop(Pid),
            %% Remove data directory via stdlib (no shell, no injection).
            case file:del_dir_r(DbPath) of
                ok -> ok;
                {error, enoent} -> ok;
                {error, _} = Err -> Err
            end;
        {error, not_found} ->
            ok
    end.

%% @doc Get database information.
%%
%% Returns metadata about the database including its name, path, and pid.
%%
%% == Example ==
%% ```
%% {ok, Info} = barrel_docdb:db_info(<<"mydb">>),
%% Name = maps:get(name, Info).
%% '''
%%
%% @param Db Database name or pid
%% @returns `{ok, InfoMap}' with database metadata
-spec db_info(binary() | pid()) -> {ok, map()} | {error, term()}.
db_info(Name) when is_binary(Name) ->
    case get_db(Name) of
        {ok, Pid} ->
            barrel_db_server:info(Pid);
        {error, _} = Error ->
            Error
    end;
db_info(Pid) when is_pid(Pid) ->
    barrel_db_server:info(Pid).

%% @doc Get the pid of a database by name.
%%
%% Returns the process identifier for the given database name.
%% Useful for operations that require direct access to the database server.
%%
%% @param Name Database name
%% @returns `{ok, Pid}' if database exists, `{error, not_found}' otherwise
-spec db_pid(binary()) -> {ok, pid()} | {error, not_found}.
db_pid(Name) when is_binary(Name) ->
    get_db(Name).

%% @doc List all open databases.
%%
%% Returns the names of all currently open databases.
%%
%% == Example ==
%% ```
%% DbNames = barrel_docdb:list_dbs().
%% %% Returns [<<"db1">>, <<"db2">>, ...]
%% '''
%%
%% @returns List of database names
-spec list_dbs() -> [binary()].
list_dbs() ->
    lists:filtermap(
        fun({Key, Value}) ->
            case Key of
                {barrel_db, Name} when is_pid(Value), is_binary(Name) ->
                    case is_process_alive(Value) of
                        true -> {true, Name};
                        false -> false
                    end;
                _ -> false
            end
        end,
        persistent_term:get()
    ).

%%====================================================================
%% Document CRUD
%%====================================================================

%% @doc Create or update a document.
%%
%% Stores a document in the database. If the document has an `<<"id">>'
%% key, that ID is used; otherwise, a unique ID is generated.
%%
%% For updates, the document must include the current `<<"_rev">>' value.
%% This ensures optimistic concurrency control.
%%
%% == Example ==
%% ```
%% %% Create a new document
%% {ok, Result} = barrel_docdb:put_doc(<<"mydb">>, #{
%%     <<"type">> => <<"user">>,
%%     <<"name">> => <<"Alice">>
%% }),
%% DocId = maps:get(<<"id">>, Result),
%% Rev = maps:get(<<"rev">>, Result).
%% '''
%%
%% @param Db Database name or pid
%% @param Doc Document map to store
%% @returns `{ok, #{<<"id">> => DocId, <<"rev">> => Rev, <<"ok">> => true}}'
%% @see put_doc/3
-spec put_doc(binary() | pid(), map()) -> {ok, map()} | {error, term()}.
put_doc(Db, Doc) ->
    put_doc(Db, Doc, #{}).

%% @doc Create or update a document with options.
%%
%% Same as `put_doc/2' but accepts additional options.
%%
%% == Options ==
%% <ul>
%% <li>`replicate => sync' - Wait for document to reach replicas before returning</li>
%% <li>`wait_for => [Target]' - List of targets to wait for (requires replicate => sync)</li>
%% </ul>
%%
%% == Example ==
%% ```
%% %% Sync write - wait for document to reach nodeC
%% {ok, _} = barrel_docdb:put_doc(<<"mydb">>, Doc, #{
%%     replicate => sync,
%%     wait_for => [<<"http://nodeC:8080/db/mydb">>]
%% }).
%% '''
%%
%% @param Db Database name or pid
%% @param Doc Document map to store
%% @param Opts Options map
%% @returns `{ok, #{<<"id">> => DocId, <<"rev">> => Rev, <<"ok">> => true}}'
-spec put_doc(binary() | pid(), map(), map()) -> {ok, map()} | {error, term()}.
put_doc(Db, Doc, Opts) ->
    DbName = db_name(Db),
    Start = erlang:monotonic_time(millisecond),
    %% Write document locally first
    Result = with_db(Db, fun(Pid) ->
        barrel_db_server:put_doc(Pid, Doc, Opts)
    end),
    %% Record metrics
    Duration = erlang:monotonic_time(millisecond) - Start,
    barrel_metrics:inc_doc_ops(DbName, put),
    barrel_metrics:observe_doc_latency(DbName, put, Duration),
    %% Handle sync replication if requested
    case {Result, maps:get(replicate, Opts, async)} of
        {{ok, WriteResult}, sync} ->
            WaitFor = maps:get(wait_for, Opts, []),
            ResultDocId = maps:get(<<"id">>, WriteResult),
            Rev = maps:get(<<"rev">>, WriteResult),
            case wait_for_sync_replication(ResultDocId, Rev, WaitFor) of
                ok -> Result;
                {error, SyncReason} -> {error, {sync_replication_failed, SyncReason}}
            end;
        _ ->
            Result
    end.

%% @doc Wait for a document revision to reach all specified targets
-spec wait_for_sync_replication(binary(), binary(), [binary() | map()]) ->
    ok | {error, term()}.
wait_for_sync_replication(_DocId, _Rev, []) ->
    ok;
wait_for_sync_replication(DocId, Rev, Targets) ->
    wait_for_sync_replication(DocId, Rev, Targets, 10, 500).

wait_for_sync_replication(_DocId, _Rev, _Targets, 0, _Delay) ->
    {error, timeout};
wait_for_sync_replication(DocId, Rev, Targets, Retries, Delay) ->
    %% Check if doc with revision exists at all targets
    Results = lists:map(
        fun(Target) ->
            Transport = get_transport_for_target(Target),
            case Transport:get_doc(Target, DocId, #{}) of
                {ok, _Doc, #{<<"rev">> := TargetRev}} when TargetRev =:= Rev -> ok;
                {ok, _, _} -> revision_mismatch;
                {error, not_found} -> not_found;
                {error, _} -> error
            end
        end,
        Targets
    ),
    case lists:all(fun(R) -> R =:= ok end, Results) of
        true ->
            ok;
        false ->
            timer:sleep(Delay),
            wait_for_sync_replication(DocId, Rev, Targets, Retries - 1, Delay)
    end.

%% @doc Get transport module for a target
get_transport_for_target(Target) when is_binary(Target) ->
    barrel_rep_transport_local;
get_transport_for_target(#{url := _} = Target) ->
    error({http_transport_removed, Target});
get_transport_for_target(_) ->
    barrel_rep_transport_local.

%% @doc Put multiple documents in a single batch.
%%
%% Efficiently writes multiple documents in a single RocksDB batch operation.
%% This is significantly faster than calling put_doc multiple times when
%% inserting many documents.
%%
%% == Example ==
%% ```
%% Docs = [
%%     #{<<"id">> => <<"doc1">>, <<"name">> => <<"Alice">>},
%%     #{<<"id">> => <<"doc2">>, <<"name">> => <<"Bob">>},
%%     #{<<"id">> => <<"doc3">>, <<"name">> => <<"Charlie">>}
%% ],
%% Results = barrel_docdb:put_docs(<<"mydb">>, Docs),
%% %% Results = [{ok, #{...}}, {ok, #{...}}, {ok, #{...}}]
%% '''
%%
%% @param Db Database name or pid
%% @param Docs List of document maps to store
%% @returns List of `{ok, Result}' or `{error, Reason}' in same order as input
%% @see put_docs/3
-spec put_docs(binary() | pid(), [map()]) -> [{ok, map()} | {error, term()}].
put_docs(Db, Docs) ->
    put_docs(Db, Docs, #{}).

%% @doc Put multiple documents with options.
%%
%% == Options ==
%% <ul>
%%   <li>`sync' - If `true', sync to disk before returning (default: false)</li>
%% </ul>
%%
%% @param Db Database name or pid
%% @param Docs List of document maps to store
%% @param Opts Options map
%% @returns List of `{ok, Result}' or `{error, Reason}' in same order as input
-spec put_docs(binary() | pid(), [map()], map()) -> [{ok, map()} | {error, term()}].
put_docs(Db, Docs, Opts) ->
    DbName = db_name(Db),
    ExtraAttrs = #{<<"db.batch_size">> => length(Docs)},
    barrel_trace:with_db_span(put_batch, DbName, ExtraAttrs, fun() ->
        with_db(Db, fun(Pid) ->
            barrel_db_server:put_docs(Pid, Docs, Opts)
        end)
    end).

%% @doc Get a document by ID.
%%
%% Retrieves a document from the database. Returns `{error, not_found}'
%% if the document doesn't exist or has been deleted.
%%
%% == Example ==
%% ```
%% {ok, Doc} = barrel_docdb:get_doc(<<"mydb">>, <<"doc1">>),
%% Name = maps:get(<<"name">>, Doc).
%% '''
%%
%% @param Db Database name or pid
%% @param DocId Document ID
%% @returns `{ok, Document}' or `{error, not_found}'
%% @see get_doc/3
-spec get_doc(binary() | pid(), binary()) -> {ok, map()} | {error, term()}.
get_doc(Db, DocId) ->
    get_doc(Db, DocId, #{}).

%% @doc Get a document with options.
%%
%% Retrieves a document with additional options.
%%
%% == Options ==
%% <ul>
%%   <li>`include_deleted' - If `true', returns deleted documents</li>
%%   <li>`rev' - Specific revision to retrieve</li>
%% </ul>
%%
%% == Example ==
%% ```
%% %% Get a deleted document
%% {ok, Doc} = barrel_docdb:get_doc(<<"mydb">>, <<"doc1">>, #{
%%     include_deleted => true
%% }).
%% '''
%%
%% @param Db Database name or pid
%% @param DocId Document ID
%% @param Opts Options map. When `raw_body => true', returns `{ok, CborBin, Meta}'
%% @returns `{ok, Document}' or `{ok, CborBin, Meta}' or `{error, not_found}'
-spec get_doc(binary() | pid(), binary(), map()) -> {ok, map()} | {ok, binary(), map()} | {error, term()}.
get_doc(Db, DocId, Opts) ->
    DbName = db_name(Db),
    Start = erlang:monotonic_time(millisecond),
    Result = with_db(Db, fun(Pid) ->
        barrel_db_server:get_doc(Pid, DocId, Opts)
    end),
    Duration = erlang:monotonic_time(millisecond) - Start,
    barrel_metrics:inc_doc_ops(DbName, get),
    barrel_metrics:observe_doc_latency(DbName, get, Duration),
    Result.

%% @doc Get multiple documents by ID (batch read).
%%
%% Efficiently fetches multiple documents in a single operation using
%% RocksDB's multi_get for improved performance over sequential reads.
%%
%% == Example ==
%% ```
%% Results = barrel_docdb:get_docs(<<"mydb">>, [<<"doc1">>, <<"doc2">>, <<"doc3">>]),
%% %% Results = [{ok, Doc1}, {ok, Doc2}, {error, not_found}]
%% '''
%%
%% @param Db Database name or pid
%% @param DocIds List of document IDs
%% @returns List of `{ok, Document}' or `{error, not_found}' in same order as input
%% @see get_docs/3
-spec get_docs(binary() | pid(), [binary()]) -> [{ok, map()} | {error, term()}].
get_docs(Db, DocIds) ->
    get_docs(Db, DocIds, #{}).

%% @doc Get multiple documents with options (batch read).
%%
%% == Options ==
%% <ul>
%%   <li>`include_deleted' - If true, include deleted documents</li>
%% </ul>
%%
%% @param Db Database name or pid
%% @param DocIds List of document IDs
%% @param Opts Options map
%% @returns List of `{ok, Document}' or `{error, not_found}' in same order as input
-spec get_docs(binary() | pid(), [binary()], map()) -> [{ok, map()} | {error, term()}].
get_docs(Db, DocIds, Opts) ->
    DbName = db_name(Db),
    ExtraAttrs = #{<<"db.batch_size">> => length(DocIds)},
    barrel_trace:with_db_span(get_batch, DbName, ExtraAttrs, fun() ->
        with_db(Db, fun(Pid) ->
            barrel_db_server:get_docs(Pid, DocIds, Opts)
        end)
    end).

%% @doc Delete a document.
%%
%% Marks a document as deleted. The document's revision history is
%% preserved for conflict resolution and replication.
%%
%% == Example ==
%% ```
%% {ok, Result} = barrel_docdb:delete_doc(<<"mydb">>, <<"doc1">>).
%% '''
%%
%% @param Db Database name or pid
%% @param DocId Document ID
%% @returns `{ok, #{<<"id">> => DocId, <<"rev">> => NewRev, <<"ok">> => true}}'
%% @see delete_doc/3
-spec delete_doc(binary() | pid(), binary()) -> {ok, map()} | {error, term()}.
delete_doc(Db, DocId) ->
    delete_doc(Db, DocId, #{}).

%% @doc Delete a document with options.
%%
%% == Options ==
%% <ul>
%%   <li>`rev' - Expected current revision (for conflict detection)</li>
%% </ul>
%%
%% @param Db Database name or pid
%% @param DocId Document ID
%% @param Opts Options map
%% @returns `{ok, #{<<"id">> => DocId, <<"rev">> => NewRev, <<"ok">> => true}}'
-spec delete_doc(binary() | pid(), binary(), map()) -> {ok, map()} | {error, term()}.
delete_doc(Db, DocId, Opts) ->
    DbName = db_name(Db),
    Start = erlang:monotonic_time(millisecond),
    Result = with_db(Db, fun(Pid) ->
        barrel_db_server:delete_doc(Pid, DocId, Opts)
    end),
    Duration = erlang:monotonic_time(millisecond) - Start,
    barrel_metrics:inc_doc_ops(DbName, delete),
    barrel_metrics:observe_doc_latency(DbName, delete, Duration),
    Result.

%% @doc Fold over all documents in the database.
%%
%% Iterates over all non-deleted documents, calling the provided function
%% for each document. The function receives the document and an accumulator.
%%
%% == Callback Return Values ==
%% <ul>
%%   <li>`{ok, NewAcc}' - Continue with new accumulator</li>
%%   <li>`{stop, FinalAcc}' - Stop iteration with final accumulator</li>
%%   <li>`stop' - Stop iteration with current accumulator</li>
%% </ul>
%%
%% == Example ==
%% ```
%% %% Count all documents
%% {ok, Count} = barrel_docdb:fold_docs(<<"mydb">>,
%%     fun(_Doc, Acc) -> {ok, Acc + 1} end,
%%     0
%% ).
%% '''
%%
%% @param Db Database name or pid
%% @param Fun Callback function `fun((Doc, Acc) -> {ok, Acc} | {stop, Acc} | stop)'
%% @param Acc Initial accumulator value
%% @returns `{ok, FinalAcc}'
-spec fold_docs(binary() | pid(), fun((map(), term()) -> {ok, term()} | {stop, term()} | stop), term()) ->
    {ok, term()}.
fold_docs(Db, Fun, Acc) ->
    DbName = db_name(Db),
    barrel_trace:with_db_span(fold, DbName, fun() ->
        with_db(Db, fun(Pid) ->
            barrel_db_server:fold_docs(Pid, Fun, Acc)
        end)
    end).

%% @doc Fold over all documents with options.
%%
%% Same as fold_docs/3 but accepts an options map.
%%
%% == Options ==
%% - `include_deleted': boolean() - include deleted documents (default: false)
%%
%% @param Db Database name or pid
%% @param Fun Callback function `fun((Doc, Acc) -> {ok, Acc} | {stop, Acc} | stop)'
%% @param Acc Initial accumulator value
%% @param Opts Options map
%% @returns `{ok, FinalAcc}'
-spec fold_docs(binary() | pid(), fun((map(), term()) -> {ok, term()} | {stop, term()} | stop), term(), map()) ->
    {ok, term()}.
fold_docs(Db, Fun, Acc, Opts) when is_map(Opts) ->
    DbName = db_name(Db),
    barrel_trace:with_db_span(fold, DbName, fun() ->
        with_db(Db, fun(Pid) ->
            barrel_db_server:fold_docs(Pid, Fun, Acc, Opts)
        end)
    end).

%% @doc Get list of conflicting revisions for a document.
%%
%% Returns the list of revision IDs that are in conflict with the current
%% winning revision. An empty list means no conflicts.
%%
%% == Example ==
%% ```
%% {ok, Conflicts} = barrel_docdb:get_conflicts(<<"mydb">>, <<"doc1">>).
%% %% Conflicts = [<<"2-abc">>, <<"2-def">>]
%% '''
%%
%% @param Db Database name or pid
%% @param DocId Document ID
%% @returns `{ok, [RevId]}' or `{error, not_found}'
-spec get_conflicts(binary() | pid(), binary()) -> {ok, [binary()]} | {error, term()}.
get_conflicts(Db, DocId) ->
    with_db(Db, fun(Pid) ->
        barrel_db_server:get_conflicts(Pid, DocId)
    end).

%% @doc Resolve a document conflict.
%%
%% Allows explicit resolution of conflicting revisions. Two resolution
%% strategies are supported:
%%
%% == Choose Resolution ==
%% Pick one of the existing revisions as the winner. All other branches
%% are marked as deleted.
%% ```
%% {ok, Result} = barrel_docdb:resolve_conflict(<<"mydb">>, <<"doc1">>,
%%     <<"2-winner">>, {choose, <<"2-abc">>}).
%% '''
%%
%% == Merge Resolution ==
%% Provide a new merged document that supersedes all conflicting branches.
%% ```
%% MergedDoc = #{<<"name">> => <<"Merged Name">>, <<"value">> => 42},
%% {ok, Result} = barrel_docdb:resolve_conflict(<<"mydb">>, <<"doc1">>,
%%     <<"2-winner">>, {merge, MergedDoc}).
%% '''
%%
%% @param Db Database name or pid
%% @param DocId Document ID
%% @param BaseRev The current winning revision (for optimistic locking)
%% @param Resolution Either `{choose, RevId}' or `{merge, Doc}'
%% @returns `{ok, #{id, rev, conflicts_resolved}}' or `{error, Reason}'
-spec resolve_conflict(binary() | pid(), binary(), binary(),
                       {choose, binary()} | {merge, map()}) ->
    {ok, map()} | {error, term()}.
resolve_conflict(Db, DocId, BaseRev, Resolution) ->
    with_db(Db, fun(Pid) ->
        barrel_db_server:resolve_conflict(Pid, DocId, BaseRev, Resolution)
    end).

%%====================================================================
%% Attachments
%%====================================================================

%% @doc Attach binary data to a document.
%%
%% Stores a binary attachment associated with a document. Attachments
%% are stored in a separate BlobDB-enabled RocksDB instance optimized
%% for large binary data.
%%
%% == Example ==
%% ```
%% Data = <<"Hello, World!">>,
%% {ok, Info} = barrel_docdb:put_attachment(<<"mydb">>, <<"doc1">>,
%%     <<"greeting.txt">>, Data).
%% '''
%%
%% @param Db Database name or pid
%% @param DocId Document ID
%% @param AttName Attachment name
%% @param Data Binary data to store
%% @returns `{ok, AttachmentInfo}'
-spec put_attachment(binary() | pid(), binary(), binary(), binary()) ->
    {ok, map()} | {error, term()}.
put_attachment(Db, DocId, AttName, Data) ->
    with_db(Db, fun(Pid) ->
        {ok, AttRef} = barrel_db_server:get_att_ref(Pid),
        {ok, Info} = barrel_db_server:info(Pid),
        DbName = maps:get(name, Info),
        barrel_att:put_attachment(AttRef, DbName, DocId, AttName, Data)
    end).

%% @doc Retrieve an attachment.
%%
%% Gets the binary data of an attachment.
%%
%% == Example ==
%% ```
%% {ok, Data} = barrel_docdb:get_attachment(<<"mydb">>, <<"doc1">>,
%%     <<"greeting.txt">>).
%% '''
%%
%% @param Db Database name or pid
%% @param DocId Document ID
%% @param AttName Attachment name
%% @returns `{ok, BinaryData}' or `{error, not_found}'
-spec get_attachment(binary() | pid(), binary(), binary()) ->
    {ok, binary()} | {error, term()}.
get_attachment(Db, DocId, AttName) ->
    with_db(Db, fun(Pid) ->
        {ok, AttRef} = barrel_db_server:get_att_ref(Pid),
        {ok, Info} = barrel_db_server:info(Pid),
        DbName = maps:get(name, Info),
        barrel_att:get_attachment(AttRef, DbName, DocId, AttName)
    end).

%% @doc Delete an attachment.
%%
%% Removes an attachment from a document.
%%
%% @param Db Database name or pid
%% @param DocId Document ID
%% @param AttName Attachment name
%% @returns `ok' or `{error, not_found}'
-spec delete_attachment(binary() | pid(), binary(), binary()) -> ok | {error, term()}.
delete_attachment(Db, DocId, AttName) ->
    with_db(Db, fun(Pid) ->
        {ok, AttRef} = barrel_db_server:get_att_ref(Pid),
        {ok, Info} = barrel_db_server:info(Pid),
        DbName = maps:get(name, Info),
        barrel_att:delete_attachment(AttRef, DbName, DocId, AttName)
    end).

%% @doc List all attachments for a document.
%%
%% Returns the names of all attachments associated with a document.
%%
%% == Example ==
%% ```
%% AttNames = barrel_docdb:list_attachments(<<"mydb">>, <<"doc1">>).
%% %% Returns [<<"file1.txt">>, <<"image.png">>]
%% '''
%%
%% @param Db Database name or pid
%% @param DocId Document ID
%% @returns List of attachment names
-spec list_attachments(binary() | pid(), binary()) -> [binary()].
list_attachments(Db, DocId) ->
    with_db(Db, fun(Pid) ->
        {ok, AttRef} = barrel_db_server:get_att_ref(Pid),
        {ok, Info} = barrel_db_server:info(Pid),
        DbName = maps:get(name, Info),
        barrel_att:list_attachments(AttRef, DbName, DocId)
    end).

%% @doc Get attachment metadata without reading the data.
%%
%% Useful for checking attachment size, content type, etc. before downloading.
%%
%% @param Db Database name or pid
%% @param DocId Document ID
%% @param AttName Attachment name
%% @returns `{ok, AttInfo}' or `{error, not_found}'
-spec get_attachment_info(binary() | pid(), binary(), binary()) ->
    {ok, map()} | {error, term()}.
get_attachment_info(Db, DocId, AttName) ->
    with_db(Db, fun(Pid) ->
        {ok, AttRef} = barrel_db_server:get_att_ref(Pid),
        {ok, Info} = barrel_db_server:info(Pid),
        DbName = maps:get(name, Info),
        barrel_att_store:get_info(AttRef, DbName, DocId, AttName)
    end).

%%====================================================================
%% Attachment Streaming API
%%====================================================================

%% @doc Open a stream for reading an attachment in chunks.
%%
%% For large attachments, use streaming to avoid loading the entire
%% attachment into memory at once.
%%
%% == Example ==
%% ```
%% {ok, Stream} = barrel_docdb:open_attachment_stream(<<"mydb">>, <<"doc1">>, <<"large.bin">>),
%% stream_to_file(Stream, File).
%%
%% stream_to_file(Stream, File) ->
%%     case barrel_docdb:read_attachment_chunk(Stream) of
%%         {ok, Chunk, Stream2} ->
%%             file:write(File, Chunk),
%%             stream_to_file(Stream2, File);
%%         eof ->
%%             ok
%%     end.
%% '''
%%
%% @param Db Database name or pid
%% @param DocId Document ID
%% @param AttName Attachment name
%% @returns `{ok, Stream}' or `{error, not_found}'
-spec open_attachment_stream(binary() | pid(), binary(), binary()) ->
    {ok, map()} | {error, term()}.
open_attachment_stream(Db, DocId, AttName) ->
    with_db(Db, fun(Pid) ->
        {ok, AttRef} = barrel_db_server:get_att_ref(Pid),
        {ok, Info} = barrel_db_server:info(Pid),
        DbName = maps:get(name, Info),
        barrel_att_store:get_stream(AttRef, DbName, DocId, AttName)
    end).

%% @doc Read the next chunk from an attachment stream.
%%
%% @param Stream The stream returned by open_attachment_stream/3
%% @returns `{ok, Chunk, NewStream}' or `eof'
-spec read_attachment_chunk(map()) -> {ok, binary(), map()} | eof | {error, term()}.
read_attachment_chunk(Stream) ->
    barrel_att_store:read_chunk(Stream).

%% @doc Close an attachment stream.
%%
%% @param Stream The stream to close
%% @returns ok
-spec close_attachment_stream(map()) -> ok.
close_attachment_stream(Stream) ->
    barrel_att_store:close_stream(Stream).

%% @doc Open a stream for writing an attachment in chunks.
%%
%% For large attachments, use streaming to avoid loading the entire
%% attachment into memory at once.
%%
%% == Example ==
%% ```
%% {ok, Writer} = barrel_docdb:open_attachment_writer(<<"mydb">>, <<"doc1">>,
%%                                                    <<"large.bin">>, <<"application/octet-stream">>),
%% {ok, Writer2} = barrel_docdb:write_attachment_chunk(Writer, Chunk1),
%% {ok, Writer3} = barrel_docdb:write_attachment_chunk(Writer2, Chunk2),
%% {ok, AttInfo} = barrel_docdb:finish_attachment_writer(Writer3).
%% '''
%%
%% @param Db Database name or pid
%% @param DocId Document ID
%% @param AttName Attachment name
%% @param ContentType MIME content type
%% @returns `{ok, Writer}'
-spec open_attachment_writer(binary() | pid(), binary(), binary(), binary()) ->
    {ok, map()} | {error, term()}.
open_attachment_writer(Db, DocId, AttName, ContentType) ->
    with_db(Db, fun(Pid) ->
        {ok, AttRef} = barrel_db_server:get_att_ref(Pid),
        {ok, Info} = barrel_db_server:info(Pid),
        DbName = maps:get(name, Info),
        barrel_att_store:put_stream(AttRef, DbName, DocId, AttName, ContentType)
    end).

%% @doc Write a chunk of data to an attachment writer.
%%
%% @param Writer The writer returned by open_attachment_writer/4
%% @param Data Binary data to write
%% @returns `{ok, NewWriter}'
-spec write_attachment_chunk(map(), binary()) -> {ok, map()} | {error, term()}.
write_attachment_chunk(Writer, Data) ->
    barrel_att_store:write_chunk(Writer, Data).

%% @doc Finish writing an attachment and store metadata.
%%
%% @param Writer The writer to finish
%% @returns `{ok, AttInfo}'
-spec finish_attachment_writer(map()) -> {ok, map()} | {error, term()}.
finish_attachment_writer(Writer) ->
    barrel_att_store:finish_stream(Writer).

%% @doc Abort an attachment writer and clean up partial data.
%%
%% Use this to clean up when an upload fails or is cancelled before
%% finish_attachment_writer/1 is called.
%%
%% @param Writer The writer to abort
%% @returns `ok'
-spec abort_attachment_writer(map()) -> ok.
abort_attachment_writer(Writer) ->
    barrel_att_store:abort_stream(Writer).

%%====================================================================
%% Query
%%====================================================================

%% @doc Find documents matching a query specification.
%%
%% Executes a declarative query against the path index. All document
%% paths are automatically indexed, enabling ad-hoc queries without
%% predefined views.
%%
%% Note: top-level fields whose key begins with `_' (e.g. `&lt;&lt;"_meta"&gt;&gt;') are
%% reserved metadata. They are stripped before storage and are neither
%% persisted nor indexed, so they cannot be queried. Use a non-`_' top-level
%% namespace for application data.
%%
%% == Query Specification ==
%% <ul>
%%   <li>`where' - List of conditions (required, unless an id scan is used)</li>
%%   <li>`select' - Fields to return (optional, defaults to full doc)</li>
%%   <li>`order_by' - Field or variable to sort by (optional)</li>
%%   <li>`limit' - Maximum results (optional)</li>
%%   <li>`offset' - Skip first N results (optional)</li>
%%   <li>`include_docs' - Include full documents (optional, default true)</li>
%%   <li>`flat' - When true (with `include_docs'), return flat documents
%%       `Doc#{&lt;&lt;"id"&gt;&gt;}' instead of `#{&lt;&lt;"id"&gt;&gt;, &lt;&lt;"doc"&gt;&gt;}' wrappers
%%       (optional, default false)</li>
%% </ul>
%%
%% == Id scans (primary key) ==
%% Standalone scans over the document id, ordered, O(matches), without a
%% `where' clause (the id is not in the path index):
%% <ul>
%%   <li>`id_prefix' - binary; all docs whose id starts with the prefix</li>
%%   <li>`id_range' - `{Start, End}'; docs with `Start =&lt; id &lt; End'
%%       (half-open). `Start'/`End' may be `undefined' for an open bound</li>
%% </ul>
%% Hierarchical/scannable keys should be modelled in the id (e.g.
%% `&lt;&lt;"user:123"&gt;&gt;'); other fields use `where'.
%%
%% == Result shape ==
%% With `include_docs => true' (the default), each result is a wrapper
%% `#{&lt;&lt;"id"&gt;&gt; => Id, &lt;&lt;"doc"&gt;&gt; => Doc}' (use `flat => true' for the flat
%% document; flat docs carry `&lt;&lt;"id"&gt;&gt;' but not `&lt;&lt;"_rev"&gt;&gt;' - use
%% {@link get_doc/2} if the rev is needed). With `include_docs => false'
%% each result is `#{&lt;&lt;"id"&gt;&gt; => Id}'.
%%
%% == Conditions ==
%% <ul>
%%   <li>`{path, Path, Value}' - Equality match on path</li>
%%   <li>`{compare, Path, Op, Value}' - Comparison (Op: '&gt;', '&lt;', '&gt;=', '=&lt;', '=/=')</li>
%%   <li>`{'and', [Clauses]}' - All conditions must match</li>
%%   <li>`{'or', [Clauses]}' - Any condition must match</li>
%%   <li>`{'not', Clause}' - Negation</li>
%%   <li>`{in, Path, Values}' - Value in list</li>
%%   <li>`{contains, Path, Value}' - Array contains value</li>
%%   <li>`{exists, Path}' - Path exists</li>
%%   <li>`{missing, Path}' - Path does not exist</li>
%%   <li>`{regex, Path, Pattern}' - Regex match</li>
%%   <li>`{prefix, Path, Prefix}' - String prefix match</li>
%% </ul>
%%
%% == Example ==
%% ```
%% %% Find all active users in org1
%% {ok, Results} = barrel_docdb:find(<<"mydb">>, #{
%%     where => [
%%         {path, [<<"type">>], <<"user">>},
%%         {path, [<<"org_id">>], <<"org1">>},
%%         {path, [<<"status">>], <<"active">>}
%%     ],
%%     limit => 100
%% }).
%% '''
%%
%% @param Db Database name or pid
%% @param QuerySpec Query specification map
%% @returns `{ok, [Document], Meta}' or `{error, Reason}'
%%          where Meta = #{has_more => boolean(), continuation => binary(), last_seq => seq()}
%% @see find/3
%% @see explain/2
-spec find(binary() | pid(), map()) -> {ok, [map()], map()} | {error, term()}.
find(Db, QuerySpec) ->
    find(Db, QuerySpec, #{}).

%% @doc Find documents with additional options.
%%
%% Same as `find/2' but allows merging additional options into the query.
%% Supports chunked execution with continuation tokens for large result sets.
%%
%% == Example ==
%% ```
%% %% Basic query
%% {ok, Results, Meta} = barrel_docdb:find(<<"mydb">>,
%%     #{where => [{path, [<<"type">>], <<"user">>}]},
%%     #{limit => 10, include_docs => false}
%% ).
%%
%% %% Chunked iteration
%% {ok, R1, #{has_more := true, continuation := Token}} =
%%     barrel_docdb:find(Db, Query, #{chunk_size => 100}),
%% {ok, R2, #{has_more := false}} =
%%     barrel_docdb:find(Db, Query, #{continuation => Token}).
%% '''
%%
%% @param Db Database name or pid
%% @param QuerySpec Query specification map
%% @param Opts Additional options: chunk_size, continuation, include_docs, limit, etc.
%% @returns `{ok, [Document], Meta}' or `{error, Reason}'
%%          where Meta = #{has_more => boolean(), continuation => binary(), last_seq => seq()}
-spec find(binary() | pid(), map(), map()) -> {ok, [map()], map()} | {error, term()}.
find(Db, QuerySpec, Opts) ->
    MetricsDbName = db_name(Db),
    Start = erlang:monotonic_time(millisecond),
    Result = with_db(Db, fun(Pid) ->
        {ok, StoreRef} = barrel_db_server:get_store_ref(Pid),
        {ok, Info} = barrel_db_server:info(Pid),
        DbName = maps:get(name, Info),
        %% Default include_docs to true for find API
        DefaultOpts = #{include_docs => true},
        MergedSpec = maps:merge(maps:merge(DefaultOpts, QuerySpec), Opts),
        case barrel_query:compile(MergedSpec) of
            {ok, Plan} ->
                %% Use chunked execution with configurable chunk_size
                %% If query has a limit, use min(limit, chunk_size) to respect it
                DefaultChunkSize = maps:get(chunk_size, Opts, 1000),
                EffectiveChunkSize = case maps:get(limit, MergedSpec, undefined) of
                    undefined -> DefaultChunkSize;
                    Limit when Limit < DefaultChunkSize -> Limit;
                    _ -> DefaultChunkSize
                end,
                ChunkOpts = case maps:get(continuation, Opts, undefined) of
                    undefined -> #{chunk_size => EffectiveChunkSize};
                    Token -> #{chunk_size => EffectiveChunkSize, continuation => Token}
                end,
                barrel_query:execute(StoreRef, DbName, Plan, ChunkOpts);
            {error, _} = Error ->
                Error
        end
    end),
    %% Record query metrics
    Duration = erlang:monotonic_time(millisecond) - Start,
    barrel_metrics:inc_query_ops(MetricsDbName),
    barrel_metrics:observe_query_latency(MetricsDbName, Duration),
    case Result of
        {ok, Results, _Meta} ->
            barrel_metrics:observe_query_results(MetricsDbName, length(Results));
        _ ->
            ok
    end,
    Result.

%% @doc Explain a query execution plan.
%%
%% Returns information about how a query would be executed without
%% actually running it. Useful for understanding query performance
%% and optimization.
%%
%% == Example ==
%% ```
%% {ok, Explanation} = barrel_docdb:explain(<<"mydb">>, #{
%%     where => [{path, [<<"type">>], <<"user">>}]
%% }),
%% Strategy = maps:get(strategy, Explanation).
%% %% Returns: index_seek | index_scan | multi_index | full_scan
%% '''
%%
%% @param Db Database name or pid (unused, for API consistency)
%% @param QuerySpec Query specification map
%% @returns `{ok, ExplanationMap}' or `{error, Reason}'
-spec explain(binary() | pid(), map()) -> {ok, map()} | {error, term()}.
explain(Db, QuerySpec) ->
    DbName = db_name(Db),
    barrel_trace:with_db_span(explain, DbName, fun() ->
        case barrel_query:compile(QuerySpec) of
            {ok, Plan} ->
                {ok, barrel_query:explain(Plan)};
            {error, _} = Error ->
                barrel_trace:record_error(Error),
                Error
        end
    end).

%%====================================================================
%% Changes
%%====================================================================

%% @doc Get changes since an HLC timestamp.
%%
%% Returns all document changes since the given HLC timestamp. Use `first'
%% to get all changes from the beginning.
%%
%% == Example ==
%% ```
%% %% Get all changes
%% {ok, Changes, LastHlc} = barrel_docdb:get_changes(<<"mydb">>, first),
%%
%% %% Get incremental changes
%% {ok, NewChanges, NewHlc} = barrel_docdb:get_changes(<<"mydb">>, LastHlc).
%% '''
%%
%% @param Db Database name or pid
%% @param Since HLC timestamp or `first'
%% @returns `{ok, [Change], LastHlc}' where each change has id, hlc, rev, changes
%% @see get_changes/3
-spec get_changes(binary() | pid(), barrel_hlc:timestamp() | first) ->
    {ok, [map()], barrel_hlc:timestamp()}.
get_changes(Db, Since) ->
    get_changes(Db, Since, #{}).

%% @doc Get changes with options.
%%
%% == Options ==
%% <ul>
%%   <li>`limit' - Maximum number of changes to return</li>
%%   <li>`include_docs' - Include full documents in results</li>
%%   <li>`descending' - Reverse order</li>
%%   <li>`doc_ids' - Filter to specific document IDs</li>
%% </ul>
%%
%% @param Db Database name or pid
%% @param Since HLC timestamp or `first'
%% @param Opts Query options
%% @returns `{ok, [Change], LastHlc}'
-spec get_changes(binary() | pid(), barrel_hlc:timestamp() | first, map()) ->
    {ok, [map()], barrel_hlc:timestamp()}.
get_changes(Db, Since, Opts) ->
    DbName = db_name(Db),
    barrel_trace:with_db_span(changes, DbName, fun() ->
        with_db(Db, fun(Pid) ->
            {ok, StoreRef} = barrel_db_server:get_store_ref(Pid),
            {ok, Info} = barrel_db_server:info(Pid),
            DbNameInner = maps:get(name, Info),
            barrel_changes:get_changes(StoreRef, DbNameInner, Since, Opts)
        end)
    end).

%% @doc Subscribe to a changes stream.
%%
%% Returns a stream pid that can be used to iterate over changes
%% as they occur.
%%
%% == Example ==
%% ```
%% {ok, Stream} = barrel_docdb:subscribe_changes(<<"mydb">>, first),
%% %% Use barrel_changes_stream:next/1 to get changes
%% '''
%%
%% @param Db Database name or pid
%% @param Since Starting HLC timestamp
%% @returns `{ok, StreamPid}'
%% @see subscribe_changes/3
-spec subscribe_changes(binary() | pid(), barrel_hlc:timestamp() | first) ->
    {ok, pid()} | {error, term()}.
subscribe_changes(Db, Since) ->
    subscribe_changes(Db, Since, #{}).

%% @doc Subscribe to a changes stream with options.
%%
%% @param Db Database name or pid
%% @param Since Starting HLC timestamp
%% @param Opts Stream options
%% @returns `{ok, StreamPid}'
-spec subscribe_changes(binary() | pid(), barrel_hlc:timestamp() | first, map()) ->
    {ok, pid()} | {error, term()}.
subscribe_changes(Db, Since, Opts) ->
    with_db(Db, fun(Pid) ->
        {ok, StoreRef} = barrel_db_server:get_store_ref(Pid),
        {ok, Info} = barrel_db_server:info(Pid),
        DbName = maps:get(name, Info),
        StreamOpts = Opts#{since => Since},
        barrel_changes_stream:start_link(StoreRef, DbName, StreamOpts)
    end).

%%====================================================================
%% Replication Primitives
%%====================================================================

%% @doc Put a document with explicit revision history.
%%
%% This function is used by replication to store documents with their
%% full revision history. Unlike `put_doc/2', this allows specifying
%% the exact revision chain.
%%
%% == Example ==
%% ```
%% Doc = #{<<"id">> => <<"doc1">>, <<"value">> => <<"replicated">>},
%% History = [<<"2-abc123">>, <<"1-def456">>],
%% {ok, DocId, Rev} = barrel_docdb:put_rev(<<"mydb">>, Doc, History, false).
%% '''
%%
%% @param Db Database name or pid
%% @param Doc Document map (must include `<<"id">>')
%% @param History List of revision IDs, newest first
%% @param Deleted Whether this is a deletion tombstone
%% @returns `{ok, DocId, RevId}'
%% @see barrel_rep
-spec put_rev(binary() | pid(), map(), [binary()], boolean()) ->
    {ok, binary(), binary()} | {error, term()}.
put_rev(Db, Doc, History, Deleted) ->
    with_db(Db, fun(Pid) ->
        barrel_db_server:put_rev(Pid, Doc, History, Deleted)
    end).

%% @doc Find missing revisions for replication.
%%
%% Compares a list of revision IDs against those stored locally and
%% returns which revisions are missing. Used by replication to determine
%% what needs to be transferred.
%%
%% == Example ==
%% ```
%% {ok, Missing, Ancestors} = barrel_docdb:revsdiff(<<"mydb">>,
%%     <<"doc1">>,
%%     [<<"3-abc">>, <<"2-def">>, <<"1-ghi">>]
%% ),
%% %% Missing = revisions we don't have
%% %% Ancestors = our revisions that could be ancestors
%% '''
%%
%% @param Db Database name or pid
%% @param DocId Document ID
%% @param RevIds List of revision IDs to check
%% @returns `{ok, MissingRevs, PossibleAncestors}'
%% @see barrel_rep
-spec revsdiff(binary() | pid(), binary(), [binary()]) ->
    {ok, [binary()], [binary()]} | {error, term()}.
revsdiff(Db, DocId, RevIds) ->
    with_db(Db, fun(Pid) ->
        barrel_db_server:revsdiff(Pid, DocId, RevIds)
    end).

%% @doc Compare revisions for multiple documents (batch).
%%
%% Takes a map of `DocId => [RevIds]' and returns a map of
%% `DocId => #{missing => [...], possible_ancestors => [...]}'.
%%
%% This is useful for replication when checking multiple documents at once.
%%
%% == Example ==
%% ```
%% RevsMap = #{
%%     <<"doc1">> => [<<"1-abc123">>],
%%     <<"doc2">> => [<<"1-def456">>, <<"2-ghi789">>]
%% },
%% {ok, Results} = barrel_docdb:revsdiff_batch(<<"mydb">>, RevsMap),
%% %% Results = #{
%% %%     <<"doc1">> => #{missing => [<<"1-abc123">>], possible_ancestors => []},
%% %%     <<"doc2">> => #{missing => [], possible_ancestors => [<<"1-def456">>]}
%% %% }
%% '''
%%
%% @param Db Database name or pid
%% @param RevsMap Map of DocId => [RevIds] to check
%% @returns `{ok, ResultMap}' where ResultMap is DocId => #{missing => [...], possible_ancestors => [...]}
%% @see revsdiff/3
-spec revsdiff_batch(binary() | pid(), #{binary() => [binary()]}) ->
    {ok, #{binary() => #{missing => [binary()], possible_ancestors => [binary()]}}}.
revsdiff_batch(Db, RevsMap) when is_map(RevsMap) ->
    with_db(Db, fun(Pid) ->
        barrel_db_server:revsdiff_batch(Pid, RevsMap)
    end).

%%====================================================================
%% Local Documents
%%====================================================================

%% @doc Store a local document.
%%
%% Local documents are stored in the database but are NOT replicated.
%% They are typically used for storing replication checkpoints and
%% other metadata.
%%
%% == Example ==
%% ```
%% ok = barrel_docdb:put_local_doc(<<"mydb">>, <<"_local/checkpoint">>, #{
%%     <<"last_seq">> => <<"100">>
%% }).
%% '''
%%
%% @param Db Database name or pid
%% @param DocId Local document ID
%% @param Doc Document content
%% @returns `ok'
-spec put_local_doc(binary() | pid(), binary(), map()) -> ok | {error, term()}.
put_local_doc(Db, DocId, Doc) ->
    with_db(Db, fun(Pid) ->
        barrel_db_server:put_local_doc(Pid, DocId, Doc)
    end).

%% @doc Get a local document.
%%
%% @param Db Database name or pid
%% @param DocId Local document ID
%% @returns `{ok, Document}' or `{error, not_found}'
-spec get_local_doc(binary() | pid(), binary()) -> {ok, map()} | {error, not_found}.
get_local_doc(Db, DocId) ->
    with_db(Db, fun(Pid) ->
        barrel_db_server:get_local_doc(Pid, DocId)
    end).

%% @doc Delete a local document.
%%
%% @param Db Database name or pid
%% @param DocId Local document ID
%% @returns `ok' or `{error, not_found}'
-spec delete_local_doc(binary() | pid(), binary()) -> ok | {error, not_found}.
delete_local_doc(Db, DocId) ->
    with_db(Db, fun(Pid) ->
        barrel_db_server:delete_local_doc(Pid, DocId)
    end).

%%====================================================================
%% System Documents (Global)
%%====================================================================
%% System documents are global (not per-database) and stored in a
%% special _barrel_system database. They're used for global configuration
%% like replication tasks, node settings, etc.

-define(SYSTEM_DB, <<"_barrel_system">>).

%% @doc Ensure the system database exists.
%% Called automatically by system_doc operations.
-spec ensure_system_db() -> ok.
ensure_system_db() ->
    case db_info(?SYSTEM_DB) of
        {ok, _} ->
            ok;
        {error, not_found} ->
            {ok, _} = create_db(?SYSTEM_DB),
            ok
    end.

%% @doc Store a global system document.
%%
%% System documents are stored in the `_barrel_system' database and are
%% NOT replicated. They are used for global configuration and state.
%%
%% == Example ==
%% ```
%% ok = barrel_docdb:put_system_doc(<<"global_config">>, #{
%%     <<"max_dbs">> => 100
%% }).
%% '''
%%
%% @param DocId System document ID
%% @param Doc Document content
%% @returns `ok'
-spec put_system_doc(binary(), map()) -> ok | {error, term()}.
put_system_doc(DocId, Doc) ->
    ensure_system_db(),
    put_local_doc(?SYSTEM_DB, DocId, Doc).

%% @doc Get a global system document.
%%
%% @param DocId System document ID
%% @returns `{ok, Document}' or `{error, not_found}'
-spec get_system_doc(binary()) -> {ok, map()} | {error, not_found}.
get_system_doc(DocId) ->
    ensure_system_db(),
    get_local_doc(?SYSTEM_DB, DocId).

%% @doc Return this node's stable identity.
%%
%% The id is generated once (hostname plus a random suffix) and persisted in
%% the `_node_id' system document. It has no dependency on discovery or
%% federation; it is a building block an external cluster or discovery layer
%% can use to identify this node.
%%
%% @returns the node id binary
-spec node_id() -> binary().
node_id() ->
    case get_system_doc(<<"_node_id">>) of
        {ok, #{<<"node_id">> := NodeId}} ->
            NodeId;
        {error, not_found} ->
            {ok, Hostname} = inet:gethostname(),
            Random = base64:encode(crypto:strong_rand_bytes(8)),
            NodeId = <<(list_to_binary(Hostname))/binary, "-", Random/binary>>,
            ok = put_system_doc(<<"_node_id">>, #{<<"node_id">> => NodeId}),
            NodeId
    end.

%% @doc Delete a global system document.
%%
%% @param DocId System document ID
%% @returns `ok' or `{error, not_found}'
-spec delete_system_doc(binary()) -> ok | {error, not_found}.
delete_system_doc(DocId) ->
    ensure_system_db(),
    delete_local_doc(?SYSTEM_DB, DocId).

%% @doc Fold over system documents with a given prefix.
%%
%% == Example ==
%% ```
%% {ok, Federations} = barrel_docdb:fold_system_docs(
%%     <<"federation:">>,
%%     fun(_DocId, Doc, Acc) -> [Doc | Acc] end,
%%     []
%% ).
%% '''
%%
%% @param Prefix Document ID prefix to filter by
%% @param Fun Callback function(DocId, Doc, Acc) -> NewAcc
%% @param Acc0 Initial accumulator
%% @returns `{ok, FinalAcc}'
-spec fold_system_docs(binary(), fun((binary(), map(), term()) -> term()), term()) ->
    {ok, term()}.
fold_system_docs(Prefix, Fun, Acc0) ->
    ensure_system_db(),
    case db_pid(?SYSTEM_DB) of
        {ok, Pid} ->
            barrel_db_server:fold_local_docs(Pid, Prefix, Fun, Acc0);
        {error, not_found} ->
            {ok, Acc0}
    end.

%%====================================================================
%% HLC (Hybrid Logical Clock)
%%====================================================================

%% @doc Get the current global HLC timestamp.
%%
%% Returns the current Hybrid Logical Clock timestamp without advancing
%% the clock. The HLC is node-global and used for ordering events across
%% distributed machines.
%%
%% == Example ==
%% ```
%% TS = barrel_docdb:get_hlc().
%% %% TS is a #timestamp{wall_time, logical} record
%% '''
%%
%% @returns The current HLC timestamp
-spec get_hlc() -> barrel_hlc:timestamp().
get_hlc() ->
    barrel_hlc:get_hlc().

%% @doc Synchronize with a remote HLC timestamp.
%%
%% Call this when receiving data from another node to maintain causality.
%% The local clock is updated to reflect the remote timestamp, ensuring
%% that subsequent events are ordered after the received data.
%%
%% == Example ==
%% ```
%% %% When receiving data from another node:
%% RemoteHlc = ... %% HLC from remote node
%% {ok, NewHlc} = barrel_docdb:sync_hlc(RemoteHlc).
%% '''
%%
%% @param RemoteHlc The HLC timestamp from the remote node
%% @returns `{ok, UpdatedHlc}' or `{error, clock_skew}'
-spec sync_hlc(barrel_hlc:timestamp()) -> {ok, barrel_hlc:timestamp()} | {error, clock_skew}.
sync_hlc(RemoteHlc) ->
    barrel_hlc:sync_hlc(RemoteHlc).

%% @doc Generate a new HLC timestamp.
%%
%% Creates a new timestamp and advances the clock. Use this when creating
%% events that will be sent to other nodes or need to be ordered.
%%
%% == Example ==
%% ```
%% TS = barrel_docdb:new_hlc().
%% %% Use TS for ordering the event
%% '''
%%
%% @returns A new HLC timestamp
-spec new_hlc() -> barrel_hlc:timestamp().
new_hlc() ->
    barrel_hlc:new_hlc().

%%====================================================================
%% Path Subscriptions
%%====================================================================

%% @doc Subscribe to document changes matching a path pattern.
%%
%% Subscribes the calling process to receive notifications for document
%% changes that match the given MQTT-style path pattern. Notifications
%% are sent as messages of the form:
%%
%% `{barrel_change, DbName, #{id => DocId, rev => Rev, hlc => Hlc,
%%                            deleted => boolean(), paths => [binary()]}}'
%%
%% == Pattern Syntax ==
%%
%% Patterns use MQTT-style wildcards:
%% <ul>
%%   <li>`+' matches exactly one path level (e.g., `<<"users/+/profile">>')</li>
%%   <li>`#' matches zero or more levels (e.g., `<<"orders/#">>')</li>
%% </ul>
%%
%% Paths are derived from document field structure:
%% `#{<<"users">> => #{<<"123">> => #{<<"name">> => <<"Alice">>}}}'
%% produces paths like `<<"users/123/name/Alice">>'.
%%
%% == Example ==
%% ```
%% %% Subscribe to all user profile changes
%% {ok, SubRef} = barrel_docdb:subscribe(<<"mydb">>, <<"users/+/profile/#">>),
%%
%% %% Receive notifications
%% receive
%%     {barrel_change, <<"mydb">>, Change} ->
%%         io:format("Document ~s changed~n", [maps:get(id, Change)])
%% end,
%%
%% %% Unsubscribe when done
%% ok = barrel_docdb:unsubscribe(SubRef).
%% '''
%%
%% @param DbName The database name
%% @param Pattern MQTT-style path pattern to match
%% @returns `{ok, SubRef}' on success, `{error, invalid_pattern}' if pattern is invalid
%% @see unsubscribe/1
-spec subscribe(db_name(), binary()) -> {ok, reference()} | {error, term()}.
subscribe(DbName, Pattern) ->
    subscribe(DbName, Pattern, #{}).

%% @doc Subscribe to document changes with options.
%%
%% Same as {@link subscribe/2} but with additional options.
%%
%% == Options ==
%% Currently no options are supported (reserved for future use).
%%
%% @param DbName The database name
%% @param Pattern MQTT-style path pattern to match
%% @param Opts Options map (reserved for future use)
%% @returns `{ok, SubRef}' on success, `{error, invalid_pattern}' if pattern is invalid
%% @see subscribe/2
%% @see unsubscribe/1
-spec subscribe(db_name(), binary(), map()) -> {ok, reference()} | {error, term()}.
subscribe(DbName, Pattern, _Opts) ->
    barrel_sub:subscribe(DbName, Pattern, self()).

%% @doc Unsubscribe from document change notifications.
%%
%% Removes a subscription previously created with {@link subscribe/2} or
%% {@link subscribe/3}. After calling this function, no more notifications
%% will be received for the given subscription.
%%
%% == Example ==
%% ```
%% {ok, SubRef} = barrel_docdb:subscribe(<<"mydb">>, <<"users/#">>),
%% %% ... receive some notifications ...
%% ok = barrel_docdb:unsubscribe(SubRef).
%% '''
%%
%% @param SubRef The subscription reference returned by subscribe/2,3
%% @returns `ok'
%% @see subscribe/2
-spec unsubscribe(reference()) -> ok.
unsubscribe(SubRef) ->
    barrel_sub:unsubscribe(SubRef, self()).

%%====================================================================
%% Query Subscriptions
%%====================================================================

%% @doc Subscribe to document changes matching a query.
%%
%% Subscribes to document change notifications for documents that match
%% the specified query. Only documents that match the query conditions
%% will trigger notifications.
%%
%% Query subscriptions are optimized using path extraction - the full
%% query is only evaluated when a change affects paths referenced by
%% the query.
%%
%% == Example ==
%% ```
%% %% Subscribe to changes for user documents
%% Query = #{where => [{path, [<<"type">>], <<"user">>}]},
%% {ok, SubRef} = barrel_docdb:subscribe_query(<<"mydb">>, Query),
%%
%% %% Receive notifications
%% receive
%%     {barrel_query_change, <<"mydb">>, #{id := DocId, rev := Rev}} ->
%%         io:format("Document ~s changed~n", [DocId])
%% end,
%%
%% %% Unsubscribe when done
%% ok = barrel_docdb:unsubscribe_query(SubRef).
%% '''
%%
%% @param DbName The database name
%% @param Query Query specification (same format as barrel_query)
%% @returns `{ok, SubRef}' on success, `{error, Reason}' on failure
%% @see subscribe_query/3
%% @see unsubscribe_query/1
-spec subscribe_query(db_name(), barrel_query:query_spec()) ->
    {ok, reference()} | {error, term()}.
subscribe_query(DbName, Query) ->
    subscribe_query(DbName, Query, #{}).

%% @doc Subscribe to document changes matching a query with options.
%%
%% Same as {@link subscribe_query/2} but with additional options.
%%
%% @param DbName The database name
%% @param Query Query specification
%% @param Opts Options (reserved for future use)
%% @returns `{ok, SubRef}' on success, `{error, Reason}' on failure
%% @see subscribe_query/2
%% @see unsubscribe_query/1
-spec subscribe_query(db_name(), barrel_query:query_spec(), map()) ->
    {ok, reference()} | {error, term()}.
subscribe_query(DbName, Query, _Opts) ->
    barrel_query_sub:subscribe(DbName, Query, self()).

%% @doc Unsubscribe from query-based change notifications.
%%
%% Removes a query subscription previously created with
%% {@link subscribe_query/2} or {@link subscribe_query/3}.
%%
%% == Example ==
%% ```
%% {ok, SubRef} = barrel_docdb:subscribe_query(<<"mydb">>, Query),
%% %% ... receive some notifications ...
%% ok = barrel_docdb:unsubscribe_query(SubRef).
%% '''
%%
%% @param SubRef The subscription reference returned by subscribe_query/2,3
%% @returns `ok'
%% @see subscribe_query/2
-spec unsubscribe_query(reference()) -> ok.
unsubscribe_query(SubRef) ->
    barrel_query_sub:unsubscribe(SubRef).

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private Extract database name from pid or binary
-spec db_name(binary() | pid()) -> binary().
db_name(Name) when is_binary(Name) ->
    Name;
db_name(Pid) when is_pid(Pid) ->
    %% For pid, we need to look up the name - use unknown for metrics if not found
    case process_info(Pid, registered_name) of
        {registered_name, _} ->
            %% Try to find name from persistent_term
            <<"unknown">>;
        _ ->
            <<"unknown">>
    end.

%% @private Get database pid by name
-spec get_db(binary()) -> {ok, pid()} | {error, not_found}.
get_db(Name) when is_binary(Name) ->
    case persistent_term:get({barrel_db, Name}, undefined) of
        undefined ->
            {error, not_found};
        Pid when is_pid(Pid) ->
            case is_process_alive(Pid) of
                true -> {ok, Pid};
                false ->
                    persistent_term:erase({barrel_db, Name}),
                    {error, not_found}
            end
    end.

%% @private Execute function with database pid
-spec with_db(binary() | pid(), fun((pid()) -> term())) -> term().
with_db(Pid, Fun) when is_pid(Pid) ->
    Fun(Pid);
with_db(Name, Fun) when is_binary(Name) ->
    case get_db(Name) of
        {ok, Pid} ->
            Fun(Pid);
        {error, _} = Error ->
            Error
    end.
