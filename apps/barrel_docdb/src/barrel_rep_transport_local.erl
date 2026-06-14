%%%-------------------------------------------------------------------
%%% @doc barrel_rep_transport_local - Local transport for replication
%%%
%%% Transport implementation for replicating between databases in
%%% the same Erlang VM. The endpoint is simply the database name.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_rep_transport_local).

-behaviour(barrel_rep_transport).

-include("barrel_docdb.hrl").

%% barrel_rep_transport callbacks
-export([
    get_doc/3,
    put_rev/4,
    revsdiff/3,
    get_changes/3,
    get_local_doc/2,
    put_local_doc/3,
    delete_local_doc/2,
    db_info/1,
    sync_hlc/2
]).

%%====================================================================
%% barrel_rep_transport callbacks
%%====================================================================

%% @doc Get a document with options
-spec get_doc(db_name(), docid(), map()) ->
    {ok, map(), map()} | {error, not_found} | {error, term()}.
get_doc(DbName, DocId, Opts) ->
    IncludeHistory = maps:get(history, Opts, false),
    GetOpts = case maps:get(rev, Opts, undefined) of
        undefined -> #{};
        RequestedRev -> #{rev => RequestedRev}
    end,
    case barrel_docdb:get_doc(DbName, DocId, GetOpts) of
        {ok, Doc} ->
            Rev = maps:get(<<"_rev">>, Doc),
            Meta = #{
                <<"rev">> => Rev,
                <<"deleted">> => maps:get(<<"_deleted">>, Doc, false)
            },
            Meta2 = case IncludeHistory of
                true ->
                    %% Build revisions from rev
                    Revisions = build_revisions(Rev),
                    Meta#{<<"revisions">> => Revisions};
                false ->
                    Meta
            end,
            {ok, Doc, Meta2};
        {error, _} = Error ->
            Error
    end.

%% @doc Put a document with explicit revision history
-spec put_rev(db_name(), map(), [revid()], boolean()) ->
    {ok, docid(), revid()} | {error, term()}.
put_rev(DbName, Doc, History, Deleted) ->
    barrel_docdb:put_rev(DbName, Doc, History, Deleted).

%% @doc Get revision differences
-spec revsdiff(db_name(), docid(), [revid()]) ->
    {ok, [revid()], [revid()]} | {error, term()}.
revsdiff(DbName, DocId, RevIds) ->
    barrel_docdb:revsdiff(DbName, DocId, RevIds).

%% @doc Get changes since a sequence
-spec get_changes(db_name(), seq() | first, map()) ->
    {ok, [map()], seq()} | {error, term()}.
get_changes(DbName, Since, Opts) ->
    barrel_docdb:get_changes(DbName, Since, Opts).

%% @doc Get a local document
-spec get_local_doc(db_name(), docid()) ->
    {ok, map()} | {error, not_found} | {error, term()}.
get_local_doc(DbName, DocId) ->
    barrel_docdb:get_local_doc(DbName, DocId).

%% @doc Put a local document
-spec put_local_doc(db_name(), docid(), map()) -> ok | {error, term()}.
put_local_doc(DbName, DocId, Doc) ->
    barrel_docdb:put_local_doc(DbName, DocId, Doc).

%% @doc Delete a local document
-spec delete_local_doc(db_name(), docid()) -> ok | {error, not_found} | {error, term()}.
delete_local_doc(DbName, DocId) ->
    barrel_docdb:delete_local_doc(DbName, DocId).

%% @doc Get database info
-spec db_info(db_name()) -> {ok, map()} | {error, term()}.
db_info(DbName) ->
    barrel_docdb:db_info(DbName).

%% @doc Synchronize local HLC with remote timestamp
%% This ensures the local clock reflects causality with remote events.
%% Note: HLC is global to the node, not per-database.
-spec sync_hlc(db_name(), barrel_hlc:timestamp()) ->
    {ok, barrel_hlc:timestamp()} | {error, term()}.
sync_hlc(_DbName, RemoteHlc) ->
    barrel_docdb:sync_hlc(RemoteHlc).

%%====================================================================
%% Internal functions
%%====================================================================

%% @doc Build revisions structure from a single revision
%% In a full implementation, this would query the revision tree
build_revisions(Rev) ->
    {Gen, Hash} = barrel_doc:parse_revision(Rev),
    #{
        <<"start">> => Gen,
        <<"ids">> => [Hash]
    }.
