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
    put_version/5,
    diff_versions/2,
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

%% @doc Get a document for replication (tombstones included) with its
%% version protocol metadata.
-spec get_doc(db_name(), docid(), map()) ->
    {ok, map(), map()} | {error, not_found} | {error, term()}.
get_doc(DbName, DocId, _Opts) ->
    case barrel_docdb:get_doc_for_replication(DbName, DocId) of
        {ok, #{doc := Doc, version := Token, vv := VVBin, deleted := Deleted}} ->
            {ok, Doc, #{version => Token, vv => VVBin, deleted => Deleted}};
        {error, _} = Error ->
            Error
    end.

%% @doc Apply a replicated version.
-spec put_version(db_name(), map(), binary(), binary(), boolean()) ->
    {ok, docid(), binary()} | {error, term()}.
put_version(DbName, Doc, VersionToken, VVBin, Deleted) ->
    barrel_docdb:put_version(DbName, Doc, VersionToken, VVBin, Deleted).

%% @doc Which offered versions is this database missing?
-spec diff_versions(db_name(), #{docid() => binary()}) ->
    {ok, #{docid() => missing | have}} | {error, term()}.
diff_versions(DbName, TokenMap) ->
    barrel_docdb:diff_versions(DbName, TokenMap).

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
