%%%-------------------------------------------------------------------
%%% @doc Document body storage using body column family (BlobDB enabled)
%%%
%%% Document bodies are stored in the "bodies" column family which has
%%% BlobDB enabled. This keeps the main LSM tree lean (only indexes and
%%% metadata) and provides efficient batch fetching with multi_get.
%%%
%%% The store is accessed via persistent_term registry using the db name.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_doc_body_store).

-include("barrel_docdb.hrl").

%% API - Current body (no revision in key)
-export([get_current_body/2, multi_get_current_bodies/2, multi_get_current_bodies/3]).
-export([multi_get_current_bodies_with_snapshot/3, multi_get_current_bodies_with_snapshot/4]).
%% API - Specific revision body (revision in key, for archived revisions)
-export([get_body/3, multi_get_bodies/3]).

-type read_profile() :: barrel_store_rocksdb:read_profile().
-type snapshot() :: barrel_store_rocksdb:snapshot().

%%====================================================================
%% API - Current Body (Preferred)
%%====================================================================

%% @doc Get the current body for a document.
%% Since current body is stored without revision in key, no rev needed.
-spec get_current_body(db_name(), docid()) ->
    {ok, binary()} | not_found | {error, term()}.
get_current_body(DbName, DocId) ->
    case persistent_term:get({barrel_store, DbName}, undefined) of
        undefined ->
            {error, store_not_found};
        StoreRef ->
            Key = barrel_store_keys:doc_body(DbName, DocId),
            barrel_store_rocksdb:body_get(StoreRef, Key)
    end.

%% @doc Batch get current bodies for multiple documents.
%% Much faster than multi_get_bodies since we don't need to know revisions.
%% Uses short_range read profile by default.
-spec multi_get_current_bodies(db_name(), [docid()]) ->
    [{ok, binary()} | not_found | {error, term()}].
multi_get_current_bodies(DbName, DocIds) ->
    multi_get_current_bodies(DbName, DocIds, short_range).

%% @doc Batch get current bodies with explicit read profile.
%% Read profiles optimize I/O based on batch size:
%% - point: Small batches (&lt;50), cache friendly
%% - short_range: Medium batches (50-200), auto readahead
%% - long_scan: Large batches (&gt;200), prefetch 2MB, avoid cache pollution
-spec multi_get_current_bodies(db_name(), [docid()], read_profile()) ->
    [{ok, binary()} | not_found | {error, term()}].
multi_get_current_bodies(DbName, DocIds, Profile) ->
    case persistent_term:get({barrel_store, DbName}, undefined) of
        undefined ->
            [{error, store_not_found} || _ <- DocIds];
        StoreRef ->
            Keys = [barrel_store_keys:doc_body(DbName, DocId) || DocId <- DocIds],
            barrel_store_rocksdb:body_multi_get(StoreRef, Keys, Profile)
    end.

%% @doc Batch get current bodies with snapshot for consistent reads.
%% Uses short_range read profile by default.
-spec multi_get_current_bodies_with_snapshot(db_name(), [docid()], snapshot()) ->
    [{ok, binary()} | not_found | {error, term()}].
multi_get_current_bodies_with_snapshot(DbName, DocIds, Snapshot) ->
    multi_get_current_bodies_with_snapshot(DbName, DocIds, Snapshot, short_range).

%% @doc Batch get current bodies with snapshot and explicit read profile.
%% Use this for query execution to ensure consistent reads across batches.
-spec multi_get_current_bodies_with_snapshot(db_name(), [docid()], snapshot(), read_profile()) ->
    [{ok, binary()} | not_found | {error, term()}].
multi_get_current_bodies_with_snapshot(DbName, DocIds, Snapshot, Profile) ->
    case persistent_term:get({barrel_store, DbName}, undefined) of
        undefined ->
            [{error, store_not_found} || _ <- DocIds];
        StoreRef ->
            Keys = [barrel_store_keys:doc_body(DbName, DocId) || DocId <- DocIds],
            barrel_store_rocksdb:body_multi_get_with_snapshot(StoreRef, Keys, Snapshot, Profile)
    end.

%%====================================================================
%% API - Specific Revision Body (for archived revisions)
%%====================================================================

%% @doc Get a specific revision body (archived, non-current revisions).
-spec get_body(db_name(), docid(), revid()) ->
    {ok, binary()} | not_found | {error, term()}.
get_body(DbName, DocId, RevId) ->
    case persistent_term:get({barrel_store, DbName}, undefined) of
        undefined ->
            {error, store_not_found};
        StoreRef ->
            Key = barrel_store_keys:doc_body_rev(DbName, DocId, RevId),
            barrel_store_rocksdb:body_get(StoreRef, Key)
    end.

%% @doc Batch get specific revision bodies (for archived revisions).
%% DocIdRevPairs is a list of {DocId, RevId} tuples.
-spec multi_get_bodies(db_name(), [{docid(), revid()}], map()) ->
    [{ok, binary()} | not_found | {error, term()}].
multi_get_bodies(DbName, DocIdRevPairs, _Opts) ->
    case persistent_term:get({barrel_store, DbName}, undefined) of
        undefined ->
            [{error, store_not_found} || _ <- DocIdRevPairs];
        StoreRef ->
            Keys = [barrel_store_keys:doc_body_rev(DbName, DocId, RevId)
                    || {DocId, RevId} <- DocIdRevPairs],
            barrel_store_rocksdb:body_multi_get(StoreRef, Keys)
    end.
