%%%-------------------------------------------------------------------
%%% @doc Caller-side point reads over the document store.
%%%
%%% Point reads (`get_doc', `get_docs') need nothing from the database
%%% server state except the store ref and the database name, so they can
%%% run in the caller's process against a RocksDB snapshot instead of
%%% serializing behind the writer. The snapshot keeps the entity and body
%%% reads consistent with each other while a concurrent write batch
%%% commits.
%%%
%%% Callers resolve the store ref via `persistent_term' (the
%%% `{barrel_store, DbName}' registry published by `barrel_db_server';
%%% see `barrel_doc_body_store' for the same pattern). The server's own
%%% `get_doc'/`get_docs' handlers delegate here too, so there is a single
%%% read implementation.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_docdb_reader).

-include("barrel_docdb.hrl").

-export([get_doc/4, get_docs/4]).

%% @doc Get a document by id, reading entity and body under one snapshot.
%%
%% Same options and results as `barrel_docdb:get_doc/3'. Returns
%% `{error, not_found}' if the store is closed underneath the caller (the
%% close race maps to the same result as a missing database).
-spec get_doc(barrel_store_rocksdb:db_ref(), db_name(), docid(), map()) ->
    {ok, map()} | {ok, binary(), map()} | {error, term()}.
get_doc(StoreRef, DbName, DocId, Opts) ->
    with_snapshot(StoreRef, fun(Snapshot) ->
        do_get_doc(StoreRef, DbName, DocId, Opts, Snapshot)
    end).

%% @doc Get multiple documents by id under one snapshot (batch read).
%%
%% Same options and results as `barrel_docdb:get_docs/3': one result per
%% input id, in order.
-spec get_docs(barrel_store_rocksdb:db_ref(), db_name(), [docid()], map()) ->
    [{ok, map()} | {error, term()}].
get_docs(StoreRef, DbName, DocIds, Opts) ->
    case with_snapshot(StoreRef, fun(Snapshot) ->
        do_get_docs(StoreRef, DbName, DocIds, Opts, Snapshot)
    end) of
        {error, _} = Err -> [Err || _ <- DocIds];
        Results -> Results
    end.

%%====================================================================
%% Internal
%%====================================================================

%% @private Run a read under a snapshot, always releasing it. A closed
%% store raises badarg from the NIF; map it to the missing-database error.
with_snapshot(StoreRef, Fun) ->
    try barrel_store_rocksdb:snapshot(StoreRef) of
        {ok, Snapshot} ->
            try
                Fun(Snapshot)
            after
                barrel_store_rocksdb:safe_release_snapshot(Snapshot)
            end;
        {error, _} = Err ->
            Err
    catch
        error:badarg ->
            {error, not_found}
    end.

%% @private Get a document by id (wide column entity + body CF).
do_get_doc(StoreRef, DbName, DocId, Opts, Snapshot) ->
    DocEntityKey = barrel_store_keys:doc_entity(DbName, DocId),
    case barrel_store_rocksdb:get_entity_with_snapshot(StoreRef, DocEntityKey, Snapshot) of
        {ok, Columns} ->
            %% The winner is stored at write time; its token is the API rev
            Rev = barrel_version:to_token(
                barrel_version:decode(
                    proplists:get_value(?COL_VERSION, Columns))),
            Deleted = bin_to_deleted(proplists:get_value(?COL_DELETED, Columns, <<"false">>)),
            IncludeDeleted = maps:get(include_deleted, Opts, false),

            case {Deleted, IncludeDeleted} of
                {true, false} ->
                    {error, not_found};
                _ ->
                    %% Get document body from body CF (current body, no rev in key)
                    BodyKey = barrel_store_keys:doc_body(DbName, DocId),
                    case barrel_store_rocksdb:body_get_with_snapshot(StoreRef, BodyKey, Snapshot) of
                        {ok, CborBin} ->
                            %% Check if raw body requested (for zero-copy CBOR responses)
                            case maps:get(raw_body, Opts, false) of
                                true ->
                                    %% Return raw CBOR body with metadata for zero-copy
                                    Meta = #{
                                        id => DocId,
                                        rev => Rev,
                                        deleted => Deleted
                                    },
                                    Meta2 = maybe_add_conflicts_to_meta(
                                        Meta, Columns, Opts,
                                        {StoreRef, DbName, DocId, Snapshot}),
                                    {ok, CborBin, Meta2};
                                false ->
                                    %% Decode CBOR to get document body
                                    DocBody = barrel_docdb_codec_cbor:decode_any(CborBin),
                                    %% Add metadata
                                    Result = DocBody#{
                                        <<"id">> => DocId,
                                        <<"_rev">> => Rev
                                    },
                                    Result2 = case Deleted of
                                        true -> Result#{<<"_deleted">> => true};
                                        false -> Result
                                    end,
                                    %% Add conflicts if requested
                                    Result3 = maybe_add_conflicts(
                                        Result2, Columns, Opts,
                                        {StoreRef, DbName, DocId, Snapshot}),
                                    %% Attach the embedding column if requested
                                    Result4 = maybe_add_embedding(Result3, Columns, Opts),
                                    {ok, Result4}
                            end;
                        not_found ->
                            {error, not_found}
                    end
            end;
        not_found ->
            {error, not_found}
    end.

%% @private Get multiple documents by id (batch entity + body fetch).
do_get_docs(StoreRef, DbName, DocIds, Opts, Snapshot) ->
    IncludeDeleted = maps:get(include_deleted, Opts, false),

    %% Build keys for both entity and body (no rev needed for body)
    DocEntityKeys = [barrel_store_keys:doc_entity(DbName, DocId) || DocId <- DocIds],
    BodyKeys = [barrel_store_keys:doc_body(DbName, DocId) || DocId <- DocIds],

    %% Batch fetch entities and bodies under the same snapshot
    DocEntityResults = barrel_store_rocksdb:multi_get_entity_with_snapshot(
        StoreRef, DocEntityKeys, Snapshot),
    DocBodyResults = barrel_store_rocksdb:body_multi_get_with_snapshot(
        StoreRef, BodyKeys, Snapshot),

    %% Combine results
    DocBodyMap = lists:foldl(
        fun({DocId, EntityResult, BodyResult}, Map) ->
            case {EntityResult, BodyResult} of
                {{ok, Columns}, {ok, CborBin}} ->
                    Rev = barrel_version:to_token(
                        barrel_version:decode(
                            proplists:get_value(?COL_VERSION, Columns))),
                    Deleted = bin_to_deleted(proplists:get_value(?COL_DELETED, Columns, <<"false">>)),
                    case {Deleted, IncludeDeleted} of
                        {true, false} ->
                            Map;  %% Skip deleted
                        _ ->
                            DocBody = barrel_docdb_codec_cbor:decode_any(CborBin),
                            Result = DocBody#{<<"id">> => DocId, <<"_rev">> => Rev},
                            Result2 = case Deleted of
                                true -> Result#{<<"_deleted">> => true};
                                false -> Result
                            end,
                            Result3 = maybe_add_embedding(Result2, Columns, Opts),
                            Map#{DocId => {ok, Result3}}
                    end;
                _ -> Map
            end
        end,
        #{},
        lists:zip3(DocIds, DocEntityResults, DocBodyResults)
    ),
    %% Return results in original order
    [maps:get(DocId, DocBodyMap, {error, not_found}) || DocId <- DocIds].

%% @private Attach the embedding column as the <<"_embedding">> object
%% (vector + source) when include_embedding => true. Off by default, and
%% the object carries its provenance, so read-modify-write loops that
%% resend it round-trip a computed vector as computed, never as a client
%% override.
maybe_add_embedding(Doc, Columns, Opts) ->
    case maps:get(include_embedding, Opts, false) of
        true ->
            case proplists:get_value(?COL_EMBEDDING, Columns) of
                undefined -> Doc;
                <<>> -> Doc;
                Bin ->
                    Src = proplists:get_value(?COL_EMBEDDING_SRC, Columns,
                                              ?EMBEDDING_SRC_CLIENT),
                    Doc#{<<"_embedding">> => #{
                        <<"vector">> => barrel_doc:decode_embedding(Bin),
                        <<"source">> => Src
                    }}
            end;
        false ->
            Doc
    end.

%% @private Convert binary back to boolean from wide column storage
-spec bin_to_deleted(binary()) -> boolean().
bin_to_deleted(<<"true">>) -> true;
bin_to_deleted(<<"false">>) -> false;
bin_to_deleted(_) -> false.

%% @private Add conflicts to document if requested and conflicts exist.
%% The entity carries the live sibling count; only conflicted documents
%% scan the version chain (under the read snapshot).
maybe_add_conflicts(Doc, Columns, Opts, ReadCtx) ->
    case maps:get(conflicts, Opts, false) of
        true ->
            case conflict_tokens(Columns, ReadCtx) of
                [] -> Doc;
                Tokens -> Doc#{<<"_conflicts">> => Tokens}
            end;
        false ->
            Doc
    end.

%% @private Add conflicts to metadata map (for raw_body responses)
maybe_add_conflicts_to_meta(Meta, Columns, Opts, ReadCtx) ->
    case maps:get(conflicts, Opts, false) of
        true ->
            case conflict_tokens(Columns, ReadCtx) of
                [] -> Meta;
                Tokens -> Meta#{conflicts => Tokens}
            end;
        false ->
            Meta
    end.

conflict_tokens(Columns, {StoreRef, DbName, DocId, Snapshot}) ->
    case proplists:get_value(?COL_NCONFLICTS, Columns, 0) of
        0 ->
            [];
        _ ->
            Start = barrel_store_keys:doc_version_prefix(DbName, DocId),
            End = barrel_store_keys:doc_version_end(DbName, DocId),
            Fold = fun(Key, Value, Acc) ->
                case Value of
                    <<1:8, _/binary>> -> %% conflict flag
                        VersionEnc = barrel_store_keys:decode_doc_version_key(
                                         DbName, DocId, Key),
                        {ok, [barrel_version:to_token(
                                  barrel_version:decode(VersionEnc)) | Acc]};
                    _ ->
                        {ok, Acc}
                end
            end,
            lists:reverse(
                barrel_store_rocksdb:fold_range_with_snapshot(
                    StoreRef, Start, End, Fold, [], Snapshot))
    end.
