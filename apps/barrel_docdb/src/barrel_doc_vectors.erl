%%%-------------------------------------------------------------------
%%% @doc Client-supplied vectors of a document's versions. The current
%%% version's vector lives in the entity; a version that stops being
%%% current keeps its vector next to its archived body, written and
%%% deleted with it. Computed vectors are derived data and never kept.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_doc_vectors).

-include("barrel_docdb.hrl").

-export([put_op/4, delete_op/3, archive_ops/3, versions/5, get/4,
         client_vector/1, is_vector/1]).

%% @doc Store the vector of a non-current version.
-spec put_op(binary(), binary(), term(), [number()]) -> tuple().
put_op(Ks, DocId, Version, Vec) ->
    {body_put, key(Ks, DocId, Version), barrel_doc:encode_embedding(Vec)}.

%% @doc Delete the vector of a version (encoded), with its archived body.
-spec delete_op(binary(), binary(), binary()) -> tuple().
delete_op(Ks, DocId, VEnc) when is_binary(VEnc) ->
    {body_delete, barrel_store_keys:doc_vector_rev(Ks, DocId, VEnc)}.

%% @doc Keep the client vector of the outgoing current version (a read
%% state from barrel_db_server:read_current/3).
-spec archive_ops(binary(), binary(), map() | undefined) -> [tuple()].
archive_ops(Ks, DocId, #{version := V, deleted := false,
                         embedding := {Bin, ?EMBEDDING_SRC_CLIENT}}) ->
    [{body_put, key(Ks, DocId, V), Bin}];
archive_ops(_Ks, _DocId, _Old) ->
    [].

%% @doc The client vectors of the given versions: the current one from
%% the read state, the others from the archive. #{Version => Vector}.
-spec versions(term(), binary(), binary(), map() | undefined, [term()]) -> map().
versions(StoreRef, Ks, DocId, Old, Versions) ->
    {Current, Others} = current(Old, Versions),
    Archived = case Others of
        [] -> [];
        _ -> lists:zip(Others, barrel_store_rocksdb:body_multi_get(
                                 StoreRef, [key(Ks, DocId, V) || V <- Others],
                                 point))
    end,
    maps:merge(Current, maps:from_list(
                          [{V, barrel_doc:decode_embedding(Bin)}
                           || {V, {ok, Bin}} <- Archived])).

current(#{version := V, deleted := false,
          embedding := {Bin, ?EMBEDDING_SRC_CLIENT}}, Versions) ->
    case lists:member(V, Versions) of
        true -> {#{V => barrel_doc:decode_embedding(Bin)}, Versions -- [V]};
        false -> {#{}, Versions}
    end;
current(#{version := V}, Versions) ->
    {#{}, Versions -- [V]};
current(undefined, Versions) ->
    {#{}, Versions}.

%% @doc The archived vector of one version.
-spec get(term(), binary(), binary(), term()) -> {ok, [float()]} | not_found.
get(StoreRef, Ks, DocId, Version) ->
    case barrel_store_rocksdb:body_get(StoreRef, key(Ks, DocId, Version)) of
        {ok, Bin} -> {ok, barrel_doc:decode_embedding(Bin)};
        _ -> not_found
    end.

%% @doc A client vector carried in a document's `_embedding': a bare
%% vector, or an object whose source is the client.
-spec client_vector(map()) -> [number()] | undefined.
client_vector(#{<<"_embedding">> := Vec}) when is_list(Vec) ->
    valid(Vec);
client_vector(#{<<"_embedding">> := #{<<"vector">> := Vec} = Obj}) ->
    case maps:get(<<"source">>, Obj, ?EMBEDDING_SRC_CLIENT) of
        ?EMBEDDING_SRC_CLIENT -> valid(Vec);
        _ -> undefined
    end;
client_vector(_Doc) ->
    undefined.

-spec is_vector(term()) -> boolean().
is_vector(Vec) ->
    is_list(Vec) andalso Vec =/= [] andalso lists:all(fun is_number/1, Vec).

valid(Vec) ->
    case is_vector(Vec) of
        true -> Vec;
        false -> undefined
    end.

key(Ks, DocId, VEnc) when is_binary(VEnc) ->
    barrel_store_keys:doc_vector_rev(Ks, DocId, VEnc);
key(Ks, DocId, Version) ->
    barrel_store_keys:doc_vector_rev(Ks, DocId, barrel_version:encode(Version)).
