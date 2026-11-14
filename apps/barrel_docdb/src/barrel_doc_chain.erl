%%%-------------------------------------------------------------------
%%% @doc The per-document version chain: scans, and the move from the
%%% 3.0 key layout.
%%%
%%% Up to 3.0 a chain key was `DbName + DocId + ":" + version'. A doc id
%%% may contain `:', so the scan of `a' also returned the rows of `a:b'
%%% (its conflicts, its history, and the rows a stub or a forget of `a'
%%% deleted). 3.1 keys carry the doc id length (barrel_store_keys), and
%%% a writable open moves the old rows once. A row belongs to the doc
%%% id whose split of the key decodes to a version its own vector
%%% names (its author at or above its own clock); a row no vector
%%% settles, from a version converted from storage before 2.0, goes to
%%% the longest doc id of the split that has a document. A read-only
%%% store opened before the move reads the old rows through the vector
%%% check. After the move the database repairs the versions 3.0 read
%%% from other documents (barrel_db_server), then finish/2 records the
%%% layout.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_doc_chain).

-export([open/3, finish/2, close/1, fold/5, fold_snapshot/6]).

%% exported for tests
-export([owner/2, owner/3]).

-define(META, <<"doc_chain">>).
-define(LAYOUT, <<"2">>).
-define(BATCH, 1000).

%% @doc Move the rows of the old layout (writable store: `moved', then
%% repair and finish/2), or mark the store as read through them
%% (read-only store that still holds them).
-spec open(boolean(), barrel_store_rocksdb:db_ref(), binary()) -> ok | moved.
open(ReadOnly, StoreRef, Ks) ->
    MetaKey = barrel_store_keys:db_meta(Ks, ?META),
    case barrel_store_rocksdb:get(StoreRef, MetaKey) of
        {ok, ?LAYOUT} ->
            ok;
        _ when ReadOnly ->
            case has_legacy(StoreRef, Ks) of
                true -> persistent_term:put({?MODULE, StoreRef}, legacy);
                false -> ok
            end;
        _ ->
            N = migrate(StoreRef, Ks),
            N > 0 andalso logger:notice("barrel_docdb ~ts: moved ~b version "
                                        "chain rows to the 3.1 layout", [Ks, N]),
            moved
    end.

%% @doc Record the 3.1 layout once the move and the repair are done: a
%% crash before it runs both again.
-spec finish(barrel_store_rocksdb:db_ref(), binary()) -> ok.
finish(StoreRef, Ks) ->
    barrel_store_rocksdb:write_batch(
      StoreRef, [{put, barrel_store_keys:db_meta(Ks, ?META), ?LAYOUT}],
      #{sync => true}).

-spec close(barrel_store_rocksdb:db_ref()) -> ok.
close(StoreRef) ->
    _ = persistent_term:erase({?MODULE, StoreRef}),
    ok.

%% @doc Fold the chain rows of one document in version order:
%% `Fun(Key, VersionEnc, Value, Acc) -> {ok, Acc}'.
-spec fold(barrel_store_rocksdb:db_ref(), binary(), binary(), fun(), term()) ->
    term().
fold(StoreRef, Ks, DocId, Fun, Acc) ->
    {Start, End, Own} = range(StoreRef, Ks, DocId),
    barrel_store_rocksdb:fold_range(StoreRef, Start, End, wrap(Own, Fun), Acc).

%% @doc fold/5 under a read snapshot.
-spec fold_snapshot(barrel_store_rocksdb:db_ref(), binary(), binary(), fun(),
                    term(), term()) -> term().
fold_snapshot(StoreRef, Ks, DocId, Fun, Acc, Snapshot) ->
    {Start, End, Own} = range(StoreRef, Ks, DocId),
    barrel_store_rocksdb:fold_range_with_snapshot(StoreRef, Start, End,
                                                  wrap(Own, Fun), Acc, Snapshot).

%% @doc Split an old-layout key tail (`DocId:VersionEnc') by its row
%% value: `{ok, DocId, VersionEnc}' for the one split whose version is
%% named by the row's own vector, else `error'.
-spec owner(binary(), binary()) -> {ok, binary(), binary()} | error.
owner(Tail, Value) ->
    owner(Tail, Value, fun(_DocId) -> false end).

%% @doc owner/2, falling back, when no split's version is named by the
%% vector, to the longest doc id `Exists' confirms. A version encoding
%% starts with its clock, never with printable text and a `:', so a
%% shorter doc id never owns the row of a longer one that exists.
-spec owner(binary(), binary(), fun((binary()) -> boolean())) ->
    {ok, binary(), binary()} | error.
owner(Tail, Value, Exists) ->
    VV = row_vv(Value),
    Splits = [{binary:part(Tail, 0, P),
               binary:part(Tail, P + 1, byte_size(Tail) - P - 1)}
              || {P, 1} <- binary:matches(Tail, <<":">>)],
    case [S || {_DocId, VEnc} = S <- Splits, own_version(VEnc, VV)] of
        [{DocId, VEnc} | _] ->
            {ok, DocId, VEnc};
        [] ->
            case [S || {DocId, VEnc} = S <- lists:reverse(Splits),
                       byte_size(VEnc) > 12, Exists(DocId)] of
                [{DocId, VEnc} | _] -> {ok, DocId, VEnc};
                [] -> error
            end
    end.

%%====================================================================
%% Internal
%%====================================================================

range(StoreRef, Ks, DocId) ->
    case persistent_term:get({?MODULE, StoreRef}, current) of
        current ->
            {barrel_store_keys:doc_version_prefix(Ks, DocId),
             barrel_store_keys:doc_version_end(Ks, DocId),
             {current, Ks, DocId}};
        legacy ->
            Prefix = barrel_store_keys:legacy_doc_version_prefix(Ks, DocId),
            {Prefix, successor(Prefix), {legacy, Prefix}}
    end.

wrap({current, Ks, DocId}, Fun) ->
    fun(Key, Value, Acc) ->
        Fun(Key, barrel_store_keys:decode_doc_version_key(Ks, DocId, Key),
            Value, Acc)
    end;
wrap({legacy, Prefix}, Fun) ->
    Skip = byte_size(Prefix),
    fun(Key, Value, Acc) ->
        <<_:Skip/binary, VEnc/binary>> = Key,
        case own_version(VEnc, row_vv(Value)) of
            true -> Fun(Key, VEnc, Value, Acc);
            false -> {ok, Acc}
        end
    end.

own_version(VEnc, VV) when byte_size(VEnc) > 12 ->
    try barrel_live:names_itself(barrel_version:decode(VEnc), VV)
    catch
        _:_ -> false
    end;
own_version(_VEnc, _VV) ->
    false.

row_vv(<<_Flag:8, _Deleted:8, VVBin/binary>>) ->
    try barrel_vv:decode(VVBin) catch _:_ -> #{} end;
row_vv(_) ->
    #{}.

has_legacy(StoreRef, Ks) ->
    Prefix = barrel_store_keys:legacy_doc_version_db_prefix(Ks),
    barrel_store_rocksdb:fold_range(StoreRef, Prefix, successor(Prefix),
                                    fun(_K, _V, _Acc) -> {stop, true} end,
                                    false).

%% Move every old row of the keyspace, in batches. A row no split
%% claims is kept where it is and reported.
migrate(StoreRef, Ks) ->
    Exists = fun(DocId) ->
                     barrel_store_rocksdb:get_entity(
                       StoreRef, barrel_store_keys:doc_entity(Ks, DocId)) =/= not_found
             end,
    Prefix = barrel_store_keys:legacy_doc_version_db_prefix(Ks),
    Skip = byte_size(Prefix),
    {Ops, N} = barrel_store_rocksdb:fold_range(
        StoreRef, Prefix, successor(Prefix),
        fun(Key, Value, {Batch, Count}) ->
            <<_:Skip/binary, Tail/binary>> = Key,
            case owner(Tail, Value, Exists) of
                {ok, DocId, VEnc} ->
                    New = barrel_store_keys:doc_version(Ks, DocId, VEnc),
                    Batch1 = [{delete, Key}, {put, New, Value} | Batch],
                    {ok, {flush(StoreRef, Batch1), Count + 1}};
                error ->
                    logger:warning("barrel_docdb ~ts: version chain row ~p "
                                   "names no version of its own; kept",
                                   [Ks, Key]),
                    {ok, {Batch, Count}}
            end
        end, {[], 0}),
    ok = write(StoreRef, Ops),
    N.

flush(StoreRef, Batch) when length(Batch) >= 2 * ?BATCH ->
    ok = write(StoreRef, Batch),
    [];
flush(_StoreRef, Batch) ->
    Batch.

write(_StoreRef, []) ->
    ok;
write(StoreRef, Ops) ->
    barrel_store_rocksdb:write_batch(StoreRef, lists:reverse(Ops),
                                     #{sync => true}).

successor(Prefix) ->
    barrel_store_keys:prefix_successor(Prefix).
