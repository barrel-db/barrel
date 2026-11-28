%% @doc The derived store of one source database instance: a RocksDB
%% directory under barrel_design's data_dir, opened with the source's
%% storage environment so an encrypted database gets encrypted rows.
-module(barrel_design_store).

-export([root/0, db_dir/1, instance_dir/2, db_name/1]).
-export([open/2, close/1, write/2, get/2, get/3, multi_get/2, fold/6, snapshot/1,
         release/1, delete_range/3, bytes/1]).

-type store() :: #{ref := rocksdb:db_handle(), env := term(), dir := string()}.
-export_type([store/0]).

-spec root() -> string().
root() ->
    filename:absname(application:get_env(barrel_design, data_dir, "data/barrel_design")).

%% Database names are used as directory names when they are plain;
%% anything else is hex-encoded behind a `~', which plain names never use.
-spec db_dir(binary()) -> string().
db_dir(Db) ->
    Plain = re:run(Db, <<"^[A-Za-z0-9_][A-Za-z0-9_.-]*$">>, [{capture, none}]) =:= match,
    Name = case Plain of
        true -> binary_to_list(Db);
        false -> "~" ++ binary_to_list(binary:encode_hex(Db, lowercase))
    end,
    filename:join(root(), Name).

-spec db_name(string()) -> binary().
db_name("~" ++ Hex) -> binary:decode_hex(list_to_binary(Hex));
db_name(Name) -> list_to_binary(Name).

-spec instance_dir(binary(), binary()) -> string().
instance_dir(Db, Instance) ->
    filename:join(db_dir(Db), binary_to_list(Instance)).

-spec open(string(), default | term()) -> {ok, store()} | {error, term()}.
open(Dir, Env) ->
    ok = filelib:ensure_path(Dir),
    Opts0 = [{create_if_missing, true}, {max_open_files, 256}],
    Opts = case Env of
        default -> Opts0;
        _ -> [{env, Env} | Opts0]
    end,
    case rocksdb:open(Dir, Opts) of
        {ok, Ref} -> {ok, #{ref => Ref, env => Env, dir => Dir}};
        {error, _} = Error -> Error
    end.

-spec close(store()) -> ok.
close(#{ref := Ref}) ->
    _ = rocksdb:close(Ref),
    ok.

-spec write(store(), [{put, binary(), binary()} | {delete, binary()}]) -> ok | {error, term()}.
write(_Store, []) -> ok;
write(#{ref := Ref}, Ops) ->
    {ok, Batch} = rocksdb:batch(),
    try
        lists:foreach(fun({put, K, V}) -> ok = rocksdb:batch_put(Batch, K, V);
                         ({delete, K}) -> ok = rocksdb:batch_delete(Batch, K)
                      end, Ops),
        rocksdb:write_batch(Ref, Batch, [{sync, false}])
    after
        rocksdb:release_batch(Batch)
    end.

-spec get(store(), binary()) -> {ok, binary()} | not_found | {error, term()}.
get(#{ref := Ref}, Key) ->
    rocksdb:get(Ref, Key, []).

-spec get(store(), binary(), rocksdb:snapshot_handle()) -> {ok, binary()} | not_found | {error, term()}.
get(#{ref := Ref}, Key, Snapshot) ->
    rocksdb:get(Ref, Key, [{snapshot, Snapshot}]).

-spec multi_get(store(), [binary()]) -> [{ok, binary()} | not_found | {error, term()}].
multi_get(_Store, []) -> [];
multi_get(#{ref := Ref}, Keys) ->
    rocksdb:multi_get(Ref, Keys, []).

-spec snapshot(store()) -> {ok, rocksdb:snapshot_handle()} | {error, term()}.
snapshot(#{ref := Ref}) -> rocksdb:snapshot(Ref).

-spec release(rocksdb:snapshot_handle()) -> ok.
release(Snapshot) ->
    try rocksdb:release_snapshot(Snapshot) catch _:_ -> ok end,
    ok.

%% @doc Fold [Lower, Upper) in key order, optionally under a snapshot.
%% Fun(Key, Value, Acc) -> {ok, Acc} | {stop, Acc}.
-spec fold(store(), binary(), binary(), fun(), term(),
           rocksdb:snapshot_handle() | undefined) -> term().
fold(#{ref := Ref}, Lower, Upper, Fun, Acc0, Snapshot) ->
    Opts0 = [{iterate_lower_bound, Lower}, {iterate_upper_bound, Upper},
             {total_order_seek, true}],
    Opts = case Snapshot of
        undefined -> Opts0;
        _ -> [{snapshot, Snapshot} | Opts0]
    end,
    {ok, It} = rocksdb:iterator(Ref, Opts),
    try loop(rocksdb:iterator_move(It, first), It, Fun, Acc0)
    after rocksdb:iterator_close(It)
    end.

loop({ok, K, V}, It, Fun, Acc) ->
    case Fun(K, V, Acc) of
        {ok, Acc1} -> loop(rocksdb:iterator_move(It, next), It, Fun, Acc1);
        {stop, Acc1} -> Acc1
    end;
loop({error, invalid_iterator}, _It, _Fun, Acc) ->
    Acc;
loop({error, Reason}, _It, _Fun, _Acc) ->
    error({store_iterator, Reason}).

-spec delete_range(store(), binary(), binary()) -> ok | {error, term()}.
delete_range(#{ref := Ref}, Lower, Upper) ->
    rocksdb:delete_range(Ref, Lower, Upper, []).

%% @doc Bytes the store's files take on disk.
-spec bytes(store()) -> non_neg_integer().
bytes(#{dir := Dir}) ->
    filelib:fold_files(Dir, ".*", true,
                       fun(F, Acc) -> Acc + filelib:file_size(F) end, 0).
