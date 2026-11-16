%%%-------------------------------------------------------------------
%%% @doc Snapshot of a running store (barrel_vectordb:backup/2). The
%%% store's server runs it between two requests, so nothing writes
%%% meanwhile: a RocksDB checkpoint of the store, the CRYPTO marker, and
%%% the disk indexes (flat files after a sync, their id RocksDB as a
%%% checkpoint). The copy is built next to the target and renamed into
%%% place once complete. It opens as a store of its own, at its new path.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_backup).

-export([run/3, copy_index_dir/4]).

%% @doc Snapshot into Dir. Parts: `db' (the store's RocksDB), `db_path',
%% and `indexes', `[{SubDir, fun((Target) -> ok | {error, _})}]' for the
%% disk indexes.
-spec run(string(), rocksdb:db_handle(), map()) -> ok | {error, term()}.
run(Dir, Db, #{db_path := DbPath, indexes := Indexes}) ->
    case filelib:is_file(Dir) of
        true ->
            {error, {backup_dir_exists, unicode:characters_to_binary(Dir)}};
        false ->
            Tmp = Dir ++ ".partial-" ++ integer_to_list(erlang:unique_integer([positive])),
            ok = filelib:ensure_dir(Tmp),
            Steps = [fun() -> rocksdb:checkpoint(Db, Tmp) end,
                     fun() -> copy_marker(DbPath, Tmp) end
                     | [fun() -> Backup(filename:join(Tmp, Sub)) end
                        || {Sub, Backup} <- Indexes]]
                ++ [fun() -> file:rename(Tmp, Dir) end],
            case steps(Steps) of
                ok ->
                    ok;
                {error, _} = Err ->
                    _ = file:del_dir_r(Tmp),
                    Err
            end
    end.

steps([]) -> ok;
steps([Step | Rest]) ->
    case Step() of
        ok -> steps(Rest);
        {error, _} = Err -> Err
    end.

copy_marker(DbPath, Tmp) ->
    Marker = filename:join(DbPath, "CRYPTO"),
    case filelib:is_regular(Marker) of
        true -> copy(Marker, filename:join(Tmp, "CRYPTO"));
        false -> ok
    end.

%% @doc Copy a disk index directory into Dst (absent): its RocksDB
%% `IdDbName' as a checkpoint of IdDb, everything else as files. A
%% missing Src (an index never written) copies nothing.
-spec copy_index_dir(file:filename_all(), string(), string(),
                     rocksdb:db_handle() | undefined) -> ok | {error, term()}.
copy_index_dir(Src0, Dst, IdDbName, IdDb) ->
    Src = unicode:characters_to_list(Src0),
    case file:list_dir(Src) of
        {ok, Names} ->
            ok = filelib:ensure_path(Dst),
            steps([fun() -> copy_entry(Src, Dst, Name, IdDbName, IdDb) end
                   || Name <- Names]);
        {error, enoent} ->
            ok;
        {error, _} = Err ->
            Err
    end.

copy_entry(_Src, Dst, IdDbName, IdDbName, IdDb) when IdDb =/= undefined ->
    rocksdb:checkpoint(IdDb, filename:join(Dst, IdDbName));
copy_entry(Src, Dst, Name, _IdDbName, _IdDb) ->
    From = filename:join(Src, Name),
    case filelib:is_dir(From) of
        true -> copy_tree(From, filename:join(Dst, Name));
        false -> copy(From, filename:join(Dst, Name))
    end.

copy_tree(From, To) ->
    {ok, Names} = file:list_dir(From),
    ok = filelib:ensure_path(To),
    steps([fun() ->
               F = filename:join(From, N),
               case filelib:is_dir(F) of
                   true -> copy_tree(F, filename:join(To, N));
                   false -> copy(F, filename:join(To, N))
               end
           end || N <- Names]).

copy(From, To) ->
    case file:copy(From, To) of
        {ok, _} -> ok;
        {error, _} = Err -> Err
    end.
