%%%-------------------------------------------------------------------
%%% @doc Read-only RocksDB opens (DB::OpenForReadOnly): nothing in the
%%% store directory is created or rewritten (RocksDB writes no info LOG
%%% read only), a missing store or column family fails.
%%% Same as barrel_rocksdb_ro in barrel_docdb (no shared dependency).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_ro).

-export([open/3]).

%% Options that would create or rewrite files; RocksDB ignores most of
%% them read only, dropped so the intent is explicit.
-define(RW_ONLY, [create_if_missing, create_missing_column_families,
                  error_if_exists]).

%% @doc Open with the same column family descriptors as a writable open.
-spec open(string(), list(), [{string(), list()}]) ->
    {ok, rocksdb:db_handle(), [rocksdb:cf_handle()]} | {error, term()}.
open(Path, DbOpts, CFs) ->
    Opts = ro_opts(DbOpts),
    case check_store(Path) of
        ok -> check_cfs(Path, Opts, CFs);
        {error, _} = Err -> Err
    end.

check_store(Path) ->
    case filelib:is_regular(filename:join(Path, "CURRENT")) of
        true -> ok;
        false -> {error, {read_only_store_missing, Path}}
    end.

check_cfs(Path, Opts, CFs) ->
    case rocksdb:list_column_families(Path, Opts) of
        {ok, Have} ->
            case [Name || {Name, _} <- CFs, not lists:member(Name, Have)] of
                [] ->
                    rocksdb:open_readonly(Path, Opts, CFs);
                Missing ->
                    {error, {read_only_upgrade_needed,
                             #{store => Path, missing_cfs => Missing}}}
            end;
        {error, _} = Err ->
            Err
    end.

ro_opts(DbOpts) ->
    [Opt || Opt <- DbOpts, not rw_only(Opt)].

rw_only({Key, _}) -> lists:member(Key, ?RW_ONLY);
rw_only(_) -> false.
