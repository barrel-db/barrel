%%%-------------------------------------------------------------------
%%% @doc Usage statistics for barrel_docdb
%%%
%%% Provides usage statistics collected from RocksDB properties.
%%% This is a thin wrapper around existing RocksDB property access
%%% with no runtime state.
%%%
%%% == Example Usage ==
%%% ```
%%% %% Get usage stats for a single database
%%% {ok, Stats} = barrel_docdb_usage:get_db_usage(<<"mydb">>).
%%% #{
%%%     database := <<"mydb">>,
%%%     document_count := 15420,
%%%     storage_bytes := 104857600,
%%%     memtable_size := 2097152,
%%%     sst_files_size := 100663296,
%%%     last_updated := 1704067200000
%%% } = Stats.
%%%
%%% %% Get usage stats for all databases
%%% {ok, AllStats} = barrel_docdb_usage:get_all_usage().
%%% '''
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_docdb_usage).

%% API
-export([
    get_db_usage/1,
    get_all_usage/0
]).

%%====================================================================
%% API
%%====================================================================

%% @doc Get usage statistics for a single database.
%% Returns a map with storage metrics from RocksDB.
-spec get_db_usage(binary()) -> {ok, map()} | {error, term()}.
get_db_usage(DbName) when is_binary(DbName) ->
    case barrel_docdb:db_pid(DbName) of
        {ok, Pid} ->
            case barrel_db_server:get_store_ref(Pid) of
                {ok, DbRef} ->
                    collect_stats(DbName, DbRef);
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Get usage statistics for all databases.
%% Returns a list of stats maps, one for each database.
-spec get_all_usage() -> {ok, [map()]}.
get_all_usage() ->
    DbNames = barrel_docdb:list_dbs(),
    Stats = lists:filtermap(
        fun(DbName) ->
            case get_db_usage(DbName) of
                {ok, S} -> {true, S};
                {error, _} -> false
            end
        end,
        DbNames
    ),
    {ok, Stats}.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private
%% Collect stats from RocksDB properties.
collect_stats(DbName, DbRef) ->
    %% Get estimated document count
    DocCount = get_estimated_keys(DbRef),

    %% Get storage size
    StorageBytes = get_storage_size(DbRef),

    %% Get memtable size
    MemtableSize = get_memtable_size(DbRef),

    %% Get SST files size
    SstFilesSize = get_sst_files_size(DbRef),

    Stats = #{
        database => DbName,
        document_count => DocCount,
        storage_bytes => StorageBytes,
        memtable_size => MemtableSize,
        sst_files_size => SstFilesSize,
        last_updated => erlang:system_time(millisecond)
    },
    {ok, Stats}.

%% @private
%% Get estimated number of keys in the database.
get_estimated_keys(DbRef) ->
    case barrel_store_rocksdb:get_property(DbRef, <<"rocksdb.estimate-num-keys">>) of
        {ok, Value} -> safe_to_integer(Value);
        {error, _} -> 0
    end.

%% @private
%% Get total storage size (using get_db_size which tries live data first).
get_storage_size(DbRef) ->
    case barrel_store_rocksdb:get_db_size(DbRef) of
        {ok, Size} -> Size;
        {error, _} -> 0
    end.

%% @private
%% Get current memtable size.
get_memtable_size(DbRef) ->
    case barrel_store_rocksdb:get_property(DbRef, <<"rocksdb.cur-size-all-mem-tables">>) of
        {ok, Value} -> safe_to_integer(Value);
        {error, _} -> 0
    end.

%% @private
%% Get total SST files size.
get_sst_files_size(DbRef) ->
    case barrel_store_rocksdb:get_property(DbRef, <<"rocksdb.total-sst-files-size">>) of
        {ok, Value} -> safe_to_integer(Value);
        {error, _} -> 0
    end.

%% @private
%% Safely convert binary to integer.
safe_to_integer(Bin) when is_binary(Bin) ->
    barrel_lib:safe(fun() -> binary_to_integer(Bin) end, 0).
