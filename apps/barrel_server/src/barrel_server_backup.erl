%%%-------------------------------------------------------------------
%%% @doc The backup module barrel-backup uses on a barrel_server node
%%% (barrel_docdb's `backup_module'). The server's databases are barrel
%%% databases, so a backup carries their vector store too: it goes
%%% through barrel_backup, with the databases opened the way the server
%%% opens them.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_backup).

-export([backup/3, restore/3, read_manifest/1, local_dbs/0]).

-spec backup(binary(), file:filename_all(), map()) -> {ok, map()} | {error, term()}.
backup(Name, Dir, Opts) ->
    case barrel_server_dbs:ensure(Name, #{}) of
        {ok, Db} -> barrel_backup:backup(Db, Dir, Opts);
        {error, _} = Err -> Err
    end.

%% @doc Restore, then hand the database to the server: it is closed and
%% opened again through barrel_server_dbs, as any request would open it.
-spec restore(file:filename_all(), binary(), map()) -> {ok, binary()} | {error, term()}.
restore(Dir, Name, Opts) ->
    Base = barrel_server_dbs:ensure_opts(),
    DocOpts = maps:merge(maps:get(docdb, Base, #{}),
                         maps:with([retention_period], Opts)),
    Open = Base#{docdb => DocOpts, vectordb => vec_opts(Name, Base)},
    case barrel_backup:restore(Dir, Name, Open) of
        {ok, Db} ->
            ok = barrel:close(Db),
            case barrel_server_dbs:ensure(Name, #{}) of
                {ok, _} -> {ok, Name};
                {error, _} = Err -> Err
            end;
        {error, _} = Err ->
            Err
    end.

-spec read_manifest(file:filename_all()) -> {ok, map()} | {error, term()}.
read_manifest(Dir) ->
    barrel_backup:read_manifest(Dir).

-spec local_dbs() -> [binary()].
local_dbs() ->
    barrel_backup:local_dbs().

%% A space keeps its vector store where barrel_spaces records it (by
%% default `<data_dir>/<space>_vec'); any other database next to its
%% documents.
vec_opts(<<"sp_", _/binary>> = Name, Base) ->
    Vec = case maps:get(vectordb, Base, #{}) of none -> #{}; V -> V end,
    case barrel_spaces:space_info(Name) of
        {ok, #{<<"vectordb">> := <<"none">>}} -> none;
        {ok, #{<<"vec_custom">> := true, <<"vec_path">> := P}} -> Vec#{db_path => binary_to_list(P)};
        _ -> Vec#{db_path => filename:join(data_dir(), binary_to_list(Name) ++ "_vec")}
    end;
vec_opts(_Name, Base) ->
    maps:get(vectordb, Base, #{}).

data_dir() ->
    unicode:characters_to_list(application:get_env(barrel_docdb, data_dir, "/tmp/barrel_data")).
