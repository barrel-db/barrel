%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. Aug 2017 09:42
%%%-------------------------------------------------------------------
-module(barrel_backup).
-author("benoitc").

-include("barrel.hrl").
%% API
-export([
  new_snapshot/2,
  restore_from_snapshot/2
]).

new_snapshot(DbName, Path) ->
  case barrel_store:open_db(DbName) of
    {ok, #db{ store = Store, conf=Config }} ->
      case check_config(Config) of
        ok ->
          case rocksdb:checkpoint(Store, Path) of
            ok ->
              save_config(Path, Config);
            Error ->
              Error
          end
      end;
    Error ->
      Error
  end.


restore_from_snapshot(DbName, Path) ->
  case barrel_store:drop_db(DbName) of
    {ok, undefined} ->
      case do_restore(DbName, Path) of
        {ok, _} ->
          {ok, undefined};
        Error ->
          Error
      end;
    {ok, OldConf} ->
      %% persist old conf
      #{ <<"_path">> := OldPath } = OldConf,
      ok = save_config(OldPath, OldConf),
      case do_restore(DbName, Path) of
        {ok, _} ->
          {ok, OldPath};
        Error ->
          Error
      end;
    Error ->
      Error
  end.

do_restore(DbName, Path) ->
  case check_dbname(DbName) of
    ok ->
      Config = load_config(Path),
      DbPath = db_path(DbName),
      barrel_file_utils:cp_r(
        [barrel_lib:to_list(Path)],
        barrel_lib:to_list(DbPath)
      ),
      barrel_store:load_db(new_config(DbName, DbPath, Config));
    Error ->
      Error
  end.

check_dbname(DbName) ->
  {ok, RegExp} = re:compile("^[a-z][a-z0-9\\_\\$()\\+\\-\\/]*$"),
  case re:run(DbName, RegExp, [{capture, none}]) of
    nomatch ->
      {error, {invalid_database_id, DbName}};
    match ->
      ok
  end.

db_path(DbName) ->
  Path = binary_to_list(
    filename:join(
      db_dir(),
      << DbName/binary, "-", (barrel_lib:uniqid())/binary >>
    )
  ),
  ok = filelib:ensure_dir(Path),
  Path.

db_dir() ->
  Dir = filename:join(barrel_store:data_dir(), "dbs"),
  ok = filelib:ensure_dir([Dir, "dummy"]),
  Dir.




%% ================
%% internals

new_config(DbName, DbPath, OldConf) ->
  Config = maps:without([<<"database_id">>, <<"_path">>], OldConf),
  Config#{
    << "database_id">> => DbName,
    << "_path" >> => barrel_lib:to_binary(DbPath)
  }.

save_config(Path, Config) ->
  ConfPath = filename:join(Path, "BARREL_CONFIG"),

  file:write_file(ConfPath, jsx:encode(Config)).

load_config(Path) ->
  ConfPath = filename:join(Path, "BARREL_CONFIG"),
  io:format("confpaths ~p~n", [ConfPath]),
  {ok, ConfBin} = file:read_file(ConfPath),
  jsx:decode(ConfBin, [return_maps]).

check_config(#{ <<"in_memory">> := true }) ->
  {error, {unsupported, "snapshot is of an ephemral db is unsupported"}};
check_config(_) ->
  ok.