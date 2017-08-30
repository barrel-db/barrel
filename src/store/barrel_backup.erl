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
      Config = load_config(Path),
      {ok, _Db} = barrel_store:create_db(Config),
      {ok, undefined};
    {ok, OldConf} ->
      %% persist old conf
      #{ <<"_path">> := OldPath } = OldConf,
      ok = save_config(OldPath, OldConf),
      Config = load_config(Path),
      {ok, _Db} = barrel_store:create_db(Config),
      {ok, OldPath};
    Error ->
      Error
  end.


%% ================
%% internals


save_config(Path, Config) ->
  ConfPath = filename:join(Path, "BARREL_CONFIG"),
  file:write_file(ConfPath, jsx:encode(Config)).

load_config(Path) ->
  ConfPath = filename:join(Path, "BARREL_CONFIG"),
  {ok, ConfBin} = file:read_file(ConfPath),
  jsx:decode(ConfBin, [return_maps]).

check_config(#{ <<"in_memory">> := true }) ->
  {error, {unsupported, "snapshot is of an ephemral db is unsupported"}};
check_config(_) ->
  ok.

