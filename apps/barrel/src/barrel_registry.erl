%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 08. Dec 2018 13:49
%%%-------------------------------------------------------------------
-module(barrel_registry).
-author("benoitc").

%% API
-export([init/1, deinit/0]).

-export([
  store_config/2,
  delete_config/1,
  config_of/1,
  exists/1,
  reference_of/1,
  where_is/1
]).

-export([
  register_name/2,
  unregister_name/1,
  whereis_name/1,
  send/2
]).

-export([with_locked_barrel/2]).
-export([local_id/1]).


-include_lib("barrel/include/barrel.hrl").

-define(REVERSE_TBL, barrel_directory_reverse).

-spec init(file:filename()) -> ok.
init(Dir) ->
  Dets = filename:join(Dir, "barrels.dets"),
  ok = filelib:ensure_dir(Dets),
  {ok, ?REVERSE_TBL} = dets:open_file(?REVERSE_TBL,
    [{file, Dets},
      {auto_save, 500},
      {access, read_write}]),
  ok.


-spec deinit() -> ok.
deinit() ->
  _ = dets:close(?REVERSE_TBL),
  ok.

-spec register_name(barrel_name(), pid()) -> yes | no.
register_name(BarrelName, Pid) ->
  gproc:register_name(?barrel(BarrelName), Pid).

-spec unregister_name(barrel_name()) -> any().
unregister_name(BarrelName) ->
  gproc:unregister_name(?barrel(BarrelName)).

-spec whereis_name(barrel_name()) -> pid | undefined.
whereis_name(BarrelName) ->
  gproc:whereis_name(?barrel(BarrelName)).

-spec send(barrel_name(),any()) -> any().
send(BarrelName, Msg) ->
  gproc:send(?barrel(BarrelName), Msg).


where_is(BarrelName) ->
  whereis_name(BarrelName).

-spec store_config(barrel_name(), barrel_config()) -> ok.
store_config(BarrelName, Config) ->
  dets:insert(?REVERSE_TBL, {BarrelName, Config}).


-spec delete_config(barrel_name()) -> ok.
delete_config(BarrelName) ->
  dets:delete(?REVERSE_TBL, BarrelName).

-spec config_of(barrel_name()) -> {ok, barrel_config()} | error.
config_of(BarrelName) ->
  case dets:lookup(?REVERSE_TBL, BarrelName) of
    [] -> error;
    [{_, Config}] -> {ok, Config}
  end.

-spec exists(barrel_name()) -> true | false.
exists(BarrelName) ->
  (config_of(BarrelName) =/= error).

-spec reference_of(barrel_name()) -> {ok, map()} | error.
reference_of(Name) ->
  case gproc:lookup_values(?barrel(Name)) of
    [{_Pid, Barrel}] ->
      {ok, Barrel};
    [] ->
      error
  end.


%% TODO: replace with our own internal locking system?
-spec with_locked_barrel(barrel_name(), fun()) -> any().
with_locked_barrel(BarrelName, Fun) ->
  LockId = {{barrel, BarrelName}, self()},
  global:trans(LockId, Fun).


local_id(Name) ->
  Prefix = barrel_lib:derive_safe_string(Name, 8),
  barrel_lib:make_uid(Prefix).