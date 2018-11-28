%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 27. Sep 2018 22:03
%%%-------------------------------------------------------------------
-module(barrel_rocksdb).
-author("benoitc").

-export([
  init_store/2
]).

-export([
  start_cache/2,
  stop_cache/1
]).

-export([start_link/2]).

%% gen_server callbaks
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  terminate/2
]).

-include_lib("barrel/include/barrel_logger.hrl").
-include("barrel_rocksdb.hrl").

%% -------------------
%% cache api

start_cache(Name, CacheSize) when is_atom(Name) ->
  Spec =
    #{id => {barrel_rocksdb_cache, Name},
      start => {barrel_rocksdb_cache, start_link, [Name, CacheSize]}},
  barrel_services:start_service(Spec).

stop_cache(Name) ->
  barrel_services:stop_service({barrel_rocksdb_cache, Name}).





init_store(Name, Options) ->
  Spec = #{ start => {?MODULE, start_link, [Name, Options]} },
  barrel_services:activate_service(store, ?MODULE, Name, Spec).

%% -------------------
%% - internals

start_link(Name, Options) ->
  gen_server:start_link({via, gproc, ?rdb_store(Name)}, ?MODULE, [Name, Options], []).


init([Name, Options = #{ path := Path }]) ->
  Retries = application:get_env(barrel, rocksdb_open_retries, ?DB_OPEN_RETRIES),
  DbOptions = barrel_rocksdb_options:db_options(Options),
  case open_db(Path, DbOptions, Retries, false) of
    {ok, Ref} ->
      Store = #{ name => Name, path => Path, ref => Ref },
      _ = gproc:set_value(?rdb_store(Name), Store),
      {ok, Store};
    {error, Error} ->
      exit(Error)
  end.

handle_call(_Msg, _From, Store) -> {reply, ok, Store}.

handle_cast(_Msg, Store) -> {noreply, Store}.

terminate(_Reason, #{ name := Name, ref := Ref }) ->
  _  = barrel_store_providers:unregister_store(Name),
  _ = rocksdb:close(Ref),
  ok.


open_db(_Path, _DbOpts, 0, LastError) ->
  {error, LastError};
open_db(Path, DbOpts,RetriesLeft, _LastError) ->
  case rocksdb:open(Path, DbOpts) of
    OKResult = {ok, _} ->
      OKResult;
    %% Check specifically for lock error, this can be caused if
    %% a crashed instance takes some time to flush leveldb information
    %% out to disk.  The process is gone, but the NIF resource cleanup
    %% may not have completed.
    {error, {db_open, OpenErr}=Reason} ->
      case lists:prefix("IO error: lock ", OpenErr) of
        true ->
          SleepFor = application:get_env(barrel, db_open_retry_delay, ?DB_OPEN_RETRY_DELAY),
          _ = ?LOG_WARNING(
            "~s: barrel rocksdb backend retrying ~p in ~p ms after error ~s\n",
            [?MODULE, Path, SleepFor, OpenErr]
          ),
          timer:sleep(SleepFor),
          open_db(Path, DbOpts, RetriesLeft - 1, Reason);
        false ->
          {error, Reason}
      end;
    {error, _} = Error ->
      Error
  end.