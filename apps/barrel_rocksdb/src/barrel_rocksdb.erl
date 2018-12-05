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
  init_store/2,
  create_barrel/2,
  open_barrel/2,
  delete_barrel/2,
  barrel_infos/2
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
-include("barrel_rocksdb_keys.hrl").

-define(IDENT_TAB, barrel_rocksdb_idents).

%% -------------------
%% store api

init_store(Name, Options) ->
  Spec = #{ start => {?MODULE, start_link, [Name, Options]} },
  barrel_services:activate_service(store, ?MODULE, Name, Spec).


create_barrel(StoreName, Name) ->
  #{ ref := Ref } = Store = gproc:lookup_value(?rdb_store(StoreName)),
  BarrelKey = barrel_rocksdb_keys:local_barrel_ident(Name),
  case find_ident(Name, Store) of
    {ok, _Ident} ->
      ?LOG_INFO("db already exists store=~p name=~p ident=~p~n", [StoreName, Name, _Ident]),
      {error, db_already_exists};
    error ->
      NewIdent = new_ident(Store),
      BarrelId = barrel_encoding:encode_nonsorting_uvarint(<<>>, NewIdent),
      case rocksdb:put(Ref, BarrelKey, BarrelId, [{sync, true}]) of
        ok ->
          _ = create_ident(Name, BarrelId, Store),
          {ok,
            #{name => Name,
              provider => {?MODULE, StoreName},
              id => BarrelId,
              last_seq => 0,
              purge_seq => 0}};
        Error ->
          Error
      end
  end.

open_barrel(StoreName, Name) ->
  #{ ref := Ref } = Store = gproc:lookup_value(?rdb_store(StoreName)),
  case find_ident(Name, Store) of
    {ok, BarrelId} ->
      {ok, Itr} = rocksdb:iterator(Ref, [{iterate_lower_bound, barrel_rocksdb_keys:doc_seq_prefix(BarrelId)}]),
      LastSeq = case rocksdb:iterator_move(Itr, {seek_for_prev, barrel_rocksdb_keys:doc_seq_max(BarrelId)}) of
                  {ok, SeqKey, _} ->
                    barrel_rocksdb_keys:decode_doc_seq(SeqKey);
                  _ -> 0
                end,
      _ = rocksdb:iterator_close(Itr),
      PurgeSeq = case rocksdb:get(Ref, barrel_rocksdb_keys:purge_seq(BarrelId), []) of
                   {ok, PurgeSeqBin} -> binary_to_term(PurgeSeqBin);
                   not_found -> 0
                 end,
      
      {ok,
        #{name => Name,
          provider => {?MODULE, StoreName},
          id => BarrelId,
          last_seq => LastSeq,
          purge_seq => PurgeSeq}};
    error ->
      {error, db_not_found}
  end.


delete_barrel(StoreName, Name) ->
  #{ ref := Ref } = Store = gproc:lookup_value(?rdb_store(StoreName)),
  BarrelKey = barrel_rocksdb_keys:local_barrel_ident(Name),
  case rocksdb:get(Ref, BarrelKey, []) of
    {ok, BarrelId} ->
      %% first delete atomically all barrel metadata
      {ok, Batch} = rocksdb:batch(),
      rocksdb:batch_delete(Batch, BarrelKey),
      rocksdb:batch_delete(Batch, barrel_rocksdb_keys:docs_count(BarrelId)),
      rocksdb:batch_delete(Batch, barrel_rocksdb_keys:docs_del_count(BarrelId)),
      rocksdb:batch_delete(Batch, barrel_rocksdb_keys:purge_seq(BarrelId)),
      ok = rocksdb:write_batch(Ref, Batch, []),
      _ = delete_ident(Name, Store),
      %% delete barrel data
      rocksdb:delete_range(
        Ref, barrel_rocksdb_keys:db_prefix(BarrelId), barrel_rocksdb_keys:db_prefix_end(BarrelId), []
      ),
      ok;
    not_found ->
      ok;
    Error ->
      Error
  end.
  
barrel_infos(StoreName, Name) ->
  #{ ref := Ref } = gproc:lookup_value(?rdb_store(StoreName)),
  BarrelKey = barrel_rocksdb_keys:local_barrel_ident(Name),
  {ok, Snapshot} = rocksdb:snapshot(Ref),
  ReadOptions = [{snapshot, Snapshot}],
  case rocksdb:get(Ref, BarrelKey, ReadOptions) of
    {ok, BarrelId} ->
      {ok, DocsCount} = db_get(Ref, barrel_rocksdb_keys:docs_count(BarrelId), 0, ReadOptions),
      {ok, DelDocsCount} = db_get(Ref, barrel_rocksdb_keys:docs_del_count(BarrelId), 0, ReadOptions),
      {ok, PurgeSeq} = db_get(Ref, barrel_rocksdb_keys:purge_seq(BarrelId), 0, ReadOptions),
      {ok, Itr} = rocksdb:iterator(Ref, [{iterate_lower_bound, barrel_rocksdb_keys:doc_seq_prefix(BarrelId)}]),
      LastSeq = case rocksdb:iterator_move(Itr, {seek_for_prev, barrel_rocksdb_keys:doc_seq_max(BarrelId)}) of
                  {ok, SeqKey, _} ->
                    barrel_rocksdb_keys:decode_doc_seq(SeqKey);
                  _ -> 0
                end,
      _ = rocksdb:iterator_close(Itr),
      _ = rocksdb:release_snapshot(Snapshot),
      {ok, #{ name => Name,
              store => StoreName,
              id => BarrelId,
              updated_seq => LastSeq,
              purge_seq => PurgeSeq,
              docs_count => DocsCount,
              docs_del_count => DelDocsCount}};
    not_found ->
      {error, db_not_found}
  end.


find_ident(Name, #{ ident_tab := Tab }) ->
  try {ok, ets:lookup_element(Tab, {b, Name}, 2)}
  catch
    error:badarg -> error
  end.


new_ident(#{ ident_tab := Tab }) ->
  ets:update_counter(Tab, '$ident_prefix', {2, 1}).

create_ident(Name, Ident, #{ ident_tab := Tab }) ->
  ets:insert(Tab, {{b, Name}, Ident}).

delete_ident(Name, #{ ident_tab := Tab }) ->
  ets:delete(Tab, {b, Name}).

%% -------------------
%% cache api

start_cache(Name, CacheSize) when is_atom(Name) ->
  Spec =
    #{id => {barrel_rocksdb_cache, Name},
      start => {barrel_rocksdb_cache, start_link, [Name, CacheSize]}},
  barrel_services:start_service(Spec).

stop_cache(Name) ->
  barrel_services:stop_service({barrel_rocksdb_cache, Name}).

%% -------------------
%% - internals

start_link(Name, Options) ->
  gen_server:start_link({via, gproc, ?rdb_store(Name)}, ?MODULE, [Name, Options], []).


init([Name, Options = #{ path := Path }]) ->
  Retries = application:get_env(barrel, rocksdb_open_retries, ?DB_OPEN_RETRIES),
  DbOptions = barrel_rocksdb_options:db_options(Options),
  case open_db(Path, DbOptions, Retries, false) of
    {ok, Ref, IdentTab} ->
      erlang:process_flag(trap_exit, true),
      Store = #{ name => Name, path => Path, ref => Ref, ident_tab => IdentTab },
      _ = gproc:set_value(?rdb_store(Name), Store),
      {ok, Store};
    {error, Error} ->
      exit(Error)
  end.

handle_call(_Msg, _From, Store) -> {reply, ok, Store}.

handle_cast(_Msg, Store) -> {noreply, Store}.

terminate(_Reason, #{ ref := Ref }) ->
  _ = rocksdb:close(Ref),
  ok.


open_db(_Path, _DbOpts, 0, LastError) ->
  {error, LastError};
open_db(Path, DbOpts,RetriesLeft, _LastError) ->
  case rocksdb:open(Path, DbOpts) of
    {ok, Ref} ->
      IdentTab = ets:new(?IDENT_TAB, [ordered_set, public, {read_concurrency, true}, {write_concurrency, true}]) ,
      ok = load_idents(Ref, IdentTab),
      {ok, Ref, IdentTab};
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


load_idents(Ref, IdentTab) ->
  ReadOptions =
    [{iterate_lower_bound, ?local_barrel_ident_prefix},
     {iterate_upper_boun, barrel_rocksdb_keys:local_barrel_ident_max()}],
  {ok, Itr} = rocksdb:iterator(Ref, ReadOptions),
  try load_idents(rocksdb:iterator_move(Itr, first), Itr, IdentTab, 0)
  after rocksdb:iterator_close(Itr)
  end.


load_idents({ok, Key, IdVal}, Itr, IdentTab, IdentMax) ->
  Name = barrel_rocksdb_keys:decode_barrel_ident(Key),
  {Ident, _} = barrel_encoding:decode_nonsorting_uvarint(IdVal),
  ets:insert(IdentTab, {{b, Name}, IdVal}),
  load_idents(rocksdb:iterator_move(Itr, next), Itr, IdentTab, erlang:max(Ident, IdentMax));
load_idents(_, _, IdentTab, IdentMax) ->
  ets:insert(IdentTab, {'$ident_prefix', IdentMax}),
  ok.
  


db_get(Ref, Key, Default, ReadOptions) ->
  case rocksdb:get(Ref, Key, ReadOptions) of
    {ok, Val} -> {ok, binary_to_term(Val)};
    not_found -> {ok, Default};
    Error -> Error
  end.
  