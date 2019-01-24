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
  init_storage/0,
  terminate_storage/2
]).

-export([
  open_barrel/2,
  delete_barrel/2,
  barrel_infos/2,
  barrel_exists/2
]).


%% write api
-export([recovery_unit/1,
         release_recovery_unit/1,
         commit/1,
         insert_doc_infos/2,
         update_doc_infos/3,
         put_local_doc/3,
         delete_local_doc/2,
         add_doc_revision/4,
         delete_doc_revision/3]).


%% query API
-export([init_ctx/2,
         release_ctx/1,
         get_doc_info/2,
         get_doc_revision/3,
         fold_docs/4,
         fold_changes/4,
         get_local_doc/2]).

-include_lib("barrel/include/barrel_logger.hrl").
-include("barrel_rocksdb.hrl").
-include("barrel_rocksdb_keys.hrl").

-define(IDENT_TAB, barrel_rocksdb_idents).

%% -------------------
%% store api

open_barrel(BarrelId, #rocksdb_store{ref=Ref}=Store) ->
  BarrelKey = barrel_rocksdb_keys:local_barrel_ident(BarrelId),
  case find_ident(BarrelId, Store) of
    {ok, Ident} ->
      {ok, Itr} = rocksdb:iterator(Ref, [{iterate_lower_bound, barrel_rocksdb_keys:doc_seq_prefix(Ident)}]),
      LastSeq = case rocksdb:iterator_move(Itr, {seek_for_prev, barrel_rocksdb_keys:doc_seq_max(Ident)}) of
                  {ok, SeqKey, _} ->
                    barrel_rocksdb_keys:decode_doc_seq(SeqKey);
                  _ -> 0
                end,
      _ = rocksdb:iterator_close(Itr),
      BarrelRef = #{  id => Ident, ref => Ref },
      {ok, BarrelRef, LastSeq};
    error ->
      NewIdentInt = new_ident(Store),
      Ident = barrel_encoding:encode_nonsorting_uvarint(<<>>, NewIdentInt),
      case rocksdb:put(Ref, BarrelKey, Ident, [{sync, true}]) of
        ok ->
          _ = create_ident(BarrelId, Ident, Store),
          BarrelRef = #{  id => Ident, ref => Ref },
          {ok, BarrelRef, 0};
        Error ->
          Error
      end
  end.

delete_barrel(Name, #rocksdb_store{ref=Ref}=Store) ->
  BarrelKey = barrel_rocksdb_keys:local_barrel_ident(Name),
  case rocksdb:get(Ref, BarrelKey, []) of
    {ok, Ident} ->
      %% first delete atomically all barrel metadata
      {ok, Batch} = rocksdb:batch(),
      rocksdb:batch_delete(Batch, BarrelKey),
      rocksdb:batch_delete(Batch, barrel_rocksdb_keys:docs_count(Ident)),
      rocksdb:batch_delete(Batch, barrel_rocksdb_keys:docs_del_count(Ident)),
      rocksdb:batch_delete(Batch, barrel_rocksdb_keys:purge_seq(Ident)),
      ok = rocksdb:write_batch(Ref, Batch, []),
      _ = delete_ident(Name, Store),
      %% delete barrel data
      rocksdb:delete_range(
        Ref, barrel_rocksdb_keys:db_prefix(Ident), barrel_rocksdb_keys:db_prefix_end(Ident), []
      ),
      ok;
    not_found ->
      ok;
    Error ->
      Error
  end.

barrel_infos(Name, #rocksdb_store{ref=Ref}) ->
  BarrelKey = barrel_rocksdb_keys:local_barrel_ident(Name),
  {ok, Snapshot} = rocksdb:snapshot(Ref),
  ReadOptions = [{snapshot, Snapshot}],
  case rocksdb:get(Ref, BarrelKey, ReadOptions) of
    {ok, Ident} ->
      {ok, DocsCount} = db_get(Ref, barrel_rocksdb_keys:docs_count(Ident), 0, ReadOptions),
      {ok, DelDocsCount} = db_get(Ref, barrel_rocksdb_keys:docs_del_count(Ident), 0, ReadOptions),
      {ok, PurgeSeq} = db_get(Ref, barrel_rocksdb_keys:purge_seq(Ident), 0, ReadOptions),
      {ok, Itr} = rocksdb:iterator(Ref, [{iterate_lower_bound, barrel_rocksdb_keys:doc_seq_prefix(Ident)}]),
      LastSeq = case rocksdb:iterator_move(Itr, {seek_for_prev, barrel_rocksdb_keys:doc_seq_max(Ident)}) of
                  {ok, SeqKey, _} ->
                    barrel_rocksdb_keys:decode_doc_seq(SeqKey);
                  _ -> 0
                end,
      _ = rocksdb:iterator_close(Itr),
      _ = rocksdb:release_snapshot(Snapshot),
      {ok, #{ updated_seq => LastSeq,
              purge_seq => PurgeSeq,
              docs_count => DocsCount,
              docs_del_count => DelDocsCount }};
    not_found ->
      {error, barrel_not_found}
  end.

barrel_exists(Name, Store) ->
  case find_ident(Name, Store) of
    {ok, _Ident} -> true;
    error -> false
  end.

find_ident(Name, #rocksdb_store{ident_tab=Tab}) ->
  try {ok, ets:lookup_element(Tab, {b, Name}, 2)}
  catch
    error:badarg -> error
  end.


new_ident(#rocksdb_store{ident_tab=Tab}) ->
  ets:update_counter(Tab, '$ident_prefix', {2, 1}).

create_ident(Name, Ident, #rocksdb_store{ident_tab=Tab}) ->
  ets:insert(Tab, {{b, Name}, Ident}).

delete_ident(Name, #rocksdb_store{ident_tab=Tab}) ->
  ets:delete(Tab, {b, Name}).



%% -------------------
%% docs

recovery_unit(#{ id := BarrelId, ref := Ref }) ->
  {ok, WB} = rocksdb:batch(),
  {ok, #{ ref => Ref,
          barrel_id => BarrelId,
          batch => WB,
          docs_count => 0,
          del_docs_count => 0 }}.

release_recovery_unit(#{ batch := Batch }) ->
  _ = (catch rocksdb:release_batch(Batch)),
  ok.

commit(#{ barrel_id := BarrelId,
          ref := Ref,
          batch := Batch,
          docs_count := DocsCount,
          del_docs_count := DocsDelCount }) ->
  ok = maybe_merge_count(DocsCount, docs_count, BarrelId, Batch),
  ok = maybe_merge_count(DocsDelCount, del_docs_count, BarrelId, Batch),
  rocksdb:write_batch(Ref, Batch, []).

maybe_merge_count(0, _Name, _Id, _Batch) -> ok;
maybe_merge_count(Count, docs_count, Id, Batch) ->
  rocksdb:batch_merge(
    Batch, barrel_rocksdb_keys:docs_count(Id), integer_to_binary(Count)
   );
maybe_merge_count(Count, del_docs_count, Id, Batch) ->
  rocksdb:batch_merge(
    Batch, barrel_rocksdb_keys:docs_del_count(Id), integer_to_binary(-Count)
   ).

insert_doc_infos(#{ barrel_id := BarrelId,
                    batch := Batch,
                    docs_count := DocsCount } = Ctx,
                 #{ id := DocId, seq := Seq } = DI) ->
  DIKey = barrel_rocksdb_keys:doc_info(BarrelId, DocId),
  SeqKey = barrel_rocksdb_keys:doc_seq(BarrelId, Seq ),
  DIVal = term_to_binary(DI),
  ok = rocksdb:batch_put(Batch, DIKey, DIVal),
  ok = rocksdb:batch_put(Batch, SeqKey, DIVal),
  Ctx#{ docs_count => DocsCount + 1 }.

update_doc_infos(#{ barrel_id := BarrelId,
                    batch := Batch,
                    docs_count := DocsCount0,
                    del_docs_count := DocsDelCount0 } = Ctx,
                 #{ id := DocId, seq := Seq, deleted := Del } = DI, OldSeq) ->

  {DocsCount1, DocsDelCount1} = case Del of
                                  false ->
                                    {DocsCount0, DocsDelCount0};
                                  true ->
                                    {DocsCount0 - 1, DocsDelCount0 + 1}
                                end,
  DIKey = barrel_rocksdb_keys:doc_info(BarrelId, DocId),
  SeqKey = barrel_rocksdb_keys:doc_seq(BarrelId, Seq ),
  OldSeqKey = barrel_rocksdb_keys:doc_seq(BarrelId, OldSeq),
  DIVal = term_to_binary(DI),
  ok = rocksdb:batch_put(Batch, DIKey, DIVal),
  ok = rocksdb:batch_put(Batch, SeqKey, DIVal),
  ok = rocksdb:batch_single_delete(Batch, OldSeqKey),
  Ctx#{ docs_count => DocsCount1, docs_del_count => DocsDelCount1 }.

add_doc_revision(#{ barrel_id := BarrelId, batch := Batch } = Ctx, DocId, DocRev, Body) ->
  RevKey = barrel_rocksdb_keys:doc_rev(BarrelId, DocId, DocRev),
  ok = rocksdb:batch_put(Batch, RevKey, term_to_binary(Body)),
  Ctx.

delete_doc_revision(#{ barrel_id := BarrelId, batch := Batch } = Ctx, DocId, DocRev) ->
  RevKey = barrel_rocksdb_keys:doc_rev(BarrelId, DocId, DocRev),
  ok = rocksdb:batch_delete(Batch, RevKey),
  Ctx.

put_local_doc(#{ id := BarrelId, ref := Ref}, DocId, LocalDoc) ->
  LocalKey = barrel_rocksdb_keys:local_doc(BarrelId, DocId),
  ok = rocksdb:put(Ref, LocalKey, term_to_binary(LocalDoc), []),
  ok.

delete_local_doc(#{ id := BarrelId, ref := Ref }, DocId) ->
  LocalKey = barrel_rocksdb_keys:local_doc(BarrelId, DocId),
  ok = rocksdb:delete(Ref, LocalKey, []),
  ok.

init_ctx(#{ id := BarrelId, ref := Ref }, IsRead) ->
  Snapshot = case IsRead of
                     true ->
                       {ok, S} = rocksdb:snapshot(Ref),
                       S;
                     false ->
                       undefined
                   end,
  {ok, #{ ref => Ref,
          barrel_id => BarrelId,
          snapshot => Snapshot }}.

release_ctx(Ctx) ->
  ok = maybe_release_snapshot(Ctx),
  ok.

maybe_release_snapshot(#{ snapshot := undefined }) -> ok;
maybe_release_snapshot(#{ snapshot := S }) ->
  rocksdb:release_snapshot(S).

read_options(#{ snapshot := undefined }) ->
  [];
read_options(#{ snapshot := Snapshot }) ->
  [{snapshot, Snapshot}];
read_options(_) ->
  [].

get_doc_info(#{ ref := Ref, barrel_id := BarrelId } = Ctx, DocId) ->
  ReadOptions = read_options(Ctx),
  DIKey = barrel_rocksdb_keys:doc_info(BarrelId, DocId),
  case rocksdb:get(Ref, DIKey, ReadOptions) of
    {ok, Bin} -> {ok, binary_to_term(Bin)};
    not_found -> {error, not_found};
    Error -> Error
  end.

get_doc_revision(#{ ref := Ref, barrel_id := BarrelId } = Ctx, DocId, Rev) ->
  ReadOptions = read_options(Ctx),
  RevKey = barrel_rocksdb_keys:doc_rev(BarrelId, DocId, Rev),
  case rocksdb:get(Ref, RevKey, ReadOptions) of
    {ok, Bin} -> {ok, binary_to_term(Bin)};
    not_found -> {error, not_found};
    Error -> Error
  end.

fold_docs(#{ ref := Ref, barrel_id := BarrelId } = Ctx, UserFun, UserAcc, Options) ->
  {LowerBound, IsNext} =
    case maps:find(next_to, Options) of
      {ok, NextTo} ->
        {barrel_rocksdb_keys:doc_info(BarrelId, NextTo), true};
      error ->
        case maps:find(start_at, Options) of
          {ok, StartAt} ->
            {barrel_rocksdb_keys:doc_info(BarrelId, StartAt), false};
          error ->
            {barrel_rocksdb_keys:doc_info(BarrelId, <<>>), false}
        end
    end,
  {UpperBound, Prev} =
    case maps:find(previous_to, Options) of
      {ok, PreviousTo} ->
        UpperBound1 = barrel_rocksdb_keys:doc_info(BarrelId, PreviousTo),
        {UpperBound1, UpperBound1};
      error ->
        case maps:find(end_at, Options) of
          {ok, EndAt} ->
            Bound = barrel_rocksdb_util:bytes_next(barrel_rocksdb_keys:doc_info(BarrelId, EndAt)),
            {Bound, false};
          error ->
            {barrel_rocksdb_keys:doc_info_max(BarrelId), false}
        end
    end,
  ReadOptions = [{iterate_lower_bound, LowerBound},
                  {iterate_upper_bound, UpperBound}] ++ read_options(Ctx),
  {ok, Itr} = rocksdb:iterator(Ref, ReadOptions),
  {Limit, Next, FirstMove} =
    case maps:find(limit_to_first, Options) of
      {ok, L} ->
        {L, fun() -> rocksdb:iterator_move(Itr, next) end, first};
      error ->
        case maps:find(limit_to_last, Options) of
          {ok, L} ->
            {L, fun() -> rocksdb:iterator_move(Itr, prev) end, last};
          error ->
            {1 bsl 32 - 1, fun() -> rocksdb:iterator_move(Itr, next) end, first}
        end
    end,
  First = case {rocksdb:iterator_move(Itr, FirstMove), IsNext} of
            {{ok, _, _}, true} ->
              Next();
            {Else, _} ->
              Else
          end,
  try do_fold_docs(First, Next, UserFun, UserAcc, Prev, Limit)
  after rocksdb:iterator_close(Itr)
  end.

do_fold_docs({ok, Key, Value}, Next, UserFun, UserAcc, PrevTo, Limit) when Limit > 0 ->
  if
    Key =/= PrevTo ->
      #{ id := DocId } = DI = binary_to_term(Value),
      case UserFun(DocId, DI, UserAcc) of
        {ok, UserAcc2} ->
          do_fold_docs(Next(), Next, UserFun, UserAcc2, PrevTo, Limit -1);
        {stop, UserAcc2} ->
          UserAcc2;
        skip ->
          do_fold_docs(Next(), Next, UserFun, UserAcc, PrevTo, Limit);
        stop ->
          UserAcc
      end;
    true ->
      UserAcc
  end;
do_fold_docs(_Else, _, _, UserAcc, _, _) ->
  UserAcc.

fold_changes(#{ ref := Ref, barrel_id := BarrelId } = Ctx, Since, UserFun, UserAcc) ->
  LowerBound = barrel_rocksdb_keys:doc_seq(BarrelId, Since),
  UpperBound = barrel_rocksdb_keys:doc_seq_max(BarrelId),
  ReadOptions = [{iterate_lower_bound, LowerBound},
                  {iterate_upper_bound, UpperBound}] ++ read_options(Ctx),
  {ok, Itr} = rocksdb:iterator(Ref, ReadOptions),
  First = rocksdb:iterator_move(Itr, first),
  try do_fold_changes(First, Itr, UserFun, UserAcc)
  after rocksdb:iterator_close(Itr)
  end.

do_fold_changes({ok, _, Value}, Itr, UserFun, UserAcc) ->
  #{ id := DocId } = DI = binary_to_term(Value),
  case UserFun(DocId, DI, UserAcc) of
    {ok, UserAcc2} ->
      do_fold_changes(rocksdb:iterator_move(Itr, next), Itr, UserFun, UserAcc2);
    {stop, UserAcc2} ->
      UserAcc2;
    ok ->
      do_fold_changes(rocksdb:iterator_move(Itr, next), Itr, UserFun, UserAcc);
    stop ->
      UserAcc;
    skip ->
      do_fold_changes(rocksdb:iterator_move(Itr, next), Itr, UserFun, UserAcc)
  end;

do_fold_changes(_, _, _, UserAcc) ->
  UserAcc.

get_local_doc(#{ ref := Ref, barrel_id := BarrelId }, DocId) ->
  LocalKey = barrel_rocksdb_keys:local_doc(BarrelId, DocId),
  case rocksdb:get(Ref, LocalKey, []) of
    {ok, DocBin} -> {ok, binary_to_term(DocBin)};
    not_found -> {error, not_found};
    Error -> Error
  end.

%% -------------------
%% internals

init_storage() ->
  DocsStorePath = barrel_config:get(docs_store_path),
  ShardPath = filename:join([DocsStorePath, "1"]),
  CacheRef = case barrel_config:get(rocksdb_cache_size) of
               false ->
                 false;
               Sz ->
                 {ok, CacheSize} =  barrel_lib:parse_size_unit(Sz),
                 %% Reserve 1 MB worth of memory from the cache. Under high
                 %% load situations we'll be using somewhat more than 1 MB
                 %% but usually not significantly more unless there is an I/O
                 %% throughput problem.
                 %%
                 %% We ensure that at least 1MB is allocated for the block cache.
                 %% Some unit tests expect to see a non-zero block cache hit rate,
                 %% but they use a cache that is small enough that all of it would
                 %% otherwise be reserved for the memtable.
                 WriteBufferSize = barrel_config:get(rocksdb_write_buffer_size),
                 Capacity = erlang:max(1 bsl 20, CacheSize - WriteBufferSize),
                 {ok, Ref} = rocksdb:new_lru_cache(Capacity),
                 ok = rocksdb:set_strict_capacity_limit(Ref, true),
                 Ref
             end,
  Retries = application:get_env(barrel, rocksdb_open_retries, ?DB_OPEN_RETRIES),
  DbOptions = barrel_rocksdb_options:db_options(CacheRef),
  case open_db(ShardPath, DbOptions, Retries, false) of
    {ok, DbRef} ->
      IdentTab = ets:new(
        ?IDENT_TAB, [ordered_set, public, {read_concurrency, true}, {write_concurrency, true}]
      ),
      ok = load_idents(DbRef, IdentTab),
      Store =
        #rocksdb_store{ref=DbRef,
                       cache_ref=CacheRef,
                       path=ShardPath,
                       ident_tab=IdentTab},
      {ok, Store};
    {error, Error} ->
      exit(Error)
  end.

terminate_storage(_Reason, #rocksdb_store{ref=Ref}) ->
  _ = rocksdb:close(Ref),
  ok.

open_db(_Path, _DbOpts, 0, LastError) ->
  {error, LastError};
open_db(Path, DbOpts,RetriesLeft, _LastError) ->
  case rocksdb:open(Path, DbOpts) of
    {ok, Ref} -> {ok, Ref};
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
