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
  init_barrel/2,
  delete_barrel/2,
  barrel_infos/2
]).

-export([
  init_ctx/3,
  release_ctx/1
]).

-export([
  write_doc_infos/4,
  write_docs/2,
  get_doc_info/2,
  get_doc_revision/3,
  fold_docs/4,
  fold_changes/4,
  insert_local_doc/2,
  delete_local_doc/2,
  get_local_doc/2,
  add_doc_revision/4,
  delete_doc_revision/3
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


init_barrel(StoreName, BarrelId) ->
  #{ ref := Ref } = Store = gproc:lookup_value(?rdb_store(StoreName)),
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
      {ok, Ident, LastSeq};
    error ->
      NewIdentInt = new_ident(Store),
      Ident = barrel_encoding:encode_nonsorting_uvarint(<<>>, NewIdentInt),
      case rocksdb:put(Ref, BarrelKey, Ident, [{sync, true}]) of
        ok ->
          _ = create_ident(BarrelId, Ident, Store),
          {ok, 0};
        Error ->
          Error
      end
  end.

delete_barrel(StoreName, Name) ->
  #{ ref := Ref } = Store = gproc:lookup_value(?rdb_store(StoreName)),
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
  
barrel_infos(StoreName, Name) ->
  #{ ref := Ref } = gproc:lookup_value(?rdb_store(StoreName)),
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
%% docs


init_ctx(StoreName, Name, IsRead) ->
  #{ ref := Ref } = Store = gproc:lookup_value(?rdb_store(StoreName)),
  case find_ident(Name, Store) of
    {ok, BarrelId} ->
      Snapshot = case IsRead of
                   true ->
                     {ok, S} = rocksdb:snapshot(Ref),
                     S;
                   false ->
                     undefined
                 end,

      {ok, #{ ref => Ref, barrel_id => BarrelId, snapshot => Snapshot }};
    error ->
      {error, db_not_found}
  end.

release_ctx(#{ snapshot := undefined }) -> ok;
release_ctx(#{ snapshot := S }) ->
  rocksdb:release_snapshot(S).

write_doc_infos(Ctx, DocInfos, AddCount, DelCount) ->
  #{ ref := Ref, barrel_id := BarrelId } = Ctx,
  {ok, WB} = rocksdb:batch(),
  lists:foreach(
    fun(#{  id := DocId, seq := Seq } = DI) ->
      DIKey = barrel_rocksdb_keys:doc_info(BarrelId, DocId),
      SeqKey = barrel_rocksdb_keys:doc_seq(BarrelId, Seq ),
      DIVal = term_to_binary(DI),
      rocksdb:batch_put(WB, DIKey, DIVal),
      rocksdb:batch_put(WB, SeqKey, DIVal)
    end,
    DocInfos
  ),
  %% update counters -)
  rocksdb:batch_merge(WB, barrel_rocksdb_keys:docs_count(BarrelId), integer_to_binary(AddCount)),
  rocksdb:batch_merge(WB, barrel_rocksdb_keys:docs_del_count(BarrelId), integer_to_binary(-DelCount)),
  try rocksdb:write_batch(Ref, WB, [])
  after rocksdb:release_batch(WB)
  end.

%% TODO: do we need to handle a batch there? we could simplu have a write_doc.
write_docs(#{ ref := Ref, barrel_id := BarrelId }, DIPairs) ->
  {ok, WB} = rocksdb:batch(),
  {AddInc, DelInc} = lists:foldl(
    fun
      ({DI, DI}, {Added, Deleted}) ->
        {Added, Deleted};
      ({#{ id := DocId, seq := Seq } = DI, not_found}, {Added, Deleted}) ->
        DI2 = flush_revisions(Ref, BarrelId, DI),
        DIKey = barrel_rocksdb_keys:doc_info(BarrelId, DocId),
        SeqKey = barrel_rocksdb_keys:doc_seq(BarrelId, Seq ),
        rocksdb:batch_put(WB, DIKey, term_to_binary(DI2)),
        rocksdb:batch_put(WB, SeqKey, term_to_binary(DI2)),
        {Added + 1, Deleted};
      ({#{ id := DocId, seq := Seq } = DI, OldDI}, {Added, Deleted}) ->
        DI2 = flush_revisions(Ref, BarrelId, DI),
        DIKey = barrel_rocksdb_keys:doc_info(BarrelId, DocId),
        SeqKey = barrel_rocksdb_keys:doc_seq(BarrelId, Seq ),
        rocksdb:batch_put(WB, DIKey, term_to_binary(DI2)),
        rocksdb:batch_put(WB, SeqKey, term_to_binary(DI2)),
        case {maps:get(deleted, DI, false), maps:get(deleted, OldDI, false)} of
          {true, false}  ->
            {Added, Deleted + 1};
          {false, true} ->
            {Added, Deleted - 1};
          {_, _} ->
            {Added, Deleted}
        end
    end,
    {0, 0},
    DIPairs
  ),
  %% update counters -)
  rocksdb:batch_merge(WB, barrel_rocksdb_keys:docs_count(BarrelId), integer_to_binary(AddInc)),
  rocksdb:batch_merge(WB, barrel_rocksdb_keys:docs_del_count(BarrelId), integer_to_binary(DelInc)),

  WriteResult = rocksdb:write_batch(Ref, WB, []),
  ok = rocksdb:release_batch(WB),
  WriteResult.

flush_revisions(Ref, BarrelId, DI = #{ id := DocId }) ->
  {BodyMap, DI2} = maps:take(body_map, DI),

  _ = maps:fold(
    fun(DocRev, Body, _) ->
      RevKey = barrel_rocksdb_keys:doc_rev(BarrelId, DocId, DocRev),
      ok = rocksdb:put(Ref, RevKey, term_to_binary(Body), []),
      ok
    end,
    ok,
    BodyMap
  ),
  DI2.

read_options(#{ snapshot := undefined }) ->
  [];
read_options(#{ snapshot := Snapshot }) ->
  [{snapshot, Snapshot}].

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

add_doc_revision(#{ ref := Ref, barrel_id := BarrelId }, DocId, DocRev, Body) ->
  RevKey = barrel_rocksdb_keys:doc_rev(BarrelId, DocId, DocRev),
  rocksdb:put(Ref, RevKey, term_to_binary(Body), []).

delete_doc_revision(#{ ref := Ref, barrel_id := BarrelId }, DocId, DocRev) ->
  RevKey = barrel_rocksdb_keys:doc_rev(BarrelId, DocId, DocRev),
  rocksdb:delete(Ref, RevKey, []).


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
            {barrel_rocksdb_keys:doc_info(BarrelId, EndAt), false};
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


insert_local_doc(#{ ref := Ref, barrel_id := BarrelId }, LocalDoc) ->
  #{ <<"id" >> := DocId } = LocalDoc,
  LocalKey = barrel_rocksdb_keys:local_doc(BarrelId, DocId),
  rocksdb:put(Ref, LocalKey, term_to_binary(LocalDoc), []).

delete_local_doc(#{ ref := Ref, barrel_id := BarrelId }, DocId) ->
  LocalKey = barrel_rocksdb_keys:local_doc(BarrelId, DocId),
  rocksdb:delete(Ref, LocalKey, []).

get_local_doc(#{ ref := Ref, barrel_id := BarrelId }, DocId) ->
  LocalKey = barrel_rocksdb_keys:local_doc(BarrelId, DocId),
  rocksdb:get(Ref, LocalKey, []).

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
  