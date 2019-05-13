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


-export([create_barrel/1,
         open_barrel/1,
         delete_barrel/1,
         barrel_infos/1]).


%% write api
-export([insert_doc/4,
         update_doc/6,
         put_local_doc/3,
         delete_local_doc/2]).


%% query API
-export([init_ctx/2,
         release_ctx/1,
         get_doc_info/2,
         get_doc_revision/3,
         fold_docs/4,
         fold_changes/4,
         get_local_doc/2]).

-export([open_view/2,
         update_view/3,
         delete_view/2,
         update_view_index/4,
         put_view_upgrade_task/3,
         get_view_upgrade_task/2,
         delete_view_upgrade_task/2,
         fold_view_index/5
        ]).

-export([put_attachment/4,
         fetch_attachment/4]).

-export([get_counter/2,
         add_counter/3,
         set_counter/3,
         delete_counter/2]).


-export([start_link/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2]).


-include("barrel.hrl").
-include("barrel_rocksdb.hrl").
-include("barrel_rocksdb_keys.hrl").

-define(db_ref, persistent_term:get({?MODULE, db_ref})).
-define(db, maps:get(ref, persistent_term:get({?MODULE, db_ref}))).

%% -------------------
%% store api

create_barrel(Name) ->
  #{ ref := Ref, counters := Counters } = ?db_ref,
  BarrelKey = barrel_rocksdb_keys:local_barrel_ident(Name),
  case rdb_get(Ref, BarrelKey, []) of
    {ok, _Ident} ->
      {error, barrel_already_exists};
    not_found ->
       ok = counters:add(Counters, 1, 1),
       Id = counters:get(Counters, 1),
       BinId = << Id:32/integer >>,
       {ok, WB} = rocksdb:batch(),
       ok = rocksdb:batch_put(WB, BarrelKey, BinId),
       ok = rocksdb:batch_put(WB, barrel_rocksdb_keys:docs_count(BinId), integer_to_binary(0)),
       ok = rocksdb:batch_put(WB, barrel_rocksdb_keys:docs_del_count(BinId), integer_to_binary(0)),
       ok = rocksdb:batch_put(WB, barrel_rocksdb_keys:purge_seq(BinId), integer_to_binary(0)),
       ok = rocksdb:write_batch(Ref, WB, [{sync, true}]),
       ok = rocksdb:release_batch(WB),
       ok
  end.

open_barrel(Name) ->
  BarrelKey = barrel_rocksdb_keys:local_barrel_ident(Name),
  case rdb_get(?db, BarrelKey, []) of
    {ok, Ident} ->
      LastSeq = get_last_seq(Ident, []),
      {ok, Ident, LastSeq};
    not_found ->
      {error, barrel_not_found};
    Error ->
      Error
  end.

get_last_seq(Ident, ReadOpts0) ->
  ReadOpts =
    [{iterate_lower_bound, barrel_rocksdb_keys:doc_seq_prefix(Ident)} | ReadOpts0],
  {ok, Itr} = rocksdb:iterator(?db, ReadOpts),
  MaxSeq = barrel_rocksdb_keys:doc_seq_max(Ident),
  LastSeq = case rdb_iterator_move(Itr, {seek_for_prev, MaxSeq}) of
              {ok, SeqKey, _} ->
                barrel_rocksdb_keys:decode_doc_seq(Ident, SeqKey);
              _ -> 0
            end,
  _ = rocksdb:iterator_close(Itr),
  LastSeq.


delete_barrel(Name) ->
  BarrelKey = barrel_rocksdb_keys:local_barrel_ident(Name),
  case rdb_get(?db, BarrelKey, []) of
    {ok, Ident} ->
      %% first delete atomically all barrel metadata
      ok = rocksdb:delete(?db, BarrelKey, []),
      %% delete barrel data
      rocksdb:delete_range(?db,
                           barrel_rocksdb_keys:db_prefix(Ident),
                           barrel_rocksdb_keys:db_prefix_end(Ident),
                           []);
    not_found ->
      ok;
    Error ->
      Error
  end.

barrel_infos(Name) ->
  BarrelKey = barrel_rocksdb_keys:local_barrel_ident(Name),
  {ok, Snapshot} = rocksdb:snapshot(?db),
  ReadOpts = [{snapshot, Snapshot}],
  case rdb_get(?db, BarrelKey, ReadOpts) of
    {ok, Ident} ->
      %% NOTE: we should rather use the multiget API from rocksdb there
      %% but until it's not exposed just get the results for each Keys
      {ok, DocsCount} = db_get_int(barrel_rocksdb_keys:docs_count(Ident), 0, ReadOpts),
      {ok, DelDocsCount} = db_get_int(barrel_rocksdb_keys:docs_del_count(Ident), 0, ReadOpts),
      {ok, PurgeSeq} = db_get_int(barrel_rocksdb_keys:purge_seq(Ident), 0, ReadOpts),
      %% get last sequence
      LastSeq = get_last_seq(Ident, ReadOpts),
      _ = rocksdb:release_snapshot(Snapshot),
      {ok, #{ updated_seq => LastSeq,
              purge_seq => PurgeSeq,
              docs_count => DocsCount,
              docs_del_count => DelDocsCount }};
    not_found ->
      {error, barrel_not_found}
  end.



%% -------------------
%% meta


get_counter(Prefix, Name) ->
  CounterKey = barrel_rocksdb_keys:counter_key(Prefix, Name),
  case rdb_get(?db, CounterKey, []) of
    {ok, Bin} -> {ok, binary_to_integer(Bin)};
    not_found -> not_found
  end.

set_counter(Prefix, Name, Value) ->
  CounterKey = barrel_rocksdb_keys:counter_key(Prefix, Name),
  rdb_put(?db, CounterKey, integer_to_binary(Value), []).


add_counter(Prefix, Name, Value) ->
  CounterKey = barrel_rocksdb_keys:counter_key(Prefix, Name),
  rocksdb:merge(?db, CounterKey, integer_to_binary(Value), []).

delete_counter(Prefix, Name) ->
  CounterKey = barrel_rocksdb_keys:counter_key(Prefix, Name),
  rocksdb:delete(?db, CounterKey, []).


%% -------------------
%% docs

insert_doc(BarrelId, DI, DocRev, DocBody) ->
  ?start_span(#{ <<"log">> => <<"insert documents in rocksdb" >> }),
  try do_insert_docs(BarrelId, DI, DocRev, DocBody)
  after
    ?end_span
  end.

do_insert_docs(BarrelId, DI, DocRev, DocBody) ->
  {ok, Batch} = rocksdb:batch(),
  batch_put_doc(Batch, BarrelId, DI, DocRev, DocBody),
  merge_docs_count(Batch, BarrelId, 1),
  write_batch(Batch).

update_doc(BarrelId, DI, DocRev, DocBody, OldSeq, OldDel) ->
  ?start_span(#{ <<"log">> => <<"update document in rocksdb" >> }),
  try do_update_doc(BarrelId,DI,DocRev, DocBody, OldSeq, OldDel)
  after
    ?end_span
  end.

do_update_doc(BarrelId, #{ deleted := Del } = DI,
           DocRev, DocBody, OldSeq, OldDel) ->

  {ok, Batch} = rocksdb:batch(),
  batch_put_doc(Batch, BarrelId, DI, DocRev, DocBody),
  OldSeqKey = barrel_rocksdb_keys:doc_seq(BarrelId, OldSeq),
  ok = rocksdb:batch_single_delete(Batch, OldSeqKey),
  case {Del, OldDel} of
    {true, false} ->
      merge_docs_count(Batch, BarrelId, -1),
      merge_docs_del_count(Batch, BarrelId, 1);
    {false, true} ->
      merge_docs_count(Batch, BarrelId, 1),
      merge_docs_del_count(Batch, BarrelId, -1);
    {_, _} ->
      ok
  end,
  write_batch(Batch).

batch_put_doc(Batch, BarrelId, #{ id := DocId, seq := Seq } = DI, DocRev, DocBody) ->
  DIKey = barrel_rocksdb_keys:doc_info(BarrelId, DocId),
  SeqKey = barrel_rocksdb_keys:doc_seq(BarrelId, Seq),
  RevKey = barrel_rocksdb_keys:doc_rev(BarrelId, DocId, DocRev),
  DIVal = term_to_binary(DI),
  rocksdb:batch_put(Batch, DIKey, DIVal),
  rocksdb:batch_put(Batch, SeqKey, DIVal),
  rocksdb:batch_put(Batch, RevKey, term_to_binary(DocBody)).

merge_docs_count(Batch, BarrelId, Val) ->
  Key = barrel_rocksdb_keys:docs_count(BarrelId),
  rocksdb:batch_merge( Batch, Key, integer_to_binary(Val)).

merge_docs_del_count(Batch, BarrelId, Val) ->
  Key = barrel_rocksdb_keys:docs_del_count(BarrelId),
  rocksdb:batch_merge(Batch, Key, integer_to_binary(Val)).

write_batch(WB) ->
  ?start_span(#{ <<"log">> => <<"write rocksdb batch">> }),
  try rocksdb:write_batch(?db, WB, [{sync, true}])
  after
    rocksdb:release_batch(WB),
    ?end_span
  end.

put_local_doc(BarrelId, DocId, LocalDoc) ->
  LocalKey = barrel_rocksdb_keys:local_doc(BarrelId, DocId),
  rdb_put(?db, LocalKey, term_to_binary(LocalDoc), []).

delete_local_doc(BarrelId, DocId) ->
  LocalKey = barrel_rocksdb_keys:local_doc(BarrelId, DocId),
  rocksdb:delete(?db, LocalKey, []).

init_ctx(BarrelId, IsRead) ->
  Snapshot = case IsRead of
               true ->
                 ?start_span(#{ <<"log">> => <<"rocksdb: get a database snapshot">> }),
                 {ok, S} = try rocksdb:snapshot(?db)
                           after
                             ?end_span
                           end,
                 S;
               false ->
                 undefined
             end,

  {ok, #{ barrel_id => BarrelId,
          snapshot => Snapshot }}.

release_ctx(Ctx) ->
  ok = maybe_release_snapshot(Ctx),
  ok.

maybe_release_snapshot(#{ snapshot := undefined }) -> ok;
maybe_release_snapshot(#{ snapshot := S }) ->
  rocksdb:release_snapshot(S).

read_options(#{ snapshot := undefined }) ->[];
read_options(#{ snapshot := Snapshot }) -> [{snapshot, Snapshot}];
read_options(_) -> [].

get_doc_info(Ctx, DocId) ->
  ?start_span(#{ <<"log">> => <<"get document info from rocksdb">> }),
  try do_get_doc_info(Ctx, DocId)
  after
    ?end_span
  end.

do_get_doc_info(#{ barrel_id := BarrelId } = Ctx, DocId) ->
  ReadOptions = read_options(Ctx),
  DIKey = barrel_rocksdb_keys:doc_info(BarrelId, DocId),
  case rdb_get(?db, DIKey, ReadOptions) of
    {ok, Bin} -> {ok, binary_to_term(Bin)};
    not_found -> {error, not_found};
    Error -> Error
  end;
do_get_doc_info(BarrelId, DocId) ->
  DIKey = barrel_rocksdb_keys:doc_info(BarrelId, DocId),
  case rdb_get(?db, DIKey, []) of
    {ok, Bin} -> {ok, binary_to_term(Bin)};
    not_found -> {error, not_found};
    Error -> Error
  end.

get_doc_revision(Ctx, DocId, Rev) ->
  ?start_span(#{ <<"log">> => <<"get document revision from rocksdb">> }),
  try
    do_get_doc_revision(Ctx, DocId, Rev)
  after
    ?end_span
  end.


do_get_doc_revision(#{ barrel_id := BarrelId } = Ctx, DocId, Rev) ->
  ReadOptions = read_options(Ctx),
  RevKey = barrel_rocksdb_keys:doc_rev(BarrelId, DocId, Rev),
  case rdb_get(?db, RevKey, ReadOptions) of
    {ok, Bin} -> {ok, binary_to_term(Bin)};
    not_found -> {error, not_found};
    Error -> Error
  end.

fold_docs(#{ barrel_id := BarrelId } = Ctx, UserFun, UserAcc, Options) ->
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
  {ok, Itr} = rocksdb:iterator(?db, ReadOptions),
  {Limit, Next, FirstMove} =
    case maps:find(limit_to_first, Options) of
      {ok, L} ->
        {L, fun() -> rdb_iterator_move(Itr, next) end, first};
      error ->
        case maps:find(limit_to_last, Options) of
          {ok, L} ->
            {L, fun() -> rdb_iterator_move(Itr, prev) end, last};
          error ->
            {1 bsl 32 - 1, fun() -> rdb_iterator_move(Itr, next) end, first}
        end
    end,
  First = case {rdb_iterator_move(Itr, FirstMove), IsNext} of
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

fold_changes(#{ barrel_id := BarrelId } = Ctx, Since, UserFun, UserAcc) ->
  LowerBound = barrel_rocksdb_keys:doc_seq(BarrelId, Since),
  UpperBound = barrel_rocksdb_keys:doc_seq_max(BarrelId),
  ReadOptions = [{iterate_lower_bound, LowerBound},
                  {iterate_upper_bound, UpperBound}] ++ read_options(Ctx),
  {ok, Itr} = rocksdb:iterator(?db, ReadOptions),
  First = rdb_iterator_move(Itr, first),
  try do_fold_changes(First, Itr, UserFun, UserAcc)
  after rocksdb:iterator_close(Itr)
  end.

do_fold_changes({ok, _, Value}, Itr, UserFun, UserAcc) ->
  #{ id := DocId } = DI = binary_to_term(Value),
  case UserFun(DocId, DI, UserAcc) of
    {ok, UserAcc2} ->
      do_fold_changes(rdb_iterator_move(Itr, next), Itr, UserFun, UserAcc2);
    {stop, UserAcc2} ->
      UserAcc2;
    ok ->
      do_fold_changes(rdb_iterator_move(Itr, next), Itr, UserFun, UserAcc);
    stop ->
      UserAcc;
    skip ->
      do_fold_changes(rdb_iterator_move(Itr, next), Itr, UserFun, UserAcc)
  end;

do_fold_changes(_, _, _, UserAcc) ->
  UserAcc.

get_local_doc(BarrelId, DocId) ->
  LocalKey = barrel_rocksdb_keys:local_doc(BarrelId, DocId),
  case rdb_get(?db, LocalKey, []) of
    {ok, DocBin} -> {ok, binary_to_term(DocBin)};
    not_found -> {error, not_found};
    Error -> Error
  end.

%% -------------------
%% view

open_view(Id, ViewId) ->
  ViewKey = barrel_rocksdb_keys:view_meta(Id, ViewId),
  case rdb_get(?db, ViewKey, []) of
    {ok, InfoBin} ->
      {ok, binary_to_term(InfoBin)};
    Error ->
      Error
  end.

update_view(Id, ViewId, View) ->
  ViewKey = barrel_rocksdb_keys:view_meta(Id, ViewId),
  rdb_put(?db, ViewKey, term_to_binary(View), []).

delete_view(Id, ViewId) ->
  Start = barrel_rocksdb_keys:view_meta(Id, ViewId),
  End = barrel_rocksdb_keys:view_prefix_end(Id, ViewId),
  %% delete  all range
  rocksdb:delete_range(?db, Start, End, []).

put_view_upgrade_task(Id, ViewId, Task) ->
  rdb_put(?db, barrel_rocksdb_keys:view_upgrade_task(Id, ViewId),
              term_to_binary(Task),
              []
             ).

get_view_upgrade_task(Id, ViewId) ->
  case rdb_get(?db, barrel_rocksdb_keys:view_upgrade_task(Id, ViewId), []) of
    {ok, TaskBin} -> {ok, binary_to_term(TaskBin)};
    Error -> Error
  end.

delete_view_upgrade_task(Id, ViewId) ->
  rocksdb:delete(?db, barrel_rocksdb_keys:view_upgrade_task(Id, ViewId), []).

update_view_index(Id, ViewId, DocId, KVs) ->
  %% get the reverse maps for the document.
  %% reverse maps contains old keys indexed
  RevMapKey = barrel_rocksdb_keys:view_doc_key(Id, ViewId, DocId),
  OldReverseMaps = case rdb_get(?db, RevMapKey, []) of
                     {ok, Bin} ->
                       binary_to_term(Bin);
                     not_found ->
                       []
                   end,
  %% we add new keys as prefixed keys with empty values to the index
  %% old keys are deleted once since they are only supposed to be unique
  %% and have only one update
  ViewPrefix = barrel_rocksdb_keys:view_prefix(Id, ViewId),
  {ok, Batch} = rocksdb:batch(),
  ReverseMaps = lists:foldl(fun({K0, V0}, Acc) ->
                                K1 = barrel_rocksdb_keys:encode_view_key(K0, ViewPrefix),
                                V1 = term_to_binary(V0),
                                rocksdb:batch_put(Batch, append_docid(K1, DocId), V1),
                                [K1 | Acc]
                            end,
                            [], KVs),
  ReverseMaps1 = lists:usort(ReverseMaps),
  ToDelete = OldReverseMaps -- ReverseMaps1,
  lists:foreach(fun(K) ->
                    rocksdb:batch_single_delete(Batch, append_docid(K, DocId))
                end, ToDelete),
  rocksdb:batch_put(Batch, RevMapKey, term_to_binary(ReverseMaps1)),
   %% write the batch
  ok = rocksdb:write_batch(?db, Batch, [{sync, true}]),
  ok = rocksdb:release_batch(Batch),
  ok.

append_docid(KeyBin, DocId) ->
  barrel_encoding:encode_binary_ascending(KeyBin, DocId).


fold_view_index(Id, ViewId, UserFun, UserAcc, Options) ->
  Prefix = barrel_rocksdb_keys:view_prefix(Id, ViewId),
  WrapperFun = fun(KeyBin, ValBin, Acc) ->
                   {DocId, Key} = barrel_rocksdb_keys:decode_view_key(Prefix, KeyBin),
                   Val = binary_to_term(ValBin),
                   UserFun({DocId, Key, Val}, Acc)
               end,

  BeginOrEqual = maps:get(begin_or_equal, Options, true),
  Begin = maps:get(begin_key, Options, [<<>>]),
  LowerBound = barrel_rocksdb_keys:encode_view_key(Begin, Prefix),
  End = maps:get(end_key, Options, [?key_max]),
  EndOrEqual = maps:get(end_or_equal, Options, true),
  End1 = barrel_rocksdb_keys:encode_view_key(End, Prefix),
  UpperBound = case EndOrEqual of
                 true ->
                   barrel_rocksdb_util:bytes_next(End1);
                 false ->
                   End1
               end,
  Reverse = maps:get(reverse, Options, false),
  Snapshot = maps:get(snapshot, Options, false),
  Limit = maps:get(limit, Options, 1 bsl 64 - 1),
  ReadOpts0 = case Snapshot of
                true ->
                  {ok, Snapshot} = rocksdb:snapshot(?db),
                  [{snapshot, Snapshot}];
                false ->
                  []
              end,
  ReadOpts = [{iterate_lower_bound, LowerBound},
              {iterate_upper_bound, UpperBound}] ++ ReadOpts0,

  {ok, Itr} = rocksdb:iterator(?db, ReadOpts),

  case Reverse of
    false ->
      Next = fun() -> rdb_iterator_move(Itr, next) end,
      Len = byte_size(LowerBound),
      First = case rdb_iterator_move(Itr, first) of
                {ok, << LowerBound:Len/binary, _/binary >>, _} when BeginOrEqual =:= false ->
                  Next();
                Else  ->
                  Else
              end,

      do_fold(First, Next, Itr, WrapperFun, UserAcc, Limit);
    true ->
      First = rdb_iterator_move(Itr, last),
      Next = fun() -> rdb_iterator_move(Itr, prev) end,
      do_fold(First, Next, Itr, WrapperFun, UserAcc, Limit)
  end.


do_fold(First, Next, Itr, WrapperFun, UserAcc, Limit) ->
  try do_fold_1(First,Next, WrapperFun, UserAcc, Limit)
  after rocksdb:iterator_close(Itr)
  end.

do_fold_1({ok, K, V}, Next, Fun, Acc, Limit) when Limit > 0 ->
  case Fun(K, V, Acc) of
    {ok, Acc2} ->
      do_fold_1(Next(), Next, Fun, Acc2, Limit - 1);
    {skip, Acc2} ->
      do_fold_1(Next(), Next, Fun, Acc2, Limit);
    {stop, Acc2} ->
      Acc2;
    ok ->
      do_fold_1(Next(), Next, Fun, Acc, Limit - 1);
    skip ->
      do_fold_1(Next(), Next, Fun, Acc, Limit);
    stop ->
      Acc
  end;
do_fold_1(_, _, _, Acc, _) ->
  Acc.


put_attachment(BarrelId, DocId, AttName, AttBin) ->
 Uid = barrel_lib:make_uid(AttName),
 AttKey = barrel_rocksdb_keys:att_prefix(BarrelId, DocId, Uid),
 case rdb_put(?db, AttKey, AttBin, []) of
   ok -> {ok, AttKey};
   Error -> Error
 end.

fetch_attachment(Ctx, _DocId, _AttName, AttKey) ->
  rdb_get(?db, AttKey, read_options(Ctx)).



rdb_get(Db, Key, Options) ->
  jobs:run(
    barrel_ioq,
    fun() ->
        rocksdb:get(Db, Key,  Options)
    end).

rdb_put(Db, Key, Value, Options) ->
  jobs:run(
    barrel_ioq,
    fun() ->
       rocksdb:put(Db, Key, Value, Options)
    end).

rdb_iterator_move(Itr, Op) ->
  jobs:run(
    barrel_ioq,
    fun() ->
        rocksdb:iterator_move(Itr, Op)
    end).





start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
  erlang:process_flag(trap_exit, true),
  Path = barrel_config:get(rocksdb_root_dir),
  CacheRef = init_cache(),
  RateLimiter = init_rate_limiter(),
  {ok, DbRef} = init_db(Path, CacheRef, RateLimiter),
  ok = persistent_term:put({?MODULE, db_ref}, DbRef),
  {TRef, LogStatInterval} = case barrel_config:get(rocksdb_log_stats) of
                              false ->
                                {undefined, false};
                              Interval when is_integer(Interval) ->
                                TRef1 = erlang:send_after(Interval, self(), stats),
                                {TRef1, Interval}
                            end,

  ?LOG_INFO("Rocksdb storage initialized in ~p~n", [Path]),
  {ok, #{ path => Path,
          ref => DbRef,
          cache_ref => CacheRef,
          rate_limiter => RateLimiter,
          tref => TRef,
          log_stat_interval => LogStatInterval }}.

handle_call(cache_info, _From, #{ cache_ref := Ref } = State) ->
  {reply, rocksdb:cache_info(Ref), State};

handle_call(_Msg, _From, State) ->
  {reply, ok, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(stats, #{ ref :=  #{ ref := Ref }, log_stat_interval := Interval } = State) ->
  {ok, Stats} = rocksdb:stats(Ref),
  ?LOG_INFO("== rocksdb stats ==~n~s~n", [Stats]),
  TRef = erlang:send_after(Interval, self(), stats),
  {noreply, State#{ tref => TRef }}.


terminate(_Reason, #{ ref := #{ ref := Ref }, cache_ref := CacheRef }) ->
  _ = persistent_term:erase({?MODULE, db_ref}),
  ok = rocksdb:close(Ref),
  ok = rocksdb:release_cache(CacheRef),

  ok.

init_cache() ->
  case barrel_config:get(rocksdb_cache_size) of
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
      {ok, Ref} = rocksdb:new_cache(lru, Capacity),
      %%ok = rocksdb:set_strict_capacity_limit(Ref, true),
      ?LOG_INFO("Rocksdb cache initialized. type=lru, capacity=~p~n", [Capacity]),
      Ref
  end.


init_rate_limiter() ->
  RateBytesPerSec = barrel_config:get(rocksdb_write_bytes_per_sec, undefined),
  init_rate_limiter(RateBytesPerSec).


init_rate_limiter(undefined) ->
  undefined;
init_rate_limiter(RateBytesPerSec) when is_integer(RateBytesPerSec) ->
  AutoTuned = barrel_config:get(rocksdb_writes_auto_tuned, true),
  {ok, Limiter} = rocksdb:new_rate_limiter(RateBytesPerSec, AutoTuned),
  Limiter;
init_rate_limiter(_) ->
  erlang:exit({badarg, rocksdb_writes_auto_tuned}).


init_db(Dir, CacheRef, RateLimiter) ->
  Retries = application:get_env(barrel, rocksdb_open_retries, ?DB_OPEN_RETRIES),
  DbOpts0 = default_db_options() ++ cf_options(CacheRef),
  DbOpts = case RateLimiter of
             undefined -> DbOpts0;
             _ -> [{rate_limiter, RateLimiter} | DbOpts0]
           end,
  case open_db(Dir, DbOpts, Retries, false) of
    {ok, Ref} ->
      %% find last ident
      {ok, Itr} = rocksdb:iterator(Ref, []),
      LastIdent = case rocksdb:iterator_move(Itr, last) of
                    {ok, << _:2/binary, Id:32/integer, _/binary >>, _} ->
                      Id;
                     _Else ->
                      0
                  end,
      ok = rocksdb:iterator_close(Itr),
      %% we stotre the last ident in an atomic counter
      DbCounters = counters:new(1, []),
      counters:put(DbCounters, 1, LastIdent),
      {ok, #{ ref => Ref, counters => DbCounters }};
    Error ->
      Error
  end.

open_db(_Dir, _DbOpts, 0, LastError) ->
  {error, LastError};
open_db(Dir, DbOpts, RetriesLeft, _LastError) ->
  case rocksdb:open(Dir, DbOpts) of
    {ok, Ref} ->
      {ok, Ref};
    %% Check specifically for lock error, this can be caused if
    %% a crashed instance takes some time to flush leveldb information
    %% out to disk.  The process is gone, but the NIF resource cleanup
    %% may not have completed.
    {error, {db_open, OpenErr}=Reason} ->
      case lists:prefix("IO error: lock ", OpenErr) of
        true ->
          SleepFor = application:get_env(barrel, db_open_retry_delay, ?DB_OPEN_RETRY_DELAY),
          ?LOG_WARNING(
             "~s: barrel rocksdb backend retrying ~p in ~p ms after error ~s\n",
             [?MODULE, Dir, SleepFor, OpenErr]
            ),
          timer:sleep(SleepFor),
          open_db(Dir, DbOpts, RetriesLeft - 1, Reason);
        false ->
          {error, Reason}
      end;
    {error, _} = Error ->
      Error
  end.

default_db_options() ->
  [
    {create_if_missing, true},
    {create_missing_column_families, true},
    {max_open_files, 10000},
    {allow_concurrent_memtable_write, true},
    {enable_write_thread_adaptive_yield, true},

    %% Periodically sync both the WAL and SST writes to smooth out disk
    %% usage. Not performing such syncs can be faster but can cause
    %% performance blips when the OS decides it needs to flush data.
    %{wal_bytes_per_sync, 512 bsl 10},  %% 512 KB
    %{bytes_per_sync, 512 bsl 10}, %% 512 KB,

    %% Because we open a long running rocksdb instance, we do not want the
    %% manifest file to grow unbounded. Assuming each manifest entry is about 1
    %% KB, this allows for 128 K entries. This could account for several hours to
    %% few months of runtime without rolling based on the workload.
    {max_manifest_file_size, 128 bsl 20} %% 128 MB,
  ].

cf_options(false) ->
  default_cf_options();
cf_options(CacheRef) ->
  BlockOptions = [{block_cache, CacheRef},
                  {cache_index_and_filter_blocks, true},
                  {partition_filters, true},
                  {cache_index_and_filter_blocks_with_high_priority, true}],
  default_cf_options() ++ [{block_based_table_options, BlockOptions}].

default_cf_options() ->
  WriteBufferSize =  barrel_config:get(rocksdb_write_buffer_size),
  [
    {write_buffer_size, WriteBufferSize}, %% 64MB
    {max_write_buffer_number, 4},
    {min_write_buffer_number_to_merge, 1},
    {level0_file_num_compaction_trigger, 2},
    {level0_slowdown_writes_trigger, 20},
    {level0_stop_writes_trigger, 32},

    {min_write_buffer_number_to_merge, 1},

    %%       level-size  file-size  max-files
    %% L1:      64 MB       4 MB         16
    %% L2:     640 MB       8 MB         80
    %% L3:    6.25 GB      16 MB        400
    %% L4:    62.5 GB      32 MB       2000
    %% L5:     625 GB      64 MB      10000
    %% L6:     6.1 TB     128 MB      50000
    %%
    {max_bytes_for_level_base, 64 bsl 20},

    {max_bytes_for_level_multiplier, 10},
    {target_file_size_base, 4 bsl 20}, %% 4MB
    {target_file_size_multiplier, 2},
    {compression, snappy},
    %{prefix_extractor, {fixed_prefix_transform, 10}},
    {merge_operator, counter_merge_operator},
    %% Disable subcompactions since they're a less stable feature, and not
    %% necessary for our workload, where frequent fsyncs naturally prevent
    %% foreground writes from getting too far ahead of compactions.
    {max_subcompactions, 1},
    %% Increase parallelism for compactions and flushes based on the
    %% number of cpus. Always use at least 2 threads, otherwise
    %% compactions and flushes may fight with each other.
    {total_threads, erlang:max(2, erlang:system_info(schedulers))}
  ].

%% -------------------
%% internals


db_get_int(Key, Default, ReadOptions) ->
  case rdb_get(?db, Key, ReadOptions) of
    {ok, Val} -> {ok, binary_to_integer(Val)};
    not_found -> {ok, Default};
    Error -> Error
  end.
