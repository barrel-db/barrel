%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 04. Jul 2018 22:18
%%%-------------------------------------------------------------------
-module(barrel_memory_storage).
-author("benoitc").

%% API
-export([
  init/2,
  terminate/2,
  handle_call/3,
  handle_cast/2
]).

-export([
  init_barrel/3,
  terminate_barrel/1,
  drop_barrel/1,
  close_barrel/1,
  list_barrels/1
]).

-export([
  write_docs_infos/4,
  write_revision/4,
  get_doc_infos/2,
  get_revision/3,
  docs_count/1,
  del_docs_count/1,
  updated_seq/1,
  commit/1,
  fold_docs/4,
  fold_changes/4,
  indexed_seq/1,
  set_indexed_seq/2,
  get_local_doc/2,
  resource_id/1
]).

-include_lib("stdlib/include/ms_transform.hrl").

-define(DB, barrel_ets_db).
-define(IDX, barrel_ets_idx).

-record(ikey, {key :: term(),
               seqno :: non_neg_integer(),
               type :: put | delete }).



init_barrel(Store, Name, Options = #{ create := true }) ->
  Db = new_tab(?DB),
  Idx = case maps:get(index_strategy, Options, consistent) of
          none -> undefined;
          _ -> new_tab(?IDX)
        end,
  Barrel =
    #{name => Name,
      store => Store,
      db => Db,
      idx => Idx,
      resource_id => 0,
      updated_seq => 0,
      indexed_seq => 0,
      docs_count => 0,
      del_docs_count => 0,
      commit_ts => 1,
      read_ts => 0},

  ok = barrel_store_provider:call(Store, {reg_barrel, Name, self()}),

  {ok, Barrel};
init_barrel(_, _, _) ->
  {error, not_found}.

terminate_barrel(#{ name := Name, store := Store }) ->
  barrel_store_provider:call(Store, {unreg_barrel, Name}).

drop_barrel(Barrel) ->
  terminate_barrel(Barrel).

close_barrel(_Barrel) ->
  ok.


list_barrels(#{ store := Store }) ->
  barrel_store_provider:call(Store, list_barrels).



new_tab(Name) ->
  ets:new(
    Name,
    [ordered_set, public, {read_concurrency, true}, {write_concurrency, true}]
  ).

docs_count(#{ docs_count := Count }) -> Count.

del_docs_count(#{ del_docs_count := Count }) -> Count.

updated_seq(#{ updated_seq := Seq }) -> Seq.

indexed_seq(#{ indexed_seq := Seq }) -> Seq.

set_indexed_seq(Seq, State) -> {ok, State#{ indexed_seq => Seq }}.

resource_id(#{ resource_id := RID }) -> RID.

get_local_doc(DocId, State) ->
  fetch({local, DocId}, State).


get_doc_infos(DocId, State) ->
  fetch({docs, DocId}, State).

get_revision(DocId, Rev, State) ->
  fetch({rev, DocId, Rev}, State).

%% TODO: we could have one ETS table / store.
write_revision(DocId, Rev, Body, #{ db := Db, commit_ts := Ts }) ->
  RevKey = #ikey{ key = {rev, DocId, Rev}, seqno=Ts, type=put },
  ets:insert(Db, {RevKey, Body}),
  ok.

write_docs_infos(DIPairs, LocalRecords, PurgedIdRevs, #{ db := Db, commit_ts := Ts } = State) ->
  #{ db := Db, commit_ts := Ts, docs_count := DocsCount0, del_docs_count := DelDocsCount0 } = State,
  {Batch0, Seqs, Rids, DocsCount, DelDocsCount} = lists:foldl(
    fun({NewDI, OldDI}, {Acc, AccSeq, AccRid, Count, DelCount}) ->
      case {NewDI, OldDI} of
        {#{ id := Id, seq := Seq, rid := RID  }=DI, not_found} ->
          RIDKey = #ikey{ key = {rid, RID}, seqno=Ts, type=put},
          IdKey = #ikey{ key = {docs, Id}, seqno=Ts, type=put},
          SeqKey = #ikey{ key = {seq, Seq}, seqno=Ts, type=put},
          {[{IdKey, DI}, {SeqKey, DI}, {RIDKey, Id} | Acc], [Seq | AccSeq], [RID | AccRid], Count + 1, DelCount};
        {#{ id := Id, seq := Seq }=DI, #{ seq := OldSeq }} ->
          IdKey = #ikey{ key = {docs, Id}, seqno=Ts, type=put},
          SeqKey = #ikey{ key = {seq, Seq}, seqno=Ts, type=put},
          RemSeqKey = #ikey{ key = {seq, OldSeq}, seqno=Ts, type=delete},
          {Count1, DelCount1}= count(NewDI, OldDI, Count, DelCount),
          {[{IdKey, DI}, {SeqKey, DI}, {RemSeqKey, undefined} | Acc], [Seq | AccSeq], AccRid, Count1, DelCount1};
        {not_found, #{ id := Id, seq := Seq, rid := RID }} ->
          RIDKey = #ikey{ key = {rid, RID}, seqno=Ts, type=delete},
          IdKey = #ikey{ key = {docs, Id}, seqno=Ts, type=delete},
          SeqKey = #ikey{ key = {seq, Seq}, seqno=Ts, type=delete},
          {Count1, DelCount1} = count(NewDI, OldDI, Count, DelCount),
          {[{IdKey, undefined}, {SeqKey, undefined}, {RIDKey, undefined} | Acc], AccSeq, AccRid, Count1, DelCount1}
      end
    end,
    {[], [], [], DocsCount0, DelDocsCount0},
    DIPairs
  ),
  Batch = lists:foldl(
            fun({Id, Rev}, Acc) ->
                RevKey = #ikey{ key = {rev, Id, Rev}, seqno=Ts, type=delete },
                [{RevKey, undefined} | Acc]
            end,
            Batch0,
            PurgedIdRevs
           ),
  FinalBatch = lists:foldl(
    fun(Record, Batch1) ->
      #{ id := Id, del := Del} = Record,
      {Type, Val} = case Del of
                      true -> {delete, undefined};
                      false -> {put, Record}
                    end,
      LocalKey = #ikey{ key = {local, Id}, seqno = Ts, type = Type },
      [{LocalKey, Val} | Batch1]
    end,
    Batch,
    LocalRecords
  ),
  ets:insert(Db, FinalBatch),
  NewState = State#{updated_seq => lmax(Seqs),
                    resource_id => lmax(Rids),
                    docs_count => DocsCount,
                    del_docs_count => DelDocsCount },
  {ok, NewState}.


lmax([]) -> 0;
lmax(L) -> lists:max(L).

commit(#{ commit_ts := Ts } = State) ->
  State#{ read_ts => Ts, commit_ts => Ts + 1 }.


count(#{ deleted := true }, #{ deleted := false }, Count, DelCount) -> {Count - 1, DelCount + 1};
count(#{ deleted := false }, #{ deleted := true }, Count, DelCount) -> {Count + 1, DelCount -1};
count(not_found, #{ deleted := true },  Count, DelCount) -> {Count, DelCount -1};
count(not_found, #{ deleted := false }, Count, DelCount) -> {Count -1, DelCount};
count(_, _, Count, DelCount) -> {Count, DelCount}.


fold_docs(UserFun, UserAcc, Options, #{ db := Db } = State) ->
  {Dir, Limit} = limit(Options),
  MS = fold_db_ms(docs, Options#{dir => Dir}, State),
  WrapperFun = fun
                 (Key, [{put, Val}], Acc) -> UserFun(Key, Val, Acc);
                 (_Key, [{delete, _}], _) -> skip
               end,
  case Dir of
    fwd ->
      traverse(ets:select(Db, MS, 1), undefined, [], WrapperFun, UserAcc, Limit);
    rev ->
      traverse_reverse(ets:select_reverse(Db, MS, 1), undefined, [], WrapperFun, UserAcc, Limit)
  end.


fold_changes(Since, UserFun, UserAcc, #{ db := Db } = State) ->
  MS = fold_db_ms(seq, #{dir => fwd, next_to => Since}, State),
  WrapperFun = fun
                 (Key, [{put, Val}], Acc) -> UserFun(Key, Val, Acc);
                 (_Key, [{delete, _}], _) -> skip
               end,
  traverse(ets:select(Db, MS, 1), undefined, [], WrapperFun, UserAcc, 1 bsl 64 - 1).


%% MVCC helpers

fetch(Key, #{ db := Db, read_ts := TS }=_State) ->
  MS = ets:fun2ms(fun({#ikey{key=K, seqno=S }, _}=KV) when K =:= Key, S =< TS -> KV end),
  case ets:select_reverse(Db, MS, 1) of
    '$end_of_table' ->
      not_found;
    {[{#ikey{type=delete}, _}], _} -> not_found;
    {[{_, Val}], _} -> {ok, Val}
  end.

traverse(_, _, _, _, Acc, 0) ->
  Acc;
traverse('$end_of_table', undefined, _, _, Acc, _) ->
  Acc;
traverse('$end_of_table', Key, KeyAcc, Fun, Acc, _Limit) ->
  case Fun(Key, merge_ops(KeyAcc, []), Acc) of
    {ok, Acc1} -> Acc1;
    {stop, Acc1} -> Acc1;
    {skip, Acc1} -> Acc1;
    stop -> Acc;
    ok -> Acc;
    skip -> Acc
  end;
traverse({[{{Key, _}, _}=KV], Cont}, undefined, KeyAcc, Fun, Acc, Limit) ->
  traverse(ets:select(Cont), Key, [KV|KeyAcc], Fun, Acc, Limit);
traverse({[{{Key, _}, _}=KV], Cont}, Key, KeyAcc, Fun, Acc, Limit) ->
  traverse(ets:select(Cont), Key, [KV|KeyAcc], Fun, Acc, Limit);
traverse({[{{NextKey, _}, _}=KV], Cont}, Key, KeyAcc, Fun, Acc, Limit) ->
  case Fun(Key, merge_ops(KeyAcc, []), Acc) of
    {ok, Acc1} ->
      traverse(ets:select(Cont), NextKey, [KV], Fun, Acc1, Limit - 1);
    {stop, Acc1} ->
      Acc1;
    {skip, Acc1} ->
      traverse(ets:select(Cont), NextKey, [KV], Fun, Acc1, Limit);
    stop ->
      Acc;
    ok ->
      traverse(ets:select(Cont), NextKey, [KV], Fun, Acc, Limit - 1);
    skip ->
      traverse(ets:select(Cont), NextKey, [KV], Fun, Acc, Limit)
  end.

traverse_reverse(_, _, _, _, Acc, 0) ->
  Acc;
traverse_reverse('$end_of_table', undefined, _, _, Acc, _) ->
  Acc;
traverse_reverse('$end_of_table', Key, KeyAcc, Fun, Acc, _) ->
  case Fun(Key, merge_ops(lists:reverse(KeyAcc), []), Acc) of
    {ok, Acc1} -> Acc1;
    {stop, Acc1} -> Acc1;
    {skip, Acc1} -> Acc1;
    stop -> Acc;
    ok -> Acc;
    skip -> Acc
  end;
traverse_reverse({[{{Key, _Type},_Val}=KV], Cont}, Key, KeyAcc, Fun, Acc, Limit) ->
  traverse_reverse(ets:select(Cont), Key, [KV | KeyAcc], Fun, Acc, Limit);
traverse_reverse({[{{NextKey, _Type}, _Val}=KV], Cont}, Key, KeyAcc, Fun, Acc, Limit) ->
  case Fun(Key, merge_ops(lists:reverse(KeyAcc), []), Acc) of
    {ok, Acc1} ->
      traverse_reverse(ets:select(Cont), NextKey, [KV], Fun, Acc1, Limit - 1);
    {stop, Acc1} ->
      Acc1;
    {skip, Acc1} ->
      traverse_reverse(ets:select(Cont), NextKey, [KV], Fun, Acc1, Limit);
    stop ->
      Acc;
    ok ->
      traverse_reverse(ets:select(Cont), NextKey, [KV], Fun, Acc, Limit - 1);
    skip ->
      traverse_reverse(ets:select(Cont), NextKey, [KV], Fun, Acc, Limit)
  end.


merge_ops([{{_Key, merge}, OP} | Rest], OPs) ->
  merge_ops(Rest, [{merge, OP} | OPs]);
merge_ops([{{_Key, Type}, OP} | _], OPs) ->
  [{Type, OP} | OPs];
merge_ops([], OPs) ->
  OPs.


limit(#{ limit_to_first := L }) -> {rev, L};
limit(#{ limit_to_last := L }) -> {fwd, L};
limit(_) -> {fwd, 1 bsl 64 - 1}.

fold_db_ms(Ident, Options, #{ read_ts := Ts }) ->
  Rule = start_key(Options,
                  end_key(Options,
                          [{'=<', '$2', {const, Ts}}])
                 ),
  [{{#ikey{key={Ident, '$1'}, seqno='$2', type='$3'}, '$4'}, Rule, [{{{{'$1', '$3'}}, '$4'}}]}].


start_key(#{ start_at := Start, dir := fwd }, MS) -> [{'>=', '$1', {const, Start}} | MS];
start_key(#{ next_to := Start, dir := fwd }, MS) -> [{'>', '$1', {const, Start}} | MS];
start_key(#{ start_at := Start, dir := rev }, MS) -> [{'=<', '$1', {const, Start}} | MS];
start_key(#{ next_to := Start, dir := rev }, MS) -> [{'<', '$1', {const, Start}} | MS];
start_key(_, MS) -> MS.

end_key(#{ end_at := End, dir := fwd }, MS) -> [{'=<', '$1', {const, End}} | MS];
end_key(#{ previous_to := End, dir := fwd }, MS) -> [{'<', '$1', {const, End}} | MS];
end_key(#{ end_at := End, dir := rev }, MS) -> [{'>=', '$1', {const, End}} | MS];
end_key(#{ previous_to := End, dir := rev }, MS) -> [{'>', '$1', {const, End}} | MS];
end_key(_, MS) -> MS.


%%% ==
%%% provider API

init(_StoreName, _Options) ->
  {ok, #{}}.

handle_call({reg_barrel, Name, Pid}, _From, Barrels) ->
  {reply, ok, Barrels#{ Name => Pid }};

handle_call({unreg_barrel, Name}, _From, Barrels) ->
  Barrels2 = case maps:is_key(Name, Barrels) of
               true -> maps:remove(Name, Barrels);
               false -> Barrels
             end,
  {reply, ok, Barrels2};

handle_call(list_barrels, _From, Barrels) ->
  {reply, maps:keys(Barrels), Barrels};

handle_call(_Msg, _From, Barrels) ->
  {reply, bad_call, Barrels}.


handle_cast(_Msg, Barrels) ->
  {noreply, Barrels}.


terminate(_, Barrels) ->
  _ = maps:fold(
    fun(_Name, Pid, _) ->
      barrel_db:close_barrel(Pid)
    end,
    ok,
    Barrels
  ),

  ok.
