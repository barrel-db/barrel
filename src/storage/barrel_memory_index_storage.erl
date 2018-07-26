%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Jul 2018 11:35
%%%-------------------------------------------------------------------
-module(barrel_memory_index_storage).
-author("benoitc").

%% API
-export([
  new/0,
  add_mutations/3,
  mvcc_key/3,
  fold/7
]).

-include_lib("stdlib/include/ms_transform.hrl").

-define(KEYS, barrel_memory_storage_idx_keys).
-define(VERSIONS, barrel_memory_storage_idx_versions).

-record(idx, {keys,
              versions}).

-record(ikey, {key :: term(),
               ts :: non_neg_integer(),
               type :: atom() }).



new() ->
  Keys = ets:new(?KEYS, [ordered_set, public, {read_concurrency, true}, {write_concurrency, true}]),
  Versions = ets:new(?VERSIONS, [ordered_set, public, {read_concurrency, true}, {write_concurrency, true}]),
  #idx{keys=Keys, versions=Versions}.


add_mutations(Mutations, Index, CommitTs) ->
  {BatchKeys, BatchValues} =
    lists:foldl(
      fun(Mutation, {AddKeys, AddValues}) ->
        case Mutation of
          {add, Path, DocId} ->
            FwdKey = {Path, DocId},
            Hash = erlang:phash2(FwdKey),
            AddKeys2 = [{FwdKey, Hash} | AddKeys],
            IKey = #ikey{key=erlang:phash2(FwdKey), ts=CommitTs, type=put},
            AddValues2 = [{IKey, undefined} | AddValues],
            {AddKeys2, AddValues2};
          {delete, Path, DocId} ->
            FwdKey = {Path, DocId},
            IKey = #ikey{key=erlang:phash2(FwdKey), ts=CommitTs, type=delete},
            AddValues2 = [{IKey, undefined} | AddValues],
            {AddKeys, AddValues2}
        end
      end,
      {[], []},
      Mutations
    ),
  ets:insert(Index#idx.keys, BatchKeys),
  ets:insert(Index#idx.versions, BatchValues),
  ok.

fold(Path, Fun, Acc, Options, Db, Index, ReadTs) ->
  {Dir, Limit} = limit(Options),
  OrderBy = maps:get(order_by, Options, order_by_id),
  MS = pattern(OrderBy, Path, Options#{ dir => Dir }),
  {First, Next} = case Dir of
                    fwd ->
                      {
                        ets:select(Index#idx.keys, MS, 1),
                        fun(Cont) -> ets:select_reverse(Cont) end
                      };
                    rev ->
                      {
                        ets:select_reverse(Index#idx.keys, MS, 1),
                        fun(Cont) -> ets:select_reverse(Cont) end
                      }
                  end,
  traverse(First, Next, Fun, Acc, Db, Index, ReadTs, Limit).
  
  
traverse(_, _, _, Acc, _, _,  _, 0) -> Acc;
traverse('$end_of_table', _, _, Acc, _, _, _, _) -> Acc;
traverse({[{Key, Hash}], Cont}, Next, Fun, Acc, Db, Index, ReadTs, Limit ) ->
  {_, DocId} = Key,
  case mvcc_key(Hash, Index, ReadTs) of
    {ok, _} ->
      case barrel_memory_storage:fetch({docs, DocId}, Db, ReadTs) of
        {ok, DI} ->
          case Fun(DocId, DI, Acc) of
            {ok, Acc1} ->
              traverse(Next(Cont), Next, Fun, Acc1, Db, Index, ReadTs, Limit -1);
            {stop, Acc1} ->
              Acc1;
            {skip, Acc1} ->
              traverse(Next(Cont), Next, Fun, Acc1, Db, Index, ReadTs, Limit);
            ok ->
              traverse(Next(Cont), Next, Fun, Acc, Db, Index, ReadTs, Limit -1);
            skip ->
              traverse(Next(Cont), Next, Fun, Acc, Db, Index, ReadTs, Limit);
            stop ->
              Acc
          end;
        not_found ->
          %% race condition ?
          traverse(Next(Cont), Next, Fun, Acc, Db, Index, ReadTs, Limit)
      end;
    not_found ->
      traverse(Next(Cont), Next, Fun, Acc, Db, Index, ReadTs, Limit)
  end.



limit(#{ limit_to_first := L }) -> {fwd, L};
limit(#{ limit_to_last := L }) -> {rev, L};
limit(_) -> {fwd, 1 bsl 64 - 1}.


pattern(order_by_id, Path, Options) ->
  Rule = start_key(Options, end_key(Options, [])),
  KeyPattern = case Rule of
                 [] -> {Path, '_'};
                 _ -> {Path, '$1'}
               end,
  [{{KeyPattern, '_'}, Rule, ['$_']}];
pattern(order_by_key, Path, Options) ->
  EqualTo = maps:get(equal_to, Options, false),
  Rule = start_key(Options, end_key(Options, [])),
  
  KeyPattern = case {Rule, EqualTo} of
                 {[], false} ->
                   {Path , '_'};
                 {[], _} ->
                   {Path ++ [EqualTo], '_'};
                 {_, false} ->
                   {Path ++ ['$1'], '_'};
                 {_, _} ->
                   {Path ++ [EqualTo, '$1'], '_'}
               end,
  [{{KeyPattern, '_'}, Rule, ['$_']}];
pattern(order_by_value, _Path, _Options) ->
  erlang:error(not_implemented).

  
start_key(Options, MS) -> start_key(Options, '$1', MS).

start_key(#{ start_at := Start }, M, MS) -> [{'>=', M, {const, Start}} | MS];
start_key(#{ next_to := Start }, M, MS) -> [{'>', M, {const, Start}} | MS];
start_key(_, _, MS) -> MS.


end_key(Options, MS) -> end_key(Options, '$1', MS).

end_key(#{ end_at := End }, M, MS) -> [{'=<', M, {const, End}} | MS];
end_key(#{ previous_to := End }, M, MS) -> [{'<', M, {const, End}} | MS];
end_key(_, _, MS) -> MS.


mvcc_key(Key, Index, ReadTs) ->
  MS = ets:fun2ms(fun({#ikey{key=K, ts=Ts}, _Val}=KV) when K =:= Key, Ts =< ReadTs -> KV end),
  case ets:select(Index#idx.versions, MS, 1) of
    {[{#ikey{key=Key, type=put}=MVCCKey, _}], _} -> {ok, MVCCKey};
    _ -> not_found
  end.