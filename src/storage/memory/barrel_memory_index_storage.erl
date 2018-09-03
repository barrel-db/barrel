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
  fold/7
]).

-include_lib("stdlib/include/ms_transform.hrl").

-define(KEYS, barrel_memory_storage_idx_keys).
-define(VERSIONS, barrel_memory_storage_idx_versions).

-record(ikey, {key :: term(),
               ts :: atom() | non_neg_integer(),
               type :: atom() }).



new() ->
 ets:new(?KEYS, [ordered_set, public, {read_concurrency, true}, {write_concurrency, true}]).


add_mutations(Mutations, Index, CommitTs) ->
  Batch =
    lists:foldl(
      fun(Mutation, AddKeys) ->
        case Mutation of
          {add, Path, DocId} ->
            FwdKey = {Path, DocId},
            IKey = #ikey{key=FwdKey, ts=CommitTs, type=put},
            [{IKey, undefined} | AddKeys];
          {delete, Path, DocId} ->
            FwdKey = {Path, DocId},
            IKey = #ikey{key=FwdKey, ts=CommitTs, type=delete},
           [{IKey, undefined} | AddKeys]
        end
      end,
      [],
      Mutations
    ),
  ets:insert(Index, Batch),
  ok.

fold(Path, Fun, Acc, Options, Db, Index, ReadTs) ->
  {Dir, Limit} = limit(Options),
  OrderBy = maps:get(order_by, Options, order_by_id),
  MS = pattern(OrderBy, Path, ReadTs, Options#{ dir => Dir }),
  {First, SkipFun} = case Dir of
                       fwd ->
                         {
                           ets:select(Index, MS, 1),
                           fun skip/1
                         };
                       rev ->
                         {
                           ets:select_reverse(Index, MS, 1),
                           fun skip_reverse/1
                         }
                     end,
  traverse(First, SkipFun, Fun, Acc, Db, Index, ReadTs, Limit).
  
  
traverse(_, _, _, Acc, _, _,  _, 0) -> Acc;
traverse('$end_of_table', _, _, Acc, _, _, _, _) -> Acc;
traverse(Found0, Skip, Fun, Acc, Db, Index, ReadTs, Limit ) ->
  case Skip(Found0) of
    {'$end_of_table', undefined} ->
      Acc;
    {Next, {#ikey{key=Key}, _}} ->
      {_, DocId} = Key,
      case barrel_memory_storage:fetch({docs, DocId}, Db, ReadTs) of
        {ok, DI} ->
          case Fun(DocId, DI, Acc) of
            {ok, Acc1} ->
              traverse(Next, Skip, Fun, Acc1, Db, Index, ReadTs, Limit -1);
            {stop, Acc1} ->
              Acc1;
            {skip, Acc1} ->
              traverse(Next, Skip, Fun, Acc1, Db, Index, ReadTs, Limit);
            ok ->
              traverse(Next, Skip, Fun, Acc, Db, Index, ReadTs, Limit -1);
            skip ->
              traverse(Next, Skip, Fun, Acc, Db, Index, ReadTs, Limit);
            stop ->
              Acc
          end;
        not_found ->
          %% race condition ?
          traverse(Next, Skip, Fun, Acc, Db, Index, ReadTs, Limit)
      end
      
  end.


skip({[{#ikey{key=Key}, _}=Current], Cont}) ->
  skip(ets:select(Cont), Key, Current).

skip('$end_of_table', _Key, {IKey, _}=Previous) ->
  case IKey#ikey.type of
    delete ->
      {'$end_of_table', undefined};
    put ->
      {'$end_of_table', Previous}
  end;
skip({[{#ikey{key=Key}, _}=Current], Cont}, Key, _Previous) ->
  skip(ets:select(Cont), Key, Current);

skip({[{#ikey{key=Key}, _}=Current], Cont}=Next, _OldKey, {IKey, _}=Previous) ->
  case IKey#ikey.type of
    delete ->
      skip(ets:select(Cont), Key, Current);
    put ->
      {Next, Previous}
  end.

skip_reverse({[{#ikey{key=Key, type=put}, _}=Found], Cont}) ->
  skip_reverse(ets:select_reverse(Cont), Key, Found);
skip_reverse({[{#ikey{key=Key, type=delete}, _}], Cont}) ->
  skip_reverse(ets:select_reverse(Cont), Key, undefined).


skip_reverse({[{#ikey{key=Key}, _}], Cont}, Key, Found) ->
  skip_reverse(ets:select_reverse(Cont), Key, Found);
skip_reverse({[{#ikey{key=Key, type=delete}, _}], Cont}, _Key, undefined) ->
  skip_reverse(ets:select_reverse(Cont), Key, undefined);
skip_reverse({[{#ikey{key=Key}, _}=Found], Cont}, _Key, undefined) ->
  skip_reverse(ets:select_reverse(Cont), Key, Found);
skip_reverse(Next, _Key, Found) ->
  {Next, Found}.
  
  
  limit(#{ limit_to_first := L }) -> {fwd, L};
limit(#{ limit_to_last := L }) -> {rev, L};
limit(_) -> {fwd, 1 bsl 64 - 1}.


pattern(order_by_id, Path, ReadTs, Options) ->
  {KeyPattern, Rule} = case start_key(Options, end_key(Options, [])) of
                 [] ->
                   P = #ikey{key={Path, '_'}, ts='$1', type='_'},
                   R = [{'=<', '$1', ReadTs}],
                   {P, R};
                 R0 ->
                   P = #ikey{key={Path, '$1'}, ts='$2', type='_'},
                   R1 = R0 ++ [{'=<', '$2', ReadTs}],
                   {P, R1}
               end,
  [{{KeyPattern, '_'}, Rule, ['$_']}];
pattern(order_by_key, Path, ReadTs, Options) ->
  EqualTo = maps:get(equal_to, Options, false),
  Rule0 = start_key(Options, end_key(Options, [])),
  {KeyPattern, Rule} = case {Rule0, EqualTo} of
                         {[], false} ->
                           P = #ikey{key={Path, '_'}, ts='$1', type='_'},
                           R = [{'=<', '$1', ReadTs}],
                           {P, R};
                         {[], _} ->
                           P = #ikey{key={Path ++ [EqualTo], '_'}, ts='$1', type='_'},
                           R = [{'=<', '$1', ReadTs}],
                           {P, R};
                         {_, false} ->
                           P = #ikey{key={Path ++ ['$1'], '_'}, ts='$2', type='_'},
                           R1 = Rule0 ++ [{'=<', '$2', ReadTs}],
                           {P, R1};
                         {_, _} ->
                           P = #ikey{key={Path ++ [EqualTo, '$1'], '_'}, ts='$2', type='_'},
                           R1 = Rule0 ++ [{'=<', '$2', ReadTs}],
                           {P, R1}
                       end,
  [{{KeyPattern, '_'}, Rule, ['$_']}];
pattern(order_by_value, _Path, _ReadTs, _Options) ->
  erlang:error(not_implemented).

  
start_key(Options, MS) -> start_key(Options, '$1', MS).

start_key(#{ start_at := Start }, M, MS) -> [{'>=', M, {const, Start}} | MS];
start_key(#{ next_to := Start }, M, MS) -> [{'>', M, {const, Start}} | MS];
start_key(_, _, MS) -> MS.


end_key(Options, MS) -> end_key(Options, '$1', MS).

end_key(#{ end_at := End }, M, MS) -> [{'=<', M, {const, End}} | MS];
end_key(#{ previous_to := End }, M, MS) -> [{'<', M, {const, End}} | MS];
end_key(_, _, MS) -> MS.