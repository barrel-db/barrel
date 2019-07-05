%% Copyright (c) 2016-2018, Benoit Chesneau
%%
%% Licensed under the Apache License, Version 2.0 (the "License"); you may not
%% use this file except in compliance with the License. You may obtain a copy of
%% the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
%% License for the specific language governing permissions and limitations under
%% the License.

%% TODO: we should store in the tree a parsed revision instead of the whole string

-module(barrel_revtree).

-export([
    new/0, new/1,
    add/2,
    contains/2,
    info/2,
    revisions/1,
    parent/2,
    history/2, history/3,
    fold_leafs/3,
    leaves/1,
    is_leaf/2,
    missing_revs/2,
    conflicts/1,
    winning_revision/1,
    prune/2, prune/3
]).

-export([is_deleted/1]).

-define(IMAX1, 16#ffffFFFFffffFFFF).

-define(DEFAULT_MAX_HISTORY, 200).

new() -> #{}.

new(#{ id := Id } = RevInfo) -> #{ Id => RevInfo }.

add(RevInfo, Tree) ->
  #{id := Id, parent := Parent} = RevInfo,
  case maps:is_key(Id, Tree) of
    true -> exit({badrev, already_exists});
    false -> ok
  end,
  case maps:is_key(Parent, Tree) of
    true -> ok;
    false when Parent =:= <<"">> -> ok;
    false -> exit({badrev, missing_parent})
  end,
  Tree#{ Id => RevInfo }.

contains(RevId, Tree) ->
  maps:is_key(RevId, Tree).

info(RevId, Tree) ->
  case maps:find(RevId, Tree) of
    {ok, RevInfo} -> {ok, RevInfo};
    error -> {error, not_found}
  end.


revisions(Tree) -> maps:keys(Tree).

parent(RevId, Tree) ->
  case maps:find(RevId, Tree) of
    {ok, RevInfo} -> maps:get(parent, RevInfo, <<"">>);
    error -> <<"">>
  end.

history(RevId, Tree) ->
  history(RevId, Tree, ?DEFAULT_MAX_HISTORY).

history(RevId, Tree, Max) ->
  history1(maps:get(RevId, Tree, nil), Tree, [], Max).

history1(nil, _Tree, History, _Max) ->
  lists:reverse(History);
history1(#{id := Id, parent := Parent}, Tree, History, Max) ->
  NewHistory = [Id | History],
  Len = length(NewHistory),
  if
    Len >= Max ->
      lists:reverse(NewHistory);
    true ->
      history1(maps:get(Parent, Tree, nil), Tree, NewHistory, Max)
  end;
history1(#{id := Id}, _Tree, History, _Max) ->
  lists:reverse([Id | History]).

fold_leafs(Fun, AccIn, Tree) ->
  Parents = maps:fold(fun
                        (_Id, #{ parent := Parent }, Acc) when Parent /= <<"">> ->
                          [Parent | Acc];
                        (_Id, _RevInfo, Acc) ->
                          Acc
                      end, [], Tree),
  LeafsMap = maps:without(Parents, Tree),
  maps:fold(fun(_RevId, RevInfo, Acc) -> Fun(RevInfo, Acc) end, AccIn, LeafsMap).

leaves(Tree) ->
  fold_leafs(fun(#{id := RevId}, Acc) -> [ RevId | Acc ] end, [], Tree).

missing_revs(Revs, RevTree) ->
  Leaves = barrel_revtree:fold_leafs(
    fun(#{ id := Id}, Acc) ->
      case lists:member(Id, Revs) of
        true -> [Id | Acc];
        false -> Acc
      end
    end, [], RevTree),
  Revs -- Leaves.

is_leaf(RevId, Tree) ->
  case maps:is_key(RevId, Tree) of
    true ->

      is_leaf_1(maps:values(Tree), RevId);
    false ->
      false
  end.

is_leaf_1([#{ parent := RevId } | _], RevId) -> false;
is_leaf_1([_ | Rest], RevId) -> is_leaf_1(Rest, RevId);
is_leaf_1([], _) -> true.

is_deleted(#{deleted := Del}) -> Del;
is_deleted(_) -> false.

conflicts(Tree) ->
  Leaves = fold_leafs(
    fun(RevInfo, Acc) ->
      Deleted = is_deleted(RevInfo),
      [RevInfo#{ deleted => Deleted }| Acc]
    end, [], Tree),
  SortedRevInfos = lists:sort(
                     fun(#{ id := RevIdA, deleted := DeletedA }, #{ id := RevIdB, deleted := DeletedB }) ->
                         % sort descending by {not deleted, rev}
                         RevA = barrel_doc:parse_revision(RevIdA),
                         RevB = barrel_doc:parse_revision(RevIdB),
                         {not DeletedA, RevA} > {not DeletedB, RevB}
                     end,
                     Leaves
                    ),
  SortedRevInfos.


winning_revision(Tree) ->
  {Leaves, ActiveCount} = fold_leafs(
    fun(RevInfo, {Acc, ActiveCount1}) ->
      Deleted = is_deleted(RevInfo),
      ActiveCount2 = case Deleted of
                       true -> ActiveCount1;
                       false -> ActiveCount1 + 1
                     end,
      {[RevInfo#{ deleted => Deleted }| Acc], ActiveCount2 }
    end, {[], 0}, Tree),

  SortedRevInfos = lists:sort(
    fun(#{ id := RevIdA, deleted := DeletedA }, #{ id := RevIdB, deleted := DeletedB }) ->
      % sort descending by {not deleted, rev}
      RevA = barrel_doc:parse_revision(RevIdA),
      RevB = barrel_doc:parse_revision(RevIdB),
      {not DeletedA, RevA} > {not DeletedB, RevB}
    end,
    Leaves
  ),
  [#{ id := WinningRev} | _] = SortedRevInfos,
  Branched = length(Leaves) > 0,
  Conflict = ActiveCount > 1,
  {WinningRev, Branched, Conflict}.


prune(Depth, Tree) ->
  prune(Depth, <<"">>, Tree).

prune(Depth, KeepRev, Tree) ->
  Sz = maps:size(Tree),
  if
    Sz =< Depth -> {0, Tree};
    true -> do_prune(Depth, KeepRev, Tree)
  end.


do_prune(Depth, KeepRev, Tree) ->
  {MinPos0, MaxDeletedPos} = fold_leafs(fun(#{id := RevId}=RevInfo, {MP, MDP}) ->
    Deleted = is_deleted(RevInfo),
    {Pos, _} = barrel_doc:parse_revision(RevId),
    case Deleted of
      true when Pos > MDP ->
        {MP, Pos};
      _ when Pos > 0, Pos < MP ->
        {Pos, MDP};
      _ ->
        {MP, MDP}
    end
                                        end, {?IMAX1, 0}, Tree),
  MinPos = if
             MinPos0 =:= ?IMAX1 -> MaxDeletedPos;
             true -> MinPos0
           end,
  MinPosToKeep0 = MinPos - Depth + 1,
  {PosToKeep, _} = barrel_doc:parse_revision(KeepRev),
  MinPosToKeep = if
                   PosToKeep > 0,  PosToKeep < MinPosToKeep0 -> PosToKeep;
                   true -> MinPosToKeep0
                 end,
  if
    MinPosToKeep > 1 ->
      maps:fold(fun(RevId, RevInfo, {N, NewTree}) ->
        {Pos, _} = barrel_doc:parse_revision(RevId),
        if
          Pos < MinPosToKeep ->
            {N + 1, maps:remove(RevId, NewTree)};
          Pos =:= MinPosToKeep ->
            RevInfo2 = RevInfo#{parent => <<"">>},
            {N, NewTree#{RevId => RevInfo2}};
          true ->
            {N, NewTree}
        end
                end, {0, Tree}, Tree);
    true ->
      {0, Tree}
  end.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% 1-one -> 2-two -> 3-three
-define(FLAT_TREE, #{<<"1-one">> => #{ id => <<"1-one">> },
                     <<"2-two">> => #{ id => <<"2-two">>, parent => <<"1-one">>},
                     <<"3-three">> => #{ id => <<"3-three">>, parent => <<"2-two">>}}).

%%                  3-three
%%                /
%% 1-one -> 2-two
%%                \
%%                  3-three-2
-define(BRANCHED_TREE, #{<<"1-one">> => #{ id => <<"1-one">> },
                         <<"2-two">> => #{ id => <<"2-two">>, parent => <<"1-one">> },
                         <<"3-three">> => #{ id => <<"3-three">>, parent => <<"2-two">> },
                         <<"3-three-2">> => #{ id => <<"3-three-2">>, parent => <<"2-two">> }}).

contains_test() ->
  ?assert(barrel_revtree:contains(<<"2-two">>, ?FLAT_TREE)).


parent_test() ->
  ?assertEqual(<<"">>, barrel_revtree:parent(<<"1-one">>, ?FLAT_TREE)),
  ?assertEqual(<<"1-one">>, barrel_revtree:parent(<<"2-two">>, ?FLAT_TREE)),
  ?assertEqual(<<"2-two">>, barrel_revtree:parent(<<"3-three">>, ?FLAT_TREE)),
  ?assertEqual(<<"2-two">>, barrel_revtree:parent(<<"3-three">>, ?BRANCHED_TREE)),
  ?assertEqual(<<"2-two">>, barrel_revtree:parent(<<"3-three-2">>, ?BRANCHED_TREE)),
  ok.

add_test() ->
  NewRev = #{ id => <<"4-four">>, parent => <<"3-three">> },
  NewTree = barrel_revtree:add(NewRev, ?FLAT_TREE),

  ?assert(barrel_revtree:contains(<<"4-four">>, NewTree)),
  ?assertEqual(<<"3-three">>, barrel_revtree:parent(<<"4-four">>, NewTree)),
  ?assertExit({badrev, already_exists}, barrel_revtree:add(NewRev, NewTree)),
  ?assertExit({badrev, missing_parent},
               barrel_revtree:add(#{ id => <<"6-six">>, parent => <<"5-five">>}, NewTree)).

leafs_test() ->
  ?assertEqual([<<"3-three">>, <<"3-three-2">>], lists:sort(barrel_revtree:leaves(?BRANCHED_TREE))),
  NewTree = barrel_revtree:add(#{ id => <<"4-four">>, parent => <<"3-three">>},?BRANCHED_TREE),
  ?assertEqual([<<"3-three-2">>, <<"4-four">>], lists:sort(barrel_revtree:leaves(NewTree))),
  NewTree2 = barrel_revtree:add(#{ id => <<"5-five">>, parent => <<"4-four">>, deleted => true }, NewTree),
  ?assertEqual([<<"3-three-2">>, <<"5-five">>],  lists:sort(barrel_revtree:leaves(NewTree2))).

history_test() ->
  ?assertEqual([<<"3-three">>, <<"2-two">>, <<"1-one">>],  barrel_revtree:history(<<"3-three">>, ?FLAT_TREE)).

history_max_test() ->
  ?assertEqual([<<"3-three">>, <<"2-two">>>],  barrel_revtree:history(<<"3-three">>, ?FLAT_TREE, 2)).


winning_revision_test() ->
  ?assertEqual({<<"3-three-2">>, true, true}, barrel_revtree:winning_revision(?BRANCHED_TREE)),
  NewTree = barrel_revtree:add(#{ id => <<"4-four">>, parent => <<"3-three">>},?BRANCHED_TREE),
  ?assertEqual({<<"4-four">>, true, true}, barrel_revtree:winning_revision(NewTree)),
  NewTree2 = barrel_revtree:add(#{ id => <<"5-five">>, parent => <<"4-four">>, deleted => true }, NewTree),
  ?assertEqual({<<"3-three-2">>, true, false},  barrel_revtree:winning_revision(NewTree2)).

prune_test() ->
  Tree = barrel_revtree:add(#{ id => <<"4-four">>, parent => <<"3-three-2">> }, ?BRANCHED_TREE),
  {0, Tree} = barrel_revtree:prune(1000, <<"">>, Tree),
  {0, Tree} = barrel_revtree:prune(3, <<"">>, Tree),
  {1, Tree1} = barrel_revtree:prune(2, <<"">>, Tree),
  ?assertEqual(4, maps:size(Tree1)),
  ?assertEqual(false, barrel_revtree:contains(<<"1-one">>, Tree1)),
  ?assertEqual(<<"">>, maps:get(parent, maps:get(<<"2-two">>, Tree1))),

  %% make sure merged conflicts don't prevevent prunint
  {1, Tree2} = barrel_revtree:prune(1, "", Tree1),
  ?assertEqual(3, maps:size(Tree2)),
  ?assertEqual(true, barrel_revtree:contains(<<"3-three">>, Tree2)),
  ?assertEqual(<<"">>, maps:get(parent, maps:get(<<"3-three">>, Tree2))),
  ?assertEqual(true, barrel_revtree:contains(<<"4-four">>, Tree2)),
  ?assertEqual(<<"3-three-2">>, maps:get(parent, maps:get(<<"4-four">>, Tree2))),


  TreeB = lists:foldl(fun(Rev, T) ->
    barrel_revtree:add(Rev, T)
                      end,
                      ?BRANCHED_TREE,
                      [#{ id => <<"4-four-2">>, parent => <<"3-three-2">>, deleted => true},
                       #{ id => <<"4-four">>, parent => <<"3-three">>},
                       #{ id => <<"5-five">>, parent => <<"4-four">> },
                       #{ id => <<"6-six">>, parent => <<"5-five">> }]),

  {0, TreeB} = barrel_revtree:prune(3, <<"1-one">>, TreeB),
  {1, TreeB1} = barrel_revtree:prune(3, <<"2-two">>, TreeB),
  {3, TreeB2} = barrel_revtree:prune(3, <<"">>, TreeB1),
  ?assertEqual(4, maps:size(TreeB2)),
  {2, TreeB3} = barrel_revtree:prune(2, <<"">>, TreeB2),
  ?assertEqual(<<"">>, maps:get(parent, maps:get(<<"5-five">>, TreeB3))),
  ?assertEqual(<<"5-five">>, maps:get(parent, maps:get(<<"6-six">>, TreeB3))),

  TreeC = maps:map(fun(RevId, RevInfo) ->
    case lists:member(RevId, [<<"3-three">>, <<"3-three-2">>]) of
      true ->
        RevInfo#{deleted => true};
      false ->
        RevInfo
    end
                   end, ?BRANCHED_TREE),

  {0, TreeC} = barrel_revtree:prune(3, <<"">>, TreeC),
  {1, _} = barrel_revtree:prune(2, <<"">>, TreeC),
  ok.

-endif.
