%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. Jan 2018 11:27
%%%-------------------------------------------------------------------
-module(barrel_db_writer).
-author("benoitc").

%% API
-export([
  start_link/3
]).

-export([
  make_op/3,
  op_from/1,
  op_record/1,
  op_type/1
]).


-export([init/1]).

-include("barrel.hrl").

start_link(DbRef, Mod, DbState) ->
  Pid = spawn_link(?MODULE, init, [[self(), DbRef, Mod, DbState]]),
  {ok, Pid}.

init([DbPid, DbRef, Mod, DbState]) ->
  State = #{ db_pid => DbPid,
             db_ref => DbRef,
             db_mod => Mod,
             db_state => DbState,
             cache => [] },
  loop(State).

loop(State) ->
  receive
    {store, Entries} ->
      NewState = merge_revtrees(Entries, State),
      loop(NewState)
  end.

get_cached(Id, #{ cache := Cache }) ->
  lists:keyfind(Id, 1, Cache).

cache(#{ id := Id } = Doc, #{ cache := Cache0 } = State) ->
  Cache1 = lists:keystore(Id, 1, Cache0, {Id, Doc}),
  State#{ cache => Cache1 }.

remove_cached(Id, #{ cache := Cache0 } = State) ->
  Cache1 = lists:keydelete(Id, 1, Cache0),
  State#{ cache => Cache1 }.


make_op(Type, Record, From) ->
  #write_op{type=Type, doc=Record, from=From}.

op_from(#write_op{} = Op) -> Op#write_op.from.
op_record(#write_op{} = Op) -> Op#write_op.doc.
op_type(#write_op{} = Op) -> Op#write_op.type.

merge_fun(merge) ->
  fun merge_revtree/4;
merge_fun(merge_with_conflict) ->
  fun merge_revtree_with_conflict/4;
merge_fun(purge) ->
  fun purge/4.

new_docinfo(DocId) ->
  #{id => DocId,
    rev => <<>>,
    deleted => false,
    revtree => barrel_revtree:new()}.

merge_revtrees([Op | Rest], State) ->
  From = op_from(Op),
  OpType = op_type(Op),
  MergeFun = merge_fun(OpType),
  #{ id := DocId } = Record = op_record(Op),
  case get_cached(DocId, State) of
    {DocId, DocInfo} ->
      State2 = MergeFun(Record, DocInfo, From, State),
      merge_revtrees(Rest, State2);
    false ->
      [Rev | _] = maps:get(revs, Record, [<<>>]),
      case fetch_docinfo(DocId, State) of
        {ok, DocInfo} ->
          State2 = MergeFun(Record, DocInfo, From, cache(DocInfo, State)),
          merge_revtrees(Rest, State2);
        {error, not_found} ->
          case OpType of
            merge when Rev =:= <<>> ->
              %% create or add a revision
              DocInfo = new_docinfo(DocId),
              State2 = MergeFun(Record, DocInfo, From, State),
              merge_revtrees(Rest, State2);
            merge ->
              reply(From, {error, DocId, not_found});
            merge_with_conflict ->
              DocInfo = new_docinfo(DocId),
              State2 = MergeFun(Record, DocInfo, From, State),
              merge_revtrees(Rest, State2);
            purge ->
              reply(From, {ok, DocId, purged})
          end;
        Error ->
          _ = lager:error(
            "error reading doc db=~p id=~p error=~p~n",
            [maps:get(db_ref, State), DocId, Error]
          ),
          reply(From, {error, DocId, read_error}),
          merge_revtrees(Rest, State)
      end
  end;
merge_revtrees([], State) ->
  update_db_state(State),
  State.

  
merge_revtree(Record, DocInfo, From, State) ->
  #{ id := DocId, rev := CurrentRev, revtree := RevTree } = DocInfo,
  [Rev | _ ] = maps:get(revs, Record, [<<>>]),
  Deleted = maps:get(deleted, Record, false),
  {Gen, _}  = barrel_doc:parse_revision(Rev),
  case Rev of
    <<>> ->
      if
        CurrentRev /= <<>> ->
          case maps:get(CurrentRev, RevTree) of
            #{ deleted := true } ->
              {CurrentGen, _} = barrel_doc:parse_revision(CurrentRev),
              merge_revtree(
                CurrentGen + 1, CurrentRev, Deleted,
                Record, DocInfo, From, State
              );
            _ ->
              reply(From, {error, DocId, {conflict, doc_exists}}),
              State
          end;
        true ->
          merge_revtree(
            Gen + 1, <<>>, Deleted,
            Record, DocInfo, From, State
          )
      end;
    _ ->
      case barrel_revtree:is_leaf(Rev, RevTree) of
        true ->
          case {maps:get(Rev, RevTree), Deleted} of
            {#{ deleted := true}, true} ->
              reply(From, {error, DocId, not_found}),
              State;
            _ ->
              merge_revtree(
                Gen + 1, Rev, Deleted,
                Record, DocInfo, From, State
              )
          end;
        false ->
          reply(From, {error, DocId, {conflict, revision_conflict}}),
          State
      end
  end.

merge_revtree(NewGen, ParentRev, Deleted, Record, DocInfo, From, #{db_ref := Db} = State) ->
  #{ id := DocId, deleted := OldDeleted, revtree := RevTree } = DocInfo,
  OldSeq = maps:get(seq, DocInfo, nil),
  #{ id := DocId,
     doc := Doc,
     hash := RevHash } = Record,
  NewRev = << (integer_to_binary(NewGen))/binary, "-", RevHash/binary  >>,
  RevInfo = #{  id => NewRev,  parent => ParentRev, deleted => Deleted },
  RevTree2 = barrel_revtree:add(RevInfo, RevTree),
  %% find winning revision and update doc infos with it
  {WinningRev, Branched, Conflict} = barrel_revtree:winning_revision(RevTree2),
  WinningRevInfo = maps:get(WinningRev, RevTree2),
  %% update the db state:
  Inc = docs_count_inc(ParentRev, Deleted, OldDeleted),
  {NewSeq, State1} = update_state(State, Inc),
  DocDeleted = barrel_revtree:is_deleted(WinningRevInfo),
  %% update docinfo
  DocInfo2 = DocInfo#{ seq => NewSeq,
                       revtree => RevTree2,
                       rev => WinningRev,
                       branched => Branched,
                       conflict => Conflict,
                       deleted => DocDeleted },
  WriteResult = case add_revision(DocId, NewRev, Doc, State1) of
                  ok ->
                    write_docinfo(DocId, NewSeq, OldSeq, DocInfo2, State1);
                  Error ->
                    Error
                end,
  case WriteResult of
    ok ->
      reply(From, {ok, Doc, DocInfo2}),
      update_db_state(State1),
      cache(DocInfo2, State1);
    WriteError  ->
      _ = lager:error(
        "error writing doc db=~p id=~p error=~p~n",
        [Db, DocId, WriteError]
      ),
      reply(From, {error, DocId, write_error}),
      State
  end.

merge_revtree_with_conflict(Record, DocInfo0, From, #{ db_ref := Db} = State0) ->
  #{ id := DocId, rev := CurrentRev, revtree := RevTree, deleted := OldDeleted } = DocInfo0,
  {OldPos, _}  = barrel_doc:parse_revision(CurrentRev),
  [NewRev | _] = Revs = maps:get(revs, Record, []),
  Deleted = maps:get(deleted, Record, false),
  Doc = maps:get(doc, Record),
  {Idx, Parent} = find_parent(Revs, RevTree, 0),
  RevTree2 = if
               Idx =:= 0 ->
                 %% parent is missing, let's store it
                 %% TODO: maybe we should change the position of the rev to 1 since we don't have any parent and consider it unique?
                 RevInfo = #{ id => NewRev,  parent => <<>>, deleted => Deleted },
                 barrel_revtree:add(RevInfo, RevTree);
               true ->
                 ToAdd = lists:sublist(Revs, Idx),
                 edit_revtree(lists:reverse(ToAdd), Parent, Deleted, RevTree)
             end,
  %% find winning revision and update doc infos with it
  {WinningRev, Branched, Conflict} = barrel_revtree:winning_revision(RevTree2),
  %% if the new winning revision is at the same position we keep the current
  %% one as winner. Else we update the doc info.
  {NewSeq, DocInfo1, NewState} = case barrel_doc:parse_revision(WinningRev) of
                                   {OldPos, _} ->
                                     {nil, DocInfo0#{ revtree => RevTree2 }, State0};
                                   {_NewPos, _} ->
                                     WinningRevInfo = maps:get(WinningRev, RevTree2),
                                     Deleted = barrel_revtree:is_deleted(WinningRevInfo),
                                     Inc = docs_count_inc(CurrentRev, Deleted, OldDeleted),
                                     {Seq, State1} = update_state(State0, Inc),
                                     {
                                       Seq,
                                       DocInfo0#{seq => Seq,
                                                 revtree => RevTree2,
                                                 current_rev => WinningRev,
                                                 branched => Branched,
                                                 conflict => Conflict,
                                                 deleted => Deleted},
                                       State1
                                     }
                                 end,
  OldSeq = maps:get(seq, DocInfo0, nil),
  WriteResult = case add_revision(DocId, NewRev, Doc, NewState) of
                  ok ->
                    write_docinfo(DocId, NewSeq, OldSeq, DocInfo1, NewState);
                  Error ->
                    Error
                end,
  case WriteResult of
    ok ->
      reply(From, {ok, Doc, DocInfo1}),
      cache(DocInfo1, NewState);
    WriteError ->
      _ = lager:error(
        "error writing doc db=~p id=~p error=~p~n",
        [Db, DocId, WriteError]
      ),
      reply(From, {error, DocId, {write_error, WriteError}}),
      State0
  end.

edit_revtree([RevId], Parent, Deleted, Tree) ->
  case Deleted of
    true ->
      barrel_revtree:add(#{ id => RevId, parent => Parent, deleted => true}, Tree);
    false ->
      barrel_revtree:add(#{ id => RevId, parent => Parent}, Tree)
  end;
edit_revtree([RevId | Rest], Parent, Deleted, Tree) ->
  Tree2 = barrel_revtree:add(#{ id => RevId, parent => Parent}, Tree),
  edit_revtree(Rest, RevId, Deleted, Tree2);
edit_revtree([], _Parent, _Deleted, Tree) ->
  Tree.

find_parent([RevId | Rest], RevTree, I) ->
  case barrel_revtree:contains(RevId, RevTree) of
    true -> {I, RevId};
    false -> find_parent(Rest, RevTree, I+1)
  end;
find_parent([], _RevTree, I) ->
  {I, <<"">>}.


purge(_Record, DocInfo, From, #{ db_ref := Db} = State) ->
  #{ id := DocId, seq := Seq, revtree := RevTree } = DocInfo,
  Revisions = barrel_revtree:revisions(RevTree),
  case purge_doc(DocId, Seq, Revisions, State) of
    ok ->
      reply(From, {ok, DocId, purged}),
      remove_cached(DocId, State);
    WriteError ->
      _ = lager:error(
        "error purging doc db=~p id=~p error=~p~n",
        [Db, DocId, WriteError]
      ),
      reply(From, {error, DocId, {write_error, WriteError}}),
      State
  end.
  
update_db_state(#{ db_pid := DbPid, db_state := DbState }) ->
  barrel_db:set_state(DbPid, DbState).

%% docs count increment:
%% parent revision is <<>> when creating a new doc then we increment the docs count,
%% when doc is already deleted or is updated we don't increment the docs count
%% in other case, the doc is deleted then we decrement the docs count
docs_count_inc(<<>>, _, _) -> 1;
docs_count_inc(_, false, true) -> 1;
docs_count_inc(_, false, _) -> 0;
docs_count_inc(_, true, true) -> 0;
docs_count_inc(_, true, _) -> -1.

update_state(#{ db_state := DbState } = State, Inc) ->
  #{ updated_seq := Seq, docs_count := DocsCount } = DbState,
  NewSeq = Seq + 1,
  DocsCount2 = DocsCount + Inc,
  {NewSeq, State#{ db_state => DbState#{updated_seq => NewSeq,
                                        docs_count => DocsCount2} }}.

add_revision(DocId, RevId, Body, #{ db_mod := Mod, db_state := DbState }) ->
  Mod:add_revision(DocId, RevId, Body, DbState).

fetch_docinfo(DocId, #{ db_mod := Mod, db_state := DbState }) ->
  Mod:fetch_docinfo(DocId, DbState).

write_docinfo(DocId, NewSeq, OldSeq, DocInfo, #{ db_mod := Mod, db_state := DbState }) ->
  Mod:write_docinfo(DocId, NewSeq, OldSeq, DocInfo, DbState).


purge_doc(DocId, LastSeq, Revisions,  #{ db_mod := Mod, db_state := DbState }) ->
  Mod:purge_doc(DocId, LastSeq, Revisions, DbState).

-compile({inline, [reply/2]}).
-spec reply(From :: {pid(), reference()}, Reply :: term()) -> ok.
reply({To, Tag}, Reply) when is_pid(To) ->
  Msg = {Tag, Reply},
  try To ! Msg of
    _ ->
      ok
  catch
    _:_ -> ok
  end.