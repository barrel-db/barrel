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
    seq => 0,
    deleted => false,
    revtree => barrel_revtree:new()}.

merge_revtrees([Op | Rest], State) ->
  From = op_from(Op),
  OpType = op_type(Op),
  MergeFun = merge_fun(OpType),
  #{ id := DocId } = Record = op_record(Op),
  case get_cached(DocId, State) of
    {DocId, DocInfo} ->
      State2 = try
                 MergeFun(Record, DocInfo, From, State)
               catch
                 exit:Error ->
                   reply(From, {error, DocId, Error}),
                   State
               end,
      merge_revtrees(Rest, State2);
    false ->
      [Rev | _] = maps:get(revs, Record, [<<>>]),
      case fetch_docinfo(DocId, State) of
        {ok, DocInfo} ->
          State2 = try
                     MergeFun(Record, DocInfo, From, cache(DocInfo, State))
                   catch
                     exit:Error ->
                       reply(From, {error, DocId, Error}),
                       State
                   end,
          merge_revtrees(Rest, State2);
        {error, not_found} ->
          case OpType of
            merge when Rev =:= <<>> ->
              %% create or add a revision
              DocInfo = new_docinfo(DocId),
              State2 = try
                         MergeFun(Record, DocInfo, From, State)
                       catch
                         exit:Error ->
                           reply(From, {error, DocId, Error}),
                           State
                       end,
              merge_revtrees(Rest, State2);
            merge ->
              reply(From, {error, DocId, not_found});
            merge_with_conflict ->
              DocInfo = new_docinfo(DocId),
              State2 = try
                         MergeFun(Record, DocInfo, From, State)
                       catch
                         exit:Error ->
                           reply(From, {error, DocId, Error}),
                           State
                       end,
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


merge_revtree(Record, #{ deleted := true } = DocInfo, From,  #{ db_ref := Db} = State) ->
  #{ id := DocId, seq := OldSeq, revtree := RevTree } = DocInfo,
  #{ revs := [Rev | _ ], deleted := NewDeleted, doc := Doc } = Record,
  case Rev =:= <<"">> andalso not NewDeleted of
    true ->
      {WinningRev, _Branched, _Conflict} = barrel_revtree:winning_revision(RevTree),
      _ = lager:info("update deleted doc id=~p rev=~p~n", [DocId, WinningRev]),
      {Gen, _}  = barrel_doc:parse_revision(WinningRev),
      NewRevHash = barrel_doc:revision_hash(Doc, WinningRev, false),
      NewRev = << (integer_to_binary(Gen+1))/binary, "-", NewRevHash/binary  >>,
      RevInfo = #{  id => NewRev,  parent => WinningRev, deleted => false },
      RevTree2 = barrel_revtree:add(RevInfo, RevTree),
      DocInfo2 = DocInfo#{ deleted => false, revtree => RevTree2 },
      {NewSeq, State1} = update_state(State, 1),
      WriteResult = case add_revision(DocId, NewRev, Doc, State1) of
                      ok ->
                        write_docinfo(DocId, NewSeq, OldSeq, DocInfo2, State1);
                      Error ->
                        Error
                    end,
      case WriteResult of
        ok ->
          reply(From, {ok, DocId, NewRev}),
          update_db_state(State1),
          barrel_event:notify(Db, db_updated),
          cache(DocInfo2, State1);
        WriteError  ->
          _ = lager:error(
            "error writing doc db=~p id=~p error=~p~n",
            [Db, DocId, WriteError]
          ),
          reply(From, {error, DocId, write_error}),
          State
      end;
    false when Rev /= <<>> ->
      reply(From, {error, DocId, not_found}),
      State;
    false ->
      reply(From, {error, DocId, {conflict, revision_conflict}}),
      State
  end;
merge_revtree(Record, DocInfo, From,  #{ db_ref := Db} = State) ->
  #{ id := DocId, seq := OldSeq, revtree := RevTree } = DocInfo,
  #{ revs := [Rev | _ ], deleted := NewDeleted, hash := RevHash, doc := Doc } = Record,
  _ = lager:info("update  doc id=~p rev=~p, deleted=~p revtree=~p~n", [DocId, Rev, NewDeleted, RevTree]),
  {Gen, _}  = barrel_doc:parse_revision(Rev),
  {DocInfo2, Rev2, Seq, NewState} = case Rev of
                                      <<"">> when map_size(RevTree) =:= 0  ->
                                        NewRev = << "1-", RevHash/binary  >>,
                                        RevInfo = #{  id => NewRev, parent => <<>> },
                                        RevTree1 = barrel_revtree:add(RevInfo, RevTree),
                                        DocInfo1 = DocInfo#{ revtree => RevTree1 },
                                        {NewSeq, State1} = update_state(State, 1),
                                        {DocInfo1#{ seq := NewSeq }, NewRev, NewSeq, State1};
                                      <<"">> ->
                                        reply(From, {error, DocId, {conflict, doc_exists}}),
                                        {DocInfo, Rev, OldSeq, State};
                                      _ ->
                                        case barrel_revtree:is_leaf(Rev, RevTree) of
                                          true ->
                                            NewRev = << (integer_to_binary(Gen+1))/binary, "-", RevHash/binary  >>,
                                            RevInfo = #{  id => NewRev, parent => Rev, deleted => NewDeleted },
                                            RevTree2 = barrel_revtree:add(RevInfo, RevTree),
                                            DocInfo1 = case NewDeleted of
                                                         false ->
                                                           DocInfo#{ deleted => false, revtree => RevTree2 };
                                                         true ->
                                                           DocInfo#{deleted => barrel_doc:is_deleted(RevTree2),
                                                                    revtree => RevTree2 }
                                                       end,
                                            {NewSeq, State1} = case maps:get(deleted, DocInfo1) of
                                                                 false -> update_state(State, 0);
                                                                 true -> update_state(State, -1)
                                                               end,
                                            {DocInfo1#{ seq := NewSeq }, NewRev, NewSeq, State1};
                                          false ->
                                            reply(From, {error, DocId, {conflict, revision_conflict}}),
                                            {DocInfo, Rev, OldSeq, State}

                                        end
                                    end,

  case DocInfo =/= DocInfo2 of
    true ->
      WriteResult = case add_revision(DocId, Rev2, Doc, NewState) of
                      ok ->
                        write_docinfo(DocId, Seq, OldSeq, DocInfo2, NewState);
                      Error ->
                        Error
                    end,
      case WriteResult of
        ok ->
          reply(From, {ok, DocId, Rev2}),
          update_db_state(NewState),
          barrel_event:notify(Db, db_updated),
          cache(DocInfo2, NewState);
        WriteError  ->
          _ = lager:error(
            "error writing doc db=~p id=~p error=~p~n",
            [Db, DocId, WriteError]
          ),
          reply(From, {error, DocId, write_error}),
          State
      end;
    false ->
      State
  end.

merge_revtree_with_conflict(Record, DocInfo, From, #{ db_ref := Db} = State) ->
  #{ id := DocId, revtree := RevTree, seq := OldSeq } = DocInfo,
  #{ revs := [LeafRev | Revs],  deleted := NewDeleted, doc := Doc  } = Record,

  %% Find the point where this doc's history branches from the current rev:
  {_MergeType, [Parent | Path]} = find_parent(Revs, RevTree, []),

  %% merge path in the revision tree
  {_, RevTree2} = lists:foldl(
    fun(RevId, {P, Tree}) ->
      Deleted = (NewDeleted =:= true andalso RevId =:= LeafRev),
      RevInfo = #{ id => RevId, parent => P, deleted => Deleted },
      {RevId, barrel_revtree:add(RevInfo, Tree)}
    end,
    {Parent, RevTree},
    Path
  ),

  %% update DocInfo, we always find is the doc is deleted there
  %% since we could have only updated an internal branch
  DocInfo2 = DocInfo#{ revtree => RevTree2, deleted => barrel_doc:is_deleted(RevTree2) },

  %% update current state and increase new seq if needed
  DocsCountInc = docs_count_inc(DocInfo2, DocInfo),
  {NewSeq, NewState} = update_state(State, DocsCountInc),

  case DocInfo =/= DocInfo2 of
    true ->
      WriteResult = case add_revision(DocId, LeafRev, Doc, NewState) of
                      ok ->
                        write_docinfo(DocId, NewSeq, OldSeq, DocInfo2, NewState);
                      Error ->
                        Error
                    end,
      case WriteResult of
        ok ->
          reply(From, {ok, DocId, LeafRev}),
          update_db_state(NewState),
          barrel_event:notify(Db, db_updated),
          cache(DocInfo2, NewState);
        WriteError  ->
          _ = lager:error(
            "error writing doc db=~p id=~p error=~p~n",
            [Db, DocId, WriteError]
          ),
          reply(From, {error, DocId, write_error}),
          State
      end;
    false ->
      State
  end.

find_parent([RevId | Rest], RevTree, Acc) ->
  case barrel_revtree:contains(RevId, RevTree) of
    true -> {extend, [RevId | Acc]};
    false -> find_parent(Rest, RevTree, [RevId | Acc])
  end;
find_parent([], _RevTree, Acc) ->
  {new_branch, [<<"">> | Acc]}.

docs_count_inc(#{ deleted := true }, #{ deleted := false }) -> -1;
docs_count_inc(#{ deleted := false }, #{ deleted := true }) -> 1;
docs_count_inc(#{ deleted := false }, #{ seq := 0 }) -> 1;
docs_count_inc(_, _) -> 0.

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