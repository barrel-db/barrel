%% Copyright (c) 2018. Benoit Chesneau
%%
%% Licensed under the Apache License, Version 2.0 (the "License"); you may not
%% use this file except in compliance with the License. You may obtain a copy of
%% the License at
%%
%%    http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
%% License for the specific language governing permissions and limitations under
%% the License.

-module(barrel_db).
-author("benoitc").

%% API
-export([
  create_barrel/2,
  delete_barrel/1,
  is_barrel/1,
  start_link/3,
  db_infos/1,
  fetch_doc/3,
  write_changes/2,
  purge_docs/2,
  get_local_doc/2,
  put_local_doc/3,
  delete_local_doc/2,
  revsdiff/3
]).

-export([
  write_changes_async/2,  write_changes_async/3,
  await_response/1, await_response/2
]).

-export([
  call/2,
  cast/2,
  do_for_ref/2,
  get_state/1,
  set_state/2,
  close/1,
  set_last_indexed_seq/2,
  do_command/2,
  updated_seq/1
]).

-export([
  init/1,
  callback_mode/0,
  terminate/3,
  code_change/4
]).

%% states
-export([
  writeable/3,
  writing/3
]).

%% jobs
-export([
  do_fetch_doc/3,
  do_revsdiff/3
]).

-export([handle_event/4]).

-include("barrel.hrl").

-define(WRITE_BATCH_SIZE, 128).


create_barrel(Name, Options) ->
  case barrel_pm:whereis_name(Name) of
    Pid when is_pid(Pid) ->
      {error, already_exists};
    undefined ->
      case barrel_db_sup:start_db(Name, create, Options) of
        {ok, _Pid} -> ok;
        {error, {already_started, _Pid}} -> ok;
        {error, Error} -> Error
      end
  end.

delete_barrel(Name) ->
  do_for_ref(
    Name,
    fun(DbPid) ->
      try
        delete_barrel_1(DbPid)
      catch
        exit:{noproc,_} -> ok;
        exit:noproc -> ok;
        %% Handle the case where the monitor triggers
        exit:{normal, _} -> ok
      end
    end).

delete_barrel_1(DbPid) ->
  MRef = erlang:monitor(process, DbPid),
  case gen_statem:call(DbPid, delete) of
    ok ->
      receive
        {'DOWN', _, process, DbPid, _} -> ok
      end;
    Other ->
      erlang:demonitor(MRef, [flush]),
      Other
  end.

is_barrel(Name) ->
  Res = do_for_ref( Name, fun(_) -> ok end),
  (Res =:= ok).

db_infos(DbRef) ->
  call(DbRef, infos).

fetch_doc(DbRef, DocId, Options) ->
  do_command(DbRef, {fetch_doc, DocId, Options}).

revsdiff(Name, DocId, RevIds) ->
  do_command(Name, {revsdiff, DocId, RevIds}).

put_local_doc(DbRef, DocId, Doc) ->
  do_command(DbRef, {put_local_doc, DocId, Doc}).

get_local_doc(DbRef, DocId) ->
  do_command(DbRef, {get_local_doc, DocId}).

delete_local_doc(DbRef, DocId) ->
  do_command(DbRef, {delete_local_doc, DocId}).

do_command({Name, Node}, Req) ->
  rpc:call(Node, barrel_db, do_command, [Name, Req]);
do_command(Name, Req) ->
  case get_state(Name) of
    {error, _} = Error ->
      Error;
    {Mod, ModState} ->
      case is_updated(ModState) of
        true ->
          MFA = case Req of
                  {fetch_doc, DocId, Options} ->
                    {barrel_db, do_fetch_doc, [DocId, Options, {Mod, ModState}]};
                  {revsdiff, DocId, RevIds} ->
                    {barrel_db, do_revsdiff, [DocId, RevIds, {Mod, ModState}]};
                  {put_local_doc, DocId, Doc} ->
                    {Mod, put_local_doc, [DocId, Doc, ModState]};
                  {get_local_doc, DocId} ->
                    {Mod, get_local_doc, [DocId, ModState]};
                  {delete_local_doc, DocId} ->
                    {Mod, delete_local_doc, [DocId, ModState]};
                  {query, FoldFun, Path, Start, End, Limit, IncludeDeleted, UserFun, UserAcc} ->
                    {barrel_index, do_query, [FoldFun, Path, Start, End, Limit, IncludeDeleted, UserFun, UserAcc, {Mod, ModState}]}
                end,
          (catch barrel_lib:do_exec(MFA));
        false ->
          timer:sleep(100),
          do_command(Name, Req)
      end
  end.

await_response(DbPid, Tag) ->
  MRef = erlang:monitor(process, DbPid),
  receive
    {Tag, Resp} -> Resp;
    {'DOWN', MRef, _, _, Reason} ->
      erlang:error({worker_down, Reason})
  after 15000 ->
    erlang:error(timeout)
  end.

purge_docs(DbRef, DocIds) ->
  write_changes(DbRef, [{purge, Id} || Id <- DocIds]).

write_changes(DbRef, Batch) ->
  case write_changes_async(DbRef, Batch) of
    {ok, RespStream} ->
      await_response(RespStream);
    Error ->
      Error
  end.

write_changes_async(DbRef, Batch) ->
  write_changes_async(DbRef, Batch, self()).

write_changes_async(DbRef, Batch, To) ->
  Tag = make_ref(),
  From = {To, Tag},
  Entries = prepare_batch(Batch, From, []),
  NumEntries = length(Entries),
  do_for_ref(
    DbRef,
    fun(DbPid) ->
      ok = gen_statem:call(DbPid, {write_changes, Entries}),
      {ok, {Tag, DbPid, NumEntries}}
    end
  ).

await_response({Tag, DbPid, NumEntries}) ->
  MRef = erlang:monitor(process, DbPid),
  try await_response_loop(MRef, Tag, [], NumEntries)
  after erlang:demonitor(MRef, [flush])
  end.

await_response_loop(_, _, Results, 0) ->
  lists:reverse(Results);
await_response_loop(MRef, Tag, Results, NumEntries) ->
  receive
    {Tag, Resp} ->
      Results2 = append_writes_summary(Resp, Results),
      await_response_loop(MRef, Tag, Results2, NumEntries - 1);
    {'DOWN', MRef, _, _, _} ->
      erlang:exit(db_down)
  after 15000 ->
    erlang:exit(timeout)
  end.



append_writes_summary({ok, DocId, purged}, Results) ->
  [{ok, DocId, undefined} | Results];
append_writes_summary({ok, DocId, RevId}, Results) ->
  [{ok, DocId, RevId} | Results];
append_writes_summary({error, DocId, not_found}, Results) ->
  [{error, {not_found, DocId}} | Results];
append_writes_summary({error, DocId, {conflict, _}=Conflict}, Results) ->
  [{error, {Conflict, DocId}} | Results];
append_writes_summary({error, DocId, read_error}, Results) ->
  [{error, {read_error, DocId}} | Results];
append_writes_summary({error, DocId, write_error}, Results) ->
  [{error, {write_error, DocId}} | Results];
append_writes_summary({error, DocId, {bad_rev, _}=BadRev}, Results) ->
  [{error, {BadRev, DocId}} | Results];
append_writes_summary(Other, __Results) ->
  erlang:error({undefined, Other}).

prepare_batch([{delete, Id, Rev} | Rest], From, Acc) ->
  Record = barrel_doc:make_record(#{<<"id">> => Id,
                                    <<"_deleted">> => true,
                                    <<"_rev">> => Rev}),
  Op = barrel_db_writer:make_op(merge, Record#{ replace => true }, From),
  prepare_batch(Rest, From, [Op | Acc]);
prepare_batch([{purge, Id} | Rest], From, Acc) ->
  Record = barrel_doc:make_record(#{<<"id">> => Id}),
  Op = barrel_db_writer:make_op(purge, Record, From),
  prepare_batch(Rest, From, [Op | Acc]);
prepare_batch([{create, Doc0} | Rest], From, Acc) ->
  Doc1 = case maps:find(<<"id">>, Doc0) of
           {ok, _Id} -> Doc0;
           error -> Doc0#{ <<"id">> => barrel_id:binary_id(62) }
         end,
  Record = barrel_doc:make_record(Doc1),
  Op = barrel_db_writer:make_op(merge, Record, From),
  prepare_batch(Rest, From, [Op | Acc]);
prepare_batch([{replace, Doc} | Rest], From, Acc) ->
  ok = check_docid(Doc),
  Record = barrel_doc:make_record(Doc),
  Op = barrel_db_writer:make_op(merge, Record#{ replace => true }, From),
  prepare_batch(Rest, From, [Op | Acc]);
prepare_batch([{add_rev, RevDoc} | Rest], From, Acc) ->
  Record = barrel_doc:make_record(RevDoc),
  Op = barrel_db_writer:make_op(merge_with_conflict, Record, From),
  prepare_batch(Rest, From, [Op | Acc]);
prepare_batch([_], _From, _Acc) ->
  erlang:error(badarg);
prepare_batch([], _From, Acc) ->
  lists:reverse(Acc).


check_docid(#{ <<"id">> := _Id}) -> ok;
check_docid(_) ->
  error(badarg).

call(DbRef, Msg) ->
  do_for_ref(
    DbRef,
    fun(DbPid) ->
      gen_statem:call(DbPid, Msg)
    end
  ).

cast(DbRef, Msg) ->
  do_for_ref(
    DbRef,
    fun(DbPid) ->
      gen_statem:cast(DbPid, Msg)
    end
  ).

do_for_ref(DbRef, Fun) ->
  try
      case barrel_pm:whereis_name(DbRef) of
        Pid when is_pid(Pid) ->
          Fun(Pid);
        {badrpc,nodedown} ->
          {error, db_not_found};
        undefined ->
          case barrel_db_sup:start_db(DbRef) of
            {ok, undefined} ->
              {error, db_not_found};
            {ok, Pid} ->
              %ok = barrel_index_actor:refresh(DbRef),
              Fun(Pid);
            {error, {already_started, Pid}} ->
              %ok = barrel_index_actor:refresh(DbRef),
              Fun(Pid);
            Err = {error, _} ->
              Err;
            Error ->
              {error, Error}
          end
      end
  catch
      exit:Reason when Reason =:= normal  ->
        do_for_ref(DbRef, Fun)
  end.


close(DbRef) ->
  case barrel_pm:whereis_name(DbRef) of
    DbPId when is_pid(DbPId) ->
      barrel_db_sup:stop_db(DbPId);
    undefined ->
      ok
  end.

%% -----------------
%% private state functions

get_state(DbPid) when is_pid(DbPid) ->
  gen_statem:call(DbPid, get_state);
get_state(Name) ->
  do_for_ref(
    Name,
    fun(DbPid) -> gen_statem:call(DbPid, get_state) end
  ).


is_updated( #{ updated_seq := UpdatedSeq, indexed_seq := IndexedSeq}) when UpdatedSeq > IndexedSeq ->
  false;
is_updated(_) ->
  true.

updated_seq(DbPid) when is_pid(DbPid) ->
  try gen_statem:call(DbPid, updated_seq)
  catch
    exit:{noproc,_} -> {error, db_not_found};
    exit:noproc ->  {error, db_not_found};
    %% Handle the case where the monitor triggers
    exit:{normal, _} -> {error, db_not_found}
  end;
updated_seq(Name) ->
  do_for_ref(
    Name,
    fun(DbPid) -> updated_seq(DbPid) end
  ).

set_state(DbPid, State) ->
  DbPid ! {set_state, State},
  ok.

set_last_indexed_seq(DbPid, Seq) ->
  DbPid ! { set_last_indexed_seq, Seq}.

start_link(Name, OpenType, Options) ->
  proc_lib:start_link(?MODULE, init, [[Name, OpenType, Options]]).

%% -----------------
%% gen_statem callbacks


init([Name, create, Options]) ->
  case barrel_storage:find_barrel(Name) of
    error ->
      Store = maps:get(store, Options, barrel_storage:get_default()),
      case barrel_storage:create_barrel(Store, Name, Options) of
        {{ok, State}, Mod} ->
          barrel_pm:register_name(Name, self()),
          process_flag(trap_exit, true),
          {ok, Writer} = barrel_db_writer:start_link(Name, Mod, State),
          %{ok, Indexer} = barrel_index_actor:start_link(Name, self(), Mod, State),
          proc_lib:init_ack({ok, self()}),
          Data = #{ store => Store,
                    name => Name,
                    mod => Mod,
                    state => State,
                    writer => Writer,
                    %indexer => Indexer,
                    pending => []},
          
          gen_statem:enter_loop(?MODULE, [], writeable, Data, {via, barrel_pm, Name});
        {Error, _} ->
          exit(Error)
      end;
    {ok, _} ->
      exit({error, db_already_exists})
  end;
init([Name, open, _Options]) ->
  case barrel_storage:find_barrel(Name) of
    {ok, Store} ->
      case barrel_storage:open_barrel(Store, Name) of
        {{ok, State}, Mod} ->
          barrel_pm:register_name(Name, self()),
          process_flag(trap_exit, true),
          {ok, Writer} = barrel_db_writer:start_link(Name, Mod, State),
          %{ok, Indexer} = barrel_index_actor:start_link(Name, self(), Mod, State),
          proc_lib:init_ack({ok, self()}),
          Data = #{ store => Store,
                    name => Name,
                    mod => Mod,
                    state => State,
                    writer => Writer,
                    %indexer => Indexer,
                    pending => []},
          gen_statem:enter_loop(?MODULE, [], writeable, Data, {via, barrel_pm, Name});
        {Error, _} ->
          proc_lib:init_ack({error, Error}),
          exit(normal)
      end;
    error ->
      proc_lib:init_ack({error, db_not_found}),
      exit(normal)
  end.

callback_mode() -> state_functions.


terminate(_Reason, _State, #{ store := Store, name := Name}) ->
  _ = (catch barrel_storage:close_barrel(Store, Name)),
  ok.

code_change(_OldVsn, State, Data, _Extra) ->
  {ok, State, Data}.

%% states

writeable({call, From}, {write_changes, Entries}, Data) ->
  _ = notify_writer(Entries, Data),
  {next_state, writing, Data#{ pending => []}, [{reply, From, ok}]};

writeable(info, {set_last_indexed_seq, Seq}, #{ mod := Mod, state := State0 } = Data) ->
  State1 = State0#{ indexed_seq => Seq },
  ok = Mod:save_state(State1),
  {keep_state, Data#{ state => State1}};
writeable(info, {set_state, State}, #{ mod := Mod } = Data) ->
  ok = Mod:save_state(State),
  {keep_state, Data#{ state => State}};
writeable(EventType, Event, Data) ->
  handle_event(EventType, Event, writeable, Data).

writing({call, From}, {write_changes, Entries}, #{ pending := Pending } = Data) ->
  Pending2 = Pending ++ Entries,
  {keep_state, Data#{ pending => Pending2}, [{reply, From, ok}]};
writing(info, {set_last_indexed_seq, Seq}, #{ mod := Mod, state := State0 } = Data) ->
  State1 = State0#{ indexed_seq => Seq },
  ok = Mod:save_state(State1),
  {keep_state, Data#{ state => State1}};
writing(info, {set_state, State}, #{ mod := Mod, pending := Pending } = Data) ->
  ok = Mod:save_state(State),
  ok = maybe_notify_changes(State, Data),
  case Pending of
    [] ->
      NewData = Data#{ state => State },
      {next_state, writeable, NewData};
    _ ->
      NewData = Data#{ state => State,  pending => [] },
      _ = notify_writer(Pending, NewData),
      {keep_state, NewData}
  end;
writing(EventType, Event, Data) ->
  handle_event(EventType, Event, writing, Data).

handle_event({call, From}, infos, _State, #{ state := State } = Data) ->
  {keep_state, Data, [{reply, From, State}]};
handle_event({call, From}, updated_seq, _State, #{ state := State } = Data) ->
  #{ updated_seq := UpdatedSeq } = State,
  {keep_state, Data, [{reply, From, {ok, UpdatedSeq}}]};
handle_event({call, From}, get_state, _State, #{ mod := Mod, state := State } = Data) ->
  {keep_state, Data, [{reply, From, {Mod, State}}]};
handle_event({call, From}, delete, _State, #{ store := Store, name := Name }) ->
  _ = (catch barrel_storage:delete_barrel(Store, Name)),
  {stop_and_reply, normal, [{reply, From, ok}]};
handle_event(info, {'EXIT', Pid, Reason}, _State, #{ db_ref := DbRef, writer := Pid } = Data) ->
  _ = lager:error("writer exitded. db=~p reason=~p~n", [DbRef, Reason]),
  {stop, normal, Data#{writer => nil}};
%handle_event(info, {'EXIT', Pid, Reason}, _State, #{ db_ref := DbRef, indexer := Pid } = Data) ->
%  _ = lager:error("indexer exitded. db=~p reason=~p~n", [DbRef, Reason]),
%  {stop, normal, Data#{writer => nil}};
handle_event(_EventType, Event, _State, #{ db_ref := DbRef } = Data) ->
  _ = lager:warning("db got unknown name=~p, event=~p", [DbRef, Event]),
  {keep_state, Data}.

%% TODO: add event handling
maybe_notify_changes(State, #{ state := State }) -> ok;
maybe_notify_changes(_NewState, _Data) ->
  ok.

notify_writer(Pending, #{ writer := Writer }) ->
  Entries = barrel_lib:group_by(
    Pending,
    fun(#write_op{doc=#{ id := Id }}) -> Id end
  ),
  Writer ! {store, Entries}.

do_fetch_doc(DocId, Options, {Mod, State}) ->
  UserRev = maps:get(rev, Options, <<"">>),
  case Mod:fetch_docinfo(DocId, State) of
    {ok, #{ deleted := true }} when UserRev =:= <<>> ->
      {error, not_found};
    {ok, #{ rev := WinningRev, revtree := RevTree}} ->
      Rev = case UserRev of
              <<"">> -> WinningRev;
              _ -> UserRev
            end,
      case maps:find(Rev, RevTree) of
        {ok, RevInfo} ->
          Del = maps:get(deleted, RevInfo, false),
          case Mod:get_revision(DocId, Rev, State) of
            {ok, Doc} ->
              WithHistory = maps:get(history, Options, false),
              MaxHistory = maps:get(max_history, Options, ?IMAX1),
              Ancestors = maps:get(ancestors, Options, []),
              case WithHistory of
                false ->
                  {ok, maybe_add_deleted(Doc#{ <<"_rev">> => Rev }, Del)};
                true ->
                  History = barrel_revtree:history(Rev, RevTree),
                  EncodedRevs = barrel_doc:encode_revisions(History),
                  Revisions = barrel_doc:trim_history(EncodedRevs, Ancestors, MaxHistory),
                  {ok, maybe_add_deleted(Doc#{ <<"_rev">> => Rev, <<"_revisions">> => Revisions }, Del)}
              end;
            Error ->
              Error
          end;
        Error ->
          Error
      end;
    Error ->
      Error
  end.

maybe_add_deleted(Doc, true) -> Doc#{ <<"_deleted">> => true };
maybe_add_deleted(Doc, false) -> Doc.

do_revsdiff(DocId, RevIds, {Mod, State}) ->
  Snapshot = Mod:get_snapshot(State),
  case Mod:fetch_docinfo(DocId, Snapshot) of
    {ok, #{revtree := RevTree}} ->
      {Missing, PossibleAncestors} = lists:foldl(
        fun(RevId, {M, A} = Acc) ->
          case barrel_revtree:contains(RevId, RevTree) of
            true -> Acc;
            false ->
              M2 = [RevId | M],
              {Gen, _} = barrel_doc:parse_revision(RevId),
              A2 = barrel_revtree:fold_leafs(
                fun(#{ id := Id}=RevInfo, A1) ->
                  Parent = maps:get(parent, RevInfo, <<"">>),
                  case lists:member(Id, RevIds) of
                    true ->
                      {PGen, _} = barrel_doc:parse_revision(Id),
                      if
                        PGen < Gen -> [Id | A1];
                        PGen =:= Gen, Parent =/= <<"">> -> [Parent | A1];
                        true -> A1
                      end;
                    false -> A1
                  end
                end, A, RevTree),
              {M2, A2}
          end
        end, {[], []}, RevIds),
      {ok, lists:reverse(Missing), lists:usort(PossibleAncestors)};
    {error, not_found} ->
      {ok, RevIds, []};
    Error ->
      Error
  end.