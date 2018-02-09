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
  start_link/1,
  db_infos/1,
  fetch_doc/3,
  write_changes/2
]).

-export([
  call/2,
  cast/2,
  do_for_ref/2,
  get_state/1,
  set_state/2,
  close/1
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
  do_fetch_doc/3
]).

-export([handle_event/4]).

-include("barrel.hrl").

-define(WRITE_BATCH_SIZE, 128).

db_infos(DbRef) ->
  call(DbRef, infos).

fetch_doc(DbRef, DocId, Options) ->
  Tag = make_ref(),
  From = {self(), Tag},
  do_for_ref(
    DbRef,
    fun(DbPid) ->
      ok = gen_statem:cast(DbPid, {fetch_doc, DocId, Options, From}),
      await_response(DbPid, Tag)
    end
  ).

await_response(DbPid, Tag) ->
  MRef = erlang:monitor(process, DbPid),
  receive
    {Tag, Resp} -> Resp;
    {'DOWN', MRef, _, _, _} ->
      erlang:error(db_down)
  after 5000 ->
    erlang:error(timeout)
  end.

write_changes(DbRef, Batch) ->
  Tag = make_ref(),
  From = {self(), Tag},
  Entries = prepare_batch(Batch, From, []),
  NumEntries = length(Entries),
  do_for_ref(
    DbRef,
    fun(DbPid) ->
      ok = gen_statem:call(DbPid, {write_changes, Entries}),
      await_response(DbPid, Tag, NumEntries)
    end
  ).

await_response(DbPid, Tag, NumEntries) ->
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
      erlang:error(db_down)
  after 5000 ->
    erlang:error(timeout)
  end.

append_writes_summary({ok, Doc0, DocInfo}, Results) ->
  Rev = maps:get(rev, DocInfo),
  Deleted = maps:get(deleted, DocInfo, false),
  Conflict = maps:get(conflict, DocInfo, false),
  Doc1 = maybe_deleted(
    maybe_conflict(
      Doc0#{ <<"_rev">> => Rev },  Conflict
    ),
    Deleted
  ),
  [Doc1 | Results];
append_writes_summary({error, DocId, not_found}, Results) ->
  ErrorDoc = error_doc(DocId, <<"not_found">>, 404),
  [ErrorDoc | Results];
append_writes_summary({error, DocId, {conflict, Conflict}}=Error, Results) ->
  _ = lager:info("got error ~p~n", [Error]),
  ErrorDoc  = error_doc(DocId,
                        <<"conflict">>,
                        409,
                        #{ <<"conflict">> => barrel_lib:to_binary(Conflict) }),
  [ErrorDoc | Results];
append_writes_summary({error, DocId, read_error}, Results) ->
  ErrorDoc = error_doc(DocId, <<"read_error">>, 500),
  [ErrorDoc | Results];
append_writes_summary({error, DocId, write_error}, Results) ->
  ErrorDoc = error_doc(DocId, <<"write_error">>, 500),
  [ErrorDoc | Results];
append_writes_summary(Other, __Results) ->
  erlang:error({bad_msg, Other}).

maybe_deleted(Doc, false) -> Doc;
maybe_deleted(Doc, true) -> Doc#{ <<"_deleted">>  => true }.

maybe_conflict(Doc, false) -> Doc;
maybe_conflict(Doc, true) -> Doc#{ <<"conflict">> => true }.

error_doc(DocId, Error, ErrorCode) -> error_doc(DocId, Error, ErrorCode, #{}).

error_doc(DocId, Error, ErrorCode, Extra) ->
  Extra#{<<"id">> => DocId,
         <<"error">> => Error,
         <<"error_code">> => ErrorCode}.

prepare_batch([{delete, Id, Rev} | Rest], From, Acc) ->
  Record = barrel_doc:make_record(#{<<"id">> => Id,
                                    <<"_deleted">> => true,
                                    <<"_rev">> => Rev}),
  Op = barrel_db_writer:make_op(merge, Record, From),
  prepare_batch(Rest, From, [Op | Acc]);
prepare_batch([{create, Doc} | Rest], From, Acc) ->
  Record = barrel_doc:make_record(Doc),
  Op = barrel_db_writer:make_op(merge, Record, From),
  prepare_batch(Rest, From, [Op | Acc]);
prepare_batch([{replace, Doc} | Rest], From, Acc) ->
  Record = barrel_doc:make_record(Doc),
  Op = barrel_db_writer:make_op(merge, Record, From),
  prepare_batch(Rest, From, [Op | Acc]);
prepare_batch([{add_rev, RevDoc} | Rest], From, Acc) ->
  Record = barrel_doc:make_record(RevDoc),
  Op = barrel_db_writer:make_op(merge_with_conflict, Record, From),
  prepare_batch(Rest, From, [Op | Acc]);
prepare_batch([_], _From, _Acc) ->
  erlang:error(badarg);
prepare_batch([], _From, Acc) ->
  lists:reverse(Acc).

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
        undefined ->
          case barrel_db_sup:start_db(DbRef) of
            {ok, undefined} ->
              {error, db_not_found};
            {ok, Pid} ->
              Fun(Pid);
            {error, {already_started, Pid}} ->
              Fun(Pid);
            {error, Error} ->
              exit({noproc, Error})
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

get_state(DbPid) ->
  gen_statem:call(DbPid, get_state).

set_state(DbPid, State) ->
  DbPid ! {set_state, State},
  ok.

start_link(DbRef) ->
  gen_statem:start_link({via, gproc, {n, l, DbRef}}, ?MODULE, [DbRef], []).

%% -----------------
%% gen_statem callbacks

init([#{ store := Store, id := Id}=DbRef]) ->
  process_flag(trap_exit, true),
  case barrel_storage:init_barrel(Store, Id) of
    {ok, State} ->
      {ok, Writer} = barrel_db_writer:start_link(DbRef, State),
      BatchSize = application:get_env(barrel, write_batch_size, ?WRITE_BATCH_SIZE),
      Data = #{ ref => DbRef,
                store => Store,
                id => Id,
                state => State,
                writer => Writer,
                write_batch_size => BatchSize,
                pending => []},
      {ok, writeable, Data};
    not_found ->
      ignore;
    Error ->
      {stop, Error}
  end.

callback_mode() -> state_functions.

terminate(_Reason, _State, #{ store := Store, id := Id}) ->
  _ = barrel_storage:close_barrel(Store, Id),
  ok.

code_change(_OldVsn, State, Data, _Extra) ->
  {ok, State, Data}.

%% states

writeable(cast, {fetch_doc, DocId, Options, From}, #{ ref := DbRef } = Data) ->
  _ = ?jobs_pool:run({?MODULE, do_fetch_doc, [DbRef,DocId, Options]}, From),
  {keep_state, Data};
writeable({call, From}, {write_changes, Entries}, Data) ->
  _ = notify_writer(Entries, Data),
  {next_state, writing, Data#{ pending => []}, [{reply, From, ok}]};
writeable(EventType, Event, Data) ->
  handle_event(EventType, Event, writeable, Data).

writing(cast, {fetch_doc, DocId, Options, From}, #{ ref := DbRef } = Data) ->
  _ = ?jobs_pool:run({?MODULE, do_fetch_doc, [DbRef, DocId, Options]}, From),
  {keep_state, Data};
writing({call, From}, {write_changes, Entries}, #{ pending := Pending } = Data) ->
  Pending2 = Pending ++ Entries,
  {keep_state, Data#{ pending => Pending2}, [{reply, From, ok}]};
writing(info, {set_state, State}, #{ pending := Pending, write_batch_size := BatchSize } = Data) ->
  ok = maybe_notify_changes(State, Data),
  case Pending of
    [] ->
      {next_state, writeable, Data#{ state => State }};
    _ ->
      {ToSend, Pending2} = filter_pending(Pending, BatchSize, []),
      _ = notify_writer(ToSend, Data),
      {keep_state, Data#{ pending => Pending2 }}
  end;
writing(EventType, Event, Data) ->
  handle_event(EventType, Event, writing, Data).

handle_event({call, From}, infos, _State, #{ state := State } = Data) ->
  {keep_state, Data, [{reply, From, State}]};
handle_event({call, From}, get_state, _State, #{ state := State } = Data) ->
  {keep_state, Data, [{reply, From, State}]};
handle_event(info, {'EXIT', Pid, Reason}, _State, #{ db_ref := DbRef, writer := Pid } = Data) ->
  _ = lager:info("writer exitded. db=~p reason=~p~n", [DbRef, Reason]),
  {stop, {writer_exit, Reason}, Data#{writer => nil}};
handle_event(_EventType, _Event, _State, Data) ->
  {keep_state, Data}.

%% TODO: add event handling
maybe_notify_changes(State, #{ state := State }) -> ok;
maybe_notify_changes(_NewState, _Data) ->
  ok.

filter_pending([], _N, Acc) ->
  {lists:reverse(Acc), []};
filter_pending(Pending, N, Acc) when N =< 0 ->
  {lists:reverse(Acc), Pending};
filter_pending([Job | Rest], N, Acc) ->
  filter_pending(Rest, N - 1, [Job | Acc]).


notify_writer(Pending, #{ writer := Writer }) ->
  Writer ! {store, Pending}.

do_fetch_doc(DbRef, DocId, Options) ->
  UserRev = maps:get(rev, Options, <<"">>),
  case barrel_storage:fetch_doc(DbRef, DocId) of
    {ok, #{ deleted := true }} when UserRev =:= <<>> ->
      {error, not_found};
    {ok, #{ rev := CurrentRev, revtree := RevTree}} ->
      Rev = case UserRev of
              <<"">> -> CurrentRev;
              _ -> UserRev
            end,
      case maps:find(Rev, RevTree) of
        {ok, RevInfo} ->
          Del = maps:get(deleted, RevInfo, false),
          case barrel_storage:fetch_doc_revision(DbRef, DocId, Rev) of
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
  