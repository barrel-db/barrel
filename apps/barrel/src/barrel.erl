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

-module(barrel).
-author("benoitc").

%% store api
-export([start_store/3, stop_store/1]).

%% API
-export([
  create_barrel/2,
  open_barrel/1,
  close_barrel/1,
  delete_barrel/1,
  barrel_infos/1
]).

-export([
  fetch_doc/3,
  save_doc/2,
  delete_doc/3,
  save_docs/2,  save_docs/3,
  delete_docs/2,
  fold_docs/4,
  fold_changes/5,
  fold_path/5,
  save_replicated_docs/2
]).

-export([start_link/3]).

%% gen_statem callbacks
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2

]).

-include_lib("barrel/include/barrel.hrl").
-include_lib("barrel/include/barrel_logger.hrl").

-type barrel_create_params() :: #{
  store_provider => atom(),
  index_policy => list()
}.

-type barrel_infos() :: #{
  updated_seq := non_neg_integer(),
  docs_count := non_neg_integer(),
  docs_del_count := non_neg_integer()
}.

-type fetch_options() :: #{
  history => boolean(),
  max_history => non_neg_integer(),
  rev => barrel_doc:revid(),
  ancestors => [barrel_doc:revid()]
}.

-type prop() :: binary().

-type query_options() :: #{
  order_by => order_by_key |order_by_value,
  equal_to => prop(),
  start_at => prop(),
  next_to => prop(),
  end_at => prop(),
  previous_to => prop(),
  include_deleted => true | false
}.

-type fold_docs_options() :: #{
  equal_to => prop(),
  start_at => prop(),
  next_to => prop(),
  end_at => prop(),
  previous_to => prop(),
  include_deleted => boolean(),
  history => boolean(),
  max_history => non_neg_integer()
}.


-type fold_changes_options() :: #{
  include_doc => true | false,
  with_history => true | false
}.

-type save_options() :: #{
  all_or_nothing => boolean()
}.



start_store(Name, Module, Options) ->
  _ = code:ensure_loaded(Module),
  case erlang:function_exported(Module, init_store, 2) of
    true ->
      Module:init_store(Name, Options);
    false ->
      erlang:exit(badarg)
  end.

stop_store(Name) ->
  barrel_services:deactivate_service(store, Name).


%% @doc create a barrel, (note: for now the options is an empty map)
-spec create_barrel(Name :: barrel_name(), Params :: barrel_create_params()) -> ok | {error, any()}.
create_barrel(Name, Params) ->
  barrel_db:create_barrel(Name, Params).

-spec open_barrel(Name :: barrel_name()) -> {ok, barrel()} | {error, barrel_not_found} | {error, any()}.
open_barrel(Name) ->
  barrel_db:open_barrel(Name).

-spec close_barrel(Name :: barrel_name()) -> ok.
close_barrel(Name) ->
  barrel_db:close_barrel(Name).

-spec delete_barrel(Name :: barrel_name()) -> ok.
delete_barrel(Name) ->
  barrel_db:delete_barrel(Name).
  
%% @doc return barrel_infos.
-spec barrel_infos(Barrel :: barrel()) -> barrel_infos().
barrel_infos(Barrel) ->
  barrel_db:barrel_infos(Barrel).


%% @doc lookup a doc by its docid.
-spec fetch_doc(Barrel, DocId, Options) -> FetchResult when
  Barrel :: barrel_name(),
  DocId :: barrel_doc:docid(),
  Options :: fetch_options(),
  FetchResult :: {ok, Doc :: barrel_doc:doc()} | {error, not_found} | {error, term()}.
fetch_doc(Barrel, DocId, Options) ->
  barrel_db:fetch_doc(Barrel, DocId, Options).

%% @doc create or replace a doc.
%% Barrel will try to create a document if no `rev' property is passed to the document
%% if the `_deleted' property is given the doc will be deleted.
%%
%% conflict rules:
%%  - if the user try to create a doc that already exists, a conflict will be returned, if the doc is not deleted
%%  - if the user try to update with a revision that doesn't correspond to a leaf of the revision tree, a
%%    conflict will be returned as well
%%  - if the user try to replace a doc that has been deleted, a not_found error will be returned
-spec save_doc(Name, Doc) -> SaveResult when
  Name :: barrel_name(),
  Doc :: barrel_doc:doc(),
  DocId :: barrel_doc:docid(),
  RevId :: barrel_doc:revid(),
  DocError :: {conflict, revision_conflict} | {conflict, doc_exists},
  SaveResult :: {ok, DocId , RevId} | {error, DocError} | {error, db_not_found}.
save_doc(Barrel, Doc) ->
  {ok, [Res]} = barrel_db:update_docs(Barrel, [Doc], #{}, interactive_edit),
  Res.


%% @doc delete a document, it doesn't delete the document from the filesystem
%% but instead create a tombstone that allows barrel to replicate a deletion.
-spec delete_doc(Name, DocId, RevId) -> DeleteResult when
  Name :: barrel(),
  DocId :: barrel_doc:docid(),
  RevId :: barrel_doc:revid(),
  DocError :: {conflict, revision_conflict} | {conflict, doc_exists},
  DeleteResult :: {ok, DocId , RevId} | {error, DocError} | {error, db_not_found}.
delete_doc(Barrel, DocId, Rev) ->
  {ok, [Res]} =  barrel_db:update_docs(
    Barrel,
    [#{ <<"id">> => DocId, <<"_rev">> => Rev, <<"_deleted">> => true}],
    #{}, interactive_edit
  ),
  Res.

%% @doc likee save_doc but create or replace multiple docs at once.
-spec save_docs(Name, Docs) -> SaveResults when
  Name :: barrel(),
  Docs :: [barrel_doc:doc()],
  DocId :: barrel_doc:docid(),
  RevId :: barrel_doc:revid(),
  DocError :: {conflict, revision_conflict} | {conflict, doc_exists},
  SaveResult :: {ok, DocId , RevId} | {error, DocError} | {error, db_not_found},
  SaveResults :: {ok, [SaveResult]}.
save_docs(Barrel, Docs) ->
  save_docs(Barrel, Docs, #{}).

-spec save_docs(Name, Docs, Options) -> SaveResults when
  Name :: barrel(),
  Docs :: [barrel_doc:doc()],
  Options :: save_options(),
  DocId :: barrel_doc:docid(),
  RevId :: barrel_doc:revid(),
  DocError :: {conflict, revision_conflict} | {conflict, doc_exists},
  SaveResult :: {ok, DocId , RevId} | {error, DocError} | {error, db_not_found},
  SaveResults :: {ok, [SaveResult]}.
save_docs(Barrel, Docs, Options) ->
  barrel_db:update_docs(Barrel, Docs, Options, interactive_edit).

-spec save_replicated_docs(Name, Docs) -> SaveResult when
  Name :: barrel(),
  Docs :: [barrel_doc:doc()],
  SaveResult :: ok.
save_replicated_docs(Barrel, Docs) ->
  barrel_db:update_docs(Barrel, Docs, #{}, replicated_changes).

%% @doc delete multiple docs
-spec delete_docs(Name, DocsOrDocsRevId) -> SaveResults when
  Name :: barrel_name(),
  DocsOrDocsRevId :: [ barrel_doc:doc() | {barrel_doc:docid(), barrel_doc:revid()}],
  DocId :: barrel_doc:docid(),
  RevId :: barrel_doc:revid(),
  DocError :: {conflict, revision_conflict} | {conflict, doc_exists},
  SaveResult :: {ok, DocId , RevId} | {error, DocError} | {error, db_not_found},
  SaveResults :: {ok, [SaveResult]}.
delete_docs(Barrel, DocsOrDocsRevId) ->
  Docs = lists:map(
    fun
      (#{ <<"id">> := _, <<"_deleted">> := true, <<"_rev">> := _ }=Doc) -> Doc;
      ({DocId, Rev})-> #{ <<"id">> => DocId, <<"_rev">> => Rev, <<"_deleted">> => true };
      (_) -> erlang:error(badarg)
    end,
    DocsOrDocsRevId
  ),
  save_docs(Barrel, Docs).

-spec fold_docs(Name, Fun, AccIn, Options) -> AccOut when
  Name :: barrel_name(),
  AccResult :: {ok, Acc2 :: any()} | {stop, Acc2 :: any()} | {skip, Acc2 :: any()} | stop | ok | skip,
  Fun :: fun( (Doc :: barrel_doc:doc(), Acc1 :: any() ) -> AccResult ),
  AccIn :: any(),
  Options :: fold_docs_options(),
  AccOut :: any().
fold_docs(Barrel, Fun, AccIn, Options) ->
  barrel_db:fold_docs(Barrel, Fun, AccIn, Options).

-spec fold_changes(Name, Since, Fun, AccIn, Options) -> AccOut when
  Name :: barrel_name(),
  Since :: non_neg_integer(),
  AccResult :: {ok, Acc2 :: any()} | {stop, Acc2 :: any()} | {skip, Acc2 :: any()} | ok | stop | skip,
  Fun :: fun( (Doc :: barrel_doc:doc(), Acc1 :: any() ) -> AccResult ),
  AccIn :: any(),
  Options :: fold_changes_options(),
  AccOut :: any().
fold_changes(Barrel, Since, Fun, AccIn, Options) ->
  barrel_db:fold_changes(Barrel, Since, Fun, AccIn, Options).


%% @doc query the barrel indexes
%%
%% To query all docs just pass "/" or "/id"
-spec fold_path(Name, Path, Fun, AccIn, Options) -> AccOut when
  Name :: barrel_name(),
  Path :: binary(),
  AccResult :: {ok, Acc2 :: any()} | {stop, Acc2 :: any()} | {skip, Acc2 :: any()} | ok | stop | skip,
  Fun :: fun( (Doc :: barrel_doc:doc(), Acc1 :: any() ) -> AccResult ),
  AccIn :: any(),
  Options :: query_options(),
  AccOut :: any().
fold_path(Barrel, Path, Fun, Acc, Options) ->
  barrel_db:fold_path(Barrel, Path, Fun, Acc, Options).



start_link(Name, StoreMod, Args) ->
  proc_lib:start_link(?MODULE, init, [self(), Name, StoreMod, Args]).


init([Parent, Name, StoreMod, Args]) ->
  case register_barrel(Name) of
    true ->
      %% TODO: support Erlang 21 only?
      case try_init_barrel(Name, StoreMod, Args) of
        {ok, {ok, Store}} ->
          ok = validate_state(Store),
          UpdatedSeq = last_seq_(Store),
          Buffer = barrel_buffer_ets:new(),
          Tid = ets:new(barrel_transactions, [public, set]),
          Writer = start_writer(Buffer, Tid, Store, UpdatedSeq),
          State = #{name => Name,
                    store => Store,
                    buffer => Buffer,
                    size => 0,
                    writer => Writer,
                    writer_state => iddle,
                    transactions => Tid},
          proc_lib:init_ack({ok, self()}),
          gen_server:enter_loop(?MODULE, [], State);

        {ok, {error, Reason}} ->
          gproc:unreg(?barrel(Name)),
          proc_lib:init_ack(Parent, {error, Reason}),
          exit(Reason);
        {'EXIT', Class, Reason, Stacktrace} ->
          proc_lib:init_ack(Parent, {error, terminate_reason(Class, Reason, Stacktrace)}),
          erlang:raise(Class, Reason, Stacktrace)
      end;
    {false, Pid} ->
      proc_lib:init_ack(Parent, {error, {already_started, Pid}})
  end.


handle_call({save_docs, Timestamp, Docs, LocalDocs, Policy}, From, State) ->
  #{ operations := Tid } = State,
  Ref = erlang:make_ref(),
  Entry  = new_entry(From, Timestamp, {save_docs, Docs, LocalDocs, Policy}),
  Sz = erlang_term:byte_size(Entry),
  case enqueue(Entry, Sz, Timestamp, State) of
    {ok, NState} ->
      ok = track_transaction(Tid, Ref, {pending, {From, Ref}, Sz, Timestamp}),
      ok = maybe_wake_up_writer(NState),
      {noreply, NState};
    buffer_full ->
      {reply, {error, buffer_full}, State}
  end;

handle_call(Request, _From, State) ->
  ?LOG_ERROR("Unknown call request: ~p", [Request]),
  Reply = ok,
  {reply, Reply, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info({modify_docs, _From, Timestamp, Op}, State) ->
  %Entry  = new_entry(From, Timestamp, {modify_docs, Op}),
  Tid = maps:get(operations, State),
  ok = track_transaction(Tid, Timestamp, {pending, Op}),
  
  {noreply, State};
  

handle_info({updated, Ref, LastSeq}, State) ->
  NState = untrack_transaction(Ref, State),
  {noreply, NState#{ updated_seq => LastSeq }}.
  

terminate(_Reason, #{ name := Name, store := Store }) ->
  gproc:unreg(?barrel(Name)),
  ok = do_close_store(Store),
  ok.


try_init_barrel(Name, StoreMod, Args) ->
  try
    {ok, StoreMod:init_barrel(Name, Args)}
  catch
      throw:R  -> {ok, R};
    Class:R:E ->
      {'EXIT', Class, R, E}
  end.


%% TODO: check behaviour
validate_state(#{ mod := Mod }) when is_atom(Mod) ->
  ok;
validate_state(_) ->
  erlang:error(badarg).


terminate_reason(error, Reason, Stacktrace) -> {Reason, Stacktrace};
terminate_reason(exit, Reason, _Stacktrace) -> Reason.


register_barrel(Name) ->
  try gproc:reg(?barrel(Name)), true
  catch
    error:_ ->
      {false, gproc:where(?barrel(Name))}
  end.

do_close_store(#{ mod := Mod } = Store) ->
  Mod:close(Store).


last_seq_(#{ mod := Mod } = Store) ->
  Mod:updated_seq(Store).

start_writer(Buffer, Tid, Store, UpdatedSeq) ->
  barrel_db_writer:start_link(self(), Buffer, Tid, Store, UpdatedSeq).


maybe_wake_up_writer(#{ writer := Pid }) ->
  case process_info(Pid, current_function) of
    {current_function,{erlang,hibernate,3}} ->
      Pid ! wakeup,
      ok;
    _ ->
      ok
  end.

new_entry(From, Timestamp, OP) ->
  {From, Timestamp, OP}.


enqueue(Term, TermSize, Ts, #{ buffer := Buffer, size :=  Sz, max_size := Max} = State) ->
  Sz2 = TermSize + Sz,
  if
    Sz2 =< Max ->
      barrel_buffer_ets:in(Term, Ts, Buffer),
      State#{ size => Sz2 };
    true ->
      buffer_full
  end.

untrack_transaction(Ref, #{ operations := Tid, size := Size} = State) ->
  true = erlang:demonitor(Ref, [flush]),
  [{Ref, {_, _, Sz, _}}] = ets:take(Tid, Ref),
  State#{ size => (Size - Sz)}.

track_transaction(Tab, Ref, State) ->
  true = ets:insert_new(Tab, {Ref, State}),
  ok.