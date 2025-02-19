%% Copyright 2016, Benoit Chesneau
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

-module(barrel).
-author("benoitc").


%% Database API

-export([
  database_names/0,
  create_database/1,
  delete_database/1,
  database_infos/1
]).

%% DOC API

-export([
  put/3,
  put_rev/5,
  get/3,
  multi_get/5,
  delete/3,
  post/3,
  fold_by_id/4,
  revsdiff/3,
  write_batch/3,
  purge_doc/2
]).

-export([object_id/0, object_id/1]).

-export([
  put_system_doc/3,
  get_system_doc/2,
  delete_system_doc/2
]).

%% CHANGE API

-export([
  changes_since/4, changes_since/5,
  subscribe_changes/3,
  await_change/1, await_change/2,
  unsubscribe_changes/1
]).

-export([
  walk/5
]).

-export([
  find_by_key/5
]).


-export([
  start_replication/1,
  stop_replication/1,
  delete_replication/1,
  replication_info/1
]).


-export([
  start_changes_listener/2,
  stop_changes_listener/1,
  get_changes/1
]).

-type dbname() :: binary().
-type db() :: binary().

%% TODO: to define
-type db_infos() :: #{
  name := binary(),
  id := binary(),
  docs_count := non_neg_integer(),
  last_update_seq := non_neg_integer(),
  system_docs_count := non_neg_integer(),
  last_index_seq => non_neg_integer()
}.

-type doc() :: map().
-type meta() :: map().
-type rev() :: binary().
-type docid() :: binary().

-type revid() :: binary().

-type revinfo() :: #{
  id := revid(),
  parent := revid(),
  deleted => boolean()
}.

-type revtree() :: #{ revid() => revinfo() }.

-type docinfo() :: #{
  id := docid(),
  current_rev := revid(),
  branched := boolean(),
  conflict := boolean(),
  revtree := revtree()
}.

-type read_options() :: #{
  rev => rev(),
  history => boolean(),
  max_history => integer(),
  ancestors => [rev()]
}.

-type write_options() :: #{
  async => boolean(),
  timeout => integer(),
  rev => rev()
}.

-type conflict() ::
  {conflict, doc_exists}
  | {conflict, revision_conflict}.

%% TODO: to define
-type fold_options() :: map().

-type change() :: #{
  id := docid(),
  seq := non_neg_integer(),
  changes := [revid()],
  revtree => revtree(),
  doc => doc()
}.

-type batch_options() :: #{
  async => boolean()
}.

-type batch_results() :: [
  {ok, docid(), revid()}
  | {error, not_found}
  | {error, {conflict, doc_exists}}
  | {error, {conflict, revision_conflict}}
  | {error, any()}
].


-export_type([
  dbname/0,
  db/0,
  doc/0,
  rev/0,
  docid/0,
  read_options/0,
  write_options/0,
  revid/0,
  revinfo/0,
  revtree/0,
  docinfo/0
]).

-include("barrel.hrl").


database_names() ->
  barrel_store:databases().

create_database(Config) ->
  barrel_store:create_db(Config).

delete_database(DbId) ->
  barrel_store:delete_db(DbId).

-spec database_infos(Db::db()) ->  {ok, DbInfos::db_infos()} | {error, term()}.
database_infos(Db) ->
  barrel_db:infos(Db).

%% @doc retrieve a document by its key
-spec get(Db, DocId, Options) -> Res when
  Db::db(),
  DocId :: docid(),
  Options :: read_options(),
  Doc :: doc(),
  Meta :: meta(),
  Res :: {ok, Doc, Meta} | {error, not_found} | {error, any()}.
get(Db, DocId, Options) ->
  barrel_db:get(Db, DocId, Options).


%% @doc retrieve several documents
-spec multi_get(Db, Fun, AccIn, DocIds, Options) -> Res when
    Db::db(),
    Fun :: fun((doc(), meta(), any() ) -> Res),
    AccIn :: any(),
    DocIds :: [docid()],
    Options :: read_options(),
    Res :: any().
multi_get(Db, Fun, AccIn, DocIds, Options) ->
  barrel_db:multi_get(Db, Fun, AccIn, DocIds, Options).

%% @doc create or update a document. Return the new created revision
%% with the docid or a conflict.
-spec put(Db, Doc, Options) -> Res when
  Db::db(),
  Doc :: doc(),
  Options :: write_options(),
  Res :: {ok, docid(), rev()} | {error, conflict()} | {error, any()}.
put(Db, Doc, Options) when is_map(Doc) ->
  Rev = maps:get(rev, Options, <<>>),
  Async = maps:get(async, Options, false),
  CreateIfMissing = maps:get(create_if_missing, Options, false),
  Batch = barrel_write_batch:put(Doc, CreateIfMissing, Rev, barrel_write_batch:new(Async)),
  update_doc(Db, Batch);
put(_,  _, _) ->
  erlang:error(badarg).

%% @doc insert a specific revision to a a document. Useful for the replication.
%% It takes the document id, the doc to edit and the revision history (list of ancestors).
-spec put_rev(Db, Doc, History, Deleted, Options) -> Res when
  Db::dbname(),
  Doc :: doc(),
  History :: [rev()],
  Deleted :: boolean(),
  Options :: write_options(),
  Res ::  {ok, docid(), rev()} | {error, conflict()} | {error, any()}.
put_rev(Db, Doc, History, Deleted, Options) when is_map(Doc) ->
  Async = maps:get(async, Options, false),
  Batch = barrel_write_batch:put_rev(Doc, History, Deleted, barrel_write_batch:new(Async)),
  update_doc(Db, Batch);
put_rev(_, _, _, _, _) ->
  erlang:error(badarg).

%% @doc delete a document
-spec delete(Db, DocId, Options) -> Res when
  Db::db(),
  DocId :: docid(),
  Options :: write_options(),
  Res :: {ok, docid(), rev()} | {error, conflict()} | {error, any()}.
delete(Db, DocId, Options) ->
  Async = maps:get(async, Options, false),
  Rev = maps:get(rev, Options, <<>>),
  Batch = barrel_write_batch:delete(DocId, Rev, barrel_write_batch:new(Async)),
  update_doc(Db, Batch).

%% @doc create a document . Like put but only create a document without updating the old one.
%% Optionally the document ID can be set in the doc.
-spec post(Db, Doc, Options) -> Res when
  Db::db(),
  Doc :: doc(),
  Options :: write_options(),
  Res :: ok | {ok, docid(), rev()} | {error, conflict()} | {error, any()}.
post(Db, Doc, Options) ->
  Async = maps:get(async, Options, false),
  IsUpsert = maps:get(is_upsert, Options, false),
  Batch = barrel_write_batch:post(Doc, IsUpsert, barrel_write_batch:new(Async)),
  update_doc(Db, Batch).

update_doc(Db, Batch) ->
  Result = barrel_db:update_docs(Db, Batch),
  case Result of
    ok -> ok;
    [Res] -> Res;
    Error -> Error
  end.

%% @doc Apply the specified updates to the database.
%% Note: The batch is not guaranteed to be atomic, atomicity is only guaranteed at the doc level.
-spec write_batch(Db, Updates, Options) -> Results when
  Db :: db(),
  Updates :: [barrel_write_batch:batch_op()],
  Options :: batch_options(),
  Results :: batch_results() | ok.
write_batch(Db, Updates, Options) when is_map(Options) ->
  Async = maps:get(async, Options, false),
  Batch = barrel_write_batch:from_list(Updates, Async),
  barrel_db:update_docs(Db, Batch);
write_batch(_, _, _) -> erlang:error(badarg).


%% @doc completely remove a doc from the database
-spec purge_doc(Db :: db(), DocId :: docid()) -> ok | {error, any()}.
purge_doc(Db, DocId) ->
  barrel_db:purge_doc(Db, DocId).

%% @doc fold all docs by Id
-spec fold_by_id(Db, Fun, AccIn, Options) -> AccOut | Error when
  Db::db(),
  FunRes :: {ok, Acc2::any()} | stop | {stop, Acc2::any()},
  Fun :: fun((Doc :: doc(), Meta :: meta(), Acc1 :: any()) -> FunRes),
  Options :: fold_options(),
  AccIn :: any(),
  AccOut :: any(),
  Error :: {error, term()}.
fold_by_id(Db, Fun, Acc, Options) ->
  barrel_db:fold_by_id(Db, Fun, Acc, Options).

%% @doc get all revisions ids that differ in a doc from the list given
-spec revsdiff(Db, DocId, RevIds) -> Res when
  Db::dbname(),
  DocId :: docid(),
  RevIds :: [revid()],
  Res:: {ok, Missing :: [revid()], PossibleAncestors :: [revid()]} | {error, any()}.
revsdiff(Db, DocId, RevIds) ->
  barrel_db:revsdiff(Db, DocId, RevIds).


%% @doc return a new 16-bytes object ID. an Object ID value consist of:
%% * a 8-bytes value representing the milliseconds since the Unix epoch,
%% * a 6-bytes value corresponding to the node ID (2-bytes) and the erlang pid (3-bytes)
%% * a 2-bytes counter.
-spec object_id() -> Id :: binary().
object_id() -> barrel_id:id().


%% @doc generate a new 16-bytes object ID and return it in a different base.
-spec object_id(Base :: non_neg_integer()) -> Id :: binary().
object_id(Base) -> barrel_id:binary_id(Base).

%% ==============================
%% system docs operations

put_system_doc(Db, DocId, Doc) ->
  barrel_db:put_system_doc(Db, DocId, Doc).

get_system_doc(Db, DocId) ->
  barrel_db:get_system_doc(Db, DocId).

delete_system_doc(Db, DocId) ->
  barrel_db:delete_system_doc(Db, DocId).
%% ==============================
%% changes operations

%% @doc fold all changes since last sequence
-spec changes_since(Db, Since, Fun, AccIn) -> AccOut when
  Db::dbname(),
  Since :: non_neg_integer(),
  FunRes :: {ok, Acc2::any()} | stop | {stop, Acc2::any()},
  Fun :: fun((Change :: change(), Acc :: any()) -> FunRes),
  AccIn :: any(),
  AccOut :: any().
changes_since(Db, Since, Fun, Acc) ->
  changes_since(Db, Since, Fun, Acc, #{}).

%% @doc fold all changes since last sequence
-spec changes_since(Db, Since, Fun, AccIn, Opts) -> AccOut when
  Db::dbname(),
  Since :: non_neg_integer(),
  FunRes :: {ok, Acc2::any()} | stop | {stop, Acc2::any()},
  Fun :: fun((Change :: change(), Acc :: any()) -> FunRes),
  AccIn :: any(),
  AccOut :: any(),
  Opts :: map().
changes_since(Db, Since, Fun, Acc, Opts) ->
  barrel_db:changes_since(Db, Since, Fun, Acc, Opts).

subscribe_changes(DbId, Since, Options) ->
  {ok, Pid} = barrel_changes_sup:start_consumer(self(), DbId, Since, Options),
  _ = erlang:put({Pid, last_seq},  Since),
  Pid.

await_change(Pid) -> await_change(Pid, infinity).

await_change(Pid, Timeout) ->
  MRef = erlang:monitor(process, Pid),
  receive
    {change, Pid, Change} ->
      Seq = maps:get(<<"seq">>, Change),
      OldSeq = erlang:get({Pid, last_seq}),
      _ = erlang:put({Pid, last_seq}, erlang:max(OldSeq, Seq)),
      Change;
    {'DOWN', MRef, process, _, normal} ->
      LastSeq = erlang:erase({Pid, last_seq}),
      {end_stream, normal, LastSeq};
    {'DOWN', MRef, process, _, Reason} ->
      LastSeq = erlang:erase({Pid, last_seq}),
      {end_stream, {error, Reason}, LastSeq}
  after Timeout ->
    {end_stream, timeout}
  end.

unsubscribe_changes(Pid) ->
  _ = barrel_changes_sup:stop_consumer(Pid),
  LastSeq = erlang:erase({Pid, last_seq}),
  {ok, LastSeq}.

%% ==============================
%% index operations


%% @doc find in the index a document by its path
-spec walk(Db, Path, Fun, AccIn, Options) -> AccOut | Error when
  Db::db(),
  Path :: binary(),
  FunRes :: {ok, Acc2::any()} | stop | {stop, Acc2::any()},
  Fun :: fun((DocId :: docid(), Doc :: doc(), Acc1 :: any()) -> FunRes),
  Options :: fold_options(),
  AccIn :: any(),
  AccOut :: any(),
  Error :: {error, term()}.
walk(Db, Path, Fun, AccIn, Opts) ->
  barrel_db:walk(Db, Path, Fun, AccIn, Opts).


%% @deprecated
find_by_key(Db, Path, Fun, AccIn, Opts) ->
  _ = lager:warning("~s : find_by_key is deprecated", [?MODULE_STRING]),
  barrel:walk(Db, Path, Fun, AccIn, Opts).


%% ==============================
%% replication

start_replication(Config) ->
  barrel_replicate:start_replication(Config).

stop_replication(Name) ->
  barrel_replicate:stop_replication(Name).

delete_replication(Name) ->
  barrel_replicate:delete_replication(Name).

replication_info(Name) ->
  case barrel_replicate:where(Name) of
    Pid when is_pid(Pid) -> barrel_replicate_task:info(Pid);
    undefined -> {error, not_found}
  end.



%% DEPRECATED CHANGES API

start_changes_listener(DbId, Options) ->
  barrel_changes_listener:start_link(DbId, Options).

stop_changes_listener(ListenerPid) ->
  barrel_changes_listener:stop(ListenerPid).

get_changes(ListenerPid) ->
  barrel_changes_listener:changes(ListenerPid).
