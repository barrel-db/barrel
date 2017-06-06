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

%% DOC API

-export([
  put/3,
  put_rev/5,
  get/3,
  multi_get/5,
  delete/3,
  post/3,
  fold_by_id/4,
  changes_since/4,
  changes_since/5,
  revsdiff/3,
  write_batch/3
]).

-export([
  put_system_doc/3,
  get_system_doc/2,
  delete_system_doc/2
]).

-export([
  walk/5
]).

-export([
  find_by_key/5
]).

%% Database API

-export([
  create_db/1,
  create_db/2,
  delete_db/1,
  db_infos/1,
  connect/1
]).

-export([
  database_infos/1
]).

-export([
  start_replication/3,
  start_replication/4,
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
-type db() :: atom().

%% TODO: to define
-type db_infos() :: #{
  name := db(),
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

-type read_options() :: [
  {rev, rev()}
  | {history, boolean()}
  | {max_history, integer()}
  | {ancestors, [rev()]}
].

-type write_options() :: [
  {async, boolean()}
  | {timeout, integer()}
  | {rev, rev()}
].

-type conflict() ::
  {conflict, doc_exists}
  | {conflict, revision_conflict}.

%% TODO: to define
-type fold_options() :: list().

-type change() :: #{
  id := docid(),
  seq := non_neg_integer(),
  changes := [revid()],
  revtree => revtree(),
  doc => doc()
}.

-type batch_options() :: [
  {async, boolean()}
].

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

-deprecated([create_db/2]).

-include("barrel.hrl").

create_db(DbId, Config) ->
  _ = lager:warning("barrel_db:create/2 is deprecated", []),
  create_db(Config#{ <<"database_id">> => DbId }).

create_db(Config) -> barrel_store:create_db(Config).


delete_db(DbId) -> barrel_store:delete_db(DbId).

-spec db_infos(Db::db()) ->  {ok, DbInfos::db_infos()} | {error, term()}.
db_infos(Db) ->  barrel_db:infos(Db).

%% new db handling api
connect(DbId) -> {ok, DbId}.

database_infos(Db) -> barrel_db:infos(Db).



%% Database API.

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
  Rev = proplists:get_value(rev, Options, <<>>),
  Async = proplists:get_value(async, Options, false),
  Batch = barrel_write_batch:put(Doc, Rev, barrel_write_batch:new(Async)),
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
  Async = proplists:get_value(async, Options, false),
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
  Async = proplists:get_value(async, Options, false),
  Rev = proplists:get_value(rev, Options, <<>>),
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
  Async = proplists:get_value(async, Options, false),
  IsUpsert = proplists:get_value(is_upsert, Options, false),
  Batch = barrel_write_batch:post(Doc, IsUpsert, barrel_write_batch:new(Async)),
  update_doc(Db, Batch).

update_doc(Db, Batch) ->
  Result = barrel_db:update_docs(Db, Batch),
  case Result of
    ok -> ok;
    [Res] -> Res
  end.

%% @doc Apply the specified updates to the database.
%% Note: The batch is not guaranteed to be atomic, atomicity is only guaranteed at the doc level.
-spec write_batch(Db, Updates, Options) -> Results when
  Db :: db(),
  Updates :: [barrel_write_batch:batch_op()],
  Options :: batch_options(),
  Results :: batch_results() | ok.
write_batch(Db, Updates, Options) when is_list(Options) ->
  Async = proplists:get_value(async, Options, false),
  Batch = barrel_write_batch:from_list(Updates, Async),
  barrel_db:update_docs(Db, Batch);
write_batch(_, _, _) -> erlang:error(badarg).

put_system_doc(DbName, DocId, Doc) ->
  barrel_db:put_system_doc(DbName, DocId, Doc).

get_system_doc(DbName, DocId) ->
  barrel_db:get_system_doc(DbName, DocId).

delete_system_doc(DbName, DocId) ->
  barrel_db:delete_system_doc(DbName, DocId).

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

%% @doc fold all changes since last sequence
-spec changes_since(Db, Since, Fun, AccIn) -> AccOut when
  Db::dbname(),
  Since :: non_neg_integer(),
  FunRes :: {ok, Acc2::any()} | stop | {stop, Acc2::any()},
  Fun :: fun((Change :: change(), Acc :: any()) -> FunRes),
  AccIn :: any(),
  AccOut :: any().
changes_since(Db, Since, Fun, Acc) ->
  barrel_db:changes_since(Db, Since, Fun, Acc, []).

%% @doc fold all changes since last sequence
-spec changes_since(Db, Since, Fun, AccIn, Opts) -> AccOut when
  Db::dbname(),
  Since :: non_neg_integer(),
  FunRes :: {ok, Acc2::any()} | stop | {stop, Acc2::any()},
  Fun :: fun((Change :: change(), Acc :: any()) -> FunRes),
  AccIn :: any(),
  AccOut :: any(),
  Opts :: list().
changes_since(Db, Since, Fun, Acc, Opts) ->
  barrel_db:changes_since(Db, Since, Fun, Acc, Opts).

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

%% @doc get all revisions ids that differ in a doc from the list given
-spec revsdiff(Db, DocId, RevIds) -> Res when
  Db::dbname(),
  DocId :: docid(),
  RevIds :: [revid()],
  Res:: {ok, Missing :: [revid()], PossibleAncestors :: [revid()]}.
revsdiff(Db, DocId, RevIds) ->
  barrel_db:revsdiff(Db, DocId, RevIds).


start_replication(Name, Source, Target) ->
  start_replication(Name, Source, Target, []).

start_replication(Name, Source, Target, Options) ->
  Config = #{source => Source, target => Target, options => Options},
  case barrel_replicate:start_replication(Name, Config) of
    ok -> {ok, Name};
    Error -> Error
  end.

stop_replication(Name) ->
  barrel_replicate:stop_replication(Name).

delete_replication(Name) ->
  barrel_replicate:delete_replication(Name).

replication_info(Name) ->
  case barrel_replicate:where(Name) of
    Pid when is_pid(Pid) -> barrel_replicate_task:info(Pid);
    undefined -> {error, not_found}
  end.



%% CHANGES API

start_changes_listener(DbId, Options) ->
  barrel_changes_listener:start_link(DbId, Options).

stop_changes_listener(ListenerPid) ->
  barrel_changes_listener:stop(ListenerPid).

get_changes(ListenerPid) ->
  barrel_changes_listener:changes(ListenerPid).
