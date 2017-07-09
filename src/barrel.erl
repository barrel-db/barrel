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
  connect/1,
  disconnect/1, disconnect/2
]).

-export([
  database_names/0, database_names/1,
  create_database/1, create_database/2,
  delete_database/1, delete_database/2,
  database_infos/1, database_infos/2
]).

%% DOC API

-export([
  put/3, put/4,
  put_rev/5, put_rev/6,
  get/3, get/4,
  multi_get/5, multi_get/6,
  delete/3, delete/4,
  post/3, post/4,
  fold_by_id/4, fold_by_id/5,
  revsdiff/3, revsdiff/4,
  write_batch/3, write_batch/4
]).

-export([
  put_system_doc/3, put_system_doc/4,
  get_system_doc/2, get_system_doc/3,
  delete_system_doc/2, delete_system_doc/3
]).

%% CHANGE API

-export([
  changes_since/4, changes_since/5, changes_since/6,
  subscribe_changes/3, subscribe_changes/4,
  await_change/1, await_change/2, await_change/3,
  unsubscribe_changes/1, unsubscribe_changes/2
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

connect(Params) ->
  barrel_remote:connect(Params).

disconnect(Channel) ->
  barrel_remote:disconnect(Channel).

disconnect(Channel, Timeout) ->
  barrel_remote:disconnect(Channel, Timeout).

%% ==============================
%% database operations

database_names() ->
  barrel_local:database_names().

database_names(Channel) ->
  barrel_remote:database_names(Channel).

create_database(Config) ->
  barrel_local:create_database(Config).

create_database(Channel, Config) ->
  barrel_remote:create_database(Channel, Config).

delete_database(DbId) ->
  barrel_local:delete_database(DbId).

delete_database(Channel, DbId) ->
  barrel_remote:delete_database(Channel, DbId).

-spec database_infos(Db::db()) ->  {ok, DbInfos::db_infos()} | {error, term()}.
database_infos(Db) ->
  barrel_local:database_infos(Db).

database_infos(Channel, DbId) ->
  barrel_remote:database_infos(Channel, DbId).


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

get(Channel, DbId, DocId, Options) ->
  barrel_remote:get(Channel, DbId, DocId, Options).

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

multi_get(Channel, DbId, Fun, Acc, DocIds, Options) ->
  barrel_remote:multi_get(Channel, DbId, Fun, Acc, DocIds, Options).

%% @doc create or update a document. Return the new created revision
%% with the docid or a conflict.
-spec put(Db, Doc, Options) -> Res when
  Db::db(),
  Doc :: doc(),
  Options :: write_options(),
  Res :: {ok, docid(), rev()} | {error, conflict()} | {error, any()}.
put(Db, Doc, Options) ->
  barrel_local:put(Db, Doc, Options).

put(Channel, DbId, Doc, Options) ->
  barrel_remote:put(Channel, DbId, Doc, Options).

%% @doc insert a specific revision to a a document. Useful for the replication.
%% It takes the document id, the doc to edit and the revision history (list of ancestors).
-spec put_rev(Db, Doc, History, Deleted, Options) -> Res when
  Db::dbname(),
  Doc :: doc(),
  History :: [rev()],
  Deleted :: boolean(),
  Options :: write_options(),
  Res ::  {ok, docid(), rev()} | {error, conflict()} | {error, any()}.
put_rev(Db, Doc, History, Deleted, Options) ->
  barrel_local:put_rev(Db, Doc, History, Deleted, Options).

put_rev(Channel, DbId, Doc, History, Deleted, Options) ->
  barrel_remote:put_rev(Channel, DbId, Doc, History, Deleted, Options).

%% @doc delete a document
-spec delete(Db, DocId, Options) -> Res when
  Db::db(),
  DocId :: docid(),
  Options :: write_options(),
  Res :: {ok, docid(), rev()} | {error, conflict()} | {error, any()}.
delete(Db, DocId, Options) ->
  barrel_local:delete(Db, DocId, Options).

delete(Channel, DbId, DocId, Options) ->
  barrel_remote:delete(Channel, DbId, DocId, Options).

%% @doc create a document . Like put but only create a document without updating the old one.
%% Optionally the document ID can be set in the doc.
-spec post(Db, Doc, Options) -> Res when
  Db::db(),
  Doc :: doc(),
  Options :: write_options(),
  Res :: ok | {ok, docid(), rev()} | {error, conflict()} | {error, any()}.
post(Db, Doc, Options) ->
  barrel_local:post(Db, Doc, Options).

post(Channel, DbId, Doc, Options) ->
  barrel_remote:post(Channel, DbId, Doc, Options).

%% @doc Apply the specified updates to the database.
%% Note: The batch is not guaranteed to be atomic, atomicity is only guaranteed at the doc level.
-spec write_batch(Db, Updates, Options) -> Results when
  Db :: db(),
  Updates :: [barrel_write_batch:batch_op()],
  Options :: batch_options(),
  Results :: batch_results() | ok.
write_batch(Db, Updates, Options) ->
  barrel_local:write_batch(Db, Updates, Options).

write_batch(Channel, DbId, Updates, Options) ->
  barrel_remote:write_batch(Channel, DbId, Updates, Options).

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
  barrel_local:fold_by_id(Db, Fun, Acc, Options).

fold_by_id(Channel, DbId, Fun, Acc, Options) ->
  barrel_remote:fold_by_id(Channel, DbId, Fun, Acc, Options).

%% @doc get all revisions ids that differ in a doc from the list given
-spec revsdiff(Db, DocId, RevIds) -> Res when
  Db::dbname(),
  DocId :: docid(),
  RevIds :: [revid()],
  Res:: {ok, Missing :: [revid()], PossibleAncestors :: [revid()]}.
revsdiff(Db, DocId, RevIds) ->
  barrel_db:revsdiff(Db, DocId, RevIds).

revsdiff(Channel, DbId, DocId, RevIds) ->
  barrel_remote:revsdiff(Channel, DbId, DocId, RevIds).

%% ==============================
%% system docs operations

put_system_doc(Db, DocId, Doc) ->
  barrel_local:put_system_doc(Db, DocId, Doc).

put_system_doc(Channel, DbId, DocId, Doc) ->
  barrel_remote:put_system_doc(Channel, DbId, DocId, Doc).

get_system_doc(Db, DocId) ->
  barrel_local:get_system_doc(Db, DocId).

get_system_doc(Channel, DbId, DocId) ->
  barrel_remote:get_system_doc(Channel, DbId, DocId).

delete_system_doc(Db, DocId) ->
  barrel_local:delete_system_doc(Db, DocId).

delete_system_doc(Channel, DbId, DocId) ->
  barrel_remote:delete_system_doc(Channel, DbId, DocId).

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
  barrel_local:changes_since(Db, Since, Fun, Acc, Opts).

changes_since(Channel, DbId, Since, Fun, Acc, Options) ->
  barrel_remote:changes_since(Channel, DbId, Since, Fun, Acc, Options).

subscribe_changes(DbId, Since, Options) ->
  barrel_local:subscribe_changes(DbId, Since, Options).

subscribe_changes(Channel, DbId, Since, Options) ->
  barrel_remote:subscribe_changes(Channel, DbId, Since, Options).

await_change(Stream) ->
  barrel_local:await_change(Stream).

await_change(Stream, Timeout) ->
  barrel_local:await_change(Stream, Timeout).

await_change(Channel, StreamRef, Timeout) ->
  barrel_remote:await_change(Channel, StreamRef, Timeout).

unsubscribe_changes(Stream) ->
  barrel_local:unsubscribe_changes(Stream).

unsubscribe_changes(Channel, StreamRef) ->
  barrel_remote:unsubscribe_changes(Channel, StreamRef).


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
