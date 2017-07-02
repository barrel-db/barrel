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

-module(barrel_local).
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
  write_batch/3
]).

-export([
  put_system_doc/3,
  get_system_doc/2,
  delete_system_doc/2
]).


-export([
  changes_since/5
]).



-include("barrel.hrl").

%% ==============================
%% database operations

database_names() ->
  barrel_store:databases().

create_database(Config) ->
  barrel_store:create_db(Config).

delete_database(DbId) ->
  barrel_store:delete_db(DbId).

database_infos(Db) -> barrel_db:infos(Db).




%% ==============================
%% doc operations

%% @doc retrieve a document by its key
get(Db, DocId, Options) ->
  barrel_db:get(Db, DocId, Options).


multi_get(Db, Fun, AccIn, DocIds, Options) ->
  barrel_db:multi_get(Db, Fun, AccIn, DocIds, Options).

put(Db, Doc, Options) when is_map(Doc) ->
  Rev = proplists:get_value(rev, Options, <<>>),
  Async = proplists:get_value(async, Options, false),
  Batch = barrel_write_batch:put(Doc, Rev, barrel_write_batch:new(Async)),
  update_doc(Db, Batch);
put(_,  _, _) ->
  erlang:error(badarg).


put_rev(Db, Doc, History, Deleted, Options) when is_map(Doc) ->
  Async = proplists:get_value(async, Options, false),
  Batch = barrel_write_batch:put_rev(Doc, History, Deleted, barrel_write_batch:new(Async)),
  update_doc(Db, Batch);
put_rev(_, _, _, _, _) ->
  erlang:error(badarg).

delete(Db, DocId, Options) ->
  Async = proplists:get_value(async, Options, false),
  Rev = proplists:get_value(rev, Options, <<>>),
  Batch = barrel_write_batch:delete(DocId, Rev, barrel_write_batch:new(Async)),
  update_doc(Db, Batch).

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

write_batch(Db, Updates, Options) when is_list(Options) ->
  Async = proplists:get_value(async, Options, false),
  Batch = barrel_write_batch:from_list(Updates, Async),
  barrel_db:update_docs(Db, Batch);
write_batch(_, _, _) -> erlang:error(badarg).

fold_by_id(Db, Fun, Acc, Options) ->
  barrel_db:fold_by_id(Db, Fun, Acc, Options).

revsdiff(Db, DocId, RevIds) ->
  barrel_db:revsdiff(Db, DocId, RevIds).

%% ==============================
%% system docs operations

put_system_doc(DbName, DocId, Doc) ->
  barrel_db:put_system_doc(DbName, DocId, Doc).

get_system_doc(DbName, DocId) ->
  barrel_db:get_system_doc(DbName, DocId).

delete_system_doc(DbName, DocId) ->
  barrel_db:delete_system_doc(DbName, DocId).

%% ==============================
%% changes operations

changes_since(Db, Since, Fun, Acc, Opts) ->
  barrel_db:changes_since(Db, Since, Fun, Acc, Opts).







