%% Copyright (c) 2018. Benoit Chesneau
%%
%% Licensed under the Apache License, Version 2.0 (the "License"); you may not
%% use this file except in compliance with the License. You may obtain a copy of
%% the License at
%%)
%%    http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
%% License for the specific language governing permissions and limitations under
%% the License.

-module(barrel_storage).
-author("benoitc").

%% barrel API

%% doc api
-export([
  id/1,
  docs_count/1,
  del_docs_count/1,
  updated_seq/1,
  resource_id/1,
  get_doc_infos/2,
  get_revision/3,
  write_docs_infos/4,
  write_revision/4,
  add_mutations/2,
  commit/1,
  fold_changes/4,
  fold_docs/4,
  fold_path/5,
  indexed_seq/1,
  set_indexed_seq/2,
  get_local_doc/2,
  drop_barrel/1,
  close_barrel/1,
  terminate_barrel/1
]).


-export([default_dir/0, data_dir/0]).

-include("barrel.hrl").

-include_lib("stdlib/include/ms_transform.hrl").

id(#{ engine := {Mod, State} }) ->
  Mod:id(State).

docs_count(#{ engine := {Mod, State} }) ->
  Mod:docs_count(State).

del_docs_count(#{ engine := {Mod, State} }) ->
  Mod:del_docs_count(State).

updated_seq(#{ engine := {Mod, State} }) ->
  Mod:updated_seq(State).

indexed_seq(#{ engine := {Mod, State} }) ->
  Mod:indexed_seq(State).

resource_id(#{ engine := {Mod, State} }) ->
  Mod:resource_id(State).

get_local_doc(#{ engine := {Mod, State} }, DocId) ->
  Mod:get_local_doc(State, DocId).

get_doc_infos(#{ engine := {Mod, State} }, DocId) ->
  Mod:get_doc_infos(DocId, State).

get_revision(_, _, <<"">>) -> #{};
get_revision(#{ engine := {Mod, State} }, DocId, Rev) ->
  Mod:get_revision(DocId, Rev, State).


write_revision(#{ engine := {Mod, State} }, DocId, Rev, Body) ->
  Mod:write_revision(DocId, Rev, Body, State).

write_docs_infos(#{ engine := {Mod, State} } = Db, DIPairs, LocalDocs, PurgedIdsRevs) ->
  {ok, NState} = Mod:write_docs_infos(DIPairs, LocalDocs, PurgedIdsRevs, State),
  {ok, Db#{ engine => {Mod, NState}}}.

add_mutations(#{ engine := {Mod, State} }, Mutations) ->
  Mod:add_mutations(Mutations, State).

commit(#{ engine := {Mod, State} } = Db) ->
  NState = Mod:commit(State),
  Db#{ engine => {Mod, NState} }.

set_indexed_seq(#{ engine := {Mod, State} } = Db, Seq) ->
  {ok, NState} =  Mod:set_indexed_seq(Seq, State),
  Db#{ engine => {Mod, NState}}.


fold_docs(#{ engine := {Mod, State} }, Fun, Acc, Options) ->
  Mod:fold_docs(Fun, Acc, Options, State).

fold_changes(#{ engine := {Mod, State} }, Since, Fun, Acc) ->
  Mod:fold_changes(Since, Fun, Acc, State).

fold_path(#{ engine := {Mod, State} }, Path, Fun, Acc, Options) ->
  Mod:fold_path(Path, Fun, Acc, Options, State).

drop_barrel(#{ engine := {Mod, State} }) ->
  Mod:drop_barrel(State).

close_barrel(#{ engine := {Mod, State} }) ->
  Mod:close_barrel(State).

terminate_barrel(#{ engine := {Mod, State} }) ->
  Mod:terminate_barrel(State).

default_dir() ->
  filename:join([?DATA_DIR, node()]).

-spec data_dir() -> string().
data_dir() ->
  Dir = application:get_env(barrel, data_dir, default_dir()),
  _ = filelib:ensure_dir(filename:join([".", Dir, "dummy"])),
  Dir.