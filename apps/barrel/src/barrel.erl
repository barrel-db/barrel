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
  create_barrel/1,
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
  save_replicated_docs/2,
  put_local_doc/3,
  delete_local_doc/2,
  get_local_doc/2
]).

-include_lib("barrel/include/barrel.hrl").
-include_lib("barrel/include/barrel_logger.hrl").


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
-spec create_barrel(Name :: barrel_name()) -> ok | {error, any()}.
create_barrel(Name) ->
  barrel_db:create_barrel(Name).

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
-spec barrel_infos(Name :: barrel_name()) -> barrel_infos().
barrel_infos(Name) ->
  barrel_db:barrel_infos(Name).


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
  {ok, _} = barrel_db:update_docs(Barrel, Docs, #{}, replicated_changes),
  ok.

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



put_local_doc(Barrel, DocId, Doc) ->
  barrel_db:put_local_doc(Barrel, DocId, Doc).

delete_local_doc(Barrel, DocId) ->
  barrel_db:delete_local_doc(Barrel, DocId).

get_local_doc(Barrel, DocId) ->
  barrel_db:get_local_doc(Barrel, DocId).