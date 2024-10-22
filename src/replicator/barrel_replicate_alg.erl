%% Copyright 2017, Bernard Notarianni
%% Copyright 2017, Benoit Chesneau
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

-module(barrel_replicate_alg).
-author("Bernard Notarianni").

%% gen_server API
-export([
  replicate/4
]).


replicate(Source, Target, Changes, Metrics) ->
  {ok, Metrics2} = lists:foldl(fun(C, {ok, Acc}) ->
                           sync_change(Source, Target, C, Acc)
                       end, {ok, Metrics}, Changes),
  {ok, Metrics2}.

sync_change(Source, Target, Change, Metrics) ->
  #{<<"id">> := DocId, <<"changes">> := History} = Change,
  {ok, MissingRevisions, _PossibleAncestors} = barrel_replicate_api_wrapper:revsdiff(Target, DocId, History),
  Metrics2 = lists:foldr(fun(Revision, Acc) ->
                             sync_revision(Source, Target, DocId, Revision, Acc)
                         end, Metrics, MissingRevisions),
  {ok, Metrics2}.

sync_revision(Source, Target, DocId, Revision, Metrics) ->
  case read_doc_with_history(Source, DocId, Revision, Metrics) of
    {undefined, undefined, Metrics2} -> Metrics2;
    {Doc, Meta, Metrics2} ->
      _ = lager:info("got ~p (~p)~n", [Doc, Meta]),
      History = barrel_doc:parse_revisions(Meta),
      Deleted = maps:get(<<"deleted">>, Meta, false),
      Metrics3 = write_doc(Target, Doc, History, Deleted, Metrics2),
      Metrics3
  end.

read_doc_with_history(Source, Id, Rev, Metrics) ->
  Get =
    fun() ->
      barrel_replicate_api_wrapper:get(Source, Id, #{rev => Rev, history => true})
    end,
  case timer:tc(Get) of
    {Time, {ok, Doc, Meta}} ->
      Metrics2 = barrel_replicate_metrics:inc(docs_read, Metrics, 1),
      Metrics3 = barrel_replicate_metrics:update_times(doc_read_times, Time, Metrics2),
      {Doc, Meta, Metrics3};
    _ ->
      Metrics2 = barrel_replicate_metrics:inc(doc_read_failures, Metrics, 1),
      {undefined, undefined, Metrics2}
  end.

write_doc(_, undefined, _, _, Metrics) ->
  Metrics;
write_doc(Target, Doc, History, Deleted, Metrics) ->
  PutRev =
    fun() ->
      barrel_replicate_api_wrapper:put_rev(Target, Doc, History, Deleted, #{})
    end,
  case timer:tc(PutRev) of
    {Time, {ok, _, _}} ->
      Metrics2 = barrel_replicate_metrics:inc(docs_written, Metrics, 1),
      Metrics3 = barrel_replicate_metrics:update_times(doc_write_times, Time, Metrics2),
      Metrics3;
    {_, Error} ->
      _ = lager:error(
        "replicate write error on dbid=~p for docid=~p: ~w",
        [Target, maps:get(<<"id">>, Doc, undefined), Error]
      ),
      barrel_replicate_metrics:inc(doc_write_failures, Metrics, 1)
  end.