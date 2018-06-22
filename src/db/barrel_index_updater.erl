%% Copyright 2018, Benoit Chesneau
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
-module(barrel_index_updater).
-author("benoitc").

%% API
-export([
  start_link/5
]).

-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2
]).

-define(INDEX_INTERVAL, 100).


start_link(Parent, DbName, Mod, ModState, StartSeq) ->
  gen_server:start_link(?MODULE, [Parent, DbName, Mod, ModState, StartSeq], []).


init([Parent, DbName, Mod, ModState, StartSeq]) ->
  process_flag(trap_exit, true),
  %% subscribe to the database
  Interval = application:get_env(barrel, index_interval, ?INDEX_INTERVAL),
  Stream = #{ barrel => DbName, interval => Interval, include_doc => true},
  ok = barrel_db_stream_mgr:subscribe(Stream, self(), StartSeq),

  _ = lager:info("subscribed stream=~p, since=~p", [Stream, StartSeq]),

  {ok, #{parent => Parent,
         db_name => DbName,
         mod => Mod,
         modstate => ModState,
         stream => Stream,
         last_seq => StartSeq}}.


handle_call(_Msg, _From, State) -> {noreply, State}.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info({changes, Stream, Changes, _Seq}, State = #{ stream := Stream, last_seq := LastSeq }) ->
  IndexedSeq = handle_changes(Changes, State),
  NewState = update_state(IndexedSeq, LastSeq, State),
  {noreply, NewState};
handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, #{ stream := Stream }) ->
  barrel_db_stream_mgr:unsubscribe(Stream, self()),
  ok.


handle_changes(Changes, State = #{ db_name := DbName, last_seq := LastSeq }) ->
  try
    process_changes(Changes, State)
  catch
    exit:pmap_timeout ->
       _ = lager:warning("index timeout db=~p", [DbName]),
      LastSeq;
    exit:Reason ->
      _ = lager:error("index error db=~p error=~p", [DbName, Reason]),
      LastSeq
  end.

num_worker() ->
  application:get_env(barrel, index_threads, 16).
worker_timeout() ->
  application:get_env(barrel, index_timeout, 5000).


update_state(Seq, Seq, State) -> State;
update_state(Seq, _LastSeq, #{ parent := Parent} = State) ->
  Parent ! {updated, Seq},
  State#{ last_seq => Seq }.

process_changes(Changes, #{ mod := Mod, modstate := ModState, last_seq := LastSeq } = State) ->
  Worker =
    fun(Change) ->
      lager:error("got change=~p~n", [Change]),
      #{ <<"id">> := DocId, <<"seq">> := BinSeq, <<"doc">> := Doc } = Change,
      Seq = barrel_db_stream_agent:bin_to_seq(BinSeq),
      OldPaths = case Mod:get_ilog(DocId, ModState) of
                   {ok, Paths} -> Paths;
                   not_found -> []
                 end,
      %% analyze the doc and compare to the old index
      NewPaths = case Doc of
                   null ->
                     %% doc has been deleted, no revision is stored
                     [];
                   _ ->
                     barrel_index:analyze(Doc)
                 end,
      Diff = barrel_index:diff(NewPaths, OldPaths),
      {NewPaths, Diff, DocId, Seq}
    end,
  Results = barrel_lib:pmap(Worker, Changes, num_worker(), worker_timeout()),
  Batch = Mod:get_batch(ModState),
  IndexedSeq = process_diffs(Results, LastSeq, State),
  Mod:commit(Batch, ModState),
  IndexedSeq.


process_diffs([Change | Rest], LastSeq, State) ->
  #{ mod := Mod, modstate := ModState } = State,
  {NewPaths, {Added, Removed}, DocId, Seq} = Change,
  Batch = Mod:get_batch(ModState),
  ok = insert(Added, DocId, Batch),
  ok = unindex(Removed, DocId, Batch),
  _ = log(DocId, NewPaths, Batch),
  Mod:commit(Batch, ModState),
  %% doc has been deleted, no revision is stored
  process_diffs(Rest, erlang:max(LastSeq, Seq), State);
process_diffs([], LastSeq, _State) ->
  LastSeq.

insert([], _DocId, _Batch) ->
  ok;
insert([Path | Rest], DocId, Batch) ->
  _ = insert(Path, DocId, fwd, Batch),
  _ = insert(lists:reverse(Path), DocId, fwd, Batch),
  insert(Rest, DocId, Batch).

unindex([], _DocId, _Batch) ->
  ok;
unindex([Path | Rest], DocId, Batch) ->
  _ = unindex(Path, DocId, fwd, Batch),
  _ = unindex(lists:reverse(Path), DocId, fwd, Batch),
  unindex(Rest, DocId, Batch).


insert(Path, DocId, Type, #{ mod := Mod } = Batch) ->
  Mod:insert(Path, DocId, Type, Batch).

unindex(Path, DocId, Type, #{ mod := Mod } = Batch) ->
  Mod:unindex(Path, DocId, Type, Batch).

log(DocId, Paths, #{ mod := Mod } = Batch) ->
  Mod:log(DocId, Paths, Batch).

