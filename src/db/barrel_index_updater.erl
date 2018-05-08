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
  start_link/4
]).

-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2
]).

-define(INDEX_INTERVAL, 100).


start_link(Parent, DbName, DbPid, StartSeq) ->
  gen_server:start_link(?MODULE, [Parent, DbName, DbPid, StartSeq], []).


init([Parent, DbName, DbPid, StartSeq]) ->
  process_flag(trap_exit, true),
  {Mod, ModState} = barrel_db:get_state(DbPid),
  %% subscribe to the database
  Interval = application:get_env(barrel, index_interval, ?INDEX_INTERVAL),
  Stream = #{ barrel => DbName, interval => Interval, include_doc => true},
  ok = barrel_db_stream_mgr:subscribe(Stream, self(), StartSeq),

  {ok, #{parent => Parent,
         db_name => DbName,
         db_pid => DbPid,
         mod => Mod,
         modstate => ModState,
         stream => Stream,
         last_seq => StartSeq}}.


handle_call(_Msg, _From, State) -> {noreply, State}.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info({changes, Stream, Changes, _Seq}, State = #{ stream := Stream, last_seq := LastSeq }) ->
  IndexedSeq = process_changes(Changes, LastSeq, State),
  NewState = update_state(IndexedSeq, LastSeq, State),
  {noreply, NewState};
handle_info(_Info, State) ->
  {noreply, State}.



update_state(Seq, Seq, State) -> State;
update_state(Seq, _LastSeq, #{ parent := Parent} = State) ->
  Parent ! {updated, Seq},
  State#{ last_seq => Seq }.

process_changes([], LastSeq, _State) ->
  LastSeq;
process_changes([Change | Rest], LastSeq, #{ mod := Mod, modstate := ModState } = State) ->
  #{ <<"id">> := DocId, <<"seq">> := Seq, <<"doc">> := Doc } = Change,
  OldPaths = case Mod:get_ilog(DocId, ModState) of
          {ok, Paths} -> Paths;
          not_found -> []
        end,
  %% analyze the doc and compare to the old index
  NewPaths = barrel_index:analyze(Doc),
  {Added, Removed} = barrel_index:diff(NewPaths, OldPaths),
  Batch = Mod:get_batch(ModState),
  %% prepare commit
  _ = log(DocId, NewPaths, Batch),
  ok = insert(Added, DocId, Batch),
  ok = unindex(Removed, DocId, Batch),
  Mod:commit(Batch, ModState),
  process_changes(Rest, erlang:max(LastSeq, Seq), State).



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

