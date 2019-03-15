-module(barrel_view_worker).

-include("barrel.hrl").

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         code_change/3,
         terminate/2]).

init(_) ->
  {ok, #{}}.

handle_call(_Msg, _From, State) ->
  {reply, ok, State}.

handle_cast({process_doc, ViewKey, BatchPid, Doc}, State) ->
  ocp:record('barrel/views/docs_indexed', 1),
  ocp:record('barrel/views/active_workers', 1),
  Start = erlang:timestamp(),
  try handle_doc(ViewKey, BatchPid, Doc)
  after
    ocp:record('barrel/views/active_workers', -1),
    ocp:record('barrel/views/index_duration', timer:now_diff(erlang:timestamp(), Start))
  end,
  erlang:garbage_collect(),
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(_Reason, _State) ->
  ok.


handle_doc({BarrelId, ViewId}=ViewKey, BatchPid, Doc) ->
  {ViewMod, ViewConfig} = get_view(ViewKey),
  {ok, DocId} = process_doc(Doc, BarrelId, ViewId, ViewMod, ViewConfig),
  BatchPid ! {ok, DocId},
  ok.


get_view(ViewKey) ->
  ets:lookup_element(?VIEWS, ViewKey, 2).

process_doc(#{ <<"id">> := DocId } = Doc,
            BarrelId, ViewId, ViewMod, ViewConfig) ->
  {ok, Barrel} = barrel_db:open_barrel(BarrelId),
  KVs = ViewMod:handle_doc(Doc, ViewConfig),
  ok = update_view_index(Barrel, ViewId, DocId, KVs),
  {ok, DocId}.

update_view_index(#{ ref := Ref }, ViewId, DocId, KVs) ->
  ?STORE:update_view_index(Ref, ViewId, DocId, KVs).
