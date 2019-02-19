-module(barrel_view_worker).

-include("barrel.hrl").

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         code_change/3,
         terminate/2]).

init([]) ->
  {ok, #{}}.

handle_call(_Msg, _From, State) ->
  {reply, ok, State}.

handle_cast({process_doc, {BarrelId, ViewId}=ViewKey, BatchPid, Doc}, State) ->
  %% TODO: cache it.
  {ViewMod, ViewConfig} = get_view(ViewKey),
  {ok, DocId} = process_doc(Doc, BarrelId, ViewId, ViewMod, ViewConfig),

  BatchPid ! {ok, DocId},
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(_Reason, _State) ->
  ok.



get_view(ViewKey) ->
  ets:lookup_element(?VIEWS, ViewKey, 2).

process_doc(#{ <<"id">> := DocId } = Doc,
            BarrelId, ViewId, ViewMod, ViewConfig) ->
  {ok, Barrel} = barrel_db:open(BarrelId),

  OldKeys = view_doc_keys(Barrel, ViewId, DocId),
  KVsMap = ViewMod:handle_doc(Doc, ViewConfig),
  Keys = maps:keys(KVsMap),
  ToRem = OldKeys -- Keys,
  ToAdd = maps:without(ToRem, ToRem),
  ok = update_view_index(Barrel, ViewId, DocId, ToRem, ToAdd),
  ok.


view_doc_keys(#{ store_mod := Mod} = Barrel, ViewId, DocId) ->
  Mod:fold_view_doc_keys(Barrel, ViewId, DocId).

update_view_index(#{ store_mod := Mod} = Barrel, ViewId, DocId, ToRem, ToAdd) ->
  Mod:update_view_index(Barrel, ViewId, DocId, ToRem, ToAdd).
