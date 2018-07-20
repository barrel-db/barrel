%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 18. Jul 2018 15:12
%%%-------------------------------------------------------------------
-module(barrel_index_worker).
-author("benoitc").

%% API
-export([
  init/1,
  handle_call/3,
  handle_cast/2
]).


init([]) ->
  {ok, #{}}.

handle_call(_Msg, _From, State) -> {reply, ok, State}.

handle_cast({index, Ref, Db, #{ id := DocId } = DI, OldDI}, State) ->
  io:format("worker pid=~p received=~p~n", [self(), {index, Ref, Db, DI, OldDI}]),
  OldDoc = get_old_doc(OldDI, Db),
  NewDoc = new_doc(DI),
  {Added0, Removed0} = barrel_index:diff(NewDoc, OldDoc),
  
  io:format("diff added=~p removed=~p~n", [Added0, Removed0]),
  Added = [{add, P, DocId} || P <- Added0],
  Removed = [{delete, P, DocId} || P <- Removed0],

  
  send_result(Db, Ref, Added ++ Removed),
  {noreply, State};
handle_cast(_Msg, State) ->
  {noreply, State}.

get_old_doc(not_found, _Db) -> #{};
get_old_doc(#{ deleted := true}, _Db) -> #{};
get_old_doc(#{ id := Id, rev := OldRev}, Db) ->
  case barrel_storage:get_revision(Db, Id, OldRev) of
    {ok, Doc} -> Doc;
    _ ->  #{}
  end.

new_doc(#{ deleted := true}) -> #{};
new_doc(#{ rev := Rev, body_map := BodyMap }) ->
  maps:get(Rev, BodyMap).

-compile({inline, [send_result/3]}).
-spec send_result(Db :: map(), Tag :: reference(), Result :: term()) -> ok.
send_result(#{ updater_pid := Pid}, Ref, Result) ->
  
  io:format("send to=~p ref=~p, result=~p~n", [Pid, Ref, Result]),
  
  try Pid ! {Ref, Result} of
    _ ->
      ok
  catch
    _:_  -> ok
  end.
