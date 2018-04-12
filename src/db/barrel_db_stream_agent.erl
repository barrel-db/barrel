%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. Apr 2018 00:24
%%%-------------------------------------------------------------------
-module(barrel_db_stream_agent).
-author("benoitc").
-behaviour(gen_server).

%% API
-export([start_link/0]).

-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2
]).


-include("barrel.hrl").

start_link() ->
  gen_server:start_link(?MODULE, [], []).

init([]) ->
  sbroker:async_ask_r(?db_stream_broker),
  {ok, #{}}.

handle_call(_Msg, _From, State) -> {noreply, State}.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info({_, {go, _Ref, { #{barrel := Name } = Stream, SubRef, Subscriber, Since}}}, St) ->
  %% get options
  IncludeDoc = maps:get(include_doc, Stream, true),
  WithHistory = maps:get(with_history, Stream, true),
  {Mod, BState} = barrel_db:get_state(Name),
  Snapshot = Mod:get_snapshot(BState),
  WrapperFun =
    fun
      (DI, {Acc0, N}) ->
        #{ id := DocId,
           rev := Rev,
           seq := Seq,
           deleted := Deleted,
           revtree := RevTree } = DI,
        Changes = case WithHistory of
                    false -> [Rev];
                    true -> barrel_revtree:history(Rev, RevTree)
                  end,
        Change0 = #{ <<"id">> => DocId,
                     <<"seq">> => Seq,
                     <<"rev">> => Rev,
                     <<"changes">> => Changes},
        Change = change_with_doc(
          change_with_deleted(Change0, Deleted),
          DocId, Rev, Mod, Snapshot, IncludeDoc
        ),
        Acc1 = [Change | Acc0],
        if
          N >= 100 ->
            {stop, {Acc1, N}};
          true ->
            {ok,  {Acc1, N+1}}
        end
    end,
  {Changes, _} = try Mod:fold_changes(Since, WrapperFun, {[], 0}, Snapshot)
                 after Mod:release_snapshot(Snapshot)
                 end,
  %% send changes
  LastSeq = send_changes(Changes, Subscriber, Name),
  %% register last seq
  barrel_db_stream_mgr:next(Stream, SubRef, LastSeq),
  sbroker:async_ask_r(?db_stream_broker),
  {noreply, St}.
  
  
send_changes([#{ <<"seq">> := LastSeq } | _] = Changes, Subscriber, Name) ->
  Subscriber ! {changes, Name, Changes, LastSeq},
  LastSeq.

change_with_deleted(Change, true) -> Change#{ <<"deleted">> => true };
change_with_deleted(Change, _) -> Change.


change_with_doc(Change, DocId, Rev, Mod, State, true) ->
  case Mod:get_revision(DocId, Rev, State) of
    {ok, Doc} -> Change#{ <<"doc">> => Doc };
    _ -> Change#{ <<"doc">> => null }
  end;
change_with_doc(Change, _, _, _, _, _) ->
  Change.