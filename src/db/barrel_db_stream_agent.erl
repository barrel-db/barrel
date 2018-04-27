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
  _ = sbroker:async_ask_r(?db_stream_broker),
  {ok, #{}}.

handle_call(_Msg, _From, State) -> {noreply, State}.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info({_, {go, _Ref, {Stream, SubRef, Subscriber, Since},
                 _RelativeTime, _SojournTime}}, St) ->
  ok = fetch_changes(Stream, SubRef, Subscriber, Since),
  {noreply, St}.


fetch_changes(#{barrel := Name } = Stream, SubRef, Subscriber, Since) ->
  %% get options
  IncludeDoc = maps:get(include_doc, Stream, true),
  WithHistory = maps:get(with_history, Stream, true),
  {Mod, BState} = barrel_db:get_state(Name),
  Snapshot = Mod:get_snapshot(BState),
  %%Snapshot = BState,
  WrapperFun =
  fun
    (DI, {Acc0, LastSeq, N}) ->
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
          {stop, {Acc1, erlang:max(Seq, LastSeq), N}};
        true ->
          {ok,  {Acc1, erlang:max(Seq, LastSeq), N+1}}
      end
  end,
  {Changes, LastSeq, _} = try Mod:fold_changes(Since, WrapperFun, {[], Since, 0}, Snapshot)
                          after Mod:release_snapshot(Snapshot)
                          end,
  %% send changes
  send_changes(lists:reverse(Changes), LastSeq, Stream, Subscriber),
  %% register last seq
  ok = barrel_db_stream_mgr:next(Stream, SubRef, LastSeq),
  _ = sbroker:async_ask_r(?db_stream_broker),
  ok.


send_changes([], _LastSeq, _Stream, _Subscriber) ->
  ok;
send_changes(Changes, LastSeq, Stream, Subscriber) ->
  Subscriber ! {changes, Stream, Changes, LastSeq},
  ok.

change_with_deleted(Change, true) -> Change#{ <<"deleted">> => true };
change_with_deleted(Change, _) -> Change.


change_with_doc(Change, DocId, Rev, Mod, State, true) ->
  case Mod:get_revision(DocId, Rev, State) of
    {ok, Doc} -> Change#{ <<"doc">> => Doc };
    _ -> Change#{ <<"doc">> => null }
  end;
change_with_doc(Change, _, _, _, _, _) ->
  Change.