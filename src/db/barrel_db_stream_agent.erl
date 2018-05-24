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
                 _RelativeTime, _SojournTime}}, State) ->
  _ = fetch_changes(Stream, SubRef, Subscriber, Since),
  {noreply, State}.

fetch_changes(#{barrel := Name } = Stream, SubRef, Subscriber, Since) ->
  %% get options
  IncludeDoc = maps:get(include_doc, Stream, true),
  WithHistory = maps:get(with_history, Stream, true),
  case barrel_db:get_state(Name) of
    {error, Reason}  ->
      _ = lager:warning("error retrieving db state: stream=~p error=~p~n", [Stream, Reason]),
      ok = barrel_db_stream_mgr:next(Stream, SubRef, Since),
      _ = sbroker:async_ask_r(?db_stream_broker);
    {Mod, BState}  ->
      Snapshot = Mod:get_snapshot(BState),
      %%Snapshot = BState,
      WrapperFun =
      fun
        (DI, {Acc0, LastSeq, N}) ->
          #{ id := DocId,
             seq := Seq,
             deleted := Deleted,
             revtree := RevTree } = DI,
          {Rev, _, _} = barrel_revtree:winning_revision(RevTree),
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
      Acc0 = {[], Since, 0},
      LastSeq = try
                  fold_changes(Since, WrapperFun, Acc0, Stream, Subscriber, Mod, BState)
                catch
                  C:E ->
                    %% for now we ignore the errors. It's most probably a race condition and should be handled
                    %% before it's happening
                    _ = lager:debug("folding changes error: stream=~p, error=~~:~p", [Stream, C, E]),
                    Since
                end,
      %% register last seq
      _ = barrel_db_stream_mgr:next(Stream, SubRef, LastSeq),
      _ = sbroker:async_ask_r(?db_stream_broker),
      ok
  end.


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




fold_changes(Since, WrapperFun, Acc, Stream, Subscriber, Mod, ModState) ->
  Snapshot = Mod:get_snapshot(ModState),
  {Changes, LastSeq, _} = try Mod:fold_changes(Since, WrapperFun, Acc, Snapshot)
                          after Mod:release_snapshot(Snapshot)
                          end,
  send_changes(lists:reverse(Changes), LastSeq, Stream, Subscriber),
  LastSeq.



