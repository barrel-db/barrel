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

-export([bin_to_seq/1]).


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
      Since1 = case Since of
                 now ->
                   {ok, UpdatedSeq} = barrel_db:updated_seq(Name),
                   UpdatedSeq;
                 _ ->
                   bin_to_seq(Since)
               end,
      WrapperFun =
      fun
        (DI, {Acc0, LastSeq, N}) ->
          #{ id := DocId,
             seq := Seq,
             deleted := Deleted,
             rev := Rev,
             revtree := RevTree } = DI,
          Changes = case WithHistory of
                      false -> [Rev];
                      true -> barrel_revtree:history(Rev, RevTree)
                    end,
          Change0 = #{ <<"id">> => DocId,
                       <<"seq">> => seq_to_bin(Seq, BState),
                       <<"rev">> => Rev,
                       <<"changes">> => Changes},
          Change = change_with_doc(
            change_with_deleted(Change0, Deleted),
            DocId, Rev, Mod, BState, IncludeDoc
          ),
          Acc1 = [Change | Acc0],
          if
            N >= 100 ->
              {stop, {Acc1, erlang:max(Seq, LastSeq), N}};
            true ->
              {ok,  {Acc1, erlang:max(Seq, LastSeq), N+1}}
          end
      end,
      Acc0 = {[], Since1, 0},
      LastSeq = try
                  fold_changes(Since1, WrapperFun, Acc0, Stream, Subscriber, Mod, BState)
                catch
                  C:E ->
                    %% for now we ignore the errors. It's most probably a race condition and should be handled
                    %% before it's happening
                    _ = lager:debug("folding changes error: stream=~p, error=~p:~p", [Stream, C, E]),
                    seq_to_bin(Since1, BState)
                end,
      %% register last seq
      _ = barrel_db_stream_mgr:next(Stream, SubRef, LastSeq),
      _ = sbroker:async_ask_r(?db_stream_broker),
      ok
  end.


seq_to_bin(Seq, #{ id := Id }) ->
  SeqBin = integer_to_binary(Seq),
  Padding = << << $0 >> || _ <- lists:seq(1, 16 - size(SeqBin)) >>,
  << Id/binary, "-", Padding/binary, SeqBin/binary >>.

bin_to_seq(Seq) when is_integer(Seq) ->
  Seq;
bin_to_seq(Bin) when is_binary(Bin) ->
  case binary:split(Bin, <<"-">>) of
    [_NodeId, BinSeq] -> binary_to_integer(BinSeq);
    [_] -> erlang:error(badarg)
  end;
bin_to_seq(_) ->
  erlang:error(badarg).


send_changes([], _LastSeq, _Stream, _Subscriber) ->
  ok;
send_changes(Changes, LastSeq, Stream, Subscriber) ->
  _ = lager:info("send changes=~p lastseq=~p, stream=~p~n", [Changes, LastSeq, Stream]),
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
  {Changes, LastSeq, _} = Mod:fold_changes(Since, WrapperFun, Acc, ModState),
  LastSeqBin = seq_to_bin(LastSeq, ModState),
  send_changes(lists:reverse(Changes), LastSeqBin, Stream, Subscriber),
  LastSeqBin.



