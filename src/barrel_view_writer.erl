%% TODO: find a better nam
-module(barrel_view_writer).
-behaviour(gen_batch_server).

-export([init/1,
         handle_batch/2,
         terminate/2]).

init([Barrel, View]) ->
  {ok, #{ barrel => Barrel,
          view => View
        }}.


handle_batch(Batch, #{ barrel := Barrel, view := View } = State) ->
  {Waiters, Docs} = prepare_batch(Batch, [], #{}),
  %% process docs
  DocIds = lists:foldl(
             fun(#{ <<"id">> := DocId } = Doc, Acc) ->
                 wpool:cast(barrel_view_pool,
                                  {process_doc, {Barrel, View}, self(), Doc}),
                 [DocId | Acc]
             end,
             [], Docs),

  ok = await_pool(DocIds),
  Actions = [{reply, From, ok} || From <- Waiters],
  {ok, Actions, State}.

terminate(_Reason, _State) ->
  ok.

%% deduplicate docs (only take the more recent one and extract waiters
prepare_batch([{call, From, wait_commit} | Rest], Waiters, Docs) ->
  prepare_batch(Rest, [From | Waiters], Docs);
prepare_batch([{cast, {index_doc, #{ <<"id">> := DocId,
                                     <<"_seq">> := Seq}= Doc}} | Rest],
              Waiters, Docs) ->
  case maps:find(DocId, Docs) of
    {ok, #{ <<"_seq">> := CurrentSeq }} when CurrentSeq < Seq ->
      prepare_batch(Rest, Waiters, maps:put(DocId, Doc, Docs));
    {ok, _} ->
      prepare_batch(Rest, Waiters, Docs);
    error ->
      prepare_batch(Rest, Waiters, maps:put(DocId, Doc, Docs))
  end;
prepare_batch([], Waiters, Docs) ->
  {Waiters, maps:values(Docs)}.


%% TODO: add timeout
await_pool([]) ->
  ok;
await_pool(DocIds) ->
  receive
    {ok, DocId} ->
      await_pool(DocIds -- [DocId])
  end.
