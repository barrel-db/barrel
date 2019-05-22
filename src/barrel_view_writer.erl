-module(barrel_view_writer).

-include("barrel.hrl").

-include("barrel_view.hrl").

%% gen_batch_server callbacks
-export([init/1,
         handle_batch/2,
         terminate/2]).

init(View) ->
  {ok, View}.

handle_batch(Batch, #view{barrel=Barrel} = View) ->
  {ok, #{ ref := Ref }} = barrel_db:open_barrel(Barrel),
  {ok, Ctx} = ?STORE:init_ctx(Ref, true),

  ok = try process_batch(Batch, [], #{}, Ctx, View)
       after
         ?STORE:release_ctx(Ctx)
       end,
  {ok, View}.

terminate(_Reason, _State) ->
  ok.



process_batch([{cast, {recover, #{<<"id">> := DocId,
                                    <<"seq">> := Seq,
                                    <<"doc">> := Doc }}} | Rest],
                Refs, Cache0, Ctx, View) ->

  case cache_get(DocId, Cache0, Ctx) of
    {CachedSeq, Cache1} when  CachedSeq >= Seq ->
      process_batch(Rest, Refs, Cache1, Ctx, View);
    {_, Cache1} ->
      Ref = make_ref(),
      jobs:enqueue(barrel_index_queue, {Ref, Doc, View, self()}),
      process_batch(Rest, [Ref | Refs], Cache1#{ DocId => Seq }, Ctx, View)
  end;
process_batch([{cast, {change, #{<<"id">> := DocId,
                                   <<"seq">> := Seq,
                                   <<"doc">> := Doc }}} | Rest],
              Refs, Cache, Ctx, View) ->
  Ref = make_ref(),
  jobs:enqueue(barrel_index_queue, {Ref, Doc, View, self()}),
  process_batch(Rest, [Ref | Refs], Cache#{ DocId => Seq }, Ctx, View);

process_batch([{cast, {recover_checkpoint, LastSeq}} |Rest], Refs, Cache, Ctx,
              #view{ref=ViewRef} = View) ->
  ok = await_workers(lists:reverse(Refs)),
  ?STORE:update_view_checkpoint(ViewRef, LastSeq),
  process_batch(Rest, [], Cache, Ctx, View);
process_batch([{cast, {done, LastSeq, MainPid}} |Rest], Refs, Cache, Ctx, View) ->
  ok = await_workers(lists:reverse(Refs)),
  ?STORE:update_indexed_seq(View#view.ref, LastSeq),
  MainPid ! {index_updated, LastSeq},
  process_batch(Rest, [], Cache, Ctx, View);
process_batch([], _Refs, _Cache, _Ctx, _View) ->
  ok.

cache_get(DocId, Cache, Ctx) ->
  case maps:get(DocId, Cache, undefined) of
    undefined ->
      case ?STORE:get_doc_info(DocId, Ctx) of
        {ok, #{ seq := Seq }} ->
          {Seq, Cache#{ DocId => Seq }};
        {error, not_found} ->
          {undefind, Cache};
        Error ->
          exit(Error)
      end;
    Seq ->
      {Seq, Cache}
  end.

await_workers([]) -> ok;
await_workers([Ref | Rest]) ->
  receive
    {Ref, ok} ->
      await_workers(Rest);
    {Ref, Error} ->
      ?LOG_ERROR("index error=~p~n", [Error]),
      exit(write_error)
  after 5000 ->
          exit(worker_timeout)
  end.
