-module(barrel_view_updater).

-include("barrel.hrl").
-include("barrel_view.hrl").

-export([run/1]).

-export([update/2]).


run(Data) ->
  spawn_link(?MODULE, update, [self(), Data]).

update(Parent, #{ barrel := BarrelId,
                  since := Since,
                  view := View }) ->
  MaxItems = barrel_config:get(view_max_items),
  MaxSize = barrel_config:get(view_max_size),
  BufferOpts = [{max_items, MaxItems}, {max_size, MaxSize}],
  {ok, BufReader} = barrel_buffer:start_link(BufferOpts),
  {ok, BufWriter} = barrel_buffer:start_link(BufferOpts),

  Ref = make_ref(),
  State = #{parent => Parent,
            update_proc => self(),
            ref => Ref,
            view => View,
            buf_reader => BufReader,
            buf_writer => BufWriter },

  spawn_link(fun() -> map_docs(State) end),
  spawn_link(fun() -> write_kvs(State) end),

  FoldFun = fun(Change, Acc) ->
                process_change(Change, BufReader, Acc)
            end,
  {ok, Barrel} = barrel:open_barrel(BarrelId),

  {ok, Acc, LastSeq} =
    barrel_db:fold_changes(Barrel, Since, FoldFun, [],
                           #{ include_doc => true }),
  case Acc of
    [] -> ok;
    _ ->
      barrel_buffer:enqueue(BufReader, lists:reverse(Acc))
  end,
  barrel_buffer:close(BufReader),
  receive
    {Ref, done} ->
      barrel_buffer:close(BufReader),
      exit({index_updated, LastSeq})
  end.

process_change(Change, BufReader, Acc) when length(Acc) >= 100 ->
  barrel_buffer:enqueue(BufReader, lists:reverse(Acc)),
  process_change(Change, BufReader, []);
process_change(#{ <<"id">> := DocId,
                  <<"seq">> := Seq,
                  <<"doc">> := Doc }, _BufReader, Acc) ->
  {ok, [{DocId, Seq, Doc} | Acc]}.


map_docs(#{ buf_reader := BufReader,
            buf_writer := BufWriter,
            view := #view{mod=Mod,
                          config=Config}  }=State) ->

  case barrel_buffer:dequeue(BufReader) of
    {ok, DocRows} ->
      ProcFun = fun({DocId, Seq, Doc}, Acc) ->
                    KVs = Mod:handle_doc(Doc, Config),
                    [{DocId, Seq, KVs} | Acc]
                end,
      Results = lists:foldl(fun(Docs, Acc) ->
                                lists:foldl(ProcFun, Acc, Docs)
                            end, [], DocRows),
      barrel_buffer:enqueue(BufWriter, Results),
      map_docs(State);
    closed ->
      barrel_buffer:close(BufWriter)
  end.

write_kvs(#{ parent := Parent,
             update_proc := Updater,
             ref := Ref,
             buf_writer := BufWriter,
             view := View } = State ) ->

  case barrel_buffer:dequeue(BufWriter) of
    {ok, Results} ->
      LastSeq = insert_results(Results, 0, View),
      ?STORE:update_indexed_seq(View#view.ref, LastSeq),
      Parent ! {index_updated, LastSeq},
      write_kvs(State);
    closed ->
      Updater ! {Ref, done}
  end.

insert_results([KVResult | Rest], LastSeq, View) ->
  UpdatedSeq = insert_results_1(KVResult, LastSeq, View),
  insert_results(Rest, UpdatedSeq, View);
insert_results([], LastSeq, _View) ->
  LastSeq.


insert_results_1([{DocId, Seq, KVs} | Rest], _LastSeq, View) ->
  ?STORE:update_view_index(View#view.ref, DocId, KVs),
  insert_results_1(Rest, Seq, View);
insert_results_1([], LastSeq, _View) ->
  LastSeq.
