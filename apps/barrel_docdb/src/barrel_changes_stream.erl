-module(barrel_changes_stream).

-export([next/1]).
-export([await/1, await/2,
         ack/2]).
-export([stop/1]).

-export([start_link/3]).

-export([init/1,
         callback_mode/0,
         terminate/3,
         handle_event/4]).

-export([iterate/3,
         push/3,
         wait_pending/3]).

-include_lib("barrel/include/barrel.hrl").

-define(CHANGES_INTERVAL, 100).
-define(BATCH_SIZE, 100).
-define(BATCHES_ON_FLY, 5).


next(StreamPid) ->
  Tag = erlang:make_ref(),
  ok = gen_server:cast(StreamPid, {next, {self(), Tag}}),
  receive
    {Tag, {ok, _} = OK} -> OK;
    {Tag, Error} -> Error
  after 5000 ->
          exit(timeout)
  end.

await(StreamPid) ->
 await(StreamPid, infinity).

await(StreamPid, Timeout) ->
  MRef = erlang:monitor(process, StreamPid),
  receive
    {changes, ReqId, Changes} ->
      erlang:demonitor(MRef, [flush]),
      {ReqId, Changes};
    {'DOWN', MRef, process, _, Reason} ->
      ?LOG_INFO("stream down reason=~p~n", [Reason]),
      []
  after
    Timeout ->
      erlang:demonitor(MRef, [flush]),
      erlang:exit(timeout)
  end.

ack(StreamPid, ReqId) ->
  gen_statem:cast(StreamPid, {ack, ReqId}).

stop(StreamPid) ->
  gen_statem:cast(StreamPid, stop).

start_link(Name, Owner, Options) ->
  gen_statem:start_link(?MODULE, [Name, Owner, Options], []).


init([Name, Owner, Options]) ->
  {ok, #{ ref := Ref } = Barrel} = barrel_db:open_barrel(Name),
  Since = maps:get(since, Options, first),
  IncludeDoc = maps:get(include_docs, Options, false),
  StreamMode = maps:get(stream_mode, Options, push),

  case StreamMode of
    push ->
      Interval = maps:get(interval, Options, ?CHANGES_INTERVAL),
      BatchSize = maps:get(batch_size, Options, ?BATCH_SIZE),
      InitState = #{ barrel => Barrel,
                     since => Since,
                     include_doc => IncludeDoc,
                     interval => Interval,
                     batch_size => BatchSize,
                     owner => Owner,
                     pending => [] },
      {ok, push, InitState, [{next_event, info, send_changes}]};
    iterate ->
      Since1 = since(Since),
      {ok, Ctx} = ?STORE:init_ctx(Ref, true),
      {ok, Cont} = ?STORE:changes_iterator(Ctx, Since1),
      InitState = #{ barrel => Barrel,
                     since => Since,
                     ctx => Ctx,
                     include_doc => IncludeDoc,
                     cont => Cont },
      {ok, iterate, InitState}
  end.


callback_mode() -> state_functions.

terminate(_Reason, _StateType, _State) ->
  ok.

iterate(cast, {next, {From, Tag}}, #{ cont := Cont,
                                      ctx := Ctx,
                                      include_doc := IncludeDoc } = State) ->
  Result = ?STORE:changes_next(Cont),
  case Result of
    {ok, DI, NewCont} ->
      ChangeDoc = change_doc(DI, IncludeDoc, Ctx),
      From ! {Tag, {ok, ChangeDoc}},
       {keep_state, State#{ cont => NewCont }};
    Error ->
      From ! {Tag, Error},
      ok = ?STORE:close_changes_iterator(Cont),
      {stop, normal, State}
  end;

iterate(cast, close, #{ cont := Cont } = State) ->
  _ = (catch ?STORE:close_changes_iterator(Cont)),
  {stop, normal, State};
iterate(EventType, Content, State) ->
  handle_event(EventType, iterate, Content, State).

change_doc(#{id := DocId,
             seq := Seq0,
             deleted := Deleted,
             rev := Rev,
             revtree := RevTree }, IncludeDoc, Ctx) ->

  Seq = barrel_sequence:to_string(Seq0),
  Changes = [RevId || #{ id := RevId } <- barrel_revtree:conflicts(RevTree)],
  Change0 =
  #{ <<"id">> => DocId,
     <<"seq">> => Seq,
     <<"rev">> => Rev,
     <<"changes">> => Changes
   },


  change_with_doc(
    change_with_deleted(Change0, Deleted),
    DocId, Rev, Ctx, IncludeDoc
   ).


change_with_deleted(Change, true) ->
  Change#{ <<"deleted">> => true };
change_with_deleted(Change, _) ->
  Change.

change_with_doc(Change, DocId, Rev, Ctx, true) ->
  {ok, Doc} = ?STORE:get_doc_revision(Ctx, DocId, Rev),
  Change#{ <<"doc">> => Doc };
change_with_doc(Change, _, _, _, _) ->
  Change.


push(info, send_changes, #{ barrel := Barrel,
                            since := Since,
                            interval := Interval,
                            owner := Owner,
                            batch_size := BatchSize,
                            pending := Pending } = State) ->
  Cb = fun(Change, Acc) ->
           Acc1 = [ Change | Acc],
           case (length(Acc1) =:= BatchSize) of
             true ->
               {stop, lists:reverse(Acc1)};
             false ->
               {ok, Acc1}
           end
       end,
  {ok, Changes, LastSeq} =
    barrel:fold_changes(Barrel, Since, Cb, [], #{}),
  ReqId = erlang:make_ref(),
  Owner ! {changes, ReqId, Changes},

  Pending2 = [ReqId | Pending],
  NewState =  State#{ since => LastSeq,
                      pending => Pending2 },
  case (length(Pending2) < ?BATCHES_ON_FLY) of
    true ->
      erlang:send_after(Interval, self(), send_changes),
      {keep_state, NewState};
    false ->
      {next_state, wait_pending, NewState}
  end;
push(EventType, Content, State) ->
  handle_event(EventType, push, Content, State).

wait_pending(cast, {ack, ReqId}, #{ pending := Pending } = State) ->
  case (Pending -- [ReqId]) of
    [] ->
      {next_state, push, State#{ pending => [] },
       [{next_event, info, send_changes}]};
    Pending ->
      ?LOG_WARNING("~s, received unknown pending request id=~p", [?MODULE_STRING, ReqId]),
      {keep_state, State};
    Pending2 ->
      {keep_state, State#{ pending => Pending2 }}
  end;
wait_pending(EventType, Content, State) ->
  handle_event(EventType, wait_pending, Content, State).


handle_event(cast, _StateType, stop, State) ->
  {stop, normal, State};

handle_event(EventType, StateType, Content, State) ->
  ?LOG_WARNING(
    "~s, unknown event type=~p, state type=~p, content=~p~n",
     [?MODULE_STRING, EventType, StateType, Content]
  ),
  {keep_state, State}.


since(first) ->
  barrel_sequence:encode({0, 0});
since(Since0) when is_binary(Since0) ->
  {_, Since1} = barrel_sequence:from_string(Since0),
  Since1;
since(Since) ->
  ?LOG_ERROR("fold_changes: invalid sequence. sinnce=~p~n", [Since]),
  erlang:error(badarg).
