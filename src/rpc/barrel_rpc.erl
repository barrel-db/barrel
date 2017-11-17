%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. Nov 2017 05:09
%%%-------------------------------------------------------------------
-module(barrel_rpc).
-author("benoitc").

%% API
-export([
  fetch_docs/6,
  revsdiffs/6,
  update_docs/4,
  subscribe_changes/3,
  unsubscribe_changes/1,
  await_changes/2,
  put_system_doc/4,
  get_system_doc/3,
  delete_system_doc/3,
  revsdiff/4
]).


-export([start_link/0]).

-export([
  init/1,
  loop/1
]).

%% system callbacks
-export([
  system_continue/3,
  system_code_change/4,
  system_terminate/4
]).


fetch_docs(Node, DbName, Fun, Acc, DocIds, Options) ->
  Ref = cast(Node, self(), {fetch_docs, DbName, DocIds, Options}),
  Deadline = maps:get(deadline, Options, infinity),
  case recv(Ref, Deadline) of
    {start_stream, Pid} ->
      MRef = erlang:monitor(process, Pid),
      fetch_loop(Pid, Ref, MRef, Deadline, Fun, Acc);
    timeout ->
      erlang:error(timeout)
  end.


fetch_loop(Pid, Ref, MRef, Deadline, Fun, Acc) ->
  receive
    {Ref, {doc, Doc, Meta}} ->
      Acc2 = Fun(Doc, Meta, Acc),
      fetch_loop(Pid, Ref, MRef, Deadline, Fun, Acc2);
    {Ref, done} ->
      erlang:demonitor(MRef, [flush]),
      Acc;
    {'DOWN', MRef, process, _, _} ->
      erlang:error(timeout)
  after Deadline ->
    erlang:error(timeout)
  end.

revsdiffs(Node, DbName, Fun, Acc, ToDiff, Options) ->
  Ref = cast(Node, self(), {revdiffs, DbName, ToDiff}),
  Deadline = maps:get(deadline, Options, 5000),
  case recv(Ref, Deadline) of
    {start_stream, Pid} ->
      MRef = erlang:monitor(process, Pid),
      revsdiffs_loop(Pid, Ref, MRef, Deadline, Fun, Acc);
    timeout ->
      erlang:error(timeout)
  end.


revsdiffs_loop(Pid, Ref, MRef, Deadline, Fun, Acc) ->
  receive
    {Ref, {revsdiff, DocId, Missing, Ancestors}} ->
      Acc2 = Fun(DocId, Missing, Ancestors, Acc),
      fetch_loop(Pid, Ref, MRef, Deadline, Fun, Acc2);
    {Ref, done} ->
      erlang:demonitor(MRef, [flush]),
      Acc;
    {'DOWN', MRef, process, _, _} ->
      erlang:error(timeout)
  after Deadline ->
    erlang:error(timeout)
  end.

%% TODO: stream docs sending completely ?
update_docs(Node, DbName, Batch, Options) ->
  Timeout = maps:get(timeout, Options, infinity),
  Ref = cast(Node, self(), {update_docs, DbName}),
  case recv(Ref, Timeout) of
    {start_stream, Pid} ->
      _ = [Pid ! {op, Op} || Op <- Batch],
      Pid ! commit,
      MRef = erlang:monitor(process, Pid),
      receive
        {Ref, Reply} -> Reply;
        {'DOWN', MRef, process, _, Error} ->
          _ = lager:error(
            "~s: remote update task on ~p with ~p down: ~p~n",
            [?MODULE_STRING, Node, DbName, Error]
          ),
          erlang:error(timeout)
      after Timeout ->
        erlang:error(timeout)
      end;
    timeout ->
      erlang:error(timeout)
  end.


subscribe_changes(Node, DbName, Options) ->
  Ref = cast(Node, self(), {subscribe, DbName, Options}),
  Deadline = maps:get(deadline, Options, 5000),
  case recv(Ref, Deadline) of
    {start_stream, Pid} ->
      {Ref, Node, Pid};
    timeout ->
      erlang:error(timeout)
  end.

await_changes({Ref, _Node, Pid}=Stream, Timeout) ->
  MRef = erlang:monitor(process, Pid),
  case recv(Ref, Pid, Timeout) of
    {change, Change}  ->
      erlang:demonitor(MRef, [flush]),
      Change;
    {done, LastSeq} ->
      erlang:demonitor(MRef, [flush]),
      {done, LastSeq};
    remote_timeout ->
      erlang:error(timeout);
    local_timeout ->
      unsubscribe_changes(Stream),
      erlang:error(timeout);
    Error ->
      _ = lager:error("~s: remote chnages feed timeout~n", [Error]),
      erlang:error(timeout)
  end.


unsubscribe_changes({StreamRef, Node, _Pid}) ->
  call(Node, self(),  {unsubscribe, StreamRef}, 5000).


put_system_doc(Node, DbName, DocId, Doc) ->
  call(Node, self(),  {put_system_doc, DbName, DocId, Doc}, infinity).

get_system_doc(Node, DbName, DocId) ->
  call(Node, self(),  {get_system_doc, DbName, DocId}, infinity).

delete_system_doc(Node, DbName, DocId) ->
  call(Node, self(),  {delete_system_doc, DbName, DocId}, infinity).


revsdiff(Node,  DbName, DocId, RevIds) ->
  call(Node, self(), {revsdiff, DbName, DocId, RevIds}, infinity).

start_link() ->
  proc_lib:start_link(?MODULE, init, [self()]).

init(Parent) ->
  %% register ourself
  true = register(?MODULE, self()),
  proc_lib:init_ack(Parent, {ok, self()}),
  %% start the loop
  process_flag(trap_exit, true),
  loop(#{ parent => Parent, handlers => maps:new(), streams => maps:new()}).

loop(S = #{ parent := Parent, handlers := Handlers, streams := Streams}) ->
  receive
    {call, From, Call} ->
      handle_call(Call, From, S);
    {'DOWN', _MRef, process, Handler, normal} ->
      case maps:take(Handler, Handlers) of
        {{Ref, _}, Handlers2} ->
          Streams2 = maps:remove(Ref, Streams),
          loop(S#{ handlers => Handlers2, streams => Streams2 });
        error ->
          loop(S)
      end;
    {'DOWN', _MRef, process, Handler, Reason} ->
      case maps:take(Handler, Handlers) of
        {{Ref, _To}=To, Handlers2} ->
          reply(To, {badrpc, {'EXIT', Reason}}),
          Streams2 = maps:remove(Ref, Streams),
          loop(S#{ handlers => Handlers2, streams => Streams2 });
        error ->
          loop(S)
      end;
    {'EXIT', Parent, Reason} ->
      erlang:exit(Reason);
    {system, From, Request} ->
      sys:handle_system_msg(
        Request, From, Parent, ?MODULE, [],
        {loop, S}
      );
    Other ->
      _ = error_logger:error_msg(
        "~s: got unexpected message: ~p~n",
        [?MODULE_STRING, Other]
      ),
      exit({unexpected_message, Other})
  end.

handle_call({update_docs, DbName}, From, S) ->
  handle_update_docs(DbName, From, S);
handle_call({fetch_docs, DbName, DocIds, Options}, From, S) ->
  handle_fetch_docs(DbName, DocIds, Options, From, S);
handle_call({revsdiffs, DbName, ToDiff}, From, S) ->
  handle_revsdiffs(DbName, ToDiff, From, S);
handle_call({subscribe, DbName, Options}, From, S) ->
  handle_subscription(DbName, Options, From, S);
handle_call({unsubscribe, Ref}, From, S = #{ streams := Streams }) ->
  reply(From, ok),
  case maps:find(Ref, Streams) of
    {ok, Process} ->
      exit(Process, normal),
      loop(S);
    error ->
      loop(S)
  end;
handle_call({revsdiff, DbName, DocId, RevIds}, From, S) ->
  handle_call_call(barrel, revsdiff, [DbName, DocId, RevIds], From, S);
handle_call({put_system_doc, DbName, DocId, Doc}, From, S) ->
  handle_call_call(barrel, put_system_doc, [DbName, DocId, Doc], From, S);
handle_call({get_system_doc, DbName, DocId}, From, S) ->
  handle_call_call(barrel, get_system_doc, [DbName, DocId], From, S);
handle_call({delete_system_doc, DbName, DocId}, From, S) ->
  handle_call_call(barrel, delete_system_doc, [DbName, DocId], From, S);
handle_call(_, From, S) ->
  reply(From, {badrpc, {'EXIT', bad_call}}),
  loop(S).

handle_update_docs(DbName, {Pid, Ref} = To, State) ->
  #{ handlers := Handlers, streams := Streams } = State,
  {Stream, _} =
    erlang:spawn_monitor(
      fun() ->
        reply(To, {start_stream, self()}),
        MRef = erlang:monitor(process, Pid),
        do_update_docs(To, DbName, MRef, [])
      end
    ),
  Handlers2 = maps:put(Stream, To, Handlers),
  Streams2 = maps:put(Ref, Stream, Streams),
  NewState = State#{ stream => Streams2, handlers => Handlers2 },
  loop(NewState).

do_update_docs(To, DbName, MRef, Batch) ->
  receive
    {op, OP} ->
      do_update_docs(To, DbName, MRef, [OP | Batch]);
    cancel ->
      exit(normal);
    commit ->
      Result = try
                 barrel:write_batch(DbName, Batch, #{})
               catch
                 _:Error ->
                   {error, Error}
               end,
      reply(To, Result);
    {'DOWN', MRef, _, _, _} ->
      exit(normal)
  end.


handle_fetch_docs(DbName, DocIds, Options, {Pid, Ref} = To, State) ->
  #{ handlers := Handlers, streams := Streams } = State,
  {Stream, _} =
    erlang:spawn_monitor(
      fun() ->
        reply(To, {start_stream, self()}),
        MRef = erlang:monitor(process, Pid),
        _ = do_fetch_docs(To, MRef, DbName, DocIds, Options),
        reply(To, done)
      end
    ),
  Handlers2 = maps:put(Stream, To, Handlers),
  Streams2 = maps:put(Ref, Stream, Streams),
  NewState = State#{ stream => Streams2, handlers => Handlers2 },
  loop(NewState).

do_fetch_docs(To, MRef, DbName, DocIds, Options) ->
  barrel:multi_get(
    DbName,
    fun(Doc, Meta, _Acc) ->
      ok = maybe_down(MRef),
      reply(To, {doc, Doc, Meta})
    end,
    ok,
    DocIds,
    Options
  ).

handle_revsdiffs(DbName, ToDiff, {Pid, Ref} = To, State) ->
  #{ handlers := Handlers, streams := Streams } = State,
  {Stream, _} =
  erlang:spawn_monitor(
    fun() ->
      reply(To, {start_stream, self()}),
      MRef = erlang:monitor(process, Pid),
      _ = do_revsdiffs(ToDiff, DbName, To, MRef),
      reply(To, done)
    end
  ),
  Handlers2 = maps:put(Stream, To, Handlers),
  Streams2 = maps:put(Ref, Stream, Streams),
  NewState = State#{ stream => Streams2, handlers => Handlers2 },
  loop(NewState).

do_revsdiffs([{DocId, History} | Rest], DbName, To, MRef) ->
  ok = maybe_down(MRef),
  case barrel:revsdiff(DbName, DocId, History) of
    {ok, Missing, Ancestors} ->
      reply(To, {revsdiff, DocId, Missing, Ancestors});
    Error ->
      reply(To, Error)
  end,
  do_revsdiffs(Rest, DbName, To, MRef);
do_revsdiffs([], _, _, _) ->
  ok.

handle_subscription(DbName, Options, {Pid, Ref} = To, S) ->
  #{ handlers := Handlers, streams := Streams } = S,
  Since = maps:get(since, Options, 0),
  {Handler, _} =
    erlang:spawn_monitor(
      fun() ->
        ChangesStream = barrel:subscribe_changes(DbName, Since, #{}),
        _ = erlang:monitor(process, ChangesStream),
        _ = erlang:monitor(process, Pid),
        reply(To, {start_stream, self()}),
        wait_changes(ChangesStream, To)
      end
    ),
  
  Streams2 = maps:put(Ref, Handler, Streams),
  Handlers2 = maps:put(Handler, To, Handlers),
  loop(S#{ handlers => Handlers2, streams => Streams2 }).


wait_changes(StreamPid,  To = {Pid, _Tag}) ->
  receive
    {change, StreamPid, Change} ->
      Seq = maps:get(<<"seq">>, Change),
      OldSeq = erlang:get({StreamPid, last_seq}),
      _ = erlang:put({Pid, last_seq}, erlang:max(OldSeq, Seq)),
      reply(To, {change, Change}),
      wait_changes(StreamPid, To);
    {'DOWN', _, process, StreamPid, _Reason} ->
      LastSeq = erlang:erase({StreamPid, last_seq}),
      reply(To, {done, LastSeq}),
      exit(normal);
    {'DOWN', _, process, Pid, _} ->
      exit(normal)
  end.

handle_call_call(Mod, Fun, Args, To, S = #{ handlers := Handlers }) ->
  {Handler, _} =
    erlang:spawn_monitor(
      fun() ->
        Reply =
                case catch apply(Mod,Fun,Args) of
                  {'EXIT', _} = Exit ->
                    {badrpc, Exit};
                  Other ->
                    Other
                end,
        
        
        reply(To, Reply)
      end
    ),
  loop(S#{ handlers => maps:put(Handler, {call, To}, Handlers) }).

reply({To, Tag}, Reply) ->
  Msg = {Tag, Reply},
  catch To ! Msg.

%% TODO: we should buffer calls instead of ignoring the return
cast(Node, FromPid, Msg) ->
  Ref = make_ref(),
  CastMsg = {call, {FromPid, Ref}, Msg},
  _ = erlang:send({barrel_rpc, Node}, CastMsg,  [noconnect, nosuspend]),
  Ref.

recv(Ref, Timeout) ->
  receive
    {Ref, Reply} -> Reply
  after Timeout ->
    local_timeout
  end.

recv(Ref, Pid, Timeout) ->
  receive
    {Ref, Reply} ->
      Reply;
    {'DOWN', _, Pid, _} ->
      remote_timeout
  after Timeout ->
    local_timeout
  end.

call(Node, FromPid, Msg, Timeout) ->
  Ref = make_ref(),
  CastMsg = {call, {FromPid, Ref}, Msg},
  _ = erlang:send({barrel_rpc, Node}, CastMsg,  [noconnect, nosuspend]),
  recv(Ref, Timeout).


maybe_down(MRef) ->
  receive
    {'DOWN', MRef, process, _, _} ->
      exit(normal)
  after 0 ->
    ok
  end.


%% -------------------------
%% system callbacks

system_continue(_, _, {loop, State}) ->
  loop(State).

-spec system_terminate(any(), _, _, _) -> no_return().
system_terminate(Reason, _, _, {loop, State}) ->
  _ = lager:info(
    "sytem terminate ~p~n", [State]
  ),
  exit(Reason).

system_code_change(Misc, _, _, _) ->
  {ok, Misc}.