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
  
  update_docs/4,
  subscribe_changes/3,
  unsubscribe/2,
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
  Deadline = maps:get(deadline, Options, 5000),
  case recv(Ref, Deadline) of
    {start_stream, Pid} ->
      fetch_loop(Pid, Ref, Deadline, Fun, Acc);
    timeout ->
      erlang:error(timeout)
  end.


fetch_loop(Pid, Ref, Deadline, Fun, Acc) ->
  case recv(Ref, Deadline) of
    {doc, Doc, Meta} ->
      Acc2 = Fun(Doc, Meta, Acc),
      _ = send_ack(Pid),
      fetch_loop(Pid, Ref, Deadline, Fun, Acc2);
    done ->
      Acc;
    timeout ->
      erlang:error(timeout)
  end.

%% TODO: stream docs sending completely ?
update_docs(Node, DbName, Batch, Options) ->
  Timeout = maps:get(timeout, Options, 5000),
  Deadline = maps:get(deadline, Options, 5000),
  Limit = maps:get(limit, Options, 1),
  Ref = cast(Node, self(), {update_docs, DbName, Timeout}),
  case recv(Ref, Timeout) of
    {start_stream, Pid} ->
      [begin
         case maybe_wait(Limit, Deadline) of
           ok ->
             (catch Pid ! {op, Op}),
             ok;
           timeout ->
             %% remote is probably dead but let's
             %% try to send the cancel action still
             (catch Pid ! cancel),
             erlang:error(timeout)
         end
       end || Op <- Batch],
      _ = (catch drain_acks(1)),
      Pid ! commit,
      recv(Ref, Timeout);
    timeout ->
      erlang:error(timeout)
  end.


subscribe_changes(Node, DbName, Options) ->
  Ref = cast(Node, self(), {subscribe, DbName, Options}),
  Deadline = maps:get(deadline, Options, 5000),
  case recv(Ref, Deadline) of
    {start_stream, Pid} ->
      {Ref, Pid};
    timeout ->
      erlang:error(timeout)
  end.

await_changes({Ref, Pid}, Timeout) ->
  case recv(Ref, Timeout) of
    {change, Change}  ->
      _ = send_ack(Pid),
      Change;
    {done, LastSeq} ->
      {done, LastSeq};
    _ ->
      erlang:error(timeout)
  end.


unsubscribe(Node, {StreamRef, _Pid}) ->
  call(Node, self(),  {unsubscribe, StreamRef}, 5000).


put_system_doc(Node, DbName, DocId, Doc) ->
  call(Node, self(),  {put_system_doc, DbName, DocId, Doc}, 5000).

get_system_doc(Node, DbName, DocId) ->
  call(Node, self(),  {get_system_doc, DbName, DocId}, 5000).

delete_system_doc(Node, DbName, DocId) ->
  call(Node, self(),  {delete_system_doc, DbName, DocId}, 5000).


revsdiff(Node,  DbName, DocId, RevIds) ->
  call(Node, self(), {revsdiff, DbName, DocId, RevIds}, 5000).

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

handle_call({update_docs, DbName, Timeout}, From, S) ->
  handle_update_docs(DbName, Timeout, From, S);
handle_call({fetch_docs, DbName, DocIds, Options}, From, S) ->
  handle_fetch_docs(DbName, DocIds, Options, From, S);
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

handle_update_docs(DbName, Timeout, {_Pid, Ref} = To, State) ->
  #{ handlers := Handlers, streams := Streams } = State,
  {Stream, _} =
    erlang:spawn_monitor(
      fun() ->
        reply(To, {start_stream, self()}),
        do_update_docs(To, DbName, Timeout, [])
      end
    ),
  Handlers2 = maps:put(Stream, To, Handlers),
  Streams2 = maps:put(Ref, Stream, Streams),
  NewState = State#{ stream => Streams2, handlers => Handlers2 },
  loop(NewState).

do_update_docs({Pid, _} = To, DbName, Timeout, Batch) ->
  receive
    {op, OP} ->
      _ = send_ack(Pid),
      do_update_docs(To, DbName, Timeout, [OP | Batch]);
    cancel ->
      exit(normal);
    commit ->
      Result = try
                 barrel:write_batch(DbName, Batch, #{})
               catch
                 _:Error ->
                   {error, Error}
               end,
      reply(To, Result)
  after Timeout ->
    erlang:error(normal)
  end.


handle_fetch_docs(DbName, DocIds, Options, {_Pid, Ref} = To, State) ->
  #{ handlers := Handlers, streams := Streams } = State,
  {Stream, _} =
    erlang:spawn_monitor(
      fun() ->
        reply(To, {start_stream, self()}),
        _ = do_fetch_docs(To, DbName, DocIds, Options),
        reply(To, done)
      end
    ),
  Handlers2 = maps:put(Stream, To, Handlers),
  Streams2 = maps:put(Ref, Stream, Streams),
  NewState = State#{ stream => Streams2, handlers => Handlers2 },
  loop(NewState).

do_fetch_docs(To, DbName, DocIds, Options) ->
  Limit = maps:get(limit, Options, 1),
  Deadline = maps:get(deadline, Options, 5000),
  barrel:multi_get(
    DbName,
    fun(Doc, Meta, _Acc) ->
      case maybe_wait(Limit, Deadline) of
        ok ->
          reply(To, {doc, Doc, Meta}),
          ok;
        timeout ->
          reply(To, {'EXIT', deadline}),
          exit(normal)
      end
    end,
    ok,
    DocIds,
    Options
  ).

handle_subscription(DbName, Options, {_Pid, Ref} = To, S) ->
  #{ handlers := Handlers, streams := Streams } = S,
  Since = maps:get(since, Options, 0),
  Limit = maps:get(limit, Options, 1),
  Timeout = maps:get(timeout, Options, 5000),
  Deadline = maps:get(deadline, Options, 5000),
  {Handler, _} =
    erlang:spawn_monitor(
      fun() ->
        _ = lager:info("subscribe ~p~n", [{DbName, Since}]),
        ChangesStream = barrel:subscribe_changes(DbName, Since, #{}),
        reply(To, {start_stream, self()}),
        wait_changes(ChangesStream, Limit, Deadline, Timeout, To)
      end
    ),
  
  Streams2 = maps:put(Ref, Handler, Streams),
  Handlers = maps:put(Handler, To, Handlers),
  loop(S#{ handlers => Handlers, streams => Streams2 }).


wait_changes(Stream, Limit, Deadline, Timeout, To) ->
  case barrel:await_change(Stream, Timeout) of
    {end_stream, _, LastSeq} ->
      reply(To, {done, LastSeq}),
      exit(normal);
    {end_stream, timeout} ->
      reply(To, {'EXIT', timeout}),
      exit(normal);
    Change ->
      case maybe_wait(Limit, Deadline) of
        ok ->
          reply(To, {change, Change}),
          wait_changes(Stream, Limit, Deadline, Timeout, To);
        timeout ->
          reply(To, {'EXIT', deadline}),
          exit(normal)
      end
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


maybe_wait(Limit, Deadline) ->
  case get(rpc_unacked) of
    undefined ->
      _ = put(rpc_unacked, 1),
      ok;
    Count when Count >= Limit ->
      wait_for_ack(Count, Deadline);
    Count ->
      drain_acks(Count)
  end.

wait_for_ack(Count, Deadline) ->
  receive
    {rpc_ack, N} ->
      drain_acks(Count -N)
  after Deadline ->
    timeout
  end.

drain_acks(Count) when Count < 0 ->
  erlang:error(negative_ack);
drain_acks(Count) ->
  receive
    {rpc_ack, N} ->
      drain_acks(Count - N)
  after 0 ->
    _ = put(rpc_unacked, Count),
    ok
  end.


reply({To, Tag}, Reply) ->
  Msg = {Tag, Reply},
  catch To ! Msg.

send_ack(Pid) -> send_ack(Pid, 1).

send_ack(Pid, N) -> Pid ! {rpc_ack, N}.

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
    erlang:error(timeout)
  end.
  

call(Node, FromPid, Msg, Timeout) ->
  Ref = make_ref(),
  CastMsg = {call, {FromPid, Ref}, Msg},
  _ = erlang:send({barrel_rpc, Node}, CastMsg,  [noconnect, nosuspend]),
  recv(Ref, Timeout).
  

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
