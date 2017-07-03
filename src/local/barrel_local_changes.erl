%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. Jun 2017 14:47
%%%-------------------------------------------------------------------

%% @doc local changes consumer working just like the remote one


-module(barrel_local_changes).
-author("benoitc").

%% API
-export([
  start_link/4
]).

-export([
  init/5,
  wait_changes/1
]).

-export([
  system_continue/3,
  system_code_change/4,
  system_terminate/4
]).


start_link(Owner, DbId, Since, Options) ->
  proc_lib:start_link(?MODULE, init, [Owner, self(), DbId, Since, Options]).

init(Owner, Parent, DbId, Since0, Options) ->
  process_flag(trap_exit, true),
  proc_lib:init_ack(Parent, {ok, self()}),
  
  %% monitor the database
  MRef = erlang:monitor(process, barrel_store:db_pid(DbId)),
  
  %% monitor the owner
  _ =  erlang:monitor(process, Owner),
  
  %% register to the changes
  _ = barrel_event:reg(DbId),
  
  State = #{
    db => DbId,
    parent => Parent,
    owner => Owner,
    db_ref => MRef,
    last_seq => Since0,
    options => Options
  },
  
  Since1 = stream_changes(State),
  wait_changes(State#{ last_seq => Since1 }).

wait_changes(State) ->
  #{ parent := Parent, owner := Owner, db_ref := Ref } = State,
  receive
    {'$barrel_event', _, db_updated} ->
      LastSeq = stream_changes(State),
      wait_changes(State#{ last_seq => LastSeq });
    {'DOWN', Ref, process, _DbPid, _Reason} ->
      exit({shutdown, db_down});
    {'DOWN', _, process, Owner, _Reason} ->
      exit(normal);
    {'EXIT', Parent, _} ->
      exit(normal);
    {system, From, Request} ->
      sys:handle_system_msg(
        Request, From, Parent, ?MODULE, [],
        {wait_changes, State});
    Other ->
      _ = lager:error("~s: got unexpected message: ~p~n", [?MODULE_STRING, Other]),
      exit(unexpected_message)
  end.

stream_changes(State) ->
  #{
    db := DbId, owner := Owner,
    last_seq := LastSeq, options := Options
  } = State,
  
  barrel_db:changes_since(
    DbId,
    LastSeq,
    fun(Change, OldSeq) ->
      Seq = maps:get(<<"seq">>, Change),
      _ = Owner ! {change, self(), Change},
      {ok, erlang:max(OldSeq, Seq)}
    end,
    LastSeq,
    Options
  ).

system_continue(_, _, {wait_changes, State}) ->
  wait_changes(State).

-spec system_terminate(any(), _, _, _) -> no_return().
system_terminate(Reason, _, _, _State) ->
  _ = lager:debug(
    "~s terminate: ~p",
    [?MODULE_STRING,Reason]
  ),
  
  catch barrel_event:unreg(),
  exit(Reason).

system_code_change(Misc, _, _, _) ->
  {ok, Misc}.
  
  
  
  