%% Copyright 2017, Benoit Chesneau
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

-module(barrel_changes_listener).
-author("benoitc").

%% API
-export([
  start_link/2,
  stop/1,
  changes/1
]).


-export([
  init_feed/3,
  wait_changes/1
]).

-export([
  system_continue/3,
  system_code_change/4,
  system_terminate/4
]).

-include("barrel.hrl").

-define(TIMEOUT, 5000).

start_link(DbId, Options) ->
  case barrel_store:whereis_db(DbId) of
    undefined ->
      _ = lager:debug(
        "~s: db ~p not found",
        [?MODULE_STRING, DbId]
      ),
      {error, db_not_found};
    Db ->
      proc_lib:start_link(?MODULE, init_feed, [self(), Db, Options])
  end.


stop(FeedPid) ->
  MRef = erlang:monitor(process, FeedPid),
  FeedPid ! stop,
  receive
    {'DOWN', MRef, _, _, _} -> ok
  end.

changes(FeedPid) ->
  Tag = make_ref(),
  MRef = erlang:monitor(process, FeedPid),
  FeedPid ! {get_changes, self(), Tag},
  receive
    {changes, Tag, Changes} -> Changes;
    {'DOWN', MRef, _, _, Reason} -> exit(Reason)
  after ?TIMEOUT ->
    erlang:demonitor(MRef, [flush]),
    exit(timeout)
  end.

init_feed(Parent, Db, Options) ->
  process_flag(trap_exit, true),
  proc_lib:init_ack(Parent, {ok, self()}),
  Since = maps:get(since, Options, 0),
  ChangeCb = maps:get(changes_cb, Options, nil),
  Mode = maps:get(mode, Options, binary),

  %% make change callback
  ChangeFun = case {Mode, ChangeCb} of
                {binary, nil} ->
                  fun(Change, {LastSeq, ChangeQ}) ->
                    Seq = maps:get(<<"seq">>, Change),
                    Encoded = encode_sse(Change),
                    {ok, {erlang:max(LastSeq, Seq), queue:in(Encoded, ChangeQ)}}
                  end;
                {binary, ChangeCb} ->
                  fun(Change, {LastSeq, ChangeQ}) ->
                    Seq = maps:get(<<"seq">>, Change),
                    ChangeCb(encode_sse(Change)),
                    {ok, {erlang:max(LastSeq, Seq), ChangeQ}}
                  end;
                {_, nil} ->
                  fun(Change, {LastSeq, ChangeQ}) ->
                    Seq = maps:get(<<"seq">>, Change),
                    {ok, {erlang:max(LastSeq, Seq), queue:in(Change, ChangeQ)}}
                  end;
                {_, ChangeCb} ->
                  fun(Change, {LastSeq, ChangeQ}) ->
                    Seq = maps:get(<<"seq">>, Change),
                    ChangeCb(Change),
                    {ok, {erlang:max(LastSeq, Seq), ChangeQ}}
                  end
              end,

  %% retrieve change options from the options given to the feed
  Ref = erlang:monitor(process, Db#db.pid),

  %% initialize the changes
  State0 =
  #{parent => Parent,
    db => Db,
    db_ref => Ref,
    opts => Options,
    change_fun => ChangeFun,
    changes => queue:new(),
    last_seq => Since},

  %% register to the changes
  barrel_event:reg(Db#db.id),

  %% get initial changes
  State1 = get_changes(State0),

  %% wait for events
  wait_changes(State1).


wait_changes(State = #{ parent := Parent , db_ref := Ref}) ->
  receive
    {get_changes, Pid, Tag} ->
      Changes = maps:get(changes, State),
      Pid ! {changes, Tag, queue:to_list(Changes)},
      wait_changes(State#{ changes => queue:new() });
    stop ->
      exit(normal);
    {'$barrel_event', _, db_updated} ->
      NewState = get_changes(State),
      wait_changes(NewState);
    {'EXIT', Parent, _} ->
      exit(normal);
    {system, From, Request} ->
      sys:handle_system_msg(
        Request, From, Parent, ?MODULE, [],
        {wait_changes, State});
    {'DOWN', Ref, process, _Pid, Reason} ->
      #{ db := #db{ id = DbId } } = State,
      _ = lager:info("[~p] database down dbid=~p reason=~p (listener pid=~p)", [?MODULE, DbId, Reason, self()]),
      exit(normal)
  end.

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

get_changes(State) ->
  #{db := Db, opts :=  Opts, change_fun := ChangeFun, last_seq := LastSeq, changes := Changes} = State,
  {LastSeq1, Changes1} = barrel_db:changes_since(
    Db#db.id, LastSeq, ChangeFun, {LastSeq, Changes},
    Opts
  ),
  State#{ last_seq => LastSeq1, changes => Changes1}.

encode_sse(Change) ->
  Seq = maps:get(<<"seq">>, Change),
  << "id: ", (integer_to_binary(Seq))/binary, "\n",
     "data: ", (jsx:encode(Change))/binary >>.
