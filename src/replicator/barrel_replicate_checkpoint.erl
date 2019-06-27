%% Copyright 2017, Bernard Notarianni
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

-module(barrel_replicate_checkpoint).

%% gen_server API
-export([ new/1
        , set_last_seq/2
        , get_last_seq/1
        , get_start_seq/1
        , maybe_write_checkpoint/1
        , write_checkpoint/1
        , read_checkpoint_doc/2
        , delete/1
        ]).

-include("barrel.hrl").


-record(st, { repid       ::binary()
            , session_id  ::binary()  % replication session (history) id
            , source
            , target
            , start_seq=0 :: integer() % start seq for current repl session
            , last_seq=0  :: integer() % last received seq from source
            , target_seq  :: undefined | integer() % target seq for current repl session
            , options :: map()
            }).

-define(CHECKPOINT_SIZE, 10).
-define(MAX_CHECKPOINT_HISTORY, 20).

%% =============================================================================
%% Checkpoints management: when, where and what
%% =============================================================================

%% @doc Create a checkpoint object
new(RepConfig) ->
  #{ id := RepId,
     source := Source,
     target := Target } = RepConfig,

  Options = maps:get(options, RepConfig, #{}),

  StartSeq = checkpoint_start_seq(Source, Target, RepId),
  State = #st{ repid=RepId
             , source=Source
             , target=Target
             , session_id=barrel_lib:uniqid(binary)
             , start_seq=StartSeq
             , options = Options
             },
  set_next_target_seq(State).

set_last_seq(LastSeq, State) ->
  State#st{last_seq=LastSeq}.
get_last_seq(State) ->
  State#st.last_seq.
get_start_seq(State) ->
  State#st.start_seq.

%% @doc Decide it we should write checkpoints, and do it.
maybe_write_checkpoint(
    #st{last_seq=LastSeq, target_seq=TargetSeq}=State
) when LastSeq >= TargetSeq ->
  ok = write_checkpoint(State),
  set_next_target_seq(State);
maybe_write_checkpoint(State) ->
  State.

checkpoint_size(#st{ options = #{ checkpoint_size := Sz }}) -> Sz;
checkpoint_size(_) -> ?CHECKPOINT_SIZE.

set_next_target_seq(State) ->
  LastSeq = State#st.last_seq,
  CheckpointSize = checkpoint_size(State),
  TargetSeq = LastSeq + CheckpointSize,
  State#st{target_seq=TargetSeq}.


history_size(#st{options = #{ checkpoint_max_history := Max } }) -> Max;
history_size(_) -> ?MAX_CHECKPOINT_HISTORY.

%% @doc Write checkpoint information on both source and target databases.
write_checkpoint(State)  ->
  #st{repid = RepId,
      start_seq = StartSeq,
      last_seq = LastSeq,
      source = Source,
      target = Target,
      session_id = SessionId} = State,

  HistorySize = history_size(State),
  Checkpoint = #{<<"source_last_seq">>   => LastSeq
                ,<<"source_start_seq">>  => StartSeq
                ,<<"session_id">> => State#st.session_id
                ,<<"end_time">> => timestamp()
                ,<<"end_time_microsec">> => erlang:system_time(micro_seconds)
                },
  _ = [add_checkpoint(Db, RepId, SessionId, HistorySize, Checkpoint) || Db <- [Source, Target]],
  ok.

add_checkpoint(Db, RepId, SessionId, HistorySize, Checkpoint) ->
  Doc = case read_checkpoint_doc(Db, RepId) of
          {ok, #{<<"history">> := H}=PreviousDoc} ->
            H2 = case hd(H) of
                   #{<<"session_id">> := SessionId} ->
                     [Checkpoint|tl(H)];
                   _ ->
                     case length(H) >= HistorySize of
                       true ->
                         S = length(H) - HistorySize + 1,
                         T = lists:reverse(lists:nthtail(S, lists:reverse(H))),
                         [Checkpoint|T];
                       false ->
                         [Checkpoint|H]
                     end
                 end,
            PreviousDoc#{<<"history">> => H2};
          _ ->
            #{<<"history">> => [Checkpoint]}
        end,
  write_checkpoint_doc(Db, RepId, Doc).

%% @doc Compute replication starting seq from checkpoints history
checkpoint_start_seq(Source, Target, RepId) ->
  LastSeqSource = read_last_seq(Source, RepId),
  LastSeqTarget = read_last_seq(Target, RepId),
  min(LastSeqTarget, LastSeqSource).

read_last_seq(Db, RepId) ->
  case catch read_checkpoint_doc(Db, RepId) of
    {ok, Doc} ->
      History = maps:get(<<"history">>, Doc),
      Sorted = lists:sort(fun(H1,H2) ->
                              T1 = maps:get(<<"end_time_microsec">>, H1),
                              T2 = maps:get(<<"end_time_microsec">>, H2),
                              T1 > T2
                          end, History),
      LastHistory = hd(Sorted),
      maps:get(<<"source_last_seq">>, LastHistory);
    {error, not_found} ->
      0;
    Other ->
      ?LOG_ERROR("replication cannot read checkpoint on ~p: ~p", [Db, Other]),
      0
  end.

write_checkpoint_doc(Db, RepId, Checkpoint) ->
  barrel_replicate_api_wrapper:put_system_doc(Db, checkpoint_docid(RepId), Checkpoint).

read_checkpoint_doc(Db, RepId) ->
  barrel_replicate_api_wrapper:get_system_doc(Db, checkpoint_docid(RepId)).


delete(Checkpoint) ->
  #st{repid = RepId,
      source = Source,
      target = Target} = Checkpoint,

  ok = delete_checkpoint_doc(Source, RepId),
  ok = delete_checkpoint_doc(Target, RepId),
  ok.

delete_checkpoint_doc(Db, RepId) ->
  barrel_replicate_api_wrapper:delete_system_doc(Db, checkpoint_docid(RepId)).

checkpoint_docid(RepId) ->
  <<"replication-checkpoint-", RepId/binary>>.

%% =============================================================================
%% Helpers
%% =============================================================================

%% RFC3339 timestamps.
%% Note: doesn't include the time seconds fraction (RFC3339 says it's optional).
timestamp() ->
  {{Year, Month, Day}, {Hour, Min, Sec}} =  calendar:now_to_local_time(erlang:timestamp()),
  UTime = erlang:universaltime(),
  LocalTime = calendar:universal_time_to_local_time(UTime),
  DiffSecs = calendar:datetime_to_gregorian_seconds(LocalTime) - calendar:datetime_to_gregorian_seconds(UTime),
  Zone = zone(DiffSecs div 3600, (DiffSecs rem 3600) div 60),
  iolist_to_binary(
    io_lib:format("~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0w~s",
                  [Year, Month, Day, Hour, Min, Sec, Zone])).

zone(Hr, Min) when Hr >= 0, Min >= 0 ->
  io_lib:format("+~2..0w:~2..0w", [Hr, Min]);
zone(Hr, Min) ->
  io_lib:format("-~2..0w:~2..0w", [abs(Hr), abs(Min)]).
