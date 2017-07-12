%% Copyright 2017, Bernard Notarianni
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

-module(barrel_replicate_checkpoint_SUITE).

-define(CH(DbId), barrel_replicate_api_wrapper:setup_channel(DbId)).

%% API
-export(
   [ all/0
   , init_per_suite/1
   , end_per_suite/1
   , init_per_testcase/2
   , end_per_testcase/2
   ]).

-export(
   [ checkpoints/1
   , history_size/1
   ]).

all() ->
  [ checkpoints
  , history_size
  ].

init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(barrel),
  _ = application:start(barrel),
  Config.

init_per_testcase(_, Config) ->
  {ok, _} = barrel:create_database(#{ <<"database_id">> => <<"testdb">> }),
  {ok, _} = barrel:create_database(#{ <<"database_id">> => <<"source">> }),
  [{db, <<"testdb">>} | Config].

end_per_testcase(_, _Config) ->
  ok = barrel:delete_database(<<"testdb">>),
  ok = barrel:delete_database(<<"source">>),
  ok.

end_per_suite(Config) ->
  ok = application:stop(barrel),
  _ = (catch rocksdb:destroy("docs", [])),
  Config.

checkpoints(_Config) ->
  RepId = <<"repid">>,
  Source = ?CH(<<"source">>),
  Target = ?CH(<<"testdb">>),

  RepConfig = #{id => RepId,
                source => Source,
                target => Target,
                options => #{ checkpoint_size => 5 } },

  C0 = barrel_replicate_checkpoint:new(RepConfig),
  C1 = barrel_replicate_checkpoint:set_last_seq(4, C0),
  C2 = barrel_replicate_checkpoint:maybe_write_checkpoint(C1),
  {error, not_found} = barrel_replicate_checkpoint:read_checkpoint_doc(Source, RepId),
  {error, not_found} = barrel_replicate_checkpoint:read_checkpoint_doc(Target, RepId),

  C3 = barrel_replicate_checkpoint:set_last_seq(5, C2),
  C4 = barrel_replicate_checkpoint:maybe_write_checkpoint(C3),
  [{0,5}] = read_start_last_seq(Source, RepId),
  [{0,5}] = read_start_last_seq(Target, RepId),

  C5 = barrel_replicate_checkpoint:set_last_seq(9, C4),
  C6 = barrel_replicate_checkpoint:maybe_write_checkpoint(C5),
  [{0,5}] = read_start_last_seq(Source, RepId),
  [{0,5}] = read_start_last_seq(Target, RepId),

  C7 = barrel_replicate_checkpoint:set_last_seq(12, C6),
  C8 = barrel_replicate_checkpoint:maybe_write_checkpoint(C7),
  [{0,12}] = read_start_last_seq(Source, RepId),
  [{0,12}] = read_start_last_seq(Target, RepId),

  ok = barrel_replicate_checkpoint:delete(C8),
  {error, not_found} = barrel_replicate_checkpoint:read_checkpoint_doc(Source, RepId),
  {error, not_found} = barrel_replicate_checkpoint:read_checkpoint_doc(Target, RepId),
  ok.

history_size(_Config) ->
  RepId = <<"repid">>,
  Source = ?CH(<<"source">>),
  Target = ?CH(<<"testdb">>),

  RepConfig = #{id => RepId,
                source => Source,
                target => Target,
                options => #{checkpoint_size => 5,
                             checkpoint_max_history => 3 }
               },

  replication_session([5,10,12], RepConfig),
  [{0,10}] = read_start_last_seq(Source, RepId),

  replication_session([15,17,22], RepConfig),
  [{10,22}, {0,10}] = read_start_last_seq(Source, RepId),

  replication_session([23,31], RepConfig),
  [{22,31}, {10,22}, {0,10}] = read_start_last_seq(Source, RepId),

  replication_session([40,50], RepConfig),
  [{31, 50}, {22,31}, {10,22}] = read_start_last_seq(Source, RepId),

  replication_session([60,70], RepConfig),
  [{50, 70}, {31, 50}, {22,31}] = read_start_last_seq(Source, RepId),
  ok.

replication_session(Seqs, RepConfig) ->
  C0 = barrel_replicate_checkpoint:new(RepConfig),
  MoveSeq = fun (N, Acc) ->
                Acc2 = barrel_replicate_checkpoint:set_last_seq(N, Acc),
                barrel_replicate_checkpoint:maybe_write_checkpoint(Acc2)
            end,
  lists:foldl(MoveSeq, C0, Seqs).

read_start_last_seq(Db, RepId) ->
  {ok, #{<<"history">> := History}} = barrel_replicate_checkpoint:read_checkpoint_doc(Db, RepId),
  [parse_history(H) || H <- History].


parse_history(#{<<"source_start_seq">> := StartSeq,
                <<"source_last_seq">> := LastSeq}) ->
  {StartSeq, LastSeq}.


