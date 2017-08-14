%% Copyright (c) 2017. Benoit Chesneau
%%
%% Licensed under the Apache License, Version 2.0 (the "License"); you may not
%% use this file except in compliance with the License. You may obtain a copy of
%% the License at
%%
%%    http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
%% License for the specific language governing permissions and limitations under
%% the License.
-module(barrel_query_SUITE).
-author("benoitc").


-export([
  all/0,
  init_per_suite/1,
  end_per_suite/1,
  init_per_testcase/2,
  end_per_testcase/2
]).

-export([
  order_by_key/1,
  multiple_docs/1,
  range/1,
  limit_at/1,
  range_with_limit/1,
  equal_to/1,
  fix_range_test/1
]).

all() ->
  [
    order_by_key,
    multiple_docs,
    range,
    limit_at,
    range_with_limit,
    equal_to,
    fix_range_test
  ].

init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(barrel),
  Config.

init_per_testcase(_, Config) ->
  {ok, _} = barrel:create_database(#{ <<"database_id">> => <<"testdb">> }),
  [{db, <<"testdb">>} | Config].

end_per_testcase(_, _Config) ->
  ok = barrel:delete_database(<<"testdb">>),
  ok.

end_per_suite(Config) ->
  ok = application:stop(barrel),
  Config.


order_by_key(_Config) ->
  Doc = #{
    <<"id">> => <<"AndersenFamily">>,
    <<"lastName">> => <<"Andersen">>,
    <<"parents">> => [
      #{ <<"firstName">> => <<"Thomas">> },
      #{ <<"firstName">> => <<"Mary Kay">>}
    ],
    <<"children">> => [
      #{
        <<"firstName">> => <<"Henriette Thaulow">>, <<"gender">> => <<"female">>, <<"grade">> =>  5,
        <<"pets">> => [#{ <<"givenName">> => <<"Fluffy">> }]
      }
    ],
    <<"address">> => #{ <<"state">> => <<"WA">>, <<"county">> => <<"King">>, <<"city">> => <<"seattle">> },
    <<"creationDate">> => 1431620472,
    <<"isRegistered">> => true
  },
  {ok, <<"AndersenFamily">>, _Rev} = barrel:post(<<"testdb">>, Doc, #{}),
  timer:sleep(400),
  {ok, _Doc1, _Meta1} = barrel:get(<<"testdb">>, <<"AndersenFamily">>, #{}),

  Fun = fun(D, _Meta, Acc) -> {ok, [maps:get(<<"id">>, D) | Acc]} end,
  [<<"AndersenFamily">>] = barrel:walk(<<"testdb">>, <<"id">>, Fun, [], #{}),
  ok.


multiple_docs(_Config) ->
  DocA = #{ <<"test">> => <<"a">> },
  DocB = #{ <<"test">> => <<"b">> },
  BatchA = [{post, DocA} || _I <- lists:seq(1, 30)],
  BatchB = [{post, DocB} || _I <- lists:seq(1, 25)],
  ResultsA = barrel:write_batch(<<"testdb">>, BatchA, #{}),
  ResultsB = barrel:write_batch(<<"testdb">>, BatchB, #{}),

  IdsA = [Id || {ok, Id, _} <- ResultsA],
  IdsB = [Id || {ok, Id, _} <- ResultsB],

  30 = length(IdsA),
  25 = length(IdsB),

  All = barrel:fold_by_id(
    <<"testdb">>,
    fun(#{ <<"id">> := Id }, _, Acc) -> {ok, [Id | Acc]} end,
    [],
    #{}
  ),
  55 = length(All),

  All20 = barrel:fold_by_id(
    <<"testdb">>,
    fun(#{ <<"id">> := Id }, _, Acc) -> {ok, [Id | Acc]} end,
    [],
    #{max => 20}
  ),
  20 = length(All20),

  All40 = barrel:fold_by_id(
    <<"testdb">>,
    fun(#{ <<"id">> := Id }, _, Acc) -> {ok, [Id | Acc]} end,
    [],
    #{max => 40}
  ),

  40 = length(All40),

  QAll = barrel:walk(
    <<"testdb">>,
    <<"test/a">>,
    fun(Id, _, Acc) -> {ok, [Id | Acc]} end,
    [],
    #{}
  ),
  30 = length(QAll),

  Q15 = barrel:walk(
    <<"testdb">>,
    <<"test/a">>,
    fun(#{ <<"id">> := Id}, _, Acc) -> {ok, [Id | Acc]} end,
    [],
    #{ limit_to_first => 15 }
  ),
  15 = length(Q15),

  QBAll = barrel:walk(
    <<"testdb">>,
    <<"test/b">>,
    fun(Id, _, Acc) -> {ok, [Id | Acc]} end,
    [],
    #{}
  ),
  25 = length(QBAll).

limit_at(_Config) ->
  Batch = [{post, #{ <<"id">> => << I:32 >>}} || I <- lists:seq(1, 30)],
  _ = barrel:write_batch(<<"testdb">>, Batch, #{}),

  Q15 = barrel:walk(
    <<"testdb">>,
    <<"id">>,
    fun(#{ <<"id">> := Id}, _, Acc) -> {ok, [Id | Acc]} end,
    [],
    #{ limit_to_first => 15 }
  ),
  15 = length(Q15),

  E15 = [ << I:32 >> || I <- lists:seq(1, 15)],
  E15 = lists:reverse(Q15),

  QL15 = barrel:walk(
    <<"testdb">>,
    <<"id">>,
    fun(#{ <<"id">> := Id}, _, Acc) -> {ok, [Id | Acc]} end,
    [],
    #{ limit_to_last => 15 }
  ),
  15 = length(QL15),
  true = (QL15 =/= Q15),
  EL15 = [ << I:32 >> || I <- lists:seq(16, 30)],
  EL15 = QL15,
  ok.

range(_Config) ->
  Batch = [
    {post, #{ <<"id">> => <<"a">>, <<"o">> => #{ <<"test1">> => 1 }}},
    {post, #{ <<"id">> => <<"b">>, <<"o">> => #{ <<"test2">> => 1 }}},
    {post, #{ <<"id">> => <<"c">>, <<"o">> => #{ <<"test3">> => 1 }}},
    {post, #{ <<"id">> => <<"d">>, <<"o">> => #{ <<"test4">> => 1 }}},
    {post, #{ <<"id">> => <<"e">>, <<"o">> => #{ <<"test5">> => 1 }}},
    {post, #{ <<"id">> => <<"f">>, <<"o">> => #{ <<"test6">> => 1 }}},
    {post, #{ <<"id">> => <<"g">>, <<"o">> => #{ <<"test7">> => 1 }}},
    {post, #{ <<"id">> => <<"h">>, <<"o">> => #{ <<"test8">> => 1 }}}
  ],
  _ = barrel:write_batch(<<"testdb">>, Batch, #{}),

  Fun = fun(#{ <<"id">> := Id }, _, Acc) -> {ok, [ Id | Acc ]} end,
  All = [<<"h">>, <<"g">>, <<"f">>, <<"e">>, <<"d">>, <<"c">>, <<"b">>, <<"a">>],
  QAll = barrel:walk(
    <<"testdb">>,
    <<"o">>,
    Fun,
    [],
    #{}
  ),
  8  = length(QAll),
  All = QAll,

  C = [<<"h">>, <<"g">>, <<"f">>, <<"e">>, <<"d">>, <<"c">>],
  QC = barrel:walk(
    <<"testdb">>,
    <<"o">>,
    Fun,
    [],
    #{ start_at => <<"test3">> }
  ),
  C = QC,
  
  C1 = [<<"h">>, <<"g">>, <<"f">>, <<"e">>, <<"d">>],
  QC1 = barrel:walk(
    <<"testdb">>,
    <<"o">>,
    Fun,
    [],
    #{ next_to => <<"test3">> }
  ),
  C1 = QC1,
  
  
  F = [<<"f">>, <<"e">>, <<"d">>, <<"c">>, <<"b">>, <<"a">>],
  QF = barrel:walk(
    <<"testdb">>,
    <<"o">>,
    Fun,
    [],
    #{ end_at => <<"test6">> }
  ),
  F = QF,
  
  F1 = [<<"e">>, <<"d">>, <<"c">>, <<"b">>, <<"a">>],
  QF1 = barrel:walk(
    <<"testdb">>,
    <<"o">>,
    Fun,
    [],
    #{ previous_to => <<"test6">> }
  ),
  F1 = QF1,

  FC = [<<"f">>, <<"e">>, <<"d">>, <<"c">>],
  QFC = barrel:walk(
    <<"testdb">>,
    <<"o">>,
    Fun,
    [],
    #{ start_at => <<"test3">>, end_at => <<"test6">> }
  ),
  FC = QFC,
  
  FC1 = [<<"e">>, <<"d">>],
  QFC1 = barrel:walk(
    <<"testdb">>,
    <<"o">>,
    Fun,
    [],
    #{ next_to => <<"test3">>, previous_to => <<"test6">> }
  ),
  FC1 = QFC1,
  ok.

range_with_limit(_Config) ->
  Batch = [
    {post, #{ <<"id">> => <<"a">>, <<"o">> => #{ <<"test1">> => 1 }}},
    {post, #{ <<"id">> => <<"b">>, <<"o">> => #{ <<"test2">> => 1 }}},
    {post, #{ <<"id">> => <<"c">>, <<"o">> => #{ <<"test3">> => 1 }}},
    {post, #{ <<"id">> => <<"d">>, <<"o">> => #{ <<"test4">> => 1 }}},
    {post, #{ <<"id">> => <<"e">>, <<"o">> => #{ <<"test5">> => 1 }}},
    {post, #{ <<"id">> => <<"f">>, <<"o">> => #{ <<"test6">> => 1 }}},
    {post, #{ <<"id">> => <<"g">>, <<"o">> => #{ <<"test7">> => 1 }}},
    {post, #{ <<"id">> => <<"h">>, <<"o">> => #{ <<"test8">> => 1 }}}
  ],
  _ = barrel:write_batch(<<"testdb">>, Batch, #{}),
  Fun = fun(#{ <<"id">> := Id }, _, Acc) -> {ok, [ Id | Acc ]} end,
  C = [<<"d">>, <<"c">>],
  QC = barrel:walk(
    <<"testdb">>,
    <<"o">>,
    Fun,
    [],
    #{ start_at => <<"test3">>, limit_to_first => 2 }
  ),
  C = QC,
  
  C1 = [<<"e">>, <<"d">>],
  QC1 = barrel:walk(
    <<"testdb">>,
    <<"o">>,
    Fun,
    [],
    #{ next_to => <<"test3">>, limit_to_first => 2 }
  ),
  C1 = QC1,
  C3 = [<<"g">>, <<"h">>],
  QC3 = barrel:walk(
    <<"testdb">>,
    <<"o">>,
    Fun,
    [],
    #{ start_at => <<"test3">>, limit_to_last => 2 }
  ),
  C3 = QC3,
  
  C4 = [<<"g">>, <<"h">>],
  QC4 = barrel:walk(
    <<"testdb">>,
    <<"o">>,
    Fun,
    [],
    #{ next_to => <<"test3">>, limit_to_last => 2 }
  ),
  C4 = QC4,
  
  F = [ <<"b">>, <<"a">> ],
  QF = barrel:walk(
    <<"testdb">>,
    <<"o">>,
    Fun,
    [],
    #{ end_at => <<"test6">>, limit_to_first => 2 }
  ),
  F = QF,
  
  F1 = [<<"b">>, <<"a">>],
  QF1 = barrel:walk(
    <<"testdb">>,
    <<"o">>,
    Fun,
    [],
    #{ previous_to => <<"test6">>, limit_to_first => 2 }
  ),
  F1 = QF1,
  
  F2 = [ <<"e">>, <<"f">>],
  QF2 = barrel:walk(
    <<"testdb">>,
    <<"o">>,
    Fun,
    [],
    #{ end_at => <<"test6">>, limit_to_last => 2 }
  ),
  F2 = QF2,
  
  F3 = [<<"d">>, <<"e">>],
  QF3 = barrel:walk(
    <<"testdb">>,
    <<"o">>,
    Fun,
    [],
    #{ previous_to => <<"test6">>, limit_to_last => 2 }
  ),
  F3 = QF3,
  ok.

equal_to(_Config) ->
  Batch = [
    {post, #{ <<"id">> => <<"a">>, <<"o">> => #{ <<"test1">> => 1 }}},
    {post, #{ <<"id">> => <<"b">>, <<"o">> => #{ <<"test2">> => 1 }}},
    {post, #{ <<"id">> => <<"c">>, <<"o">> => #{ <<"test3">> => 2 }}},
    {post, #{ <<"id">> => <<"d">>, <<"o">> => #{ <<"test4">> => 2 }}},
    {post, #{ <<"id">> => <<"e">>, <<"o">> => #{ <<"test5">> => 1 }}},
    {post, #{ <<"id">> => <<"f">>, <<"o">> => #{ <<"test6">> => 3 }}},
    {post, #{ <<"id">> => <<"g">>, <<"o">> => #{ <<"test7">> => 1 }}},
    {post, #{ <<"id">> => <<"h">>, <<"o">> => #{ <<"test8">> => 1 }}}
  ],
  _ = barrel:write_batch(<<"testdb">>, Batch, #{}),
  
  Fun = fun(#{ <<"id">> := Id }, _, Acc) -> {ok, [ Id | Acc ]} end,
  Q1 = barrel:walk(
    <<"testdb">>,
    <<"o">>,
    Fun,
    [],
    #{ equal_to => 1}
  ),
  5 = length(Q1),
  [<<"h">>, <<"g">>,  <<"e">>,  <<"b">>, <<"a">>] = Q1,
  Q2 = barrel:walk(
    <<"testdb">>,
    <<"o">>,
    Fun,
    [],
    #{ equal_to => 2}
  ),
   2 = length(Q2),
  [<<"d">>, <<"c">>] = Q2,
  Q3 = barrel:walk(
    <<"testdb">>,
    <<"o">>,
    Fun,
    [],
    #{ equal_to => 3}
  ),
  1 = length(Q3),
  [<<"f">>] = Q3,
  ok.


fix_range_test(_Config) ->
  Ids = [
    <<"9NGOZFVYl83mhUc8g2">>,<<"9NGOZFVYl83mhUc8g1">>,<<"9NGOZFVYl83mhUc8g0">>,<<"9NGOZFVYl83mhUc8fz">>,
    <<"9NGOZFVYl83mhUc8fy">>,<<"9NGOZFVYl83mhUc8fx">>,<<"9NGOZFVYl83mhUc8fw">>,<<"9NGOZFVYl83mhUc8fv">>,
    <<"9NGOZFVYl83mhUc8fu">>,<<"9NGOZFVYl83mhUc8ft">>,<<"9NGOZFVYl83mhUc8fs">>,<<"9NGOZFVYl83mhUc8fr">>,
    <<"9NGOZFVYl83mhUc8fq">>,<<"9NGOZFVYl83mhUc8fp">>,<<"9NGOZFVYl83mhUc8fo">>
  ],
  
  Batch = lists:foldl(
    fun(Id, Acc) ->
      [{post, #{ <<"id">> => <<"doc-", Id/binary>>, <<"docId">> => Id }} | Acc]
    end,
    [],
    Ids
  ),
  
  _ = barrel:write_batch(<<"testdb">>, Batch, #{}),
  Fun = fun(#{ <<"docId">> := Id }, _, Acc) -> {ok, Acc ++ [Id]} end,
  
  All = barrel:walk(<<"testdb">>, <<"docId">>, Fun, [], #{ limit_to_last => 15 }),
  Ids = All,
  Nth = lists:nth(10, Ids),
  ExpectBefore = lists:sublist(Ids, 11, 5),
  Before = barrel:walk(
    <<"testdb">>, <<"docId">>, Fun, [], #{ previous_to => Nth, limit_to_last => 10 }
  ),
  Before = ExpectBefore,
  ExpectAfter = lists:reverse(lists:sublist(Ids, 1, 9)),
  After = barrel:walk(
    <<"testdb">>, <<"docId">>, Fun, [], #{ next_to => Nth, limit_to_first => 10 }
  ),
  After = ExpectAfter.
  
