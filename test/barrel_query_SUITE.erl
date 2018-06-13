%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. Apr 2018 14:46
%%%-------------------------------------------------------------------
-module(barrel_query_SUITE).
-author("benoitc").

%% API
-export([
  all/0,
  init_per_suite/1,
  end_per_suite/1,
  init_per_testcase/2,
  end_per_testcase/2
]).

-export([
  basic/1,
  multiple_docs/1,
  limit_at/1,
  equal_to/1,
  range/1,
  range_with_limit/1,
  fix_range_test/1
]).

all() ->
  [
    basic,
    multiple_docs,
    limit_at,
%%    equal_to,
    range,
    range_with_limit,
    fix_range_test
  ].

init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(barrel),
  {ok, _} = barrel_store_sup:start_store(default, barrel_memory_storage, #{}),
  
  Config.

init_per_testcase(_, Config) ->
  ok = barrel:create_barrel(<<"test">>, #{}),
  Config.

end_per_testcase(_, _Config) ->
  ok = barrel:delete_barrel(<<"test">>),
  ok.

end_per_suite(Config) ->
  ok = application:stop(barrel),
  Config.


basic(_Suite) ->
  Docs = [
    #{ <<"id">> => <<"a">>, <<"v">> => 1, <<"o">> => #{ <<"o1">> => 1, << "o2">> => 1}}
  ],
  [{ok, <<"a">>, Rev}] = barrel:save_docs(<<"test">>, Docs),
  Fun = fun(Doc, Acc) -> {ok, [Doc | Acc]} end,
  [#{ <<"id">> := <<"a">>, <<"v">> := 1, <<"_rev">> := Rev }] = barrel:query(<<"test">>, <<"/id">>, Fun, [], #{}),
  ok.



multiple_docs(_Config) ->
  DocA = #{ <<"test">> => <<"a">> },
  DocB = #{ <<"test">> => <<"b">> },
  BatchA = [DocA || _I <- lists:seq(1, 30)],
  BatchB = [DocB || _I <- lists:seq(1, 25)],
  ResultsA = barrel:save_docs(<<"test">>, BatchA),
  ResultsB = barrel:save_docs(<<"test">>, BatchB),

  IdsA = [Id || {ok, Id, _} <- ResultsA],
  IdsB = [Id || {ok, Id, _} <- ResultsB],

  30 = length(IdsA),
  25 = length(IdsB),


  {ok, #{ <<"test">> := << "a">> }} = barrel:fetch_doc(<<"test">>, lists:nth(1, IdsA), #{}),

  All = barrel:query(
    <<"test">>,
    <<"/id">>,
    fun(#{ <<"id">> := Id }, Acc) -> {ok, [Id | Acc]} end,
    [],
    #{}
  ),
  55 = length(All),

  All20 = barrel:query(
    <<"test">>,
    <<"/id">>,
    fun(#{ <<"id">> := Id }, Acc) -> {ok, [Id | Acc]} end,
    [],
    #{limit_to_first => 20}
  ),
  20 = length(All20),

  All40 = barrel:query(
    <<"test">>,
    <<"/id">>,
    fun(#{ <<"id">> := Id }, Acc) -> {ok, [Id | Acc]} end,
    [],
    #{limit_to_first => 40}
  ),

  40 = length(All40),

  QAll = barrel:query(
    <<"test">>,
    <<"test/a">>,
    fun(#{ <<"id">> := Id }, Acc) -> {ok, [Id | Acc]} end,
    [],
    #{}
  ),
  30 = length(QAll),

  Q15 = barrel:query(
    <<"test">>,
    <<"test/a">>,
    fun(#{ <<"id">> := Id}, Acc) -> {ok, [Id | Acc]} end,
    [],
    #{ limit_to_first => 15 }
  ),
  15 = length(Q15),

  QBAll = barrel:query(
    <<"test">>,
    <<"test/b">>,
    fun(#{ <<"id">> := Id }, Acc) -> {ok, [Id | Acc]} end,
    [],
    #{}
  ),
  25 = length(QBAll).

limit_at(_Config) ->
  Batch = [#{ <<"id">> => << I:32 >>} || I <- lists:seq(1, 30)],
  _ = barrel:save_docs(<<"test">>, Batch),

  Q15 = barrel:query(
    <<"test">>,
    <<"id">>,
    fun(#{ <<"id">> := Id}, Acc) -> {ok, [Id | Acc]} end,
    [],
    #{ limit_to_first => 15 }
  ),
  15 = length(Q15),

  E15 = [ << I:32 >> || I <- lists:seq(1, 15)],
  E15 = lists:reverse(Q15),

  QL15 = barrel:query(
    <<"test">>,
    <<"id">>,
    fun(#{ <<"id">> := Id}, Acc) -> {ok, [Id | Acc]} end,
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
    #{ <<"id">> => <<"a">>, <<"o">> => #{ <<"test1">> => 1 }},
    #{ <<"id">> => <<"b">>, <<"o">> => #{ <<"test2">> => 1 }},
    #{ <<"id">> => <<"c">>, <<"o">> => #{ <<"test3">> => 1 }},
    #{ <<"id">> => <<"d">>, <<"o">> => #{ <<"test4">> => 1 }},
    #{ <<"id">> => <<"e">>, <<"o">> => #{ <<"test5">> => 1 }},
    #{ <<"id">> => <<"f">>, <<"o">> => #{ <<"test6">> => 1 }},
    #{ <<"id">> => <<"g">>, <<"o">> => #{ <<"test7">> => 1 }},
    #{ <<"id">> => <<"h">>, <<"o">> => #{ <<"test8">> => 1 }}
  ],
  _ = barrel:save_docs(<<"test">>, Batch),

  Fun = fun(#{ <<"id">> := Id }, Acc) -> {ok, [ Id | Acc ]} end,
  All = [<<"h">>, <<"g">>, <<"f">>, <<"e">>, <<"d">>, <<"c">>, <<"b">>, <<"a">>],
  QAll = barrel:query(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{}
  ),
  8  = length(QAll),
  All = QAll,

  C = [<<"h">>, <<"g">>, <<"f">>, <<"e">>, <<"d">>, <<"c">>],
  QC = barrel:query(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{ start_at => <<"test3">> }
  ),
  C = QC,

  C1 = [<<"h">>, <<"g">>, <<"f">>, <<"e">>, <<"d">>],
  QC1 = barrel:query(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{ next_to => <<"test3">> }
  ),
  C1 = QC1,


  F = [<<"f">>, <<"e">>, <<"d">>, <<"c">>, <<"b">>, <<"a">>],
  QF = barrel:query(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{ end_at => <<"test6">> }
  ),
  F = QF,

  F1 = [<<"e">>, <<"d">>, <<"c">>, <<"b">>, <<"a">>],
  QF1 = barrel:query(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{ previous_to => <<"test6">> }
  ),
  F1 = QF1,

  FC = [<<"f">>, <<"e">>, <<"d">>, <<"c">>],
  QFC = barrel:query(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{ start_at => <<"test3">>, end_at => <<"test6">> }
  ),
  FC = QFC,

  FC1 = [<<"e">>, <<"d">>],
  QFC1 = barrel:query(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{ next_to => <<"test3">>, previous_to => <<"test6">> }
  ),
  FC1 = QFC1,
  ok.

range_with_limit(_Config) ->
  Batch = [
    #{ <<"id">> => <<"a">>, <<"o">> => #{ <<"test1">> => 1 }},
    #{ <<"id">> => <<"b">>, <<"o">> => #{ <<"test2">> => 1 }},
    #{ <<"id">> => <<"c">>, <<"o">> => #{ <<"test3">> => 1 }},
    #{ <<"id">> => <<"d">>, <<"o">> => #{ <<"test4">> => 1 }},
    #{ <<"id">> => <<"e">>, <<"o">> => #{ <<"test5">> => 1 }},
    #{ <<"id">> => <<"f">>, <<"o">> => #{ <<"test6">> => 1 }},
    #{ <<"id">> => <<"g">>, <<"o">> => #{ <<"test7">> => 1 }},
    #{ <<"id">> => <<"h">>, <<"o">> => #{ <<"test8">> => 1 }}
  ],
  _ = barrel:save_docs(<<"test">>, Batch),
  Fun = fun(#{ <<"id">> := Id }, Acc) -> {ok, [ Id | Acc ]} end,
  C = [<<"d">>, <<"c">>],
  QC = barrel:query(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{ start_at => <<"test3">>, limit_to_first => 2 }
  ),
  C = QC,

  C1 = [<<"e">>, <<"d">>],
  QC1 = barrel:query(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{ next_to => <<"test3">>, limit_to_first => 2 }
  ),
  C1 = QC1,
  C3 = [<<"g">>, <<"h">>],
  QC3 = barrel:query(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{ start_at => <<"test3">>, limit_to_last => 2 }
  ),
  C3 = QC3,

  C4 = [<<"g">>, <<"h">>],
  QC4 = barrel:query(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{ next_to => <<"test3">>, limit_to_last => 2 }
  ),
  C4 = QC4,

  F = [ <<"b">>, <<"a">> ],
  QF = barrel:query(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{ end_at => <<"test6">>, limit_to_first => 2 }
  ),
  F = QF,

  F1 = [<<"b">>, <<"a">>],
  QF1 = barrel:query(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{ previous_to => <<"test6">>, limit_to_first => 2 }
  ),
  F1 = QF1,

  F2 = [ <<"e">>, <<"f">>],
  QF2 = barrel:query(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{ end_at => <<"test6">>, limit_to_last => 2 }
  ),
  F2 = QF2,

  F3 = [<<"d">>, <<"e">>],
  QF3 = barrel:query(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{ previous_to => <<"test6">>, limit_to_last => 2 }
  ),
  F3 = QF3,
  ok.

equal_to(_Config) ->
  Batch = [
    #{ <<"id">> => <<"a">>, <<"o">> => #{ <<"test1">> => 1 }},
    #{ <<"id">> => <<"b">>, <<"o">> => #{ <<"test2">> => 1 }},
    #{ <<"id">> => <<"c">>, <<"o">> => #{ <<"test3">> => 2 }},
    #{ <<"id">> => <<"d">>, <<"o">> => #{ <<"test4">> => 2 }},
    #{ <<"id">> => <<"e">>, <<"o">> => #{ <<"test5">> => 1 }},
    #{ <<"id">> => <<"f">>, <<"o">> => #{ <<"test6">> => 3 }},
    #{ <<"id">> => <<"g">>, <<"o">> => #{ <<"test7">> => 1 }},
    #{ <<"id">> => <<"h">>, <<"o">> => #{ <<"test8">> => 1 }}
  ],
  _ = barrel:save_docs(<<"test">>, Batch),

  Fun = fun(#{ <<"id">> := Id }, Acc) -> {ok, [ Id | Acc ]} end,
  Q1 = barrel:query(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{ equal_to => 1}
  ),
  5 = length(Q1),
  [<<"h">>, <<"g">>,  <<"e">>,  <<"b">>, <<"a">>] = Q1,
  Q2 = barrel:query(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{ equal_to => 2}
  ),
  2 = length(Q2),
  [<<"d">>, <<"c">>] = Q2,
  Q3 = barrel:query(
    <<"test">>,
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
      [#{ <<"id">> => <<"doc-", Id/binary>>, <<"docId">> => Id } | Acc]
    end,
    [],
    Ids
  ),

  _ = barrel:save_docs(<<"test">>, Batch),
  Fun = fun(#{ <<"docId">> := Id }, Acc) -> {ok, Acc ++ [Id]} end,

  All = barrel:query(<<"test">>, <<"docId">>, Fun, [], #{ limit_to_last => 15 }),
  Ids = All,
  Nth = lists:nth(10, Ids),
  ExpectBefore = lists:sublist(Ids, 11, 5),
  Before = barrel:query(
    <<"test">>, <<"docId">>, Fun, [], #{ previous_to => Nth, limit_to_last => 10 }
  ),
  Before = ExpectBefore,
  ExpectAfter = lists:reverse(lists:sublist(Ids, 1, 9)),
  After = barrel:query(
    <<"test">>, <<"docId">>, Fun, [], #{ next_to => Nth, limit_to_first => 10 }
  ),
  After = ExpectAfter.
