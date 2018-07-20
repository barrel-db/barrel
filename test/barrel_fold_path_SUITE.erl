%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. Apr 2018 14:46
%%%-------------------------------------------------------------------
-module(barrel_fold_path_SUITE).
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
  {ok, _} = barrel_store_provider_sup:start_store(default, barrel_memory_storage, #{}),
  
  Config.

init_per_testcase(_, Config) ->
  ok = barrel:create_barrel(<<"test">>, #{}),
  Config.

end_per_testcase(_, _Config) ->
  ok = barrel:drop_barrel(<<"test">>),
  ok.

end_per_suite(Config) ->
  ok = application:stop(barrel),
  Config.


basic(_Suite) ->
  Docs = [
    #{ <<"id">> => <<"a">>, <<"v">> => 1, <<"o">> => #{ <<"o1">> => 1, << "o2">> => 1}}
  ],
  {ok, [{ok, <<"a">>, Rev}]} = barrel:save_docs(<<"test">>, Docs),
  Fun = fun(Doc, Acc) -> {ok, [Doc | Acc]} end,
  [#{ <<"id">> := <<"a">>, <<"v">> := 1, <<"_rev">> := Rev }] = barrel:fold_path(<<"test">>, <<"/id">>, Fun, [], #{}),
  ok.



multiple_docs(_Config) ->
  DocA = #{ <<"test">> => <<"a">> },
  DocB = #{ <<"test">> => <<"b">> },
  BatchA = [DocA || _I <- lists:seq(1, 30)],
  BatchB = [DocB || _I <- lists:seq(1, 25)],
  {ok, ResultsA} = barrel:save_docs(<<"test">>, BatchA),
  {ok, ResultsB} = barrel:save_docs(<<"test">>, BatchB),

  IdsA = [Id || {ok, Id, _} <- ResultsA],
  IdsB = [Id || {ok, Id, _} <- ResultsB],

  30 = length(IdsA),
  25 = length(IdsB),


  {ok, #{ <<"test">> := << "a">> }} = barrel:fetch_doc(<<"test">>, lists:nth(1, IdsA), #{}),

  All = barrel:fold_path(
    <<"test">>,
    <<"/id">>,
    fun(#{ <<"id">> := Id }, Acc) -> {ok, [Id | Acc]} end,
    [],
    #{}
  ),
  55 = length(All),

  All20 = barrel:fold_path(
    <<"test">>,
    <<"/id">>,
    fun(#{ <<"id">> := Id }, Acc) -> {ok, [Id | Acc]} end,
    [],
    #{limit_to_first => 20}
  ),
  20 = length(All20),

  All40 = barrel:fold_path(
    <<"test">>,
    <<"/id">>,
    fun(#{ <<"id">> := Id }, Acc) -> {ok, [Id | Acc]} end,
    [],
    #{limit_to_first => 40}
  ),

  40 = length(All40),

  QAll = barrel:fold_path(
    <<"test">>,
    <<"test/a">>,
    fun(#{ <<"id">> := Id }, Acc) -> {ok, [Id | Acc]} end,
    [],
    #{}
  ),
  30 = length(QAll),

  Q15 = barrel:fold_path(
    <<"test">>,
    <<"test/a">>,
    fun(#{ <<"id">> := Id}, Acc) -> {ok, [Id | Acc]} end,
    [],
    #{ limit_to_first => 15 }
  ),
  15 = length(Q15),

  QBAll = barrel:fold_path(
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

  Q15 = barrel:fold_path(
    <<"test">>,
    <<"id">>,
    fun(#{ <<"id">> := Id}, Acc) -> {ok, [Id | Acc]} end,
    [],
    #{ limit_to_first => 15 }
  ),
  15 = length(Q15),

  E15 = [ << I:32 >> || I <- lists:seq(1, 15)],
  E15 = lists:reverse(Q15),

  QL15 = barrel:fold_path(
    <<"test">>,
    <<"id">>,
    fun(#{ <<"id">> := Id}, Acc) -> {ok, [Id | Acc]} end,
    [],
    #{ limit_to_last => 15 }
  ),
  15 = length(QL15),
  io:format("q15=~p~n~nql15=~p~n", [Q15, QL15]),


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
  QAll = barrel:fold_path(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{}
  ),
  8  = length(QAll),
  All = QAll,

  C = [<<"h">>, <<"g">>, <<"f">>, <<"e">>, <<"d">>, <<"c">>],
  QC = barrel:fold_path(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{ start_at => <<"test3">>, order_by => order_by_key }
  ),
  C = QC,

  C1 = [<<"h">>, <<"g">>, <<"f">>, <<"e">>, <<"d">>],
  QC1 = barrel:fold_path(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{ next_to => <<"test3">>, order_by => order_by_key}
  ),
  C1 = QC1,


  F = [<<"f">>, <<"e">>, <<"d">>, <<"c">>, <<"b">>, <<"a">>],
  QF = barrel:fold_path(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{ end_at => <<"test6">>, order_by => order_by_key }
  ),
  F = QF,

  F1 = [<<"e">>, <<"d">>, <<"c">>, <<"b">>, <<"a">>],
  QF1 = barrel:fold_path(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{ previous_to => <<"test6">>, order_by => order_by_key }
  ),
  F1 = QF1,

  FC = [<<"f">>, <<"e">>, <<"d">>, <<"c">>],
  QFC = barrel:fold_path(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{ start_at => <<"test3">>, end_at => <<"test6">>, order_by => order_by_key }
  ),
  FC = QFC,

  FC1 = [<<"e">>, <<"d">>],
  QFC1 = barrel:fold_path(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{ next_to => <<"test3">>, previous_to => <<"test6">>, order_by => order_by_key }
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
  QC = barrel:fold_path(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{ start_at => <<"test3">>, limit_to_first => 2, order_by => order_by_key }
  ),
  C = QC,

  C1 = [<<"e">>, <<"d">>],
  QC1 = barrel:fold_path(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{ next_to => <<"test3">>, limit_to_first => 2, order_by => order_by_key }
  ),
  C1 = QC1,
  C3 = [<<"g">>, <<"h">>],
  QC3 = barrel:fold_path(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{ start_at => <<"test3">>, limit_to_last => 2, order_by => order_by_key }
  ),
  C3 = QC3,

  C4 = [<<"g">>, <<"h">>],
  QC4 = barrel:fold_path(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{ next_to => <<"test3">>, limit_to_last => 2, order_by => order_by_key }
  ),
  C4 = QC4,

  F = [ <<"b">>, <<"a">> ],
  QF = barrel:fold_path(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{ end_at => <<"test6">>, limit_to_first => 2, order_by => order_by_key }
  ),
  F = QF,

  F1 = [<<"b">>, <<"a">>],
  QF1 = barrel:fold_path(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{ previous_to => <<"test6">>, limit_to_first => 2, order_by => order_by_key }
  ),
  F1 = QF1,

  F2 = [ <<"e">>, <<"f">>],
  QF2 = barrel:fold_path(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{ end_at => <<"test6">>, limit_to_last => 2, order_by => order_by_key }
  ),
  F2 = QF2,

  F3 = [<<"d">>, <<"e">>],
  QF3 = barrel:fold_path(
    <<"test">>,
    <<"o">>,
    Fun,
    [],
    #{ previous_to => <<"test6">>, limit_to_last => 2, order_by => order_by_key }
  ),
  F3 = QF3,
  ok.

equal_to(_Config) ->
  Batch = [
    #{ <<"id">> => <<"a">>, <<"o">> => #{ <<"test">> => 1 }},
    #{ <<"id">> => <<"b">>, <<"o">> => #{ <<"test">> => 1 }},
    #{ <<"id">> => <<"c">>, <<"o">> => #{ <<"test">> => 2 }},
    #{ <<"id">> => <<"d">>, <<"o">> => #{ <<"test">> => 2 }},
    #{ <<"id">> => <<"e">>, <<"o">> => #{ <<"test">> => 1 }},
    #{ <<"id">> => <<"f">>, <<"o">> => #{ <<"test">> => 3 }},
    #{ <<"id">> => <<"g">>, <<"o">> => #{ <<"test">> => 1 }},
    #{ <<"id">> => <<"h">>, <<"o">> => #{ <<"test">> => 1 }}
  ],
  _ = barrel:save_docs(<<"test">>, Batch),

  Fun = fun(#{ <<"id">> := Id }, Acc) -> {ok, [ Id | Acc ]} end,
  Q1 = barrel:fold_path(
    <<"test">>,
    <<"/o/test">>,
    Fun,
    [],
    #{ equal_to => 1, order_by => order_by_key}
  ),
  5 = length(Q1),
  [<<"h">>, <<"g">>,  <<"e">>,  <<"b">>, <<"a">>] = Q1,
  Q2 = barrel:fold_path(
    <<"test">>,
    <<"/o/test">>,
    Fun,
    [],
    #{ equal_to => 2, order_by => order_by_key}
  ),
  2 = length(Q2),
  [<<"d">>, <<"c">>] = Q2,
  Q3 = barrel:fold_path(
    <<"test">>,
    <<"/o/test">>,
    Fun,
    [],
    #{ equal_to => 3, order_by => order_by_key}
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

  All = barrel:fold_path(<<"test">>, <<"docId">>, Fun, [], #{ limit_to_last => 15, order_by => order_by_key }),
  Ids = All,
  Nth = lists:nth(10, Ids),
  ExpectBefore = lists:sublist(Ids, 11, 5),
  Before = barrel:fold_path(
    <<"test">>, <<"docId">>, Fun, [], #{ previous_to => Nth, limit_to_last => 10, order_by => order_by_key }
  ),
  Before = ExpectBefore,
  ExpectAfter = lists:reverse(lists:sublist(Ids, 1, 9)),
  After = barrel:fold_path(
    <<"test">>, <<"docId">>, Fun, [], #{ next_to => Nth, limit_to_first => 10, order_by => order_by_key }
  ),
  After = ExpectAfter.
