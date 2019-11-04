-module(barrel_ars_view).

-export([version/0,
         init/1,
         handle_doc/2]).


%% for debugging purpose
-export([analyze/1]).

-include_lib("barrel/include/barrel.hrl").

version() -> 1.

init(Config) -> {ok, Config}.

%% initial version ignore the config
handle_doc(Doc, _Config) ->
  analyze(Doc).


analyze(Record) ->
  maps:fold(
    fun(K, V, Acc) when is_map(V) ->
        analyze_record(V, [ K], Acc);
       (K, V, Acc) when is_list(V) ->
        analyze_sequence(V, [ K], Acc);
       (K, V, Acc) ->
        [{[ K, short(V)], <<>>} | Acc]
    end,
    [],
    Record
   ).

analyze_record(R, S, Acc) ->
  maps:fold(
    fun(K, V, Acc1) when is_map(V) ->
        analyze_record(V, S ++ [K], Acc1);
       (K, V, Acc1) when is_list(V) ->
        analyze_sequence(V, S ++ [K], Acc1);
       (K, V, Acc1) ->
        [{S ++ [K, short(V)], <<>>} | Acc1]
    end,
    Acc,
    R
   ).

analyze_sequence(R, S, Acc) ->
  analyze_sequence_1(R, S, 0, Acc).


analyze_sequence_1([R | Rest], S, I, Acc) when is_map(R) ->
  Acc2 = analyze_record(R, S ++ [I], Acc),
  analyze_sequence_1(Rest, S, I+1, Acc2);
analyze_sequence_1([R | Rest], S, I, Acc) when is_list(R) ->
  Acc2 = analyze_sequence_1(R, S ++ [I] , 0, Acc),
  analyze_sequence_1(Rest, S, I+1, Acc2);
analyze_sequence_1([R | Rest], S, I, Acc) ->
  analyze_sequence_1(Rest, S, I+1, [{S ++ [I, short(R)], <<>>} | Acc]);
analyze_sequence_1([], _, _, Acc) ->
  Acc.


%% todo make it configurable
short(<< S:100/binary, _/binary >>) -> S;
short(S) when is_binary(S) -> S;
short(S) -> S.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(doc,
  #{
    <<"a">> => 1,
    <<"b">> => <<"2">>,
    <<"c">> => #{
      <<"a">> => 1,
      <<"b">> => [<<"a">>, <<"b">>, <<"c">>],
      <<"c">> => #{ <<"a">> => 1, <<"b">> => 2}
    },
    <<"d">> => [<<"a">>, <<"b">>, <<"c">>],
    <<"e">> => [#{<<"a">> => 1}, #{ <<"b">> => 2, <<"c">> => 3}]
  }).

analyze_test() ->
  ?assertEqual(
     [{[<<"e">>,1,<<"c">>,3],<<>>},
      {[<<"e">>,1,<<"b">>,2],<<>>},
      {[<<"e">>,0,<<"a">>,1],<<>>},
      {[<<"d">>,2,<<"c">>],<<>>},
      {[<<"d">>,1,<<"b">>],<<>>},
      {[<<"d">>,0,<<"a">>],<<>>},
      {[<<"c">>,<<"c">>,<<"b">>,2],<<>>},
      {[<<"c">>,<<"c">>,<<"a">>,1],<<>>},
      {[<<"c">>,<<"b">>,2,<<"c">>],<<>>},
      {[<<"c">>,<<"b">>,1,<<"b">>],<<>>},
      {[<<"c">>,<<"b">>,0,<<"a">>],<<>>},
      {[<<"c">>,<<"a">>,1],<<>>},
      {[<<"b">>,<<"2">>],<<>>},
      {[<<"a">>,1],<<>>}], analyze(?doc)).

-endif.
