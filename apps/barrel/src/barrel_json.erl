-module(barrel_json).

-export([
  encode_index_keys/1
]).


encode_index_keys(J) ->
  B = barrel_encoding:encode_json_ascending(<<>>),
  maps:fold(
    fun
      (K, V, Acc) when is_map(V) ->
        encode_object(
          V, barrel_encoding:encode_json_key_ascending(B, K, false), Acc
        );
      (K, V, Acc) when is_list(V) ->
        encode_array(
          V, barrel_encoding:encode_json_key_ascending(B, K, false), Acc
        );
      (K, V, Acc) ->
        B2 = barrel_encoding:encode_json_key_ascending(B, K, true),
        [encode_json_term(V, B2) | Acc]
    end,
    [],
    J
  ).

encode_json_term(L, B0) when is_atom(L) ->
  B1 = barrel_encoding:add_json_path_terminator(B0),
  barrel_encoding:encode_literal_ascending(B1, L);
encode_json_term(S, B0) when is_binary(S) ->
  B1 = barrel_encoding:add_json_path_terminator(B0),
  barrel_encoding:encode_binary_ascending(B1, S);
encode_json_term(N, B0) when is_number(N) ->
  B1 = barrel_encoding:add_json_path_terminator(B0),
  barrel_encoding:encode_float_ascending(B1, N).

encode_object(Obj, B, Acc) when map_size(Obj) =:= 0->
  [barrel_encoding:encode_json_empty_object(B) | Acc];
encode_object(Obj, B, Acc) ->
  maps:fold(
    fun
      (K, V, Acc1) when is_map(V) ->
        encode_object(
          V, barrel_encoding:encode_json_key_ascending(B, K, false), Acc1
        );
      (K, V, Acc1) when is_list(V) ->
        encode_array(
          V, barrel_encoding:encode_json_key_ascending(B, K, false), Acc1
        );
      (K, V, Acc1) ->
        B2 = barrel_encoding:encode_json_key_ascending(B, K, true),
        [encode_json_term(V, B2) | Acc1]
    end,
    Acc,
    Obj
  ).

encode_array([], B, Acc) ->
  [barrel_encoding:encode_json_empty_array(B) | Acc];
encode_array(A, B, Acc) ->
  encode_array1(A, B, Acc).

encode_array1([Obj | Rest], B, Acc) when is_map(Obj) ->
  B1 = barrel_encoding:encode_array_ascending(B),
  Acc2 = encode_object(Obj, B1, Acc),
  encode_array1(Rest, B, Acc2);
encode_array1([L | Rest], B, Acc) when is_list(L) ->
  B1 = barrel_encoding:encode_array_ascending(B),
  Acc2 = encode_array(L, B1, Acc),
  encode_array1(Rest, B, Acc2);
encode_array1([T | Rest], B, Acc) ->
  B1 = barrel_encoding:encode_array_ascending(B),
  encode_array1(Rest, B, [encode_json_term(T, B1) | Acc]);
encode_array1([], _B, Acc) ->
  Acc.


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
    <<"e">> => [#{<<"a">> => 1}, #{ <<"b">> => 2}]
  }).

basic_test() ->
  Keys = encode_index_keys(?doc),
  ?assertEqual(13, length(Keys)).



-endif.
