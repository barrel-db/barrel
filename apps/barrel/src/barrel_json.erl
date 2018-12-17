-module(barrel_json).

-export([
  encode_index_keys/1,
  pretty_print/1
]).

-define(nested_key(B, K),  barrel_encoding:encode_json_nested(B, K)).
-define(array_index(B, I),  barrel_encoding:encode_json_array_index(B, I)).
-define(key(B, K,End), barrel_encoding:encode_json_key_ascending(B, K, End)).
-define(add_terminator(B), barrel_encoding:add_json_path_terminator(B)).
-define(array_key(B, I), barrel_encoding:encode_array_ascending(B, I)).




%% @doc encode keys for the index
%% TODO: `make value precision configurable
-spec encode_index_keys(map()) -> [binary()].
encode_index_keys(J) ->
  B = barrel_encoding:encode_json_ascending(<<>>),
  maps:fold(
    fun
      (K, V, Acc) when is_map(V) ->
        encode_object(
          V, ?key(B, K, false), [?nested_key(B, K) | Acc]
        );
      (K, V, Acc) when is_list(V) ->
        encode_array(
          V, ?key(B, K, false), [?nested_key(B, K) | Acc]
        );
      (K, V, Acc) ->
        [encode_json_term(V, ?key(B, K, true)) |  [?nested_key(B, K) | Acc]]
    end,
    [],
    J
  ).

encode_json_term(L, B) when is_atom(L) ->
  barrel_encoding:encode_literal_ascending(?add_terminator(B), L);
encode_json_term(S, B) when is_binary(S) ->
  barrel_encoding:encode_binary_ascending(?add_terminator(B), short(S));
encode_json_term(N, B) when is_integer(N) ->
  barrel_encoding:encode_varint_ascending(?add_terminator(B), N);
encode_json_term(N, B) when is_number(N) ->
  barrel_encoding:encode_float_ascending(?add_terminator(B), N).

encode_object(Obj, B, Acc) when map_size(Obj) =:= 0->
  [barrel_encoding:encode_json_empty_object(B) | Acc];
encode_object(Obj, B, Acc) ->
  maps:fold(
    fun
      (K, V, Acc1) when is_map(V) ->
        encode_object(
          V, ?key(B, K, false), [?nested_key(B, K)  | Acc1]
        );
      (K, V, Acc1) when is_list(V) ->
        encode_array(
          V, ?key(B, K, false), [?nested_key(B, K) | Acc1]
        );
      (K, V, Acc1) ->
        [encode_json_term(V, ?key(B, K, true)) | [?nested_key(B, K)  | Acc1]]
    end,
    Acc,
    Obj
  ).

encode_array([], B, Acc) ->
  [barrel_encoding:encode_json_empty_array(B) | Acc];
encode_array(A, B, Acc) ->
  encode_array1(A, B, 0, Acc).

encode_array1([Obj | Rest], B, Idx, Acc) when is_map(Obj) ->
  B1 = ?array_key(B, Idx),
  Acc2 = encode_object(Obj, B1, [?array_index(B, Idx) | Acc]),
  encode_array1(Rest, B, Idx + 1, Acc2);
encode_array1([L | Rest], B, Idx, Acc) when is_list(L) ->
  B1 = ?array_key(B, Idx),
  Acc2 = encode_array(L, B1, [?array_index(B, Idx) | Acc]),
  encode_array1(Rest, B, Idx + 1, Acc2);
encode_array1([T | Rest], B, Idx, Acc) ->
  B1 = ?array_key(B, Idx),
  encode_array1(Rest, B, Idx + 1, [encode_json_term(T, B1), ?array_index(B, Idx) | Acc]);
encode_array1([], _B, _, Acc) ->
  Acc.


pretty_print(BinKeys) when is_list(BinKeys) ->
  lists:foldl(
    fun(BinKey, B) ->
      case barrel_encoding:pp_ikey(BinKey) of
        [Key] when is_binary(Key) ->
          << B/binary, "/OBJECT_KEY/", Key/binary, "\n" >>;
        KeyParts ->
          Last = case lists:last(KeyParts) of
                   L when is_atom(L) -> [<<"/">>, <<"BOOL">>, <<"/">>, barrel_lib:to_binary(L)];
                   N when is_integer(N) -> [<<"/">>,<<"NUMBER">>, <<"/">>, integer_to_binary(N)];
                   N when is_number(N) -> [<<"/">>, <<"NUMBER">>, <<"/">>, float_to_binary(N)];
                   Bin when is_binary(Bin) -> [<<"/">>, <<"STRING">>, <<"/">>, Bin]
                 end,
          Parts = lists:map(fun
                              (arr) -> [<<"/">>, <<"ARR">>];
                              (K) when is_integer(K) -> [<<"/">>, integer_to_binary(K)];
                              (K) -> [<<"/">>, <<"OBJECT_KEY">>, <<"/">>, K]
                            end, lists:droplast(KeyParts)),
          << B/binary, (iolist_to_binary(Parts ++ [Last]))/binary, "\n" >>
      end
    end,
    <<>>,
    BinKeys).

%% internal

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
    <<"e">> => [#{<<"a">> => 1}, #{ <<"b">> => 2}]
  }).

basic_test() ->
  Keys = encode_index_keys(?doc),
  ?assertEqual(33, length(Keys)),
  DecodedKeys = [barrel_encoding:pp_ikey(Key) || Key <- Keys ],
  ?assertEqual(
    [[<<"a">>],
      [<<"a">>,1],
      [<<"b">>],
      [<<"b">>,<<"2">>],
      [<<"c">>],
      [<<"c">>, <<"a">>],
      [<<"c">>,<<"a">>,1],
      [<<"c">>,<<"b">>],
      [<<"c">>, <<"b">>, 0],
      [<<"c">>,<<"b">>,0,<<"a">>],
      [<<"c">>, <<"b">>, 1],
      [<<"c">>,<<"b">>,1,<<"b">>],
      [<<"c">>, <<"b">>, 2],
      [<<"c">>,<<"b">>,2,<<"c">>],
      [<<"c">>,<<"c">>],
      [<<"c">>,<<"c">>,<<"a">>],
      [<<"c">>,<<"c">>,<<"a">>,1],
      [<<"c">>,<<"c">>,<<"b">>],
      [<<"c">>,<<"c">>,<<"b">>,2],
      [<<"d">>],
      [<<"d">>,0],
      [<<"d">>,0,<<"a">>],
      [<<"d">>,1],
      [<<"d">>,1,<<"b">>],
      [<<"d">>,2],
      [<<"d">>,2,<<"c">>],
      [<<"e">>],
      [<<"e">>,0],
      [<<"e">>,0,<<"a">>],
      [<<"e">>,0,<<"a">>,1],
      [<<"e">>,1],
      [<<"e">>,1, <<"b">>],
      [<<"e">>,1,<<"b">>,2]], lists:sort(DecodedKeys)).



-endif.
