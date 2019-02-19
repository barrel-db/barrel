%% Copyright 2018, Benoit Chesneau
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


%% TODO:: indexing policy
-module(barrel_ars).


-export([
  analyze/1,
  pp_segment/1,
  pp_segments/1,
  decode_segment/1
]).

-include("util/barrel_encoding.hrl").

-define(segment_size, 3).

-type atom_term() :: binary() | number() | true | false | null.
-type record() :: map().
-type sequence() :: [atom_term() | record()].
-type segment() :: binary().


-spec analyze(record()) -> [segment()].
analyze(Record) ->
  lists:usort(analyze1(Record)).

analyze1(Record) ->
  maps:fold(
    fun
      (K, V, Acc) when is_map(V) ->
        encode_record(V, [K, '$'], Acc);
      (K, V, Acc) when is_list(V) ->
        encode_sequence(V,  [K, '$'], Acc);
      (K, V, Acc) ->
        [encode_segment(['$', K, V]) | Acc]
    end,
    [],
    Record
  ).

encode_segment(Parts) ->
  encode_segment(Parts, <<>>).

encode_segment(['$' | Rest], B) ->
  encode_segment(Rest, barrel_encoding:encode_json_ascending(B));
encode_segment([K], B) ->
  encode_json_term(K, B);
encode_segment([K | Rest], B) when is_integer(K) ->
  encode_segment(Rest, barrel_encoding:encode_uvarint_ascending(B, K));

encode_segment([K | Rest], B) ->
  encode_segment(Rest, barrel_encoding:encode_binary_ascending(B, K)).


encode_json_term(L, B) when is_atom(L) ->
  barrel_encoding:encode_literal_ascending(B, L);
encode_json_term(S, B) when is_binary(S) ->
  barrel_encoding:encode_binary_ascending(B, short(S));
encode_json_term(N, B) when is_integer(N) ->
  barrel_encoding:encode_varint_ascending(B, N);
encode_json_term(N, B) when is_number(N) ->
  barrel_encoding:encode_float_ascending(B, N).


maybe_encode_segment(S, Acc) when length(S) =:= ?segment_size ->
  {lists:droplast(S), [encode_segment(lists:reverse(S)) | Acc]};
maybe_encode_segment(S, Acc) ->
  {S, Acc}.

-spec encode_record(record(), list(), list()) -> list().
encode_record(R, S, Acc) ->
  maps:fold(
    fun
      (K, V, Acc1) when is_map(V) ->
        {S2, Acc2} = maybe_encode_segment([K | S], Acc1),
        encode_record(V, S2, Acc2);
      (K, V, Acc1) when is_list(V) ->
        {S2, Acc2} = maybe_encode_segment([K | S], Acc1),
        encode_sequence(V, S2, Acc2);
      (K, V, Acc1) ->
        {S2, Acc2} = maybe_encode_segment([K | S], Acc1),
        [encode_segment(lists:reverse([V | S2])) | Acc2]
    end,
    Acc,
    R
  ).

-spec encode_sequence(sequence(), list(), list()) -> list().
encode_sequence(Seq, Seg, Acc) ->
  encode_sequence(Seq, Seg, 0, Acc).

encode_sequence([Record | Rest], Seg, Idx, Acc) when is_map(Record) ->
  {S1, Acc1} = maybe_encode_segment([Idx | Seg], Acc),
  Acc2 = encode_record(Record, S1, Acc1),
  encode_sequence(Rest, Seg, Idx + 1, Acc2);
encode_sequence([Seq | Rest], Seg, Idx, Acc) when is_list(Seq) ->
  {S1, Acc1} = maybe_encode_segment([Idx | Seg], Acc),
  Acc2 = encode_sequence(Seq, S1, Acc1),
  encode_sequence(Rest, Seg, Idx + 1, Acc2);
encode_sequence([T | Rest], Seg, Idx, Acc) ->
  {S1, Acc1} = maybe_encode_segment([Idx | Seg], Acc),
  encode_sequence(Rest, Seg, Idx +1, [encode_segment(lists:reverse([T | S1])) | Acc1]);
encode_sequence([], _, _, Acc) ->
  Acc.


pp_segment(Bin) ->
  pp_segment(Bin, <<>>, ?segment_size).

pp_segment(<< ?JSON_INVERTED_INDEX, Rest/binary >>, Acc, N) ->
  pp_segment(Rest, << Acc/binary, "$/" >>, N - 1);
pp_segment(Bin, Acc, N) when N > 1 ->
  case barrel_encoding:pick_encoding(Bin) of
    bytes ->
      {Key, Rest} = barrel_encoding:decode_binary_ascending(Bin),
      pp_segment(Rest, << Acc/binary, Key/binary, "/" >>, N - 1);
    int ->
      {Idx, Rest} = barrel_encoding:decode_varint_ascending(Bin),
      pp_segment(Rest, << Acc/binary, (integer_to_binary(Idx))/binary, "/" >>, N - 1);
    _ ->
      erlang:error(badarg)
  end;
pp_segment(Bin, Acc, 1) ->
  case barrel_encoding:pick_encoding(Bin) of
    bytes ->
      {Val, _} = barrel_encoding:decode_binary_ascending(Bin),
      << Acc/binary, Val/binary >>;
    int ->
      {Val, _} = barrel_encoding:decode_varint_ascending(Bin),
      << Acc/binary, (integer_to_binary(Val))/binary >>;
    float ->
      {Val, _} = barrel_encoding:decode_varint_ascending(Bin),
      << Acc/binary, (float_to_binary(Val))/binary >>;
    literal ->
      {Val, _} = barrel_encoding:decode_varint_ascending(Bin),
      << Acc/binary, (atom_to_binary(Val, utf8))/binary >>;
    _ ->
      erlang:error(badarg)
  end.


pp_segments(Segments) ->
  iolist_to_binary([<< (pp_segment(Segment))/binary, "\n" >> || Segment <- lists:usort(Segments)]).

decode_segment(Bin) ->
  decode_segment(Bin, [], ?segment_size).

decode_segment(<< ?JSON_INVERTED_INDEX, Rest/binary >>, Acc, N) ->
  decode_segment(Rest, ['$' | Acc], N - 1);
decode_segment(Bin, Acc, N) when N > 1 ->
  case barrel_encoding:pick_encoding(Bin) of
    bytes ->
      {Key, Rest} = barrel_encoding:decode_binary_ascending(Bin),
      decode_segment(Rest, [Key | Acc], N - 1);
    int ->
      {Idx, Rest} = barrel_encoding:decode_varint_ascending(Bin),
      decode_segment(Rest, [Idx | Acc], N - 1);
    _ ->
      erlang:error(badarg)
  end;
decode_segment(Bin, Acc, 1) ->
  case barrel_encoding:pick_encoding(Bin) of
    bytes ->
      {Val, Rest} = barrel_encoding:decode_binary_ascending(Bin),
      {lists:reverse([Val | Acc]), Rest};
    int ->
      {Val, Rest} = barrel_encoding:decode_varint_ascending(Bin),
      {lists:reverse([Val | Acc]), Rest};
    float ->
      {Val, Rest} = barrel_encoding:decode_varint_ascending(Bin),
      {lists:reverse([Val | Acc]), Rest};
    literal ->
      {Val, Rest} = barrel_encoding:decode_varint_ascending(Bin),
      {lists:reverse([Val | Acc]), Rest};
    _ ->
      erlang:error(badarg)
  end.

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
    <<"e">> => [#{<<"a">> => 1}, #{ <<"b">> => 2, <<"c">> => 3}]
  }).

analyze_test() ->
  Segments = analyze(?doc),
  Decoded =
    lists:foldl(
      fun(S, Acc) -> {D, _} = decode_segment(S), [D | Acc] end,
      [],
      Segments
    ),
  ?assertEqual(
    [
      [0, <<"a">>, 1],
      [1, <<"b">>, 2],
      [1, <<"c">>, 3],
      ['$', <<"a">>, 1],
      ['$', <<"b">>, <<"2">>],
      ['$', <<"c">>, <<"a">>],
      ['$', <<"c">>, <<"b">>],
      ['$', <<"c">>, <<"c">>],
      ['$', <<"d">>, 0],
      ['$', <<"d">>, 1],
      ['$', <<"d">>, 2],
      ['$', <<"e">>, 0],
      ['$', <<"e">>, 1],
      [<<"b">>, 0, <<"a">>],
      [<<"b">>, 1, <<"b">>],
      [<<"b">>, 2, <<"c">>],
      [<<"c">>, <<"a">>, 1],
      [<<"c">>, <<"b">>, 0],
      [<<"c">>, <<"b">>, 1],
      [<<"c">>, <<"b">>, 2],
      [<<"c">>, <<"c">>, <<"a">>],
      [<<"c">>, <<"c">>, <<"b">>],
      [<<"d">>, 0, <<"a">>],
      [<<"d">>, 1, <<"b">>],
      [<<"d">>, 2, <<"c">>],
      [<<"e">>, 0, <<"a">>],
      [<<"e">>, 1, <<"b">>],
      [<<"e">>, 1, <<"c">>]
    ],
    lists:sort(Decoded)
  ).


-endif.
