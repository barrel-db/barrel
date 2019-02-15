-module(barrel_encoding).

-export([encode_uint32_ascending/2, encode_uint32_descending/2,
         decode_uint32_ascending/1, decode_uint32_descending/1,
         encode_uint64_ascending/2, encode_uint64_descending/2,
         decode_uint64_ascending/1, decode_uint64_descending/1,
         encode_varint_ascending/2, encode_varint_descending/2,
         decode_varint_ascending/1, decode_varint_descending/1,
         encode_uvarint_ascending/2, encode_uvarint_descending/2,
         decode_uvarint_ascending/1, decode_uvarint_descending/1,
         encode_binary_ascending/2, encode_binary_descending/2,
         decode_binary_ascending/1, decode_binary_descending/1,
         encode_literal_ascending/2, decode_literal_ascending/1,
         encode_literal_descending/2, decode_literal_descending/1,
         encode_nonsorting_uvarint/2,
         decode_nonsorting_uvarint/1,
         encode_float_ascending/2, encode_float_descending/2,
         decode_float_ascending/1, decode_float_descending/1]).

%% JSON encoding
-export([
  encode_json_empty_array/1,
  encode_json_empty_object/1,
  encode_json_key_ascending/3,
  encode_array_ascending/2,
  encode_array_index_ascending/3,
  encode_array_ascending/1,
  add_json_path_terminator/1,
  encode_json_ascending/1
]).

-export([pick_encoding/1]).

-include("barrel_logger.hrl").

-include("barrel_encoding.hrl").

pick_encoding(<< ?BYTES_MARKER, _/binary >>) -> bytes;
pick_encoding(<< ?BYTES_MARKER_DESC, _/binary >>) -> bytes_desc;
pick_encoding(<< ?LITERAL_MARKER, _/binary >>) -> literal;
pick_encoding(<< ?LITERAL_MARKER_DESC, _/binary >>) -> literal_desc;
pick_encoding(<< M, _/binary >>) when M >= ?INT_MIN, M =< ?INT_MAX -> int;
pick_encoding(<< M, _/binary >>) when M >= ?FLOAT_NAN, M =< ?FLOAT_NAN_DESC -> float;
pick_encoding(_) -> erlang:error(badarg).

%% @doc  encodes the uint32 value using a big-endian 8 byte representation.
%% The bytes are appended to the supplied buffer and the final buffer is returned.
encode_uint32_ascending(B, V) when is_binary(B), is_integer(V), V >= 0 ->
  << B/binary, V:32/big-integer >>;
encode_uint32_ascending(B, V) when is_binary(B), is_integer(V) ->
  << N:32/native-unsigned >> = << V:32/signed-native >>,
  << B/binary, N:32/big-integer >>;
encode_uint32_ascending(_, _) ->
  erlang:error(badarg).

%% @doc encodes the uint32 value so that it sorts in reverse order, from largest to smallest.
encode_uint32_descending(B, V) when is_integer(V) ->
  encode_uint32_ascending(B, bnot V);
encode_uint32_descending(_, _) ->
  erlang:error(badarg).

%% @doc decodes a uint32 from the input buffer, treating
%% the input as a big-endian 4 byte uint32 representation. The remainder
%% of the input buffer and the decoded uint32 are returned.
decode_uint32_ascending(<< V:32/big-integer, B/binary >>) -> {to_uint32(V), B};
decode_uint32_ascending(_B) -> erlang:error(badarg).

%% @doc decodes a uint32 value which was encoded using `encode_uint32_descending/2'.
decode_uint32_descending(B) ->
  {V, LeftOver} = decode_uint32_ascending(B),
  {to_uint32(bnot V), LeftOver}.


to_uint32(N) ->
  Mask = (1 bsl 32) - 1,
  N band Mask.

%% @doc encodes the uint64 value using a big-endian 8 byte representation.
%% The bytes are appended to the supplied buffer and  the final buffer is returned.
encode_uint64_ascending(B, V) when V >= 0 ->
  << B/binary, V:64/big-integer >>;
encode_uint64_ascending(B, V)  ->
  << N:64/native-unsigned >> = << V:64/signed-native >>,
  << B/binary, N:64/big-integer >>.

%% @doc encodes the uint64 value so that it sorts in  reverse order,
%% from largest to smallest.
encode_uint64_descending(B, V) when is_integer(V) ->
  encode_uint64_ascending(B, bnot V);
encode_uint64_descending(_, _) ->
  erlang:error(badarg).

%% @doc decodes a uint64 from the input buffer, treating
%% the input as a big-endian 8 byte uint64 representation. The remainder
%% of the input buffer and the decoded uint64 are returned.
decode_uint64_ascending(<< V:64/big-integer, B/binary >>) -> {to_uint64(V), B};
decode_uint64_ascending(_B) -> erlang:error(badarg).

%% @doc D decodes a uint64 value which was encoded using `encode_uint_64_descending/2'.
decode_uint64_descending(B) ->
  {V, LeftOver} = decode_uint64_ascending(B),
  {to_uint64(bnot V), LeftOver}.

to_uint64(N) ->
  Mask = (1 bsl 64) - 1,
  N band Mask.


%% @doc EncodeVarintAscending encodes the int64 value using a variable length
%% (length-prefixed) representation. The length is encoded as a single
%% byte. If the value to be encoded is negative the length is encoded
%% as 8-numBytes. If the value is positive it is encoded as
%% 8+num_bytes. The encoded bytes are appended to the supplied buffer
%% and the final buffer is returned.
encode_varint_ascending(B, V) when V < 0 ->
  encode_varint_ascending_1(B, V);
encode_varint_ascending(B, V) ->
  encode_uvarint_ascending(B, to_uint64(V)).


encode_varint_ascending_1(B, V) when V >= -16#ff ->
  << B/binary, (?INT_MIN + 7), V >>;
encode_varint_ascending_1(B, V) when V >= -16#ffff ->
  << B/binary, (?INT_MIN + 6), (V bsr 8), V >>;
encode_varint_ascending_1(B, V) when V >= -16#ffffff ->
  << B/binary, (?INT_MIN + 5), (V bsr 16), (V bsr 8), V >>;
encode_varint_ascending_1(B, V) when V >= -16#ffffffff ->
  << B/binary, (?INT_MIN + 4), (V bsr 24), (V bsr 16), (V bsr 8), V >>;
encode_varint_ascending_1(B, V) when V >= -16#ffffffffff ->
  << B/binary, (?INT_MIN + 3), (V bsr 32), (V bsr 24),  (V bsr 16), (V bsr 8), V >>;
encode_varint_ascending_1(B, V) when V >= -16#ffffffffffff ->
  << B/binary, (?INT_MIN + 2), (V bsr 40), (V bsr 32), (V bsr 24), (V bsr 16), (V bsr 8), V >>;
encode_varint_ascending_1(B, V) when V >= -16#ffffffffffffff ->
  << B/binary, (?INT_MIN + 1), (V bsr 48), (V bsr 40), (V bsr 32), (V bsr 24),  (V bsr 16), (V bsr 8), V >>;
encode_varint_ascending_1(B, V) ->
  << B/binary, ?INT_MIN, (V bsr 56), (V bsr 48), (V bsr 40), (V bsr 32), (V bsr 24),  (V bsr 16), (V bsr 8), V >>.



%% @doc EncodeVarintDescending encodes the int64 value so that it sorts in reverse
%% order, from largest to smallest.
encode_varint_descending(B, V)  ->
  encode_varint_ascending(B, bnot V).


%% @doc decodes a value encoded by `encode_varint_ascending/2'.
decode_varint_ascending(<<>>) -> erlang:error(badarg);
decode_varint_ascending(<< L, _/binary >> = B) ->
  Length = L - ?INT_ZERO,
  decode_varint_ascending_1(B, Length, -Length).


decode_varint_ascending_1(<< _L, B/binary >>, Len, Len2) when Len < 0, byte_size(B) < Len2 ->
  erlang:error(badarg);
decode_varint_ascending_1(<< _L, B0/binary >>, Len, Len2) when Len < 0 ->
  << B1:Len2/binary, LeftOver/binary >> = B0,
  V = fold_binary(B1,
                  fun(T, V1) ->
                      V2 = (V1 bsl 8)  bor (bnot T) band 16#ff,
                      V2
                  end,
                  0),
  {bnot V, LeftOver};
decode_varint_ascending_1(B, _Len, _Len2) ->
  {V, LeftOver} = decode_uvarint_ascending(B),
  {to_uint64(V), LeftOver}.

%% @doc decodes a value encoded by encode_varint_ascending
decode_varint_descending(B) ->
  {V, LeftOver} = decode_varint_ascending(B),
  {bnot V, LeftOver}.



%% @doc EncodeUvarintAscending encodes the uint64 value using a variable length
%% (length-prefixed) representation. The length is encoded as a single
%% byte indicating the number of encoded bytes (-8) to follow. See
%% `encode_varint_ascending/2' for rationale. The encoded bytes are appended to the
%% supplied buffer and the final buffer is returned.-
-spec encode_uvarint_ascending(B, V) -> B2 when
    B :: binary(),
    V :: integer(),
    B2 :: binary().
encode_uvarint_ascending(B, V) when V =< ?INT_SMALL ->
  << B/binary, (?INT_ZERO + V) >>;
encode_uvarint_ascending(B, V) when V =< 16#ff ->
  << B/binary, (?INT_MAX - 7), V >>;
encode_uvarint_ascending(B, V) when V =< 16#ffff ->
  << B/binary, (?INT_MAX - 6), (V bsr 8), V >>;
encode_uvarint_ascending(B, V) when V =< 16#ffffff ->
  << B/binary, (?INT_MAX - 5), (V bsr 16), (V bsr 8), V >>;
encode_uvarint_ascending(B, V) when V =< 16#ffffffff ->
  << B/binary, (?INT_MAX - 4), (V bsr 24), (V bsr 16), (V bsr 8), V >>;
encode_uvarint_ascending(B, V) when V =< 16#ffffffffff ->
  << B/binary, (?INT_MAX - 3), (V bsr 32), (V bsr 24),  (V bsr 16), (V bsr 8), V >>;
encode_uvarint_ascending(B, V) when V =< 16#ffffffffffff ->
  << B/binary, (?INT_MAX - 2), (V bsr 40), (V bsr 32), (V bsr 24), (V bsr 16), (V bsr 8), V >>;
encode_uvarint_ascending(B, V) when V =< 16#ffffffffffffff ->
  << B/binary, (?INT_MAX - 1), (V bsr 48), (V bsr 40), (V bsr 32), (V bsr 24),  (V bsr 16), (V bsr 8), V >>;
encode_uvarint_ascending(B, V) ->
  << B/binary, ?INT_MAX, (V bsr 56), (V bsr 48), (V bsr 40), (V bsr 32), (V bsr 24),  (V bsr 16), (V bsr 8), V >>.


encode_uvarint_descending(B, 0) ->
  << B/binary, (?INT_MIN + 8) >>;
encode_uvarint_descending(B, V) when V =< 16#ff ->
  V1 = to_uint64(bnot V),
  << B/binary, (?INT_MIN + 7), V1 >>;
encode_uvarint_descending(B, V) when V =< 16#ffff  ->
  V1 = to_uint64(bnot V),
  << B/binary, (?INT_MIN + 6), (V1 bsr 8),  V1 >>;
encode_uvarint_descending(B, V) when V =< 16#ffffff ->
  V1 = to_uint64(bnot V),
  << B/binary, (?INT_MIN + 5), (V1 bsr 16), (V1 bsr 8), V1 >>;
encode_uvarint_descending(B, V) when V =< 16#ffffff ->
  V1 = to_uint64(bnot V),
  << B/binary, (?INT_MIN + 4), (V1 bsr 24), (V1 bsr 16), (V1 bsr 8), V1 >>;
encode_uvarint_descending(B, V) when V =< 16#ffffffff ->
  V1 = to_uint64(bnot V),
  << B/binary, (?INT_MIN + 3), (V1 bsr 32), (V1 bsr 24), (V1 bsr 16), (V1 bsr 8), V1 >>;
encode_uvarint_descending(B, V) when V =< 16#ffffffffff ->
  V1 = to_uint64(bnot V),
  << B/binary, (?INT_MIN + 2), (V1 bsr 40), (V1 bsr 32), (V1 bsr 24), (V1 bsr 16), (V1 bsr 8), V1 >>;
encode_uvarint_descending(B, V) when V =< 16#ffffffffffff ->
  V1 = to_uint64(bnot V),
  << B/binary, (?INT_MIN + 1), (V1 bsr 48), (V1 bsr 40), (V1 bsr 32), (V1 bsr 24), (V1 bsr 16), (V1 bsr 8), V1 >>;
encode_uvarint_descending(B, V) ->
  V1 = bnot V,
  << B/binary, ?INT_MIN, (V1 bsr 56), (V1 bsr 48), (V1 bsr 40), (V1 bsr 32), (V1 bsr 24),
    (V1 bsr 16), (V1 bsr 8), V1 >>.


decode_uvarint_ascending(<<>>) -> erlang:error(badarg);
decode_uvarint_ascending(<< B_0, B/binary >>) ->
  Len = B_0 - ?INT_ZERO,
  decode_uvarint_ascending_1(B, Len, Len - ?INT_SMALL).

decode_uvarint_ascending_1(B, Len, _Len2) when Len =< ?INT_SMALL ->
  {to_uint64(Len), B};
decode_uvarint_ascending_1(_B, _Len, Len2) when Len2 < 0; Len2 > 8 ->
  ?LOG_ERROR("invalid uvarint length of %p", [Len2]),
  erlang:error(badarg);
decode_uvarint_ascending_1(B, _Len, Len2) when byte_size(B) < Len2 ->
  ?LOG_ERROR("insufficient bytes to decode uvarint value ~p", [B]),
  erlang:error(badarg);
decode_uvarint_ascending_1(B0, _Len, Len2) ->
  << B1:Len2/binary, LeftOver/binary >> = B0,
  V = fold_binary(B1,
                  fun(T, V1) ->
                      V2 = (V1 bsl 8) bor to_uint64(T),
                      V2
                  end,
                  0),
  {V, LeftOver}.


decode_uvarint_descending(<<>>) ->
  ?LOG_ERROR("insufficient bytes to decode uvarint value", []),
  erlang:error(badarg);
decode_uvarint_descending(<< B_0, B/binary >>) ->
  Len = ?INT_ZERO - B_0,
  decode_uvarint_descending_1(B, Len).

decode_uvarint_descending_1(_B, Len) when Len < 0; Len > 8 ->
  ?LOG_ERROR("invalid uvarint length of %p", [Len]),
  erlang:error(badarg);
decode_uvarint_descending_1(B, Len) when byte_size(B) < Len ->
  ?LOG_ERROR("insufficient bytes to decode uvarint value ~p", [B]),
  erlang:error(badarg);
decode_uvarint_descending_1(B0, Len) ->
  << B1:Len/binary, LeftOver/binary >> = B0,
  V = fold_binary(B1,
                  fun(T, V1) ->
                      V2 = (V1 bsl 8) bor (to_uint64(bnot T band 16#ff)),
                      V2
                  end,
                 0),
  {V, LeftOver}.

fold_binary(<< C, Rest/binary >>, Fun, Acc) -> fold_binary(Rest, Fun, Fun(C, Acc));
fold_binary(<<>>, _Fun, Acc) -> Acc.


encode_binary_ascending(B, Bin) ->
  Bin2 = binary:replace(Bin, << ?ESCAPE >>, << ?ESCAPE, ?ESCAPED_00 >>, [global]),
  << B/binary, ?BYTES_MARKER, Bin2/binary, ?ESCAPE, ?ESCAPED_TERM >>.

encode_binary_descending(B, Bin) ->
  Bin2 = inverse(
           << (binary:replace(Bin, << ?ESCAPE >>, << ?ESCAPE, ?ESCAPED_00 >>, [global]))/binary,
               ?ESCAPE, ?ESCAPED_TERM >>
          ),
  << B/binary, ?BYTES_MARKER_DESC, Bin2/binary >>.

decode_binary_ascending(<< ?BYTES_MARKER, B/binary >>) ->
  case binary:split(B, << ?ESCAPE, ?ESCAPED_TERM >>) of
    [Bin, LeftOver] -> {binary:replace(Bin, << ?ESCAPE, ?ESCAPED_00 >>, << ?ESCAPE >>, [global]), LeftOver};
    _ -> erlang:error(badarg)
  end;
decode_binary_ascending(_) ->
  erlang:error(badarg).

decode_binary_descending(<< ?BYTES_MARKER_DESC, B/binary >>) ->
  case binary:split(B, inverse(<< ?ESCAPE, ?ESCAPED_TERM >>)) of
    [Bin, LeftOver] ->
      Bin2 = binary:replace(
               inverse(Bin),
               << ?ESCAPE, ?ESCAPED_00 >>,
               << ?ESCAPE >>,
               [global]
              ),
      {Bin2, LeftOver};
    _ ->
      erlang:error(badarg)
  end;
decode_binary_descending(_) ->
  erlang:error(badarg).


inverse(B1) ->
  S = bit_size(B1),
  <<V1:S>> = B1,
  V2 = bnot V1,
  <<V2:S>>.

encode_literal_ascending(B, L) ->
  ok = is_literal(L),
  << B/binary, ?LITERAL_MARKER, (atom_to_binary(L, latin1))/binary, ?ESCAPE, ?ESCAPED_TERM >>.

encode_literal_descending(B, L) ->
  ok = is_literal(L),
  Bin2 = inverse(<< (atom_to_binary(L, latin1))/binary, ?ESCAPE, ?ESCAPED_TERM >>),
  << B/binary, ?LITERAL_MARKER_DESC, Bin2/binary >>.

is_literal(true) -> ok;
is_literal(false) -> ok;
is_literal(null) -> ok;
is_literal(_) -> erlang:error(badarg).

decode_literal_ascending(<< ?LITERAL_MARKER, B/binary >>) ->
  case binary:split(B, << ?ESCAPE, ?ESCAPED_TERM >>) of
    [Bin, LeftOver] -> {binary_to_atom(Bin, latin1), LeftOver};
    _ -> erlang:error(badarg)
  end;
decode_literal_ascending(_) ->
  erlang:error(badarg).

decode_literal_descending(<< ?LITERAL_MARKER_DESC, B/binary >>) ->
  case binary:split(B, inverse(<< ?ESCAPE, ?ESCAPED_TERM >>)) of
    [Bin, LeftOver] ->
      {binary_to_atom(inverse(Bin), latin1), LeftOver};
    _ ->
      erlang:error(badarg)
  end.



%% @doc encodes a uint64, appends it to the supplied buffer,
%% and returns the final buffer. The encoding used is similar to
%% encoding/binary, but with the most significant bits first
%% - Unsigned integers are serialized 7 bits at a time, starting with the
%% most significant bits.
%% - The most significant bit (msb) in each output byte indicates if there
%% is a continuation byte (msb = 1).
encode_nonsorting_uvarint(B, X) when X < (1 bsl 7) ->
  << B/binary, X >>;
encode_nonsorting_uvarint(B, X) when X < (1 bsl 14) ->
  << B/binary, (16#80 bor (X bsr 7)), (16#7f band X) >>;
encode_nonsorting_uvarint(B, X) when X < (1 bsl 21) ->
  << B/binary, (16#80 bor (X bsr 14)), (16#80 bor (X bsr 7)), (16#7f band X) >>;
encode_nonsorting_uvarint(B, X) when X < (1 bsl 28) ->
  << B/binary, (16#80 bor (X bsr 21)), (16#80 bor (X bsr 14)), (16#80 bor (X bsr 7)), (16#7f band X) >>;
encode_nonsorting_uvarint(B, X) when X < (1 bsl 35) ->
  << B/binary, (16#80 bor (X bsr 28)), (16#80 bor (X bsr 21)), (16#80 bor (X bsr 14)), (16#80 bor (X bsr 7)),
     (16#7f band X) >>;
encode_nonsorting_uvarint(B, X) when X < (1 bsl 42) ->
  << B/binary, (16#80 bor (X bsr 35)), (16#80 bor (X bsr 28)), (16#80 bor (X bsr 21)), (16#80 bor (X bsr 14)),
     (16#80 bor (X bsr 7)), (16#7f band X) >>;
encode_nonsorting_uvarint(B, X) when X < (1 bsl 49) ->
  << B/binary, (16#80 bor (X bsr 42)), (16#80 bor (X bsr 35)), (16#80 bor (X bsr 28)), (16#80 bor (X bsr 21)),
     (16#80 bor (X bsr 14)), (16#80 bor (X bsr 7)), (16#7f band X) >>;
encode_nonsorting_uvarint(B, X) when X < (1 bsl 56) ->
  << B/binary, (16#80 bor (X bsr 49)), (16#80 bor (X bsr 42)), (16#80 bor (X bsr 35)), (16#80 bor (X bsr 28)),
     (16#80 bor (X bsr 21)), (16#80 bor (X bsr 14)), (16#80 bor (X bsr 7)), (16#7f band X) >>;
encode_nonsorting_uvarint(B, X) when X < (1 bsl 63) ->
  << B/binary, (16#80 bor (X bsr 56)), (16#80 bor (X bsr 49)), (16#80 bor (X bsr 42)), (16#80 bor (X bsr 35)),
     (16#80 bor (X bsr 28)), (16#80 bor (X bsr 21)), (16#80 bor (X bsr 14)), (16#80 bor (X bsr 7)), (16#7f band X) >>;
encode_nonsorting_uvarint(B, X) ->
  << B/binary, (16#80 bor (X bsr 63)), (16#80 bor (X bsr 56)), (16#80 bor (X bsr 49)), (16#80 bor (X bsr 42)),
     (16#80 bor (X bsr 35)), (16#80 bor (X bsr 28)), (16#80 bor (X bsr 21)), (16#80 bor (X bsr 14)),
     (16#80 bor (X bsr 7)), (16#7f band X) >>.


%% @doc decodes a value encoded by `encode_nonsorting_uvarint/2'. It
%% returns the length of the encoded varint and value.
decode_nonsorting_uvarint(B) ->
  decode_nonsorting_uvarint(B, 0).

decode_nonsorting_uvarint(<< C, Rest/binary >>, V0) when C < 16#80 ->
  V1 = V0 bsl 7 + to_uint64(C band 16#7f),
  {V1, Rest};
decode_nonsorting_uvarint(<< C, Rest/binary >>, V0) ->
  V1 = V0 bsl 7 + to_uint64(C band 16#7f),
  decode_nonsorting_uvarint(Rest, V1);
decode_nonsorting_uvarint(<<>>, _) ->
  {0, <<>>}.



encode_float_ascending(B, nan) ->
  << B/binary, ?FLOAT_NAN >>;
encode_float_ascending(B, F) ->
  << Sign:1, _:11, _:52 >> = BinF = << F/float >>,
  U = binary:decode_unsigned(BinF),
  encode_float_ascending(Sign, U, B).

encode_float_ascending(_, 0, B) ->
  << B/binary, ?FLOAT_ZERO >>;
encode_float_ascending(0, U, B) ->
  encode_uint64_ascending(<< B/binary, ?FLOAT_POS >>, U);
encode_float_ascending(1, U, B) ->
  encode_uint64_ascending(<< B/binary, ?FLOAT_NEG >>, bnot U ).

encode_float_descending(B, nan) ->
  << B/binary, ?FLOAT_NAN_DESC >>;
encode_float_descending(B, F) ->
  encode_float_ascending(B, -F).

decode_float_ascending(<< ?FLOAT_NAN, B/binary >>) -> {nan, B};
decode_float_ascending(<< ?FLOAT_NAN_DESC, B/binary >>) -> {nan, B};
decode_float_ascending(<< ?FLOAT_ZERO, B/binary >>) -> {0, B};
decode_float_ascending(<< ?FLOAT_NEG, B/binary >>) ->
  {U, LeftOver} = decode_uint64_ascending(B),
  << F/float >> = binary:encode_unsigned(to_uint64(bnot U)),
  {F, LeftOver};
decode_float_ascending(<< ?FLOAT_POS, B/binary >>) ->
  {U, LeftOver} = decode_uint64_ascending(B),
  << F/float >> = binary:encode_unsigned(U),
  {F, LeftOver};
decode_float_ascending(_) ->
  erlang:error(badarg).

decode_float_descending(B) ->
  {F, LeftOver} = decode_float_ascending(B),
  {-F, LeftOver}.


%% @doc returns a binary with a byte to signify an empty JSON object.
encode_json_empty_object(B) ->
  << B/binary, ?ESCAPE, ?ESCAPED_TERM, ?JSON_EMPTY_OBJECT >>.

%% @doc returns a binary b with a byte to signify an empty JSON array.
encode_json_empty_array(B) ->
  << B/binary, ?ESCAPE, ?ESCAPED_TERM, ?JSON_EMPTY_ARRAY >>.

%%  adds a json path terminator to a binary
add_json_path_terminator(B) ->
  << B/binary, ?ESCAPE, ?ESCAPED_TERM >>.

%% @doc ncodes the JSON key string value with a JSON specific escaped
%% terminator. This allows us to encode keys in the same number of bytes as a string,
%% while at the same time giving us a sentinel to identify JSON keys. The end parameter is used
%% to determine if this is the last key in a a JSON path. If it is we don't add a separator after it.
encode_json_key_ascending(B, Key, true) ->
  encode_binary_ascending(B, Key); 
encode_json_key_ascending(B, Key, false) ->
  << (encode_binary_ascending(B, Key))/binary, ?ESCAPED_JSON_OBJECT_KEY_TERM >>.

%% @doc encodes a value used to signify membership of an array for JSON objects.
encode_array_ascending(B) ->
  << B/binary, ?ESCAPE, ?ESCAPED_JSON_ARRAY>>.

encode_array_ascending(B, I) ->
  <<  (encode_uvarint_ascending(B, I))/binary, ?ESCAPE, ?ESCAPED_JSON_ARRAY>>.

%% @doc encodes a value used to signify membership of an array for JSON objects.
encode_array_index_ascending(B, I, true) ->
  encode_uvarint_ascending(B, I);
encode_array_index_ascending(B, I, false) ->
  << (encode_uvarint_ascending(B, I))/binary, ?ESCAPED_JSON_ARRAY_KEY>>.


%% @doc encodes a JSON Type. The encoded bytes are appended to the
%% supplied binary and the final binary is returned.
encode_json_ascending(B) ->
  << B/binary, ?JSON_INVERTED_INDEX >>.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

encode_uint32_ascending_test() ->
  Tests = [{ << 0, 0, 0, 0 >>, 0 },
           { << 0, 0, 0, 1 >>, 1 },
           { << 0, 0, 1, 0 >>, 1 bsl 8 },
           { << 16#ff, 16#ff, 16#ff, 16#ff >>, 1 bsl 32 - 1 }], %% max uint32
  test_encode_decode(Tests, fun encode_uint32_ascending/2, fun decode_uint32_ascending /1).

encode_uint32_descending_test() ->
  Tests = [{ << 16#ff, 16#ff, 16#ff, 16#ff >>, 0 },
           { << 16#ff, 16#ff, 16#ff, 16#fe >>, 1 },
           { << 16#ff, 16#ff, 16#fe, 16#ff >>, 1 bsl 8 },
           { << 0, 0, 0, 0 >>, 1 bsl 32 - 1 }], %% max uint32
  test_encode_decode(Tests, fun encode_uint32_descending/2, fun decode_uint32_descending/1).


encode_uint64_ascending_test() ->
  Tests = [{ << 0, 0, 0, 0, 0, 0, 0, 0 >>, 0 },
           { << 0, 0, 0, 0, 0, 0, 0, 1 >>, 1 },
           { << 0, 0, 0, 0, 0, 0, 1, 0 >>, 1 bsl 8 },
           { << 16#ff, 16#ff, 16#ff, 16#ff, 16#ff, 16#ff, 16#ff, 16#ff >>, 1 bsl 64 - 1 }], %% max uint64
  test_encode_decode(Tests, fun encode_uint64_ascending/2, fun decode_uint64_ascending/1).

encode_uint64_descending_test() ->
  Tests = [{ << 16#ff, 16#ff, 16#ff, 16#ff, 16#ff, 16#ff, 16#ff, 16#ff >>, 0 },
           { << 16#ff, 16#ff, 16#ff, 16#ff, 16#ff, 16#ff, 16#ff , 16#fe >>, 1 },
           { << 16#ff, 16#ff, 16#ff, 16#ff, 16#ff, 16#ff, 16#fe, 16#ff >>, 1 bsl 8 },
           { << 0, 0, 0, 0, 0, 0, 0, 0 >>, 1 bsl 64 - 1 }], %% max uint 64
  test_encode_decode(Tests, fun encode_uint64_descending/2, fun decode_uint64_descending/1).


encode_varint_ascending_test() ->
  Tests = [{ << 16#86, 16#ff, 16#00 >>, -1 bsl 8 },
           { << 16#87, 16#ff >>, -1 },
           { << 16#88 >>, 0 },
           { << 16#89 >>, 1 },
           { << 16#f5 >>, 109 },
           { << 16#f6, 16#f70 >>, 112 },
           { << 16#f7, 16#01, 16#00 >>, 1 bsl 8 },
           { << 16#fd, 16#7f, 16#ff, 16#ff, 16#ff, 16#ff, 16#ff, 16#ff, 16#ff >>, 16#7FFFFFFFFFFFFFFF}], %% max int64
  test_encode_decode(Tests, fun encode_varint_ascending/2, fun decode_varint_ascending/1).

encode_varint_descending_test() ->
  Tests = [{ << 16#fd, 16#7f, 16#ff, 16#ff, 16#ff, 16#ff, 16#ff, 16#ff, 16#ff >>, -(16#7FFFFFFFFFFFFFFF + 1) }, %% min int64
           { << 16#fd, 16#7f, 16#ff, 16#ff, 16#ff, 16#ff, 16#ff, 16#ff, 16#fe >>, -(16#7FFFFFFFFFFFFFFF + 1) + 1 },
           { << 16#f6, 16#ff >>, -1 bsl 8 },
           { << 16#f5 >>, -110 },
           { << 16#87, 16#ff >>, 0 },
           { << 16#87, 16#fe >>, 1 },
           { << 16#86, 16#fe, 16#ff >>, 1 bsl 8 },
           { << 16#80, 16#80, 16#00, 16#00, 16#00, 16#00, 16#00, 16#00, 16#00 >>, 16#7FFFFFFFFFFFFFFF }],
  test_encode_decode(Tests, fun encode_varint_descending/2, fun decode_varint_descending/1).

encode_uvarint_ascending_test() ->
  Tests = [{ << 16#88 >>, 0 },
           { << 16#89 >>, 1 },
           { << 16#f5 >>, 109 },
           { << 16#f6, 16#6e >>, 110 },
           { << 16#f7, 16#01, 16#00 >>, 1 bsl 8 },
           { << 16#fd, 16#ff, 16#ff, 16#ff, 16#ff, 16#ff, 16#ff, 16#ff, 16#ff >>, 1 bsl 64 - 1 }],
  test_encode_decode(Tests, fun encode_uvarint_ascending/2, fun decode_uvarint_ascending/1).

encode_uvarint_descending_test() ->
  Tests = [{ << 16#88 >>, 0 },
           { << 16#87, 16#fe >>, 1 },
           { << 16#86, 16#fe, 16#ff >>, 1 bsl 8 },
           { << 16#80, 16#00, 16#00, 16#00, 16#00, 16#00, 16#00, 16#00, 16#01 >>, (1 bsl 64 - 1) - 1 },
           { << 16#80, 16#00, 16#00, 16#00, 16#00, 16#00, 16#00, 16#00, 16#00 >>, 1 bsl 64 - 1 }],
  test_encode_decode(Tests, fun encode_uvarint_descending/2, fun decode_uvarint_descending/1).

encode_binary_ascending_test() ->
  Tests = [{ << 16#12, 16#00, 16#ff, 1, "a", 16#00, 16#01 >>, << 0, 1, "a" >> },
           { << 16#12, 16#00, 16#ff, "a", 16#00, 16#01 >>, << 0, "a" >> },
           { << 16#12, "a", 16#00, 16#01 >>, <<"a">> },
           { << 16#12, "b", 16#00, 16#01 >>, <<"b">> },
           { << 16#12, "b", 16#00, 16#ff, 16#00, 16#01 >>, <<"b", 0 >> },
           { << 16#12, "b", 16#00, 16#ff, 16#00, 16#ff, 16#00, 16#01 >>, <<"b", 0, 0 >> },
           { << 16#12, "b", 16#00, 16#ff, 16#00, 16#ff, "a", 16#00, 16#01 >>, <<"b", 0, 0, "a" >> },
           { << 16#12, "b", 16#ff, 16#00, 16#01 >>, <<"b", 16#ff >> },
           { << 16#12, $h, $e, $l, $l, $o, 16#00, 16#01 >>, <<"hello">> },
           { << 16#12, "hello", 16#00, 16#01 >>, <<"hello">> }],
  test_encode_decode(Tests, fun encode_binary_ascending/2, fun decode_binary_ascending/1).

encode_binary_descending_test() ->
  Tests = [{ << 16#13, (bnot $h), (bnot $e), (bnot $l), (bnot $l), (bnot $o), 16#ff, 16#fe >>, <<"hello">> },
           { << 16#13, (bnot $b), 16#00, 16#ff, 16#fe >>, << "b", 16#ff >> },
           { << 16#13, (bnot $b), 16#ff, 16#00, 16#ff, 16#00, (bnot $a), 16#ff, 16#fe >>, << "b", 0, 0, "a" >> },
           { << 16#13, (bnot $b), 16#ff, 16#00, 16#ff, 16#00, 16#ff, 16#fe >>, << "b", 0, 0 >> },
           { << 16#13, (bnot $b), 16#ff, 16#00, 16#ff, 16#fe >>, << "b", 0 >> },
           { << 16#13, (bnot $b),  16#ff, 16#fe >>, << "b" >> },
           { << 16#13, (bnot $a),  16#ff, 16#fe >>, << "a" >> },
           { << 16#13, 16#ff, 16#00, 16#00, (bnot $a),  16#ff, 16#fe >>, << 0, 16#ff, "a" >> },
           { << 16#13, 16#ff, 16#00, (bnot $a),  16#ff, 16#fe >>, << 0, "a" >> },
           { << 16#13, 16#ff, 16#00, 16#fe, (bnot $a),  16#ff, 16#fe >>, << 0, 1, "a" >> }],
 test_encode_decode(Tests, fun encode_binary_descending/2, fun decode_binary_descending/1).


encode_literal_ascending_test() ->
  Tests = [{ << 16#14, 16#66, 16#61, 16#6c, 16#73, 16#65, 16#00, 16#01 >>, false },
           { << 16#14, 16#6e, 16#75, 16#6c, 16#6c, 16#00, 16#01 >>, null },
           { << 16#14, 16#74, 16#72, 16#75, 16#65, 16#00, 16#01 >>, true } ],
  test_encode_decode(Tests, fun encode_literal_ascending/2, fun decode_literal_ascending/1).

encode_literal_descending_test() ->
  Tests = [{ << 16#15, (bnot 16#66), (bnot 16#61), (bnot 16#6c), (bnot 16#73), (bnot 16#65), 16#ff, 16#fe >>, false },
           { << 16#15, (bnot 16#6e), (bnot 16#75), (bnot 16#6c), (bnot 16#6c), 16#ff, 16#fe >>, null },
           { << 16#15, (bnot 16#74), (bnot 16#72), (bnot 16#75), (bnot 16#65), 16#ff, 16#fe >>, true } ],
  test_encode_decode(Tests, fun encode_literal_descending/2, fun decode_literal_descending/1).

encode_nonsorting_uvarint_test() ->
  TestEncodeFun = fun(I) ->
                      {I, <<>>} = decode_nonsorting_uvarint(encode_nonsorting_uvarint(<<>>, I)),
                      true
                  end,
  _ = lists:map(TestEncodeFun, edge_case_uint64()),
  _ = lists:map(TestEncodeFun, rand_pow_distributed_int63(1000)).

encode_float_ascending_test() ->
  Tests = [{ << 16#03, 16#00, 16#1e, 16#33, 16#0c, 16#7a, 16#14, 16#37, 16#5f >>, -1.0e308 },
           { << 16#03, 16#3f, 16#3c, 16#77, 16#ff, 16#ff, 16#ff, 16#ff, 16#ff >>, -10000.0 },
           { << 16#03, 16#3f, 16#3c, 16#78, 16#7f, 16#ff, 16#ff, 16#ff, 16#ff >>, -9999.0 },
           { << 16#03, 16#3F, 16#a6, 16#ff, 16#ff, 16#ff, 16#ff, 16#ff, 16#ff >>, -100.0 },
           { << 16#03, 16#40, 16#0f, 16#ff, 16#ff, 16#ff, 16#ff, 16#ff, 16#ff >>, -1.0 },
           { << 16#03, 16#40, 16#ab, 16#d9, 16#01, 16#8e, 16#75, 16#79, 16#28 >>, -0.00123 },
           { << 16#04 >>, 0 },
           { << 16#05, 16#3f, 16#54, 16#26, 16#fe, 16#71, 16#8a, 16#86, 16#d7 >>, 0.00123 },
           { << 16#05, 16#3f, 16#89, 16#30, 16#be, 16#0d, 16#ed, 16#28, 16#8d >>, 0.0123 },
           { << 16#05, 16#3f, 16#bf, 16#7c, 16#ed, 16#91, 16#68, 16#72, 16#b0 >>, 0.123 },
           { << 16#05, 16#3f, 16#f0, 16#00, 16#00, 16#00, 16#00, 16#00, 16#00 >>, 1.0 },
           { << 16#05, 16#40, 16#24, 16#00, 16#00, 16#00, 16#00, 16#00, 16#00 >>, 10.0 },
           { << 16#05, 16#40, 16#28, 16#b0, 16#a3, 16#d7, 16#0a, 16#3d, 16#71 >>, 12.345 },
           { << 16#05, 16#40, 16#58, 16#c0, 16#00, 16#00, 16#00, 16#00, 16#00 >>, 99.0 },
           { << 16#05, 16#40, 16#58, 16#c0, 16#01, 16#a3, 16#6e, 16#2e, 16#b2 >>, 99.0001 },
           { << 16#05, 16#40, 16#58, 16#c0, 16#a3, 16#d7, 16#0a, 16#3d, 16#71 >>, 99.01 },
           { << 16#05, 16#40, 16#59, 16#00, 16#00, 16#00, 16#00, 16#00, 16#00 >>, 100.0 },
           { << 16#05, 16#40, 16#59, 16#00, 16#a3, 16#d7, 16#0a, 16#3d, 16#71 >>, 100.01 },
           { << 16#05, 16#40, 16#59, 16#06, 16#66, 16#66, 16#66, 16#66, 16#66 >>, 100.1 },
           { << 16#05, 16#40, 16#93, 16#48, 16#00, 16#00, 16#00, 16#00, 16#00 >>, 1234.0 },
           { << 16#05, 16#40, 16#93, 16#4a, 16#00, 16#00, 16#00, 16#00, 16#00 >>, 1234.5 },
           { << 16#05, 16#40, 16#c3, 16#87, 16#80, 16#00, 16#00, 16#00, 16#00 >>, 9999.0 },
           { << 16#05, 16#40, 16#c3, 16#87, 16#80, 16#00, 16#08, 16#63, 16#7c >>, 9999.000001 },
           { << 16#05, 16#40, 16#c3, 16#87, 16#80, 16#00, 16#4b, 16#7f, 16#5a >>, 9999.000009 },
           { << 16#05, 16#40, 16#c3, 16#87, 16#80, 16#00, 16#53, 16#e2, 16#d6 >>, 9999.00001 },
           { << 16#05, 16#40, 16#c3, 16#87, 16#80, 16#02, 16#f2, 16#f9, 16#87 >>, 9999.00009 },
           { << 16#05, 16#40, 16#c3, 16#87, 16#80, 16#03, 16#3e, 16#78, 16#e2 >>, 9999.000099 },
           { << 16#05, 16#40, 16#c3, 16#87, 16#80, 16#03, 16#46, 16#dc, 16#5d >>, 9999.0001 },
           { << 16#05, 16#40, 16#c3, 16#87, 16#80, 16#20, 16#c4, 16#9b, 16#a6 >>, 9999.001 },
           { << 16#05, 16#40, 16#c3, 16#87, 16#81, 16#47, 16#ae, 16#14, 16#7b >>, 9999.01 },
           { << 16#05, 16#40, 16#c3, 16#87, 16#8c, 16#cc, 16#cc, 16#cc, 16#cd >>, 9999.1 },
           { << 16#05, 16#40, 16#c3, 16#88, 16#00, 16#00, 16#00, 16#00, 16#00 >>, 10000.0 },
           { << 16#05, 16#40, 16#c3, 16#88, 16#80, 16#00, 16#00, 16#00, 16#00 >>, 10001.0 },
           { << 16#05, 16#40, 16#c8, 16#1c, 16#80, 16#00, 16#00, 16#00, 16#00 >>, 12345.0 },
           { << 16#05, 16#40, 16#fe, 16#23, 16#a0, 16#00, 16#00, 16#00, 16#00 >>, 123450.0 },
           { << 16#05, 16#7f, 16#e1, 16#cc, 16#f3, 16#85, 16#eb, 16#c8, 16#a0 >>, 1.0e308 }],
  test_encode_decode(Tests, fun encode_float_ascending/2, fun decode_float_ascending/1),

  %% test ascending order
  lists:foldl(fun
                ({_Encoded, Value}, nil) ->
                  encode_float_ascending(<<>>, Value);
                ({_Encoded, Value}, Last) ->
                  New = encode_float_ascending(<<>>, Value),
                  true = (New > Last),
                  New
              end,
              nil,
              Tests),

  %% test appending work
  true = (encode_float_ascending(<<"hello">>, 2.0) > encode_float_ascending(<<"hello">>, 1.0)),
  true = (encode_float_descending(<<"hello">>, 1.0) > encode_float_descending(<<"hello">>, 2.0)).




%% == helpers

test_encode_decode([{Encoded, Value} | Rest], Enc, Dec) ->
  Encoded = Enc(<<>>, Value),
  {Value, <<>>} = Dec(Enc(<<>>, Value)),
  test_encode_decode(Rest, Enc, Dec);
test_encode_decode([], _Enc, _Dec) ->
  ok.

edge_case_uint64() ->
  Cases = lists:foldl(fun(I, Acc) ->
                          X = 1 bsl I,
                          [to_uint64(X+1), to_uint64(X), to_uint64(X-1) | Acc]
                      end,
                      [2, 1, 0],
                      lists:seq(2, 64)),
  lists:reverse([ 1 bsl 64 - 1 | Cases ]).

rand_pow_distributed_int63(Count) ->
  Values = lists:foldl(fun(_I, Acc) ->
                           Digits = rand:uniform(63) + 1,
                           X = rand:uniform(1 bsl Digits),
                           Acc2 = case (X bsr (Digits - 1)) of
                                    0 ->
                                      [to_uint64(rand:uniform(1 bsl Digits)) | Acc];
                                    _ -> [X | Acc]
                                  end,
                           Acc2
                       end,
                       [],
                       lists:seq(1, Count)),
  Values.


-endif.
