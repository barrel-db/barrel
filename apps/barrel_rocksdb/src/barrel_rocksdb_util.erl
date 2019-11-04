%% Copyright (c) 2018. Benoit Chesneau
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
-module(barrel_rocksdb_util).
-author("benoitc").

%% API
-export([
  bytes_next/1,
  bytes_prefix_end/1
]).

-include("barrel_rocksdb_keys.hrl").


%% @doc return the newt possibile key by appending 16#00 to it
-spec bytes_next(binary()) -> binary().
bytes_next(B) ->
  << B/binary, 16#00 >>.

%% @doc return ending prefix

-spec bytes_prefix_end(binary()) -> binary().
bytes_prefix_end(<<>>) ->
  ?key_max;
bytes_prefix_end(B) ->
  R = binary:encode_unsigned(binary:decode_unsigned(B, little)),
  bytes_prefix_end(R, B).

bytes_prefix_end( <<>>, B) -> B;
bytes_prefix_end( << C, Rest/binary >>, B) ->
  C1 = C + 1,
  if
    C1 =/= 0 ->
      I = byte_size(Rest),
      << T:I/binary, R, _/binary >> = B,
      << T/binary, (R+1) >>;
    true ->
      bytes_prefix_end(Rest, B)
  end.


