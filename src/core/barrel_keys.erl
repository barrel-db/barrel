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

-module(barrel_keys).
-author("benoitc").

%% API
-export([
  prefix/1,
  db_meta_key/1,
  doc_key/1,
  seq_key/1,
  sys_key/1,
  rev_key/2,
  res_key/1
]).

-export([
  forward_path_key/2,
  reverse_path_key/2,
  encode_partial_path/2,
  forward_key_prefix/1,
  reverse_key_prefix/1
]).

-export([enc/2]).

prefix(db_meta) ->  << 0, 0, 0 >>;
prefix(doc) ->  <<  0, 50, 0 >>;
prefix(seq) ->  <<  0, 100, 0>>;
prefix(res) ->  <<  0, 200, 0>>;
prefix(sys_doc) ->  << 0, 300, 0 >>;
prefix(idx_forward_path) ->  <<  0, 410, 0 >>;
prefix(idx_reverse_path) ->  <<  0, 420, 0 >>;
prefix(idx_snapshot) -> << 0, 400, 0 >>;
prefix(_) ->  erlang:error(badarg).

%% metadata keys

%% db keys

db_meta_key(Meta) -> << (prefix(db_meta))/binary, (barrel_lib:to_binary(Meta))/binary >>.

doc_key(DocId) ->  << (prefix(doc))/binary,  DocId/binary >>.

seq_key(Seq) -> << (prefix(seq))/binary, Seq:64>>.

sys_key(DocId) -> << (prefix(sys_doc))/binary,  DocId/binary>>.

rev_key(DocId, Rev) -> << DocId/binary, 1, Rev/binary >>.

res_key(RId) -> << (prefix(res))/binary, RId:64>>.

%% index keys

short(<< S:100/binary, _/binary >>) -> S;
short(S) when is_binary(S) -> S;
short(S) -> S.

forward_path_key(Path, Seq)  ->
  barrel_encoding:encode_varint_ascending(
    encode_partial_path(Path, prefix(idx_forward_path)),
    Seq
  ).

reverse_path_key(Path, Seq)  ->
  barrel_encoding:encode_varint_ascending(
    encode_partial_path(lists:reverse(Path), prefix(idx_reverse_path)),
    Seq
  ).

encode_partial_path([P], Bin) ->
  enc(short(P), Bin);
encode_partial_path([P | Rest], Bin) ->
  Bin2 = << Bin/binary, (xxhash:hash32(P))/binary >>,
  encode_partial_path(Rest, Bin2).

forward_key_prefix(Path) ->
  encode_partial_path(Path, prefix(idx_forward_path)).

reverse_key_prefix(Path) ->
  encode_partial_path(lists:reverse(Path), prefix(idx_reverse_path)).

enc(B, P) when is_binary(P) ->
  barrel_encoding:encode_binary_ascending(B, P);
enc(B, P) when is_integer(P) ->
  barrel_encoding:encode_varint_ascending(B, P);
enc(B, P) when is_float(P) ->
  barrel_encoding:encode_float_ascending(B, P);
enc(B, false) ->
  barrel_encoding:encode_literal_ascending(B, false);
enc(B, null) ->
  barrel_encoding:encode_literal_ascending(B, null);
enc(B, true) ->
  barrel_encoding:encode_literal_ascending(B, true);
enc(B, Else) ->
  barrel_encoding:encode_binary_ascending(B, barrel_lib:to_binary(Else)).
