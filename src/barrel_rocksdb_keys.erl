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
-module(barrel_rocksdb_keys).
-author("benoitc").

%% API
-export([
  local_barrel_ident/1,
  local_barrel_ident_max/0,
  decode_barrel_ident/1,
  docs_count/1,
  docs_del_count/1,
  purge_seq/1
]).

-export([
  db_prefix/1,
  db_prefix_end/1
]).

-export([
  doc_info/2,
  doc_info_max/1,
  doc_seq/2,
  decode_doc_seq/2,
  doc_seq_max/1,
  doc_seq_prefix/1,
  doc_rev/3,
  local_doc/2
]).

-export([view_prefix/2,
         view_meta/2,
         view_prefix_end/2,
         view_upgrade_task/2,
         view_doc_key/3,
         view_key/3,
         encode_view_key/2,
         decode_view_key/2]).


-include("barrel.hrl").
-include("barrel_rocksdb_keys.hrl").

%% ----
%% local storage

%% barrel ident
local_barrel_ident_max() ->
  local_barrel_ident(?key_max).

local_barrel_ident(Name) ->
  barrel_encoding:encode_binary_ascending(?local_barrel_ident_prefix, Name).

decode_barrel_ident(<< _:3/binary, Key/binary >>) ->
  ?LOG_INFO("decode barrel ident=~p~n", [Key]),
  {Name, _} = barrel_encoding:decode_binary_ascending(Key),
  Name.

db_prefix(BarrelId) ->
  << ?db_prefix/binary, BarrelId/binary >>.

%% ----
%% barrels local metadata

%% @doc key for document count of a barrel
docs_count(BarrelId) ->
  << (db_prefix(BarrelId))/binary, ?docs_count_suffix/binary >>.

%% @doc key for deleted document count of a barrel
docs_del_count(BarrelId) ->
  << (db_prefix(BarrelId))/binary, ?docs_del_count_suffix/binary >>.

%% @doc purge sequence key for a barrel
purge_seq(BarrelId) ->
  << (db_prefix(BarrelId))/binary, ?purge_seq_suffix/binary >>.

%% ----
%% barrel replicated documents

db_prefix_end(BarrelId) ->
  barrel_rocksdb_util:bytes_prefix_end(db_prefix(BarrelId)).

%% @doc document info key
doc_info(BarrelId, DocId) ->
  barrel_encoding:encode_binary_ascending(
    << (db_prefix(BarrelId))/binary, ?docs_info_suffix/binary >>,
    DocId
  ).

doc_info_max(BarrelId) ->
  << (db_prefix(BarrelId))/binary,
     (barrel_rocksdb_util:bytes_prefix_end(?docs_info_suffix))/binary >>.

%% @doc document sequence key
doc_seq(BarrelId, Seq) ->
  barrel_encoding:encode_uint64_ascending(
    << (db_prefix(BarrelId))/binary, ?docs_sec_suffix/binary >>,
    Seq
  ).

decode_doc_seq(BarrelId, SeqKey) ->
  case binary:split(SeqKey, doc_seq_prefix(BarrelId)) of
    [<<>>, SeqPart] ->
      {Seq, _} = barrel_encoding:decode_uint64_ascending(SeqPart),
      Seq;
    [] ->
      erlang:error(badarg)
  end.


doc_seq_prefix(BarrelId) -> << (db_prefix(BarrelId))/binary, ?docs_sec_suffix/binary >>.

%% @doc max document sequence key
doc_seq_max(BarrelId) ->
  doc_seq(BarrelId, 1 bsl 64 - 1).

%% @doc document revision key
doc_rev(BarrelId, DocId, DocRev) ->
  barrel_encoding:encode_binary_ascending(
    << (db_prefix(BarrelId))/binary, ?docs_revision_suffix/binary >>,
    << DocId/binary, DocRev/binary >>
  ).

%% @doc local document key
local_doc(BarrelId, DocId) ->
  barrel_encoding:encode_binary_ascending(
    << (db_prefix(BarrelId))/binary, ?local_doc_prefix/binary >>,
    DocId
  ).


view_prefix(BarrelId, ViewId) ->
  << (db_prefix(BarrelId))/binary, ?view_key/binary, ViewId/binary >>.

view_meta(BarrelId, ViewId) ->
  << (view_prefix(BarrelId, ViewId))/binary, ?view_meta_suffix/binary >>.


view_prefix_end(BarrelId, ViewId) ->
  barrel_rocksdb_util:bytes_prefix_end(view_prefix(BarrelId, ViewId)).


view_upgrade_task(BarrelId, ViewId) ->
  << (view_prefix(BarrelId, ViewId))/binary, ?view_upgrade_suffix/binary >>.

view_doc_key(BarrelId, ViewId, DocId) ->
   << (view_prefix(BarrelId, ViewId))/binary, ?reverse_map_prefix/binary, DocId/binary >>.

view_key(BarrelId, ViewId, Key) when is_list(Key) ->
  Prefix = << (view_prefix(BarrelId, ViewId))/binary >>,
  encode_view_key(Key, Prefix);
view_key(BarrelId, ViewId, Key) when is_binary(Key); is_number(Key) ->
  view_key(BarrelId, ViewId, [Key]);
view_key(BarrelId, ViewId, Key) ->
  ok = barrel_encoding:is_literal(Key),
  view_key(BarrelId, ViewId, [Key]).

encode_view_key(Key, AccBin) ->
  encode_view_key_1(Key, << AccBin/binary, ?index_prefix/binary >>).

encode_view_key_1([Term|Rest], AccBin) ->
  encode_view_key_1(Rest, encode_view_term(Term, AccBin));
encode_view_key_1([], AccBin) ->
  AccBin.


encode_view_term(L, B) when is_atom(L) ->
  barrel_encoding:encode_literal_ascending(B, L);
encode_view_term(S, B) when is_binary(S) ->
  barrel_encoding:encode_binary_ascending(B, S);
encode_view_term(N, B) when is_integer(N) ->
  barrel_encoding:encode_varint_ascending(B, N);
encode_view_term(N, B) when is_number(N) ->
  barrel_encoding:encode_float_ascending(B, N).


decode_view_key(Prefix, Bin) ->
  case binary:split(Bin, << Prefix/binary, ?index_prefix/binary >>) of
    [_ViewPrefix, KeyBin] ->
      decode_view_key_1(KeyBin, []);
    _Else ->
      erlang:error(badarg)
  end.

decode_view_key_1(<<>>, Acc) ->
  [DocId | Key] = Acc,
  {DocId, Key};
decode_view_key_1(Bin, Acc) ->
  case barrel_encoding:pick_encoding(Bin) of
    bytes ->
      {Val, Rest} = barrel_encoding:decode_binary_ascending(Bin),
      decode_view_key_1(Rest, [Val | Acc]);
    int ->
       {Val, Rest} = barrel_encoding:decode_varint_ascending(Bin),
       decode_view_key_1(Rest, [Val | Acc]);
    float ->
      {Val, Rest} = barrel_encoding:decode_float_ascending(Bin),
      decode_view_key_1(Rest, [Val | Acc]);
    literal ->
      {Val, Rest} = barrel_encoding:decode_literal_ascending(Bin),
      decode_view_key_1(Rest, [Val | Acc]);
    _Else ->
      erlang:error(badarg)
  end.

