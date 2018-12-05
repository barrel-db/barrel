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
  doc_seq/2,
  decode_doc_seq/1,
  doc_seq_max/1,
  doc_seq_prefix/1,
  doc_rev/3
]).


-include_lib("barrel/include/barrel_logger.hrl").
-include("barrel_rocksdb_keys.hrl").



%% ----
%% local storage

%% barrel ident
local_barrel_ident_max() -> barrel_rocksdb_util:bytes_prefix_end(?local_barrel_ident_prefix).
local_barrel_ident(Name) ->
  barrel_encoding:encode_binary_ascending(?local_barrel_ident_prefix, Name).

decode_barrel_ident(<< _:3/binary, Key/binary >>) ->
  ?LOG_INFO("decode barrel ident=~p~n", [Key]),
  {Name, _} = barrel_encoding:decode_binary_ascending(Key),
  Name.

%% ----
%% barrels local metadata

%% @doc key for document count of a barrel
docs_count(BarrelId) ->
  barrel_encoding:encode_binary_ascending(?docs_count_prefix, BarrelId).

%% @doc key for deleted document count of a barrel
docs_del_count(BarrelId) ->
  barrel_encoding:encode_binary_ascending(?docs_del_count_prefix, BarrelId).

%% @doc purge sequence key for a barrel
purge_seq(BarrelId) ->
  barrel_encoding:encode_binary_ascending(?purge_seq_prefix, BarrelId).


%% ----
%% barrel replicated documents

db_prefix(BarrelId) ->
  << ?db_prefix/binary, BarrelId/binary >>.

db_prefix_end(BarrelId) ->
  barrel_rocksdb_util:bytes_prefix_end(db_prefix(BarrelId)).


%% @doc document info key
doc_info(BarrelId, DocId) ->
  barrel_encoding:encode_binary_ascending(
    << (db_prefix(BarrelId))/binary, ?docs_info_suffix/binary >>,
    DocId
  ).

%% @doc document sequence key
doc_seq(BarrelId, Seq) ->
  barrel_encoding:encode_uint64_ascending(
    << (db_prefix(BarrelId))/binary, ?docs_sec_suffix/binary >>,
    Seq
  ).

decode_doc_seq(SecKey) ->
  {Seq, _} = barrel_encoding:decode_uint64_descending(SecKey),
  Seq.

doc_seq_prefix(BarrelId) -> << (db_prefix(BarrelId))/binary, ?docs_sec_suffix/binary >>.

%% @doc max document sequence key
doc_seq_max(BarrelId) ->
  barrel_encoding:encode_uint64_ascending(
    << (db_prefix(BarrelId))/binary, ?docs_sec_suffix/binary >>,
    1 bsl 64 - 1
  ).

%% @doc document revision key
doc_rev(BarrelId, DocId, DocRev) ->
  barrel_encoding:encode_binary_ascending(
    << (db_prefix(BarrelId))/binary, ?docs_revision_suffix/binary >>,
    << DocId/binary, DocRev/binary >>
  ).

  
  
