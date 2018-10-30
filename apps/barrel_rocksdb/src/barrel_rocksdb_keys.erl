%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. Sep 2018 11:29
%%%-------------------------------------------------------------------
-module(barrel_rocksdb_keys).
-author("benoitc").

%% API
-export([
  make_db_prefix/1,
  decode_db_predix/1
]).

-export([
  barrel_key/1
]).

%% global keys (single byte)
-define(local_prefix, 16#01).
-define(local_max, 16#02).
-define(meta1_prefix, ?local_max).
-define(meta2_prefix, 16#03).
-define(meta_max, 16#04).
-define(system_prefix, ?meta_max).
-define(system_max, 16#05).


make_db_prefix(Id) ->
  barrel_encoding:encode_uvarint_ascending(<<>>, Id).

decode_db_predix(Key) ->
  case barrel_encoding:pick_encoding(Key) of
    int ->
      {Id, _} = barrel_encoding:decode_uvarint_ascending(Key),
      Id
  end.


barrel_key(Name) ->
  << 0, 0, 0, 100, Name/binary >>.
