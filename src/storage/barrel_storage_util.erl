%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 08. Feb 2018 22:47
%%%-------------------------------------------------------------------
-module(barrel_storage_util).
-author("benoitc").

%% API
-export([
  encode_continuation/1,
  decode_continuation/1
]).

encode_continuation(Cont) ->
  base64:encode(term_to_binary(Cont)).

decode_continuation(Bin) ->
  binary_to_term(base64:decode(Bin)).
