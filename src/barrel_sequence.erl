-module(barrel_sequence).

-export([init/1,
         inc/1,
         sequence_max/0,
         encode/1,
         decode/1,
         to_string/1, to_string/2,
         from_string/1]).

-include("barrel.hrl").

-define(enc(B, V), barrel_encoding:encode_uint64_ascending(B, V)).

-define(dec(B), barrel_encoding:decode_uint64_ascending(B)).


init(Barrel) ->
  Epoch = ?EPOCH_STORE:new_epoch(Barrel),
  {Epoch, 0}.

inc({Epoch, Seq}) -> {Epoch, Seq + 1}.

sequence_max() -> { 1 bsl 64 -1, 0}.


encode({Epoch, Seq}) ->
  ?enc(?enc(<<>>, Epoch), Seq).

decode(SeqBin) ->
  {Epoch, Rest} = ?dec(SeqBin),
  {Seq, _} = ?dec(Rest),
  {Epoch, Seq}.


to_string(Seq) ->
  to_string(erlang:node(), Seq).

to_string(Node, Seq) when is_binary(Node) ->
  uid_b64:encode(<< Node/binary, "@", (encode(Seq))/binary >>);

to_string(Node, Seq) ->
  to_string(hid(Node), Seq).

from_string(B64) ->
  Decoded = uid_b64:decode(B64),
  case binary:split(Decoded, <<"@">>) of
    [Node, SeqBin] ->
      {Node, decode(SeqBin)};
    _ ->
      erlang:error(badarg)
  end.

hid(Node) ->
  <<(erlang:phash(Node, 1 bsl 32)):32>>.
