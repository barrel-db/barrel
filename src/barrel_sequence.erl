-module(barrel_sequence).

-export([init/1,
         inc/1,
         sequence_max/0,
         sequence_min/0,
         encode/1,
         decode/1,
         to_string/1, to_string/2,
         from_string/1]).

-include("barrel.hrl").

-define(enc(B, V), barrel_encoding:encode_uint64_ascending(B, V)).

-define(dec(B), barrel_encoding:decode_uint64_ascending(B)).


-type barrel_node() :: binary() | atom().
-type sequence() :: term().

init(Barrel) ->
  Epoch = ?EPOCH_STORE:new_epoch(Barrel),
  {Epoch, 0}.

inc({Epoch, Seq}) -> {Epoch, Seq + 1}.

encode({Epoch, Seq}) ->
  ?enc(?enc(<<>>, Epoch), Seq);
encode(_) ->
  erlang:error(badarg).

decode(SeqBin) ->
  {Epoch, Rest} = ?dec(SeqBin),
  {Seq, _} = ?dec(Rest),
  {Epoch, Seq}.

sequence_max() -> encode({ 1 bsl 64 -1, 0}).

sequence_min() -> encode({0, 0}).

%% @doc return a global sequence as string using the current node() as node.
-spec to_string(sequence()) -> binary().
to_string(Seq) ->
  to_string(erlang:node(), Seq).

%% @doc return a global sequence as string
-spec to_string(barrel_node(), sequence()) -> binary().
to_string(Node, Seq) when is_binary(Node) ->
  << (uid_b64:encode(Node))/binary, ":",  (uid_b64:encode(Seq))/binary >>;
to_string(Node, Seq) ->
  to_string(hid(Node), Seq).

%% @doc extract the node id and its local sequence from a global sequence string
-spec from_string(binary()) -> {barrel_node(), sequence()}.
from_string(Bin) ->
  case binary:split(Bin, <<":">>) of
    [NodeBin, SeqBin] ->
      {uid_b64:decode(NodeBin), uid_b64:decode(SeqBin)};
    _ ->
      erlang:error(badarg)
  end.

hid(Node) ->
  <<(erlang:phash(Node, 1 bsl 32)):32>>.
