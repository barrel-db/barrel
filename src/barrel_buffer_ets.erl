%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 26. Oct 2018 09:59
%%%-------------------------------------------------------------------
-module(barrel_buffer_ets).
-author("benoitc").

%% API
-export([
  new/0,
  len/1,
  in/3,
  out/1,
  remove/3

]).


new() ->
  ets:new(barrel_buffer, [public, ordered_set]).


len(Buffer) -> ets:info(Buffer, size).

in(Item, Ts, Buffer) ->
  ets:insert_new(Buffer, {Ts, Item}).

out(Buffer) ->
  case ets:first(Buffer) of
    '$end_of_table' -> empty;
    Key ->
      [{_, Item}] = ets:take(Buffer, Key),
      {ok, Item}
  end.

remove(Item, Ts, Buffer) ->
  ets:delete_object(Buffer, {Ts, Item}).
