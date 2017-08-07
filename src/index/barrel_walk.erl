%% Copyright 2016, Benoit Chesneau
%%
%% Licensed under the Apache License, Version 2.0 (the "License"); you may not
%% use this file except in compliance with the License. You may obtain a copy of
%% the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
%% License for the specific language governing permissions and limitations under
%% the License.

-module(barrel_walk).

-export([walk/5]).

-include("barrel.hrl").


-define(DEFAULT_MAX, 1000).


walk(#db{store=Store}, Path, UserFun, AccIn, Options0) ->
  Options1 = validate_options(Options0),
  PathParts = decode_path(Path, []),
  
  lager:info("path parts are ~p~n", [PathParts]),

  %% set fold options
  {MoveAction, Inclusive, First, InRange} = make_range(PathParts, Options1),
  OrderBy = maps:get(order_by, Options1, order_by_key),
  EqualTo = maps:get(equal_to, Options1, undefined),
  Limit = maps:get(limit, Options1, ?DEFAULT_MAX),
  
  
  lager:info("inclusive are ~p~n", [{MoveAction, Inclusive, First, InRange}]),
  %% rocksdb options
  {ok, Snapshot} = rocksdb:snapshot(Store),
  ReadOptions = [{snapshot, Snapshot}],

  %% wrapper function to retrieve the results from the iterator
  WrapperFun =
    fun(KeyBin, _, Acc) ->
      {Rid, Pointer} = parse_rid(KeyBin, OrderBy),
      Res = rocksdb:get(Store, barrel_keys:res_key(Rid), ReadOptions),
      case Res of
        {ok, Bin} ->
          DocInfo = binary_to_term(Bin),
          {ok, Doc, Meta} = barrel_db:get_current_revision(DocInfo),
          case EqualTo of
            undefined ->
              UserFun(Doc, Meta, Acc);
            Val ->
              case get_value(Pointer, Doc) of
                Val -> UserFun(Doc, Meta, Acc);
                _ -> {ok, Acc}
              end
          end;
        _Else ->
          {ok, Acc}
      end
    end,

  %% initialize the iterator
  {ok, Itr} = rocksdb:iterator(Store, ReadOptions),
  Next = fun() -> rocksdb:iterator_move(Itr, MoveAction) end,
  Start = case MoveAction of
            next -> rocksdb:iterator_move(Itr, First);
            prev -> rocksdb:iterator_move(Itr, {seek_for_prev, First})
          end,
  

  %% start folding
  try
    fold_loop(
      maybe_next(Start, Next, Inclusive),
      Next, WrapperFun, AccIn, InRange, Limit - 1
    )
  after safe_iterator_close(Itr)
  end.

maybe_next({ok, Key, _V} = Start, Next, MatchNext) when is_function(MatchNext) ->
  lager:info("maybe ~p~n", [Key]),
  case MatchNext(Key) of
    true -> Start;
    false ->  Next()
  end;
maybe_next(Start, _Next, _Inclusive) ->
  Start.



safe_iterator_close(Itr) -> (catch rocksdb:iterator_close(Itr)).

fold_loop({error, iterator_closed}, _Next, _WrapperFun, Acc, _Cmp, _Limit) ->
  throw({iterator_closed, Acc});
fold_loop({error, invalid_iterator}, _Next, _WrapperFun, Acc, _Cmp, _Limit) ->
  Acc;
fold_loop({ok, K, V}, Next, WrapperFun, Acc0, Cmp, Limit) ->
  case Cmp(K) of
    true ->
      case WrapperFun(K, V, Acc0) of
        {ok, Acc1} when Limit > 0 ->
          fold_loop(Next(), Next, WrapperFun, Acc1, Cmp, Limit - 1);
        {ok, Acc1} -> Acc1;
        {skip, Acc1} ->
          fold_loop(Next(), Next, WrapperFun, Acc1, Cmp, Limit);
        skip ->
          fold_loop(Next(), Next, WrapperFun, Acc0, Cmp, Limit);
        stop -> Acc0;
        {stop, Acc1} -> Acc1;
        Acc1 -> Acc1
      end;
    false ->
      Acc0
  end.

get_value([Key | Rest], Obj) when is_map(Obj) ->
  get_value(Rest, maps:get(Key, Obj));
get_value([BinInt | Rest], Obj) when is_list(Obj) ->
  Idx = binary_to_integer(BinInt) + 1, %% erlang lists start at 1
  get_value(Rest, lists:nth(Idx, Obj));
get_value([Key], Obj) ->
  ToMatch = case jsx:is_json(Key) of
              true -> jsx:decode(Key);
              false -> Key
            end,
  if
    Obj =:= ToMatch -> null;
    true -> erlang:error(badarg)
  end;
get_value([], Obj) ->
  Obj.

encode_key(Parts, order_by_key) ->
  barrel_keys:encode_path_forward(
    barrel_keys:prefix(idx_forward_path),
    Parts
  );
encode_key(Parts, order_by_value) ->
  barrel_keys:encode_path_reverse(
    barrel_keys:prefix(idx_reverse_path),
    Parts
  ).

encode_key(Parts, Seq, order_by_key) ->
  barrel_keys:forward_path_key(Parts, Seq);
encode_key(Parts, Seq, order_by_value) ->
  barrel_keys:reverse_path_key(Parts, Seq).


make_range(Path, #{ move := prev }= Options) ->
  OrderBy = maps:get(order_by, Options, order_by_key),
  Prefix = encode_key(Path, OrderBy),
  {Inclusive, First}  = case maps:find(end_at, Options) of
             {ok, EndAt} ->
               {true,
                 encode_key(Path ++ [EndAt], 16#7FFFFFFFFFFFFFFF, OrderBy)
               };
             error ->
               case maps:get(previous_to, Options, undefined) of
                 undefined ->
                   {true, encode_key(Path, 16#7FFFFFFFFFFFFFFF, OrderBy)};
                 Prev0 ->
                   Prev1 = encode_key(Path ++ [Prev0], 16#7FFFFFFFFFFFFFFF, OrderBy),
                   {
                     fun(K) -> (match_prefix(K, Prefix) andalso K =< Prev1) end,
                     Prev1
                   }
               end
           end,
  InRange = case maps:get(start_at, Options, undefined)  of
              undefined ->
                case maps:get(next_to, Options, undefined) of
                  undefined ->
                    fun(K) -> match_prefix(K, Prefix) end;
                  Next0 ->
                    Next1 = encode_key(Path ++ [Next0], 16#7FFFFFFFFFFFFFFF, OrderBy),
                    fun(K) -> (match_prefix(K, Prefix) andalso K > Next1) end
                end;
              StartAt0 ->
                StartAt1 = encode_key(Path ++ [StartAt0], OrderBy),
                fun(K) -> (match_prefix(K, Prefix) andalso K >= StartAt1) end
            end,
  {prev, Inclusive, First, InRange};
make_range(Path, #{ move := next }= Options) ->
  OrderBy = maps:get(order_by, Options, order_by_key),
  Prefix = encode_key(Path, OrderBy),
  {Inclusive, First} = case maps:find(start_at, Options) of
            {ok, StartAt} ->
              {true,
                encode_key(Path ++ [StartAt], OrderBy)
              };
            error ->
              case maps:get(next_to, Options, undefined) of
                undefined ->
                  {true, Prefix};
                Next0 ->
                  Next1 = encode_key(Path ++ [Next0], 16#7FFFFFFFFFFFFFFF, OrderBy),
                  {
                    fun(K) ->
                      (match_prefix(K, Prefix) andalso K > Next1)
                    end,
                    Next1
                  }
              end
          end,
  InRange = case maps:get(end_at, Options, undefined)  of
              undefined ->
                case maps:get(previous_to, Options, undefined)  of
                  undefined ->
                    fun(K) -> match_prefix(K, Prefix) end;
                  Prev0 ->
                    Prev1 = encode_key(Path ++ [Prev0], OrderBy),
                    fun(K) -> (match_prefix(K, Prefix) andalso K < Prev1) end
                end;
              EndAt0 ->
                EndAt1 = encode_key(Path ++ [EndAt0], 16#7FFFFFFFFFFFFFFF, OrderBy),
                fun(K) -> (match_prefix(K, Prefix) andalso K =< EndAt1) end
            end,
  {next, Inclusive, First, InRange}.

match_prefix(Bin, Prefix) ->
  L = byte_size(Prefix),
  case Bin of
    << Prefix:L/binary, _/binary >> ->
      true;
    _ ->
      false
  end.

validate_options(Opts) ->
  maps:fold(fun validate_options_fun/3, #{ move => next }, Opts).

validate_options_fun(order_by, OrderBy, Options) when OrderBy =:= order_by_key; OrderBy =:= order_by_value ->
  Options#{ order_by => OrderBy };
validate_options_fun(start_at, Key, Options) when is_binary(Key) ->
  Options#{ start_at => Key };
validate_options_fun(end_at, Key, Options) when is_binary(Key) ->
  Options#{ end_at => Key };
validate_options_fun(previous_to, Key, Options) when is_binary(Key) ->
  Options#{ previous_to => Key };
validate_options_fun(next_to, Key, Options) when is_binary(Key) ->
  Options#{ next_to => Key };
validate_options_fun(equal_to, Val, Options)  ->
  Options#{ equal_to => Val };
validate_options_fun(limit_to_last, Limit, Options) when is_integer(Limit), Limit > 0 ->
  case maps:find(limit_to_first, Options) of
    {ok, _} -> erlang:error(badarg);
    error -> Options#{ limit => Limit, move => prev }
  end;
validate_options_fun(limit_to_first, Limit, Options) when is_integer(Limit), Limit > 0 ->
  case maps:find(limit_to_last, Options) of
    {ok, _} -> erlang:error(badarg);
    error -> Options#{ limit => Limit, move => next }
  end;
validate_options_fun(include_docs, IncludeDocs, Options) when is_boolean(IncludeDocs) ->
  Options#{ include_docs => IncludeDocs };
validate_options_fun(_, _, _) ->
  erlang:error(badarg).

parse_rid(Path, OrderBy) ->
  Prefix = case OrderBy of
             order_by_key -> barrel_keys:prefix(idx_forward_path);
             order_by_value -> barrel_keys:prefix(idx_reverse_path)
           end,
  Sz = byte_size(Prefix),
  << Prefix:Sz/binary, Encoded/binary >> = Path,
  [ Rid | Rest ] = decode_partial_path(Encoded, []),
  [ << "$" >> | Rest1 ] =  lists:reverse(Rest),
  {Rid, Rest1}.


decode_path(<<>>, Acc) ->
  [ << "$" >> | lists:reverse(Acc)];
decode_path(<< $/, Rest/binary >>, Acc) ->
  decode_path(Rest, [<<>> |Acc]);
decode_path(<< $[, Rest/binary >>, Acc) ->
  decode_path(Rest, [<<>> |Acc]);
decode_path(<< $], Rest/binary >>, [BinInt | Acc] ) ->
  case (catch binary_to_integer(BinInt)) of
    {'EXIT', _} ->
      erlang:error(bad_path);
    Int ->
      decode_path(Rest, [Int | Acc])
  end;
decode_path(<<Codepoint/utf8, Rest/binary>>, []) ->
  decode_path(Rest, [<< Codepoint/utf8 >>]);
decode_path(<<Codepoint/utf8, Rest/binary>>, [Current|Done]) ->
  decode_path(Rest, [<< Current/binary, Codepoint/utf8 >> | Done]).

decode_partial_path(<<"">>, Parts) ->
  Parts;
decode_partial_path(B, Parts) ->
  case barrel_encoding:pick_encoding(B) of
    int ->
      {P, R} = barrel_encoding:decode_varint_ascending(B),
      decode_partial_path(R, [P | Parts]);
    float ->
      {P, R} = barrel_encoding:decode_float_ascending(B),
      decode_partial_path(R, [P | Parts]);
    literal ->
      {P, R} = barrel_encoding:decode_literal_ascending(B),
      decode_partial_path(R, [P | Parts]);
    bytes ->
      {P, R} = barrel_encoding:decode_binary_ascending(B),
      decode_partial_path(R, [P | Parts])
  end.