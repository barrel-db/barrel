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

-export([
  walk/5,
  pull/3, pull/1
]).

-include("barrel.hrl").

-define(DEFAULT_MAX, 1 bsl 32 -1).

-export([decode_path/2]).


pull(#{ db := DbId, path := Path, options := Options, cont := ContKey }) ->
  barrel_db:with_db(
    DbId,
    fun(Db) ->
      pull(Db, Path, ContKey, Options)
    end
  ).

pull(DbId, Path, Options) when is_binary(DbId) ->
  barrel_db:with_db(
    DbId,
    fun(Db) ->
      pull(Db, Path, undefined, Options)
    end
  );
pull(_, _, _) ->
  erlang:error(badarg).

pull(#db{store=Store}=Db, Path, ContKey, Options) ->
  {ok, Snapshot} = rocksdb:snapshot(Store),
  try pull_1(Db, Path, ContKey, Options, Snapshot)
  after rocksdb:release_snapshot(Snapshot)
  end.


pull_1(#db{id=DbId, store=Store}, Path, ContKey, Options, Snapshot) ->
  Limit = maps:get(limit, Options, ?DEFAULT_MAX),
  WithHistory = maps:get(history, Options, last) =:= all,
  PathParts0 = decode_path(Path, []),
  {PathParts, Move} = case lists:last(PathParts0) of
                        <<"_", Last/binary >> ->
                          PathParts1 = lists:droplast(PathParts0) ++ [Last],
                          {PathParts1, prev};
                        _ ->
                          {PathParts0, next}
                      end,
  Prefix = encode_key(PathParts, order_by_key),
  {First, RangeFun} = case ContKey of
                        undefined ->
                          FirstParts = case maps:find(start_at, Options) of
                                     {ok, StartAt} ->
                                       PathParts ++ [StartAt];
                                     error ->
                                       PathParts
                                   end,
                          case Move of
                            prev ->
                              {
                                encode_key(FirstParts, 16#7FFFFFFFFFFFFFFF, order_by_key),
                                fun(K) -> match_prefix(K, Prefix) end
                              };
                            _ ->
                              {
                                encode_key(FirstParts, order_by_key),
                                fun(K) -> match_prefix(K, Prefix) end
                              }
                          end;
                        _ when Move =:= prev ->
                          {
                            ContKey,
                            fun(K) -> match_prefix(K, Prefix) end
                          };
                        _ ->
                          {
                            ContKey,
                            fun(K) -> match_prefix(K, Prefix) end
                          }
                      end,
  ReadOptions = [{snapshot, Snapshot}],
  Ref = erlang:make_ref(),
  %% wrapper function to retrieve the results from the iterator
  PullFun =
    fun(KeyBin, _, Acc) ->
      erlang:put(Ref, KeyBin),
      {Rid, _Pattern} = parse_rid(KeyBin, order_by_key),
      Res = rocksdb:get(Store, barrel_keys:res_key(Rid), ReadOptions),
      case Res of
        {ok, Bin} ->
          DocInfo = binary_to_term(Bin),
          RevId = maps:get(current_rev, DocInfo),
          Deleted = maps:get(deleted, DocInfo),
          DocId = maps:get(id, DocInfo),
          Rid = maps:get(rid, DocInfo),
          RevTree = maps:get(revtree, DocInfo),
          Changes = case WithHistory of
                      false -> [RevId];
                      true -> barrel_revtree:history(RevId, RevTree)
                    end,
          
          Row = pull_with_deleted(
            #{ <<"id">> => DocId,
               <<"rev">> => RevId,
               <<"changes">> => Changes },
            Deleted
          ),
          [Row | Acc];
        _Else ->
          Acc
      end
    end,
  Cont0 = #{ db => DbId, path => Path, options => Options },
  %% initialize the iterator
  {ok, Itr} = rocksdb:iterator(Store, ReadOptions),
  Next = fun() -> rocksdb:iterator_move(Itr, Move) end,
  Start = case Move of
            next -> rocksdb:iterator_move(Itr, First);
            prev -> rocksdb:iterator_move(Itr, {seek_for_prev, First})
          end,
  try do_pull(
    Start, Next, PullFun, [], RangeFun, Cont0, ContKey, Ref, Limit - 1)
  after safe_iterator_close(Itr)
  end.

do_pull(Start, Next, PullFun, Acc, Cmp, Cont0, ContKey, Ref, Limit) ->
  Result = pull_loop(Start, Next, PullFun, Acc, ContKey, Cmp, Limit),
  case Result of
    [] -> 'end_of_pull';
    _ ->
      ContKey2 = erlang:erase(Ref),
      Cont1 = Cont0#{ cont => ContKey2 },
      {Result, Cont1}
  end.
 

pull_loop({error, iterator_closed}, _Next, _WrapperFun, Acc, _ContKey, _Cmp, _Limit) ->
  Acc;
pull_loop({error, invalid_iterator}, _Next, _WrapperFun, Acc, _ContKey, _Cmp, _Limit) ->
  Acc;
pull_loop({ok, ContKey, _V}, Next, WrapperFun, Acc, ContKey, Cmp, Limit) ->
  pull_loop(Next(), Next, WrapperFun, Acc, ContKey, Cmp, Limit);
pull_loop({ok, K, V}, Next, WrapperFun, Acc0, ContKey, Cmp, Limit) ->
  case Cmp(K) of
    true ->
      case WrapperFun(K, V, Acc0) of
        Acc1 when Limit > 0 ->
          pull_loop(Next(), Next, WrapperFun, Acc1, ContKey, Cmp, Limit - 1);
        Acc1 -> Acc1
      end;
    false ->
      Acc0
  end.


pull_with_deleted(Row, true) -> Row#{<<"deleted">> => true};
pull_with_deleted(Row, _) -> Row.

walk(#db{store=Store}, Path, UserFun, AccIn, Options) ->
  {ok, Snapshot} = rocksdb:snapshot(Store),
  try do_walk(Store, Snapshot, Path, UserFun, AccIn, Options)
  after rocksdb:release_snapshot(Snapshot)
  end.

do_walk(Store, Snapshot, Path, UserFun, AccIn, Options0) ->
  Options1 = validate_options(Options0),
  PathParts = decode_path(Path, []),
  %% set fold options
  {MoveAction, First, RangeFun} = make_range(PathParts, Options1),
  OrderBy = maps:get(order_by, Options1, order_by_key),
  EqualTo = case maps:get(equal_to, Options1, undefined) of
              undefined -> undefined;
              Val -> barrel_index:short(Val)
            end,
  Limit = maps:get(limit, Options1, query_limit()),
  %% rocksdb options
  ReadOptions = [{snapshot, Snapshot}],
  %% wrapper function to retrieve the results from the iterator
  WrapperFun =
    fun(KeyBin, _, Acc) ->
      {Rid, Pattern} = parse_rid(KeyBin, OrderBy),
      case is_equal(EqualTo, Pattern) of
        true ->
          Res = rocksdb:get(Store, barrel_keys:res_key(Rid), ReadOptions),
          case Res of
            {ok, Bin} ->
              DocInfo = binary_to_term(Bin),
              {ok, Doc, Meta} = barrel_db:get_current_revision(DocInfo),
              UserFun(Doc, Meta, Acc);
            _Else ->
              {ok, Acc}
          end;
        false ->
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
      Start, Next, WrapperFun, AccIn, RangeFun, Limit - 1
    )
  after safe_iterator_close(Itr)
  end.

is_equal(undefined, _) -> true;
is_equal(Val, Val) -> true;
is_equal(_, _) -> false.

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

encode_key(Parts, order_by_key) ->
  barrel_keys:encode_parts(
    Parts,
    barrel_keys:prefix(idx_forward_path)
  );
encode_key(Parts, order_by_value) ->
  barrel_keys:encode_parts(
    lists:reverse(Parts),
    barrel_keys:prefix(idx_reverse_path)
  ).

encode_key(Parts, Seq, order_by_key) ->
  barrel_keys:forward_path_key(Parts, Seq);
encode_key(Parts, Seq, order_by_value) ->
  barrel_keys:reverse_path_key(Parts, Seq).


make_range_fun(Prefix, undefined, undefined, _, _) ->
  fun(K) -> match_prefix(K, Prefix) end;
make_range_fun(Prefix, undefined, Last, _, true) ->
  fun(K) -> match_prefix(K, Prefix) andalso K =< Last end;
make_range_fun(Prefix, undefined, Last, _, false) ->
  fun(K) -> match_prefix(K, Prefix) andalso K < Last end;
make_range_fun(Prefix, Start, undefined, true, _) ->
  fun(K) -> match_prefix(K, Prefix) andalso K >= Start end;
make_range_fun(Prefix, Start, undefined, false, _) ->
  fun(K) -> match_prefix(K, Prefix) andalso K > Start end;
make_range_fun(Prefix, Start, Last, true, true) ->
  fun(K) -> match_prefix(K, Prefix) andalso (K >= Start andalso K =< Last) end;
make_range_fun(Prefix, Start, Last, false, true) ->
  fun(K) -> match_prefix(K, Prefix) andalso (K > Start andalso K =< Last) end;
make_range_fun(Prefix, Start, Last, true, false) ->
  fun(K) -> match_prefix(K, Prefix) andalso (K >= Start andalso K < Last) end;
make_range_fun(Prefix, Start, Last, false, false) ->
  fun(K) -> match_prefix(K, Prefix) andalso (K > Start andalso K < Last) end.


make_range(Path, #{ move := prev }= Options) ->
  OrderBy = maps:get(order_by, Options, order_by_key),
  Prefix = encode_key(Path, OrderBy),
  {StartInclusive, First} = case Options of
                              #{ end_at := EndAt } ->
                                {true, encode_key(Path ++ [EndAt], 16#7FFFFFFFFFFFFFFF, OrderBy)};
                              #{ previous_to := PreviousTo } ->
                                {false, encode_key(Path ++ [PreviousTo], OrderBy)};
                              _ ->
                                {true, encode_key(Path, 16#7FFFFFFFFFFFFFFF, OrderBy)}
                            end,
  {LastInclusive, Last} = case Options of
                          #{ start_at := StartAt } ->
                            Start = encode_key(Path ++ [StartAt], 16#7FFFFFFFFFFFFFFF, OrderBy),
                            {true, Start};
                          #{ next_to := NextTo } ->
                            Start = encode_key(Path ++ [NextTo], 16#7FFFFFFFFFFFFFFF, OrderBy),
                            {false, Start};
                          _ ->
                            {true, undefined}
                        end,
  RangeFun = make_range_fun(Prefix, Last, First, LastInclusive, StartInclusive),
  {prev, First, RangeFun};
make_range(Path, #{ move := next }= Options) ->
  OrderBy = maps:get(order_by, Options, order_by_key),
  Prefix = encode_key(Path, OrderBy),
  {StartInclusive, First} = case Options of
                              #{ start_at := StartAt } ->
                                {true, encode_key(Path ++ [StartAt], OrderBy)};
                              #{ next_to := NextTo } ->
                                {false, encode_key(Path ++ [NextTo], 16#7FFFFFFFFFFFFFF, OrderBy)};
                              _ ->
                                {true, Prefix}
                            end,
  {LastInclusive, Last} = case Options of
                          #{ end_at := EndAt } ->
                            End = encode_key(Path ++ [EndAt], 16#7FFFFFFFFFFFFFF, OrderBy),
                            {true, End};
                          #{ previous_to := PreviousTo } ->
                            End = encode_key(Path ++ [PreviousTo], OrderBy),
                            {false, End};
                          _ ->
                            {true, undefined}
                        end,
  RangeFun = make_range_fun(Prefix, First, Last, StartInclusive, LastInclusive),
  {next, First, RangeFun}.

match_prefix(Bin, Prefix) ->
  L = byte_size(Prefix),
  case Bin of
    << Prefix:L/binary, _/binary >> -> true;
    _ -> false
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
  [Match | _] =  Rest,
  {Rid, Match}.


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


query_limit() ->
  application:get_env(barrel, query_limit, ?DEFAULT_MAX).