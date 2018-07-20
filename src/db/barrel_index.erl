%% Copyright 2018, Benoit Chesneau
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


-module(barrel_index).
-author("benoitc").

%% API
-export([
  diff/2,
  analyze/1,
  do_query/9
]).

-export([short/1]).

-export([query/5]).

-define(STRING_PRECISION, 100).

%% %% @doc get the operations maintenance to do between
%% 2 instances of a document
-spec diff(D1, D2) -> {Added, Removed} when
  D1 :: map() | list(), %% new instance of the document or list of new paths
  D2 :: map() | list(), %% old instance of the document or list of old paths
  Added :: list(), %% paths added
  Removed :: list(). %% paths removed
diff(D1, D2) when is_map(D1), is_map(D2) ->
  A1 = analyze(D1),
  A2 = analyze(D2),
  diff(A1, A2);
diff(A1, A2) when is_list(A1), is_list(A2) ->
  Removed = A2 -- A1,
  Added = A1 -- A2,
  {Added, Removed};
diff(_, _) ->
  erlang:error(badarg).

%% @doc analyze a document and yield paths to update
-spec analyze(D) -> [P] when
  D :: map(), %% document body
  P :: list(binary() | integer() | float() | atom()). %% list of path
analyze(D) ->
  maps:fold(
    fun
      (<<"_attachments">>, _V, Acc) ->
        Acc;
      (K, V, Acc) when is_map(V) ->
        object(V, [<<"$">>, K], [[<<"$">>, K] | Acc]);
      (K, V, Acc) when is_list(V) ->
        array(V, [<<"$">>, K], [[<<"$">>, K] | Acc]);
      (K, V, Acc) ->
        [[<<"$">>, K] , [<<"$">>, K, short(V)] | Acc]
    end,
    [],
    D
  ).

object(Obj, Root, Acc0) ->
  maps:fold(
    fun
      (K, V, Acc) when is_map(V) ->
        object(V, Root ++ [K], [Root ++ [K] | Acc]);
      (K, V, Acc) when is_list(V) ->
        array(V, Root ++ [K], [Root ++ [K] | Acc]);
      (K, V, Acc) ->
        [Root ++ [K], Root ++ [K, V] | Acc]
    end,
    Acc0,
    Obj
  ).

array(Arr, Root,  Acc) -> array(Arr, Root, 0, Acc).

array([Item | Rest], Root, Idx, Acc0) when is_map(Item) ->
  Acc1 = object(Item, Root ++ [Idx], [Root ++ [Idx] | Acc0]),
  array(Rest, Root, Idx + 1, Acc1);
array([Item | Rest], Root, Idx, Acc0) when is_list(Item) ->
  Acc1 = array(Item, Root ++ [Idx], [Root ++ [Idx] | Acc0]),
  array(Rest, Root, Idx + 1, Acc1);
array([Item | Rest], Root, Idx, Acc0) ->
  Acc1 = [Root ++ [Idx], Root ++ [Idx, Item] | Acc0 ],
  array(Rest, Root, Idx +1, Acc1);
array([], _Root, _Idx, Acc) ->
  Acc.

short(<< S:100/binary, _/binary >>) -> S;
short(S) when is_binary(S) -> S;
short(S) -> S.

query(Barrel, Path0, Fun, Acc, Options) ->
  %%ok = barrel_index_actor:refresh(Barrel),

  Path1 = normalize_path(Path0),
  DecodedPath = decode_path(Path1, []),
  OrderBy = maps:get(order_by, Options, order_by_key),
  Limit = limit(Options),
  IncludeDeleted = maps:get(include_deleted, Options, false),
  {Path, {StartInclusive, StartPath}, {EndInclusive, EndPath}} = case maps:find(equal_to, Options) of
                                 {ok, EqualTo} ->
                                   {DecodedPath ++ [EqualTo], {true, undefined}, {true, undefined}};
                                 error ->
                                   Start = start_at(Options, DecodedPath),
                                   End = end_at(Options, DecodedPath),
                                   {DecodedPath, Start, End}
                               end,
  {FoldFun, ByFun} = case OrderBy of
                       order_by_key ->
                         {fold_path, fun(P) -> P end};
                       order_by_value ->
                         {fold_reverse_path, fun lists:reverse/1}

                     end,
  Command = {query,
             FoldFun, ByFun(Path),
             {StartInclusive, ByFun(StartPath)}, {EndInclusive, ByFun(EndPath)}, Limit, IncludeDeleted, Fun, Acc},
  barrel_db:do_command(Barrel, Command).


do_query(FoldFun, Path, Start, End, Limit, IncludeDeleted, UserFun, UserAcc, {Mod, ModState}) ->
  Snapshot = Mod:get_snapshot(ModState),
  WrapperFun =
  fun
    (#{ id := DocId, rev := Rev, deleted := true }, Acc) when IncludeDeleted =:= true ->
      case  Mod:get_revision(DocId, Rev, Snapshot) of
        {ok, Doc} ->
          UserFun(Doc#{ <<"_rev">> => Rev, <<"_deleted">> => true }, Acc);
        {error, not_found} ->
          UserFun(#{ <<"id">> => DocId, <<"_rev">> => Rev, <<"_deleted">> => true }, Acc)
      end;
    (#{ deleted := true }, _Acc)  ->
      skip;
    (#{ id := DocId, rev := Rev }, Acc) ->
      {ok, Doc} = Mod:get_revision(DocId, Rev, Snapshot),
      UserFun(Doc#{ <<"_rev">> => Rev }, Acc)
  end,
  Mod:FoldFun(Path, Start, End, Limit, WrapperFun, UserAcc, Snapshot).


start_at(#{ start_at := Start }, Path) -> {true, Path ++ [Start]};
start_at(#{ next_to := Start }, Path) -> {false, Path ++ [Start]};
start_at(_, _) -> {true, undefined}.

end_at(#{ end_at := End }, Path) -> {true, Path ++ [End]};
end_at(#{ previous_to := End }, Path) -> {false, Path ++ [End]};
end_at(_, _) -> {true, undefined}.

limit(#{ limit_to_first := L }) -> {limit_to_first, L};
limit(#{ limit_to_last:= L }) -> {limit_to_last, L};
limit(_) -> undefined.

normalize_path(<<>>) -> <<"/id">>;
normalize_path(<<"/">>) -> <<"/id">>;
normalize_path(<< "/", _/binary >> = P) ->  P;
normalize_path(P) ->  <<"/", P/binary >>.

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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(doc,
  #{
    <<"a">> => 1,
    <<"b">> => <<"2">>,
    <<"c">> => #{
      <<"a">> => 1,
      <<"b">> => [<<"a">>, <<"b">>, <<"c">>],
      <<"c">> => #{ <<"a">> => 1, <<"b">> => 2}
    },
    <<"d">> => [<<"a">>, <<"b">>, <<"c">>],
    <<"e">> => [#{<<"a">> => 1}, #{ <<"b">> => 2}]
  }).

-define(doc2,
  #{
    <<"a">> => 1,
    <<"text">> => <<" Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Aenean commodo ligula eget dolor. Aenean massa. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Donec quam felis, ultricies nec, pellentesque eu, pretium quis, sem. Nulla consequat massa quis enim. Donec pede justo, fringilla vel, aliquet nec, vulputate eget, arcu. In enim justo, rhoncus ut, imperdiet a, venenatis vitae, justo. Nullam dictum felis eu pede mollis pretium. Integer tincidunt. Cras dapibus. Vivamus elementum semper nisi. Aenean vulputate eleifend tellus. Aenean leo ligula, porttitor eu, consequat vitae, eleifend ac, enim. Aliquam lorem ante, dapibus in, viverra quis, feugiat a, tellus. Phasellus viverra nulla ut metus varius laoreet. Quisque rutrum. Aenean imperdiet. Etiam ultricies nisi vel augue. Curabitur ullamcorper ultricies nisi. Nam eget dui. Etiam rhoncus. Maecenas tempus, tellus eget condimentum rhoncus, sem quam semper libero, sit amet adipiscing sem neque sed ipsum. Nam quam nunc, blandit vel, luctus pulvinar, hendrerit id, lorem. Maecenas nec odio et ante tincidunt tempus. Donec vitae sapien ut libero venenatis faucibus. Nullam quis ante. Etiam sit amet orci eget eros faucibus tincidunt. Duis leo. Sed fringilla mauris sit amet nibh. Donec sodales sagittis magna. Sed consequat, leo eget bibendum sodales, augue velit cursus nunc, quis gravida magna mi a libero. Fusce vulputate eleifend sapien. Vestibulum purus quam, scelerisque ut, mollis sed, nonummy id, metus. Nullam accumsan lorem in dui. Cras ultricies mi eu turpis hendrerit fringilla. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; In ac dui quis mi consectetuer lacinia. Nam pretium turpis et arcu. Duis arcu tortor, suscipit eget, imperdiet nec, imperdiet iaculis, ipsum. Sed aliquam ultrices mauris. Integer ante arcu, accumsan a, consectetuer eget, posuere ut, mauris. Praesent adipiscing. Phasellus ullamcorper ipsum rutrum nunc. Nunc nonummy metus. Vestibulum volutpat pretium libero. Cras id dui. Aenean ut eros et nisl sagittis vestibulum. Nullam nulla eros, ultricies sit amet, nonummy id, imperdiet feugiat, pede. Sed lectus. Donec mollis hendrerit risus. Phasellus nec sem in justo pellentesque facilisis. Etiam imperdiet imperdiet orci. Nunc nec neque. Phasellus leo dolor, tempus non, auctor et, hendrerit quis, nisi. Curabitur ligula sapien, tincidunt non, euismod vitae, posuere imperdiet, leo. Maecenas malesuada. Praesent congue erat at massa. Sed cursus turpis vitae tortor. Donec posuere vulputate arcu. Phasellus accumsan cursus velit. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Sed aliquam, nisi quis porttitor congue, elit erat euismod orci, ac placerat dolor lectus quis orci. Phasellus consectetuer vestibulum elit. Aenean tellus metus, bibendum sed, posuere ac, mattis non, nunc. Vestibulum fringilla pede sit amet augue. In turpis. Pellentesque posuere. Praesent turpis. Aenean posuere, tortor sed cursus feugiat, nunc augue blandit nunc, eu sollicitudin urna dolor sagittis lacus. Donec elit libero, sodales nec, volutpat a, suscipit non, turpis. Nullam sagittis. Suspendisse pulvinar, augue ac venenatis condimentum, sem libero volutpat nibh, nec pellentesque velit pede quis nunc. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Fusce id purus. Ut varius tincidunt libero. Phasellus dolor. Maecenas vestibulum mollis diam. Pellentesque ut neque. Pellentesque habitant morbi tristique senectus et netus et">>
  }).

analyze_test() ->
  Expected = [
               [<<"$">>, <<"a">>],
               [<<"$">>, <<"a">>, 1],
               [<<"$">>, <<"b">>],
               [<<"$">>, <<"b">>, <<"2">>],
               [<<"$">>, <<"c">>],
               [<<"$">>, <<"c">>, <<"a">>],
               [<<"$">>, <<"c">>, <<"a">>, 1],
               [<<"$">>, <<"c">>, <<"b">>],
               [<<"$">>, <<"c">>, <<"b">>, 0],
               [<<"$">>, <<"c">>, <<"b">>, 0, <<"a">>],
               [<<"$">>, <<"c">>, <<"b">>, 1],
               [<<"$">>, <<"c">>, <<"b">>, 1, <<"b">>],
               [<<"$">>, <<"c">>, <<"b">>, 2],
               [<<"$">>, <<"c">>, <<"b">>, 2, <<"c">>],
               [<<"$">>, <<"c">>, <<"c">>],
               [<<"$">>, <<"c">>, <<"c">>, <<"a">>],
               [<<"$">>, <<"c">>, <<"c">>, <<"a">>, 1],
               [<<"$">>, <<"c">>, <<"c">>, <<"b">>],
               [<<"$">>, <<"c">>, <<"c">>, <<"b">>, 2],
               [<<"$">>, <<"d">>],
               [<<"$">>, <<"d">>, 0],
               [<<"$">>, <<"d">>, 0, <<"a">>],
               [<<"$">>, <<"d">>, 1],
               [<<"$">>, <<"d">>, 1, <<"b">>],
               [<<"$">>, <<"d">>, 2],
               [<<"$">>, <<"d">>, 2, <<"c">>],
               [<<"$">>, <<"e">>],
               [<<"$">>, <<"e">>, 0],
               [<<"$">>, <<"e">>, 0, <<"a">>],
               [<<"$">>, <<"e">>, 0, <<"a">>, 1],
               [<<"$">>, <<"e">>, 1],
               [<<"$">>, <<"e">>, 1, <<"b">>],
               [<<"$">>, <<"e">>, 1, <<"b">>, 2]
             
             ],
  ?assertEqual(Expected, lists:sort(analyze(?doc))).

analyze_long_text_test() ->
  A = lists:sort(analyze(?doc2)),
  [_, P1, _, P2] = A,
  ?assertEqual([<<"$">>, <<"a">>, 1], P1),
  [<<"$">>, <<"text">>, PartialText] = P2,
  ?assertEqual(100, byte_size(PartialText)).


diff_test() ->
  Old = #{ <<"a">> => 1,
           <<"b">> => [0, 1],
           <<"c">> => #{ <<"a">> => 1}},
  New = #{ <<"a">> => 1,
           <<"b">> => [0, 1, 3],
           <<"d">> => #{ <<"a">> => 1}},
  {Added, Removed} = diff(New, Old),
  ?assertEqual([[<<"$">>,<<"c">>], [<<"$">>,<<"c">>,<<"a">>], [<<"$">>,<<"c">>,<<"a">>,1]], lists:sort(Removed)),
  ?assertEqual([[<<"$">>,<<"b">>,2], [<<"$">>,<<"b">>,2,3],
                [<<"$">>,<<"d">>],[<<"$">>,<<"d">>,<<"a">>],[<<"$">>,<<"d">>,<<"a">>,1]], lists:sort(Added)).

-endif.
