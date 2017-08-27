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

-module(barrel_lib).
-include_lib("syntax_tools/include/merl.hrl").

-export([
  to_atom/1,
  to_binary/1,
  to_hex/1,
  hex_to_binary/1,
  uniqid/0, uniqid/1,
  binary_join/2,
  load_config/2,
  pmap/2, pmap/3, pmap/4
]).

to_atom(V) when is_atom(V) -> V;
to_atom(V) when is_list(V) -> list_to_atom(V);
to_atom(V) when is_binary(V) ->
  case catch binary_to_existing_atom(V, utf8) of
    {'EXIT', _} -> binary_to_atom(V, utf8);
    B -> B
  end;
to_atom(_) -> error(badarg).

to_binary(V) when is_binary(V) -> V;
to_binary(V) when is_list(V) -> list_to_binary(V);
to_binary(V) when is_atom(V) -> atom_to_binary(V, utf8);
to_binary(V) when is_integer(V) -> integer_to_binary(V);
to_binary(_) -> error(badarg).

to_hex([]) -> [];
to_hex(Bin) when is_binary(Bin) ->
    << <<(to_digit(H)),(to_digit(L))>> || <<H:4,L:4>> <= Bin >>;
to_hex([H|T]) ->
    [to_digit(H div 16), to_digit(H rem 16) | to_hex(T)].

to_digit(N) when N < 10 -> $0 + N;
to_digit(N)             -> $a + N-10.

hex_to_binary(Bin) when is_binary(Bin) ->
  << <<(binary_to_integer( <<H, L>>, 16))>> || << H, L >> <= Bin >>.


uniqid() -> uniqid(binary).

uniqid(string)    -> uuid:uuid_to_string(uuid:get_v4(), standard);
uniqid(binary)    -> uuid:uuid_to_string(uuid:get_v4(), binary_standard);
uniqid(integer)   -> <<Id:128>> = uuid:get_v4(), Id;
uniqid(float)     -> <<Id:128>> = uuid:get_v4(), Id * 1.0;
uniqid(_) -> error(badarg).


-spec binary_join([binary()], binary()) -> binary().
binary_join([], _Sep) -> <<>>;
binary_join([Part], _Sep) -> to_binary(Part);
binary_join([Head|Tail], Sep) ->
  lists:foldl(
    fun (Value, Acc) -> <<Acc/binary, Sep/binary, (to_binary(Value))/binary>> end,
    to_binary(Head),
    Tail
  ).

%% @doc Utility that converts a given property list into a module that provides
%% constant time access to the various key/value pairs.
%%
%% Example:
%%
%%   load_config(store_config, [{backends, [{rocksdb_ram, barrel_rocksdb},
%%                                          {rocksdb_disk, barrel_rocksdb}]},
%%                              {data_dir, "/path/to_datadir"}]).
%%
%% creates the module store_config:
%%   store_config:backends(). => [{rocksdb_ram,barrel_rocksdb},{rocksdb_disk,barrel_rocksdb}]
%%   store_config:data_dir => "/path/to_datadir"
%%
-spec load_config(atom(), [{atom(), any()}]) -> ok.
load_config(Resource, Config) when is_atom(Resource), is_list(Config) ->
  Module = ?Q("-module(" ++ atom_to_list(Resource) ++ ")."),
  Functions = lists:foldl(fun({K, V}, Acc) ->
    [make_function(K,
      V)
      | Acc]
                          end,
    [], Config),
  Exported = [?Q("-export([" ++ atom_to_list(K) ++ "/0]).") || {K, _V} <-
    Config],
  Forms = lists:flatten([Module, Exported, Functions]),
  merl:compile_and_load(Forms, [verbose]),
  ok.

make_function(K, V) ->
  Cs = [?Q("() -> _@V@")],
  F = erl_syntax:function(merl:term(K), Cs),
  ?Q("'@_F'() -> [].").



%% @doc parallel map implementation
-spec pmap(F, List1) -> List2 when
  F :: fun(),
  List1 :: list(),
  List2 :: list().
pmap(Fun, List) -> pmap(Fun, List, length(List)).

%% @doc parallel map implementation with default timeout to 5000
-spec pmap(F, List1, Workers) -> List2 when
  F :: fun(),
  List1 :: list(),
  Workers :: non_neg_integer(), %% number of workers
  List2 :: list().
pmap(Fun, List, Workers) ->
  pmap(Fun, List, Workers, 5000).


-spec pmap(F, List1, Workers, Timeout) -> List2 when
  F :: fun(),
  List1 :: list(),
  Workers :: non_neg_integer(), %% number of workers
  Timeout :: non_neg_integer(), %% timeout
  List2 :: list().
pmap(Fun, List, NWorkers0, Timeout) ->
  NWorkers1 = erlang:min(length(List), NWorkers0),
  Parent = self(),
  Workers = [
    spawn_monitor(fun() -> pmap_worker(Parent, Fun) end)
    || _ <- lists:seq(1, NWorkers1)
  ],
  {Running, _} = lists:foldr(
    fun(E, {R, [{Pid, _}=W | Rest]}) ->
      Ref = erlang:make_ref(),
      Pid ! {Ref, E},
      {[Ref | R], Rest ++ [W]}
    end,
    {[], Workers},
    List
  ),
  Res = collect(Running, Timeout),
  [erlang:demonitor(MRef, [flush]) || {_Pid, MRef} <- Workers],
  Res.

collect([], _Timeout) -> [];
collect([Ref | Next], Timeout) ->
  receive
    {Ref, Res} ->
      [Res | collect(Next, Timeout)];
    {'DOWN', _MRef, process, _Pid, Reason} ->
      exit(Reason)
  after Timeout ->
    exit(pmap_timeout)
  end.

pmap_worker(Parent, Fun) ->
  receive
    {Ref, E} ->
      Parent ! {Ref, Fun(E)},
      pmap_worker(Parent, Fun)
  end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

pmap_test() ->
  L = [ 1, 2, 3, 4 ],
  Expected = [ 2, 4, 6, 8 ],
  Result = pmap(
    fun(E) -> E * 2 end,
    L,
    4
  ),
  ?assertEqual(Expected, Result).

-endif.
