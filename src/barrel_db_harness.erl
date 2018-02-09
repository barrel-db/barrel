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

-module(barrel_db_harness).
-author("benoitc").

%% API
-export([
  new/1,
  destroy/1,
  generate/2,
  timed_generate/2,
  fetch/2,
  fetch/3,
  timed_fetch/2,
  timed_fetch/3
]).


-include_lib("eunit/include/eunit.hrl").


new(Name) ->
  _ = barrel_storage:register_storage_provider(default, barrel_memory_storage, #{}),
  {ok, DbRef} = barrel:get_barrel(default, Name),
  DbRef.

destroy(Name) ->
  barrel:destroy_barrel(default, Name).


generate(DbRef, N)  ->
  C = erlang:system_info(schedulers),
  Workers = [spawn_worker(DbRef) || _I <- lists:seq(1, C)],
  try generate(N, DbRef, Workers, C, [])
  after stop_workers(Workers)
  end.

timed_generate(DbRef, N) ->
  ?debugTime("generating, docs", generate(DbRef, N)).

generate(0, _DbRef, _Workers, _C, Acc) ->
  lists:sort(
    fun(#{ <<"id">> := A}, #{ <<"id">> := B }) -> A < B end,
    Acc
  );
generate(N, DbRef, Workers, C, Acc) ->
  NWorkers= erlang:min(N, C),
  N2 = send_workers(lists:sublist(Workers, 1, NWorkers), write_doc, N),
  Acc2 = collect(NWorkers, DbRef, []) ++ Acc,
  generate(N2, DbRef, Workers, C, Acc2).


fetch(DbRef, N) ->
  C = erlang:system_info(schedulers),
  fetch(DbRef, N, C).

timed_fetch(DbRef, N) ->
  ?debugTime("fetching, docs", fetch(DbRef, N)).

timed_fetch(DbRef, N, C) ->
  ?debugTime("fetching, docs", fetch(DbRef, N, C)).


fetch(DbRef, N, C) ->
  Workers = [spawn_worker(DbRef) || _I <- lists:seq(1, C)],
  fetch(N, Workers, C, DbRef, []).

fetch(N, _Workers, _C, _DbRef, Acc) when N =< 0 ->
  Acc;
fetch(N, Workers, C, DbRef, Acc) ->
  NWorkers= erlang:min(N, C),
  N2 = send_workers(lists:sublist(Workers, 1, NWorkers), fetch_doc, N),
  Acc2 = collect(NWorkers, DbRef, []) ++ Acc,
  fetch(N2, Workers, C, DbRef, Acc2).


worker(Fetcher, DbRef) ->
  receive
    {fetch_doc, I} ->
      Resp = (catch barrel_db:fetch_doc(DbRef, docid(I), #{})),
      Fetcher ! {DbRef, Resp},
      worker(Fetcher, DbRef);
    {write_doc, I} ->
      Batch = [{create, #{ <<"id">> => docid(I) }}],
      [Doc] = barrel_db:write_changes(DbRef, Batch),
      Fetcher ! {DbRef, Doc},
      worker(Fetcher, DbRef);
    stop ->
      exit(normal)
  end.

spawn_worker(DbRef) ->
  Self = self(),
  spawn_link(fun() -> worker(Self, DbRef) end).

send_workers([W | Rest], Action, N) ->
  W ! {Action, N},
  send_workers(Rest, Action, N - 1);
send_workers([], _Action, N) ->
  N.

stop_workers(Workers) ->
  lists:foreach(
    fun(W) -> W ! stop end,
    Workers
  ).

collect(0, _DbRef, Acc) ->
  Acc;
collect(N, DbRef, Acc) ->
  receive
    {DbRef, Resp} ->
      collect(N - 1, DbRef, [Resp | Acc])
  end.

docid(I) -> << "doc-", (integer_to_binary(I))/binary >>.
  