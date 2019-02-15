%%
%% Copyright (C) 2017 Benoit Chesneau
%%

%% @doc general abstraction to maintain an LRU
%%
%% TODO: optimize using a simple nif? appending is normally slow but no measures yet.

-module(barrel_lrulist).
-author("benoitc").

%% API
-export([
  new/0,
  push/2,
  pop/1,
  touch/2,
  drop/2,
  is_empty/1

]).

-type lrulist() :: list().

-export_types([lrulist/0]).


%% @doc Create an LRU
-spec new() -> lrulist().
new() -> [].

%% @doc Push element into the LRU at the cold end
-spec push(LruElement :: term(), Lru1 :: lrulist()) -> Lru2 :: lrulist().
push(LruElement, LruList) ->
  case lists:member(LruElement, LruList) of
    true -> LruList;
    false -> LruList ++ [LruElement]
  end.


%% @doc Evict an element from the LRU
-spec pop( Lru1 :: lrulist()) -> empty | {ok, LruElement :: term(), Lru2 :: lrulist()}.
pop([]) -> empty;
pop([ El | Rest] ) -> {ok, El, Rest}.

%% @doc Move the element from the front of the list to the back of the list
-spec touch(LruElement :: term(), Lru1 :: lrulist()) -> Lru2 :: lrulist().
touch(LruElement, [LruElement|Rest]) ->
  Rest ++ [LruElement];
touch(LruElement, [H|Rest]) ->
  [H|touch(LruElement, Rest)];
touch(LruElement, []) ->
  [LruElement].

%% @doc Drop the element from the list
-spec drop(LruElement :: term(), Lru1 :: lrulist()) -> Lru2 :: lrulist().
drop(LruElement, LruList) ->
  lists:delete(LruElement, LruList).

%% @doc Check if the LRU is empty
-spec is_empty(LruList :: lrulist()) -> boolean().
is_empty([]) -> true;
is_empty(_) -> false.
