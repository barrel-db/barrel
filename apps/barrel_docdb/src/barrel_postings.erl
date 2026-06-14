%% @doc Postings Resource API for barrel_docdb
%%
%% This module wraps the rocksdb postings_* resource API for efficient
%% posting list operations. The resource API parses posting list binaries
%% once and provides O(1) or O(log n) lookups for repeated access.
%%
%% Key benefits:
%% - Parse once, lookup many times (~0.1 us per lookup)
%% - Native set operations (intersection, union, difference)
%% - Pre-sorted keys in V2 format
%% - Roaring64 bitmap for fast existence checks
%%
%% @end
-module(barrel_postings).

-export([
    %% Resource lifecycle
    open/1,
    to_binary/1,

    %% Lookups
    contains/2,
    bitmap_contains/2,

    %% Metadata
    count/1,
    keys/1,

    %% Set operations (return {ok, Resource} or error)
    intersection/2,
    union/2,
    difference/2,
    intersect_all/1,
    intersection_count/2
]).

-export_type([postings/0]).

-opaque postings() :: reference().

%% @doc Parse a posting list binary into a resource for fast repeated lookups.
%% The binary must be a valid V1 or V2 posting list format.
-spec open(binary()) -> {ok, postings()} | {error, term()}.
open(Binary) when is_binary(Binary) ->
    rocksdb:postings_open(Binary).

%% @doc Convert a postings resource back to binary format.
-spec to_binary(postings()) -> binary().
to_binary(Postings) ->
    rocksdb:postings_to_binary(Postings).

%% @doc Check if a key exists in the posting list (exact match).
%% Uses O(log n) binary search on sorted keys.
-spec contains(postings(), binary()) -> boolean().
contains(Postings, Key) when is_binary(Key) ->
    rocksdb:postings_contains(Postings, Key).

%% @doc Check if a key exists using the embedded roaring bitmap.
%% O(1) hash-based lookup, may have rare false positives.
%% Use contains/2 for exact checks when false positives are unacceptable.
-spec bitmap_contains(postings(), binary()) -> boolean().
bitmap_contains(Postings, Key) when is_binary(Key) ->
    rocksdb:postings_bitmap_contains(Postings, Key).

%% @doc Get the number of active keys in the posting list.
-spec count(postings()) -> non_neg_integer().
count(Postings) ->
    rocksdb:postings_count(Postings).

%% @doc Get all active keys in lexicographic (sorted) order.
%% Keys are pre-sorted in V2 format, so this is O(n).
-spec keys(postings()) -> [binary()].
keys(Postings) ->
    rocksdb:postings_keys(Postings).

%% @doc Compute intersection (AND) of two posting lists.
%% Both arguments can be postings resources or binaries.
-spec intersection(postings() | binary(), postings() | binary()) ->
    {ok, postings()} | {error, term()}.
intersection(A, B) ->
    rocksdb:postings_intersection(A, B).

%% @doc Compute union (OR) of two posting lists.
%% Both arguments can be postings resources or binaries.
-spec union(postings() | binary(), postings() | binary()) ->
    {ok, postings()} | {error, term()}.
union(A, B) ->
    rocksdb:postings_union(A, B).

%% @doc Compute difference (A - B) of two posting lists.
%% Returns keys in A that are not in B.
-spec difference(postings() | binary(), postings() | binary()) ->
    {ok, postings()} | {error, term()}.
difference(A, B) ->
    rocksdb:postings_difference(A, B).

%% @doc Compute intersection of multiple posting lists.
%% Efficiently processes smallest lists first.
%% All list elements can be postings resources or binaries.
-spec intersect_all([postings() | binary()]) -> {ok, postings()} | {error, term()}.
intersect_all(Postings) when is_list(Postings), length(Postings) > 0 ->
    rocksdb:postings_intersect_all(Postings);
intersect_all([]) ->
    {error, empty_list}.

%% @doc Get the cardinality of intersection without materializing the result.
%% Uses roaring bitmap for fast computation.
-spec intersection_count(postings() | binary(), postings() | binary()) ->
    non_neg_integer().
intersection_count(A, B) ->
    rocksdb:postings_intersection_count(A, B).
