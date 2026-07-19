%%%-------------------------------------------------------------------
%%% @doc Rendezvous (HRW) sharding for a corpus.
%%%
%%% Maps a document key to exactly one of N shards, so each shard indexes
%%% only its slice and a document's entry never moves (the key is stable).
%%% Also holds the per-corpus metadata (shard count + config) the query
%%% planner needs before it can address the shards.
%%%
%%% A shard is identified by a `Ref': the corpus name when `N =:= 1'
%%% (unchanged single-shard path) and `{Corpus, I}' for `N > 1'.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_shards).

-export([shard_for/2, refs/2]).
-export([put_meta/2, get_meta/1, erase_meta/1]).

-type ref() :: term().
-export_type([ref/0]).

%% @doc The shard index owning `Key' out of `N' shards: the index with the
%% highest `phash2({I, Key})', tie-broken by the lowest index.
-spec shard_for(binary(), pos_integer()) -> non_neg_integer().
shard_for(_Key, 1) ->
    0;
shard_for(Key, N) ->
    {_Hash, NegI} =
        lists:max([{erlang:phash2({I, Key}), -I} || I <- lists:seq(0, N - 1)]),
    -NegI.

%% @doc The shard refs for a corpus of `N' shards, index order.
-spec refs(term(), pos_integer()) -> [ref()].
refs(Corpus, 1) ->
    [Corpus];
refs(Corpus, N) ->
    [{Corpus, I} || I <- lists:seq(0, N - 1)].

%%====================================================================
%% Corpus metadata (persistent_term: set at open, read by the planner)
%%====================================================================

-spec put_meta(term(), map()) -> ok.
put_meta(Corpus, Meta) ->
    persistent_term:put({?MODULE, Corpus}, Meta).

-spec get_meta(term()) -> {ok, map()} | undefined.
get_meta(Corpus) ->
    case persistent_term:get({?MODULE, Corpus}, undefined) of
        undefined -> undefined;
        Meta -> {ok, Meta}
    end.

-spec erase_meta(term()) -> ok.
erase_meta(Corpus) ->
    _ = persistent_term:erase({?MODULE, Corpus}),
    ok.
