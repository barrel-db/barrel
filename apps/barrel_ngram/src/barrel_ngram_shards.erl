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
-export([put_meta/2, get_meta/1, erase_meta/1, all_corpora/0]).
-export([put_pending_meta/2, get_pending_meta/1, erase_pending_meta/1,
         all_pending_corpora/0]).

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

%% @doc Every corpus with a currently-active (query-trusted) meta entry.
%% A single atomic point-in-time scan of the whole VM's persistent terms,
%% filtered by this module's own key shape. Used by `barrel_ngram_app'
%% to clear stale entries on `application:stop/1' (persistent_term
%% survives that, shard processes do not).
-spec all_corpora() -> [term()].
all_corpora() ->
    [C || {{?MODULE, C}, _} <- persistent_term:get()].

%%====================================================================
%% Pending corpus metadata (discovery-only, NEVER query-trusted)
%%====================================================================
%%
%% A SEPARATE cache from put_meta/get_meta/erase_meta above, under its
%% own key shape so all_corpora/0's filter never conflates the two.
%% Written by barrel_ngram_corpus_lifecycle as soon as a request's
%% config is reconciled (BEFORE any shard starts, or before a disk-fresh
%% request is durably committed), so close/1 -- which receives only a
%% corpus name, no data_dir -- can still discover and stop the real refs
%% of a corpus whose open was interrupted before full activation.
%%
%% NEVER consulted by live_meta/1 or any query-path call site: an entry
%% here describes a request that was reconciled, not necessarily one
%% that ever finished starting real, correctly-configured shards. Only
%% `get_meta/1' above (populated on full, verified success) is
%% query-trusted. See barrel_ngram_corpus_lifecycle's moduledoc for why
%% conflating the two is unsafe (a live-process collision scenario can
%% make a request-derived entry look valid by coincidence).

-define(PENDING_TAG, barrel_ngram_shards_pending).

-spec put_pending_meta(term(), map()) -> ok.
put_pending_meta(Corpus, Meta) ->
    persistent_term:put({?PENDING_TAG, Corpus}, Meta).

-spec get_pending_meta(term()) -> {ok, map()} | undefined.
get_pending_meta(Corpus) ->
    case persistent_term:get({?PENDING_TAG, Corpus}, undefined) of
        undefined -> undefined;
        Meta -> {ok, Meta}
    end.

-spec erase_pending_meta(term()) -> ok.
erase_pending_meta(Corpus) ->
    _ = persistent_term:erase({?PENDING_TAG, Corpus}),
    ok.

%% @doc Every corpus with a currently-pending meta entry. See
%% all_corpora/0 -- the pending-cache counterpart, swept alongside it by
%% `barrel_ngram_app:stop/1' so an interrupted open's entry does not
%% leak across an `application:stop/start' cycle.
-spec all_pending_corpora() -> [term()].
all_pending_corpora() ->
    [C || {{?PENDING_TAG, C}, _} <- persistent_term:get()].
