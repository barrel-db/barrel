%%%-------------------------------------------------------------------
%%% @doc barrel_ngram top-level supervisor.
%%%
%%% Owns the name registry, the dynamic shard supervisor, and the
%%% corpus-lifecycle coordinator supervisor. Isolation from the rest of
%%% the umbrella is this subtree: a shard crash never escapes it.
%%%
%%% `rest_for_one', with children ordered `Registry', `ShardSup',
%%% `CorpusLifecycleSup': a `Registry' crash (a fresh, empty ETS table on
%%% restart) cascades to force-terminate and freshly restart BOTH
%%% `ShardSup' (so no now-unregistered shard can collide with a
%%% duplicate started against the same directory later) AND
%%% `CorpusLifecycleSup' (so no orphaned lifecycle coordinator can keep
%%% mutating shards/metadata after losing its `via'-name mutex) --
%%% neither is possible under a plain `one_for_one', which would restart
%%% only the registry itself and leave both siblings' state stale.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    SupFlags = #{
        strategy => rest_for_one,
        intensity => 5,
        period => 60
    },

    Registry = #{
        id => barrel_ngram_registry,
        start => {barrel_ngram_registry, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [barrel_ngram_registry]
    },

    ShardSup = #{
        id => barrel_ngram_shard_sup,
        start => {barrel_ngram_shard_sup, start_link, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [barrel_ngram_shard_sup]
    },

    CorpusLifecycleSup = #{
        id => barrel_ngram_corpus_lifecycle_sup,
        start => {barrel_ngram_corpus_lifecycle_sup, start_link, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [barrel_ngram_corpus_lifecycle_sup]
    },

    {ok, {SupFlags, [Registry, ShardSup, CorpusLifecycleSup]}}.
