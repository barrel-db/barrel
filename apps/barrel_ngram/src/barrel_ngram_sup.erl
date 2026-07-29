%%%-------------------------------------------------------------------
%%% @doc barrel_ngram top-level supervisor.
%%%
%%% Owns the name registry and the dynamic shard supervisor. Isolation
%%% from the rest of the umbrella is this subtree: a shard crash never
%%% escapes it.
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
        strategy => one_for_one,
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

    {ok, {SupFlags, [Registry, ShardSup]}}.
