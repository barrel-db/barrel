%%%-------------------------------------------------------------------
%%% @doc Dynamic supervisor for ngram shard processes.
%%%
%%% `simple_one_for_one': one child per corpus shard. M1 runs a single
%%% shard per corpus; the sharding milestone fans a corpus over several
%%% shard children without changing this supervisor.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_shard_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([start_shard/2, stop_shard/1]).
-export([init/1]).

-define(SERVER, ?MODULE).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% @doc Start a shard for a corpus. Returns the existing shard if one is
%% already running for the corpus.
-spec start_shard(term(), map()) -> {ok, pid()} | {error, term()}.
start_shard(Corpus, Config) ->
    case supervisor:start_child(?SERVER, [Corpus, Config]) of
        {ok, Pid} -> {ok, Pid};
        {error, {already_started, Pid}} -> {ok, Pid};
        {error, _} = Error -> Error
    end.

%% @doc Stop the shard for a corpus.
-spec stop_shard(term()) -> ok | {error, not_found}.
stop_shard(Corpus) ->
    case barrel_ngram_registry:whereis_name({shard, Corpus}) of
        undefined -> {error, not_found};
        Pid -> supervisor:terminate_child(?SERVER, Pid)
    end.

-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 5,
        period => 60
    },

    Shard = #{
        id => barrel_ngram_shard,
        start => {barrel_ngram_shard, start_link, []},
        restart => temporary,
        shutdown => 5000,
        type => worker,
        modules => [barrel_ngram_shard]
    },

    {ok, {SupFlags, [Shard]}}.
