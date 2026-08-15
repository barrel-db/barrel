%%%-------------------------------------------------------------------
%%% @doc Dynamic supervisor for one-shot corpus lifecycle coordinators.
%%%
%%% `simple_one_for_one': one coordinator per in-flight `open'/`close'.
%%% `restart => temporary' -- a coordinator is one-shot by design, it
%%% must never be auto-restarted by its own supervisor after its
%%% `init/1'/`handle_continue/2' returns `{stop, {shutdown, _}}' or
%%% crashes.
%%%
%%% Supervised (not a raw, unlinked `gen_server:start/4') specifically so
%%% a `barrel_ngram_registry' crash can forcibly clear any orphaned
%%% coordinator via ordinary OTP supervision -- see `barrel_ngram_sup''s
%%% `rest_for_one' ordering.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_corpus_lifecycle_sup).

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
        strategy => simple_one_for_one,
        intensity => 5,
        period => 60
    },

    Coordinator = #{
        id => barrel_ngram_corpus_lifecycle,
        start => {barrel_ngram_corpus_lifecycle, start_link, []},
        restart => temporary,
        shutdown => 5000,
        type => worker,
        modules => [barrel_ngram_corpus_lifecycle]
    },

    {ok, {SupFlags, [Coordinator]}}.
