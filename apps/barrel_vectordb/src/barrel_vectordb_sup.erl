%%%-------------------------------------------------------------------
%%% @doc barrel_vectordb top level supervisor
%%%
%%% This supervisor is the root of the barrel_vectordb OTP application.
%%% barrel_vectordb is an embedded library: individual stores are started
%%% on demand by the embedding application via
%%% {@link barrel_vectordb:start_link/1}. Its only static child is the
%%% name registry stores register in ({@link barrel_vectordb_registry}),
%%% which maps binary store names to pids without minting atoms.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the barrel_vectordb supervisor.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% @private
init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 60
    },
    Registry = #{
        id => barrel_vectordb_registry,
        start => {barrel_vectordb_registry, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker
    },
    StoreSup = #{
        id => barrel_vectordb_store_sup,
        start => {barrel_vectordb_store_sup, start_link, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor
    },
    {ok, {SupFlags, [Registry, StoreSup]}}.
