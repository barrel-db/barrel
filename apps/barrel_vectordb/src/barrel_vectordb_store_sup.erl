%%%-------------------------------------------------------------------
%%% @doc Dynamic supervisor for vector store processes.
%%%
%%% Mirrors {@link barrel_db_sup} for the docdb side: stores start under a
%%% simple_one_for_one supervisor (temporary children) instead of being
%%% linked to the caller, so a composed database opened on behalf of a
%%% manager is not tied to the transient process that opened it. The store
%%% is found by name through {@link barrel_vectordb_registry}; a crash of a
%%% temporary child is not auto-restarted, the owner reopens on next use
%%% (same contract as the docdb server).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_store_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([start_store/1]).

-export([init/1]).

-define(SERVER, ?MODULE).

%% @doc Start the store supervisor.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% @doc Start a vector store under the supervisor (parent is the
%% supervisor, not the caller). `Config' is the {@link barrel_vectordb}
%% store config, including `name'.
-spec start_store(map()) -> {ok, pid()} | {error, term()}.
start_store(Config) ->
    supervisor:start_child(?SERVER, [Config]).

-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    SupFlags = #{strategy => simple_one_for_one,
                 intensity => 5,
                 period => 60},
    %% start_link/1 extracts `name' from the config and start_links the
    %% server; called from here it links the store to this supervisor.
    Store = #{id => barrel_vectordb_store,
              start => {barrel_vectordb, start_link, []},
              restart => temporary,
              shutdown => 5000,
              type => worker,
              modules => [barrel_vectordb_server]},
    {ok, {SupFlags, [Store]}}.
