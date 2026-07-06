%%%-------------------------------------------------------------------
%%% @doc Top supervisor for the agent layer. Spaces are plain barrel
%%% databases owned by the facade's lifecycle manager; the only static
%%% child (added with sessions) is the janitor that garbage-collects
%%% orphaned session messages.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_spaces_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @private
init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 60
    },
    {ok, {SupFlags, []}}.
