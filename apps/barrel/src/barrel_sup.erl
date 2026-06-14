%%%-------------------------------------------------------------------
%%% @doc barrel top level supervisor.
%%%
%%% barrel is an embeddable library: composed databases are opened on
%%% demand by the embedding application via {@link barrel:open/2}, so this
%%% supervisor has no static children of its own.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% @private
init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 60
    },
    {ok, {SupFlags, []}}.
