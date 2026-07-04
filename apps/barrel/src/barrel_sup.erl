%%%-------------------------------------------------------------------
%%% @doc barrel top level supervisor.
%%%
%%% barrel is an embeddable library: composed databases are opened on
%%% demand by the embedding application via {@link barrel:open/2}. The
%%% only static child is the record-indexer supervisor, which hosts one
%%% indexer per record-mode database.
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
    Children = [
        #{
            id => barrel_record_sup,
            start => {barrel_record_sup, start_link, []},
            type => supervisor,
            shutdown => infinity
        }
    ],
    {ok, {SupFlags, Children}}.
