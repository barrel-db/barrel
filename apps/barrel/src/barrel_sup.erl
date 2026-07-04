%%%-------------------------------------------------------------------
%%% @doc barrel top level supervisor.
%%%
%%% barrel is an embeddable library: composed databases are opened on
%%% demand by the embedding application via {@link barrel:open/2}. The
%%% static children are the record-indexer supervisor (one indexer per
%%% record-mode database) and the live-query supervisor (one worker
%%% per SUBSCRIBE statement).
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
        },
        #{
            id => barrel_bql_live_sup,
            start => {barrel_bql_live_sup, start_link, []},
            type => supervisor,
            shutdown => infinity
        }
    ],
    {ok, {SupFlags, Children}}.
