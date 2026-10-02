%%%-------------------------------------------------------------------
%%% @doc Supervisor of the model processes shared through
%%% barrel_embed_pool. Children are temporary: the pool starts a new one
%%% on the next acquire after a crash.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_embed_pool_sup).

-behaviour(supervisor).

-export([start_link/0, start_server/1]).
-export([init/1]).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @private Child start: apply the start MFA the pool was given.
-spec start_server({module(), atom(), [term()]}) -> {ok, pid()} | {error, term()}.
start_server({M, F, A}) ->
    apply(M, F, A).

init([]) ->
    SupFlags = #{strategy => simple_one_for_one, intensity => 10, period => 10},
    Child = #{id => server,
              start => {?MODULE, start_server, []},
              restart => temporary,
              shutdown => 5000},
    {ok, {SupFlags, [Child]}}.
