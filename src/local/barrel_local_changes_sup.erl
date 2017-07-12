%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. Jun 2017 15:32
%%%-------------------------------------------------------------------
-module(barrel_local_changes_sup).
-author("benoitc").

%% API
-export([
  start_link/0,
  start_consumer/4,
  stop_consumer/1
]).

-export([init/1]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_consumer(Owner, DbId, Since, Options) ->
  supervisor:start_child(
    ?MODULE,
    [Owner, DbId, Since, Options]
  ).

stop_consumer(Pid) ->
  supervisor:terminate_child(?MODULE, Pid).

init(_) ->
  Child = #{
    id => local_changes_consumer,
    start => {barrel_local_changes, start_link, []},
    restart => temporary,
    shutdown => infinity,
    type => worker,
    modules => [barrel_local_changes]
  },
  SupFlags = #{strategy => simple_one_for_one, intensity => 0, period => 1},
  {ok, {SupFlags, [Child]}}.