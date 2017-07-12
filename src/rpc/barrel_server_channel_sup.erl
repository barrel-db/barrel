%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 27. Jun 2017 04:45
%%%-------------------------------------------------------------------
-module(barrel_server_channel_sup).
-author("benoitc").

-behaviour(supervisor).

%% API
-export([
  start_link/0,
  start_channel/2
]).

-export([init/1]).


start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_channel(Reader, Writer) ->
  supervisor:start_child(?MODULE, [Reader, Writer]).

init(_) ->
  Channel_spec = child_spec(),
  SupFlags = simple_one_for_one_flags(),
  {ok, {SupFlags, [Channel_spec]}}.

simple_one_for_one_flags() ->
  #{ strategy => simple_one_for_one, intensity => 0, period => 1}.


child_spec() ->
  #{
    id => channel,
    start => {barrel_server_channel, start_link, []},
    restart => temporary,
    shutdown => infinity,
    type => supervisor,
    modules => [barrel_channel]
  }.