%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. May 2018 22:17
%%%-------------------------------------------------------------------
-module(barrel_db_reader_sup).
-author("benoitc").

%% API
-export([start_link/2]).

-export([init/1]).

-include("barrel.hrl").

start_link(Name, NumReaders) ->
  supervisor:start_link({via, gproc, ?db_reader_sup(Name)}, ?MODULE, [Name, NumReaders]).


init([Name, NumReaders]) ->
  Specs = [
    #{id => {Name, Id},
      start => {barrel_db_reader, start_link, [Name, Id]},
      restart => permanent,
      shutdown => 5000,
      type => worker,
      modules => [barrel_db_reader ]} || Id <- lists:seq(1, NumReaders)
  ],
  SupFlags = #{ strategy => one_for_one, intensity => 10, period => 10 },
  {ok, {SupFlags, Specs}}.