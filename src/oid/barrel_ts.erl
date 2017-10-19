%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 13. Jul 2017 10:51
%%%-------------------------------------------------------------------
-module(barrel_ts).
-behaviouur(gen_server).
%% API
-export([
  start_link/1,
  get_last_ts/0
]).

-export([
  read_timestamp/0,
  write_timestamp/0,
  curr_time_millis/0
]).


-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

-define(DEFAULT_CONFIG, "BARREL_TS").
-define(DEFAULT_INTERVAL, 1000).

%% ==============================
%% PUBLIC API

start_link(LastTs) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [LastTs], []).

get_last_ts() ->
  gen_server:call(?MODULE, get_ts).

%% ==============================
%% gen server API

init([LastTs]) ->
  Interval = get_interval(),
  {ok, TRef} = timer:send_interval(Interval, persist),
  {ok, #{ tref => TRef, ts => LastTs}}.


handle_call(get_ts, _From, State) ->
  {Ts, NewState} = read_ts(State),
  {reply, {ok, Ts}, NewState};

handle_call(Msg, _From, State) ->
  {reply, {bad_call, Msg}, State}.

handle_info(persist, State = #{ ts := OldTs } ) ->
  Ts = curr_time_millis(),
  case (Ts >= OldTs) of
    true ->
      NewState = State#{ ts => Ts },
      ok = persist_ts(Ts),
      {noreply, NewState};
    false ->
      _ = lager:error(
            "~s: system running backwards, failing storing timestamp~n",
            [?MODULE_STRING]
           ),
      {stop, clock_running_backwards, State}
  end;

handle_info(_Msg, State) -> {noreply, State}.

handle_cast(_Msg, State) -> {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_, State, _) -> {ok, State}.


%% ==============================
%% internals

read_timestamp() ->
  case read_file(persist_file()) of
    {ok, Ts} -> {ok, Ts};
    {error, enoent} -> write_timestamp();
    Error ->
      Error
  end.

write_timestamp() ->
  write_timestamp(curr_time_millis()).

write_timestamp(Ts) ->
  ok = file:write_file(persist_file(), term_to_binary(Ts)),
  {ok, Ts}.

curr_time_millis() -> erlang:system_time(millisecond).


read_ts(#{ ts := Ts} = State) -> {Ts, State};
read_ts(State) ->
  case read_timestamp() of
    {ok, Ts} -> {Ts, State#{ ts => Ts }};
    Error -> Error
  end.

persist_ts(Ts) ->
  {ok, _Ts} = write_timestamp(Ts),
  ok.


persist_file() ->
  case init:get_argument(ts_file) of
    {ok, [[P]]} -> P;
    _ ->
      FileName = case application:get_env(barrel, ts_file) of
                   undefined -> ?DEFAULT_CONFIG;
                   {ok, P} -> P
                 end,
      FullPath = filename:join(barrel_store:data_dir(), FileName),
      ok = filelib:ensure_dir(FullPath),
      FullPath
  end.

read_file(Name) ->
  case file:read_file(Name) of
    {ok, Bin} ->
      Term = erlang:binary_to_term(Bin),
      {ok,  Term};
    Error ->
      Error
  end.


get_interval() ->
  application:get_env(barrel, persist_ts_interval, ?DEFAULT_INTERVAL).

%% ==============================
%% tests

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

persistent_clock_test() ->
  {ok, TS0} = write_timestamp(),
  {ok, TS1} = read_timestamp(),
  ?assert(TS0 =:= TS1).


-endif.
