%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. Apr 2018 01:13
%%%-------------------------------------------------------------------
-module(barrel_db_stream_mgr).
-author("benoitc").
-behaviour(gen_server).

%% API
-export([
  subscribe/3, subscribe/4,
  unsubscribe/2, unsubscribe/3,
  next/3
]).

-export([start_link/0]).

-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2
]).

-include("barrel.hrl").

-dialyzer({nowarn_function, enqueue_stream/4}).

subscribe(Stream, Pid, Since) ->
  gen_server:call(?MODULE, {subscribe, Stream, Pid, Since}).

subscribe(Node, Stream, Pid, Since) ->
  gen_server:call({?MODULE, Node}, {subscribe, Stream, Pid, Since}).


unsubscribe(Stream, Pid) ->
  gen_server:call(?MODULE, {unsubscribe, Stream, Pid}).

unsubscribe(Node, Stream, Pid) ->
  gen_server:call({?MODULE, Node}, {unsubscribe, Stream, Pid}).


next(Stream, SubRef, LastSeq) ->
  gen_server:call(?MODULE, {next, Stream, SubRef, LastSeq}).


start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [],[]).

init([]) ->
  {ok, #{ streams => #{}, monitors => #{}}}.


%% subscribe to a databa

handle_call({next, Stream, SubRef, LastSeq},  {FromPid, _Tag},
            State = #{ streams := Streams0, monitors := Monitors0}) ->
  case maps:find(Stream, Streams0) of
    {ok, Subs0} ->
      {_, Sub} = lists:keyfind(SubRef, 1, Subs0),
      Subs1 = lists:keyreplace(SubRef, 1, Subs0, {SubRef, Sub#{ since => LastSeq }}),
      Streams1 = Streams0#{ Stream => Subs1 },
      Monitors1 = maps:filter(fun
                                (MRef, Pid) when Pid =:= FromPid ->
                                  erlang:demonitor(MRef, [flush]),
                                  false;
                                (_, _) ->
                                  true
                              end, Monitors0),
      {reply, ok, State#{ streams => Streams1, monitors => Monitors1 }};
    error ->
      {reply, {error, unknown_stream}, State}
  end;
handle_call({subscribe, Stream, Pid, Since},_From, State = #{ streams := Streams0 }) ->
  #{ barrel := Name, interval := Interval } = Stream,
  case barrel_db:is_barrel(Name) of
    true ->
      SubRef = erlang:make_ref(),
      {ok, T} = timer:send_interval(Interval, {trigger_fetch, Stream, SubRef}),
      Sub = #{ pid => Pid, timer => T,  since => Since},
      _ = enqueue_stream(Stream, SubRef, Pid, Since),
      %% we only care about unique streams there, so just store stream
      Streams1 = case maps:find(Stream, Streams0) of
                   {ok, Subscribers} ->
                     Streams0#{ Stream => [{SubRef, Sub} | Subscribers] };
                   error ->
                     Streams0#{ Stream => [{SubRef, Sub} ] }
                 end,
      {reply, ok, State#{ streams => Streams1 }};
    false ->
      {reply, {error, unknown_barrel}, State}
  end;
handle_call({unsubscribe, Stream, Pid}, _From, State = #{ streams := Streams0 }) ->
  case maps:find(Stream, Streams0) of
    {ok, Subs} ->
      case lists:filter(fun({_Ref, #{ pid := P }}) ->
                          P =:= Pid
                        end, Subs) of
        [] ->
          {reply, ok, State};
        [{SubRef, Sub}] ->
          #{ timer := T } = Sub,
          _ = timer:cancel(T),
          Subs1 = lists:keydelete(SubRef, 1, Subs),
          Streams1 = case lists:keydelete(SubRef, 1, Subs) of
                       [] -> maps:remove(Stream, Streams0);
                       Subs1 -> Streams0#{ Stream => Subs1 }
                     end,
          {reply, ok, State#{ streams => Streams1 }}
      end;
    error ->
      {reply, {error, unknown_stream}, State}
  end;
handle_call(_Msg, _From, State) -> {noreply, State}.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info({Sub = {_Stream, _SubRef}, {go, _Ref, Pid, _RelativeTime, _SojournTime}},
            State = #{ monitors := Monitors }) ->
  MRef = erlang:monitor(process, Pid),
  {noreply, State#{monitors => Monitors#{MRef => Sub}}};
handle_info({_Stream, {drop, _SojournTime}}, State) ->
  {noreply, State};

handle_info({trigger_fetch, Stream, SubRef}, State = #{ streams := Streams}) ->
  Subs = maps:get(Stream,Streams),
  {_, #{ pid := Pid, since := Since }} = lists:keyfind(SubRef, 1, Subs),
  _ = enqueue_stream(Stream, SubRef, Pid, Since),
  {noreply, State}.

-spec enqueue_stream(map(), reference(), pid(), term()) -> {await, any(), pid()} | {drop, 0}.
enqueue_stream(StreamRef, SubRef, Subscriber, Since) ->
  sbroker:async_ask(?db_stream_broker, {StreamRef, SubRef, Subscriber, Since},
                    {self(), {StreamRef, SubRef}}).