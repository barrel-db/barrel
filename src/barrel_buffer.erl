-module(barrel_buffer).

-export([enqueue/2,
         dequeue/1, dequeue/2,
         infos/1]).

-export([close/1]).
-export([start_link/1]).


-export([init/1,
         handle_call/3,
         handle_cast/2,
         terminate/2]).

-define(MAX, 1 bsl 32 - 1).

-type buffer_options() :: [{max_size, non_neg_integer()} |
                          {max_items, non_neg_integer()} ].

-type buffer_infos() :: #{num := non_neg_integer(),
                         size := non_neg_integer(),
                         max_size := non_neg_integer(),
                         max_items := non_neg_integer()}.

-spec enqueue(pid(), term()) -> ok.
enqueue(Buffer, Item)  ->
  gen_server:call(Buffer, {enqueue, Item, erlang_term:byte_size(Item)}, infinity).

-spec dequeue(pid()) -> {ok, list()} | closed.
dequeue(Buffer) ->
  dequeue(Buffer, ?MAX).

-spec dequeue(pid(), non_neg_integer()) -> {ok, list()} | closed.
dequeue(Buffer, Max) ->
  try gen_server:call(Buffer, {dequeue, Max}, infinity)
  catch
    _:_ -> closed
  end.

-spec infos(pid()) -> buffer_infos().
infos(Buffer) ->
  gen_server:call(Buffer, infos).

-spec close(pid()) -> ok.
close(Buffer) ->
  gen_server:cast(Buffer, close).

-spec start_link(buffer_options()) -> {ok, pid()} | {error, any()} | ignore.
start_link(Options) ->
  gen_server:start_link(?MODULE, Options, []).

%% ---
%% gen_server callbacks

init(Options) ->
  InitState = #{ queue => queue:new(),
                 size => 0,
                 num => 0,
                 max_size => proplists:get_value(max_size, Options),
                 max_items => proplists:get_value(max_items, Options),
                 producers => [],
                 consumers => [],
                 need_close => false},
  {ok, InitState}.

handle_call({enqueue, Item, Size}, From,  #{ consumers := []} = State) ->

  #{ queue := Q0,
     size := Size0,
     num := Num,
     max_size := MaxSize,
     max_items := MaxItems,
     producers := Producers,
     consumers := []} = State,

  Q1 = queue:in({Item, Size}, Q0),
  Num1 = Num +1,
  Size1 = Size0 + Size,

  if
    Size1 >= MaxSize; Num1 >= MaxItems ->
      {noreply, State#{ queue => Q1,
                        size => Size1,
                        num => Num1,
                        producers => [From | Producers] }};
    true ->
      {reply, ok, State#{ queue => Q1,
                          size => Size1,
                          num => Num1}}
  end;

handle_call({enqueue, Item, _Size}, _From,  #{ consumers := Consumers0} = State) ->
  %% an item from a buffer can only be sent to one consumer
  [C | Consumers1] = Consumers0,
  _ = gen_server:reply(C, {ok, [Item]}),
  {reply, ok, State#{ consumers => Consumers1}};

handle_call({dequeue, Max}, From, #{ num := 0,
                                     consumers := Consumers } = State) ->
  if
    Max > 0 ->
      {noreply, State#{ consumers => [From | Consumers] }};
    true ->
      {reply, {ok, []}, State}
  end;

handle_call({dequeue, Max}, _From, #{ num := Num,
                                      need_close := Close } = State0) ->
  if
    Max =< Num ->
      {Reply, State1} = dequeue_items(Max, [], State0),
      {reply, Reply, State1};
    true ->
      #{ queue := Q, producers := Producers } = State0,
      _ = [gen_server:reply(P, ok) || P <- Producers],
      Items = [Item || {Item, _} <- queue:to_list(Q)],
      NState =  State0#{ queue => queue:new(),
                                       num => 0,
                                       size => 0 },
      case Close of
        false ->
          {reply, {ok, Items}, NState};
        true ->
          {stop, normal, {ok, Items}, NState}
      end
  end;

handle_call(close, _From, #{ num := 0 } = State) ->
  {stop, normal, ok, State};


handle_call(infos, _From, State) ->
  #{ num := Num,
     size := Size,
     max_size := MaxSize,
     max_items := MaxItems } = State,

  {reply, #{ num => Num,
             size => Size,
             max_size => MaxSize,
             max_items => MaxItems }, State}.


handle_cast(close, #{ num := 0 } = State) ->
  {stop, normal, State};

handle_cast(close, State) ->
  {noreply, State#{ need_close => true }};

handle_cast(_Msg, State) ->
  {noreply, State}.

terminate(_Reason, #{ consumers := Consumers }) ->
  _ = [gen_server:reply(C, closed) || C <- Consumers],
  ok.


%% ---
%% helpers

dequeue_items(0, Items, State) ->
  {{ok, lists:reverse(Items)}, State};

dequeue_items(N, Items, #{queue := Q0,
                          num := Num,
                          size := Size,
                          producers := Producers0 }=State) ->

  {{value, {Item, ItemSize}}, Q1} = queue:out(Q0),
  Producers1 = case Producers0 of
                 [] -> Producers0;
                 [P | Rest] ->
                   gen_server:reply(P, ok),
                   Rest
               end,
  dequeue_items(N -1, [Item | Items], State#{ queue => Q1,
                                               num => Num - 1,
                                               size => Size - ItemSize,
                                               producers => Producers1}).
