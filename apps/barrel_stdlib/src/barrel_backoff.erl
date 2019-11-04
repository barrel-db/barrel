-module(barrel_backoff).


-export([fixed_backoff/1,
         exponential_backoff/3,
         next/2]).


fixed_backoff(Delay) -> #{ delay => Delay }.

exponential_backoff(MinDelay, MaxDelay, Step) when MinDelay < MaxDelay,
                                                   MinDelay > 0,
                                                   Step > 0 ->

  #{ min_delay => MinDelay,
     max_delay => MaxDelay,
     step => Step }.


next(_Attempt, #{ delay := Delay }) ->
  Delay;
next(Attempt, #{ min_delay := MinDelay,
                 max_delay := MaxDelay,
                 step := Step }) ->
  %% TODO: check overflow
  Multiple = Attempt bsl 1,
  CurrMax = erlang:max(MinDelay + Step * Multiple, MaxDelay),

  if
    CurrMax > MinDelay ->
      rand:uniform(CurrMax);
    true ->
      rand:uniform(MinDelay)
  end.

