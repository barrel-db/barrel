-module(barrel_fold_process).

-export([start_link/1]).

-export([init/2]).

%% should be configurable
-define(FOLD_TIMEOUT, 5 * 1000 * 1000). %% 5 seconds



start_link(Fold) ->
  proc_lib:start_link(?MODULE, init, [self(), Fold]).

init(Parent, {fold_view, BarrelId, ViewId, To, Options}) ->
  {ok, #{ store_mod := Store, ref := Ref }} = barrel:open_barrel(BarrelId),
  proc_lib:init_ack(Parent, {ok, self()}),
  %% link to the client
  true = link(To),
  Timeout = barrel_config:get(fold_timeout),
  FoldFun = fun({DocId, Key, Value}, Ts) ->
                Row = #{ key => Key,
                         value => Value,
                         id => DocId },

                %% TODO: replace by partisan call
                To ! {self(), {ok, Row}},
                Now = erlang:timestamp(),
                Diff = timer:now_dif(Now, Ts),
                if
                  Diff >= Timeout ->
                    {stop, timeout};
                  true ->
                    {ok, Now}
                end
            end,

  case Store:fold_view_index(Ref, ViewId, FoldFun, erlang:timestamp(), Options) of
    {stop, timeout} ->
      exit({fold_timeout, self()});
    _ ->
       To ! {self(), done}
  end.






