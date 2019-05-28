-module(barrel_fold_process).

-export([start_link/1]).

-export([init/2]).

-include("barrel.hrl").

%% should be configurable
-define(FOLD_TIMEOUT, 5 * 1000 * 1000). %% 5 seconds



start_link(Fold) ->
  proc_lib:start_link(?MODULE, init, [self(), Fold]).

init(Parent, {fold_view, BarrelId, ViewId, To, Options}) ->
  proc_lib:init_ack(Parent, {ok, self()}),
  %% link to the client
  true = link(To),
  FoldFun = fun({DocId, Key, Value}, _) ->
                Row = #{ key => Key,
                         value => Value,
                         id => DocId },

                %% TODO: replace by partisan call
                To ! {self(), {ok, Row}},
                ok
            end,
 {ok, #{ ref := Ref }} = barrel_db:open_barrel(BarrelId),
 ok = ?STORE:fold_view_index(Ref, ViewId, FoldFun, ok, Options),
 To ! {self(), done},
 %% remove the link to avoid a message to linked processes
 %% that are trapping exit signals
 true = unlink(To).
