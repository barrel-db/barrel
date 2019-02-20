-module(barrel_ars_view).

-export([version/0,
         init/1,
         handle_doc/2]).


version() -> 1.

init(Config) -> {ok, Config}.

handle_doc(Doc, _Config) ->
  io:format("~s, received doc=~p~n", [?MODULE_STRING, Doc]),
  [].

