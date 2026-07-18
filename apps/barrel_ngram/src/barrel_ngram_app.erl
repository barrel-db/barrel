%%%-------------------------------------------------------------------
%%% @doc barrel_ngram application module.
%%%
%%% Starts the supervision subtree for the trigram index.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_app).

-behaviour(application).

-export([start/2, stop/1]).

-spec start(application:start_type(), term()) -> {ok, pid()} | {error, term()}.
start(_StartType, _StartArgs) ->
    barrel_ngram_sup:start_link().

-spec stop(term()) -> ok.
stop(_State) ->
    ok.
