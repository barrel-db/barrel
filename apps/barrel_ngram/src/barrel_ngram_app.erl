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

%% @doc Clears every corpus's `persistent_term' meta (both the
%% query-trusted cache and the discovery-only pending cache) --
%% `persistent_term' survives `application:stop/start' (no VM restart)
%% while shard processes do not, so stale meta would otherwise make
%% `is_open/1' lie and a query crash with `noproc'.
-spec stop(term()) -> ok.
stop(_State) ->
    [barrel_ngram_shards:erase_meta(C) || C <- barrel_ngram_shards:all_corpora()],
    [barrel_ngram_shards:erase_pending_meta(C) || C <- barrel_ngram_shards:all_pending_corpora()],
    ok.
