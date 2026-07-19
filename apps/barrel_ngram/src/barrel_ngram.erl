%%%-------------------------------------------------------------------
%%% @doc barrel_ngram: exact substring search over barrel_docdb.
%%%
%%% A byte-level trigram index giving exact lexical recall (identifiers,
%%% error strings, config keys) that semantic search misses. A corpus is
%%% bound to a database and a gram selector; indexing is driven by the
%%% database's changes feed, and every query result is confirmed against
%%% the real document text.
%%%
%%% == M1 usage ==
%%% ```
%%% ok = barrel_ngram:open(<<"code">>, #{db => <<"mydb">>}),
%%% {ok, _} = barrel_ngram:index(<<"code">>),
%%% {ok, Hits} = barrel_ngram:search(<<"code">>, <<"connect_timeout">>, #{}).
%%% '''
%%%
%%% Requires the barrel_ngram application to be started.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram).

-export([open/2, close/1, index/1, refresh/1, compact/1, search/2, search/3]).

-type corpus() :: binary() | atom().
-export_type([corpus/0]).

%% @doc Open a corpus bound to a database.
%%
%% Options:
%% <ul>
%%   <li>`db' (required) - the barrel_docdb database name to index.</li>
%%   <li>`selector' - gram selector module (default
%%       `barrel_ngram_selector_dense').</li>
%%   <li>`fields' - `all' or a list of field names to index (default
%%       `all').</li>
%%   <li>`data_dir' - segment storage directory (default from app env).</li>
%% </ul>
-spec open(corpus(), map()) -> ok | {error, term()}.
open(Corpus, Opts) ->
    case maps:is_key(db, Opts) of
        false ->
            {error, {missing_option, db}};
        true ->
            Config = normalize(Corpus, Opts),
            case barrel_ngram_shard_sup:start_shard(Corpus, Config) of
                {ok, _Pid} -> ok;
                {error, _} = Err -> Err
            end
    end.

%% @doc Close a corpus and stop its shard.
-spec close(corpus()) -> ok | {error, not_found}.
close(Corpus) ->
    barrel_ngram_shard_sup:stop_shard(Corpus).

%% @doc Catch the corpus up to the current head of its database's changes
%% feed and freeze the buffer. The index is kept live in the background by
%% a feed subscription; this is the synchronous catch-up point for tests
%% and ops. Alias of {@link refresh/1}.
-spec index(corpus()) -> {ok, map()} | {error, term()}.
index(Corpus) ->
    refresh(Corpus).

%% @doc Synchronously drain the changes feed up to now and freeze the
%% active buffer into a segment.
-spec refresh(corpus()) -> {ok, map()} | {error, term()}.
refresh(Corpus) ->
    barrel_ngram_shard:refresh(Corpus).

%% @doc Compact every live segment into one, physically evicting
%% superseded and deleted entries. Returns `{error, busy}' if a background
%% compaction is already running.
-spec compact(corpus()) -> {ok, map()} | {error, term()}.
compact(Corpus) ->
    barrel_ngram_shard:compact(Corpus).

%% @equiv search(Corpus, Literal, #{})
-spec search(corpus(), binary()) -> {ok, [barrel_ngram_query:hit()]} | {error, term()}.
search(Corpus, Literal) ->
    search(Corpus, Literal, #{}).

%% @doc Substring search. Returns hits with the matching document id and
%% the match spans within its corpus text.
-spec search(corpus(), binary(), map()) ->
    {ok, [barrel_ngram_query:hit()]} | {error, term()}.
search(Corpus, Literal, Opts) ->
    barrel_ngram_query:search(Corpus, Literal, Opts).

%%====================================================================
%% Internal
%%====================================================================

normalize(Corpus, Opts) ->
    Base = #{
        corpus => Corpus,
        db => maps:get(db, Opts),
        selector => maps:get(selector, Opts, barrel_ngram_selector_dense),
        fields => maps:get(fields, Opts, all),
        data_dir => maps:get(data_dir, Opts,
                             application:get_env(barrel_ngram, data_dir,
                                                 "data/barrel_ngram"))
    },
    %% pass tuning options through to the shard (defaults live there)
    maps:merge(Base, maps:with([freeze_threshold, compact_threshold,
                                selector_opts], Opts)).
