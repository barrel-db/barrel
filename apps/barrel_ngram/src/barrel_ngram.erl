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

-export([open/2, close/1, is_open/1, index/1, refresh/1, compact/1,
         search/2, search/3, regex/2, regex/3]).

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
%%   <li>`shards' - number of shards to spread the corpus across by
%%       rendezvous hashing (default 1). Fixed for the corpus.</li>
%%   <li>`postings' - posting-list codec, `varint' (default) or `roaring'
%%       (a native bitmap AND for large dense corpora).</li>
%% </ul>
-spec open(corpus(), map()) -> ok | {error, term()}.
open(Corpus, Opts) ->
    case maps:is_key(db, Opts) of
        false ->
            {error, {missing_option, db}};
        true ->
            N = maps:get(shards, Opts, 1),
            Config = normalize(Corpus, Opts),
            case start_shards(Corpus, N, Config) of
                ok ->
                    barrel_ngram_shards:put_meta(Corpus,
                                                 #{shards => N, config => Config}),
                    ok;
                {error, _} = Err ->
                    Err
            end
    end.

%% @doc Close a corpus, stopping every shard.
-spec close(corpus()) -> ok.
close(Corpus) ->
    _ = [barrel_ngram_shard_sup:stop_shard(Ref) || Ref <- corpus_refs(Corpus)],
    barrel_ngram_shards:erase_meta(Corpus),
    ok.

%% @doc Whether a corpus is currently open (cheap metadata check).
-spec is_open(corpus()) -> boolean().
is_open(Corpus) ->
    case barrel_ngram_shards:get_meta(Corpus) of
        {ok, _} -> true;
        undefined -> false
    end.

%% @doc Catch the corpus up to the current head of its database's changes
%% feed and freeze the buffer. The index is kept live in the background by
%% a feed subscription; this is the synchronous catch-up point for tests
%% and ops. Alias of {@link refresh/1}.
-spec index(corpus()) -> {ok, map()} | {error, term()}.
index(Corpus) ->
    refresh(Corpus).

%% @doc Synchronously drain the changes feed up to now and freeze every
%% shard's buffer into a segment.
-spec refresh(corpus()) -> {ok, map()} | {error, term()}.
refresh(Corpus) ->
    fan(Corpus, fun barrel_ngram_shard:refresh/1).

%% @doc Compact every shard's live segments, physically evicting superseded
%% and deleted entries. Returns `{error, busy}' if a background compaction
%% is already running on a shard.
-spec compact(corpus()) -> {ok, map()} | {error, term()}.
compact(Corpus) ->
    fan(Corpus, fun barrel_ngram_shard:compact/1).

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

%% @equiv regex(Corpus, Regex, #{})
-spec regex(corpus(), binary()) ->
    {ok, [barrel_ngram_query:hit()]} | {error, term()}.
regex(Corpus, Regex) ->
    regex(Corpus, Regex, #{}).

%% @doc Regex search (PCRE syntax). Returns hits with the matching id and
%% the match spans within its corpus text. `{error, {bad_regex, _}}' if the
%% pattern does not compile.
-spec regex(corpus(), binary(), map()) ->
    {ok, [barrel_ngram_query:hit()]} | {error, term()}.
regex(Corpus, Regex, Opts) ->
    barrel_ngram_query:regex_search(Corpus, Regex, Opts).

%%====================================================================
%% Internal
%%====================================================================

start_shards(Corpus, N, Config) ->
    Refs = lists:zip(lists:seq(0, N - 1), barrel_ngram_shards:refs(Corpus, N)),
    lists:foldl(
        fun({I, Ref}, ok) ->
                SC = Config#{shard_index => I, shards => N},
                case barrel_ngram_shard_sup:start_shard(Ref, SC) of
                    {ok, _Pid} -> ok;
                    {error, _} = Err -> Err
                end;
           (_, {error, _} = Err) ->
                Err
        end, ok, Refs).

corpus_refs(Corpus) ->
    N = case barrel_ngram_shards:get_meta(Corpus) of
        {ok, #{shards := Sh}} -> Sh;
        undefined -> 1
    end,
    barrel_ngram_shards:refs(Corpus, N).

%% Fan an op across shards. A single-shard corpus passes the shard's result
%% straight through (so per-shard fields like segments/doc_count survive);
%% a multi-shard corpus returns an aggregate, or the first error.
fan(Corpus, Fun) ->
    case corpus_refs(Corpus) of
        [Ref] ->
            Fun(Ref);
        Refs ->
            Results = [Fun(Ref) || Ref <- Refs],
            case [E || {error, _} = E <- Results] of
                [Err | _] ->
                    Err;
                [] ->
                    {ok, #{
                        shards => length(Refs),
                        segments => sum(segments, Results),
                        doc_count => sum(doc_count, Results)
                    }}
            end
    end.

sum(Key, Results) ->
    lists:sum([maps:get(Key, M, 0) || {ok, M} <- Results]).

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
                                selector_opts, postings], Opts)).
