%%%-------------------------------------------------------------------
%%% @doc Corpus-level persisted config (`corpus.meta').
%%%
%%% One file per corpus, regardless of shard count -- unlike the
%%% per-shard manifest, this is layout-independent: `barrel_ngram_shards'
%%% ref shapes differ between a single-shard corpus (the bare corpus
%%% name) and a multi-shard one (`{Corpus, I}' per shard), so only a
%%% corpus-level file can record `shards => N' itself durably, closing
%%% the "shard count change orphans the old set" gap a per-shard-only
%%% manifest could not.
%%%
%%% Content: `#{version, state, db, db_instance_id, shards,
%%% phase2_selector_opts, fields, postings}'. `state' (`initializing' or
%%% `active') is internal bookkeeping for crash recovery, not a
%%% requested option -- see {@link barrel_ngram_corpus_lifecycle}.
%%%
%%% Written/read atomically (temp file + rename), matching the existing
%%% per-shard manifest's own convention.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_corpus_config).

-export([save/2, load/1, delete/1, cleanup_orphan_tmp/1]).

-define(FILENAME, "corpus.meta").
-define(VERSION, 1).

-type config() :: #{
    version := pos_integer(),
    state := initializing | active,
    db := binary(),
    db_instance_id := binary(),
    shards := pos_integer(),
    phase2_selector_opts := map(),
    fields := all | [binary()],
    postings := varint | roaring
}.
-export_type([config/0]).

%% @doc Write the corpus.meta atomically (temp + rename).
-spec save(map(), config()) -> ok | {error, term()}.
save(Config, Map) ->
    Path = path(Config),
    case filelib:ensure_dir(Path) of
        ok ->
            Tmp = tmp_path(Path),
            case file:write_file(Tmp, term_to_binary(Map#{version => ?VERSION})) of
                ok -> file:rename(Tmp, Path);
                {error, _} = Err -> Err
            end;
        {error, _} = Err ->
            Err
    end.

%% @doc Load the corpus.meta. Three-way, not collapsed to a boolean:
%% `not_found' means genuinely no file (a fresh or pre-fix corpus);
%% `{error, Reason}' means a file exists but could not be read/decoded
%% (truncated, corrupt, permission denied) -- callers must never treat
%% the two the same way (see `barrel_ngram_corpus_lifecycle:run_op/2').
-spec load(map()) -> {ok, config()} | not_found | {error, term()}.
load(Config) ->
    Path = path(Config),
    case file:read_file(Path) of
        {ok, Bin} ->
            try binary_to_term(Bin) of
                #{version := V} = M when V =:= ?VERSION ->
                    {ok, M};
                #{version := V} ->
                    {error, {unsupported_corpus_meta_version, V, ?VERSION}};
                _ ->
                    {error, corrupt_corpus_meta}
            catch
                _:_ -> {error, corrupt_corpus_meta}
            end;
        {error, enoent} ->
            not_found;
        {error, _} = Err ->
            Err
    end.

%% @doc Remove the corpus.meta file outright (tolerating `enoent').
-spec delete(map()) -> ok | {error, term()}.
delete(Config) ->
    case file:delete(path(Config)) of
        ok -> ok;
        {error, enoent} -> ok;
        {error, _} = Err -> Err
    end.

%% @doc Remove a stray write-in-progress temp file, if one survived an
%% interrupted `save/2' (a VM/coordinator death, or a rename failure,
%% between the temp write and the rename). Safe to call unconditionally,
%% real corpus.meta present or not: the per-corpus lifecycle lock (the
%% coordinator's own `via'-name registration) guarantees no concurrent
%% writer to the same temp file could ever be racing this cleanup.
-spec cleanup_orphan_tmp(map()) -> ok.
cleanup_orphan_tmp(Config) ->
    case file:delete(tmp_path(path(Config))) of
        ok -> ok;
        {error, enoent} -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Internal
%%====================================================================

path(Config) ->
    filename:join([data_dir(Config), corpus_name(maps:get(corpus, Config)), ?FILENAME]).

tmp_path(Path) ->
    iolist_to_binary([to_binary(Path), <<".tmp">>]).

data_dir(Config) ->
    maps:get(data_dir, Config,
             application:get_env(barrel_ngram, data_dir, "data/barrel_ngram")).

corpus_name(Corpus) when is_binary(Corpus) -> Corpus;
corpus_name(Corpus) when is_atom(Corpus) -> atom_to_binary(Corpus, utf8);
corpus_name(Corpus) -> iolist_to_binary(io_lib:format("~p", [Corpus])).

to_binary(P) when is_binary(P) -> P;
to_binary(P) when is_list(P) -> list_to_binary(P).
