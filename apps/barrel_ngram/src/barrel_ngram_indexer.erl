%%%-------------------------------------------------------------------
%%% @doc One-shot index build from the changes feed.
%%%
%%% M1 builds a corpus by draining the compacted changes feed once
%%% (`get_changes(Db, first)'), indexing each live document, then
%%% freezing to a single segment stamped with the feed's high watermark.
%%% This exercises the feed contract (one row per live doc at its latest
%%% HLC, keyed by stable id) and records the watermark that the
%%% incremental-indexing milestone resumes from. Deletes and incremental
%%% tailing are out of scope for M1.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_indexer).

-export([build/1]).

%% @doc Build the corpus index from its configured database.
-spec build(term()) -> {ok, map()} | {error, term()}.
build(Corpus) ->
    {ok, Config} = barrel_ngram_shard:get_config(Corpus),
    Db = maps:get(db, Config),
    case barrel_docdb:get_changes(Db, first, #{include_docs => true}) of
        {ok, Changes, LastHlc} ->
            Docs = [{maps:get(id, C), barrel_ngram_corpus:doc_text(maps:get(doc, C), Config)}
                    || C <- Changes,
                       maps:get(deleted, C, false) =:= false,
                       is_map(maps:get(doc, C, undefined))],
            ok = barrel_ngram_shard:index_docs(Corpus, Docs),
            case barrel_ngram_shard:freeze(Corpus, barrel_hlc:encode(LastHlc)) of
                {ok, Path} ->
                    {ok, #{corpus => Corpus, docs => length(Docs),
                           watermark => LastHlc, segment => Path}};
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.
