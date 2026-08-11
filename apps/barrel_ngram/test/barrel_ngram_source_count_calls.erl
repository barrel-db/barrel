%%%-------------------------------------------------------------------
%%% @doc Test-only barrel_ngram_source that counts `pread/4' calls in the
%%% calling process's dictionary, wrapping barrel_ngram_source_mem for the
%%% actual bytes. Proves how MANY windowed reads a query made, where
%%% barrel_ngram_source_assert_window/_max_window only prove how big each
%%% one was. `InitArg' is `{CounterKey, DocsMap}'; the query runs in the
%%% caller's own process (see barrel_ngram_query's moduledoc), so a plain
%%% `put'/`get' counter is safe without any synchronization.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_source_count_calls).

-behaviour(barrel_ngram_source).

-export([pread/4, doc_size/2]).

pread({CounterKey, Docs}, DocId, Offset, Len) ->
    put(CounterKey, count(CounterKey) + 1),
    barrel_ngram_source_mem:pread(Docs, DocId, Offset, Len).

doc_size({_CounterKey, Docs}, DocId) ->
    barrel_ngram_source_mem:doc_size(Docs, DocId).

count(CounterKey) ->
    case get(CounterKey) of
        undefined -> 0;
        N -> N
    end.
