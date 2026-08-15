%%%-------------------------------------------------------------------
%%% @doc Test-only barrel_ngram_source that asserts every pread is
%%% exactly the expected window size, wrapping barrel_ngram_source_mem
%%% for the actual bytes.
%%%
%%% `InitArg' is `{ExpectedLen, DocsMap}'. A pread whose `Len' does not
%%% match `ExpectedLen' raises immediately -- loud and test-failing on
%%% purpose, unlike the production source contract's soft `{error, _}'
%%% returns -- so a query path that (by mistake) falls back to a
%%% full-document-sized read instead of an actual windowed one is caught,
%%% not silently tolerated.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_source_assert_window).

-behaviour(barrel_ngram_source).

-export([pread/4, doc_size/2]).

pread({ExpectedLen, Docs}, DocId, Offset, Len) ->
    case Len of
        ExpectedLen -> ok;
        _ -> error({unexpected_pread_length, DocId, Offset, Len,
                    expected, ExpectedLen})
    end,
    barrel_ngram_source_mem:pread(Docs, DocId, Offset, Len).

doc_size({_ExpectedLen, Docs}, DocId) ->
    barrel_ngram_source_mem:doc_size(Docs, DocId).
