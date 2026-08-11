%%%-------------------------------------------------------------------
%%% @doc Test-only barrel_ngram_source that asserts every pread is AT
%%% MOST a given window size, wrapping barrel_ngram_source_mem for the
%%% actual bytes.
%%%
%%% Unlike barrel_ngram_source_assert_window (an exact-size assertion, for
%%% the literal path where the window is always exactly byte_size(Literal)),
%%% a regex window is bounded ABOVE (PrefixMax + AnchorLen + SuffixMax,
%%% possibly clamped smaller near a document's edges) -- so this checks
%%% `Len =< MaxLen', not equality. `InitArg' is `{MaxLen, DocsMap}'.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_source_assert_max_window).

-behaviour(barrel_ngram_source).

-export([pread/4, doc_size/2]).

pread({MaxLen, Docs}, DocId, Offset, Len) ->
    case Len =< MaxLen of
        true -> ok;
        false -> error({pread_exceeds_max_window, DocId, Offset, Len, max, MaxLen})
    end,
    barrel_ngram_source_mem:pread(Docs, DocId, Offset, Len).

doc_size({_MaxLen, Docs}, DocId) ->
    barrel_ngram_source_mem:doc_size(Docs, DocId).
