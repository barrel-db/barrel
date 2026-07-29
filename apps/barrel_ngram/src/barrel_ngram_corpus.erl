%%%-------------------------------------------------------------------
%%% @doc Corpus text extraction.
%%%
%%% The single definition of "the bytes that represent a document" for a
%%% corpus. Both the indexer (which selects grams from these bytes) and
%%% the query confirm pass (which runs the real substring match on them)
%%% call `doc_text/2', so the index and the confirm can never disagree
%%% about what a document contains.
%%%
%%% M1 indexes the binary string values of a document's non-reserved
%%% top-level fields, sorted by field name and joined by a newline. The
%%% `fields' config narrows this to a named subset. Non-binary values
%%% (numbers, nested maps, lists) are skipped.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_corpus).

-export([doc_text/2]).

%% Reserved document keys never contribute to the indexed text.
-define(RESERVED, [<<"id">>, <<"_rev">>, <<"_deleted">>,
                   <<"_conflicts">>, <<"_embedding">>]).

%% @doc The indexed byte string for a document under a corpus config.
-spec doc_text(map(), map()) -> binary().
doc_text(Doc, Config) ->
    Fields = maps:get(fields, Config, all),
    Candidates = maps:without(?RESERVED, Doc),
    Selected = case Fields of
        all -> Candidates;
        List when is_list(List) -> maps:with(List, Candidates)
    end,
    Values = [V || {_K, V} <- lists:keysort(1, maps:to_list(Selected)),
                   is_binary(V)],
    iolist_to_binary(lists:join(<<"\n">>, Values)).
