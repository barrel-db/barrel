%%%-------------------------------------------------------------------
%%% @doc Substring query path: gram intersection then the confirm pass.
%%%
%%% Trigram presence is a necessary but not sufficient condition for a
%%% substring match, so every candidate is confirmed by fetching the
%%% document and running the real substring match on its corpus text.
%%%
%%% Plan:
%%% <ol>
%%%   <li>Ask the selector for the literal's reliable grams.</li>
%%%   <li>`{reliable, Grams}': intersect those grams' posting lists to get
%%%       candidate ordinals. `brute_force' (literal shorter than a
%%%       trigram): take every ordinal.</li>
%%%   <li>Map ordinals to keys via the segment sidecar, fetch the docs
%%%       from barrel_docdb, and keep only those whose corpus text really
%%%       contains the literal. Each hit carries its match spans.</li>
%%% </ol>
%%%
%%% The query runs in the calling process against its own immutable read
%%% handle, never inside the shard loop.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_query).

-export([search/3]).

-type hit() :: #{id := binary(), spans := [{non_neg_integer(), non_neg_integer()}]}.
-export_type([hit/0]).

%% @doc Substring search for `Literal' in `Corpus'.
-spec search(term(), binary(), map()) -> {ok, [hit()]} | {error, term()}.
search(_Corpus, <<>>, _Opts) ->
    {error, empty_literal};
search(Corpus, Literal, _Opts) when is_binary(Literal) ->
    {ok, Config} = barrel_ngram_shard:get_config(Corpus),
    case barrel_ngram_shard:get_segment(Corpus) of
        none ->
            {ok, []};
        {ok, Path} ->
            case barrel_ngram_segment:open(Path) of
                {ok, Handle} ->
                    try
                        run(Handle, Config, Literal)
                    after
                        barrel_ngram_segment:close(Handle)
                    end;
                {error, _} = Err ->
                    Err
            end
    end.

%%====================================================================
%% Internal
%%====================================================================

run(Handle, Config, Literal) ->
    case candidate_ordinals(Handle, Config, Literal) of
        {error, _} = Err ->
            Err;
        Ordinals ->
            KeyPairs = barrel_ngram_segment:keys(Handle, Ordinals),
            Db = maps:get(db, Config),
            {ok, confirm(Db, KeyPairs, Literal, Config)}
    end.

%% @private Candidate ordinals before the confirm pass.
candidate_ordinals(Handle, Config, Literal) ->
    Selector = maps:get(selector, Config, barrel_ngram_selector_dense),
    case barrel_ngram_selector:reliable_grams(Selector, Literal) of
        brute_force ->
            all_ordinals(Handle);
        {reliable, []} ->
            all_ordinals(Handle);
        {reliable, Grams} ->
            intersect_grams(Handle, Grams)
    end.

all_ordinals(Handle) ->
    case barrel_ngram_segment:doc_count(Handle) of
        0 -> [];
        N -> lists:seq(0, N - 1)
    end.

%% @private Intersect the posting lists of the given grams. A missing
%% gram makes the intersection empty.
intersect_grams(Handle, Grams) ->
    collect_lists(Handle, Grams, []).

collect_lists(_Handle, [], Acc) ->
    barrel_ngram_postings:intersect_all(Acc);
collect_lists(Handle, [G | Rest], Acc) ->
    case barrel_ngram_segment:lookup_postings(Handle, G) of
        empty ->
            [];   %% one absent gram => no candidates
        {ok, Ords} ->
            collect_lists(Handle, Rest, [Ords | Acc]);
        {error, _} = Err ->
            Err
    end.

%% @private Fetch each candidate and keep the real substring matches.
confirm(Db, KeyPairs, Literal, Config) ->
    Keys = [K || {_O, K} <- KeyPairs],
    Results = case Keys of
        [] -> [];
        _ -> barrel_docdb:get_docs(Db, Keys)
    end,
    Hits = lists:filtermap(
        fun({K, {ok, Doc}}) ->
                Text = barrel_ngram_corpus:doc_text(Doc, Config),
                case binary:matches(Text, Literal) of
                    [] -> false;
                    Spans -> {true, #{id => K, spans => Spans}}
                end;
           ({_K, _Other}) ->
                false
        end, lists:zip(Keys, Results)),
    lists:sort(fun(#{id := A}, #{id := B}) -> A =< B end, Hits).
