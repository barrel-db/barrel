%%%-------------------------------------------------------------------
%%% @doc Substring query path: fan across segments, then the confirm pass.
%%%
%%% Candidates are gathered from every live segment (gram intersection, or
%%% the whole segment for a sub-trigram literal) plus the shard's unfrozen
%%% buffer, then de-duplicated by id. Every candidate is confirmed by
%%% fetching the current document and running the real substring match on
%%% its corpus text, so trigram false positives, stale entries left by an
%%% update, and deleted documents (fetched as `not_found') are all
%%% dropped. Trigram presence is necessary, never sufficient.
%%%
%%% The query runs in the calling process against its own immutable read
%%% handles, never inside the shard loop.
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
    {ok, Segments} = barrel_ngram_shard:get_manifest(Corpus),
    BufferKeys = barrel_ngram_shard:buffer_keys(Corpus),
    Selector = maps:get(selector, Config, barrel_ngram_selector_dense),
    SelectorOpts = maps:get(selector_opts, Config, #{}),
    case segment_keys(Segments, Selector, SelectorOpts, Literal) of
        {error, _} = Err ->
            Err;
        SegKeys ->
            Keys = lists:usort(SegKeys ++ BufferKeys),
            Db = maps:get(db, Config),
            {ok, confirm(Db, Keys, Literal, Config)}
    end.

%%====================================================================
%% Candidate gathering
%%====================================================================

%% @private Candidate ids across all segments (de-dup happens in search/3).
segment_keys(Segments, Selector, SelectorOpts, Literal) ->
    lists:foldl(
        fun(_Seg, {error, _} = Err) ->
                Err;
           ({_Gen, Path}, Acc) ->
                case barrel_ngram_segment:open(Path) of
                    {ok, H} ->
                        try candidate_keys(H, Selector, SelectorOpts, Literal) of
                            Keys -> Keys ++ Acc
                        after
                            barrel_ngram_segment:close(H)
                        end;
                    {error, _} = Err ->
                        Err
                end
        end, [], Segments).

candidate_keys(Handle, Selector, SelectorOpts, Literal) ->
    Ordinals = case barrel_ngram_selector:reliable_grams(Selector, SelectorOpts, Literal) of
        brute_force -> all_ordinals(Handle);
        {reliable, []} -> all_ordinals(Handle);
        {reliable, Grams} -> intersect_grams(Handle, Grams)
    end,
    [K || {_O, K} <- barrel_ngram_segment:keys(Handle, Ordinals)].

all_ordinals(Handle) ->
    case barrel_ngram_segment:doc_count(Handle) of
        0 -> [];
        N -> lists:seq(0, N - 1)
    end.

%% @private Intersect the posting lists of the grams. A missing gram makes
%% the intersection empty.
intersect_grams(Handle, Grams) ->
    collect_lists(Handle, Grams, []).

collect_lists(_Handle, [], Acc) ->
    barrel_ngram_postings:intersect_all(Acc);
collect_lists(Handle, [G | Rest], Acc) ->
    case barrel_ngram_segment:lookup_postings(Handle, G) of
        empty -> [];
        {ok, Ords} -> collect_lists(Handle, Rest, [Ords | Acc]);
        {error, _} -> []
    end.

%%====================================================================
%% Confirm pass
%%====================================================================

%% @private Fetch each candidate and keep the real substring matches.
confirm(_Db, [], _Literal, _Config) ->
    [];
confirm(Db, Keys, Literal, Config) ->
    Results = barrel_docdb:get_docs(Db, Keys),
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
