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

-export([search/3, regex_search/3]).

-define(RE_MATCH_LIMIT, 100000).

-type hit() :: #{id := binary(), spans := [{non_neg_integer(), non_neg_integer()}]}.
-export_type([hit/0]).

%% @doc Substring search for `Literal' in `Corpus'. Fans across the
%% corpus's shards and merges. Each document lives in exactly one shard, so
%% the union needs no cross-shard dedup.
-spec search(term(), binary(), map()) -> {ok, [hit()]} | {error, term()}.
search(_Corpus, <<>>, _Opts) ->
    {error, empty_literal};
search(Corpus, Literal, _Opts) when is_binary(Literal) ->
    {N, Config} = corpus_nc(Corpus),
    Refs = barrel_ngram_shards:refs(Corpus, N),
    merge_hits([search_shard(Ref, Config, Literal) || Ref <- Refs]).

%% @private Substring candidates from one shard, confirmed.
search_shard(Ref, Config, Literal) ->
    {ok, Segments} = barrel_ngram_shard:get_manifest(Ref),
    BufferKeys = barrel_ngram_shard:buffer_keys(Ref),
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

%% @private Corpus shard count + config from the meta, defaulting to a
%% single shard whose config is read from the shard itself.
corpus_nc(Corpus) ->
    case barrel_ngram_shards:get_meta(Corpus) of
        {ok, #{shards := N, config := Config}} ->
            {N, Config};
        undefined ->
            {ok, Config} = barrel_ngram_shard:get_config(Corpus),
            {1, Config}
    end.

%% @private Merge per-shard results: first error wins, else union the hits
%% and sort by id.
merge_hits(Results) ->
    case [E || {error, _} = E <- Results] of
        [Err | _] ->
            Err;
        [] ->
            Hits = lists:append([H || {ok, H} <- Results]),
            {ok, lists:sort(fun(#{id := A}, #{id := B}) -> A =< B end, Hits)}
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

%%====================================================================
%% Regex search
%%====================================================================

%% @doc Regex search: turn the regex into a mandatory-trigram query,
%% intersect it over the index (only when the selector indexes every gram),
%% then confirm each candidate with the real regex engine.
-spec regex_search(term(), binary(), map()) -> {ok, [hit()]} | {error, term()}.
regex_search(Corpus, Regex, _Opts) when is_binary(Regex) ->
    case re:compile(Regex) of
        {error, Reason} ->
            {error, {bad_regex, Reason}};
        {ok, RE} ->
            {N, Config} = corpus_nc(Corpus),
            Selector = maps:get(selector, Config, barrel_ngram_selector_dense),
            SelectorOpts = maps:get(selector_opts, Config, #{}),
            %% the trigram query is corpus-wide (depends only on the
            %% selector), so compute it once and reuse across shards.
            Query = case barrel_ngram_selector:covers_all_grams(Selector, SelectorOpts) of
                true -> barrel_ngram_regex:trigram_query(Regex);
                false -> all
            end,
            Refs = barrel_ngram_shards:refs(Corpus, N),
            merge_hits([regex_search_shard(Ref, Query, RE, Config) || Ref <- Refs])
    end.

%% @private Regex candidates from one shard, confirmed.
regex_search_shard(Ref, Query, RE, Config) ->
    {ok, Segments} = barrel_ngram_shard:get_manifest(Ref),
    BufferKeys = barrel_ngram_shard:buffer_keys(Ref),
    SegKeys = regex_segment_keys(Segments, Query),
    Keys = lists:usort(SegKeys ++ BufferKeys),
    Db = maps:get(db, Config),
    {ok, regex_confirm(Db, Keys, RE, Config)}.

%% @private Candidate ids across all segments for a trigram query.
regex_segment_keys(Segments, Query) ->
    lists:foldl(
        fun({_Gen, Path}, Acc) ->
            case barrel_ngram_segment:open(Path) of
                {ok, H} ->
                    try eval_keys(H, Query) of
                        Keys -> Keys ++ Acc
                    after
                        barrel_ngram_segment:close(H)
                    end;
                {error, _} ->
                    Acc
            end
        end, [], Segments).

eval_keys(Handle, Query) ->
    Ordinals = eval_query(Handle, Query),
    [K || {_O, K} <- barrel_ngram_segment:keys(Handle, Ordinals)].

%% @private Evaluate a trigram query to candidate ordinals.
eval_query(Handle, all) ->
    all_ordinals(Handle);
eval_query(_Handle, none) ->
    [];
eval_query(Handle, {gram, G}) ->
    case barrel_ngram_segment:lookup_postings(Handle, G) of
        {ok, Ords} -> Ords;
        empty -> [];
        {error, _} -> []
    end;
eval_query(Handle, {'and', Qs}) ->
    barrel_ngram_postings:intersect_all([eval_query(Handle, Q) || Q <- Qs]);
eval_query(Handle, {'or', Qs}) ->
    barrel_ngram_postings:union_all([eval_query(Handle, Q) || Q <- Qs]).

%% @private Fetch each candidate and keep the real regex matches.
regex_confirm(_Db, [], _RE, _Config) ->
    [];
regex_confirm(Db, Keys, RE, Config) ->
    Results = barrel_docdb:get_docs(Db, Keys),
    Hits = lists:filtermap(
        fun({K, {ok, Doc}}) ->
                Text = barrel_ngram_corpus:doc_text(Doc, Config),
                case re:run(Text, RE,
                            [global, {capture, first, index},
                             {match_limit, ?RE_MATCH_LIMIT},
                             {match_limit_recursion, ?RE_MATCH_LIMIT}]) of
                    {match, Matches} ->
                        {true, #{id => K, spans => [{S, L} || [{S, L}] <- Matches]}};
                    nomatch ->
                        false;
                    {error, _} ->
                        false
                end;
           ({_K, _Other}) ->
                false
        end, lists:zip(Keys, Results)),
    lists:sort(fun(#{id := A}, #{id := B}) -> A =< B end, Hits).
