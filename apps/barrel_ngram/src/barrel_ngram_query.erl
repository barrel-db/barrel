%%%-------------------------------------------------------------------
%%% @doc Substring query path: fan across segments, then the confirm pass.
%%%
%%% A literal's candidates come from three lanes per shard, verified
%%% independently and merged: dense (phase-1 intersection, full re-scan
%%% confirm), positional (phase-2 distance-checked candidates, windowed
%%% confirm via `source' when configured else folded into dense's
%%% fetch-and-rescan), and buffer (the shard's unfrozen buffer, never
%%% phase-2 indexed, always full-content confirm). A key with a
%%% dense-sourced candidacy is dropped from the positional lane -- dense's
%%% re-scan already covers it.
%%%
%%% A key present in the buffer snapshot (live or tombstoned) is
%%% authoritative over any segment occurrence of the same key, dropped
%%% from both segment lanes before verification.
%%%
%%% The query runs in the calling process against its own immutable read
%%% handles, never inside the shard loop.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_query).

-export([search/3, regex_search/3]).

-define(RE_MATCH_LIMIT, 100000).

%% Phase-1 (dense, non-positional) is the always-on selector; there is no
%% longer a corpus-wide selector choice (see the barrel_ngram moduledoc).
-define(PHASE1_SELECTOR, barrel_ngram_selector_dense).

%% A key with more candidate starts than this gets one full-content
%% confirm instead of one windowed read per candidate: when a literal's
%% (or a regex anchor's) only reliable phase-2 gram happens to also occur
%% many times elsewhere in one particular document (repetitive content --
%% a log line, a generated config), single_gram_candidates has no second
%% gram to distance-check against and every occurrence becomes a
%% candidate, which windowed one-at-a-time is slower than just reading
%% the document once. See barrel_ngram_planner:segment_plan/2.
-define(WINDOW_CANDIDATE_CAP, 32).

-type hit() :: #{id := binary(), spans := [{non_neg_integer(), non_neg_integer()}]}.
-export_type([hit/0]).

%% @doc Substring search for `Literal' in `Corpus'. Fans across the
%% corpus's shards and merges (no cross-shard dedup needed -- each
%% document lives in exactly one shard).
%%
%% `Opts' may carry `case_sensitive => false' (default `true'); see
%% {@link barrel_ngram_planner}'s moduledoc for what that changes.
-spec search(term(), binary(), map()) -> {ok, [hit()]} | {error, term()}.
search(_Corpus, <<>>, _Opts) ->
    {error, empty_literal};
search(Corpus, Literal, Opts) when is_binary(Literal) ->
    case maps:get(case_sensitive, Opts, true) of
        true -> search_case_sensitive(Corpus, Literal);
        false -> caseless_search(Corpus, Literal)
    end.

search_case_sensitive(Corpus, Literal) ->
    {N, Config} = corpus_nc(Corpus),
    Refs = barrel_ngram_shards:refs(Corpus, N),
    merge_hits([search_shard(Ref, Config, Literal) || Ref <- Refs]).

%% @private Substring candidates from one shard, confirmed (the
%% dense/positional/buffer three-lane split -- see the moduledoc).
search_shard(Ref, Config, Literal) ->
    {ok, Segments, BufferSnapshot} = barrel_ngram_shard:snapshot(Ref),
    PositionalOpts = maps:get(phase2_selector_opts, Config, #{}),
    case segment_candidates(Segments, Literal, PositionalOpts) of
        {error, _} = Err ->
            Err;
        {DenseKeys, PositionalMap} ->
            {DenseCandidates, BufferLiveKeys} = apply_precedence(DenseKeys, BufferSnapshot),
            PositionalCandidates = maps:without(maps:keys(BufferSnapshot), PositionalMap),
            Db = maps:get(db, Config),
            DenseHits = confirm(Db, DenseCandidates, Literal, Config),
            PositionalHits = confirm_positional(Db, PositionalCandidates, Literal, Config),
            BufferHits = confirm_buffer(Db, BufferLiveKeys, Literal, Config),
            {ok, merge_sorted_hits(merge_sorted_hits(DenseHits, PositionalHits), BufferHits)}
    end.

%%====================================================================
%% Case-insensitive literal search
%%====================================================================

%% @private ASCII-only: narrow via barrel_ngram_planner's per-position
%% case-variant query over phase-1 (dense), verify `[caseless]'. Any
%% non-ASCII byte: no narrowing (`all'), verify `[caseless, unicode]'
%% with corpus-document UTF-8 validation before every match (see
%% caseless_confirm/5). Never touches phase-2 or windowed verification --
%% see barrel_ngram_planner's moduledoc for why.
caseless_search(Corpus, Literal) ->
    case barrel_ngram_planner:case_mode(Literal) of
        {error, _} = Err ->
            Err;
        {Query, REOpts, ValidateDocs} ->
            case re:compile(barrel_ngram_planner:escape_literal(Literal), REOpts) of
                {ok, RE} ->
                    {N, Config} = corpus_nc(Corpus),
                    Refs = barrel_ngram_shards:refs(Corpus, N),
                    merge_hits([caseless_search_shard(Ref, Query, RE, ValidateDocs, Config)
                                || Ref <- Refs]);
                {error, Reason} ->
                    {error, {bad_regex, Reason}}
            end
    end.

%% @private Reuses regex_segment_keys/2 and apply_precedence/2 as-is;
%% only the query and match/verify step differ from regex search.
caseless_search_shard(Ref, Query, RE, ValidateDocs, Config) ->
    {ok, Segments, BufferSnapshot} = barrel_ngram_shard:snapshot(Ref),
    case regex_segment_keys(Segments, Query) of
        {error, _} = Err ->
            Err;
        SegKeys ->
            {SegCandidates, BufferLiveKeys} = apply_precedence(SegKeys, BufferSnapshot),
            Db = maps:get(db, Config),
            Keys = lists:usort(SegCandidates ++ BufferLiveKeys),
            caseless_confirm(Db, Keys, RE, ValidateDocs, Config)
    end.

%% @private Fetch and re-scan every candidate. When `ValidateDocs', a
%% candidate whose text is not valid UTF-8 aborts the whole call with
%% `{error, {invalid_document_encoding, DocId}}' rather than being
%% silently treated as a non-match.
caseless_confirm(_Db, [], _RE, _ValidateDocs, _Config) ->
    {ok, []};
caseless_confirm(Db, Keys, RE, ValidateDocs, Config) ->
    Results = barrel_docdb:get_docs(Db, Keys),
    caseless_confirm_pairs(lists:zip(Keys, Results), RE, ValidateDocs, Config, []).

caseless_confirm_pairs([], _RE, _ValidateDocs, _Config, Acc) ->
    {ok, lists:sort(fun(#{id := A}, #{id := B}) -> A =< B end, Acc)};
caseless_confirm_pairs([{K, {ok, Doc}} | Rest], RE, ValidateDocs, Config, Acc) ->
    Text = barrel_ngram_corpus:doc_text(Doc, Config),
    case ValidateDocs andalso not barrel_ngram_planner:utf8_valid(Text) of
        true ->
            {error, {invalid_document_encoding, K}};
        false ->
            case regex_spans(Text, RE) of
                [] -> caseless_confirm_pairs(Rest, RE, ValidateDocs, Config, Acc);
                Spans -> caseless_confirm_pairs(Rest, RE, ValidateDocs, Config,
                                               [#{id => K, spans => Spans} | Acc])
            end
    end;
caseless_confirm_pairs([{_K, _Other} | Rest], RE, ValidateDocs, Config, Acc) ->
    caseless_confirm_pairs(Rest, RE, ValidateDocs, Config, Acc).

%% @private Drop any segment-derived candidate whose key is shadowed by a
%% buffer entry (see the moduledoc). Returns the deduplicated surviving
%% segment candidates and the live buffer keys.
apply_precedence(SegKeys, BufferSnapshot) ->
    SegCandidates = lists:usort(
        [K || K <- SegKeys, not maps:is_key(K, BufferSnapshot)]),
    BufferLiveKeys = [K || {K, {_Hlc, live}} <- maps:to_list(BufferSnapshot)],
    {SegCandidates, BufferLiveKeys}.

merge_sorted_hits(A, B) ->
    lists:sort(fun(#{id := X}, #{id := Y}) -> X =< Y end, A ++ B).

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

%% @private Dense candidate keys and positional match-starts across every
%% segment. `PositionalMap' excludes any key with a dense-sourced
%% candidacy elsewhere -- dense's re-scan already covers it.
segment_candidates(Segments, Literal, PositionalOpts) ->
    LiteralPlan = barrel_ngram_planner:literal_plan(Literal, PositionalOpts),
    case gather_segment_candidates(Segments, Literal, LiteralPlan, [], #{}) of
        {error, _} = Err -> Err;
        {DenseKeys, PositionalMap} -> {DenseKeys, maps:without(DenseKeys, PositionalMap)}
    end.

gather_segment_candidates([], _Literal, _LiteralPlan, DenseAcc, PosAcc) ->
    {DenseAcc, PosAcc};
gather_segment_candidates([{_Gen, Path} | Rest], Literal, LiteralPlan, DenseAcc, PosAcc) ->
    case barrel_ngram_segment:open(Path) of
        {ok, H} ->
            {DenseAcc1, PosAcc1} =
                try
                    case barrel_ngram_planner:segment_plan(H, LiteralPlan) of
                        dense ->
                            Keys = candidate_keys(H, ?PHASE1_SELECTOR, #{}, Literal),
                            {Keys ++ DenseAcc, PosAcc};
                        {positional, OrdStarts} ->
                            {DenseAcc, merge_positional(H, OrdStarts, PosAcc)}
                    end
                after
                    barrel_ngram_segment:close(H)
                end,
            gather_segment_candidates(Rest, Literal, LiteralPlan, DenseAcc1, PosAcc1);
        {error, _} = Err ->
            Err
    end.

%% @private Resolve a segment's {Ordinal, [Start]} candidates to keys and
%% fold them into the running Key -> ordset-of-starts accumulator
%% (another segment, or a repeated gram within this one, can contribute
%% more starts for the same key).
merge_positional(Handle, OrdStarts, PosAcc) ->
    Ordinals = [O || {O, _Starts} <- OrdStarts],
    KeyMap = maps:from_list(barrel_ngram_segment:keys(Handle, Ordinals)),
    lists:foldl(
        fun({O, Starts}, Acc) ->
            case maps:find(O, KeyMap) of
                {ok, Key} ->
                    maps:update_with(
                        Key, fun(Existing) -> lists:umerge(Existing, Starts) end,
                        Starts, Acc);
                error ->
                    Acc
            end
        end, PosAcc, OrdStarts).

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
%% the intersection empty. Roaring segments intersect the raw blocks in the
%% NIF (no Erlang list materialization); varint segments decode and gallop.
intersect_grams(Handle, Grams) ->
    case barrel_ngram_segment:codec(Handle) of
        varint -> collect_lists(Handle, Grams, []);
        roaring -> collect_blocks(Handle, Grams, [])
    end.

collect_lists(_Handle, [], Acc) ->
    barrel_ngram_postings:intersect_all(Acc);
collect_lists(Handle, [G | Rest], Acc) ->
    case barrel_ngram_segment:lookup_postings(Handle, G) of
        empty -> [];
        {ok, Ords} -> collect_lists(Handle, Rest, [Ords | Acc]);
        {error, _} -> []
    end.

collect_blocks(_Handle, [], Acc) ->
    barrel_ngram_roaring:decode(barrel_ngram_roaring:intersect_all(Acc));
collect_blocks(Handle, [G | Rest], Acc) ->
    case barrel_ngram_segment:lookup_block(Handle, G) of
        empty -> [];
        {ok, Block} -> collect_blocks(Handle, Rest, [Block | Acc]);
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

%% @private Verify positional (phase-2-narrowed) candidates: an exact
%% windowed read via `source' (no full-document fetch) when configured,
%% else fold this already-narrower-than-plain-dense key set into
%% `confirm/4''s unchanged fetch-and-rescan path -- phase-2 still helps
%% without `source', purely by shrinking who gets fetched.
confirm_positional(_Db, Map, _Literal, _Config) when map_size(Map) =:= 0 ->
    [];
confirm_positional(Db, Map, Literal, Config) ->
    case maps:get(source, Config, undefined) of
        undefined ->
            confirm(Db, maps:keys(Map), Literal, Config);
        Source ->
            {WindowMap, OverflowKeys} = split_by_candidate_count(Map),
            Len = byte_size(Literal),
            WindowHits = lists:filtermap(
                fun({Key, Starts}) ->
                    case barrel_ngram_verify:windowed(Source, Key, Literal, Starts) of
                        [] -> false;
                        Verified -> {true, #{id => Key, spans => [{S, Len} || S <- Verified]}}
                    end
                end, maps:to_list(WindowMap)),
            OverflowHits = confirm(Db, OverflowKeys, Literal, Config),
            merge_sorted_hits(WindowHits, OverflowHits)
    end.

%% @private Keys whose candidate count exceeds ?WINDOW_CANDIDATE_CAP are
%% pulled out for a full-content confirm instead of windowed verification.
split_by_candidate_count(Map) ->
    maps:fold(
        fun(Key, Starts, {WindowMap, Overflow}) ->
            case length(Starts) > ?WINDOW_CANDIDATE_CAP of
                true -> {WindowMap, [Key | Overflow]};
                false -> {WindowMap#{Key => Starts}, Overflow}
            end
        end, {#{}, []}, Map).

%% @private Fetch each buffer-derived candidate's current full text --
%% via `source' (whole document, no positional data to window against)
%% when configured, else the same `barrel_docdb'-based path `confirm/4'
%% uses for segment candidates -- and keep the real substring matches.
confirm_buffer(_Db, [], _Literal, _Config) ->
    [];
confirm_buffer(Db, Keys, Literal, Config) ->
    Texts = buffer_texts(Db, Keys, Config),
    hits_from_texts(Keys, Texts, fun(Text) -> binary:matches(Text, Literal) end).

%% @private Buffer candidates' current text: `source''s `doc_size'+`pread'
%% (the whole document) when configured, else `barrel_docdb:get_docs/2' +
%% `doc_text/2', matching `confirm/4''s existing fallback exactly.
buffer_texts(Db, Keys, Config) ->
    case maps:get(source, Config, undefined) of
        undefined ->
            Results = barrel_docdb:get_docs(Db, Keys),
            [case R of
                 {ok, Doc} -> {ok, barrel_ngram_corpus:doc_text(Doc, Config)};
                 Other -> Other
             end || R <- Results];
        Source ->
            [fetch_via_source(Source, K) || K <- Keys]
    end.

fetch_via_source(Source, Key) ->
    case barrel_ngram_source:doc_size(Source, Key) of
        {ok, Size} -> barrel_ngram_source:pread(Source, Key, 0, Size);
        {error, _} = Err -> Err
    end.

%% @private Shared by the literal and regex buffer-confirm passes: keep
%% the candidates whose fetched text yields at least one span.
hits_from_texts(Keys, Texts, SpansFun) ->
    Hits = lists:filtermap(
        fun({K, {ok, Text}}) ->
                case SpansFun(Text) of
                    [] -> false;
                    Spans -> {true, #{id => K, spans => Spans}}
                end;
           ({_K, _Other}) ->
                false
        end, lists:zip(Keys, Texts)),
    lists:sort(fun(#{id := A}, #{id := B}) -> A =< B end, Hits).

%%====================================================================
%% Regex search
%%====================================================================

%% @doc Regex search: turn the regex into a mandatory-trigram query,
%% intersect it over the index, then confirm each candidate with the real
%% regex engine. An eligible AND-chain pattern (see
%% {@link barrel_ngram_planner:regex_plan/2}) also gets windowed
%% verification via `source' when configured.
%%
%% `Opts' may carry `case_sensitive => false' (default `true'); a pattern
%% with its own leading `(?i)' is caseless regardless of `Opts'. Either
%% way, case-insensitive regex search never narrows or windows -- see
%% {@link barrel_ngram_planner}'s moduledoc.
-spec regex_search(term(), binary(), map()) -> {ok, [hit()]} | {error, term()}.
regex_search(Corpus, Regex, Opts) when is_binary(Regex) ->
    Analyzed = barrel_ngram_regex:analyze(Regex),
    case effective_case_sensitive(Opts, Analyzed) of
        true -> regex_search_case_sensitive(Corpus, Regex, Analyzed);
        false -> regex_search_caseless(Corpus, Regex, has_leading_caseless(Analyzed))
    end.

effective_case_sensitive(Opts, Analyzed) ->
    maps:get(case_sensitive, Opts, true) andalso not has_leading_caseless(Analyzed).

has_leading_caseless({ok, _Node, _Query, #{leading_flags := Flags}}) ->
    lists:member(caseless, Flags);
has_leading_caseless(unsupported) ->
    false.

%% @private Today's flow, unchanged: dense/positional/windowed per
%% {@link barrel_ngram_planner:regex_plan/2}. `Analyzed' is reused from
%% the dispatcher instead of re-parsing the pattern.
regex_search_case_sensitive(Corpus, Regex, Analyzed) ->
    case re:compile(Regex) of
        {error, Reason} ->
            {error, {bad_regex, Reason}};
        {ok, RE} ->
            {N, Config} = corpus_nc(Corpus),
            %% the trigram query and the regex plan are both corpus-wide
            %% (depend only on the fixed phase-1 selector and the pattern
            %% itself), so compute each once and reuse across shards.
            Query = case barrel_ngram_selector:covers_all_grams(?PHASE1_SELECTOR, #{}) of
                true -> barrel_ngram_regex:trigram_query(Regex);
                false -> all
            end,
            PositionalOpts = maps:get(phase2_selector_opts, Config, #{}),
            RegexPlan = barrel_ngram_planner:regex_plan(Analyzed, PositionalOpts),
            Refs = barrel_ngram_shards:refs(Corpus, N),
            merge_hits([regex_search_shard(Ref, Query, RE, RegexPlan, Config) || Ref <- Refs])
    end.

%% @private No narrowing, ever (`Query = all'). `Regex' is compiled
%% directly -- never through `escape_literal/1', which would destroy syntax.
regex_search_caseless(Corpus, Regex, HasLeadingCaseless) ->
    case barrel_ngram_planner:regex_case_mode(Regex, HasLeadingCaseless) of
        {error, _} = Err ->
            Err;
        {REOpts, ValidateDocs} ->
            case re:compile(Regex, REOpts) of
                {ok, RE} ->
                    {N, Config} = corpus_nc(Corpus),
                    Refs = barrel_ngram_shards:refs(Corpus, N),
                    merge_hits([caseless_search_shard(Ref, all, RE, ValidateDocs, Config)
                                || Ref <- Refs]);
                {error, Reason} ->
                    {error, {bad_regex, Reason}}
            end
    end.

%% @private `full_scan': today's unchanged flow (dense-only trigram-query
%% intersection, full re:run verification).
regex_search_shard(Ref, Query, RE, full_scan, Config) ->
    {ok, Segments, BufferSnapshot} = barrel_ngram_shard:snapshot(Ref),
    case regex_segment_keys(Segments, Query) of
        {error, _} = Err ->
            Err;
        SegKeys ->
            {SegCandidates, BufferLiveKeys} = apply_precedence(SegKeys, BufferSnapshot),
            Db = maps:get(db, Config),
            SegHits = regex_confirm(Db, SegCandidates, RE, Config),
            BufferHits = regex_confirm_buffer(Db, BufferLiveKeys, RE, Config),
            {ok, merge_sorted_hits(SegHits, BufferHits)}
    end;
%% @private `{windowed, ...}': the same per-segment dense/positional split
%% a literal search for `AnchorBytes' would use.
regex_search_shard(Ref, _Query, RE, {windowed, AnchorBytes, PrefixMax, SuffixMax, GramOffs},
                   Config) ->
    {ok, Segments, BufferSnapshot} = barrel_ngram_shard:snapshot(Ref),
    case gather_segment_candidates(Segments, AnchorBytes, {reliable, GramOffs}, [], #{}) of
        {error, _} = Err ->
            Err;
        {DenseKeys, PositionalMap0} ->
            PositionalMap = maps:without(DenseKeys, PositionalMap0),
            {DenseCandidates, BufferLiveKeys} = apply_precedence(DenseKeys, BufferSnapshot),
            PositionalCandidates = maps:without(maps:keys(BufferSnapshot), PositionalMap),
            Db = maps:get(db, Config),
            DenseHits = regex_confirm(Db, DenseCandidates, RE, Config),
            PositionalHits = regex_confirm_positional(Db, PositionalCandidates, RE,
                                                       byte_size(AnchorBytes), PrefixMax,
                                                       SuffixMax, Config),
            BufferHits = regex_confirm_buffer(Db, BufferLiveKeys, RE, Config),
            {ok, merge_sorted_hits(merge_sorted_hits(DenseHits, PositionalHits), BufferHits)}
    end.

%% @private Candidate ids across all segments for a trigram query. A
%% segment-open error propagates (matching segment_keys/2's already-strict
%% behavior) instead of silently returning an incomplete result.
regex_segment_keys(Segments, Query) ->
    lists:foldl(
        fun(_Seg, {error, _} = Err) ->
                Err;
           ({_Gen, Path}, Acc) ->
                case barrel_ngram_segment:open(Path) of
                    {ok, H} ->
                        try eval_keys(H, Query) of
                            Keys -> Keys ++ Acc
                        after
                            barrel_ngram_segment:close(H)
                        end;
                    {error, _} = Err ->
                        Err
                end
        end, [], Segments).

eval_keys(Handle, Query) ->
    Ordinals = eval_query(Handle, Query),
    [K || {_O, K} <- barrel_ngram_segment:keys(Handle, Ordinals)].

%% @private Evaluate a trigram query to candidate ordinals, per the
%% segment's codec. Roaring combines binaries natively and decodes once.
eval_query(Handle, Query) ->
    case barrel_ngram_segment:codec(Handle) of
        varint -> eval_varint(Handle, Query);
        roaring -> barrel_ngram_roaring:decode(eval_roaring(Handle, Query))
    end.

eval_varint(Handle, all) ->
    all_ordinals(Handle);
eval_varint(_Handle, none) ->
    [];
eval_varint(Handle, {gram, G}) ->
    case barrel_ngram_segment:lookup_postings(Handle, G) of
        {ok, Ords} -> Ords;
        empty -> [];
        {error, _} -> []
    end;
eval_varint(Handle, {'and', Qs}) ->
    barrel_ngram_postings:intersect_all([eval_varint(Handle, Q) || Q <- Qs]);
eval_varint(Handle, {'or', Qs}) ->
    barrel_ngram_postings:union_all([eval_varint(Handle, Q) || Q <- Qs]).

eval_roaring(Handle, all) ->
    barrel_ngram_roaring:encode(all_ordinals(Handle));
eval_roaring(_Handle, none) ->
    barrel_ngram_roaring:encode([]);
eval_roaring(Handle, {gram, G}) ->
    case barrel_ngram_segment:lookup_block(Handle, G) of
        {ok, Block} -> Block;
        _ -> barrel_ngram_roaring:encode([])
    end;
eval_roaring(Handle, {'and', Qs}) ->
    barrel_ngram_roaring:intersect_all([eval_roaring(Handle, Q) || Q <- Qs]);
eval_roaring(Handle, {'or', Qs}) ->
    barrel_ngram_roaring:union_all([eval_roaring(Handle, Q) || Q <- Qs]).

%% @private Fetch each candidate and keep the real regex matches.
regex_confirm(_Db, [], _RE, _Config) ->
    [];
regex_confirm(Db, Keys, RE, Config) ->
    Results = barrel_docdb:get_docs(Db, Keys),
    Hits = lists:filtermap(
        fun({K, {ok, Doc}}) ->
                Text = barrel_ngram_corpus:doc_text(Doc, Config),
                case regex_spans(Text, RE) of
                    [] -> false;
                    Spans -> {true, #{id => K, spans => Spans}}
                end;
           ({_K, _Other}) ->
                false
        end, lists:zip(Keys, Results)),
    lists:sort(fun(#{id := A}, #{id := B}) -> A =< B end, Hits).

%% @private Verify positional (phase-2-narrowed) regex candidates: an
%% exact windowed read via `source' ({@link barrel_ngram_verify:windowed_regex/7},
%% no full-document fetch) when configured, else fold this
%% already-narrower-than-plain-dense key set into `regex_confirm/4''s
%% unchanged fetch-and-rescan path.
regex_confirm_positional(_Db, Map, _RE, _AnchorLen, _PrefixMax, _SuffixMax, _Config)
        when map_size(Map) =:= 0 ->
    [];
regex_confirm_positional(Db, Map, RE, AnchorLen, PrefixMax, SuffixMax, Config) ->
    case maps:get(source, Config, undefined) of
        undefined ->
            regex_confirm(Db, maps:keys(Map), RE, Config);
        Source ->
            {WindowMap, OverflowKeys} = split_by_candidate_count(Map),
            WindowHits = lists:filtermap(
                fun({Key, Starts}) ->
                    case barrel_ngram_verify:windowed_regex(
                           Source, Key, RE, PrefixMax, AnchorLen, SuffixMax, Starts) of
                        [] -> false;
                        Spans -> {true, #{id => Key, spans => Spans}}
                    end
                end, maps:to_list(WindowMap)),
            OverflowHits = regex_confirm(Db, OverflowKeys, RE, Config),
            merge_sorted_hits(WindowHits, OverflowHits)
    end.

%% @private Fetch each buffer-derived candidate's current full text --
%% source-aware, mirroring confirm_buffer/4 -- and keep the real regex
%% matches.
regex_confirm_buffer(_Db, [], _RE, _Config) ->
    [];
regex_confirm_buffer(Db, Keys, RE, Config) ->
    Texts = buffer_texts(Db, Keys, Config),
    hits_from_texts(Keys, Texts, fun(Text) -> regex_spans(Text, RE) end).

regex_spans(Text, RE) ->
    case re:run(Text, RE,
                [global, {capture, first, index},
                 {match_limit, ?RE_MATCH_LIMIT},
                 {match_limit_recursion, ?RE_MATCH_LIMIT}]) of
        {match, Matches} -> [{S, L} || [{S, L}] <- Matches];
        nomatch -> [];
        {error, _} -> []
    end.
