%%%-------------------------------------------------------------------
%%% @doc Phase-2 (sparse, positional) indexing and buffer/segment
%%% precedence -- Step 3 of the positional-index work.
%%%
%%% Phase-2 postings don't drive query results yet (later steps); these
%%% tests prove the indexing and precedence PLUMBING built in this step is
%%% correct in isolation, directly against segment/buffer internals and
%%% through the real corpus lifecycle.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_positional_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([phase2_payload_round_trips_through_a_real_freeze/1]).
-export([query_before_any_refresh_is_correct/1,
         buffer_update_masks_older_segment_content/1,
         buffered_delete_masks_older_segment_hit/1,
         same_key_across_multiple_segments_resolves_to_newest/1,
         source_configured_buffer_verification_uses_source/1]).
-export([windowed_search_returns_correct_spans/1,
         windowed_search_finds_multiple_occurrences/1,
         pread_size_bounded_proof/1]).
-export([windowed_regex_search_returns_correct_spans/1,
         windowed_regex_bounded_prefix_widens_window/1,
         windowed_regex_pread_size_bounded_proof/1]).
-export([overflow_candidates_fall_back_to_full_confirm/1]).

-define(POS_OPTS, #{radius => 2, sample_rate => 2}).

all() ->
    [phase2_payload_round_trips_through_a_real_freeze,
     query_before_any_refresh_is_correct,
     buffer_update_masks_older_segment_content,
     buffered_delete_masks_older_segment_hit,
     same_key_across_multiple_segments_resolves_to_newest,
     source_configured_buffer_verification_uses_source,
     windowed_search_returns_correct_spans,
     windowed_search_finds_multiple_occurrences,
     pread_size_bounded_proof,
     windowed_regex_search_returns_correct_spans,
     windowed_regex_bounded_prefix_widens_window,
     windowed_regex_pread_size_bounded_proof,
     overflow_candidates_fall_back_to_full_confirm].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    {ok, _} = application:ensure_all_started(barrel_ngram),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    Db = iolist_to_binary([<<"ngram_pos_">>, atom_to_binary(TC, utf8)]),
    Corpus = Db,
    DataDir = filename:join(?config(priv_dir, Config), atom_to_list(TC)),
    _ = barrel_docdb:delete_db(Db),
    {ok, _} = barrel_docdb:create_db(Db),
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir,
                                     phase2_selector_opts => ?POS_OPTS}),
    [{db, Db}, {corpus, Corpus}, {data_dir, DataDir} | Config].

end_per_testcase(_TC, Config) ->
    Corpus = ?config(corpus, Config),
    Db = ?config(db, Config),
    _ = barrel_ngram:close(Corpus),
    _ = barrel_docdb:delete_db(Db),
    ok.

%%====================================================================
%% Test cases
%%====================================================================

%% A document indexed through the real corpus lifecycle (put_doc ->
%% refresh -> freeze) must produce, in the resulting segment, exactly the
%% phase-2 payload barrel_ngram_selector_sparse:select_grams_positional/2
%% computes directly over the same corpus text -- and nothing for a gram
%% that selector never selected.
phase2_payload_round_trips_through_a_real_freeze(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    Text = <<"error: connect_timeout exceeded in the connection pool budget again">>,
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"doc1">>, <<"body">> => Text}),
    {ok, _} = barrel_ngram:refresh(Corpus),
    {ok, [{_Gen, Path}]} = barrel_ngram_shard:get_manifest(Corpus),
    {ok, H} = barrel_ngram_segment:open(Path),
    try
        CorpusText = barrel_ngram_corpus:doc_text(#{<<"body">> => Text}, #{fields => all}),
        ExpectedByGram = group_by_gram(
            barrel_ngram_selector_sparse:select_grams_positional(CorpusText, ?POS_OPTS)),
        %% only doc in this segment, so its ordinal is 0
        maps:foreach(
            fun(Gram, ExpectedOffsets) ->
                {ok, Block} = barrel_ngram_segment:lookup_positional_block(H, Gram),
                ?assertEqual([{0, ExpectedOffsets}],
                             barrel_ngram_postings_positional:decode(Block))
            end, ExpectedByGram),
        AllGrams = barrel_ngram_selector_dense:select_grams(CorpusText, #{}),
        NeverSelected = AllGrams -- maps:keys(ExpectedByGram),
        ?assert(length(NeverSelected) > 0),   %% sanity: the sampling actually narrowed something
        lists:foreach(
            fun(G) ->
                ?assertEqual(not_found, barrel_ngram_segment:lookup_positional_block(H, G))
            end, NeverSelected)
    after
        barrel_ngram_segment:close(H)
    end.

%%====================================================================
%% Buffer / segment precedence
%%====================================================================

%% Query before any refresh: the doc lives only in the buffer, never
%% frozen to a segment, and must still be found. No refresh means the
%% only path a change has into the buffer is the live push subscription,
%% which is asynchronous -- poll rather than assert immediately (same
%% idiom as barrel_ngram_incremental_SUITE:live_subscription/1).
query_before_any_refresh_is_correct(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"doc1">>,
                                         <<"body">> => <<"connect_timeout in the buffer">>}),
    ok = wait_until(fun() -> search_ids(Corpus, <<"connect_timeout">>) =:= [<<"doc1">>] end, 100),
    ?assertEqual([<<"doc1">>], search_ids(Corpus, <<"connect_timeout">>)).

%% Freeze a doc into a segment, then update it without refreshing again:
%% the update sits in the buffer, unfrozen (reaching it only via the async
%% push subscription -- poll). The segment's now-stale content must not
%% surface, and the buffer's current content must.
buffer_update_masks_older_segment_content(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    {ok, R} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"doc1">>,
                                         <<"body">> => <<"connect_timeout original">>}),
    {ok, _} = barrel_ngram:refresh(Corpus),
    Rev = maps:get(<<"rev">>, R),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"doc1">>,
                                         <<"body">> => <<"connect_timeout updated">>,
                                         <<"_rev">> => Rev}),
    %% no second refresh -- the update stays in the buffer
    ok = wait_until(fun() -> search_ids(Corpus, <<"updated">>) =:= [<<"doc1">>] end, 100),
    ?assertEqual([<<"doc1">>], search_ids(Corpus, <<"updated">>)),
    ?assertEqual([], search_ids(Corpus, <<"original">>)).

%% Freeze a live doc into a segment, then delete it without refreshing
%% again: the tombstone sits in the buffer, unfrozen. The segment's stale
%% live entry must not surface the deleted doc.
buffered_delete_masks_older_segment_hit(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    {ok, R} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"doc1">>,
                                         <<"body">> => <<"connect_timeout soon deleted">>}),
    {ok, _} = barrel_ngram:refresh(Corpus),
    Rev = maps:get(<<"rev">>, R),
    {ok, _} = barrel_docdb:delete_doc(Db, <<"doc1">>, #{rev => Rev}),
    %% no second refresh -- the tombstone stays in the buffer
    ?assertEqual([], search_ids(Corpus, <<"connect_timeout">>)).

%% The same key can be a live candidate from two different, uncompacted
%% segments (frozen at different times, before any compaction collapses
%% them) -- the query must still resolve to exactly one hit, reflecting
%% current truth, not double-count or return a stale version.
same_key_across_multiple_segments_resolves_to_newest(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    {ok, R} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"doc1">>,
                                         <<"body">> => <<"connect_timeout first version">>}),
    {ok, _} = barrel_ngram:refresh(Corpus),   %% segment 1: "first version"
    Rev = maps:get(<<"rev">>, R),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"doc1">>,
                                         <<"body">> => <<"connect_timeout second version">>,
                                         <<"_rev">> => Rev}),
    {ok, _} = barrel_ngram:refresh(Corpus),   %% segment 2: "second version" (no compaction)
    ?assertEqual([], search_ids(Corpus, <<"first version">>)),
    ?assertEqual([<<"doc1">>], search_ids(Corpus, <<"second version">>)),
    ?assertEqual([<<"doc1">>], search_ids(Corpus, <<"connect_timeout">>)).

%% When `source' is configured, a buffer-derived candidate is verified
%% through it instead of barrel_docdb. Proved by deliberately diverging
%% the two: docdb's content does not contain the literal, the source's
%% (mem-backed) content for the same key does -- a hit proves the source
%% path fired.
source_configured_buffer_verification_uses_source(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    DataDir = ?config(data_dir, Config),
    ok = barrel_ngram:close(Corpus),
    SourceMap = #{<<"doc1">> => <<"totally different text with connect_timeout inside">>},
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir,
                                     source => {barrel_ngram_source_mem, SourceMap}}),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"doc1">>,
                                         <<"body">> => <<"nothing interesting in docdb">>}),
    %% no refresh -- doc1 stays in the buffer, reachable only via the
    %% async push subscription (poll), exercising the source-aware
    %% buffer verification path specifically
    ok = wait_until(fun() -> search_ids(Corpus, <<"connect_timeout">>) =:= [<<"doc1">>] end, 100),
    ?assertEqual([<<"doc1">>], search_ids(Corpus, <<"connect_timeout">>)),
    ?assertEqual([], search_ids(Corpus, <<"nothing interesting">>)).

%%====================================================================
%% Windowed literal verification (Step 5a: the positional lane end to end)
%%====================================================================

%% A literal frozen into a segment, with `source' configured: the reported
%% spans must match a brute-force binary:matches/2 scan exactly, proving
%% the distance-check-derived candidate start(s) survived windowed
%% verification and landed as the right byte positions.
windowed_search_returns_correct_spans(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    DataDir = ?config(data_dir, Config),
    Text = <<"error: connect_timeout exceeded in the connection pool budget again">>,
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"doc1">>, <<"body">> => Text}),
    {ok, _} = barrel_ngram:refresh(Corpus),
    ok = barrel_ngram:close(Corpus),
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir,
                                     phase2_selector_opts => ?POS_OPTS,
                                     source => {barrel_ngram_source_mem, #{<<"doc1">> => Text}}}),
    Literal = <<"connect_timeout">>,
    Expected = binary:matches(Text, Literal),
    {ok, [Hit]} = barrel_ngram:search(Corpus, Literal),
    ?assertEqual(<<"doc1">>, maps:get(id, Hit)),
    ?assertEqual(lists:sort(Expected), lists:sort(maps:get(spans, Hit))).

%% A literal occurring more than once in the same document: every real
%% occurrence must be independently verified and reported, not just the
%% first distance-check candidate found.
windowed_search_finds_multiple_occurrences(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    DataDir = ?config(data_dir, Config),
    Text = <<"connect_timeout here and also connect_timeout there for good measure">>,
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"doc1">>, <<"body">> => Text}),
    {ok, _} = barrel_ngram:refresh(Corpus),
    ok = barrel_ngram:close(Corpus),
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir,
                                     phase2_selector_opts => ?POS_OPTS,
                                     source => {barrel_ngram_source_mem, #{<<"doc1">> => Text}}}),
    Literal = <<"connect_timeout">>,
    Expected = binary:matches(Text, Literal),
    ?assert(length(Expected) >= 2),
    {ok, [Hit]} = barrel_ngram:search(Corpus, Literal),
    ?assertEqual(lists:sort(Expected), lists:sort(maps:get(spans, Hit))).

%% The proof that windowing is real, not just plausible-looking: wrap the
%% source in one that raises if any pread ever asks for more than
%% byte_size(Literal) bytes, over a document large enough that a
%% full-document read would look nothing like a windowed one. Search must
%% still find the real match without ever tripping the assertion.
pread_size_bounded_proof(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    DataDir = ?config(data_dir, Config),
    Padding = binary:copy(<<"padding words to bulk out this document ">>, 200),
    Text = <<Padding/binary, "error: connect_timeout exceeded in the pool">>,
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"doc1">>, <<"body">> => Text}),
    {ok, _} = barrel_ngram:refresh(Corpus),
    ok = barrel_ngram:close(Corpus),
    Literal = <<"connect_timeout">>,
    AssertSource = {barrel_ngram_source_assert_window,
                    {byte_size(Literal), #{<<"doc1">> => Text}}},
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir,
                                     phase2_selector_opts => ?POS_OPTS,
                                     source => AssertSource}),
    Expected = binary:matches(Text, Literal),
    {ok, [Hit]} = barrel_ngram:search(Corpus, Literal),
    ?assertEqual(lists:sort(Expected), lists:sort(maps:get(spans, Hit))).

%%====================================================================
%% Windowed regex verification (Step 6b: the positional lane, regex)
%%====================================================================

%% A regex that's a clean AND-chain (no anchors/alternation) with a
%% bounded gap between two literal runs: the reported spans must match a
%% brute-force re:run scan exactly, proving the chosen anchor's
%% distance-check candidates survived windowed regex verification and
%% landed at the right byte positions.
windowed_regex_search_returns_correct_spans(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    DataDir = ?config(data_dir, Config),
    Text = <<"error: connect_42_backoff_ms exceeded in the pool">>,
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"doc1">>, <<"body">> => Text}),
    {ok, _} = barrel_ngram:refresh(Corpus),
    ok = barrel_ngram:close(Corpus),
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir,
                                     phase2_selector_opts => ?POS_OPTS,
                                     source => {barrel_ngram_source_mem, #{<<"doc1">> => Text}}}),
    Regex = <<"connect_[0-9]{2}_backoff_ms">>,
    {ok, RE} = re:compile(Regex),
    {match, Matches} = re:run(Text, RE, [global, {capture, first, index}]),
    Expected = [{S, L} || [{S, L}] <- Matches],
    {ok, [Hit]} = barrel_ngram:regex(Corpus, Regex),
    ?assertEqual(<<"doc1">>, maps:get(id, Hit)),
    ?assertEqual(lists:sort(Expected), lists:sort(maps:get(spans, Hit))).

%% The regex analog of the literal path's prefix-widening proof: the real
%% full match ("[0-9]{2}connect_timeout") starts 2 bytes before the
%% chosen anchor literal's own position. If PrefixMax weren't correctly
%% widening the window backward, this match would never be found.
windowed_regex_bounded_prefix_widens_window(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    DataDir = ?config(data_dir, Config),
    Text = <<"code 42connect_timeout_error here">>,
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"doc1">>, <<"body">> => Text}),
    {ok, _} = barrel_ngram:refresh(Corpus),
    ok = barrel_ngram:close(Corpus),
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir,
                                     phase2_selector_opts => ?POS_OPTS,
                                     source => {barrel_ngram_source_mem, #{<<"doc1">> => Text}}}),
    Regex = <<"[0-9]{2}connect_timeout_error">>,
    {ok, RE} = re:compile(Regex),
    {match, Matches} = re:run(Text, RE, [global, {capture, first, index}]),
    Expected = [{S, L} || [{S, L}] <- Matches],
    ?assert(length(Expected) > 0),
    {ok, [Hit]} = barrel_ngram:regex(Corpus, Regex),
    ?assertEqual(lists:sort(Expected), lists:sort(maps:get(spans, Hit))).

%% The regex analog of the literal path's pread-size-bounded proof: a
%% source that raises if any read exceeds the regex's own computed
%% max window size, over a document padded far larger than that window.
%% Search must still find the real match without ever tripping it.
windowed_regex_pread_size_bounded_proof(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    DataDir = ?config(data_dir, Config),
    Padding = binary:copy(<<"padding words to bulk out this document ">>, 200),
    Text = <<Padding/binary, "error: connect_99_backoff_ms exceeded">>,
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"doc1">>, <<"body">> => Text}),
    {ok, _} = barrel_ngram:refresh(Corpus),
    ok = barrel_ngram:close(Corpus),
    Regex = <<"connect_[0-9]{2}_backoff_ms">>,
    %% PrefixMax/SuffixMax for either literal run here is comfortably
    %% under a few hundred bytes; the padding is over 8000 -- a generous
    %% max window still catches a full-document-sized read as a failure.
    MaxWindow = 512,
    AssertSource = {barrel_ngram_source_assert_max_window, {MaxWindow, #{<<"doc1">> => Text}}},
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir,
                                     phase2_selector_opts => ?POS_OPTS,
                                     source => AssertSource}),
    {ok, RE} = re:compile(Regex),
    {match, Matches} = re:run(Text, RE, [global, {capture, first, index}]),
    Expected = [{S, L} || [{S, L}] <- Matches],
    {ok, [Hit]} = barrel_ngram:regex(Corpus, Regex),
    ?assertEqual(lists:sort(Expected), lists:sort(maps:get(spans, Hit))).

%% `single_gram_candidates' (no second gram to distance-check against)
%% treats every occurrence of that one gram as a candidate. When the gram
%% also occurs many times elsewhere in the SAME document -- here,
%% `<<"aaconxy">>''s sole reliable phase-2 gram under ?POS_OPTS is "con"
%% at offset 2, and the padding scatters ~58 unrelated "con" occurrences
%% through it -- barrel_ngram_query falls back to one full-content confirm
%% for that document instead of one windowed read per candidate. Proved
%% by counting `source' calls (a naive implementation would pread~58
%% times), not just by checking the result, which is correct either way.
overflow_candidates_fall_back_to_full_confirm(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    DataDir = ?config(data_dir, Config),
    Literal = <<"aaconxy">>,
    MkChunk = fun(N) -> <<($0 + N rem 10), $z, "con", $z, ($0 + N rem 7), " ">> end,
    Padding = iolist_to_binary([MkChunk(N) || N <- lists:seq(1, 100)]),
    InsertPos = 300,
    <<Pre:InsertPos/binary, Post/binary>> = Padding,
    Text = <<Pre/binary, Literal/binary, Post/binary>>,
    Expected = binary:matches(Text, Literal),
    ?assertEqual(1, length(Expected)),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"doc1">>, <<"body">> => Text}),
    {ok, _} = barrel_ngram:refresh(Corpus),
    ok = barrel_ngram:close(Corpus),
    CounterKey = overflow_test_pread_count,
    erase(CounterKey),
    Source = {barrel_ngram_source_count_calls, {CounterKey, #{<<"doc1">> => Text}}},
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir,
                                     phase2_selector_opts => ?POS_OPTS, source => Source}),
    {ok, [Hit]} = barrel_ngram:search(Corpus, Literal),
    ?assertEqual(<<"doc1">>, maps:get(id, Hit)),
    ?assertEqual(Expected, maps:get(spans, Hit)),
    PreadCalls = case get(CounterKey) of undefined -> 0; N -> N end,
    ?assert(PreadCalls < 10).

%%====================================================================
%% Helpers
%%====================================================================

search_ids(Corpus, Literal) ->
    {ok, Hits} = barrel_ngram:search(Corpus, Literal),
    lists:sort([maps:get(id, H) || H <- Hits]).

%% Poll a predicate up to Attempts times, 50 ms apart (same idiom as
%% barrel_ngram_incremental_SUITE).
wait_until(_Pred, 0) ->
    {error, timeout};
wait_until(Pred, Attempts) ->
    case Pred() of
        true -> ok;
        false -> timer:sleep(50), wait_until(Pred, Attempts - 1)
    end.

group_by_gram(GramOffs) ->
    lists:foldl(
        fun({G, Off}, Acc) ->
            maps:update_with(G, fun(L) -> lists:usort([Off | L]) end, [Off], Acc)
        end, #{}, GramOffs).
