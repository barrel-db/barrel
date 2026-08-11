%%%-------------------------------------------------------------------
%%% @doc Step 7 edge-case coverage for the positional-index work: document
%%% boundaries, UTF-8 multibyte offsets, multiline content, an unsupported
%%% PCRE construct forcing (correct) full-scan, an anchored regex forcing
%%% full-content verification even when windowing infrastructure is
%%% present, an empty document's `{0,0}' `source' read, an invalid-UTF-8
%%% document hiding a real match, and a random-content property test
%%% against `binary:matches/2'.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_edge_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([doc_boundary_literal/1,
         doc_boundary_regex/1,
         utf8_multibyte_literal/1,
         utf8_multibyte_regex/1,
         multiline_content/1,
         unsupported_lookahead_forces_full_scan/1,
         anchored_regex_never_touches_source/1,
         empty_document_matches_bol_eol_via_buffer/1,
         invalid_document_encoding_hides_real_match/1,
         random_ascii_property_equivalence/1]).

-define(POS_OPTS, #{radius => 2, sample_rate => 2}).

all() ->
    [doc_boundary_literal,
     doc_boundary_regex,
     utf8_multibyte_literal,
     utf8_multibyte_regex,
     multiline_content,
     unsupported_lookahead_forces_full_scan,
     anchored_regex_never_touches_source,
     empty_document_matches_bol_eol_via_buffer,
     invalid_document_encoding_hides_real_match,
     random_ascii_property_equivalence].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    {ok, _} = application:ensure_all_started(barrel_ngram),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    Db = iolist_to_binary([<<"ngram_edge_">>, atom_to_binary(TC, utf8)]),
    Corpus = Db,
    DataDir = filename:join(?config(priv_dir, Config), atom_to_list(TC)),
    _ = barrel_docdb:delete_db(Db),
    {ok, _} = barrel_docdb:create_db(Db),
    [{db, Db}, {corpus, Corpus}, {data_dir, DataDir} | Config].

end_per_testcase(_TC, Config) ->
    Corpus = ?config(corpus, Config),
    Db = ?config(db, Config),
    _ = barrel_ngram:close(Corpus),
    _ = barrel_docdb:delete_db(Db),
    ok.

%%====================================================================
%% Document-boundary matches (windowed literal and regex)
%%====================================================================

%% A literal match sitting exactly at offset 0 (nothing before it) and one
%% ending exactly at the document's last byte (nothing after it), both
%% resolved through the real windowed candidate/distance-check/pread path
%% with `source' configured.
doc_boundary_literal(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    DataDir = ?config(data_dir, Config),
    AtStart = <<"connect_timeout begins this message and then trails off with padding">>,
    AtEnd = <<"this message has a lot of padding before it ends with connect_timeout">>,
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"start">>, <<"body">> => AtStart}),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"end">>, <<"body">> => AtEnd}),
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir,
                                     phase2_selector_opts => ?POS_OPTS,
                                     source => {barrel_ngram_source_mem,
                                               #{<<"start">> => AtStart, <<"end">> => AtEnd}}}),
    {ok, _} = barrel_ngram:refresh(Corpus),
    Literal = <<"connect_timeout">>,
    {ok, Hits} = barrel_ngram:search(Corpus, Literal),
    ?assertEqual(
       lists:sort([{<<"start">>, binary:matches(AtStart, Literal)},
                   {<<"end">>, binary:matches(AtEnd, Literal)}]),
       lists:sort([{maps:get(id, H), maps:get(spans, H)} || H <- Hits])).

%% The regex analog: PrefixMax must clamp at the document start (not read
%% a negative offset) when the anchor sits at offset 0, and SuffixMax must
%% clamp at the document end (not overrun) when the anchor's tail is the
%% document's last bytes.
doc_boundary_regex(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    DataDir = ?config(data_dir, Config),
    %% "42" (the bounded 2-digit prefix) sits at the very start
    PrefixAtStart = <<"42connect_timeout and then padding to bulk this document out">>,
    %% "99" (the bounded 2-digit suffix) is the document's last two bytes
    SuffixAtEnd = <<"padding to bulk this document out before connect_timeout99">>,
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"pstart">>, <<"body">> => PrefixAtStart}),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"send">>, <<"body">> => SuffixAtEnd}),
    Source = {barrel_ngram_source_mem,
             #{<<"pstart">> => PrefixAtStart, <<"send">> => SuffixAtEnd}},
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir,
                                     phase2_selector_opts => ?POS_OPTS, source => Source}),
    {ok, _} = barrel_ngram:refresh(Corpus),
    PrefixRe = <<"[0-9]{2}connect_timeout">>,
    {ok, [PrefixHit]} = barrel_ngram:regex(Corpus, PrefixRe),
    ?assertEqual(<<"pstart">>, maps:get(id, PrefixHit)),
    ?assertEqual(oracle_regex(PrefixAtStart, PrefixRe), maps:get(spans, PrefixHit)),
    SuffixRe = <<"connect_timeout[0-9]{2}">>,
    {ok, [SuffixHit]} = barrel_ngram:regex(Corpus, SuffixRe),
    ?assertEqual(<<"send">>, maps:get(id, SuffixHit)),
    ?assertEqual(oracle_regex(SuffixAtEnd, SuffixRe), maps:get(spans, SuffixHit)).

%%====================================================================
%% UTF-8 multibyte content around the match
%%====================================================================

%% Multi-byte characters before and around the literal must not perturb
%% the BYTE offsets windowed verification reports.
utf8_multibyte_literal(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    DataDir = ?config(data_dir, Config),
    Text = <<"café configuration for 日本語 support: connect_timeout while décodage runs"/utf8>>,
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"doc1">>, <<"body">> => Text}),
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir,
                                     phase2_selector_opts => ?POS_OPTS,
                                     source => {barrel_ngram_source_mem, #{<<"doc1">> => Text}}}),
    {ok, _} = barrel_ngram:refresh(Corpus),
    Literal = <<"connect_timeout">>,
    Expected = binary:matches(Text, Literal),
    ?assert(length(Expected) > 0),
    {ok, [Hit]} = barrel_ngram:search(Corpus, Literal),
    ?assertEqual(<<"doc1">>, maps:get(id, Hit)),
    ?assertEqual(Expected, maps:get(spans, Hit)).

%% Same proof for windowed regex: multi-byte characters sit both before
%% the chosen anchor and between the anchor and the class it's bounded by.
utf8_multibyte_regex(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    DataDir = ?config(data_dir, Config),
    Text = <<"日本語 prefix café: connect_timeout runs while décodage続く"/utf8>>,
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"doc1">>, <<"body">> => Text}),
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir,
                                     phase2_selector_opts => ?POS_OPTS,
                                     source => {barrel_ngram_source_mem, #{<<"doc1">> => Text}}}),
    {ok, _} = barrel_ngram:refresh(Corpus),
    Regex = <<"connect_[a-z]{7}">>,
    {ok, [Hit]} = barrel_ngram:regex(Corpus, Regex),
    ?assertEqual(<<"doc1">>, maps:get(id, Hit)),
    ?assertEqual(oracle_regex(Text, Regex), maps:get(spans, Hit)).

%%====================================================================
%% Multiline content
%%====================================================================

%% A literal spanning nothing unusual, just present on one line of a
%% 2-line and a 5-line document -- `\n' is an ordinary byte to the
%% windowed path, not a special case.
multiline_content(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    DataDir = ?config(data_dir, Config),
    TwoLines = <<"first line has nothing special\nsecond line has connect_timeout right here">>,
    FiveLines = <<"line one\nline two\nline three has connect_timeout in it\n"
                  "line four\nline five closes things out">>,
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"two">>, <<"body">> => TwoLines}),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"five">>, <<"body">> => FiveLines}),
    Source = {barrel_ngram_source_mem, #{<<"two">> => TwoLines, <<"five">> => FiveLines}},
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir,
                                     phase2_selector_opts => ?POS_OPTS, source => Source}),
    {ok, _} = barrel_ngram:refresh(Corpus),
    Literal = <<"connect_timeout">>,
    {ok, Hits} = barrel_ngram:search(Corpus, Literal),
    ?assertEqual(
       lists:sort([{<<"two">>, binary:matches(TwoLines, Literal)},
                   {<<"five">>, binary:matches(FiveLines, Literal)}]),
       lists:sort([{maps:get(id, H), maps:get(spans, H)} || H <- Hits])).

%%====================================================================
%% Unsupported PCRE construct: fail closed, not silently wrong
%%====================================================================

%% `(?=bar)' (a lookahead) is `unsupported' by the analyzer, so narrowing
%% is `all' (no trigram constraint at all) -- if the parser instead
%% silently mis-read the lookahead as the literal bytes `?=bar', the
%% derived (wrong) trigram query would never match real "foobar" content,
%% silently dropping the genuine match. `foo(?=bar)' matches "foo" only
%% when immediately followed by "bar" (zero-width, not consumed); one
%% document has "foobar" (must match), a near-identical one has "foobaz"
%% (must not) -- proving both directions against an independent `re:run'
%% oracle, with windowing infrastructure present (phase2_selector_opts +
%% source configured) to prove it doesn't get mis-narrowed even when the
%% corpus could otherwise support it.
unsupported_lookahead_forces_full_scan(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    DataDir = ?config(data_dir, Config),
    Match = <<"prefix content foobar padding more text to bulk this document out">>,
    NoMatch = <<"prefix content foobaz padding more text to bulk this document out">>,
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"m">>, <<"body">> => Match}),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"nm">>, <<"body">> => NoMatch}),
    Source = {barrel_ngram_source_mem, #{<<"m">> => Match, <<"nm">> => NoMatch}},
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir,
                                     phase2_selector_opts => ?POS_OPTS, source => Source}),
    {ok, _} = barrel_ngram:refresh(Corpus),
    Regex = <<"foo(?=bar)">>,
    ?assertEqual(unsupported, barrel_ngram_regex:analyze(Regex)),
    {ok, Hits} = barrel_ngram:regex(Corpus, Regex),
    ?assertEqual([<<"m">>], [maps:get(id, H) || H <- Hits]),
    [Hit] = Hits,
    ?assertEqual(oracle_regex(Match, Regex), maps:get(spans, Hit)).

%%====================================================================
%% Anchored regex: full-content verification, even when windowing
%% infrastructure is otherwise available
%%====================================================================

%% `^connect_timeout' and `connect_timeout$' must never take the windowed
%% path (a sliced read would break `^'/`$' zero-width semantics), even
%% with `phase2_selector_opts'/`source' configured. `source' is a spy
%% that raises on ANY pread (MaxWindow = 0); `regex_confirm/4' (the
%% full-content path) never touches `source' at all (uses `barrel_docdb'
%% directly), so surviving to correct results proves both that the match
%% is found correctly AND that no windowed read happened.
anchored_regex_never_touches_source(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    DataDir = ?config(data_dir, Config),
    AtStart = <<"connect_timeout begins right here with lots of trailing padding text "
               "to bulk this document out substantially so it looks nothing like a window">>,
    NotAtStart = <<"some prefix appears before connect_timeout shows up in this padded "
                  "document with plenty of extra bulk text trailing after it too">>,
    AtEnd = <<"some leading padding text here that goes on for quite a while before "
             "this document finally ends with connect_timeout">>,
    NotAtEnd = <<"some content here before connect_timeout appears mid-document with "
                "quite a bit more padding text following right after it before the end">>,
    Docs = #{<<"start">> => AtStart, <<"notstart">> => NotAtStart,
             <<"end">> => AtEnd, <<"notend">> => NotAtEnd},
    maps:foreach(fun(Id, Text) ->
                     {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => Id, <<"body">> => Text})
                 end, Docs),
    AssertSource = {barrel_ngram_source_assert_max_window, {0, Docs}},
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir,
                                     phase2_selector_opts => ?POS_OPTS,
                                     source => AssertSource}),
    {ok, _} = barrel_ngram:refresh(Corpus),
    {ok, StartHits} = barrel_ngram:regex(Corpus, <<"^connect_timeout">>),
    ?assertEqual([<<"start">>], [maps:get(id, H) || H <- StartHits]),
    {ok, EndHits} = barrel_ngram:regex(Corpus, <<"connect_timeout$">>),
    ?assertEqual([<<"end">>], [maps:get(id, H) || H <- EndHits]).

%%====================================================================
%% Empty document: the `Len =:= 0' -> `{ok, <<>>}' `source' contract
%%====================================================================

%% An empty (`doc_size =:= 0') document must still be readable via
%% `source' -- without the `Len =:= 0' special case, `Offset >= Size'
%% would reject even the trivial `{0, 0}' read as `eof', silently
%% dropping it. Only the BUFFER verification path (never phase-2 indexed)
%% actually calls `source:pread/4' for a document this small; a frozen
%% segment's full-scan confirm uses `barrel_docdb' directly regardless of
%% `source', so the document is kept unfrozen (never refreshed) to
%% exercise the real code path.
empty_document_matches_bol_eol_via_buffer(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    DataDir = ?config(data_dir, Config),
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir,
                                     source => {barrel_ngram_source_mem,
                                               #{<<"empty1">> => <<>>}}}),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"empty1">>, <<"body">> => <<>>}),
    ok = wait_until(fun() ->
                        {ok, Hits} = barrel_ngram:regex(Corpus, <<"^$">>),
                        Hits =/= []
                    end, 40),
    {ok, Hits} = barrel_ngram:regex(Corpus, <<"^$">>),
    ?assertEqual([#{id => <<"empty1">>, spans => [{0, 0}]}], Hits).

%%====================================================================
%% Invalid UTF-8 document encoding hides a real match
%%====================================================================

%% A document with invalid UTF-8 bytes AND a genuine matching substring
%% elsewhere: the abort must happen BEFORE the real match is found, not
%% be indistinguishable from "no match" -- proving this needs a document
%% that really does contain what would otherwise be a hit.
invalid_document_encoding_hides_real_match(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    DataDir = ?config(data_dir, Config),
    BadText = <<"connect ", 255, 255, " visit the CAFÉ today for real"/utf8>>,
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"doc1">>, <<"body">> => BadText}),
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir}),
    {ok, _} = barrel_ngram:refresh(Corpus),
    Literal = <<"café"/utf8>>,
    ?assertEqual({error, {invalid_document_encoding, <<"doc1">>}},
                 barrel_ngram:search(Corpus, Literal, #{case_sensitive => false})).

%%====================================================================
%% Random-content property test: full oracle equality, not one-directional
%%====================================================================

%% Every trigram over a 4-letter alphabet (64 combinations, guaranteeing
%% heavy collisions across a small random corpus) plus two longer embedded
%% needles, searched against 14 random documents: `search/2''s spans for
%% EVERY document must equal `binary:matches/2' exactly -- zero false
%% positives (an extra doc or span) and zero false negatives (a missing
%% one). `phase2_selector_opts => #{radius => 0, sample_rate => 1}' makes
%% every position of every gram reliable and sampled, so this exercises
%% the windowed/positional path (single-gram and distance-checked pair
%% lanes both, since the 3-byte trigrams and 12-byte needles need
%% different lane counts) for the whole sweep, not just the dense one.
random_ascii_property_equivalence(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    DataDir = ?config(data_dir, Config),
    Seed = gen_docs(14),
    lists:foreach(fun({Id, Text}) ->
                      {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => Id, <<"body">> => Text})
                  end, Seed),
    SourceMap = maps:from_list(Seed),
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir,
                                     phase2_selector_opts => #{radius => 0, sample_rate => 1},
                                     source => {barrel_ngram_source_mem, SourceMap}}),
    {ok, _} = barrel_ngram:refresh(Corpus),
    Literals = trigram_universe() ++ [<<"needle_alpha">>, <<"needle_beta">>,
                                      <<"aaaa">>, <<"dddd">>],
    lists:foreach(
        fun(Lit) ->
            Expected = expected_hits(SourceMap, Lit),
            {ok, Hits} = barrel_ngram:search(Corpus, Lit),
            Actual = normalize_hits(Hits),
            ?assertEqual({Lit, Expected}, {Lit, Actual})
        end, Literals).

%%====================================================================
%% Helpers
%%====================================================================

oracle_regex(Text, Regex) ->
    {ok, RE} = re:compile(Regex),
    case re:run(Text, RE, [global, {capture, first, index}]) of
        {match, Matches} -> [{S, L} || [{S, L}] <- Matches];
        nomatch -> []
    end.

%% Poll a predicate up to Attempts times, 50 ms apart (same idiom as
%% barrel_ngram_incremental_SUITE / barrel_ngram_positional_SUITE).
wait_until(_Pred, 0) ->
    {error, timeout};
wait_until(Pred, Attempts) ->
    case Pred() of
        true -> ok;
        false -> timer:sleep(50), wait_until(Pred, Attempts - 1)
    end.

gen_docs(N) ->
    Base = [{doc_id(I), random_ascii_text(40 + rand:uniform(60))} || I <- lists:seq(1, N)],
    [{Id1, Text1}, {Id2, Text2} | Rest] = Base,
    [{Id1, embed(Text1, <<"needle_alpha">>)}, {Id2, embed(Text2, <<"needle_beta">>)} | Rest].

doc_id(I) -> iolist_to_binary(io_lib:format("doc~4..0B", [I])).

random_ascii_text(N) ->
    Alphabet = "abcd",
    Len = length(Alphabet),
    list_to_binary([lists:nth(rand:uniform(Len), Alphabet) || _ <- lists:seq(1, N)]).

embed(Text, Needle) ->
    Pos = rand:uniform(byte_size(Text)) - 1,
    <<Pre:Pos/binary, Post/binary>> = Text,
    <<Pre/binary, Needle/binary, Post/binary>>.

trigram_universe() ->
    [list_to_binary([A, B, C]) || A <- "abcd", B <- "abcd", C <- "abcd"].

expected_hits(SourceMap, Lit) ->
    lists:sort(
      [{Id, lists:sort(binary:matches(Text, Lit))}
       || {Id, Text} <- maps:to_list(SourceMap), binary:matches(Text, Lit) =/= []]).

normalize_hits(Hits) ->
    lists:sort([{maps:get(id, H), lists:sort(maps:get(spans, H))} || H <- Hits]).
