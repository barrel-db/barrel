%%%-------------------------------------------------------------------
%%% @doc Case-insensitive literal and regex search -- Step 5b (literal) and
%%% Step 6c (regex) of the positional-index work (see barrel_ngram_planner's
%%% moduledoc for the ASCII/non-ASCII split and why phase-2/windowed
%%% verification never apply to either).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_caseless_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([ascii_caseless_finds_mixed_case/1,
         non_ascii_caseless_full_scan/1,
         invalid_literal_encoding_rejected/1,
         invalid_document_encoding_aborts_unicode_query/1,
         metachar_literal_matches_only_literal_bytes/1,
         default_is_case_sensitive/1]).
-export([regex_case_sensitive_option_finds_mixed_case/1,
         regex_own_leading_i_matches_without_option/1,
         regex_non_ascii_caseless_full_scan/1,
         caseless_regex_never_touches_source/1,
         regex_invalid_pattern_encoding_rejected/1,
         regex_invalid_document_encoding_aborts_unicode_query/1]).

all() ->
    [ascii_caseless_finds_mixed_case, non_ascii_caseless_full_scan,
     invalid_literal_encoding_rejected, invalid_document_encoding_aborts_unicode_query,
     metachar_literal_matches_only_literal_bytes, default_is_case_sensitive,
     regex_case_sensitive_option_finds_mixed_case,
     regex_own_leading_i_matches_without_option,
     regex_non_ascii_caseless_full_scan,
     caseless_regex_never_touches_source,
     regex_invalid_pattern_encoding_rejected,
     regex_invalid_document_encoding_aborts_unicode_query].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    {ok, _} = application:ensure_all_started(barrel_ngram),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    Db = iolist_to_binary([<<"ngram_case_">>, atom_to_binary(TC, utf8)]),
    Corpus = Db,
    DataDir = filename:join(?config(priv_dir, Config), atom_to_list(TC)),
    _ = barrel_docdb:delete_db(Db),
    {ok, _} = barrel_docdb:create_db(Db),
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir}),
    [{db, Db}, {corpus, Corpus}, {data_dir, DataDir} | Config].

end_per_testcase(_TC, Config) ->
    _ = barrel_ngram:close(?config(corpus, Config)),
    _ = barrel_docdb:delete_db(?config(db, Config)),
    ok.

%%====================================================================
%% Test cases
%%====================================================================

%% An ASCII literal in lowercase must find mixed-case real content only
%% when case_sensitive => false; the default (case-sensitive) misses it.
ascii_caseless_finds_mixed_case(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    Text = <<"Error: Connect_Timeout exceeded">>,
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"doc1">>, <<"body">> => Text}),
    {ok, _} = barrel_ngram:refresh(Corpus),
    ?assertEqual({ok, []}, barrel_ngram:search(Corpus, <<"connect_timeout">>)),
    {ok, [Hit]} = barrel_ngram:search(Corpus, <<"connect_timeout">>, #{case_sensitive => false}),
    ?assertEqual(<<"doc1">>, maps:get(id, Hit)),
    ExpectedSpans = oracle_spans(Text, <<"connect_timeout">>, [caseless]),
    ?assertEqual(lists:sort(ExpectedSpans), lists:sort(maps:get(spans, Hit))).

%% A non-ASCII literal never narrows (phase-1 query is `all'); it still
%% finds a case-variant match via a full Unicode-caseless scan.
non_ascii_caseless_full_scan(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    Text = <<"visit the café today"/utf8>>,
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"doc1">>, <<"body">> => Text}),
    {ok, _} = barrel_ngram:refresh(Corpus),
    Literal = <<"CAFÉ"/utf8>>,
    {ok, [Hit]} = barrel_ngram:search(Corpus, Literal, #{case_sensitive => false}),
    ?assertEqual(<<"doc1">>, maps:get(id, Hit)),
    ExpectedSpans = oracle_spans(Text, Literal, [caseless, unicode]),
    ?assertEqual(lists:sort(ExpectedSpans), lists:sort(maps:get(spans, Hit))).

%% A literal with a non-ASCII byte that is not itself valid UTF-8 has no
%% sound path through either branch -- rejected outright, not silently
%% treated as ASCII or as an empty result.
invalid_literal_encoding_rejected(Config) ->
    Corpus = ?config(corpus, Config),
    BadLiteral = <<"caf", 255, 255>>,
    ?assertEqual({error, {invalid_literal_encoding, BadLiteral}},
                 barrel_ngram:search(Corpus, BadLiteral, #{case_sensitive => false})).

%% A candidate document whose corpus text is not valid UTF-8 must abort
%% the WHOLE unicode-mode query with a clear error, never be silently
%% skipped as if it were just a non-match (a real match elsewhere in that
%% same document could otherwise disappear without any signal).
invalid_document_encoding_aborts_unicode_query(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    BadText = <<"connect ", 255, 255, " timeout">>,
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"doc1">>, <<"body">> => BadText}),
    {ok, _} = barrel_ngram:refresh(Corpus),
    %% a non-ASCII literal forces unicode mode with no narrowing (`all'),
    %% so doc1 is a candidate regardless of its own content
    Literal = <<"café"/utf8>>,
    ?assertEqual({error, {invalid_document_encoding, <<"doc1">>}},
                 barrel_ngram:search(Corpus, Literal, #{case_sensitive => false})).

%% Proves the escaping helper is really in effect end to end, not just at
%% the unit level: a caseless literal containing a regex metacharacter
%% matches only the literal bytes, not an arbitrary character in its place.
metachar_literal_matches_only_literal_bytes(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"doc1">>, <<"body">> => <<"A.B">>}),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"doc2">>, <<"body">> => <<"AxB">>}),
    {ok, _} = barrel_ngram:refresh(Corpus),
    {ok, Hits} = barrel_ngram:search(Corpus, <<"a.b">>, #{case_sensitive => false}),
    ?assertEqual([<<"doc1">>], [maps:get(id, H) || H <- Hits]).

default_is_case_sensitive(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"doc1">>, <<"body">> => <<"Connect_Timeout">>}),
    {ok, _} = barrel_ngram:refresh(Corpus),
    ?assertEqual({ok, []}, barrel_ngram:search(Corpus, <<"connect_timeout">>)),
    {ok, Hits} = barrel_ngram:search(Corpus, <<"Connect_Timeout">>),
    ?assertEqual([<<"doc1">>], [maps:get(id, H) || H <- Hits]).

%%====================================================================
%% Case-insensitive regex -- Step 6c
%%====================================================================

%% `case_sensitive => false' on an ASCII regex works the same as it does
%% for a literal query: mixed-case real content is found, the default
%% (case-sensitive) misses it.
regex_case_sensitive_option_finds_mixed_case(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    Text = <<"Error: Connect_99_Backoff exceeded">>,
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"doc1">>, <<"body">> => Text}),
    {ok, _} = barrel_ngram:refresh(Corpus),
    Regex = <<"connect_[0-9]{2}_backoff">>,
    ?assertEqual({ok, []}, barrel_ngram:regex(Corpus, Regex)),
    {ok, [Hit]} = barrel_ngram:regex(Corpus, Regex, #{case_sensitive => false}),
    ?assertEqual(<<"doc1">>, maps:get(id, Hit)),
    ?assertEqual(lists:sort(oracle_regex_spans(Text, Regex, [caseless])),
                 lists:sort(maps:get(spans, Hit))).

%% A pattern with its own leading `(?i)' is caseless regardless of `Opts'
%% (the default `case_sensitive => true' does not override the pattern's
%% own declared semantics) -- no explicit option is needed.
regex_own_leading_i_matches_without_option(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    Text = <<"Error: Connect_99_Backoff exceeded">>,
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"doc1">>, <<"body">> => Text}),
    {ok, _} = barrel_ngram:refresh(Corpus),
    Regex = <<"(?i)connect_[0-9]{2}_backoff">>,
    {ok, [Hit]} = barrel_ngram:regex(Corpus, Regex),
    ?assertEqual(<<"doc1">>, maps:get(id, Hit)),
    ?assertEqual(lists:sort(oracle_regex_spans(Text, Regex, [])),
                 lists:sort(maps:get(spans, Hit))).

%% A non-ASCII regex never narrows (phase-1 query is `all'); it still
%% finds a case-variant match via a full Unicode-caseless scan.
regex_non_ascii_caseless_full_scan(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    Text = <<"visit the café today"/utf8>>,
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"doc1">>, <<"body">> => Text}),
    {ok, _} = barrel_ngram:refresh(Corpus),
    Regex = <<"CAFÉ[a-z ]+"/utf8>>,
    {ok, [Hit]} = barrel_ngram:regex(Corpus, Regex, #{case_sensitive => false}),
    ?assertEqual(<<"doc1">>, maps:get(id, Hit)),
    ?assertEqual(lists:sort(oracle_regex_spans(Text, Regex, [caseless, unicode])),
                 lists:sort(maps:get(spans, Hit))).

%% THE proof that a caseless regex never takes the windowed path even
%% when the same pattern would otherwise be windowed-eligible in
%% case-sensitive mode (see regex_plan_windowed_for_eligible_pattern_test
%% in barrel_ngram_planner_tests): `source' is a spy that raises on any
%% pread at all (MaxWindow = 0), so surviving to a correct hit proves
%% verification never touched `source' -- it went through the ordinary
%% docdb full-content path, not a windowed read.
caseless_regex_never_touches_source(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    DataDir = ?config(data_dir, Config),
    Text = <<"error: Connect_99_Backoff_MS exceeded">>,
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"doc1">>, <<"body">> => Text}),
    {ok, _} = barrel_ngram:refresh(Corpus),
    ok = barrel_ngram:close(Corpus),
    AssertSource = {barrel_ngram_source_assert_max_window, {0, #{<<"doc1">> => Text}}},
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir, source => AssertSource}),
    Regex = <<"connect_[0-9]{2}_backoff_ms">>,
    {ok, [Hit]} = barrel_ngram:regex(Corpus, Regex, #{case_sensitive => false}),
    ?assertEqual(<<"doc1">>, maps:get(id, Hit)),
    ?assertEqual(lists:sort(oracle_regex_spans(Text, Regex, [caseless])),
                 lists:sort(maps:get(spans, Hit))).

%% A regex pattern with a non-ASCII byte that is not itself valid UTF-8
%% has no sound path through either branch -- rejected outright.
regex_invalid_pattern_encoding_rejected(Config) ->
    Corpus = ?config(corpus, Config),
    BadRegex = <<"caf", 255, 255, "[0-9]+">>,
    ?assertEqual({error, {invalid_literal_encoding, BadRegex}},
                 barrel_ngram:regex(Corpus, BadRegex, #{case_sensitive => false})).

%% A candidate document whose corpus text is not valid UTF-8 must abort
%% the WHOLE unicode-mode regex query, never be silently skipped.
regex_invalid_document_encoding_aborts_unicode_query(Config) ->
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    BadText = <<"connect ", 255, 255, " timeout">>,
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"doc1">>, <<"body">> => BadText}),
    {ok, _} = barrel_ngram:refresh(Corpus),
    Regex = <<"café[a-z]*"/utf8>>,
    ?assertEqual({error, {invalid_document_encoding, <<"doc1">>}},
                 barrel_ngram:regex(Corpus, Regex, #{case_sensitive => false})).

%%====================================================================
%% Helpers
%%====================================================================

%% Independent oracle: compile the RAW literal (not via barrel_ngram_planner,
%% which is what's under test) directly with `re', escaping metacharacters
%% by hand via re:compile's own quoting -- Erlang's re has no `literal'
%% option usable with `caseless' (see barrel_ngram_planner's moduledoc), so
%% build the pattern the same primitive way, independently of the module
%% under test.
oracle_spans(Text, Literal, REOpts) ->
    Escaped = iolist_to_binary(
                [case B of
                     $\\ -> <<"\\\\">>; $. -> <<"\\.">>; $^ -> <<"\\^">>;
                     $$ -> <<"\\$">>; $| -> <<"\\|">>; $? -> <<"\\?">>;
                     $* -> <<"\\*">>; $+ -> <<"\\+">>; $( -> <<"\\(">>;
                     $) -> <<"\\)">>; $[ -> <<"\\[">>; $] -> <<"\\]">>;
                     ${ -> <<"\\{">>; $} -> <<"\\}">>; _ -> <<B>>
                 end || <<B>> <= Literal]),
    {ok, RE} = re:compile(Escaped, REOpts),
    case re:run(Text, RE, [global, {capture, first, index}]) of
        {match, Matches} -> [{S, L} || [{S, L}] <- Matches];
        nomatch -> []
    end.

%% Independent oracle for the regex tests: `Regex' compiled RAW (never
%% through escape_literal/1 -- it's a real pattern here, not a literal).
oracle_regex_spans(Text, Regex, REOpts) ->
    {ok, RE} = re:compile(Regex, REOpts),
    case re:run(Text, RE, [global, {capture, first, index}]) of
        {match, Matches} -> [{S, L} || [{S, L}] <- Matches];
        nomatch -> []
    end.
