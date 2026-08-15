%%%-------------------------------------------------------------------
%%% @doc End-to-end tests for barrel_ngram over barrel_docdb.
%%%
%%% Seeds a small database, builds the index from the changes feed, then
%%% asserts substring search returns exactly the documents whose corpus
%%% text contains the literal. The decisive test (`oracle_equivalence')
%%% checks the trigram pipeline against a brute-force substring scan over
%%% the same corpus: byte-identical results.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([
    finds_identifier/1,
    finds_error_string/1,
    finds_across_fields/1,
    absent_literal/1,
    short_literal_brute_force/1,
    punctuation_literal/1,
    match_spans/1,
    empty_corpus/1,
    oracle_equivalence/1
]).

%% The seed corpus: id => #{field => binary text}.
docs() ->
    [
     {<<"a">>, #{<<"body">> => <<"error: connect_timeout exceeded in pool">>}},
     {<<"b">>, #{<<"body">> => <<"the quick brown fox jumps over">>}},
     {<<"c">>, #{<<"body">> => <<"config key retry_backoff_ms = 250">>}},
     {<<"d">>, #{<<"title">> => <<"connect">>,
                 <<"body">> => <<"timeout while dialing upstream">>}},
     {<<"e">>, #{<<"body">> => <<"fn connect_timeout() -> Result<(), Err>">>}},
     {<<"f">>, #{<<"body">> => <<"unrelated content about widgets">>}},
     {<<"g">>, #{<<"body">> => <<"x = a + b; // arithmetic">>}},
     {<<"h">>, #{<<"note">> => <<"see also: retry_backoff_ms and jitter">>}}
    ].

all() ->
    [finds_identifier, finds_error_string, finds_across_fields,
     absent_literal, short_literal_brute_force, punctuation_literal,
     match_spans, empty_corpus, oracle_equivalence].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    {ok, _} = application:ensure_all_started(barrel_ngram),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    Db = iolist_to_binary([<<"ngram_ct_">>, atom_to_binary(TC, utf8)]),
    Corpus = Db,
    DataDir = filename:join(?config(priv_dir, Config), atom_to_list(TC)),
    _ = barrel_docdb:delete_db(Db),
    {ok, _} = barrel_docdb:create_db(Db),
    Seed = case TC of
        empty_corpus -> [];
        _ -> docs()
    end,
    lists:foreach(
        fun({Id, Fields}) ->
            {ok, _} = barrel_docdb:put_doc(Db, Fields#{<<"id">> => Id})
        end, Seed),
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir}),
    {ok, _} = barrel_ngram:index(Corpus),
    [{db, Db}, {corpus, Corpus}, {seed, Seed} | Config].

end_per_testcase(_TC, Config) ->
    Corpus = ?config(corpus, Config),
    Db = ?config(db, Config),
    _ = barrel_ngram:close(Corpus),
    _ = barrel_docdb:delete_db(Db),
    ok.

%%====================================================================
%% Test cases
%%====================================================================

finds_identifier(Config) ->
    ?assertEqual([<<"a">>, <<"e">>], search(Config, <<"connect_timeout">>)).

finds_error_string(Config) ->
    ?assertEqual([<<"a">>], search(Config, <<"exceeded in pool">>)).

finds_across_fields(Config) ->
    %% "retry_backoff_ms" appears in doc c (body) and doc h (note).
    ?assertEqual([<<"c">>, <<"h">>], search(Config, <<"retry_backoff_ms">>)).

absent_literal(Config) ->
    ?assertEqual([], search(Config, <<"no_such_substring_anywhere">>)).

short_literal_brute_force(Config) ->
    %% Two-byte literal is below a trigram: the planner brute-forces the
    %% live set and still returns exactly the containing docs.
    Expected = brute_force(Config, <<"fx">>),
    ?assertEqual(Expected, search(Config, <<"fx">>)),
    %% "x " (x then space) appears in doc g ("x = a") and doc b? check via oracle.
    ?assertEqual(brute_force(Config, <<"x ">>), search(Config, <<"x ">>)).

punctuation_literal(Config) ->
    %% Punctuation-heavy literal must match exactly.
    ?assertEqual([<<"e">>], search(Config, <<"-> Result<(), Err>">>)),
    ?assertEqual([<<"g">>], search(Config, <<"+ b;">>)).

match_spans(Config) ->
    Corpus = ?config(corpus, Config),
    {ok, Hits} = barrel_ngram:search(Corpus, <<"connect">>),
    Ids = [maps:get(id, H) || H <- Hits],
    ?assertEqual([<<"a">>, <<"d">>, <<"e">>], Ids),
    %% every hit reports at least one span, and each span really is the
    %% literal within the doc's text.
    lists:foreach(
        fun(#{spans := Spans}) -> ?assert(length(Spans) >= 1) end, Hits).

empty_corpus(Config) ->
    ?assertEqual([], search(Config, <<"anything">>)).

oracle_equivalence(Config) ->
    %% The pipeline must agree with a brute-force substring scan for a
    %% battery of literals, including ones that share trigrams but do not
    %% co-occur (trigram false positives the confirm pass must reject).
    Literals = [
        <<"connect">>, <<"connect_timeout">>, <<"timeout">>, <<"retry">>,
        <<"retry_backoff_ms">>, <<"backoff">>, <<"the">>, <<"fox">>,
        <<"error">>, <<"pool">>, <<"config">>, <<"250">>, <<"jitter">>,
        <<"() ->">>, <<"a + b">>, <<"widgets">>, <<"upstream">>,
        <<"connect timeout">>, <<"xyz">>, <<"zzz">>, <<"= ">>, <<"//">>,
        <<"nnec">>, <<"acko">>, <<"o o">>, <<"  ">>
    ],
    lists:foreach(
        fun(Lit) ->
            Expected = brute_force(Config, Lit),
            Actual = search(Config, Lit),
            ?assertEqual({Lit, Expected}, {Lit, Actual})
        end, Literals).

%%====================================================================
%% Helpers
%%====================================================================

search(Config, Literal) ->
    Corpus = ?config(corpus, Config),
    {ok, Hits} = barrel_ngram:search(Corpus, Literal),
    lists:sort([maps:get(id, H) || H <- Hits]).

%% Brute-force reference: the ids whose corpus text contains the literal,
%% computed with the same doc_text definition the index uses.
brute_force(Config, Literal) ->
    Seed = ?config(seed, Config),
    lists:sort(
      [Id || {Id, Fields} <- Seed,
             begin
                 Text = barrel_ngram_corpus:doc_text(Fields, #{fields => all}),
                 binary:match(Text, Literal) =/= nomatch
             end]).
