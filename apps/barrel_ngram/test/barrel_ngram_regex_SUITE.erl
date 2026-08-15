%%%-------------------------------------------------------------------
%%% @doc Regex differential-oracle tests (M5).
%%%
%%% `barrel_ngram:regex/2' must equal a brute-force `re:run' scan over
%%% every document, for a battery of regexes, trigram-accelerated over a
%%% corpus's dense index, and across the live lifecycle. This proves the
%%% trigram query is sound.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_regex_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([oracle_dense/1, oracle_lifecycle/1, bad_regex/1,
         segment_error_propagates/1, extended_mode_oracle/1]).

-define(RE_LIMIT, 100000).

all() ->
    [oracle_dense, oracle_lifecycle, bad_regex, segment_error_propagates,
     extended_mode_oracle].

docs() ->
    [
     {<<"a">>, <<"error: connect_timeout exceeded in the connection pool">>},
     {<<"b">>, <<"the quick brown fox jumps over the lazy dog">>},
     {<<"c">>, <<"config key retry_backoff_ms = 250 with jitter budget">>},
     {<<"d">>, <<"fn connect_timeout() -> Result<(), Err> { retry() }">>},
     {<<"e">>, <<"unrelated content about widgets and gadgets 42">>},
     {<<"f">>, <<"x = a + b; // arithmetic on the upstream pool budget">>},
     {<<"g">>, <<"see also retry_backoff_ms and the jitter budget 999">>},
     {<<"h">>, <<"connection pool exhausted, connect_timeout hit twice">>}
    ].

regexes() ->
    [<<"connect_timeout">>, <<"connect_\\w+">>, <<"retry_\\w+_ms">>,
     <<"foo|bar|pool">>, <<"conn.ction">>, <<"error.*pool">>, <<"[0-9]+">>,
     <<"\\d\\d\\d">>, <<"^config">>, <<"budget$">>, <<"pool">>,
     <<"widget|gadget">>, <<"co">>, <<"nonexistent_zzz">>,
     <<"-> Result">>, <<"retry(_backoff)?">>, <<"[a-z]+_timeout">>,
     <<"the (quick|lazy)">>].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    {ok, _} = application:ensure_all_started(barrel_ngram),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    Db = iolist_to_binary([<<"ngram_re_">>, atom_to_binary(TC, utf8)]),
    Dense = <<Db/binary, "_dense">>,
    Base = filename:join(?config(priv_dir, Config), atom_to_list(TC)),
    _ = barrel_docdb:delete_db(Db),
    {ok, _} = barrel_docdb:create_db(Db),
    ok = barrel_ngram:open(Dense,
                           #{db => Db, data_dir => filename:join(Base, "dense"),
                             compact_threshold => infinity}),
    [{db, Db}, {dense, Dense} | Config].

end_per_testcase(_TC, Config) ->
    _ = barrel_ngram:close(?config(dense, Config)),
    _ = barrel_docdb:delete_db(?config(db, Config)),
    ok.

%%====================================================================
%% Test cases
%%====================================================================

oracle_dense(Config) ->
    Db = ?config(db, Config),
    Seed = seed(Db, docs()),
    {ok, _} = barrel_ngram:refresh(?config(dense, Config)),
    assert_oracle(?config(dense, Config), Seed).

oracle_lifecycle(Config) ->
    Db = ?config(db, Config),
    Revs = maps:from_list([{Id, put_doc(Db, Id, T)} || {Id, T} <- docs()]),
    {ok, _} = barrel_ngram:refresh(?config(dense, Config)),
    %% update one, delete another, add a fresh one
    _ = put_doc(Db, <<"a">>, <<"alpha now: connect_timeout and pool budget both">>,
                maps:get(<<"a">>, Revs)),
    ok = del_doc(Db, <<"b">>, maps:get(<<"b">>, Revs)),
    _ = put_doc(Db, <<"z">>, <<"new doc with retry_backoff_ms and jitter 777">>),
    {ok, _} = barrel_ngram:refresh(?config(dense, Config)),
    {ok, _} = barrel_ngram:compact(?config(dense, Config)),
    Seed = current_seed(Db),
    assert_oracle(?config(dense, Config), Seed).

bad_regex(Config) ->
    ?assertMatch({error, {bad_regex, _}},
                 barrel_ngram:regex(?config(dense, Config), <<"(unclosed">>)),
    ?assertMatch({error, {bad_regex, _}},
                 barrel_ngram:regex(?config(dense, Config), <<"[z-a]">>)).

%% A segment that becomes unreadable between the shard's snapshot and the
%% query opening it (e.g. a concurrent compaction deletes it, or it is
%% corrupted on disk) must fail the whole query loudly, not silently
%% return an incomplete result -- both regex_segment_keys/2 (the fix) and
%% literal search's segment_keys/2 (already correct, checked here for
%% parity) must propagate. Corrupting the file while the corpus stays
%% open (no reopen) is what exercises the query-time path specifically:
%% barrel_ngram_config_SUITE covers the separate open-time eager check.
segment_error_propagates(Config) ->
    Db = ?config(db, Config),
    Dense = ?config(dense, Config),
    _ = seed(Db, docs()),
    {ok, _} = barrel_ngram:refresh(Dense),
    {ok, [{_Gen, Path}]} = barrel_ngram_shard:get_manifest(Dense),
    ok = corrupt_magic(Path),
    ?assertEqual({error, invalid_magic}, barrel_ngram:regex(Dense, <<"connect_timeout">>)),
    ?assertEqual({error, invalid_magic}, barrel_ngram:search(Dense, <<"connect_timeout">>)).

%% Finding 3: `(?x)' extended mode against real content, through the
%% actual trigram-accelerated query path, compared against re:run with
%% `[extended]' compiled directly (an independent route to the same
%% semantics: barrel_ngram:regex/2 sees the `(?x)'-prefixed pattern --
%% this module's own inline-flag path -- the oracle reference compiles
%% the SAME body without the prefix but with re:compile's `[extended]'
%% option instead).
extended_mode_oracle(Config) ->
    Db = ?config(db, Config),
    Seed = seed(Db, docs()),
    {ok, _} = barrel_ngram:refresh(?config(dense, Config)),
    Cases = [{<<"(?x) connect _ timeout">>, <<" connect _ timeout">>},
             {<<"(?x) retry _ \\w+ _ ms # trailing comment\n">>,
              <<" retry _ \\w+ _ ms # trailing comment\n">>},
             {<<"(?x) [0-9] + \\s* budget">>, <<" [0-9] + \\s* budget">>}],
    lists:foreach(
        fun({Inline, Body}) ->
            {ok, RE} = re:compile(Body, [extended]),
            Expected = lists:sort(
                [Id || {Id, Text} <- maps:to_list(Seed), re_matches(Text, RE)]),
            Actual = regex(?config(dense, Config), Inline),
            ?assertEqual({Inline, Expected}, {Inline, Actual})
        end, Cases).

re_matches(Text, RE) ->
    DocText = barrel_ngram_corpus:doc_text(#{<<"body">> => Text}, #{fields => all}),
    case re:run(DocText, RE,
                [global, {capture, first, index},
                 {match_limit, ?RE_LIMIT},
                 {match_limit_recursion, ?RE_LIMIT}]) of
        {match, _} -> true;
        _ -> false
    end.

%%====================================================================
%% Helpers
%%====================================================================

corrupt_magic(Path) ->
    {ok, Bin} = file:read_file(Path),
    <<_OldMagic:8/binary, Rest/binary>> = Bin,
    file:write_file(Path, <<"BADMAGIC", Rest/binary>>).

assert_oracle(Corpus, Seed) ->
    lists:foreach(
        fun(Re) ->
            Expected = brute_force(Seed, Re),
            Actual = regex(Corpus, Re),
            ?assertEqual({Re, Expected}, {Re, Actual})
        end, regexes()).

regex(Corpus, Re) ->
    {ok, Hits} = barrel_ngram:regex(Corpus, Re),
    lists:sort([maps:get(id, H) || H <- Hits]).

%% brute-force reference over the corpus text, same re:run semantics.
brute_force(Seed, Re) ->
    {ok, RE} = re:compile(Re),
    lists:sort(
      [Id || {Id, Text} <- maps:to_list(Seed),
             begin
                 DocText = barrel_ngram_corpus:doc_text(#{<<"body">> => Text},
                                                        #{fields => all}),
                 case re:run(DocText, RE,
                             [global, {capture, first, index},
                              {match_limit, ?RE_LIMIT},
                              {match_limit_recursion, ?RE_LIMIT}]) of
                     {match, _} -> true;
                     _ -> false
                 end
             end]).

seed(Db, Docs) ->
    lists:foreach(fun({Id, T}) -> put_doc(Db, Id, T) end, Docs),
    maps:from_list(Docs).

%% Current live docs (id -> body) straight from the database.
current_seed(Db) ->
    {ok, Changes, _} = barrel_docdb:get_changes(Db, first, #{include_docs => true}),
    maps:from_list(
      [{maps:get(id, C), maps:get(<<"body">>, maps:get(doc, C))}
       || C <- Changes, maps:get(deleted, C, false) =:= false]).

put_doc(Db, Id, Text) ->
    put_doc(Db, Id, Text, undefined).

put_doc(Db, Id, Text, Rev) ->
    Doc0 = #{<<"id">> => Id, <<"body">> => Text},
    Doc = case Rev of
        undefined -> Doc0;
        _ -> Doc0#{<<"_rev">> => Rev}
    end,
    {ok, R} = barrel_docdb:put_doc(Db, Doc),
    maps:get(<<"rev">>, R).

del_doc(Db, Id, Rev) ->
    {ok, _} = barrel_docdb:delete_doc(Db, Id, #{rev => Rev}),
    ok.
