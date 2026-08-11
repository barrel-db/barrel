%%%-------------------------------------------------------------------
%%% @doc Roaring-vs-varint differential oracle (M8).
%%%
%%% The same database indexed under a `postings => varint' corpus and a
%%% `postings => roaring' corpus must return byte-identical `search' and
%%% `regex' results. Covered statically and across the lifecycle
%%% (updates/deletes/compaction).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_roaring_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([oracle_static/1, oracle_lifecycle/1]).

all() ->
    [oracle_static, oracle_lifecycle].

docs() ->
    [{iolist_to_binary([<<"doc">>, integer_to_binary(K)]), body(K)}
     || K <- lists:seq(1, 40)].

body(K) ->
    Words = [<<"connect_timeout">>, <<"retry_backoff_ms">>, <<"jitter">>,
             <<"pool">>, <<"budget">>, <<"upstream">>, <<"error">>,
             <<"config">>, <<"widget">>, <<"the quick brown fox">>],
    Pick = [lists:nth(1 + (K * P) rem length(Words), Words) || P <- [1, 3, 7, 2]],
    iolist_to_binary(lists:join(<<" ">>, Pick ++ [integer_to_binary(K)])).

literals() ->
    [<<"connect_timeout">>, <<"retry_backoff_ms">>, <<"jitter">>, <<"pool">>,
     <<"budget">>, <<"the quick">>, <<"co">>, <<"nonexistent">>, <<"1">>].

regexes() ->
    [<<"connect_\\w+">>, <<"retry_\\w+_ms">>, <<"pool|budget">>,
     <<"[0-9]+">>, <<"jitter">>, <<"nonexistent_zzz">>].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    {ok, _} = application:ensure_all_started(barrel_ngram),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    Db = iolist_to_binary([<<"ngram_roar_">>, atom_to_binary(TC, utf8)]),
    Base = filename:join(?config(priv_dir, Config), atom_to_list(TC)),
    _ = barrel_docdb:delete_db(Db),
    {ok, _} = barrel_docdb:create_db(Db),
    V = <<Db/binary, "_v">>,
    R = <<Db/binary, "_r">>,
    ok = barrel_ngram:open(V, #{db => Db, data_dir => filename:join(Base, "v"),
                                postings => varint, compact_threshold => infinity}),
    ok = barrel_ngram:open(R, #{db => Db, data_dir => filename:join(Base, "r"),
                                postings => roaring, compact_threshold => infinity}),
    [{db, Db}, {varint, V}, {roaring, R} | Config].

end_per_testcase(_TC, Config) ->
    _ = barrel_ngram:close(?config(varint, Config)),
    _ = barrel_ngram:close(?config(roaring, Config)),
    _ = barrel_docdb:delete_db(?config(db, Config)),
    ok.

%%====================================================================
%% Test cases
%%====================================================================

oracle_static(Config) ->
    Db = ?config(db, Config),
    lists:foreach(fun({Id, T}) -> put_doc(Db, Id, T) end, docs()),
    refresh_both(Config),
    assert_agree(Config).

oracle_lifecycle(Config) ->
    Db = ?config(db, Config),
    Revs = maps:from_list([{Id, put_doc(Db, Id, T)} || {Id, T} <- docs()]),
    refresh_both(Config),
    _ = put_doc(Db, <<"doc1">>, <<"now connect_timeout and pool and budget too">>,
                maps:get(<<"doc1">>, Revs)),
    ok = del_doc(Db, <<"doc2">>, maps:get(<<"doc2">>, Revs)),
    _ = put_doc(Db, <<"doc99">>, <<"fresh retry_backoff_ms budget pool 7">>),
    refresh_both(Config),
    {ok, _} = barrel_ngram:compact(?config(varint, Config)),
    {ok, _} = barrel_ngram:compact(?config(roaring, Config)),
    assert_agree(Config).

%%====================================================================
%% Helpers
%%====================================================================

assert_agree(Config) ->
    V = ?config(varint, Config),
    R = ?config(roaring, Config),
    lists:foreach(
        fun(L) -> ?assertEqual({search, L, search(V, L)}, {search, L, search(R, L)}) end,
        literals()),
    lists:foreach(
        fun(Re) -> ?assertEqual({regex, Re, regex(V, Re)}, {regex, Re, regex(R, Re)}) end,
        regexes()).

refresh_both(Config) ->
    {ok, _} = barrel_ngram:refresh(?config(varint, Config)),
    {ok, _} = barrel_ngram:refresh(?config(roaring, Config)),
    ok.

search(Corpus, L) ->
    {ok, Hits} = barrel_ngram:search(Corpus, L),
    lists:sort([maps:get(id, H) || H <- Hits]).

regex(Corpus, Re) ->
    {ok, Hits} = barrel_ngram:regex(Corpus, Re),
    lists:sort([maps:get(id, H) || H <- Hits]).

put_doc(Db, Id, Text) ->
    put_doc(Db, Id, Text, undefined).

put_doc(Db, Id, Text, Rev) ->
    Doc0 = #{<<"id">> => Id, <<"body">> => Text},
    Doc = case Rev of
        undefined -> Doc0;
        _ -> Doc0#{<<"_rev">> => Rev}
    end,
    {ok, Res} = barrel_docdb:put_doc(Db, Doc),
    maps:get(<<"rev">>, Res).

del_doc(Db, Id, Rev) ->
    {ok, _} = barrel_docdb:delete_doc(Db, Id, #{rev => Rev}),
    ok.
