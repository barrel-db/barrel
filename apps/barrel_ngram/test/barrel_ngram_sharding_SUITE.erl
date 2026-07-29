%%%-------------------------------------------------------------------
%%% @doc Sharded-vs-unsharded oracle (M6).
%%%
%%% The same database indexed under a single-shard corpus and an N-shard
%%% corpus must return byte-identical `search' and `regex' results, since
%%% each document is owned by exactly one shard. Covered statically and
%%% across the lifecycle, plus a distribution check that ownership is
%%% partitioned (every id in a shard hashes to it, none missing or double).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_sharding_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([oracle_static/1, oracle_lifecycle/1, distribution/1]).

-define(N, 4).

all() ->
    [oracle_static, oracle_lifecycle, distribution].

docs() ->
    [{iolist_to_binary([<<"doc">>, integer_to_binary(K)]), body(K)}
     || K <- lists:seq(1, 30)].

body(K) ->
    Words = [<<"connect_timeout">>, <<"retry_backoff_ms">>, <<"jitter">>,
             <<"pool">>, <<"budget">>, <<"upstream">>, <<"error">>,
             <<"config">>, <<"widget">>, <<"the quick brown fox">>],
    Pick = [lists:nth(1 + (K * P) rem length(Words), Words) || P <- [1, 3, 7]],
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
    Db = iolist_to_binary([<<"ngram_shard_">>, atom_to_binary(TC, utf8)]),
    One = <<Db/binary, "_one">>,
    Many = <<Db/binary, "_many">>,
    Base = filename:join(?config(priv_dir, Config), atom_to_list(TC)),
    _ = barrel_docdb:delete_db(Db),
    {ok, _} = barrel_docdb:create_db(Db),
    ok = barrel_ngram:open(One, #{db => Db, data_dir => filename:join(Base, "one"),
                                  compact_threshold => infinity}),
    ok = barrel_ngram:open(Many, #{db => Db, data_dir => filename:join(Base, "many"),
                                   shards => ?N, compact_threshold => infinity}),
    [{db, Db}, {one, One}, {many, Many} | Config].

end_per_testcase(_TC, Config) ->
    _ = barrel_ngram:close(?config(one, Config)),
    _ = barrel_ngram:close(?config(many, Config)),
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
    %% update a few, delete a few, add one
    _ = put_doc(Db, <<"doc1">>, <<"now connect_timeout and pool and budget too">>,
                maps:get(<<"doc1">>, Revs)),
    _ = put_doc(Db, <<"doc2">>, <<"changed to jitter upstream widget 999">>,
                maps:get(<<"doc2">>, Revs)),
    ok = del_doc(Db, <<"doc3">>, maps:get(<<"doc3">>, Revs)),
    ok = del_doc(Db, <<"doc4">>, maps:get(<<"doc4">>, Revs)),
    _ = put_doc(Db, <<"doc99">>, <<"fresh retry_backoff_ms budget pool 42">>),
    refresh_both(Config),
    {ok, _} = barrel_ngram:compact(?config(one, Config)),
    {ok, _} = barrel_ngram:compact(?config(many, Config)),
    assert_agree(Config).

distribution(Config) ->
    Db = ?config(db, Config),
    Many = ?config(many, Config),
    Ids = [begin _ = put_doc(Db, Id, T), Id end || {Id, T} <- docs()],
    {ok, _} = barrel_ngram:refresh(Many),
    PerShard = [{I, shard_keys(Many, I)} || I <- lists:seq(0, ?N - 1)],
    %% every id in a shard hashes to that shard
    lists:foreach(
        fun({I, Keys}) ->
            lists:foreach(
                fun(K) -> ?assertEqual(I, barrel_ngram_shards:shard_for(K, ?N)) end,
                Keys)
        end, PerShard),
    %% the shards partition the live ids exactly (none missing, none doubled)
    AllShardKeys = lists:append([Ks || {_I, Ks} <- PerShard]),
    ?assertEqual(length(AllShardKeys), length(lists:usort(AllShardKeys))),
    ?assertEqual(lists:sort(Ids), lists:sort(AllShardKeys)).

%%====================================================================
%% Helpers
%%====================================================================

assert_agree(Config) ->
    One = ?config(one, Config),
    Many = ?config(many, Config),
    lists:foreach(
        fun(L) ->
            ?assertEqual({search, L, search(One, L)}, {search, L, search(Many, L)})
        end, literals()),
    lists:foreach(
        fun(Re) ->
            ?assertEqual({regex, Re, regex(One, Re)}, {regex, Re, regex(Many, Re)})
        end, regexes()).

shard_keys(Corpus, I) ->
    {ok, Segs} = barrel_ngram_shard:get_manifest({Corpus, I}),
    lists:usort(lists:append([seg_live_keys(Path) || {_Gen, Path} <- Segs])).

seg_live_keys(Path) ->
    {ok, H} = barrel_ngram_segment:open(Path),
    try
        [K || {_O, K, _Hlc, Deleted} <- barrel_ngram_segment:entries(H), not Deleted]
    after
        barrel_ngram_segment:close(H)
    end.

refresh_both(Config) ->
    {ok, _} = barrel_ngram:refresh(?config(one, Config)),
    {ok, _} = barrel_ngram:refresh(?config(many, Config)),
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
    {ok, R} = barrel_docdb:put_doc(Db, Doc),
    maps:get(<<"rev">>, R).

del_doc(Db, Id, Rev) ->
    {ok, _} = barrel_docdb:delete_doc(Db, Id, #{rev => Rev}),
    ok.
