%%%-------------------------------------------------------------------
%%% @doc Differential-oracle tests for the sparse selector (M4).
%%%
%%% The same database is indexed under two corpora, one dense and one
%%% sparse, and their confirmed hit sets must be byte-identical over a
%%% battery of literals. Dense already equals a brute-force scan (the M1
%%% oracle), so this proves sparse correct. Covered statically, across the
%%% live lifecycle (updates/deletes/compaction), and with a size check
%%% that the sparse index actually selects fewer grams.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_sparse_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([oracle_static/1, oracle_lifecycle/1, size_reduction/1]).

all() ->
    [oracle_static, oracle_lifecycle, size_reduction].

docs() ->
    [
     {<<"a">>, <<"error: connect_timeout exceeded in the pool">>},
     {<<"b">>, <<"the quick brown fox jumps over the lazy dog">>},
     {<<"c">>, <<"config key retry_backoff_ms = 250 with jitter">>},
     {<<"d">>, <<"fn connect_timeout() -> Result<(), Err> { retry() }">>},
     {<<"e">>, <<"unrelated content about widgets and gadgets">>},
     {<<"f">>, <<"x = a + b; // arithmetic on the upstream pool">>},
     {<<"g">>, <<"see also retry_backoff_ms and the jitter budget">>},
     {<<"h">>, <<"connection pool exhausted, connect_timeout hit">>}
    ].

literals() ->
    [<<"connect_timeout">>, <<"retry_backoff_ms">>, <<"exceeded in the pool">>,
     <<"connection pool">>, <<"the">>, <<"quick brown">>, <<"jitter">>,
     <<"upstream">>, <<"-> Result<(), Err>">>, <<"() ->">>, <<"+ b;">>,
     <<"widgets and gadgets">>, <<"co">>, <<"x ">>, <<"//">>, <<"= 250">>,
     <<"nonexistent_substring">>, <<"zzz">>, <<"retry">>, <<"pool">>].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    {ok, _} = application:ensure_all_started(barrel_ngram),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    Db = iolist_to_binary([<<"ngram_sparse_">>, atom_to_binary(TC, utf8)]),
    Dense = <<Db/binary, "_dense">>,
    Sparse = <<Db/binary, "_sparse">>,
    Base = filename:join(?config(priv_dir, Config), atom_to_list(TC)),
    _ = barrel_docdb:delete_db(Db),
    {ok, _} = barrel_docdb:create_db(Db),
    ok = barrel_ngram:open(Dense,
                           #{db => Db, data_dir => filename:join(Base, "dense"),
                             compact_threshold => infinity}),
    ok = barrel_ngram:open(Sparse,
                           #{db => Db, data_dir => filename:join(Base, "sparse"),
                             selector => barrel_ngram_selector_sparse,
                             selector_opts => #{radius => 3, sample_rate => 4},
                             compact_threshold => infinity}),
    [{db, Db}, {dense, Dense}, {sparse, Sparse} | Config].

end_per_testcase(_TC, Config) ->
    _ = barrel_ngram:close(?config(dense, Config)),
    _ = barrel_ngram:close(?config(sparse, Config)),
    _ = barrel_docdb:delete_db(?config(db, Config)),
    ok.

%%====================================================================
%% Test cases
%%====================================================================

oracle_static(Config) ->
    Db = ?config(db, Config),
    lists:foreach(fun({Id, Text}) -> put_doc(Db, Id, Text) end, docs()),
    refresh_both(Config),
    assert_agree(Config, literals()).

oracle_lifecycle(Config) ->
    Db = ?config(db, Config),
    %% initial load
    Revs0 = maps:from_list([{Id, put_doc(Db, Id, Text)} || {Id, Text} <- docs()]),
    refresh_both(Config),
    %% update one, delete another
    R = maps:get(<<"a">>, Revs0),
    _ = put_doc(Db, <<"a">>, <<"alpha now mentions retry_backoff_ms instead">>, R),
    ok = del_doc(Db, <<"d">>, maps:get(<<"d">>, Revs0)),
    refresh_both(Config),
    %% compact both, then compare
    {ok, _} = barrel_ngram:compact(?config(dense, Config)),
    {ok, _} = barrel_ngram:compact(?config(sparse, Config)),
    assert_agree(Config, literals()).

size_reduction(Config) ->
    Db = ?config(db, Config),
    %% a larger, repetitive corpus so sampling clearly bites
    lists:foreach(
        fun(N) ->
            Id = iolist_to_binary([<<"doc">>, integer_to_binary(N)]),
            Text = iolist_to_binary(
                     [<<"connect_timeout retry_backoff_ms jitter upstream pool ">>,
                      integer_to_binary(N),
                      <<" the quick brown fox exceeded the connection budget">>]),
            put_doc(Db, Id, Text)
        end, lists:seq(1, 40)),
    refresh_both(Config),
    {ok, _} = barrel_ngram:compact(?config(dense, Config)),
    {ok, _} = barrel_ngram:compact(?config(sparse, Config)),
    DenseGrams = distinct_grams(?config(dense, Config)),
    SparseGrams = distinct_grams(?config(sparse, Config)),
    ct:pal("dense grams=~p sparse grams=~p", [DenseGrams, SparseGrams]),
    ?assert(SparseGrams < DenseGrams),
    %% and results still agree
    assert_agree(Config, literals()).

%%====================================================================
%% Helpers
%%====================================================================

assert_agree(Config, Literals) ->
    Dense = ?config(dense, Config),
    Sparse = ?config(sparse, Config),
    lists:foreach(
        fun(Lit) ->
            D = search(Dense, Lit),
            S = search(Sparse, Lit),
            ?assertEqual({Lit, D}, {Lit, S})
        end, Literals).

refresh_both(Config) ->
    {ok, _} = barrel_ngram:refresh(?config(dense, Config)),
    {ok, _} = barrel_ngram:refresh(?config(sparse, Config)),
    ok.

search(Corpus, Lit) ->
    {ok, Hits} = barrel_ngram:search(Corpus, Lit),
    lists:sort([maps:get(id, H) || H <- Hits]).

%% Distinct grams stored in a corpus's single (post-compaction) segment.
distinct_grams(Corpus) ->
    {ok, [{_Gen, Path}]} = barrel_ngram_shard:get_manifest(Corpus),
    {ok, H} = barrel_ngram_segment:open(Path),
    try
        length(barrel_ngram_segment:all_postings(H))
    after
        barrel_ngram_segment:close(H)
    end.

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
