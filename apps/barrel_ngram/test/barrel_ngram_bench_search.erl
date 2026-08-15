%%%-------------------------------------------------------------------
%%% @doc End-to-end query benchmark and profiling, over a real corpus.
%%%
%%% `barrel_ngram_bench.erl'/`barrel_ngram_bench_positional.erl' measure
%%% the codec/merge-join primitives in isolation; this measures the thing
%%% those primitives are FOR: a real `search/2,3'/`regex/2,3' call against
%%% a real `barrel_docdb'-backed corpus, comparing candidate verification
%%% with no `source' configured (fetch the whole document via
%%% `barrel_docdb:get_docs/2') against a windowed `source' (read just the
%%% matched region) -- the actual decision a deployment makes, and the
%%% payoff phase-2 narrowing plus windowing is meant to deliver as
%%% documents grow larger. Every comparison first checks both
%%% configurations return the identical hit count, so a latency win is
%%% never a correctness regression in disguise.
%%%
%%% Not run by the test suite; invoke `run/0' or `profile/0' directly.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_bench_search).

-export([run/0, run/1]).
-export([profile/0, profile/1]).

-define(POS_OPTS, #{radius => 3, sample_rate => 4}).
-define(LITERAL, <<"zzqconnect_42_marker">>).
-define(REGEX, <<"zzqconnect_[0-9]{2}_marker">>).

%%====================================================================
%% Latency: no source (full-document fetch) vs windowed source
%%====================================================================

run() ->
    run(#{doc_count => 200, reps => 5, doc_sizes => [500, 5000, 50000]}).

run(#{doc_count := DocCount, reps := Reps, doc_sizes := DocSizes}) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    {ok, _} = application:ensure_all_started(barrel_ngram),
    io:format("~n== end-to-end query: no source (full fetch) vs windowed source ==~n"
              "~p documents, ~p reps~n~n", [DocCount, Reps]),
    io:format("~-10s ~-9s ~-14s ~-14s~n", ["doc bytes", "query", "no-source ms", "source ms"]),
    lists:foreach(fun(Sz) -> bench_doc_size(DocCount, Sz, Reps) end, DocSizes),
    ok.

bench_doc_size(DocCount, DocSize, Reps) ->
    {Db, Docs} = seed(DocCount, DocSize),
    {Dense, Src} = open_pair(Db, DocSize, Docs),
    {ok, _} = barrel_ngram:refresh(Dense),
    {ok, _} = barrel_ngram:refresh(Src),
    assert_same_hits(DocCount, Dense, Src),
    TLitNo = time_us(Reps, fun() -> barrel_ngram:search(Dense, ?LITERAL) end),
    TLitSrc = time_us(Reps, fun() -> barrel_ngram:search(Src, ?LITERAL) end),
    TReNo = time_us(Reps, fun() -> barrel_ngram:regex(Dense, ?REGEX) end),
    TReSrc = time_us(Reps, fun() -> barrel_ngram:regex(Src, ?REGEX) end),
    io:format("~-10B literal   ~-14.2f ~-14.2f~n", [DocSize, TLitNo / 1000, TLitSrc / 1000]),
    io:format("~-10B regex     ~-14.2f ~-14.2f~n", [DocSize, TReNo / 1000, TReSrc / 1000]),
    teardown(Db, Dense, Src).

%% @private Every document carries the literal, so both configurations
%% must return exactly `DocCount' hits -- a latency difference below must
%% never come from one side quietly finding fewer (or more) documents.
assert_same_hits(DocCount, Dense, Src) ->
    {ok, H1} = barrel_ngram:search(Dense, ?LITERAL),
    {ok, H2} = barrel_ngram:search(Src, ?LITERAL),
    DocCount = length(H1),
    DocCount = length(H2),
    {ok, R1} = barrel_ngram:regex(Dense, ?REGEX),
    {ok, R2} = barrel_ngram:regex(Src, ?REGEX),
    DocCount = length(R1),
    DocCount = length(R2),
    ok.

%%====================================================================
%% Profiling: fprof over one representative end-to-end search call
%%====================================================================

%% @doc Profile one real `search/2' call with `fprof' and print the call
%% graph sorted by own time, to find where a slow query actually spends
%% its time beyond what `run/0''s totals show. `source => false' profiles
%% the whole-document-fetch path instead of the windowed one.
profile() ->
    profile(#{doc_count => 300, doc_size => 20000, source => true}).

profile(#{doc_count := DocCount, doc_size := DocSize, source := UseSource}) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    {ok, _} = application:ensure_all_started(barrel_ngram),
    {Db, Docs} = seed(DocCount, DocSize),
    Corpus = corpus_name(profile, DocSize),
    Base = #{db => Db, data_dir => tmp_dir(Corpus), phase2_selector_opts => ?POS_OPTS},
    Opts = case UseSource of
        true -> Base#{source => {barrel_ngram_source_mem, Docs}};
        false -> Base
    end,
    ok = barrel_ngram:open(Corpus, Opts),
    {ok, _} = barrel_ngram:refresh(Corpus),
    io:format("~n== profiling search/2 (source: ~p, ~p docs, ~p bytes each) ==~n~n",
              [UseSource, DocCount, DocSize]),
    fprof:trace([start, {procs, [self()]}]),
    {ok, _} = barrel_ngram:search(Corpus, ?LITERAL),
    fprof:trace(stop),
    fprof:profile(),
    fprof:analyse([dest, totals, {sort, own}]),
    _ = file:delete("fprof.trace"),   %% fprof:trace's default-location byproduct
    _ = barrel_ngram:close(Corpus),
    _ = barrel_docdb:delete_db(Db),
    _ = file:del_dir_r(tmp_dir(Corpus)),
    ok.

%%====================================================================
%% Corpus setup
%%====================================================================

open_pair(Db, DocSize, Docs) ->
    Dense = corpus_name(dense, DocSize),
    Src = corpus_name(source, DocSize),
    ok = barrel_ngram:open(Dense, #{db => Db, data_dir => tmp_dir(Dense),
                                    phase2_selector_opts => ?POS_OPTS}),
    ok = barrel_ngram:open(Src, #{db => Db, data_dir => tmp_dir(Src),
                                  phase2_selector_opts => ?POS_OPTS,
                                  source => {barrel_ngram_source_mem, Docs}}),
    {Dense, Src}.

teardown(Db, Dense, Src) ->
    _ = barrel_ngram:close(Dense),
    _ = barrel_ngram:close(Src),
    _ = barrel_docdb:delete_db(Db),
    _ = file:del_dir_r(tmp_dir(Dense)),
    _ = file:del_dir_r(tmp_dir(Src)),
    ok.

corpus_name(Tag, DocSize) ->
    iolist_to_binary(io_lib:format("ngram_bench_~p_~p_~p",
                                   [Tag, DocSize, erlang:unique_integer([positive])])).

tmp_dir(Corpus) ->
    filename:join("/tmp", Corpus).

%% @private `DocCount' documents of `DocSize' bytes each, every one
%% carrying `?LITERAL' at a random position, so hit counts are identical
%% (and known) across every configuration compared above.
seed(DocCount, DocSize) ->
    Db = iolist_to_binary([<<"ngram_bench_search_">>,
                           integer_to_list(erlang:unique_integer([positive]))]),
    _ = barrel_docdb:delete_db(Db),
    {ok, _} = barrel_docdb:create_db(Db),
    Docs = [{doc_id(N), random_text(DocSize)} || N <- lists:seq(1, DocCount)],
    lists:foreach(
        fun({Id, Text}) ->
            {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => Id, <<"body">> => Text})
        end, Docs),
    {Db, maps:from_list(Docs)}.

doc_id(N) -> iolist_to_binary([<<"doc">>, integer_to_binary(N)]).

random_text(Size) when Size > byte_size(?LITERAL) ->
    Filler = <<"error config retry backoff pool timeout connection upstream "
              "budget jitter widget arithmetic ">>,
    Reps = (Size div byte_size(Filler)) + 2,
    Padding = binary:part(binary:copy(Filler, Reps), 0, Size),
    Pos = rand:uniform(Size - byte_size(?LITERAL)) - 1,
    <<Pre:Pos/binary, _Skip:(byte_size(?LITERAL))/binary, Post/binary>> = Padding,
    <<Pre/binary, ?LITERAL/binary, Post/binary>>.

%%====================================================================
%% Helpers
%%====================================================================

time_us(Reps, Fun) ->
    _ = Fun(),   %% warm up
    {T, _} = timer:tc(fun() -> repeat(Reps, Fun) end),
    T / Reps.

repeat(0, _Fun) -> ok;
repeat(N, Fun) -> _ = Fun(), repeat(N - 1, Fun).
