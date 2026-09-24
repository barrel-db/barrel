%%%-------------------------------------------------------------------
%%% @doc Segment integrity and lease cases: a query racing a compaction,
%%% read errors that must fail the query, corrupt or truncated segments.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_integrity_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([query_during_compaction/1,
         regex_during_compaction/1,
         posting_read_error_fails_query/1,
         key_read_error_fails_query/1]).

all() ->
    [query_during_compaction,
     regex_during_compaction,
     posting_read_error_fails_query,
     key_read_error_fails_query].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    {ok, _} = application:ensure_all_started(barrel_ngram),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    Db = iolist_to_binary([<<"ngram_integ_">>, atom_to_binary(TC, utf8)]),
    Dir = filename:join(?config(priv_dir, Config), atom_to_list(TC)),
    _ = barrel_docdb:delete_db(Db),
    {ok, _} = barrel_docdb:create_db(Db),
    ok = barrel_ngram:open(Db, #{db => Db, data_dir => Dir,
                                 compact_threshold => infinity,
                                 freeze_threshold => 1000000}),
    %% three segments, so a compaction has inputs to delete
    lists:foreach(
        fun(I) ->
            Id = integer_to_binary(I),
            {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => Id,
                                                 <<"body">> => <<"needle haystack ", Id/binary>>}),
            {ok, _} = barrel_ngram:refresh(Db)
        end, lists:seq(1, 3)),
    [{db, Db}, {corpus, Db}, {dir, Dir} | Config].

end_per_testcase(_TC, Config) ->
    _ = (try meck:unload() catch _:_ -> ok end),
    _ = barrel_ngram:close(?config(corpus, Config)),
    _ = barrel_docdb:delete_db(?config(db, Config)),
    ok.

%%====================================================================
%% Cases
%%====================================================================

%% A compaction that lands between the query's snapshot and its first
%% segment open must not make the query fail with enoent.
query_during_compaction(Config) ->
    C = ?config(corpus, Config),
    compact_on_first_open(C),
    ?assertEqual({ok, [<<"1">>, <<"2">>, <<"3">>]}, ids(barrel_ngram:search(C, <<"needle">>))),
    %% the compaction really ran and the inputs are gone afterwards
    ?assertEqual(1, length(segment_files(Config))).

regex_during_compaction(Config) ->
    C = ?config(corpus, Config),
    compact_on_first_open(C),
    ?assertEqual({ok, [<<"1">>, <<"2">>, <<"3">>]},
                 ids(barrel_ngram:regex(C, <<"needle\\s+hay">>))).

%% A posting read error must fail the query, not read as an empty list.
posting_read_error_fails_query(Config) ->
    C = ?config(corpus, Config),
    break_fd_on_open(),
    ?assertMatch({error, _}, barrel_ngram:search(C, <<"needle">>)),
    ?assertMatch({error, _}, barrel_ngram:regex(C, <<"need+le">>)).

%% A short literal reads keys only (no postings); a key read error must
%% fail the query too.
key_read_error_fails_query(Config) ->
    C = ?config(corpus, Config),
    break_fd_on_open(),
    ?assertMatch({error, _}, barrel_ngram:search(C, <<"ne">>)).

%%====================================================================
%% Helpers
%%====================================================================

%% Run a synchronous compaction from the test process right before its
%% first segment open (after the query took its snapshot).
compact_on_first_open(C) ->
    Self = self(),
    meck:new(barrel_ngram_segment, [passthrough]),
    meck:expect(barrel_ngram_segment, open,
        fun(Path) ->
            case self() =:= Self andalso get(compacted) =:= undefined of
                true ->
                    put(compacted, true),
                    {ok, _} = barrel_ngram:compact(C);
                false ->
                    ok
            end,
            meck:passthrough([Path])
        end).

%% Hand the query a handle whose fd is already closed, so every pread
%% fails.
break_fd_on_open() ->
    Self = self(),
    meck:new(barrel_ngram_segment, [passthrough]),
    meck:expect(barrel_ngram_segment, open,
        fun(Path) ->
            case meck:passthrough([Path]) of
                {ok, H} when self() =:= Self ->
                    ok = file:close(element(2, H)),
                    {ok, H};
                Other ->
                    Other
            end
        end).

ids({ok, Hits}) -> {ok, lists:sort([maps:get(id, H) || H <- Hits])};
ids(Other) -> Other.

segment_files(Config) ->
    Dir = filename:join(?config(dir, Config), binary_to_list(?config(corpus, Config))),
    {ok, Files} = file:list_dir(Dir),
    [F || F <- Files, filename:extension(F) =:= ".ngseg"].
