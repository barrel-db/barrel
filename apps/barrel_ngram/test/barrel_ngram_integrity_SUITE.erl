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
         key_read_error_fails_query/1,
         manifest_records_checksums/1,
         corrupt_segment_fails_open/1,
         truncated_segment_fails_open/1,
         truncated_segment_fails_query/1,
         legacy_manifest_requires_reindex/1, lease_pins_until_release/1]).

all() ->
    [query_during_compaction,
     regex_during_compaction,
     posting_read_error_fails_query,
     key_read_error_fails_query,
     manifest_records_checksums,
     corrupt_segment_fails_open,
     truncated_segment_fails_open,
     truncated_segment_fails_query,
     legacy_manifest_requires_reindex, lease_pins_until_release].

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
    %% the compaction ran and the inputs go once the lease is released (a
    %% call after the release cast is ordered behind it)
    {ok, _} = barrel_ngram_shard:get_manifest(C),
    ?assertEqual(1, length(segment_files(Config))).

%% A lease pins its files until released; a lease whose owner dies is
%% released too.
lease_pins_until_release(Config) ->
    C = ?config(corpus, Config),
    Self = self(),
    Owner = spawn(fun() ->
        {ok, Lease, Segs, _} = barrel_ngram:safe_shard_call(C, lease_snapshot),
        Self ! {leased, Lease, Segs},
        receive release -> barrel_ngram_shard:release(C, Lease) end,
        receive stop -> ok end
    end),
    {Lease, Segs} = receive {leased, L, S} -> {L, S} after 5000 -> ct:fail(no_lease) end,
    {ok, _} = barrel_ngram:compact(C),
    %% inputs survive the compaction while leased
    ?assert(lists:all(fun({_, P}) -> filelib:is_regular(P) end, Segs)),
    Owner ! release,
    ok = wait_until(fun() -> not lists:any(fun({_, P}) -> filelib:is_regular(P) end, Segs) end, 100),
    %% releasing twice is harmless
    ok = barrel_ngram_shard:release(C, Lease),
    %% a dead owner releases its lease
    Owner2 = spawn(fun() ->
        {ok, _L2, Segs2, _} = barrel_ngram:safe_shard_call(C, lease_snapshot),
        Self ! {leased2, Segs2},
        receive stop -> ok end
    end),
    Segs2 = receive {leased2, S2} -> S2 after 5000 -> ct:fail(no_lease) end,
    {ok, _} = barrel_docdb:put_doc(?config(db, Config), #{<<"id">> => <<"4">>,
                                                          <<"body">> => <<"needle 4">>}),
    {ok, _} = barrel_ngram:refresh(C),
    {ok, _} = barrel_ngram:compact(C),
    ?assert(lists:all(fun({_, P}) -> filelib:is_regular(P) end, Segs2)),
    exit(Owner2, kill),
    ok = wait_until(fun() -> not lists:any(fun({_, P}) -> filelib:is_regular(P) end, Segs2) end, 100),
    Owner ! stop.

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

%% Freeze and merge both record the segment sha256 and size in the
%% manifest, and they match the file on disk.
manifest_records_checksums(Config) ->
    C = ?config(corpus, Config),
    check_manifest_checksums(Config, 3),
    {ok, _} = barrel_ngram:compact(C),
    check_manifest_checksums(Config, 1).

%% A flipped byte in a committed segment fails the next open explicitly.
corrupt_segment_fails_open(Config) ->
    C = ?config(corpus, Config),
    ok = barrel_ngram:close(C),
    [Seg | _] = segment_paths(Config),
    {ok, Bin} = file:read_file(Seg),
    Pos = byte_size(Bin) - 3,
    <<Pre:Pos/binary, B, Post/binary>> = Bin,
    ok = file:write_file(Seg, <<Pre/binary, (B bxor 16#ff), Post/binary>>),
    Res = reopen(Config, #{}),
    ?assertMatch({error, _}, Res),
    ?assert(contains(Res, checksum_mismatch)),
    ?assertNot(barrel_ngram:is_open(C)).

%% A truncated segment fails open even with layout-only verification.
truncated_segment_fails_open(Config) ->
    C = ?config(corpus, Config),
    ok = barrel_ngram:close(C),
    [Seg | _] = segment_paths(Config),
    truncate(Seg, 8),
    Res = reopen(Config, #{verify_segments => layout}),
    ?assertMatch({error, _}, Res),
    ?assert(contains(Res, size_mismatch)).

%% A segment truncated behind an open corpus fails the query rather than
%% reading the missing tail as absent grams.
truncated_segment_fails_query(Config) ->
    C = ?config(corpus, Config),
    [truncate(Seg, 8) || Seg <- segment_paths(Config)],
    ?assertMatch({error, _}, barrel_ngram:search(C, <<"needle">>)).

%% A v2 manifest (no checksums) is rejected like any older format and
%% rebuilt with on_legacy => reindex.
legacy_manifest_requires_reindex(Config) ->
    C = ?config(corpus, Config),
    ok = barrel_ngram:close(C),
    Path = filename:join(corpus_dir(Config), "manifest"),
    {ok, Bin} = file:read_file(Path),
    M = binary_to_term(Bin),
    Segs = [maps:without([sha256, bytes], S) || S <- maps:get(segments, M)],
    ok = file:write_file(Path, term_to_binary(M#{version => 2, segments => Segs})),
    Res = reopen(Config, #{}),
    ?assert(contains(Res, unsupported_manifest_version)),
    ok = reopen(Config, #{on_legacy => reindex}),
    {ok, _} = barrel_ngram:refresh(C),
    ?assertEqual({ok, [<<"1">>, <<"2">>, <<"3">>]}, ids(barrel_ngram:search(C, <<"needle">>))).

%%====================================================================
%% Helpers
%%====================================================================

reopen(Config, Extra) ->
    Db = ?config(db, Config),
    barrel_ngram:open(?config(corpus, Config),
                      Extra#{db => Db, data_dir => ?config(dir, Config),
                             compact_threshold => infinity}).

check_manifest_checksums(Config, N) ->
    {ok, M} = barrel_ngram_manifest:load(corpus_dir(Config)),
    Segs = barrel_ngram_manifest:list_segments(M),
    ?assertEqual(N, length(Segs)),
    lists:foreach(
        fun(#{file := F, sha256 := Sha, bytes := Bytes}) ->
            ?assertEqual({ok, Sha, Bytes},
                         barrel_ngram_fs:sha256_file(filename:join(corpus_dir(Config), F)))
        end, Segs).

wait_until(_Fun, 0) ->
    timeout;
wait_until(Fun, N) ->
    case Fun() of
        true -> ok;
        false -> timer:sleep(20), wait_until(Fun, N - 1)
    end.

truncate(Path, By) ->
    {ok, Bin} = file:read_file(Path),
    ok = file:write_file(Path, binary:part(Bin, 0, byte_size(Bin) - By)).

%% Whether `Needle' occurs anywhere inside `Term'.
contains(Needle, Needle) -> true;
contains(Term, Needle) when is_tuple(Term) -> contains(tuple_to_list(Term), Needle);
contains([H | T], Needle) -> contains(H, Needle) orelse contains(T, Needle);
contains(_Term, _Needle) -> false.

corpus_dir(Config) ->
    filename:join(?config(dir, Config), binary_to_list(?config(corpus, Config))).

segment_paths(Config) ->
    [filename:join(corpus_dir(Config), F) || F <- lists:sort(segment_files(Config))].

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
    {ok, Files} = file:list_dir(corpus_dir(Config)),
    [F || F <- Files, filename:extension(F) =:= ".ngseg"].
