%%%-------------------------------------------------------------------
%%% @doc Attachment change feed tests against the blob store: feed
%%% rows move with writes, tombstones land, the LWW origin guard
%%% converges, digests verify at the commit point, chunk transitions
%%% leave no leftovers, and the sweep ages tombstones out.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_att_feed_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([put_creates_feed_row/1,
         overwrite_moves_row/1,
         delete_lands_tombstone/1,
         pagination_since_exclusive/1,
         lww_origin_guard/1,
         digest_verification/1,
         stream_roundtrip/1,
         stream_digest_mismatch/1,
         stream_lww_ignored/1,
         chunk_transitions_leave_nothing/1,
         fold_skips_chunk_keys/1,
         sweep_ages_tombstones/1,
         rebuild_feed/1]).

-define(DB, <<"attdb">>).

all() ->
    [put_creates_feed_row,
     overwrite_moves_row,
     delete_lands_tombstone,
     pagination_since_exclusive,
     lww_origin_guard,
     digest_verification,
     stream_roundtrip,
     stream_digest_mismatch,
     stream_lww_ignored,
     chunk_transitions_leave_nothing,
     fold_skips_chunk_keys,
     sweep_ages_tombstones,
     rebuild_feed].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Dir = "/tmp/barrel_att_feed_test_"
        ++ integer_to_list(erlang:system_time(millisecond)),
    [{dir, Dir} | Config].

end_per_suite(Config) ->
    os:cmd("rm -rf " ++ ?config(dir, Config)),
    ok.

init_per_testcase(TC, Config) ->
    Path = filename:join(?config(dir, Config), atom_to_list(TC)),
    %% tiny chunk sizes so chunked paths trigger with small payloads
    {ok, Ref} = barrel_att_store_blob:open(Path, #{chunk_threshold => 16,
                                                   chunk_size => 8}),
    [{att, Ref} | Config].

end_per_testcase(_TC, Config) ->
    ok = barrel_att_store_blob:close(?config(att, Config)),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

changes(Ref) ->
    {ok, Entries, _} = barrel_att_store_blob:att_changes(Ref, ?DB, first, #{}),
    Entries.

digest(Data) ->
    <<"sha256-",
      (binary:encode_hex(crypto:hash(sha256, Data), lowercase))/binary>>.

origin() ->
    barrel_hlc:new_hlc().

%%====================================================================
%% Cases
%%====================================================================

put_creates_feed_row(Config) ->
    Ref = ?config(att, Config),
    {ok, Info} = barrel_att_store_blob:put(Ref, ?DB, <<"d">>, <<"f.txt">>,
                                           <<"hello">>, #{}),
    ?assertEqual(digest(<<"hello">>), maps:get(digest, Info)),
    [Entry] = changes(Ref),
    ?assertMatch(#{op := put, id := <<"d">>, name := <<"f.txt">>,
                   length := 5}, Entry),
    ?assertEqual(digest(<<"hello">>), maps:get(digest, Entry)),
    ?assertEqual(maps:get(content_type, Info),
                 maps:get(content_type, Entry)).

overwrite_moves_row(Config) ->
    Ref = ?config(att, Config),
    {ok, _} = barrel_att_store_blob:put(Ref, ?DB, <<"d">>, <<"f.txt">>,
                                        <<"one">>, #{}),
    [#{seq := Seq1}] = changes(Ref),
    {ok, _} = barrel_att_store_blob:put(Ref, ?DB, <<"d">>, <<"f.txt">>,
                                        <<"two">>, #{}),
    [Entry] = changes(Ref),
    ?assertEqual(digest(<<"two">>), maps:get(digest, Entry)),
    ?assert(barrel_hlc:less(Seq1, maps:get(seq, Entry))).

delete_lands_tombstone(Config) ->
    Ref = ?config(att, Config),
    {ok, _} = barrel_att_store_blob:put(Ref, ?DB, <<"d">>, <<"f.txt">>,
                                        <<"data">>, #{}),
    ok = barrel_att_store_blob:delete(Ref, ?DB, <<"d">>, <<"f.txt">>),
    ?assertEqual(not_found,
                 barrel_att_store_blob:get(Ref, ?DB, <<"d">>, <<"f.txt">>)),
    [Entry] = changes(Ref),
    ?assertMatch(#{op := delete, id := <<"d">>, name := <<"f.txt">>}, Entry),
    %% a local delete of something never written stays a no-op
    ok = barrel_att_store_blob:delete(Ref, ?DB, <<"d">>, <<"ghost">>),
    ?assertEqual(1, length(changes(Ref))).

pagination_since_exclusive(Config) ->
    Ref = ?config(att, Config),
    [begin
         {ok, _} = barrel_att_store_blob:put(
             Ref, ?DB, <<"d">>, <<"f", (integer_to_binary(I))/binary>>,
             <<"x">>, #{})
     end || I <- lists:seq(1, 3)],
    {ok, Page1, Last1} = barrel_att_store_blob:att_changes(
        Ref, ?DB, first, #{limit => 2}),
    ?assertEqual(2, length(Page1)),
    {ok, Page2, _} = barrel_att_store_blob:att_changes(
        Ref, ?DB, Last1, #{limit => 2}),
    ?assertEqual(1, length(Page2)),
    Names = [maps:get(name, E) || E <- Page1 ++ Page2],
    ?assertEqual(3, length(lists:usort(Names))).

lww_origin_guard(Config) ->
    Ref = ?config(att, Config),
    OldOrigin = origin(),
    NewOrigin = origin(),
    {ok, _} = barrel_att_store_blob:put(
        Ref, ?DB, <<"d">>, <<"f.txt">>, <<"current">>,
        #{origin_hlc => NewOrigin}),
    %% an older origin loses
    {ok, ignored} = barrel_att_store_blob:put(
        Ref, ?DB, <<"d">>, <<"f.txt">>, <<"stale">>,
        #{origin_hlc => OldOrigin}),
    {ok, <<"current">>} =
        barrel_att_store_blob:get(Ref, ?DB, <<"d">>, <<"f.txt">>),
    %% redelivery (equal origin, equal digest) is a no-op
    {ok, ignored} = barrel_att_store_blob:put(
        Ref, ?DB, <<"d">>, <<"f.txt">>, <<"current">>,
        #{origin_hlc => NewOrigin}),
    %% equal origin, different digest: byte-order tie-break, both ways
    {DLow, DHigh} = case {digest(<<"aa">>), digest(<<"bb">>)} of
        {A, B} when A < B -> {<<"aa">>, <<"bb">>};
        _ -> {<<"bb">>, <<"aa">>}
    end,
    TieOrigin = origin(),
    {ok, _} = barrel_att_store_blob:put(
        Ref, ?DB, <<"d">>, <<"tie">>, DLow, #{origin_hlc => TieOrigin}),
    {ok, Info} = barrel_att_store_blob:put(
        Ref, ?DB, <<"d">>, <<"tie">>, DHigh, #{origin_hlc => TieOrigin}),
    ?assertEqual(digest(DHigh), maps:get(digest, Info)),
    {ok, ignored} = barrel_att_store_blob:put(
        Ref, ?DB, <<"d">>, <<"tie">>, DLow, #{origin_hlc => TieOrigin}),
    %% a genuinely newer origin wins over everything
    {ok, _} = barrel_att_store_blob:put(
        Ref, ?DB, <<"d">>, <<"f.txt">>, <<"newest">>,
        #{origin_hlc => origin()}),
    {ok, <<"newest">>} =
        barrel_att_store_blob:get(Ref, ?DB, <<"d">>, <<"f.txt">>),
    %% a replicated delete with an old origin never kills newer data
    ok = barrel_att_store_blob:delete(
        Ref, ?DB, <<"d">>, <<"f.txt">>, #{origin_hlc => OldOrigin}),
    {ok, <<"newest">>} =
        barrel_att_store_blob:get(Ref, ?DB, <<"d">>, <<"f.txt">>).

digest_verification(Config) ->
    Ref = ?config(att, Config),
    ?assertEqual(
        {error, digest_mismatch},
        barrel_att_store_blob:put(Ref, ?DB, <<"d">>, <<"f.txt">>, <<"data">>,
                                  #{expected_digest => <<"sha256-wrong">>})),
    ?assertEqual(not_found,
                 barrel_att_store_blob:get(Ref, ?DB, <<"d">>, <<"f.txt">>)),
    ?assertEqual([], changes(Ref)),
    {ok, _} = barrel_att_store_blob:put(
        Ref, ?DB, <<"d">>, <<"f.txt">>, <<"data">>,
        #{expected_digest => digest(<<"data">>)}),
    ?assertEqual(1, length(changes(Ref))).

stream_roundtrip(Config) ->
    Ref = ?config(att, Config),
    {ok, S0} = barrel_att_store_blob:put_stream(
        Ref, ?DB, <<"d">>, <<"big.bin">>, <<"application/octet-stream">>,
        #{}),
    {ok, S1} = barrel_att_store_blob:write_chunk(S0, <<"0123456789">>),
    {ok, S2} = barrel_att_store_blob:write_chunk(S1, <<"abcdefghij">>),
    {ok, Info} = barrel_att_store_blob:finish_stream(S2),
    Data = <<"0123456789abcdefghij">>,
    ?assertEqual(digest(Data), maps:get(digest, Info)),
    {ok, Data} = barrel_att_store_blob:get(Ref, ?DB, <<"d">>, <<"big.bin">>),
    [Entry] = changes(Ref),
    ?assertEqual(digest(Data), maps:get(digest, Entry)),
    ?assertEqual(20, maps:get(length, Entry)).

stream_digest_mismatch(Config) ->
    Ref = ?config(att, Config),
    {ok, S0} = barrel_att_store_blob:put_stream(
        Ref, ?DB, <<"d">>, <<"big.bin">>, <<"application/octet-stream">>,
        #{expected_digest => <<"sha256-wrong">>}),
    {ok, S1} = barrel_att_store_blob:write_chunk(S0, <<"0123456789abc">>),
    ?assertEqual({error, digest_mismatch},
                 barrel_att_store_blob:finish_stream(S1)),
    ?assertEqual(not_found,
                 barrel_att_store_blob:get(Ref, ?DB, <<"d">>, <<"big.bin">>)),
    ?assertEqual([], changes(Ref)).

stream_lww_ignored(Config) ->
    Ref = ?config(att, Config),
    OldOrigin = origin(),
    {ok, _} = barrel_att_store_blob:put(
        Ref, ?DB, <<"d">>, <<"f.bin">>, <<"current">>,
        #{origin_hlc => origin()}),
    {ok, S0} = barrel_att_store_blob:put_stream(
        Ref, ?DB, <<"d">>, <<"f.bin">>, <<"application/octet-stream">>,
        #{origin_hlc => OldOrigin}),
    {ok, S1} = barrel_att_store_blob:write_chunk(S0, <<"stale streamed">>),
    {ok, ignored} = barrel_att_store_blob:finish_stream(S1),
    {ok, <<"current">>} =
        barrel_att_store_blob:get(Ref, ?DB, <<"d">>, <<"f.bin">>),
    ?assertEqual(1, length(changes(Ref))).

chunk_transitions_leave_nothing(Config) ->
    Ref = ?config(att, Config),
    Big = binary:copy(<<"x">>, 40),      %% 5 chunks of 8
    Small = <<"tiny">>,                  %% single value
    Medium = binary:copy(<<"y">>, 17),   %% 3 chunks
    {ok, _} = barrel_att_store_blob:put(Ref, ?DB, <<"d">>, <<"f">>, Big, #{}),
    {ok, Big} = barrel_att_store_blob:get(Ref, ?DB, <<"d">>, <<"f">>),
    %% shrink chunked -> chunked: stale higher chunks cleared
    {ok, _} = barrel_att_store_blob:put(Ref, ?DB, <<"d">>, <<"f">>, Medium,
                                        #{}),
    {ok, Medium} = barrel_att_store_blob:get(Ref, ?DB, <<"d">>, <<"f">>),
    ?assertEqual(3, length(raw_chunk_keys(Ref))),
    %% chunked -> single: all chunks cleared
    {ok, _} = barrel_att_store_blob:put(Ref, ?DB, <<"d">>, <<"f">>, Small,
                                        #{}),
    {ok, Small} = barrel_att_store_blob:get(Ref, ?DB, <<"d">>, <<"f">>),
    ?assertEqual(0, length(raw_chunk_keys(Ref))),
    %% one feed row throughout
    ?assertEqual(1, length(changes(Ref))).

fold_skips_chunk_keys(Config) ->
    Ref = ?config(att, Config),
    Big = binary:copy(<<"x">>, 40),
    {ok, _} = barrel_att_store_blob:put(Ref, ?DB, <<"d">>, <<"big">>, Big,
                                        #{}),
    {ok, _} = barrel_att_store_blob:put(Ref, ?DB, <<"d">>, <<"small">>,
                                        <<"s">>, #{}),
    Names = barrel_att_store_blob:fold(
        Ref, ?DB, <<"d">>, fun(Name, _V, Acc) -> {ok, [Name | Acc]} end, []),
    ?assertEqual([<<"big">>, <<"small">>], lists:sort(Names)),
    %% delete_all removes both, incl. every chunk
    ok = barrel_att_store_blob:delete_all(Ref, ?DB, <<"d">>),
    ?assertEqual(not_found,
                 barrel_att_store_blob:get(Ref, ?DB, <<"d">>, <<"big">>)),
    ?assertEqual(0, length(raw_chunk_keys(Ref))).

sweep_ages_tombstones(Config) ->
    Ref = ?config(att, Config),
    {ok, _} = barrel_att_store_blob:put(Ref, ?DB, <<"d">>, <<"live">>,
                                        <<"v">>, #{}),
    {ok, _} = barrel_att_store_blob:put(Ref, ?DB, <<"d">>, <<"gone">>,
                                        <<"v">>, #{}),
    ok = barrel_att_store_blob:delete(Ref, ?DB, <<"d">>, <<"gone">>),
    Cutoff = barrel_hlc:new_hlc(),
    {ok, #{tombstones_swept := 1}} =
        barrel_att_store_blob:sweep_att_feed(Ref, ?DB, Cutoff),
    [Entry] = changes(Ref),
    ?assertMatch(#{op := put, name := <<"live">>}, Entry),
    ?assertEqual(Cutoff, barrel_att_store_blob:att_floor(Ref, ?DB)),
    %% re-running the sweep is a no-op
    {ok, #{tombstones_swept := 0}} =
        barrel_att_store_blob:sweep_att_feed(Ref, ?DB, Cutoff).

rebuild_feed(Config) ->
    Ref = ?config(att, Config),
    Big = binary:copy(<<"z">>, 40),
    {ok, _} = barrel_att_store_blob:put(Ref, ?DB, <<"d1">>, <<"a">>,
                                        <<"one">>, #{}),
    {ok, _} = barrel_att_store_blob:put(Ref, ?DB, <<"d2">>, <<"b">>, Big,
                                        #{}),
    {ok, #{rows := 2}} = barrel_att_store_blob:rebuild_feed(Ref, ?DB),
    %% still one row per attachment, and rebuilt rows lose to any
    %% real write (minimum origin)
    ?assertEqual(2, length(changes(Ref))),
    {ok, _} = barrel_att_store_blob:put(
        Ref, ?DB, <<"d1">>, <<"a">>, <<"fresh">>,
        #{origin_hlc => origin()}),
    {ok, <<"fresh">>} = barrel_att_store_blob:get(Ref, ?DB, <<"d1">>, <<"a">>),
    %% idempotent
    {ok, #{rows := 2}} = barrel_att_store_blob:rebuild_feed(Ref, ?DB).

%%====================================================================
%% Raw inspection
%%====================================================================

%% All chunk keys in the store (keys containing NUL after the ':').
raw_chunk_keys(#{ref := Ref}) ->
    {ok, Itr} = rocksdb:iterator(Ref, []),
    try
        raw_chunk_loop(rocksdb:iterator_move(Itr, first), Itr, [])
    after
        rocksdb:iterator_close(Itr)
    end.

raw_chunk_loop({error, _}, _Itr, Acc) ->
    Acc;
raw_chunk_loop({ok, <<16#FF, _/binary>>, _V}, Itr, Acc) ->
    %% feed/index/meta keys
    raw_chunk_loop(rocksdb:iterator_move(Itr, next), Itr, Acc);
raw_chunk_loop({ok, Key, _V}, Itr, Acc) ->
    Acc1 = case binary:match(Key, <<0, 0, 0>>) of
        nomatch -> Acc;
        _ -> [Key | Acc]
    end,
    raw_chunk_loop(rocksdb:iterator_move(Itr, next), Itr, Acc1).
