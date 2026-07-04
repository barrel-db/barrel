%%%-------------------------------------------------------------------
%%% @doc Writer-level channel feed tests: membership rows land, move
%%% and disappear correctly across local writes, deletes, recreates,
%%% replicated arrivals, and the concurrent-loser row move.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_channel_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([create_lands_member_rows/1,
         update_moves_row/1,
         transition_writes_leave/1,
         rejoin_after_leave/1,
         delete_lands_tombstone/1,
         recreate_over_tombstone/1,
         replicated_arrival_lands/1,
         loser_arrival_moves_row/1,
         tombstone_winner_row_moves/1,
         invalid_channels_rejected/1]).

all() ->
    [create_lands_member_rows,
     update_moves_row,
     transition_writes_leave,
     rejoin_after_leave,
     delete_lands_tombstone,
     recreate_over_tombstone,
     replicated_arrival_lands,
     loser_arrival_moves_row,
     tombstone_winner_row_moves,
     invalid_channels_rejected].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Dir = "/tmp/barrel_channel_test_"
        ++ integer_to_list(erlang:system_time(millisecond)),
    [{dir, Dir} | Config].

end_per_suite(Config) ->
    os:cmd("rm -rf " ++ ?config(dir, Config)),
    ok.

init_per_testcase(TC, Config) ->
    Db = atom_to_binary(TC, utf8),
    {ok, _} = barrel_docdb:create_db(Db, #{
        data_dir => ?config(dir, Config),
        channels => #{
            <<"posts">> => [<<"type/post">>],
            <<"acme">> => [<<"org/acme/#">>]
        }
    }),
    [{db, Db} | Config].

end_per_testcase(_TC, Config) ->
    try barrel_docdb:delete_db(?config(db, Config)) catch _:_ -> ok end,
    ok.

%%====================================================================
%% Helpers
%%====================================================================

%% Raw channel feed rows: [{Hlc, Row}] in feed order (the read side
%% lands in a later step; these tests inspect storage directly).
rows(Db, Channel) ->
    StoreRef = persistent_term:get({barrel_store, Db}),
    Start = barrel_store_keys:channel_prefix(Db, Channel),
    End = barrel_store_keys:channel_end(Db, Channel),
    Rows = barrel_store_rocksdb:fold_range(
        StoreRef, Start, End,
        fun(Key, Value, Acc) ->
            {_Chan, Hlc} = barrel_store_keys:decode_channel_key(Db, Key),
            {ok, [{Hlc, barrel_channel:decode_row(Value)} | Acc]}
        end,
        []),
    lists:reverse(Rows).

feed_hlc(Db, DocId) ->
    {ok, Changes, _} = barrel_docdb:get_changes(Db, first),
    [Hlc] = [maps:get(hlc, C) || C <- Changes, maps:get(id, C) =:= DocId],
    Hlc.

post_doc(Id) ->
    #{<<"id">> => Id, <<"type">> => <<"post">>, <<"title">> => <<"t">>}.

remote_version(Author) ->
    V = barrel_version:new(barrel_hlc:new_hlc(), Author),
    VV = barrel_vv:bump(barrel_vv:new(), V),
    {barrel_version:to_token(V), barrel_vv:encode(VV)}.

%%====================================================================
%% Cases
%%====================================================================

create_lands_member_rows(Config) ->
    Db = ?config(db, Config),
    {ok, #{<<"rev">> := Rev}} = barrel_docdb:put_doc(
        Db, #{<<"id">> => <<"a">>, <<"type">> => <<"post">>,
              <<"org">> => <<"acme">>}),
    [{Hlc, Row}] = rows(Db, <<"posts">>),
    ?assertMatch(#{flag := member, id := <<"a">>, deleted := false}, Row),
    ?assertEqual(Rev, maps:get(rev, Row)),
    %% row HLC tracks the doc's current change HLC
    ?assertEqual(feed_hlc(Db, <<"a">>), Hlc),
    %% multi-channel membership: one row per channel
    [{_, AcmeRow}] = rows(Db, <<"acme">>),
    ?assertMatch(#{flag := member, id := <<"a">>}, AcmeRow),
    %% a doc outside every channel writes nothing
    {ok, _} = barrel_docdb:put_doc(
        Db, #{<<"id">> => <<"b">>, <<"type">> => <<"user">>}),
    ?assertEqual(1, length(rows(Db, <<"posts">>))).

update_moves_row(Config) ->
    Db = ?config(db, Config),
    {ok, #{<<"rev">> := Rev1}} = barrel_docdb:put_doc(Db, post_doc(<<"a">>)),
    [{Hlc1, _}] = rows(Db, <<"posts">>),
    {ok, #{<<"rev">> := Rev2}} = barrel_docdb:put_doc(
        Db, (post_doc(<<"a">>))#{<<"title">> => <<"t2">>,
                                 <<"_rev">> => Rev1}),
    [{Hlc2, Row}] = rows(Db, <<"posts">>),
    ?assertNotEqual(Hlc1, Hlc2),
    ?assertEqual(Rev2, maps:get(rev, Row)),
    ?assertMatch(#{flag := member}, Row).

transition_writes_leave(Config) ->
    Db = ?config(db, Config),
    {ok, #{<<"rev">> := Rev1}} = barrel_docdb:put_doc(Db, post_doc(<<"a">>)),
    {ok, _} = barrel_docdb:put_doc(
        Db, #{<<"id">> => <<"a">>, <<"type">> => <<"user">>,
              <<"_rev">> => Rev1}),
    %% the member row is gone; a leave row marks the departure
    [{_, Row}] = rows(Db, <<"posts">>),
    ?assertMatch(#{flag := leave, id := <<"a">>}, Row).

rejoin_after_leave(Config) ->
    Db = ?config(db, Config),
    {ok, #{<<"rev">> := Rev1}} = barrel_docdb:put_doc(Db, post_doc(<<"a">>)),
    {ok, #{<<"rev">> := Rev2}} = barrel_docdb:put_doc(
        Db, #{<<"id">> => <<"a">>, <<"type">> => <<"user">>,
              <<"_rev">> => Rev1}),
    {ok, #{<<"rev">> := Rev3}} = barrel_docdb:put_doc(
        Db, (post_doc(<<"a">>))#{<<"_rev">> => Rev2}),
    %% the leave row was replaced by the fresh member row
    [{_, Row}] = rows(Db, <<"posts">>),
    ?assertMatch(#{flag := member}, Row),
    ?assertEqual(Rev3, maps:get(rev, Row)).

delete_lands_tombstone(Config) ->
    Db = ?config(db, Config),
    {ok, #{<<"rev">> := Rev1}} = barrel_docdb:put_doc(Db, post_doc(<<"a">>)),
    {ok, #{<<"rev">> := Rev2}} = barrel_docdb:delete_doc(
        Db, <<"a">>, #{rev => Rev1}),
    [{Hlc, Row}] = rows(Db, <<"posts">>),
    ?assertMatch(#{flag := member, deleted := true}, Row),
    ?assertEqual(Rev2, maps:get(rev, Row)),
    ?assertEqual(feed_hlc(Db, <<"a">>), Hlc).

recreate_over_tombstone(Config) ->
    Db = ?config(db, Config),
    {ok, #{<<"rev">> := Rev1}} = barrel_docdb:put_doc(Db, post_doc(<<"a">>)),
    {ok, _} = barrel_docdb:delete_doc(Db, <<"a">>, #{rev => Rev1}),
    {ok, #{<<"rev">> := Rev3}} = barrel_docdb:put_doc(Db, post_doc(<<"a">>)),
    [{_, Row}] = rows(Db, <<"posts">>),
    ?assertMatch(#{flag := member, deleted := false}, Row),
    ?assertEqual(Rev3, maps:get(rev, Row)).

replicated_arrival_lands(Config) ->
    Db = ?config(db, Config),
    {Token, VVBin} = remote_version(<<"peer_a">>),
    {ok, _, _} = barrel_docdb:put_version(
        Db, post_doc(<<"a">>), Token, VVBin, false),
    [{Hlc, Row}] = rows(Db, <<"posts">>),
    ?assertMatch(#{flag := member, id := <<"a">>}, Row),
    ?assertEqual(Token, maps:get(rev, Row)),
    ?assertEqual(feed_hlc(Db, <<"a">>), Hlc).

%% A concurrent loser keeps the local winner but moves its feed row:
%% the channel row must follow (one row, keyed at the current HLC).
loser_arrival_moves_row(Config) ->
    Db = ?config(db, Config),
    {LoserTok, LoserVV} = remote_version(<<"peer_a">>),
    {ok, #{<<"rev">> := LocalRev}} = barrel_docdb:put_doc(
        Db, post_doc(<<"a">>)),
    {ok, _, Winner} = barrel_docdb:put_version(
        Db, post_doc(<<"a">>), LoserTok, LoserVV, false),
    ?assertEqual(LocalRev, Winner),
    [{Hlc, Row}] = rows(Db, <<"posts">>),
    ?assertMatch(#{flag := member, deleted := false}, Row),
    ?assertEqual(LocalRev, maps:get(rev, Row)),
    ?assertEqual(feed_hlc(Db, <<"a">>), Hlc).

%% Same, with a tombstone winner: membership cannot be recomputed from
%% the body, so the row move must still carry the tombstone.
tombstone_winner_row_moves(Config) ->
    Db = ?config(db, Config),
    {LoserTok, LoserVV} = remote_version(<<"peer_a">>),
    {ok, #{<<"rev">> := Rev1}} = barrel_docdb:put_doc(Db, post_doc(<<"a">>)),
    {ok, #{<<"rev">> := DelRev}} = barrel_docdb:delete_doc(
        Db, <<"a">>, #{rev => Rev1}),
    {ok, _, Winner} = barrel_docdb:put_version(
        Db, post_doc(<<"a">>), LoserTok, LoserVV, false),
    ?assertEqual(DelRev, Winner),
    [{Hlc, Row}] = rows(Db, <<"posts">>),
    ?assertMatch(#{flag := member, deleted := true}, Row),
    ?assertEqual(feed_hlc(Db, <<"a">>), Hlc).

invalid_channels_rejected(Config) ->
    ?assertMatch(
        {error, {invalid_channels, {invalid_channel_pattern, _, _}}},
        barrel_docdb:create_db(<<"bad_channels_db">>, #{
            data_dir => ?config(dir, Config),
            channels => #{<<"c">> => [<<"a/#/b">>]}
        })).
