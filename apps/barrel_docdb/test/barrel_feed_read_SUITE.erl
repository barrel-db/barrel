%% read_feed/3: one feed page and its watermark under one snapshot, each
%% change paired with the body stored in its own feed row.
-module(barrel_feed_read_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([empty_db/1, pages_end_at_watermark/1, one_row_per_doc/1,
         body_matches_rev_under_rewrites/1, deletes_and_expiry/1,
         bulk_writes/1, replicated_and_winner_change/1,
         body_fallback/1, encrypted_store_env/1, instance_after_recreate/1,
         invalid_args/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [empty_db, pages_end_at_watermark, one_row_per_doc,
     body_matches_rev_under_rewrites, deletes_and_expiry, bulk_writes,
     replicated_and_winner_change, body_fallback, encrypted_store_env,
     instance_after_recreate, invalid_args].

init_per_suite(Config) ->
    Dir = filename:join(?config(priv_dir, Config), "data"),
    _ = application:load(barrel_docdb),
    ok = application:set_env(barrel_docdb, data_dir, Dir),
    {ok, _} = application:ensure_all_started(barrel_docdb),
    [{data_dir, Dir} | Config].

end_per_suite(_Config) ->
    ok = application:stop(barrel_docdb).

init_per_testcase(Case, Config) ->
    Db = <<"feed_", (atom_to_binary(Case))/binary>>,
    _ = barrel_docdb:delete_db(Db),
    [{db, Db} | Config].

end_per_testcase(_Case, Config) ->
    _ = barrel_docdb:delete_db(?config(db, Config)),
    ok.

%%====================================================================
%% Cases
%%====================================================================

empty_db(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:create_db(Db),
    {ok, [], Next, #{last_seq := Watermark, instance_id := Id}} =
        barrel_docdb:read_feed(Db, first, #{}),
    ?assertEqual(Next, Watermark),
    {ok, Id} = barrel_docdb:db_instance_id(Db).

pages_end_at_watermark(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:create_db(Db),
    [put_n(Db, I) || I <- lists:seq(1, 5)],
    {ok, #{last_seq := Observed}} = barrel_docdb:db_observed_version(Db),
    {ok, [_, _] = P1, N1, #{last_seq := Observed}} =
        barrel_docdb:read_feed(Db, first, #{limit => 2}),
    ?assertEqual(maps:get(seq, lists:last(P1)), N1),
    {ok, [_, _] = P2, N2, _} = barrel_docdb:read_feed(Db, N1, #{limit => 2}),
    ?assertEqual(maps:get(seq, lists:last(P2)), N2),
    {ok, [_] = P3, N3, _} = barrel_docdb:read_feed(Db, N2, #{limit => 2}),
    %% a short page reaches the watermark
    ?assertEqual(Observed, N3),
    ?assertEqual([integer_to_binary(I) || I <- lists:seq(1, 5)],
                 [Id || #{id := Id} <- P1 ++ P2 ++ P3]),
    Seqs = [S || #{seq := S} <- P1 ++ P2 ++ P3],
    ?assertEqual(lists:usort(Seqs), Seqs),
    {ok, [], N3, _} = barrel_docdb:read_feed(Db, N3, #{limit => 2}).

one_row_per_doc(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:create_db(Db),
    {ok, #{<<"rev">> := R1}} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"a">>, <<"n">> => 1}),
    put_n(Db, 2),
    {ok, #{<<"rev">> := R2}} = barrel_docdb:put_doc(
        Db, #{<<"id">> => <<"a">>, <<"_rev">> => R1, <<"n">> => 3}),
    {ok, [#{id := <<"2">>},
          #{id := <<"a">>, rev := R2, doc := #{<<"id">> := <<"a">>, <<"n">> := 3}}],
     _, _} = barrel_docdb:read_feed(Db, first, #{}).

%% A writer rewrites one document while a reader pages the feed: every
%% change it sees carries the body of the revision it names.
body_matches_rev_under_rewrites(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:create_db(Db),
    Self = self(),
    Writer = spawn_link(fun() -> rewrite_loop(Db, undefined, 1, 300, Self, #{}) end),
    Seen = read_until_done(Db, first, Writer, []),
    Revs = receive {revs, Writer, R} -> R end,
    ?assert(length(Seen) > 0),
    lists:foreach(fun(#{rev := Rev, doc := #{<<"n">> := N}}) ->
        ?assertEqual({Rev, N}, {Rev, maps:get(Rev, Revs)})
    end, Seen).

deletes_and_expiry(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:create_db(Db),
    Expires = erlang:system_time(millisecond) + 3600000,
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"t">>}, #{expires_at => Expires}),
    {ok, #{<<"rev">> := R}} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"d">>}),
    {ok, _} = barrel_docdb:delete_doc(Db, <<"d">>, #{rev => R}),
    {ok, [#{id := <<"t">>, expires_at := Expires, doc := #{<<"id">> := <<"t">>}},
          #{id := <<"d">>, deleted := true} = Del], _, _} =
        barrel_docdb:read_feed(Db, first, #{}),
    ?assertNot(maps:is_key(doc, Del)),
    ?assertNot(maps:is_key(expires_at, Del)).

bulk_writes(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:create_db(Db),
    Docs = [#{<<"id">> => integer_to_binary(I), <<"n">> => I} || I <- lists:seq(1, 3)],
    [{ok, _}, {ok, _}, {ok, _}] = barrel_docdb:put_docs(Db, Docs),
    {ok, Changes, _, _} = barrel_docdb:read_feed(Db, first, #{}),
    ?assertEqual([{<<"1">>, 1}, {<<"2">>, 2}, {<<"3">>, 3}],
                 lists:sort([{Id, N} || #{id := Id, doc := #{<<"n">> := N}} <- Changes])).

%% A replicated arrival, then a concurrent local edit that moves the row:
%% the feed always carries the current winner and its body.
replicated_and_winner_change(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:create_db(Db),
    {Token, VV} = remote_version(<<"peer_a">>),
    {ok, _, _} = barrel_docdb:put_version(
        Db, #{<<"id">> => <<"r">>, <<"v">> => <<"remote">>}, Token, VV, false),
    {ok, [#{id := <<"r">>, rev := Token, doc := #{<<"v">> := <<"remote">>}}], S1, _} =
        barrel_docdb:read_feed(Db, first, #{}),
    {Loser, LoserVV} = remote_version(<<"peer_b">>),
    {ok, _, Winner} = barrel_docdb:put_version(
        Db, #{<<"id">> => <<"r">>, <<"v">> => <<"other">>}, Loser, LoserVV, false),
    {ok, #{<<"_rev">> := Winner} = Current} = barrel_docdb:get_doc(Db, <<"r">>),
    {ok, [#{id := <<"r">>, rev := Winner, doc := Body}], _, _} =
        barrel_docdb:read_feed(Db, S1, #{}),
    ?assertEqual(maps:get(<<"v">>, Current), maps:get(<<"v">>, Body)).

%% A feed row written without its body falls back to the body column
%% family under the same snapshot.
body_fallback(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:create_db(Db),
    {ok, #{<<"rev">> := Rev}} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"b">>, <<"x">> => 1}),
    {ok, [#{seq := Seq}], _, _} = barrel_docdb:read_feed(Db, first, #{}),
    {ok, Pid} = barrel_docdb:open_db(Db),
    {ok, StoreRef} = barrel_db_server:get_store_ref(Pid),
    Key = barrel_store_keys:doc_hlc(barrel_keyspace:resolve(Db), barrel_hlc:decode(Seq)),
    Bodyless = barrel_changes:encode_change(#{id => <<"b">>, rev => Rev}),
    ok = barrel_store_rocksdb:write_batch(StoreRef, [{put, Key, Bodyless}]),
    {ok, [#{id := <<"b">>, rev := Rev, doc := #{<<"id">> := <<"b">>, <<"x">> := 1}}], _, _} =
        barrel_docdb:read_feed(Db, first, #{}).

encrypted_store_env(Config) ->
    Db = ?config(db, Config),
    Plain = <<Db/binary, "_plain">>,
    _ = barrel_docdb:delete_db(Plain),
    application:set_env(barrel_docdb, test_encryption_master, <<"feed secret">>),
    try
        {ok, _} = barrel_docdb:create_db(
            Db, #{encryption => #{provider => barrel_docdb_test_keyprovider}}),
        {ok, _} = barrel_docdb:create_db(Plain),
        {ok, Env} = barrel_docdb:store_env(Db),
        ?assertNotEqual(default, Env),
        {ok, default} = barrel_docdb:store_env(Plain),
        put_n(Db, 1),
        {ok, [#{id := <<"1">>, doc := #{<<"n">> := 1}}], _, _} =
            barrel_docdb:read_feed(Db, first, #{})
    after
        _ = barrel_docdb:delete_db(Plain),
        application:unset_env(barrel_docdb, test_encryption_master)
    end.

instance_after_recreate(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:create_db(Db),
    put_n(Db, 1),
    {ok, _, _, #{instance_id := Before}} = barrel_docdb:read_feed(Db, first, #{}),
    ok = barrel_docdb:delete_db(Db),
    {ok, _} = barrel_docdb:create_db(Db),
    {ok, [], _, #{instance_id := After}} = barrel_docdb:read_feed(Db, first, #{}),
    ?assertNotEqual(Before, After).

invalid_args(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:create_db(Db),
    {error, {invalid_since, <<"x">>}} = barrel_docdb:read_feed(Db, <<"x">>, #{}),
    {error, {invalid_limit, 0}} = barrel_docdb:read_feed(Db, first, #{limit => 0}),
    {error, {invalid_limit, 10001}} = barrel_docdb:read_feed(Db, first, #{limit => 10001}),
    {error, not_found} = barrel_docdb:read_feed(<<"feed_missing_db">>, first, #{}).

%%====================================================================
%% Helpers
%%====================================================================

put_n(Db, I) ->
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => integer_to_binary(I), <<"n">> => I}).

remote_version(Author) ->
    V = barrel_version:new(barrel_hlc:new_hlc(), Author),
    VV = barrel_vv:bump(barrel_vv:new(), V),
    {barrel_version:to_token(V), barrel_vv:encode(VV)}.

rewrite_loop(_Db, _Rev, N, Max, Parent, Revs) when N > Max ->
    Parent ! {revs, self(), Revs};
rewrite_loop(Db, Rev, N, Max, Parent, Revs) ->
    Doc0 = #{<<"id">> => <<"hot">>, <<"n">> => N},
    Doc = case Rev of undefined -> Doc0; _ -> Doc0#{<<"_rev">> => Rev} end,
    {ok, #{<<"rev">> := NewRev}} = barrel_docdb:put_doc(Db, Doc),
    rewrite_loop(Db, NewRev, N + 1, Max, Parent, Revs#{NewRev => N}).

%% Page the feed until the writer has finished and the feed is drained.
read_until_done(Db, Since, Writer, Acc) ->
    Done = not is_process_alive(Writer),
    {ok, Page, Next, _} = barrel_docdb:read_feed(Db, Since, #{limit => 7}),
    Acc1 = Page ++ Acc,
    case {Done, Page} of
        {true, []} -> Acc1;
        _ -> read_until_done(Db, Next, Writer, Acc1)
    end.
