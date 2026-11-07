%%% @doc Upgrading from 2.0: pre-2.1 filtered and attachment checkpoints
%%% (and task positions) replay once, unfiltered and current ones stand,
%%% and barrel_rep:reconcile/3 repairs what 2.0 already damaged.
-module(barrel_review_upgrade_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, suite/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([upgrade_acceptance/1,
         old_filtered_checkpoint_replayed/1,
         old_att_checkpoint_replayed/1,
         old_unfiltered_checkpoint_kept/1,
         current_checkpoint_kept_across_restarts/1,
         old_task_positions_replayed/1,
         reconcile_repairs_body_and_index/1,
         reconcile_repairs_sibling_body/1,
         reconcile_never_writes_empty_body/1,
         reconcile_refuses_read_only/1]).

-define(FILTER, #{query => #{where => [{path, [<<"type">>], <<"user">>}]}}).
-define(LOCAL, barrel_rep_transport_local).

all() ->
    [upgrade_acceptance,
     old_filtered_checkpoint_replayed,
     old_att_checkpoint_replayed,
     old_unfiltered_checkpoint_kept,
     current_checkpoint_kept_across_restarts,
     old_task_positions_replayed,
     reconcile_repairs_body_and_index,
     reconcile_repairs_sibling_body,
     reconcile_never_writes_empty_body,
     reconcile_refuses_read_only].

suite() ->
    [{timetrap, {seconds, 120}}].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    Src = <<(atom_to_binary(TC))/binary, "_src">>,
    Tgt = <<(atom_to_binary(TC))/binary, "_tgt">>,
    DataDir = filename:join(?config(priv_dir, Config), atom_to_list(TC)),
    [begin
         _ = barrel_docdb:delete_db(D),
         {ok, _} = barrel_docdb:create_db(D, #{data_dir => DataDir})
     end || D <- [Src, Tgt]],
    [{src, Src}, {tgt, Tgt}, {data_dir, DataDir} | Config].

end_per_testcase(_TC, Config) ->
    {ok, Tasks} = barrel_rep_tasks:list_tasks(),
    [_ = barrel_rep_tasks:delete_task(Id) || #{id := Id} <- Tasks],
    _ = barrel_docdb:delete_db(?config(src, Config)),
    _ = barrel_docdb:delete_db(?config(tgt, Config)),
    ok.

%%====================================================================
%% The reviewer's fixture
%%====================================================================

%% 150 source docs, 20 on the target, an old filtered checkpoint at the
%% end of the feed, an attachment skipped after a failure with its old
%% checkpoint past it, a body stored empty: replicate, then reconcile.
upgrade_acceptance(Config) ->
    {Src, Tgt} = dbs(Config),
    Ids = seed_users(Src, 150),
    {ok, _} = barrel_docdb:put_attachment(Src, id(150), <<"a.txt">>,
                                          <<"bytes">>),
    copy_ids(Src, Tgt, lists:sublist(Ids, 20)),
    RepId = rep_id(Src, Tgt, ?FILTER),
    old_checkpoint(Src, Tgt, RepId, feed_end(Src)),
    old_checkpoint(Src, Tgt, <<"att-", RepId/binary>>, att_feed_end(Src)),
    empty_body(Tgt, id(2)),
    {ok, _} = barrel_rep:replicate(Src, Tgt, #{filter => ?FILTER}),
    {ok, R} = barrel_rep:reconcile(Src, Tgt, #{filter => ?FILTER}),
    ?assertMatch(#{docs_checked := 150, bodies_repaired := 1,
                   doc_failures := [], att_failures := []}, R),
    [?assertMatch({ok, #{<<"type">> := <<"user">>}},
                  barrel_docdb:get_doc(Tgt, Id)) || Id <- Ids],
    ?assertEqual({ok, <<"bytes">>},
                 barrel_docdb:get_attachment(Tgt, id(150), <<"a.txt">>)),
    ?assertMatch({ok, #{<<"n">> := 2}}, barrel_docdb:get_doc(Tgt, id(2))).

%%====================================================================
%% Checkpoints
%%====================================================================

old_filtered_checkpoint_replayed(Config) ->
    {Src, Tgt} = dbs(Config),
    Ids = seed_users(Src, 150),
    copy_ids(Src, Tgt, lists:sublist(Ids, 20)),
    RepId = rep_id(Src, Tgt, ?FILTER),
    old_checkpoint(Src, Tgt, RepId, feed_end(Src), [<<"kept">>]),
    {ok, R} = barrel_rep:replicate(Src, Tgt, #{filter => ?FILTER,
                                               batch_size => 7}),
    %% present docs are read, not rewritten
    ?assertMatch(#{docs_written := 130}, R),
    [{ok, _} = barrel_docdb:get_doc(Tgt, Id) || Id <- Ids],
    assert_vsn2(Src, Tgt, RepId).

old_att_checkpoint_replayed(Config) ->
    {Src, Tgt} = dbs(Config),
    {ok, _} = barrel_docdb:put_doc(Src, #{<<"id">> => <<"d">>}),
    {ok, _} = barrel_docdb:put_attachment(Src, <<"d">>, <<"skipped">>,
                                          <<"lost">>),
    {ok, _} = barrel_rep:replicate(Src, Tgt, #{attachments => false}),
    RepId = rep_id(Src, Tgt, #{}),
    old_checkpoint(Src, Tgt, <<"att-", RepId/binary>>, att_feed_end(Src)),
    {ok, R} = barrel_rep:replicate(Src, Tgt),
    ?assertMatch(#{att_sync := #{atts_written := 1}}, R),
    ?assertEqual({ok, <<"lost">>},
                 barrel_docdb:get_attachment(Tgt, <<"d">>, <<"skipped">>)),
    assert_vsn2(Src, Tgt, <<"att-", RepId/binary>>).

%% The filtered paging bug never touched unfiltered positions.
old_unfiltered_checkpoint_kept(Config) ->
    {Src, Tgt} = dbs(Config),
    Ids = seed_users(Src, 30),
    copy_ids(Src, Tgt, lists:sublist(Ids, 10)),
    RepId = rep_id(Src, Tgt, #{}),
    old_checkpoint(Src, Tgt, RepId, feed_end(Src)),
    {ok, R} = barrel_rep:replicate(Src, Tgt, #{attachments => false}),
    ?assertMatch(#{docs_read := 0}, R),
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(Tgt, id(30))),
    assert_vsn2(Src, Tgt, RepId).

current_checkpoint_kept_across_restarts(Config) ->
    {Src, Tgt} = dbs(Config),
    DataDir = ?config(data_dir, Config),
    seed_users(Src, 30),
    {ok, #{docs_written := 30}} =
        barrel_rep:replicate(Src, Tgt, #{filter => ?FILTER}),
    RepId = rep_id(Src, Tgt, ?FILTER),
    assert_vsn2(Src, Tgt, RepId),
    assert_vsn2(Src, Tgt, <<"att-", RepId/binary>>),
    [begin
         ok = barrel_docdb:close_db(D),
         {ok, _} = barrel_docdb:create_db(D, #{data_dir => DataDir})
     end || D <- [Src, Tgt]],
    {ok, R} = barrel_rep:replicate(Src, Tgt, #{filter => ?FILTER}),
    ?assertMatch(#{docs_read := 0}, R),
    assert_vsn2(Src, Tgt, RepId).

%% A pre-2.1 filtered task replays its doc and attachment positions.
old_task_positions_replayed(Config) ->
    {Src, Tgt} = dbs(Config),
    Ids = seed_users(Src, 40),
    {ok, _} = barrel_docdb:put_attachment(Src, id(1), <<"a">>, <<"att">>),
    copy_ids(Src, Tgt, lists:sublist(Ids, 5)),
    Id = <<"oldtask">>,
    Now = erlang:system_time(millisecond),
    End = barrel_rep_checkpoint:encode_seq(feed_end(Src)),
    Doc = #{<<"_type">> => <<"rep_task">>, <<"id">> => Id,
            <<"config">> => #{<<"source">> => Src, <<"target">> => Tgt,
                              <<"mode">> => <<"one_shot">>,
                              <<"filter">> =>
                                  barrel_rep_filter:to_wire(?FILTER)},
            <<"status">> => <<"running">>, <<"last_seq">> => End,
            <<"pending">> => [],
            <<"att_seq">> => barrel_rep_checkpoint:encode_seq(
                               att_feed_end(Src)),
            <<"created_at">> => Now, <<"updated_at">> => Now},
    ok = barrel_docdb:put_local_doc(<<"_replication_tasks">>,
                                    <<"rep_task:", Id/binary>>, Doc),
    ok = wait_until(fun() ->
                        case barrel_rep_tasks:get_task(Id) of
                            {ok, #{status := completed}} -> true;
                            _ -> false
                        end
                    end, 100, 150),
    [{ok, _} = barrel_docdb:get_doc(Tgt, I) || I <- Ids],
    ?assertEqual({ok, <<"att">>},
                 barrel_docdb:get_attachment(Tgt, id(1), <<"a">>)),
    {ok, #{<<"vsn">> := 2}} =
        barrel_docdb:get_local_doc(<<"_replication_tasks">>,
                                   <<"rep_task:", Id/binary>>).

%%====================================================================
%% Reconcile
%%====================================================================

%% The repaired winner is indexed again and moves in the feed.
reconcile_repairs_body_and_index(Config) ->
    {Src, Tgt} = dbs(Config),
    seed_users(Src, 3),
    {ok, _} = barrel_rep:replicate(Src, Tgt),
    empty_body(Tgt, id(2)),
    %% same state: a replication changes nothing
    {ok, #{docs_written := 0}} = barrel_rep:replicate(Src, Tgt),
    {ok, Before, _} = barrel_docdb:find(Tgt, #{where => [{path, [<<"n">>], 2}]}),
    {ok, #{bodies_repaired := 1, docs_written := 0, docs_checked := 3}} =
        barrel_rep:reconcile(Src, Tgt, #{}),
    ?assertMatch({ok, #{<<"n">> := 2, <<"type">> := <<"user">>}},
                 barrel_docdb:get_doc(Tgt, id(2))),
    {ok, After, _} = barrel_docdb:find(Tgt, #{where => [{path, [<<"n">>], 2}]}),
    ?assertEqual([], [D || #{<<"id">> := I} = D <- Before, I =:= id(2)]),
    ?assertMatch([_], [D || #{<<"id">> := I} = D <- After, I =:= id(2)]),
    %% the repair is a change on the target, the state is the source's
    {ok, Changes, _} = barrel_docdb:get_changes(Tgt, first, #{}),
    ?assertEqual(id(2), maps:get(id, lists:last(Changes))),
    {ok, #{digest := D1}} = barrel_docdb:get_doc_state(Src, id(2)),
    {ok, #{digest := D1}} = barrel_docdb:get_doc_state(Tgt, id(2)),
    %% a second run finds nothing to repair
    {ok, #{bodies_repaired := 0}} = barrel_rep:reconcile(Src, Tgt, #{}).

reconcile_repairs_sibling_body(Config) ->
    {Src, Tgt} = dbs(Config),
    {ok, _} = barrel_docdb:put_doc(Src, #{<<"id">> => <<"c">>,
                                          <<"side">> => <<"src">>}),
    {ok, _} = barrel_docdb:put_doc(Tgt, #{<<"id">> => <<"c">>,
                                          <<"side">> => <<"tgt">>}),
    {ok, _} = barrel_rep:replicate(Src, Tgt),
    {ok, _} = barrel_rep:replicate(Tgt, Src),
    {ok, #{bodies := Bodies}} = barrel_docdb:get_doc_state(Tgt, <<"c">>),
    ?assertEqual(2, map_size(Bodies)),
    {ok, #{<<"_rev">> := Winner}} = barrel_docdb:get_doc(Tgt, <<"c">>),
    [Sib] = maps:keys(Bodies) -- [Winner],
    #{Sib := SibBody} = Bodies,
    archived_empty(Tgt, <<"c">>, Sib),
    {ok, #{bodies := #{Sib := #{}}}} = barrel_docdb:get_doc_state(Tgt, <<"c">>),
    {ok, #{bodies_repaired := 1}} = barrel_rep:reconcile(Src, Tgt, #{}),
    {ok, #{bodies := #{Sib := SibBody}}} =
        barrel_docdb:get_doc_state(Tgt, <<"c">>).

%% A damaged source never empties a healthy target.
reconcile_never_writes_empty_body(Config) ->
    {Src, Tgt} = dbs(Config),
    seed_users(Src, 2),
    {ok, _} = barrel_rep:replicate(Src, Tgt),
    empty_body(Src, id(1)),
    {ok, #{bodies_repaired := 0}} = barrel_rep:reconcile(Src, Tgt, #{}),
    ?assertMatch({ok, #{<<"n">> := 1}}, barrel_docdb:get_doc(Tgt, id(1))).

reconcile_refuses_read_only(Config) ->
    {Src, Tgt} = dbs(Config),
    seed_users(Src, 2),
    ok = barrel_docdb:close_db(Tgt),
    {ok, _} = barrel_docdb:create_db(Tgt, #{data_dir => ?config(data_dir,
                                                                Config),
                                            read_only => true}),
    ?assertEqual({error, read_only}, barrel_rep:reconcile(Src, Tgt, #{})).

%%====================================================================
%% Helpers
%%====================================================================

dbs(Config) ->
    {?config(src, Config), ?config(tgt, Config)}.

id(N) ->
    iolist_to_binary(io_lib:format("u~3..0w", [N])).

seed_users(Db, N) ->
    Ids = [id(I) || I <- lists:seq(1, N)],
    [{ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => id(I),
                                          <<"type">> => <<"user">>,
                                          <<"n">> => I})
     || I <- lists:seq(1, N)],
    Ids.

copy_ids(Src, Tgt, Ids) ->
    {ok, #{failed := []}} = barrel_rep_alg:replicate_ids(Src, Tgt, ?LOCAL,
                                                         ?LOCAL, Ids),
    ok.

rep_id(Src, Tgt, Filter) ->
    barrel_rep:rep_id(Src, ?LOCAL, Tgt, ?LOCAL, Filter).

feed_end(Db) ->
    {ok, _, Last} = barrel_docdb:get_changes(Db, first, #{}),
    Last.

att_feed_end(Db) ->
    {ok, _, Last} = barrel_docdb:att_changes(Db, first, #{}),
    Last.

old_checkpoint(Src, Tgt, RepId, Seq) ->
    old_checkpoint(Src, Tgt, RepId, Seq, []).

%% The 2.0.0 checkpoint format: no vsn.
old_checkpoint(Src, Tgt, RepId, Seq, Pending) ->
    Entry = #{<<"source_last_seq">> => barrel_rep_checkpoint:encode_seq(Seq),
              <<"source_start_seq">> => <<"first">>,
              <<"pending">> => Pending,
              <<"session_id">> => <<"old">>,
              <<"end_time">> => <<"2026-01-01T00:00:00Z">>,
              <<"end_time_microsec">> => erlang:system_time(microsecond)},
    [ok = barrel_docdb:put_local_doc(D, <<"replication-checkpoint-",
                                          RepId/binary>>,
                                     #{<<"history">> => [Entry]})
     || D <- [Src, Tgt]],
    ok.

assert_vsn2(Src, Tgt, RepId) ->
    [begin
         {ok, #{<<"history">> := History}} =
             barrel_docdb:get_local_doc(D, <<"replication-checkpoint-",
                                             RepId/binary>>),
         Newest = lists:last(lists:keysort(1, [{maps:get(<<"end_time_microsec">>, H), H}
                                               || H <- History])),
         ?assertMatch({_, #{<<"vsn">> := 2}}, Newest)
     end || D <- [Src, Tgt]],
    ok.

%% What the 2.0 missing-body bug left: a live winner stored and indexed
%% as #{}.
empty_body(Db, DocId) ->
    Ks = barrel_keyspace:resolve(Db),
    StoreRef = persistent_term:get({barrel_store, Db}),
    {ok, Paths} = barrel_ars_index:get_doc_paths(StoreRef, Db, DocId),
    store_write(Db, [{body_put, barrel_store_keys:doc_body(Ks, DocId),
                      barrel_docdb_codec_cbor:encode_cbor(#{})}
                     | barrel_ars_index:remove_doc_ops(Db, DocId, Paths)]).

archived_empty(Db, DocId, Token) ->
    Ks = barrel_keyspace:resolve(Db),
    V = barrel_version:encode(barrel_version:from_token(Token)),
    store_write(Db, [{body_put, barrel_store_keys:doc_body_rev(Ks, DocId, V),
                      barrel_docdb_codec_cbor:encode_cbor(#{})}]).

store_write(Db, Ops) ->
    StoreRef = persistent_term:get({barrel_store, Db}),
    ok = barrel_store_rocksdb:write_batch(StoreRef, Ops, #{}).

wait_until(_Fun, _Ms, 0) ->
    timeout;
wait_until(Fun, Ms, N) ->
    case Fun() of
        true -> ok;
        false -> timer:sleep(Ms), wait_until(Fun, Ms, N - 1)
    end.
