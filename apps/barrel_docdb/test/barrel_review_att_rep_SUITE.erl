%%% @doc Attachment replication defects from the 2.0.0 review: failures
%%% and checkpoints, unreadable checkpoints, the durability barrier,
%%% per-direction task positions, kept deletes and filters.
-module(barrel_review_att_rep_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, suite/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([failed_put_holds_checkpoint/1,
         failure_stops_in_feed_order/1,
         source_gone_without_successor_fails/1,
         source_gone_after_delete_skipped/1,
         unreadable_source_checkpoint/1,
         unreadable_target_checkpoint/1,
         checkpoint_write_failure_retry_converges/1,
         flush_before_every_checkpoint/1,
         failed_flush_holds_checkpoint/1,
         flush_unsupported_fails_before_mutation/1,
         best_effort_skips_flush/1,
         backend_flush/1,
         invalid_durability_rejected/1,
         task_failure_keeps_position/1,
         both_task_keeps_each_side/1,
         both_task_restart_keeps_positions/1,
         deletes_kept_past_retention/1,
         path_filter_excludes_private/1,
         query_filter_excludes_private/1,
         channel_filter_excludes_private/1,
         unknown_channel_stops_run/1,
         filter_evaluation_error_stops_run/1,
         doc_entering_scope_gets_attachment/1,
         empty_filtered_page_continues/1,
         no_progress_page_fails/1,
         source_without_filter_support_fails/1,
         task_filter_applies_to_attachments/1]).

-define(CHANNELS, #{<<"public">> => [<<"type/public">>]}).

all() ->
    [failed_put_holds_checkpoint,
     failure_stops_in_feed_order,
     source_gone_without_successor_fails,
     source_gone_after_delete_skipped,
     unreadable_source_checkpoint,
     unreadable_target_checkpoint,
     checkpoint_write_failure_retry_converges,
     flush_before_every_checkpoint,
     failed_flush_holds_checkpoint,
     flush_unsupported_fails_before_mutation,
     best_effort_skips_flush,
     backend_flush,
     invalid_durability_rejected,
     task_failure_keeps_position,
     both_task_keeps_each_side,
     both_task_restart_keeps_positions,
     deletes_kept_past_retention,
     path_filter_excludes_private,
     query_filter_excludes_private,
     channel_filter_excludes_private,
     unknown_channel_stops_run,
     filter_evaluation_error_stops_run,
     doc_entering_scope_gets_attachment,
     empty_filtered_page_continues,
     no_progress_page_fails,
     source_without_filter_support_fails,
     task_filter_applies_to_attachments].

suite() ->
    [{timetrap, {seconds, 60}}].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    Src = <<(atom_to_binary(TC))/binary, "_src">>,
    Tgt = <<(atom_to_binary(TC))/binary, "_tgt">>,
    Opts = #{channels => ?CHANNELS, retention_period => 1},
    [begin
         _ = barrel_docdb:delete_db(D),
         {ok, _} = barrel_docdb:create_db(D, Opts)
     end || D <- [Src, Tgt]],
    [{src, Src}, {tgt, Tgt} | Config].

end_per_testcase(_TC, Config) ->
    _ = meck:unload(),
    {ok, Tasks} = barrel_rep_tasks:list_tasks(),
    [_ = barrel_rep_tasks:delete_task(Id) || #{id := Id} <- Tasks],
    _ = barrel_docdb:delete_db(?config(src, Config)),
    _ = barrel_docdb:delete_db(?config(tgt, Config)),
    ok.

%%====================================================================
%% Failures and the checkpoint
%%====================================================================

failed_put_holds_checkpoint(Config) ->
    {Src, Tgt} = dbs(Config),
    put_att(Src, <<"d">>, <<"a">>, <<"attachment">>),
    mock_local(),
    meck:expect(barrel_rep_transport_local, put_attachment,
                fun(_, _, _, _, _) -> {error, unauthorized} end),
    ?assertMatch({error, {att_sync_failed, {att_transfer_failed,
                                            #{att_write_failures := 1}}}},
                 barrel_rep:replicate(Src, Tgt)),
    meck:unload(barrel_rep_transport_local),
    {ok, R} = barrel_rep:replicate(Src, Tgt),
    ?assertMatch(#{atts_written := 1}, att_stats(R)),
    ?assertEqual({ok, <<"attachment">>}, get_att(Tgt, <<"d">>, <<"a">>)).

%% Puts and deletes apply in feed order; nothing past the first failure
%% applies, and the checkpoint stops right before it.
failure_stops_in_feed_order(Config) ->
    {Src, Tgt} = dbs(Config),
    put_att(Src, <<"d">>, <<"x">>, <<"x">>),
    {ok, _} = barrel_rep:replicate(Src, Tgt),
    put_att(Src, <<"d">>, <<"a">>, <<"a">>),
    ok = barrel_docdb:delete_attachment(Src, <<"d">>, <<"x">>),
    put_att(Src, <<"d">>, <<"c">>, <<"c">>),
    mock_local(),
    meck:expect(barrel_rep_transport_local, delete_attachment,
                fun(_, _, _, _) -> {error, disk_full} end),
    ?assertMatch({error, {att_sync_failed, {att_transfer_failed,
                                            #{atts_written := 1}}}},
                 barrel_rep:replicate(Src, Tgt)),
    ?assertEqual({ok, <<"a">>}, get_att(Tgt, <<"d">>, <<"a">>)),
    ?assertEqual({ok, <<"x">>}, get_att(Tgt, <<"d">>, <<"x">>)),
    ?assertEqual({error, not_found}, get_att(Tgt, <<"d">>, <<"c">>)),
    meck:unload(barrel_rep_transport_local),
    %% the retry resumes at the delete: `a' is not offered again
    {ok, R} = barrel_rep:replicate(Src, Tgt),
    ?assertMatch(#{atts_written := 1, atts_deleted := 1, atts_skipped := 0},
                 att_stats(R)),
    ?assertEqual({error, not_found}, get_att(Tgt, <<"d">>, <<"x">>)),
    ?assertEqual({ok, <<"c">>}, get_att(Tgt, <<"d">>, <<"c">>)).

source_gone_without_successor_fails(Config) ->
    {Src, Tgt} = dbs(Config),
    put_att(Src, <<"d">>, <<"a">>, <<"a">>),
    mock_local(),
    meck:expect(barrel_rep_transport_local, get_attachment_stream,
                fun(_, _, _) -> {error, not_found} end),
    ?assertMatch({error, {att_sync_failed, {att_transfer_failed,
                                            #{att_read_failures := 1}}}},
                 barrel_rep:replicate(Src, Tgt)),
    meck:unload(barrel_rep_transport_local),
    {ok, _} = barrel_rep:replicate(Src, Tgt),
    ?assertEqual({ok, <<"a">>}, get_att(Tgt, <<"d">>, <<"a">>)).

%% Deleted at the source after the feed read: its delete comes later.
source_gone_after_delete_skipped(Config) ->
    {Src, Tgt} = dbs(Config),
    put_att(Src, <<"d">>, <<"a">>, <<"a">>),
    {ok, Page, _} = barrel_docdb:att_changes(Src, first),
    ok = barrel_docdb:delete_attachment(Src, <<"d">>, <<"a">>),
    mock_local(),
    meck:expect(barrel_rep_transport_local, att_changes,
                fun(_, first, _) -> {ok, Page, maps:get(seq, hd(Page))};
                   (Db, Since, Opts) -> meck:passthrough([Db, Since, Opts])
                end),
    {ok, R} = barrel_rep:replicate(Src, Tgt),
    ?assertMatch(#{atts_skipped := 1, att_read_failures := 0}, att_stats(R)),
    ?assertEqual({error, not_found}, get_att(Tgt, <<"d">>, <<"a">>)).

unreadable_source_checkpoint(Config) ->
    unreadable_checkpoint(Config, src, source).

unreadable_target_checkpoint(Config) ->
    unreadable_checkpoint(Config, tgt, target).

unreadable_checkpoint(Config, Key, Side) ->
    {Src, Tgt} = dbs(Config),
    Bad = ?config(Key, Config),
    put_att(Src, <<"d">>, <<"a">>, <<"a">>),
    mock_local(),
    meck:expect(barrel_rep_transport_local, get_local_doc,
                fun(Db, <<"replication-checkpoint-att-", _/binary>>)
                      when Db =:= Bad ->
                        {error, io_error};
                   (Db, Id) ->
                        meck:passthrough([Db, Id])
                end),
    ?assertEqual({error, {att_sync_failed,
                          {checkpoint_unreadable, Side, io_error}}},
                 barrel_rep:replicate(Src, Tgt)),
    %% zero attachment mutations
    ?assertEqual(0, meck:num_calls(barrel_rep_transport_local,
                                   put_attachment, '_')),
    ?assertEqual({error, not_found}, get_att(Tgt, <<"d">>, <<"a">>)).

checkpoint_write_failure_retry_converges(Config) ->
    {Src, Tgt} = dbs(Config),
    put_att(Src, <<"d">>, <<"a">>, <<"a">>),
    mock_local(),
    meck:expect(barrel_rep_transport_local, put_local_doc,
                fun(_, <<"replication-checkpoint-att-", _/binary>>, _) ->
                        {error, disk_full};
                   (Db, Id, Doc) ->
                        meck:passthrough([Db, Id, Doc])
                end),
    ?assertMatch({error, {att_sync_failed,
                          {checkpoint_failed, _, disk_full}}},
                 barrel_rep:replicate(Src, Tgt)),
    %% delivered before the checkpoint failed
    ?assertEqual({ok, <<"a">>}, get_att(Tgt, <<"d">>, <<"a">>)),
    meck:unload(barrel_rep_transport_local),
    {ok, R} = barrel_rep:replicate(Src, Tgt),
    ?assertMatch(#{atts_written := 0, atts_skipped := 1}, att_stats(R)),
    {ok, R2} = barrel_rep:replicate(Src, Tgt),
    ?assertMatch(#{atts_written := 0, atts_skipped := 0}, att_stats(R2)).

%%====================================================================
%% Durability
%%====================================================================

%% A batch the target already had still syncs before the checkpoint.
flush_before_every_checkpoint(Config) ->
    {Src, Tgt} = dbs(Config),
    put_att(Src, <<"d">>, <<"a">>, <<"a">>),
    {ok, _} = barrel_rep:replicate(Src, Tgt),
    Self = self(),
    mock_local(),
    meck:expect(barrel_rep_transport_local, flush_attachments,
                fun(Db) -> Self ! flush, meck:passthrough([Db]) end),
    Fun = fun(Seq) -> Self ! {checkpoint, Seq}, ok end,
    {ok, Stats} = barrel_rep_att:sync_from(
                    Src, Tgt, barrel_rep_transport_local,
                    barrel_rep_transport_local, first, Fun, #{}),
    ?assertMatch(#{atts_skipped := 1}, Stats),
    ?assertMatch([flush, {checkpoint, _}], drain()).

failed_flush_holds_checkpoint(Config) ->
    {Src, Tgt} = dbs(Config),
    put_att(Src, <<"d">>, <<"a">>, <<"a">>),
    mock_local(),
    meck:expect(barrel_rep_transport_local, flush_attachments,
                fun(_) -> {error, eio} end),
    ?assertEqual({error, {att_sync_failed, {att_flush_failed, eio}}},
                 barrel_rep:replicate(Src, Tgt)),
    meck:unload(barrel_rep_transport_local),
    %% the checkpoint did not move: the attachment is offered again
    {ok, R} = barrel_rep:replicate(Src, Tgt),
    ?assertMatch(#{atts_skipped := 1}, att_stats(R)).

flush_unsupported_fails_before_mutation(Config) ->
    Src = ?config(src, Config),
    Tgt = <<"flush_unsupported_minimal_tgt">>,
    _ = barrel_docdb:delete_db(Tgt),
    {ok, _} = barrel_docdb:create_db(Tgt, #{
        att_opts => #{backend => barrel_docdb_test_att_backend_minimal}}),
    try
        put_att(Src, <<"d">>, <<"a">>, <<"a">>),
        ?assertEqual({error, {att_sync_failed, att_flush_unsupported}},
                     barrel_rep:replicate(Src, Tgt)),
        ?assertEqual({error, not_found}, get_att(Tgt, <<"d">>, <<"a">>))
    after
        _ = barrel_docdb:delete_db(Tgt)
    end.

best_effort_skips_flush(Config) ->
    {Src, Tgt} = dbs(Config),
    put_att(Src, <<"d">>, <<"a">>, <<"a">>),
    mock_local(),
    {ok, _} = barrel_rep:replicate(Src, Tgt, #{att_durability => best_effort}),
    ?assertEqual(0, meck:num_calls(barrel_rep_transport_local,
                                   flush_attachments, '_')),
    ?assertEqual({ok, <<"a">>}, get_att(Tgt, <<"d">>, <<"a">>)).

backend_flush(Config) ->
    Src = ?config(src, Config),
    ?assertEqual(ok, barrel_docdb:flush_attachments(Src)),
    None = <<"backend_flush_none">>,
    Min = <<"backend_flush_minimal">>,
    {ok, _} = barrel_docdb:create_db(None, #{att_opts => #{backend => none}}),
    {ok, _} = barrel_docdb:create_db(Min, #{
        att_opts => #{backend => barrel_docdb_test_att_backend_minimal}}),
    try
        ?assertEqual(ok, barrel_docdb:flush_attachments(None)),
        ?assertEqual({error, unsupported}, barrel_docdb:flush_attachments(Min)),
        ?assertEqual({ok, #{flush => false, filter => true}},
                     barrel_docdb:att_capabilities(Min))
    after
        _ = barrel_docdb:delete_db(None),
        _ = barrel_docdb:delete_db(Min)
    end.

invalid_durability_rejected(Config) ->
    {Src, Tgt} = dbs(Config),
    ?assertEqual({error, {invalid_option, {att_durability, sometimes}}},
                 barrel_rep:replicate(Src, Tgt, #{att_durability => sometimes})),
    ?assertEqual({error, {invalid_option, {att_durability, sometimes}}},
                 barrel_rep_tasks:start_task(#{source => Src, target => Tgt,
                                               att_durability => sometimes})).

%%====================================================================
%% Tasks
%%====================================================================

task_failure_keeps_position(Config) ->
    {Src, Tgt} = dbs(Config),
    put_att(Src, <<"d">>, <<"a">>, <<"a">>),
    mock_local(),
    meck:expect(barrel_rep_transport_local, put_attachment,
                fun(_, _, _, _, _) -> {error, unauthorized} end),
    {ok, Id} = barrel_rep_tasks:start_task(#{source => Src, target => Tgt,
                                             mode => continuous}),
    ok = wait_until(fun() ->
             case barrel_rep_tasks:get_task(Id) of
                 {ok, #{last_error := E}} ->
                     binary:match(E, <<"att_transfer_failed">>) =/= nomatch;
                 _ -> false
             end
         end),
    {ok, #{status := running, att_seq := first}} = barrel_rep_tasks:get_task(Id),
    meck:unload(barrel_rep_transport_local),
    ok = wait_until(fun() -> get_att(Tgt, <<"d">>, <<"a">>) =:= {ok, <<"a">>} end).

%% Each direction of a `both' task keeps its own attachment position.
both_task_keeps_each_side(Config) ->
    {Src, Tgt} = dbs(Config),
    put_att(Tgt, <<"b">>, <<"a">>, <<"older-target">>),
    put_att(Src, <<"a">>, <<"a">>, <<"newer-source">>),
    mock_local(),
    meck:expect(barrel_rep_transport_local, get_changes,
                fun(D, Since, Opts) ->
                    case D of Tgt -> timer:sleep(150); _ -> ok end,
                    meck:passthrough([D, Since, Opts])
                end),
    {ok, Id} = barrel_rep_tasks:start_task(#{source => Src, target => Tgt,
                                             direction => both}),
    {ok, #{status := completed}} = await_task(Id),
    ?assertEqual({ok, <<"older-target">>}, get_att(Src, <<"b">>, <<"a">>)),
    ?assertEqual({ok, <<"newer-source">>}, get_att(Tgt, <<"a">>, <<"a">>)).

both_task_restart_keeps_positions(Config) ->
    {Src, Tgt} = dbs(Config),
    put_att(Src, <<"s">>, <<"a">>, <<"s">>),
    put_att(Tgt, <<"t">>, <<"a">>, <<"t">>),
    {ok, Id} = barrel_rep_tasks:start_task(#{source => Src, target => Tgt,
                                             direction => both,
                                             mode => continuous}),
    ok = wait_until(fun() ->
             get_att(Tgt, <<"s">>, <<"a">>) =:= {ok, <<"s">>}
                 andalso get_att(Src, <<"t">>, <<"a">>) =:= {ok, <<"t">>}
         end),
    ok = wait_until(fun() -> positions(Id) =/= {first, first} end),
    ok = wait_until(fun() ->
             {P, Q} = positions(Id), P =/= first andalso Q =/= first
         end),
    ok = barrel_rep_tasks:pause_task(Id),
    Before = positions(Id),
    ok = barrel_rep_tasks:resume_task(Id),
    timer:sleep(300),
    {Push, Pull} = positions(Id),
    {Push0, Pull0} = Before,
    ?assertNot(barrel_hlc:less(Push, Push0)),
    ?assertNot(barrel_hlc:less(Pull, Pull0)),
    %% both sides still flow after the restart
    put_att(Tgt, <<"t2">>, <<"a">>, <<"t2">>),
    put_att(Src, <<"s2">>, <<"a">>, <<"s2">>),
    ok = wait_until(fun() ->
             get_att(Src, <<"t2">>, <<"a">>) =:= {ok, <<"t2">>}
                 andalso get_att(Tgt, <<"s2">>, <<"a">>) =:= {ok, <<"s2">>}
         end).

positions(Id) ->
    {ok, Task} = barrel_rep_tasks:get_task(Id),
    {maps:get(att_seq, Task), maps:get(pull_att_seq, Task, first)}.

%%====================================================================
%% Deletes
%%====================================================================

%% Delete rows outlive the retention window (tombstone_purge_after 0):
%% a resync from first removes the attachment and a stale put loses.
deletes_kept_past_retention(Config) ->
    {Src, Tgt} = dbs(Config),
    put_att(Src, <<"d">>, <<"a">>, <<"old-value">>),
    {ok, _} = barrel_rep:replicate(Src, Tgt),
    {ok, [#{origin := OldOrigin}], _} = barrel_docdb:att_changes(Src, first),
    ok = barrel_docdb:delete_attachment(Src, <<"d">>, <<"a">>),
    timer:sleep(1200),
    {ok, Stats} = barrel_docdb:sweep_retention(Src),
    ?assertEqual(0, maps:get(att_tombstones_swept, Stats)),
    ?assertEqual(undefined, barrel_docdb:att_floor(Src)),
    {ok, [#{op := delete}], _} = barrel_docdb:att_changes(Src, first),
    {ok, R} = barrel_rep_att:sync_from(
                Src, Tgt, barrel_rep_transport_local,
                barrel_rep_transport_local, first, fun(_) -> ok end, #{}),
    ?assertMatch(#{atts_deleted := 1}, R),
    ?assertEqual({error, not_found}, get_att(Tgt, <<"d">>, <<"a">>)),
    %% an old replicated put does not bring it back
    {ok, ignored} = barrel_docdb:put_attachment(
                      Tgt, <<"d">>, <<"a">>, <<"old-value">>,
                      #{origin_hlc => OldOrigin}),
    ?assertEqual({error, not_found}, get_att(Tgt, <<"d">>, <<"a">>)).

%%====================================================================
%% Filters
%%====================================================================

path_filter_excludes_private(Config) ->
    filter_excludes_private(Config, #{paths => [<<"type/public">>]}).

query_filter_excludes_private(Config) ->
    filter_excludes_private(
      Config, #{query => #{where => [{path, [<<"type">>], <<"public">>}]}}).

channel_filter_excludes_private(Config) ->
    filter_excludes_private(Config, #{channel => <<"public">>}).

filter_excludes_private(Config, Filter) ->
    {Src, Tgt} = dbs(Config),
    put_doc(Src, <<"pub">>, <<"public">>),
    put_doc(Src, <<"priv">>, <<"private">>),
    put_att(Src, <<"pub">>, <<"a">>, <<"public attachment">>),
    put_att(Src, <<"priv">>, <<"a">>, <<"private attachment">>),
    {ok, R} = barrel_rep:replicate(Src, Tgt, #{filter => Filter}),
    ?assertMatch(#{docs_written := 1}, R),
    ?assertEqual({ok, <<"public attachment">>}, get_att(Tgt, <<"pub">>, <<"a">>)),
    ?assertEqual({error, not_found}, get_att(Tgt, <<"priv">>, <<"a">>)),
    %% the source feed itself filters
    {ok, Entries, _} = barrel_docdb:att_changes(Src, first,
                                                #{filter => Filter}),
    ?assertEqual([<<"pub">>], [Id || #{id := Id} <- Entries]).

unknown_channel_stops_run(Config) ->
    {Src, Tgt} = dbs(Config),
    put_att(Src, <<"d">>, <<"a">>, <<"a">>),
    ?assertEqual({error, {unknown_channel, <<"nope">>}},
                 barrel_docdb:att_changes(Src, first,
                                          #{filter => #{channel => <<"nope">>}})),
    ?assertEqual({error, {unknown_channel, <<"nope">>}},
                 barrel_rep_att:sync_from(
                   Src, Tgt, barrel_rep_transport_local,
                   barrel_rep_transport_local, first, fun(_) -> ok end,
                   #{filter => #{channel => <<"nope">>}})),
    ?assertMatch({error, _}, barrel_rep:replicate(
                               Src, Tgt,
                               #{filter => #{channel => <<"nope">>}})),
    ?assertEqual({error, not_found}, get_att(Tgt, <<"d">>, <<"a">>)).

filter_evaluation_error_stops_run(Config) ->
    {Src, Tgt} = dbs(Config),
    put_doc(Src, <<"pub">>, <<"public">>),
    put_att(Src, <<"pub">>, <<"a">>, <<"a">>),
    ok = meck:new(barrel_query, [passthrough]),
    meck:expect(barrel_query, match, fun(_, _) -> error(boom) end),
    Filter = #{query => #{where => [{path, [<<"type">>], <<"public">>}]}},
    ?assertMatch({error, {filter_failed, _}},
                 barrel_docdb:att_changes(Src, first, #{filter => Filter})),
    ?assertMatch({error, {filter_failed, _}},
                 barrel_rep_att:sync_from(
                   Src, Tgt, barrel_rep_transport_local,
                   barrel_rep_transport_local, first, fun(_) -> ok end,
                   #{filter => Filter})),
    ?assertEqual({error, not_found}, get_att(Tgt, <<"pub">>, <<"a">>)).

%% A document moving into scope brings its unchanged attachment.
doc_entering_scope_gets_attachment(Config) ->
    {Src, Tgt} = dbs(Config),
    Filter = #{query => #{where => [{path, [<<"type">>], <<"public">>}]}},
    put_doc(Src, <<"d">>, <<"private">>),
    put_att(Src, <<"d">>, <<"a">>, <<"kept">>),
    {ok, _} = barrel_rep:replicate(Src, Tgt, #{filter => Filter}),
    ?assertEqual({error, not_found}, get_att(Tgt, <<"d">>, <<"a">>)),
    {ok, Doc} = barrel_docdb:get_doc(Src, <<"d">>),
    {ok, _} = barrel_docdb:put_doc(Src, Doc#{<<"type">> => <<"public">>}),
    {ok, _} = barrel_rep:replicate(Src, Tgt, #{filter => Filter}),
    ?assertEqual({ok, <<"kept">>}, get_att(Tgt, <<"d">>, <<"a">>)).

empty_filtered_page_continues(Config) ->
    {Src, Tgt} = dbs(Config),
    Filter = #{query => #{where => [{path, [<<"type">>], <<"public">>}]}},
    put_doc(Src, <<"p1">>, <<"private">>),
    put_doc(Src, <<"p2">>, <<"private">>),
    put_doc(Src, <<"pub">>, <<"public">>),
    {ok, _} = barrel_rep:replicate(Src, Tgt, #{filter => Filter}),
    %% the first attachment page holds only private rows
    put_att(Src, <<"p1">>, <<"a">>, <<"1">>),
    put_att(Src, <<"p2">>, <<"a">>, <<"2">>),
    put_att(Src, <<"pub">>, <<"a">>, <<"public">>),
    {ok, R} = barrel_rep:replicate(Src, Tgt, #{filter => Filter,
                                               att_batch_size => 2}),
    ?assertMatch(#{atts_written := 1}, att_stats(R)),
    ?assertEqual({ok, <<"public">>}, get_att(Tgt, <<"pub">>, <<"a">>)),
    ?assertEqual({error, not_found}, get_att(Tgt, <<"p1">>, <<"a">>)).

no_progress_page_fails(Config) ->
    {Src, Tgt} = dbs(Config),
    put_att(Src, <<"d">>, <<"a">>, <<"a">>),
    {ok, [E], Seq} = barrel_docdb:att_changes(Src, first),
    mock_local(),
    meck:expect(barrel_rep_transport_local, att_changes,
                fun(_, _, _) -> {ok, [E], Seq} end),
    ?assertEqual({error, {no_progress, Seq, Seq}},
                 barrel_rep_att:sync_from(
                   Src, Tgt, barrel_rep_transport_local,
                   barrel_rep_transport_local, Seq, fun(_) -> ok end, #{})).

source_without_filter_support_fails(Config) ->
    {Src, Tgt} = dbs(Config),
    put_doc(Src, <<"priv">>, <<"private">>),
    put_att(Src, <<"priv">>, <<"a">>, <<"a">>),
    mock_local(),
    meck:expect(barrel_rep_transport_local, att_capabilities,
                fun(_) -> {ok, #{flush => true, filter => false}} end),
    ?assertEqual({error, {att_sync_failed, att_filter_unsupported}},
                 barrel_rep:replicate(Src, Tgt, #{filter => #{paths => [<<"type/public">>]}})),
    ?assertEqual({error, not_found}, get_att(Tgt, <<"priv">>, <<"a">>)).

task_filter_applies_to_attachments(Config) ->
    {Src, Tgt} = dbs(Config),
    put_doc(Src, <<"pub">>, <<"public">>),
    put_doc(Src, <<"priv">>, <<"private">>),
    put_att(Src, <<"pub">>, <<"a">>, <<"public">>),
    put_att(Src, <<"priv">>, <<"a">>, <<"private">>),
    {ok, Id} = barrel_rep_tasks:start_task(
                 #{source => Src, target => Tgt,
                   filter => #{paths => [<<"type/public">>]}}),
    {ok, #{status := completed}} = await_task(Id),
    ?assertEqual({ok, <<"public">>}, get_att(Tgt, <<"pub">>, <<"a">>)),
    ?assertEqual({error, not_found}, get_att(Tgt, <<"priv">>, <<"a">>)).

%%====================================================================
%% Helpers
%%====================================================================

dbs(Config) ->
    {?config(src, Config), ?config(tgt, Config)}.

mock_local() ->
    ok = meck:new(barrel_rep_transport_local, [passthrough]).

put_att(Db, DocId, Name, Data) ->
    {ok, _} = barrel_docdb:put_attachment(Db, DocId, Name, Data).

get_att(Db, DocId, Name) ->
    barrel_docdb:get_attachment(Db, DocId, Name).

put_doc(Db, Id, Type) ->
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => Id, <<"type">> => Type}).

att_stats(Result) ->
    maps:get(att_sync, Result).

drain() ->
    receive Msg -> [Msg | drain()]
    after 0 -> []
    end.

await_task(Id) ->
    await_task(Id, 500).

await_task(_Id, 0) ->
    timeout;
await_task(Id, N) ->
    case barrel_rep_tasks:get_task(Id) of
        {ok, #{status := S} = Task} when S =:= completed; S =:= failed ->
            {ok, Task};
        _ ->
            timer:sleep(10),
            await_task(Id, N - 1)
    end.

wait_until(Fun) ->
    wait_until(Fun, 300).

wait_until(_Fun, 0) ->
    timeout;
wait_until(Fun, N) ->
    case Fun() of
        true -> ok;
        false -> timer:sleep(20), wait_until(Fun, N - 1)
    end.
