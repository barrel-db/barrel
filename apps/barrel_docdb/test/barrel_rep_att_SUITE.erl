%%%-------------------------------------------------------------------
%%% @doc Attachment replication: the second phase of a replication run
%%% ships blobs content-addressed with LWW convergence, independent
%%% checkpoints and floor-guarded resync.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_rep_att_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([basic_sync_and_digest_skip/1,
         update_propagates/1,
         delete_propagates/1,
         bidirectional_lww_convergence/1,
         checkpoint_resume/1,
         off_switch/1,
         mixed_docs_and_attachments/1,
         floor_forces_resync/1]).

all() ->
    [basic_sync_and_digest_skip,
     update_propagates,
     delete_propagates,
     bidirectional_lww_convergence,
     checkpoint_resume,
     off_switch,
     mixed_docs_and_attachments,
     floor_forces_resync].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Dir = "/tmp/barrel_rep_att_test_"
        ++ integer_to_list(erlang:system_time(millisecond)),
    [{dir, Dir} | Config].

end_per_suite(Config) ->
    os:cmd("rm -rf " ++ ?config(dir, Config)),
    ok.

init_per_testcase(TC, Config) ->
    Src = <<(atom_to_binary(TC, utf8))/binary, "_src">>,
    Tgt = <<(atom_to_binary(TC, utf8))/binary, "_tgt">>,
    Dir = ?config(dir, Config),
    {ok, _} = barrel_docdb:create_db(Src, #{data_dir => Dir}),
    {ok, _} = barrel_docdb:create_db(Tgt, #{data_dir => Dir}),
    [{src, Src}, {tgt, Tgt} | Config].

end_per_testcase(_TC, Config) ->
    try barrel_docdb:delete_db(?config(src, Config)) catch _:_ -> ok end,
    try barrel_docdb:delete_db(?config(tgt, Config)) catch _:_ -> ok end,
    ok.

att_stats(Result) ->
    maps:get(att_sync, Result).

%%====================================================================
%% Cases
%%====================================================================

basic_sync_and_digest_skip(Config) ->
    Src = ?config(src, Config),
    Tgt = ?config(tgt, Config),
    {ok, _} = barrel_docdb:put_attachment(Src, <<"d1">>, <<"a.txt">>,
                                          <<"alpha">>),
    {ok, _} = barrel_docdb:put_attachment(Src, <<"d2">>, <<"b.bin">>,
                                          binary:copy(<<"z">>, 200000)),
    {ok, R1} = barrel_rep:replicate(Src, Tgt),
    ?assertMatch(#{atts_written := 2, atts_skipped := 0}, att_stats(R1)),
    {ok, <<"alpha">>} = barrel_docdb:get_attachment(Tgt, <<"d1">>,
                                                    <<"a.txt">>),
    {ok, Big} = barrel_docdb:get_attachment(Tgt, <<"d2">>, <<"b.bin">>),
    ?assertEqual(200000, byte_size(Big)),
    %% content types survive the trip
    {ok, SrcInfo} = barrel_docdb:get_attachment_info(Src, <<"d1">>,
                                                     <<"a.txt">>),
    {ok, TgtInfo} = barrel_docdb:get_attachment_info(Tgt, <<"d1">>,
                                                     <<"a.txt">>),
    ?assertEqual(maps:get(content_type, SrcInfo),
                 maps:get(content_type, TgtInfo)),
    ?assertEqual(maps:get(digest, SrcInfo), maps:get(digest, TgtInfo)),
    %% second run: nothing to do (checkpointed, zero re-transfer)
    {ok, R2} = barrel_rep:replicate(Src, Tgt),
    ?assertMatch(#{atts_written := 0}, att_stats(R2)).

update_propagates(Config) ->
    Src = ?config(src, Config),
    Tgt = ?config(tgt, Config),
    {ok, _} = barrel_docdb:put_attachment(Src, <<"d">>, <<"f">>, <<"v1">>),
    {ok, _} = barrel_rep:replicate(Src, Tgt),
    {ok, _} = barrel_docdb:put_attachment(Src, <<"d">>, <<"f">>, <<"v2">>),
    {ok, R} = barrel_rep:replicate(Src, Tgt),
    ?assertMatch(#{atts_written := 1}, att_stats(R)),
    {ok, <<"v2">>} = barrel_docdb:get_attachment(Tgt, <<"d">>, <<"f">>).

delete_propagates(Config) ->
    Src = ?config(src, Config),
    Tgt = ?config(tgt, Config),
    {ok, _} = barrel_docdb:put_attachment(Src, <<"d">>, <<"f">>, <<"v1">>),
    {ok, _} = barrel_rep:replicate(Src, Tgt),
    ok = barrel_docdb:delete_attachment(Src, <<"d">>, <<"f">>),
    {ok, R} = barrel_rep:replicate(Src, Tgt),
    ?assertMatch(#{atts_deleted := 1}, att_stats(R)),
    ?assertEqual({error, not_found},
                 barrel_docdb:get_attachment(Tgt, <<"d">>, <<"f">>)),
    %% redelivery of the whole feed is harmless
    {ok, _} = barrel_rep:replicate(Src, Tgt).

bidirectional_lww_convergence(Config) ->
    A = ?config(src, Config),
    B = ?config(tgt, Config),
    %% both sides write the same attachment concurrently
    {ok, _} = barrel_docdb:put_attachment(A, <<"d">>, <<"f">>, <<"from a">>),
    {ok, _} = barrel_docdb:put_attachment(B, <<"d">>, <<"f">>, <<"from b">>),
    %% sync both ways twice: no oscillation, both converge to one value
    {ok, _} = barrel_rep:replicate(A, B),
    {ok, _} = barrel_rep:replicate(B, A),
    {ok, _} = barrel_rep:replicate(A, B),
    {ok, _} = barrel_rep:replicate(B, A),
    {ok, VA} = barrel_docdb:get_attachment(A, <<"d">>, <<"f">>),
    {ok, VB} = barrel_docdb:get_attachment(B, <<"d">>, <<"f">>),
    ?assertEqual(VA, VB),
    ?assert(lists:member(VA, [<<"from a">>, <<"from b">>])),
    %% and a further round moves nothing
    {ok, R} = barrel_rep:replicate(A, B),
    ?assertMatch(#{atts_written := 0, atts_ignored := 0}, att_stats(R)).

checkpoint_resume(Config) ->
    Src = ?config(src, Config),
    Tgt = ?config(tgt, Config),
    {ok, _} = barrel_docdb:put_attachment(Src, <<"d1">>, <<"a">>, <<"1">>),
    {ok, _} = barrel_rep:replicate(Src, Tgt),
    {ok, _} = barrel_docdb:put_attachment(Src, <<"d2">>, <<"b">>, <<"2">>),
    {ok, R} = barrel_rep:replicate(Src, Tgt),
    %% only the delta ships; the first attachment is not even offered
    ?assertMatch(#{atts_written := 1, atts_skipped := 0}, att_stats(R)).

off_switch(Config) ->
    Src = ?config(src, Config),
    Tgt = ?config(tgt, Config),
    {ok, _} = barrel_docdb:put_attachment(Src, <<"d">>, <<"f">>, <<"v">>),
    {ok, R} = barrel_rep:replicate(Src, Tgt, #{attachments => false}),
    ?assertEqual(disabled, att_stats(R)),
    ?assertEqual({error, not_found},
                 barrel_docdb:get_attachment(Tgt, <<"d">>, <<"f">>)).

mixed_docs_and_attachments(Config) ->
    Src = ?config(src, Config),
    Tgt = ?config(tgt, Config),
    {ok, _} = barrel_docdb:put_doc(Src, #{<<"id">> => <<"d">>,
                                          <<"kind">> => <<"report">>}),
    {ok, _} = barrel_docdb:put_attachment(Src, <<"d">>, <<"body.pdf">>,
                                          <<"pdf bytes">>),
    {ok, R} = barrel_rep:replicate(Src, Tgt),
    ?assertMatch(#{docs_written := 1}, R),
    ?assertMatch(#{atts_written := 1}, att_stats(R)),
    {ok, _} = barrel_docdb:get_doc(Tgt, <<"d">>),
    {ok, <<"pdf bytes">>} =
        barrel_docdb:get_attachment(Tgt, <<"d">>, <<"body.pdf">>).

floor_forces_resync(Config) ->
    Dir = ?config(dir, Config),
    Src = <<"floor_src2">>,
    Tgt = ?config(tgt, Config),
    {ok, _} = barrel_docdb:create_db(Src, #{data_dir => Dir,
                                            retention_period => 1}),
    try
        {ok, _} = barrel_docdb:put_attachment(Src, <<"d">>, <<"keep">>,
                                              <<"kept">>),
        {ok, _} = barrel_rep:replicate(Src, Tgt),
        %% a delete swept past the window moves the floor above the
        %% target's checkpoint
        {ok, _} = barrel_docdb:put_attachment(Src, <<"d">>, <<"gone">>,
                                              <<"bye">>),
        ok = barrel_docdb:delete_attachment(Src, <<"d">>, <<"gone">>),
        timer:sleep(1200),
        {ok, _} = barrel_docdb:sweep_retention(Src),
        ?assertNotEqual(undefined, barrel_docdb:att_floor(Src)),
        %% replication restarts from first and still converges
        {ok, R} = barrel_rep:replicate(Src, Tgt),
        ?assertMatch(#{atts_written := 0}, att_stats(R)),
        {ok, <<"kept">>} = barrel_docdb:get_attachment(Tgt, <<"d">>,
                                                       <<"keep">>)
    after
        _ = barrel_docdb:delete_db(Src)
    end.
