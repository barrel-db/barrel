%%% @doc Attachment store defects from the 2.0.0 review: per-upload
%%% chunk generations, snapshot reads, writer-serialized commits, feed
%%% clock floors, writer restarts and inline values shaped like chunk
%%% metadata.
-module(barrel_review_att_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([stream_overwrite_keeps_old/1,
         concurrent_overwrite_and_delete/1,
         older_replicated_write_loses/1,
         one_feed_row_per_attachment/1,
         clock_restart_keeps_feed_order/1,
         reopen_after_clock_back_keeps_feed_order/1,
         unreachable_clock_floor_fails_writes/1,
         writer_killed_with_open_streams/1,
         branch_attachments_stay_separate/1,
         read_only_rejects_commits/1,
         legacy_chunk_layout/1,
         metadata_shaped_values_read_back/1,
         abandoned_uploads_removed/1,
         abandoned_uploads_removed_without_retention/1]).

all() ->
    [stream_overwrite_keeps_old,
     concurrent_overwrite_and_delete,
     older_replicated_write_loses,
     one_feed_row_per_attachment,
     clock_restart_keeps_feed_order,
     reopen_after_clock_back_keeps_feed_order,
     unreachable_clock_floor_fails_writes,
     writer_killed_with_open_streams,
     branch_attachments_stay_separate,
     read_only_rejects_commits,
     legacy_chunk_layout,
     metadata_shaped_values_read_back,
     abandoned_uploads_removed,
     abandoned_uploads_removed_without_retention].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    Db = atom_to_binary(TC),
    Dbs = [Db, <<Db/binary, "_b">>],
    [barrel_docdb:delete_db(D) || D <- Dbs],
    ok = hlc:set_maxoffset(0, whereis(barrel_hlc_clock)),
    [{db, Db}, {dbs, Dbs} | Config].

end_per_testcase(_TC, Config) ->
    _ = meck:unload(),
    ok = hlc:set_maxoffset(0, whereis(barrel_hlc_clock)),
    [barrel_docdb:delete_db(D) || D <- lists:reverse(?config(dbs, Config))],
    ok.

%%====================================================================
%% Upload generations
%%====================================================================

stream_overwrite_keeps_old(Config) ->
    Db = ?config(db, Config),
    {ok, _} = create(Db),
    Old = <<"AAAABBBB">>,
    {ok, _} = barrel_docdb:put_attachment(Db, <<"d">>, <<"a">>, Old,
                                          #{sync => true}),
    {ok, W} = open_writer(Db, #{}),
    {ok, W2} = barrel_docdb:write_attachment_chunk(W, <<"XXXX">>),
    %% an unfinished replacement is invisible
    ?assertEqual({ok, Old}, read_att(Db)),
    ?assertEqual({ok, Old}, stream_read(Db)),
    ok = barrel_docdb:abort_attachment_writer(W2),
    ?assertEqual({ok, Old}, read_att(Db)),
    %% a stale replicated upload is ignored and leaves it intact
    {ok, W3} = open_writer(Db, #{origin_hlc => barrel_hlc:min()}),
    {ok, W4} = barrel_docdb:write_attachment_chunk(W3, <<"XXXXYYYY">>),
    ?assertEqual({ok, ignored}, barrel_docdb:finish_attachment_writer(W4)),
    ?assertEqual({ok, Old}, read_att(Db)),
    ?assertEqual({ok, Old}, stream_read(Db)),
    %% and nothing staged is left behind
    ?assertEqual([], staged_gens(Db, <<"d">>, <<"a">>) -- published_gen(Db)).

concurrent_overwrite_and_delete(Config) ->
    Db = ?config(db, Config),
    {ok, _} = create(Db),
    A = binary:copy(<<"a">>, 64),
    B = binary:copy(<<"b">>, 64),
    {ok, _} = barrel_docdb:put_attachment(Db, <<"d">>, <<"a">>, A),
    Self = self(),
    N = 150,
    Writers = [spawn_link(fun() ->
                   [begin
                        _ = case I rem 3 of
                            0 -> barrel_docdb:put_attachment(Db, <<"d">>, <<"a">>, A);
                            1 -> stream_put(Db, B);
                            2 -> barrel_docdb:delete_attachment(Db, <<"d">>, <<"a">>)
                        end
                    end || I <- lists:seq(1, N)],
                   Self ! {done, self(), []}
               end) || _ <- [1, 2]],
    Readers = [spawn_link(fun() ->
                   Bad = [R || _ <- lists:seq(1, N),
                               R <- [Read(Db)],
                               not lists:member(R, [{ok, A}, {ok, B},
                                                    {error, not_found}])],
                   Self ! {done, self(), Bad}
               end) || Read <- [fun read_att/1, fun stream_read/1,
                                fun read_att/1, fun stream_read/1]],
    Bad = lists:append([receive {done, P, R} -> R
                        after 60000 -> ct:fail(timeout)
                        end || P <- Writers ++ Readers]),
    ?assertEqual([], lists:usort(Bad)).

%%====================================================================
%% Writer-serialized commits
%%====================================================================

older_replicated_write_loses(Config) ->
    Db = ?config(db, Config),
    {ok, _} = create(Db),
    Now = erlang:system_time(millisecond),
    H1 = barrel_hlc:from_wall_time(Now + 1),
    H2 = barrel_hlc:from_wall_time(Now + 2),
    Self = self(),
    ok = meck:new(barrel_att_feed, [passthrough, no_link]),
    ok = meck:expect(barrel_att_feed, check,
                     fun(Ref, D, Id, Name, Origin, Digest) ->
                         R = meck:passthrough([Ref, D, Id, Name, Origin, Digest]),
                         case Origin of
                             H1 -> Self ! {checked, self()},
                                   receive continue -> ok end;
                             _ -> ok
                         end,
                         R
                     end),
    Put = fun(Data, Origin) ->
              spawn(fun() ->
                  Self ! {put, Data,
                          barrel_docdb:put_attachment(Db, <<"d">>, <<"a">>, Data,
                                                      #{origin_hlc => Origin})}
              end)
          end,
    _ = Put(<<"old">>, H1),
    Checker = receive {checked, P} -> P after 5000 -> ct:fail(no_check) end,
    _ = Put(<<"new">>, H2),
    timer:sleep(100),
    Checker ! continue,
    Results = [receive {put, D, R} -> {D, R} after 5000 -> ct:fail(no_put) end
               || _ <- [1, 2]],
    ?assertMatch([{ok, _}, {ok, _}], [R || {_, R} <- lists:sort(Results)]),
    ?assertEqual({ok, <<"new">>}, read_att(Db)).

one_feed_row_per_attachment(Config) ->
    Db = ?config(db, Config),
    {ok, _} = create(Db),
    ok = meck:new(barrel_att_feed, [passthrough, no_link, non_strict]),
    Slow = fun(Args) -> R = meck:passthrough(Args), timer:sleep(20), R end,
    ok = meck:expect(barrel_att_feed, ops,
                     fun(A, B, C, D, E, F, G) -> Slow([A, B, C, D, E, F, G]) end),
    ok = meck:expect(barrel_att_feed, ops,
                     fun(A, B, C, D, E, F, G, H) ->
                         Slow([A, B, C, D, E, F, G, H])
                     end),
    Self = self(),
    Pids = [spawn_link(fun() ->
                Self ! {done, self(),
                        barrel_docdb:put_attachment(Db, <<"d">>, <<"a">>,
                                                    integer_to_binary(I))}
            end) || I <- lists:seq(1, 8)],
    [receive {done, P, {ok, _}} -> ok after 10000 -> ct:fail(timeout) end
     || P <- Pids],
    meck:unload(barrel_att_feed),
    {ok, Rows, _} = barrel_docdb:att_changes(Db, first),
    ?assertEqual(1, length([R || #{id := <<"d">>, name := <<"a">>} = R <- Rows])).

%%====================================================================
%% Feed clock
%%====================================================================

clock_restart_keeps_feed_order(Config) ->
    Db = ?config(db, Config),
    {ok, _} = create(Db),
    advance_clock(),
    {ok, _} = barrel_docdb:put_attachment(Db, <<"before">>, <<"a">>, <<"old">>),
    {ok, _, Seq} = barrel_docdb:att_changes(Db, first),
    restart_clock(),
    {ok, _} = barrel_docdb:put_attachment(Db, <<"after">>, <<"a">>, <<"new">>),
    {ok, Since, _} = barrel_docdb:att_changes(Db, Seq),
    ?assertMatch([#{id := <<"after">>}], Since).

reopen_after_clock_back_keeps_feed_order(Config) ->
    Db = ?config(db, Config),
    {ok, _} = create(Db),
    advance_clock(),
    {ok, _} = barrel_docdb:put_attachment(Db, <<"before">>, <<"a">>, <<"old">>),
    {ok, _, Seq} = barrel_docdb:att_changes(Db, first),
    ok = barrel_docdb:close_db(Db),
    restart_clock(),
    {ok, _} = create(Db),
    {ok, _} = barrel_docdb:put_attachment(Db, <<"after">>, <<"a">>, <<"new">>),
    {ok, Since, _} = barrel_docdb:att_changes(Db, Seq),
    ?assertMatch([#{id := <<"after">>}], Since).

unreachable_clock_floor_fails_writes(Config) ->
    Db = ?config(db, Config),
    {ok, _} = create(Db),
    advance_clock(),
    {ok, _} = barrel_docdb:put_attachment(Db, <<"d">>, <<"a">>, <<"old">>,
                                          #{sync => true}),
    ok = barrel_docdb:close_db(Db),
    restart_clock(),
    ok = hlc:set_maxoffset(1000, whereis(barrel_hlc_clock)),
    {ok, Pid} = create(Db),
    ?assertEqual({error, clock_skew},
                 barrel_docdb:put_attachment(Db, <<"d">>, <<"a">>, <<"new">>)),
    ?assertEqual({error, clock_skew},
                 barrel_docdb:put_doc(Db, #{<<"id">> => <<"x">>})),
    ?assertEqual({ok, <<"old">>}, read_att(Db)),
    ?assert(is_process_alive(Pid)),
    ?assertEqual({ok, Pid}, barrel_docdb:open_db(Db)).

%%====================================================================
%% Writer restarts, branches, read-only
%%====================================================================

%% Streams opened under a writer that restarted fail with db_restarted;
%% the upload publishes nothing.
writer_killed_with_open_streams(Config) ->
    Db = ?config(db, Config),
    {ok, Pid} = create(Db),
    Old = <<"0123456789abcdef">>,
    {ok, _} = barrel_docdb:put_attachment(Db, <<"d">>, <<"a">>, Old,
                                          #{sync => true}),
    {ok, S} = barrel_docdb:open_attachment_stream(Db, <<"d">>, <<"a">>),
    {ok, <<"0123">>, S1} = barrel_docdb:read_attachment_chunk(S),
    {ok, W} = open_writer(Db, #{}),
    {ok, W1} = barrel_docdb:write_attachment_chunk(W, <<"XXXXYYYY">>),
    kill(Pid),
    _ = await_restart(Db, Pid),
    ?assertEqual({error, db_restarted}, barrel_docdb:read_attachment_chunk(S1)),
    ?assertEqual({error, db_restarted},
                 barrel_docdb:write_attachment_chunk(W1, <<"ZZZZ">>)),
    ?assertEqual({error, db_restarted},
                 barrel_docdb:finish_attachment_writer(W1)),
    ?assertEqual(ok, barrel_docdb:abort_attachment_writer(W1)),
    ?assertEqual({ok, Old}, read_att(Db)),
    {ok, _} = barrel_docdb:put_attachment(Db, <<"d">>, <<"a">>, <<"new">>),
    ?assertEqual({ok, <<"new">>}, read_att(Db)).

branch_attachments_stay_separate(Config) ->
    Db = ?config(db, Config),
    Branch = <<Db/binary, "_b">>,
    {ok, Parent} = create(Db),
    {ok, _} = barrel_docdb:put_attachment(Db, <<"d">>, <<"a">>, <<"parent">>),
    {ok, BranchPid} = barrel_docdb:branch_db(Db, Branch, #{}),
    %% the writer that commits allocates the feed HLC
    P0 = writer_high(Parent, Db),
    B0 = writer_high(BranchPid, Branch),
    {ok, _} = barrel_docdb:put_attachment(Branch, <<"d">>, <<"a">>,
                                          <<"branch">>),
    {ok, W} = barrel_docdb:open_attachment_writer(
                Branch, <<"d">>, <<"s">>, <<"application/octet-stream">>),
    {ok, W1} = barrel_docdb:write_attachment_chunk(W, <<"BBBBBBBB">>),
    {ok, _} = barrel_docdb:finish_attachment_writer(W1),
    ok = barrel_docdb:delete_attachment(Branch, <<"d">>, <<"a">>),
    ?assertEqual(P0, writer_high(Parent, Db)),
    B1 = writer_high(BranchPid, Branch),
    ?assert(barrel_hlc:less(B0, B1)),
    {ok, _} = barrel_docdb:put_attachment(Db, <<"d">>, <<"s">>, <<"PPPPPPPP">>),
    ?assertEqual(B1, writer_high(BranchPid, Branch)),
    ?assert(barrel_hlc:less(P0, writer_high(Parent, Db))),
    ?assertEqual({ok, <<"parent">>}, read_att(Db)),
    ?assertEqual({error, not_found}, read_att(Branch)),
    ?assertEqual({ok, <<"BBBBBBBB">>},
                 barrel_docdb:get_attachment(Branch, <<"d">>, <<"s">>)),
    ?assertEqual({ok, <<"PPPPPPPP">>},
                 barrel_docdb:get_attachment(Db, <<"d">>, <<"s">>)).

read_only_rejects_commits(Config) ->
    Db = ?config(db, Config),
    {ok, _} = create(Db),
    {ok, _} = barrel_docdb:put_attachment(Db, <<"d">>, <<"a">>, <<"v">>,
                                          #{sync => true}),
    ok = barrel_docdb:close_db(Db),
    {ok, Pid} = barrel_docdb:create_db(Db, #{read_only => true}),
    ?assertEqual({error, read_only},
                 barrel_db_server:att_commit(Pid, undefined, fun(_) -> ok end)),
    ?assertEqual({error, read_only},
                 barrel_docdb:put_attachment(Db, <<"d">>, <<"a">>, <<"w">>)),
    ?assertEqual({error, read_only},
                 barrel_docdb:delete_attachment(Db, <<"d">>, <<"a">>)),
    ?assertEqual({ok, <<"v">>}, read_att(Db)).

%%====================================================================
%% Storage formats
%%====================================================================

%% Chunks written before generations existed (and a meta without one)
%% still read, and an overwrite or delete removes them.
legacy_chunk_layout(Config) ->
    Db = ?config(db, Config),
    {ok, Pid} = create(Db),
    {ok, #{ref := Ref}} = barrel_db_server:get_att_ref(Pid),
    Data = <<"0123456789ab">>,
    Digest = digest(Data),
    Json = #{<<"chunked">> => true, <<"chunk_size">> => 4,
             <<"chunk_count">> => 3, <<"length">> => 12,
             <<"content_type">> => <<"application/octet-stream">>,
             <<"digest">> => Digest},
    Metas = [{<<"v1">>, <<"BARREL_CHUNK_V1:",
                          (iolist_to_binary(json:encode(Json)))/binary>>},
             {<<"t2b">>, term_to_binary(#{chunked => true, chunk_size => 4,
                                          chunk_count => 3, length => 12,
                                          content_type => <<"x">>,
                                          digest => Digest})}],
    lists:foreach(
      fun({Name, Meta}) ->
          Key = att_key(Db, <<"d">>, Name),
          ok = rocksdb:put(Ref, Key, Meta, []),
          [ok = rocksdb:put(Ref, <<Key/binary, 0, I:32>>, C, [])
           || {I, C} <- lists:zip([0, 1, 2], [<<"0123">>, <<"4567">>, <<"89ab">>])],
          ?assertEqual({ok, Data},
                       barrel_docdb:get_attachment(Db, <<"d">>, Name)),
          ?assertEqual({ok, <<"4567">>},
                       barrel_docdb:read_attachment_range(Db, <<"d">>, Name, 4, 4))
      end, Metas),
    %% overwrite (chunked) and delete leave no old-layout chunk behind
    {ok, _} = barrel_docdb:put_attachment(Db, <<"d">>, <<"v1">>, <<"NEWNEWNE">>),
    ?assertEqual({ok, <<"NEWNEWNE">>},
                 barrel_docdb:get_attachment(Db, <<"d">>, <<"v1">>)),
    ?assertEqual([], legacy_chunks(Ref, Db, <<"d">>, <<"v1">>)),
    ok = barrel_docdb:delete_attachment(Db, <<"d">>, <<"t2b">>),
    ?assertEqual([], legacy_chunks(Ref, Db, <<"d">>, <<"t2b">>)),
    ?assertEqual({error, not_found},
                 barrel_docdb:get_attachment(Db, <<"d">>, <<"t2b">>)),
    ok = barrel_docdb:delete_attachment(Db, <<"d">>, <<"v1">>),
    ?assertEqual([], chunk_keys(Ref, Db, <<"d">>)).

%% Inline values shaped like chunk metadata are user data.
metadata_shaped_values_read_back(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:create_db(Db, #{}),
    T2b = term_to_binary(#{chunked => true, chunk_count => 0}),
    ?assertEqual(36, byte_size(T2b)),
    V1 = <<"BARREL_CHUNK_V1:{\"chunked\":true,\"chunk_size\":4,"
           "\"chunk_count\":0,\"length\":0,\"content_type\":\"x\","
           "\"digest\":\"y\"}">>,
    lists:foreach(
      fun({Name, Value}) ->
          {ok, _} = barrel_docdb:put_attachment(Db, <<"d">>, Name, Value),
          ?assertEqual({ok, Value},
                       barrel_docdb:get_attachment(Db, <<"d">>, Name)),
          ?assertMatch({ok, #{length := L, chunked := false}}
                         when L =:= byte_size(Value),
                       barrel_docdb:get_attachment_info(Db, <<"d">>, Name)),
          ?assertEqual({ok, Value}, stream_read(Db, Name))
      end, [{<<"t2b">>, T2b}, {<<"v1">>, V1}]).

%% Staged uploads older than att_upload_timeout go on the retention
%% sweep and at open; younger ones stay and still publish.
abandoned_uploads_removed(Config) ->
    Db = ?config(db, Config),
    Opts = #{att_upload_timeout => 1},
    {ok, Pid} = create(Db, Opts),
    {ok, Old0} = open_writer(Db, #{}),
    {ok, Old} = barrel_docdb:write_attachment_chunk(Old0, <<"OOOOOOOO">>),
    timer:sleep(1200),
    {ok, Young0} = open_writer(Db, #{}),
    {ok, Young} = barrel_docdb:write_attachment_chunk(Young0, <<"YYYYYYYY">>),
    ?assertEqual(2, length(staged_gens(Db, <<"d">>, <<"a">>))),
    {ok, Stats} = barrel_docdb:sweep_retention(Db),
    ?assertEqual(1, maps:get(att_uploads_removed, Stats)),
    ?assertEqual([gen(Young)], staged_gens(Db, <<"d">>, <<"a">>)),
    ?assertEqual({error, upload_expired},
                 barrel_docdb:finish_attachment_writer(Old)),
    {ok, _} = barrel_docdb:finish_attachment_writer(Young),
    ?assertEqual({ok, <<"YYYYYYYY">>}, read_att(Db)),
    %% at open
    {ok, Late0} = open_writer(Db, #{}),
    {ok, _Late} = barrel_docdb:write_attachment_chunk(Late0, <<"LLLLLLLL">>),
    timer:sleep(1200),
    ok = barrel_docdb:close_db(Db),
    {ok, Pid2} = create(Db, Opts),
    ?assertNotEqual(Pid, Pid2),
    ?assertEqual([gen(Young)], staged_gens(Db, <<"d">>, <<"a">>)),
    ?assertEqual({ok, <<"YYYYYYYY">>}, read_att(Db)).

%% With retention off, the upload sweep runs on its own timer.
abandoned_uploads_removed_without_retention(Config) ->
    Db = ?config(db, Config),
    {ok, Pid} = create(Db, #{retention_period => 0,
                             retention_check_interval => 200,
                             att_upload_timeout => 1}),
    {ok, W0} = open_writer(Db, #{}),
    {ok, W} = barrel_docdb:write_attachment_chunk(W0, <<"OOOOOOOO">>),
    ?assertEqual([gen(W)], staged_gens(Db, <<"d">>, <<"a">>)),
    timer:sleep(1600),
    ?assertEqual([], staged_gens(Db, <<"d">>, <<"a">>)),
    ?assertEqual({ok, Pid}, barrel_docdb:open_db(Db)),
    ?assertEqual({error, upload_expired},
                 barrel_docdb:finish_attachment_writer(W)).

%%====================================================================
%% Helpers
%%====================================================================

create(Db) ->
    create(Db, #{}).

create(Db, Opts) ->
    barrel_docdb:create_db(Db, Opts#{att_opts => #{chunk_threshold => 4,
                                                    chunk_size => 4}}).

read_att(Db) ->
    barrel_docdb:get_attachment(Db, <<"d">>, <<"a">>).

open_writer(Db, Opts) ->
    barrel_docdb:open_attachment_writer(Db, <<"d">>, <<"a">>,
                                        <<"application/octet-stream">>, Opts).

stream_put(Db, Data) ->
    {ok, W} = open_writer(Db, #{}),
    {ok, W1} = barrel_docdb:write_attachment_chunk(W, Data),
    barrel_docdb:finish_attachment_writer(W1).

stream_read(Db) ->
    stream_read(Db, <<"a">>).

stream_read(Db, Name) ->
    case barrel_docdb:open_attachment_stream(Db, <<"d">>, Name) of
        {ok, S} -> collect(S, []);
        {error, _} = Error -> Error
    end.

collect(S, Acc) ->
    case barrel_docdb:read_attachment_chunk(S) of
        {ok, Chunk, S1} -> collect(S1, [Chunk | Acc]);
        eof -> {ok, iolist_to_binary(lists:reverse(Acc))};
        {error, _} = Error -> barrel_docdb:close_attachment_stream(S), Error
    end.

gen(#{gen := Gen}) -> Gen.

advance_clock() ->
    Future = barrel_hlc:from_wall_time(erlang:system_time(millisecond)
                                       + 3600000),
    {ok, _} = barrel_hlc:sync_hlc(Future),
    ok.

%% A fresh clock process forgets how far it was moved (as after a
%% crash), without spending the supervisor's restart budget.
restart_clock() ->
    ok = supervisor:terminate_child(barrel_docdb_sup, barrel_hlc_clock),
    {ok, _} = supervisor:restart_child(barrel_docdb_sup, barrel_hlc_clock),
    ok.

kill(Pid) ->
    MRef = monitor(process, Pid),
    exit(Pid, kill),
    receive {'DOWN', MRef, process, Pid, _} -> ok end.

await_restart(Db, Old) ->
    await_restart(Db, Old, 100).

await_restart(_Db, _Old, 0) ->
    ct:fail(no_restart);
await_restart(Db, Old, N) ->
    case barrel_docdb:open_db(Db) of
        {ok, Pid} when Pid =/= Old -> Pid;
        _ -> timer:sleep(20), await_restart(Db, Old, N - 1)
    end.

writer_high(Pid, Db) ->
    {dictionary, Dict} = process_info(Pid, dictionary),
    proplists:get_value({barrel_db_server, hlc_high, Db}, Dict).

digest(Data) ->
    <<"sha256-", (binary:encode_hex(crypto:hash(sha256, Data), lowercase))/binary>>.

att_key(Db, DocId, Name) ->
    <<(byte_size(Db)):16, Db/binary, (byte_size(DocId)):16, DocId/binary,
      $:, Name/binary>>.

%% Chunk keys under a document: name, NUL, suffix.
chunk_keys(Ref, Db, DocId) ->
    Prefix = <<(byte_size(Db)):16, Db/binary, (byte_size(DocId)):16,
               DocId/binary, $:>>,
    {ok, Itr} = rocksdb:iterator(Ref, [{iterate_lower_bound, Prefix},
                                       {iterate_upper_bound,
                                        <<Prefix/binary, 16#FF>>}]),
    try keys(rocksdb:iterator_move(Itr, first), Itr, Prefix, [])
    after rocksdb:iterator_close(Itr)
    end.

keys({ok, Key, _}, Itr, Prefix, Acc) ->
    PLen = byte_size(Prefix),
    <<_:PLen/binary, Rest/binary>> = Key,
    Acc1 = case binary:split(Rest, <<0>>) of
        [Name, Suffix] -> [{Name, Suffix} | Acc];
        [_] -> Acc
    end,
    keys(rocksdb:iterator_move(Itr, next), Itr, Prefix, Acc1);
keys({error, _}, _Itr, _Prefix, Acc) ->
    lists:reverse(Acc).

legacy_chunks(Ref, Db, DocId, Name) ->
    [S || {N, S} <- chunk_keys(Ref, Db, DocId), N =:= Name, byte_size(S) =:= 4].

staged_gens(Db, DocId, Name) ->
    {ok, Pid} = barrel_docdb:open_db(Db),
    {ok, #{ref := Ref}} = barrel_db_server:get_att_ref(Pid),
    lists:usort([Gen || {N, <<Gen:64, _:32>>} <- chunk_keys(Ref, Db, DocId),
                        N =:= Name]).

%% The generation of the published value of d/a (none when inline).
published_gen(Db) ->
    {ok, Pid} = barrel_docdb:open_db(Db),
    {ok, #{ref := Ref}} = barrel_db_server:get_att_ref(Pid),
    case rocksdb:get(Ref, att_key(Db, <<"d">>, <<"a">>), []) of
        {ok, <<"BARREL_CHUNK_V1:", Json/binary>>} ->
            case json:decode(Json) of
                #{<<"gen">> := Gen} -> [Gen];
                _ -> []
            end;
        _ ->
            []
    end.
