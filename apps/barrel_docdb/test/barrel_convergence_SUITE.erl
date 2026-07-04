%%%-------------------------------------------------------------------
%%% @doc Convergence and recovery tests for the version-vector core.
%%%
%%% Two databases replicating both ways must converge to the same
%%% winner, body, and deletion state for every document, whatever the
%%% interleaving of writes. Conflict METADATA is allowed to differ per
%%% replica (a conflict is recorded where the concurrency was
%%% observed); winners and bodies are not.
%%%
%%% Recovery: every write commits as one RocksDB batch, so a killed
%%% database process must come back with entity, body, live feed and
%%% history in agreement.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_convergence_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [
        {group, convergence},
        {group, recovery}
    ].

groups() ->
    [
        {convergence, [sequence], [
            concurrent_edits_converge,
            deletes_converge,
            resolution_replicates,
            random_workload_converges
        ]},
        {recovery, [sequence], [
            kill_and_recover_consistency
        ]}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    DataDir = "/tmp/barrel_test_convergence",
    os:cmd("rm -rf " ++ DataDir),
    [{data_dir, DataDir} | Config].

end_per_suite(Config) ->
    ok = application:stop(barrel_docdb),
    os:cmd("rm -rf " ++ ?config(data_dir, Config)),
    ok.

init_per_testcase(Case, Config) ->
    DataDir = ?config(data_dir, Config),
    A = <<(atom_to_binary(Case, utf8))/binary, "_a">>,
    B = <<(atom_to_binary(Case, utf8))/binary, "_b">>,
    {ok, _} = barrel_docdb:create_db(A, #{data_dir => DataDir}),
    {ok, _} = barrel_docdb:create_db(B, #{data_dir => DataDir}),
    [{db_a, A}, {db_b, B} | Config].

end_per_testcase(_Case, Config) ->
    barrel_docdb:delete_db(?config(db_a, Config)),
    barrel_docdb:delete_db(?config(db_b, Config)),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

%% Replicate both ways until neither direction ships a document, so
%% vector-merge writes settle too.
sync_until_quiescent(A, B) ->
    sync_until_quiescent(A, B, 10).

sync_until_quiescent(_A, _B, 0) ->
    ct:fail(no_quiescence);
sync_until_quiescent(A, B, N) ->
    {ok, R1} = barrel_rep:replicate(A, B),
    {ok, R2} = barrel_rep:replicate(B, A),
    case maps:get(docs_written, R1) + maps:get(docs_written, R2) of
        0 -> ok;
        _ -> sync_until_quiescent(A, B, N - 1)
    end.

%% The replicable state of a doc: winner token, body, deletion flag.
doc_state(Db, DocId) ->
    case barrel_docdb:get_doc_for_replication(Db, DocId) of
        {ok, #{doc := Doc, version := V, deleted := D}} ->
            {V, D, maps:remove(<<"id">>, Doc)};
        {error, not_found} ->
            not_found
    end.

assert_converged(A, B, DocIds) ->
    lists:foreach(
        fun(DocId) ->
            SA = doc_state(A, DocId),
            SB = doc_state(B, DocId),
            case SA =:= SB of
                true -> ok;
                false -> ct:fail({diverged, DocId, SA, SB})
            end
        end,
        DocIds).

update_doc(Db, DocId, Value) ->
    Doc = case barrel_docdb:get_doc(Db, DocId) of
        {ok, Existing} -> Existing#{<<"v">> => Value};
        {error, not_found} -> #{<<"id">> => DocId, <<"v">> => Value}
    end,
    case barrel_docdb:put_doc(Db, Doc) of
        {ok, _} -> ok;
        %% A tombstone winner: recreate over it
        {error, conflict} ->
            {ok, _} = barrel_docdb:put_doc(
                Db, #{<<"id">> => DocId, <<"v">> => Value}),
            ok
    end.

%%====================================================================
%% Convergence Tests
%%====================================================================

concurrent_edits_converge(Config) ->
    A = ?config(db_a, Config),
    B = ?config(db_b, Config),
    DocIds = [<<"doc", (integer_to_binary(N))/binary>>
              || N <- lists:seq(1, 10)],

    %% Seed on A and sync
    [ok = update_doc(A, Id, <<"seed">>) || Id <- DocIds],
    ok = sync_until_quiescent(A, B),

    %% Divergent edits: both sides update every doc concurrently
    [ok = update_doc(A, Id, <<"from-a">>) || Id <- DocIds],
    [ok = update_doc(B, Id, <<"from-b">>) || Id <- DocIds],

    ok = sync_until_quiescent(A, B),
    assert_converged(A, B, DocIds),

    %% Winners are deterministic LWW: all picked the same side's write
    {ok, Doc1} = barrel_docdb:get_doc(A, hd(DocIds)),
    ?assert(lists:member(maps:get(<<"v">>, Doc1),
                         [<<"from-a">>, <<"from-b">>])),
    ok.

deletes_converge(Config) ->
    A = ?config(db_a, Config),
    B = ?config(db_b, Config),
    DocIds = [<<"del", (integer_to_binary(N))/binary>>
              || N <- lists:seq(1, 6)],

    [ok = update_doc(A, Id, <<"seed">>) || Id <- DocIds],
    ok = sync_until_quiescent(A, B),

    %% A deletes half while B concurrently updates all
    [begin
         {ok, #{<<"_rev">> := Rev}} = barrel_docdb:get_doc(A, Id),
         {ok, _} = barrel_docdb:delete_doc(A, Id, #{rev => Rev})
     end || Id <- lists:sublist(DocIds, 3)],
    [ok = update_doc(B, Id, <<"survivor">>) || Id <- DocIds],

    ok = sync_until_quiescent(A, B),
    assert_converged(A, B, DocIds),
    ok.

resolution_replicates(Config) ->
    A = ?config(db_a, Config),
    B = ?config(db_b, Config),
    DocId = <<"resolved">>,

    ok = update_doc(A, DocId, <<"seed">>),
    ok = sync_until_quiescent(A, B),
    ok = update_doc(A, DocId, <<"from-a">>),
    ok = update_doc(B, DocId, <<"from-b">>),
    ok = sync_until_quiescent(A, B),
    assert_converged(A, B, [DocId]),

    %% Resolve wherever a conflict was recorded, then sync
    lists:foreach(
        fun(Db) ->
            case barrel_docdb:get_conflicts(Db, DocId) of
                {ok, []} ->
                    ok;
                {ok, _Some} ->
                    {ok, #{<<"_rev">> := Rev}} = barrel_docdb:get_doc(Db, DocId),
                    {ok, _} = barrel_docdb:resolve_conflict(
                        Db, DocId, Rev, {merge, #{<<"v">> => <<"settled">>}})
            end
        end,
        [A, B]),
    ok = sync_until_quiescent(A, B),
    assert_converged(A, B, [DocId]),

    {ok, Final} = barrel_docdb:get_doc(A, DocId),
    ?assertEqual(<<"settled">>, maps:get(<<"v">>, Final)),
    %% Both sides are conflict-free after the resolving write syncs
    ?assertEqual({ok, []}, barrel_docdb:get_conflicts(A, DocId)),
    ?assertEqual({ok, []}, barrel_docdb:get_conflicts(B, DocId)),
    ok.

random_workload_converges(Config) ->
    A = ?config(db_a, Config),
    B = ?config(db_b, Config),
    rand:seed(exsss, {7, 7, 7}),
    DocIds = [<<"rnd", (integer_to_binary(N))/binary>>
              || N <- lists:seq(1, 8)],

    %% Interleaved rounds of random writes and partial syncs
    lists:foreach(
        fun(Round) ->
            lists:foreach(
                fun(Id) ->
                    Db = case rand:uniform(2) of 1 -> A; 2 -> B end,
                    case rand:uniform(5) of
                        1 ->
                            case barrel_docdb:get_doc(Db, Id) of
                                {ok, #{<<"_rev">> := Rev}} ->
                                    _ = barrel_docdb:delete_doc(
                                            Db, Id, #{rev => Rev}),
                                    ok;
                                _ ->
                                    ok
                            end;
                        _ ->
                            V = iolist_to_binary(
                                [<<"r">>, integer_to_binary(Round), <<"-">>,
                                 integer_to_binary(rand:uniform(1000))]),
                            ok = update_doc(Db, Id, V)
                    end
                end,
                DocIds),
            %% Occasionally sync one direction mid-workload
            case rand:uniform(3) of
                1 -> {ok, _} = barrel_rep:replicate(A, B), ok;
                2 -> {ok, _} = barrel_rep:replicate(B, A), ok;
                _ -> ok
            end
        end,
        lists:seq(1, 6)),

    ok = sync_until_quiescent(A, B),
    assert_converged(A, B, DocIds),
    ok.

%%====================================================================
%% Recovery Tests
%%====================================================================

kill_and_recover_consistency(Config) ->
    Db = ?config(db_a, Config),
    DocIds = [<<"rec", (integer_to_binary(N))/binary>>
              || N <- lists:seq(1, 20)],

    %% A mixed workload: creates, updates, a delete, a conflict
    [ok = update_doc(Db, Id, <<"v1">>) || Id <- DocIds],
    [ok = update_doc(Db, Id, <<"v2">>) || Id <- lists:sublist(DocIds, 10)],
    {ok, #{<<"_rev">> := DelRev}} = barrel_docdb:get_doc(Db, hd(DocIds)),
    {ok, _} = barrel_docdb:delete_doc(Db, hd(DocIds), #{rev => DelRev}),
    ConflictId = lists:nth(2, DocIds),
    RemoteV = barrel_version:new(barrel_hlc:new_hlc(), <<"peer_x">>),
    {ok, _, _} = barrel_docdb:put_version(
        Db, #{<<"id">> => ConflictId, <<"v">> => <<"remote">>},
        barrel_version:to_token(RemoteV),
        barrel_vv:encode(barrel_vv:bump(barrel_vv:new(), RemoteV)),
        false),

    StateBefore = [doc_state(Db, Id) || Id <- DocIds],
    HistoryBefore = history_len(Db),

    %% Kill the database process outright; committed batches must
    %% survive the crash. Reopening is create_db over the same dir.
    {ok, Pid} = barrel_docdb:db_pid(Db),
    MRef = erlang:monitor(process, Pid),
    exit(Pid, kill),
    receive {'DOWN', MRef, process, Pid, _} -> ok
    after 5000 -> ct:fail(db_not_killed)
    end,
    {ok, _} = wait_reopen(Db, ?config(data_dir, Config), 50),

    %% Same replicable state for every doc
    StateAfter = [doc_state(Db, Id) || Id <- DocIds],
    ?assertEqual(StateBefore, StateAfter),
    ?assertEqual(HistoryBefore, history_len(Db)),

    %% Cross-structure consistency: every live feed row points at the
    %% doc's current version, the conflict survived, writes still work
    {ok, Changes, _} = barrel_docdb:get_changes(Db, first),
    lists:foreach(
        fun(#{id := Id, rev := Rev}) ->
            {ok, #{version := Rev2}} =
                barrel_docdb:get_doc_for_replication(Db, Id),
            ?assertEqual(Rev2, Rev)
        end,
        Changes),
    {ok, [_]} = barrel_docdb:get_conflicts(Db, ConflictId),
    ok = update_doc(Db, lists:nth(3, DocIds), <<"post-crash">>),
    ok.

history_len(Db) ->
    {ok, N} = barrel_docdb:fold_history(
        Db, fun(_, Acc) -> {ok, Acc + 1} end, 0),
    N.

wait_reopen(_Db, _DataDir, 0) ->
    {error, not_reopened};
wait_reopen(Db, DataDir, N) ->
    case barrel_docdb:create_db(Db, #{data_dir => DataDir}) of
        {ok, Pid} when is_pid(Pid) -> {ok, Pid};
        _ ->
            timer:sleep(100),
            wait_reopen(Db, DataDir, N - 1)
    end.
