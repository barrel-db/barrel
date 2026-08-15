%%%-------------------------------------------------------------------
%%% @doc Incremental-index tests for barrel_ngram (M2).
%%%
%%% Exercises the live lifecycle: updates and deletes reflected in
%%% results, query fan across multiple segments, watermark recovery, and
%%% the incremental oracle (a live put/update/delete workload must yield
%%% byte-identical results to a brute-force scan over the final database
%%% state). `refresh/1' is the deterministic catch-up point.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_incremental_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([live_subscription/1, live_update/1, live_delete/1,
         multi_segment_fan/1, recovery/1, incremental_oracle/1,
         superseded_collapse/1, delete_eviction/1, auto_compaction/1,
         compaction_crash_safety/1, post_compaction_oracle/1,
         compaction_worker_prompt_close/1, compaction_worker_killed_via_link/1,
         refresh_error_propagation/1,
         db_recreated_same_name_resubscribe/1]).

all() ->
    [live_subscription, live_update, live_delete, multi_segment_fan,
     recovery, incremental_oracle,
     superseded_collapse, delete_eviction, auto_compaction,
     compaction_crash_safety, post_compaction_oracle,
     compaction_worker_prompt_close, compaction_worker_killed_via_link,
     refresh_error_propagation,
     db_recreated_same_name_resubscribe].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    {ok, _} = application:ensure_all_started(barrel_ngram),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    Db = iolist_to_binary([<<"ngram_inc_">>, atom_to_binary(TC, utf8)]),
    Corpus = Db,
    Dir = filename:join(?config(priv_dir, Config), atom_to_list(TC)),
    _ = barrel_docdb:delete_db(Db),
    {ok, _} = barrel_docdb:create_db(Db),
    %% auto-compaction is exercised only by the cases that need a
    %% background merge worker in flight; elsewhere disable it so the
    %% synchronous compact/1 tests stay deterministic.
    Threshold = case TC of
        auto_compaction -> 3;
        compaction_worker_prompt_close -> 3;
        compaction_worker_killed_via_link -> 3;
        _ -> infinity
    end,
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => Dir,
                                     compact_threshold => Threshold}),
    [{db, Db}, {corpus, Corpus}, {dir, Dir} | Config].

end_per_testcase(_TC, Config) ->
    _ = barrel_ngram:close(?config(corpus, Config)),
    _ = barrel_docdb:delete_db(?config(db, Config)),
    ok.

%%====================================================================
%% Test cases
%%====================================================================

live_subscription(Config) ->
    %% No refresh: changes must become searchable purely from the
    %% background feed subscription (delivery + apply + ack keep flowing).
    Db = ?config(db, Config), C = ?config(corpus, Config),
    Ids = [iolist_to_binary([<<"s">>, integer_to_binary(N)])
           || N <- lists:seq(1, 25)],
    lists:foreach(fun(Id) -> _ = put_doc(Db, Id, <<"subscribed payload">>) end, Ids),
    Expected = lists:sort(Ids),
    ok = wait_until(fun() -> search(C, <<"subscribed">>) =:= Expected end, 100),
    ?assertEqual(Expected, search(C, <<"subscribed">>)).

live_update(Config) ->
    Db = ?config(db, Config), C = ?config(corpus, Config),
    Rev1 = put_doc(Db, <<"d1">>, <<"alpha connect_timeout beta">>),
    refresh(C),
    ?assertEqual([<<"d1">>], search(C, <<"connect_timeout">>)),
    %% update: drop the old identifier, add new text
    _Rev2 = put_doc(Db, <<"d1">>, <<"alpha gamma delta">>, Rev1),
    refresh(C),
    %% old grams still sit in the first segment; confirm drops the stale hit
    ?assertEqual([], search(C, <<"connect_timeout">>)),
    ?assertEqual([<<"d1">>], search(C, <<"gamma delta">>)).

live_delete(Config) ->
    Db = ?config(db, Config), C = ?config(corpus, Config),
    Rev1 = put_doc(Db, <<"d1">>, <<"error connect_timeout here">>),
    _ = put_doc(Db, <<"d2">>, <<"unrelated content">>),
    refresh(C),
    ?assertEqual([<<"d1">>], search(C, <<"connect_timeout">>)),
    ok = del_doc(Db, <<"d1">>, Rev1),
    refresh(C),
    %% gone from results though its grams remain in the frozen segment
    ?assertEqual([], search(C, <<"connect_timeout">>)),
    ?assertEqual([<<"d2">>], search(C, <<"content">>)).

multi_segment_fan(Config) ->
    Db = ?config(db, Config), C = ?config(corpus, Config),
    _ = put_doc(Db, <<"a">>, <<"apple pie recipe">>), refresh(C),
    _ = put_doc(Db, <<"b">>, <<"banana apple split">>), refresh(C),
    _ = put_doc(Db, <<"c">>, <<"cherry apple tart">>), refresh(C),
    {ok, Segs} = barrel_ngram_shard:get_manifest(C),
    ?assert(length(Segs) >= 3),
    ?assertEqual([<<"a">>, <<"b">>, <<"c">>], search(C, <<"apple">>)),
    ?assertEqual([<<"b">>], search(C, <<"banana">>)),
    ?assertEqual([], search(C, <<"durian">>)).

recovery(Config) ->
    Db = ?config(db, Config), C = ?config(corpus, Config),
    Dir = ?config(dir, Config),
    _ = put_doc(Db, <<"a">>, <<"recover alpha payload">>),
    refresh(C),
    ?assertEqual([<<"a">>], search(C, <<"recover">>)),
    %% take the shard down, mutate while it is gone
    ok = barrel_ngram:close(C),
    _ = put_doc(Db, <<"b">>, <<"recover beta payload">>),
    %% bring it back: manifest reloads, resubscribe replays the tail
    ok = barrel_ngram:open(C, #{db => Db, data_dir => Dir}),
    refresh(C),
    ?assertEqual([<<"a">>, <<"b">>], search(C, <<"recover">>)),
    ?assertEqual([<<"b">>], search(C, <<"beta">>)).

incremental_oracle(Config) ->
    Db = ?config(db, Config), C = ?config(corpus, Config),
    _ = rand:seed(exsss, {7, 11, 13}),
    Tracker = run_workload(Db, C, 80, #{}, 0),
    refresh(C),
    Literals = [<<"connect">>, <<"timeout">>, <<"retry">>, <<"backoff">>,
                <<"error">>, <<"pool">>, <<"config">>, <<"jitter">>,
                <<"upstream">>, <<"widget">>, <<"retry backoff">>,
                <<"connect timeout">>, <<"nonexistent">>, <<"co">>, <<"e ">>,
                <<"z">>, <<" ">>],
    lists:foreach(
        fun(Lit) ->
            Expected = brute_force(Tracker, Lit),
            Actual = search(C, Lit),
            ?assertEqual({Lit, Expected}, {Lit, Actual})
        end, Literals).

superseded_collapse(Config) ->
    Db = ?config(db, Config), C = ?config(corpus, Config),
    R1 = put_doc(Db, <<"a">>, <<"apple version one">>), refresh(C),
    _ = put_doc(Db, <<"b">>, <<"banana stays">>), refresh(C),
    _ = put_doc(Db, <<"a">>, <<"apricot version two">>, R1), refresh(C),
    %% a now lives in two segments (v1 and v2); three segments total
    {ok, Before} = barrel_ngram_shard:get_manifest(C),
    ?assert(length(Before) >= 3),
    {ok, #{segments := Segs, doc_count := DocCount}} = barrel_ngram:compact(C),
    ?assertEqual(1, Segs),
    ?assertEqual(2, DocCount),   %% a (collapsed) + b
    %% results unchanged: newest content wins, superseded content gone
    ?assertEqual([<<"a">>], search(C, <<"apricot">>)),
    ?assertEqual([], search(C, <<"apple">>)),
    ?assertEqual([<<"b">>], search(C, <<"banana">>)).

delete_eviction(Config) ->
    Db = ?config(db, Config), C = ?config(corpus, Config),
    R1 = put_doc(Db, <<"a">>, <<"delete_me alpha">>), refresh(C),
    _ = put_doc(Db, <<"b">>, <<"keep beta">>), refresh(C),
    ?assertEqual([<<"a">>], search(C, <<"delete_me">>)),
    ok = del_doc(Db, <<"a">>, R1), refresh(C),
    ?assertEqual([], search(C, <<"delete_me">>)),
    {ok, #{segments := Segs, doc_count := DocCount}} = barrel_ngram:compact(C),
    ?assertEqual(1, Segs),
    ?assertEqual(1, DocCount),   %% a physically evicted, only b remains
    ?assertEqual([], search(C, <<"delete_me">>)),
    ?assertEqual([<<"b">>], search(C, <<"keep">>)).

auto_compaction(Config) ->
    %% opened with compact_threshold => 3: the third freeze triggers a
    %% background merge that settles the segment count back down.
    Db = ?config(db, Config), C = ?config(corpus, Config),
    _ = put_doc(Db, <<"a">>, <<"common apple">>), refresh(C),
    _ = put_doc(Db, <<"b">>, <<"common banana">>), refresh(C),
    _ = put_doc(Db, <<"c">>, <<"common cherry">>), refresh(C),
    ok = wait_until(fun() ->
                        {ok, Segs} = barrel_ngram_shard:get_manifest(C),
                        length(Segs) =< 1
                    end, 100),
    ?assertEqual([<<"a">>, <<"b">>, <<"c">>], search(C, <<"common">>)),
    ?assertEqual([<<"b">>], search(C, <<"banana">>)).

compaction_crash_safety(Config) ->
    Db = ?config(db, Config), C = ?config(corpus, Config),
    Dir = ?config(dir, Config),
    _ = put_doc(Db, <<"a">>, <<"survives compaction">>), refresh(C),
    ?assertEqual([<<"a">>], search(C, <<"survives">>)),
    %% simulate a crash mid-merge: a merged temp segment written but the
    %% manifest never swapped. It must be cleaned up on reopen.
    CorpusDir = filename:join(Dir, binary_to_list(C)),
    Stray = filename:join(CorpusDir, "segment-merge-999999.ngseg"),
    ok = file:write_file(Stray, <<"garbage">>),
    ok = barrel_ngram:close(C),
    ok = barrel_ngram:open(C, #{db => Db, data_dir => Dir,
                                compact_threshold => infinity}),
    ?assertNot(filelib:is_file(Stray)),
    ?assertEqual([<<"a">>], search(C, <<"survives">>)).

%% Finding 8: close/1 must not block waiting for an in-flight background
%% compaction to finish. barrel_ngram_merge:merge/2 is mocked to block
%% until signaled, standing in for a slow merge; close/1 must still
%% return promptly, and no orphaned segment-merge-*.ngseg temp file may
%% exist immediately afterward (the explicit kill-then-sweep in
%% terminate/2, not the next reopen's cleanup_orphans/2 pass).
compaction_worker_prompt_close(Config) ->
    Db = ?config(db, Config), C = ?config(corpus, Config), Dir = ?config(dir, Config),
    Self = self(),
    meck:new(barrel_ngram_merge, [passthrough]),
    meck:expect(barrel_ngram_merge, merge,
        fun(Paths, Drop) ->
            Self ! {merge_started, self()},
            receive proceed -> ok after 5000 -> ok end,
            meck:passthrough([Paths, Drop])
        end),
    _ = put_doc(Db, <<"a">>, <<"one common">>), refresh(C),
    _ = put_doc(Db, <<"b">>, <<"two common">>), refresh(C),
    _ = put_doc(Db, <<"c">>, <<"three common">>), refresh(C),
    WorkerPid = receive
        {merge_started, WPid} -> WPid
    after 2000 -> ct:fail(merge_not_started)
    end,
    T0 = erlang:monotonic_time(millisecond),
    ok = barrel_ngram:close(C),
    T1 = erlang:monotonic_time(millisecond),
    ?assert((T1 - T0) < 2000),
    %% the worker must be genuinely gone by the time close/1 returns, not
    %% merely "close returned quickly while the worker keeps running
    %% unsupervised in the background" -- terminate/2's explicit kill,
    %% waited on synchronously, is what makes this true
    ?assertNot(is_process_alive(WorkerPid)),
    CorpusDir = filename:join(Dir, binary_to_list(C)),
    {ok, Files} = file:list_dir(CorpusDir),
    ?assertNot(lists:any(fun(F) -> lists:prefix("segment-merge-", F) end, Files)),
    meck:unload(barrel_ngram_merge),
    %% fresh reopen afterward still works cleanly
    ok = barrel_ngram:open(C, #{db => Db, data_dir => Dir, compact_threshold => infinity}),
    ?assertEqual([<<"a">>, <<"b">>, <<"c">>], search(C, <<"common">>)).

%% Finding 8: an external exit(ShardPid, kill) bypasses terminate/2
%% entirely (kill is untrappable even with trap_exit) -- the worker must
%% still go down promptly, via the LINK alone, demonstrating that
%% property specifically rather than the terminate/2-side explicit kill.
compaction_worker_killed_via_link(Config) ->
    Db = ?config(db, Config), C = ?config(corpus, Config),
    Self = self(),
    meck:new(barrel_ngram_merge, [passthrough]),
    meck:expect(barrel_ngram_merge, merge,
        fun(Paths, Drop) ->
            Self ! {merge_started, self()},
            receive proceed -> ok after 5000 -> ok end,
            meck:passthrough([Paths, Drop])
        end),
    _ = put_doc(Db, <<"a">>, <<"one common">>), refresh(C),
    _ = put_doc(Db, <<"b">>, <<"two common">>), refresh(C),
    _ = put_doc(Db, <<"c">>, <<"three common">>), refresh(C),
    WorkerPid = receive
        {merge_started, WPid} -> WPid
    after 2000 -> ct:fail(merge_not_started)
    end,
    ShardPid = barrel_ngram_registry:whereis_name({shard, C}),
    true = is_pid(ShardPid),
    true = is_process_alive(WorkerPid),
    exit(ShardPid, kill),
    ok = wait_until(fun() -> not is_process_alive(WorkerPid) end, 100),
    ?assertNot(is_process_alive(WorkerPid)),
    meck:unload(barrel_ngram_merge).

%% Finding 6: refresh/1 must propagate a get_changes failure as
%% {error, {refresh_incomplete, Reason}} instead of the old behavior of
%% silently discarding it and reporting {ok, _} on partial progress.
refresh_error_propagation(Config) ->
    Db = ?config(db, Config), C = ?config(corpus, Config),
    _ = put_doc(Db, <<"a">>, <<"alpha">>),
    meck:new(barrel_docdb, [passthrough]),
    meck:expect(barrel_docdb, get_changes,
                fun(_Db, _Since, _Opts) -> {error, injected} end),
    Result = barrel_ngram:refresh(C),
    meck:unload(barrel_docdb),
    ?assertEqual({error, {refresh_incomplete, injected}}, Result),
    %% no partial state lost: an ordinary (unmocked) refresh right after
    %% still fully drains and finds the document
    ?assertMatch({ok, _}, barrel_ngram:refresh(C)),
    ?assertEqual([<<"a">>], search(C, <<"alpha">>)).

%% Finding 2's "recreated under the same name" gap, closed for the
%% ONGOING subscription case, not just at open/2 time: the shard pins
%% db_instance_id once at init/1 and re-checks it on every subscribe AND
%% resubscribe. Recreate the database under the same name without ever
%% closing the corpus, then force the shard's stream to drop -- the
%% resubscribe must detect the mismatch and stop the shard with
%% {shutdown, {db_instance_mismatch, _, _}} (not restart-looping: a
%% {shutdown, _} reason does not trigger a `transient' child's
%% auto-restart) rather than silently reattaching to the new instance.
db_recreated_same_name_resubscribe(Config) ->
    Db = ?config(db, Config), C = ?config(corpus, Config),
    _ = put_doc(Db, <<"a">>, <<"original content">>),
    refresh(C),
    ?assertEqual([<<"a">>], search(C, <<"original">>)),
    ShardPid = barrel_ngram_registry:whereis_name({shard, C}),
    true = is_pid(ShardPid),
    SupPid = whereis(barrel_ngram_shard_sup),
    StreamPid = stream_pid_of(ShardPid, SupPid),
    true = is_pid(StreamPid),
    ok = barrel_docdb:delete_db(Db),
    {ok, _} = barrel_docdb:create_db(Db),
    %% force the drop -> resubscribe path (both current call sites funnel
    %% through subscribe/1 -> check_db_instance/4)
    exit(StreamPid, kill),
    ok = wait_until(fun() -> not is_process_alive(ShardPid) end, 100),
    ?assertNot(is_process_alive(ShardPid)),
    %% no restart loop: the ref stays unregistered, not crash-restarted
    ?assertEqual(undefined, barrel_ngram_registry:whereis_name({shard, C})),
    %% fail-closed, not silently reattached: a query surfaces the shard's
    %% absence cleanly rather than a noproc crash or a stale/wrong hit
    ?assertEqual({error, corpus_not_open}, barrel_ngram:search(C, <<"original">>)).

%% The shard's only two links right after a normal subscribe are its own
%% supervisor and the changes-feed stream process -- identify the stream
%% by elimination rather than reaching into #state{} internals.
stream_pid_of(ShardPid, SupPid) ->
    {links, Links} = process_info(ShardPid, links),
    case [P || P <- Links, is_pid(P), P =/= SupPid] of
        [StreamPid] -> StreamPid;
        Other -> ct:fail({unexpected_shard_links, Other})
    end.

post_compaction_oracle(Config) ->
    Db = ?config(db, Config), C = ?config(corpus, Config),
    _ = rand:seed(exsss, {21, 4, 96}),
    Tracker = run_workload(Db, C, 60, #{}, 0),
    refresh(C),
    {ok, #{segments := Segs}} = barrel_ngram:compact(C),
    ?assertEqual(1, Segs),
    Literals = [<<"connect">>, <<"timeout">>, <<"retry">>, <<"backoff">>,
                <<"error">>, <<"pool">>, <<"config">>, <<"jitter">>,
                <<"upstream">>, <<"widget">>, <<"retry backoff">>,
                <<"nonexistent">>, <<"co">>, <<"z">>],
    lists:foreach(
        fun(Lit) ->
            Expected = brute_force(Tracker, Lit),
            Actual = search(C, Lit),
            ?assertEqual({Lit, Expected}, {Lit, Actual})
        end, Literals).

%%====================================================================
%% Workload (deterministic)
%%====================================================================

vocab() ->
    [<<"connect">>, <<"timeout">>, <<"retry">>, <<"backoff">>, <<"error">>,
     <<"pool">>, <<"config">>, <<"jitter">>, <<"upstream">>, <<"widget">>].

%% Tracker: #{Id => {Text, Rev}} for live docs. Counter mints fresh ids so
%% deletes never resurrect (which would need the tombstone rev).
run_workload(_Db, _C, 0, Tracker, _Counter) ->
    Tracker;
run_workload(Db, C, N, Tracker, Counter) ->
    Ids = maps:keys(Tracker),
    {Tracker1, Counter1} =
        case rand:uniform(6) of
            R when R =< 3 ->
                %% new document
                Id = iolist_to_binary([<<"doc">>, integer_to_binary(Counter)]),
                Text = random_text(),
                Rev = put_doc(Db, Id, Text),
                {Tracker#{Id => {Text, Rev}}, Counter + 1};
            4 when Ids =/= [] ->
                %% update an existing document
                Id = lists:nth(rand:uniform(length(Ids)), Ids),
                {_Old, OldRev} = maps:get(Id, Tracker),
                Text = random_text(),
                Rev = put_doc(Db, Id, Text, OldRev),
                {Tracker#{Id => {Text, Rev}}, Counter};
            5 when Ids =/= [] ->
                %% delete an existing document
                Id = lists:nth(rand:uniform(length(Ids)), Ids),
                {_Old, OldRev} = maps:get(Id, Tracker),
                ok = del_doc(Db, Id, OldRev),
                {maps:remove(Id, Tracker), Counter};
            _ ->
                %% freeze point (forces multiple segments)
                refresh(C),
                {Tracker, Counter}
        end,
    run_workload(Db, C, N - 1, Tracker1, Counter1).

random_text() ->
    V = vocab(),
    K = 2 + rand:uniform(4),
    Words = [lists:nth(rand:uniform(length(V)), V) || _ <- lists:seq(1, K)],
    iolist_to_binary(lists:join(<<" ">>, Words)).

%%====================================================================
%% Helpers
%%====================================================================

put_doc(Db, Id, Text) ->
    put_doc(Db, Id, Text, undefined).

put_doc(Db, Id, Text, Rev) ->
    Doc0 = #{<<"id">> => Id, <<"body">> => Text},
    Doc = case Rev of
        undefined -> Doc0;
        _ -> Doc0#{<<"_rev">> => Rev}
    end,
    {ok, R} = barrel_docdb:put_doc(Db, Doc),
    maps:get(<<"rev">>, R).

del_doc(Db, Id, Rev) ->
    {ok, _} = barrel_docdb:delete_doc(Db, Id, #{rev => Rev}),
    ok.

refresh(C) ->
    {ok, _} = barrel_ngram:refresh(C),
    ok.

%% Poll a predicate up to Attempts times, 50 ms apart.
wait_until(_Pred, 0) ->
    {error, timeout};
wait_until(Pred, Attempts) ->
    case Pred() of
        true -> ok;
        false -> timer:sleep(50), wait_until(Pred, Attempts - 1)
    end.

search(C, Lit) ->
    {ok, Hits} = barrel_ngram:search(C, Lit),
    lists:sort([maps:get(id, H) || H <- Hits]).

brute_force(Tracker, Lit) ->
    lists:sort(
      [Id || {Id, {Text, _Rev}} <- maps:to_list(Tracker),
             begin
                 DocText = barrel_ngram_corpus:doc_text(#{<<"body">> => Text},
                                                        #{fields => all}),
                 binary:match(DocText, Lit) =/= nomatch
             end]).
