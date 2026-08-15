%%%-------------------------------------------------------------------
%%% @doc Corpus lifecycle (open/close) coordinator tests.
%%%
%%% Covers the finding 1+2 fix: corpus-level `corpus.meta', the
%%% serialized open/close coordinator, atomic start-and-classify shard
%%% startup, rollback on a failed open, crash-durability across an
%%% interrupted open, and the `rest_for_one' supervision cascade. Other
%%% suites already cover the per-shard-manifest config check
%%% (`barrel_ngram_config_SUITE') and the `db_instance_id' resubscribe
%%% check (`barrel_ngram_incremental_SUITE'); this suite is specifically
%%% about the corpus-level coordinator introduced by that fix.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_lifecycle_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([reopen_different_shards_rejected_while_live/1,
         same_shards_different_data_dir_rejected_while_live/1,
         two_corpora_same_name_different_data_dir/1,
         app_stop_start_clears_active_and_pending_meta/1,
         legacy_corpus_requires_reindex/1,
         orphan_tmp_cleanup_allows_fresh_open/1,
         corrupt_corpus_meta_rejected/1,
         activation_failure_not_query_visible/1,
         interrupted_activation_resumes_and_activates/1,
         interrupted_activation_resume_different_opts_rejected/1,
         close_during_initializing_multi_shard/1,
         rollback_on_shard_start_failure_multi_shard/1,
         corpus_state_inconsistent_detected/1,
         corpus_state_inconsistent_real_shard_after_wipe/1,
         lifecycle_unavailable_when_sup_gone/1,
         registry_crash_cascades_and_fails_closed/1,
         registry_crash_makes_every_corpus_report_not_open/1,
         concurrent_open_same_corpus_serialized/1,
         validate_open_opts_path_traversal_and_shape/1,
         search_on_never_opened_corpus_fails_closed/1,
         atom_and_binary_corpus_names_are_one_identity/1]).

all() ->
    [reopen_different_shards_rejected_while_live,
     same_shards_different_data_dir_rejected_while_live,
     two_corpora_same_name_different_data_dir,
     app_stop_start_clears_active_and_pending_meta,
     legacy_corpus_requires_reindex,
     orphan_tmp_cleanup_allows_fresh_open,
     corrupt_corpus_meta_rejected,
     activation_failure_not_query_visible,
     interrupted_activation_resumes_and_activates,
     interrupted_activation_resume_different_opts_rejected,
     close_during_initializing_multi_shard,
     rollback_on_shard_start_failure_multi_shard,
     corpus_state_inconsistent_detected,
     corpus_state_inconsistent_real_shard_after_wipe,
     lifecycle_unavailable_when_sup_gone,
     registry_crash_cascades_and_fails_closed,
     registry_crash_makes_every_corpus_report_not_open,
     concurrent_open_same_corpus_serialized,
     validate_open_opts_path_traversal_and_shape,
     search_on_never_opened_corpus_fails_closed,
     atom_and_binary_corpus_names_are_one_identity].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    {ok, _} = application:ensure_all_started(barrel_ngram),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    Db = iolist_to_binary([<<"ngram_lc_">>, atom_to_binary(TC, utf8)]),
    Corpus = Db,
    DataDir = filename:join(?config(priv_dir, Config), atom_to_list(TC)),
    _ = barrel_docdb:delete_db(Db),
    {ok, _} = barrel_docdb:create_db(Db),
    [{db, Db}, {corpus, Corpus}, {data_dir, DataDir} | Config].

end_per_testcase(_TC, Config) ->
    _ = try meck:unload(barrel_ngram_shard_sup) catch _:_ -> ok end,
    _ = try meck:unload(barrel_ngram_corpus_config) catch _:_ -> ok end,
    Db = ?config(db, Config),
    Corpus = ?config(corpus, Config),
    _ = barrel_ngram:close(Corpus),
    _ = barrel_docdb:delete_db(Db),
    ok.

%%====================================================================
%% Test cases
%%====================================================================

%% Shard-count change while the corpus is live and unclosed: the
%% runtime-meta reconciliation (checked against get_meta/get_pending_meta
%% BEFORE any disk read) rejects it before the disjoint-ref-shape
%% orphaning bug (a shard count change spawns an entirely different set
%% of shard directories) can ever happen. The original single shard is
%% untouched and still discoverable/stoppable afterward.
reopen_different_shards_rejected_while_live(Config) ->
    Db = ?config(db, Config), Corpus = ?config(corpus, Config), DataDir = ?config(data_dir, Config),
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir}),
    Result = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir, shards => 3}),
    ?assertEqual({error, {config_mismatch, shards, 1, 3}}, Result),
    ?assertEqual(true, barrel_ngram:is_open(Corpus)),
    ?assertMatch({ok, #{}}, barrel_ngram:refresh(Corpus)).

%% Same shard count, different data_dir, while live: the review round's
%% specific regression -- an earlier version of this check compared only
%% `shards' and missed this variant.
same_shards_different_data_dir_rejected_while_live(Config) ->
    Db = ?config(db, Config), Corpus = ?config(corpus, Config), DataDir = ?config(data_dir, Config),
    OtherDir = filename:join(DataDir, "other"),
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir}),
    Result = barrel_ngram:open(Corpus, #{db => Db, data_dir => OtherDir}),
    ?assertMatch({error, {config_mismatch, data_dir, _, _}}, Result),
    %% rejected before any write under OtherDir
    ?assertEqual({error, enoent}, file:list_dir(OtherDir)),
    ?assertEqual(true, barrel_ngram:is_open(Corpus)).

%% Two valid, separately-created corpora sharing the same NAME under two
%% different data_dirs: B is closed (its corpus.meta persists, valid);
%% A is opened and left live; reopening under B must be rejected by the
%% runtime-meta check BEFORE step 3's disk-based reconciliation (which
%% would otherwise find B's own persisted config internally consistent
%% and let it straight through).
%% shards is kept equal (1) on both sides so this isolates the data_dir
%% check specifically -- runtime_reconcile_fields/0 checks `shards'
%% before `data_dir', so a scenario differing in both would (correctly)
%% report the shards mismatch first instead.
two_corpora_same_name_different_data_dir(Config) ->
    Db = ?config(db, Config), Corpus = ?config(corpus, Config), DataDir = ?config(data_dir, Config),
    DirA = filename:join(DataDir, "a"),
    DirB = filename:join(DataDir, "b"),
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DirB}),
    ok = barrel_ngram:close(Corpus),
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DirA}),
    Result = barrel_ngram:open(Corpus, #{db => Db, data_dir => DirB}),
    ?assertMatch({error, {config_mismatch, data_dir, _, _}}, Result),
    {ok, Meta} = barrel_ngram_shards:get_meta(Corpus),
    ?assertEqual(1, maps:get(shards, Meta)),
    ?assertEqual(iolist_to_binary(DirA), iolist_to_binary(maps:get(data_dir, Meta))).

%% persistent_term survives application:stop/start (no VM restart) while
%% shard processes do not -- both the active (get_meta) and the
%% discovery-only (get_pending_meta) caches must be cleared, or a stale
%% entry would make is_open/1 lie or leak an interrupted open forever.
app_stop_start_clears_active_and_pending_meta(Config) ->
    Db = ?config(db, Config), Corpus = ?config(corpus, Config), DataDir = ?config(data_dir, Config),
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir}),
    ?assertMatch({ok, _}, barrel_ngram_shards:get_meta(Corpus)),
    %% simulate an open that reconciled but never reached activation --
    %% the discovery-only pending cache is what makes an interrupted
    %% open's shards discoverable by close/1; it uses its own key shape,
    %% so both are exercised here without needing to fault-inject a real
    %% interrupted open.
    PendingCorpus = <<Corpus/binary, "_pending">>,
    ok = barrel_ngram_shards:put_pending_meta(PendingCorpus, #{shards => 1}),
    ?assertMatch({ok, _}, barrel_ngram_shards:get_pending_meta(PendingCorpus)),
    ok = application:stop(barrel_ngram),
    {ok, _} = application:ensure_all_started(barrel_ngram),
    ?assertEqual(undefined, barrel_ngram_shards:get_meta(Corpus)),
    ?assertEqual(undefined, barrel_ngram_shards:get_pending_meta(PendingCorpus)),
    ?assertEqual(false, barrel_ngram:is_open(Corpus)).

%% Real on-disk artifacts (a manifest -- via an ordinary indexed corpus)
%% but no corpus.meta: exactly what a corpus created before this fix
%% landed looks like. No safe auto-migration (db/shards were never
%% recoverable from what a pre-fix corpus persisted).
legacy_corpus_requires_reindex(Config) ->
    Db = ?config(db, Config), Corpus = ?config(corpus, Config), DataDir = ?config(data_dir, Config),
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir}),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"a">>, <<"body">> => <<"connect_timeout">>}),
    {ok, _} = barrel_ngram:refresh(Corpus),
    ok = barrel_ngram:close(Corpus),
    MetaPath = filename:join([DataDir, Corpus, "corpus.meta"]),
    ok = file:delete(MetaPath),
    Result = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir}),
    ?assertEqual({error, {legacy_corpus_requires_reindex, Corpus}}, Result),
    ?assertEqual(false, barrel_ngram:is_open(Corpus)).

%% A stray corpus.meta.tmp survives a crash between the temp write and
%% the rename in save/2. Without cleanup_orphan_tmp/1 running BEFORE
%% has_any_artifact/1, that lone .tmp file gets counted as "an artifact
%% exists", permanently misrouting every future open of a corpus that
%% was NEVER actually opened even once to legacy_corpus_requires_reindex.
orphan_tmp_cleanup_allows_fresh_open(Config) ->
    Db = ?config(db, Config), Corpus = ?config(corpus, Config), DataDir = ?config(data_dir, Config),
    TmpPath = filename:join([DataDir, Corpus, "corpus.meta.tmp"]),
    ok = filelib:ensure_dir(TmpPath),
    ok = file:write_file(TmpPath, <<"garbage-in-progress-write">>),
    Result = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir}),
    ?assertEqual(ok, Result),
    ?assertEqual(true, barrel_ngram:is_open(Corpus)),
    ?assertEqual({error, enoent}, file:read_file(TmpPath)).

%% A truncated/corrupt corpus.meta is a distinct failure from both
%% legacy_corpus_requires_reindex and config_mismatch: a real problem
%% (disk corruption, a torn write worse than an ordinary crash) that
%% needs an operator's attention, not a silent "must be legacy".
corrupt_corpus_meta_rejected(Config) ->
    Db = ?config(db, Config), Corpus = ?config(corpus, Config), DataDir = ?config(data_dir, Config),
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir}),
    ok = barrel_ngram:close(Corpus),
    MetaPath = filename:join([DataDir, Corpus, "corpus.meta"]),
    ok = file:write_file(MetaPath, <<"not a valid term_to_binary blob">>),
    Result = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir}),
    ?assertMatch({error, {corpus_meta_corrupt, _}}, Result),
    ?assertEqual(false, barrel_ngram:is_open(Corpus)).

%% Every shard is genuinely up and correctly configured; only the final
%% `state => active' bookkeeping write fails. Rolling back live, working
%% shards would be actively harmful -- they are left running, but the
%% corpus is correctly NOT query-visible until the write succeeds.
activation_failure_not_query_visible(Config) ->
    Db = ?config(db, Config), Corpus = ?config(corpus, Config), DataDir = ?config(data_dir, Config),
    meck:new(barrel_ngram_corpus_config, [passthrough]),
    meck:expect(barrel_ngram_corpus_config, save,
        fun(_C, #{state := active}) -> {error, injected_activation_failure};
           (C, M) -> meck:passthrough([C, M])
        end),
    Result = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir}),
    ?assertEqual({error, {activation_failed, injected_activation_failure}}, Result),
    ?assertEqual(false, barrel_ngram:is_open(Corpus)),
    ShardPid = barrel_ngram_registry:whereis_name({shard, Corpus}),
    ?assert(is_pid(ShardPid)),
    ?assert(is_process_alive(ShardPid)),
    MetaPath = filename:join([DataDir, Corpus, "corpus.meta"]),
    {ok, Bin} = file:read_file(MetaPath),
    ?assertMatch(#{state := initializing}, binary_to_term(Bin)),
    ?assertEqual({error, corpus_not_open}, barrel_ngram:search(Corpus, <<"whatever">>)),
    meck:unload(barrel_ngram_corpus_config),
    %% a retry with the same options resumes: the shard is found
    %% `existing' and the activation write is retried for real
    Retry = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir}),
    ?assertEqual(ok, Retry),
    ?assertEqual(true, barrel_ngram:is_open(Corpus)),
    ?assertEqual(ShardPid, barrel_ngram_registry:whereis_name({shard, Corpus})).

%% Same fault as above, but the coordinator is killed outright right
%% after the failed activation write (no clean {error, _} return this
%% time) -- proves resumption also works via the crash-durability path
%% (corpus.meta left as `initializing' on disk), not just the ordinary
%% error-return path.
interrupted_activation_resumes_and_activates(Config) ->
    Db = ?config(db, Config), Corpus = ?config(corpus, Config), DataDir = ?config(data_dir, Config),
    meck:new(barrel_ngram_corpus_config, [passthrough]),
    meck:expect(barrel_ngram_corpus_config, save,
        fun(_C, #{state := active}) -> {error, injected};
           (C, M) -> meck:passthrough([C, M])
        end),
    ?assertMatch({error, {activation_failed, injected}},
                 barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir})),
    meck:unload(barrel_ngram_corpus_config),
    ?assertEqual(false, barrel_ngram:is_open(Corpus)),
    ?assertEqual(ok, barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir})),
    ?assertEqual(true, barrel_ngram:is_open(Corpus)).

%% An interrupted (still `initializing') corpus.meta is not treated as
%% still negotiable -- a resume with different options is rejected
%% exactly as it would be against an `active' corpus.
interrupted_activation_resume_different_opts_rejected(Config) ->
    Db = ?config(db, Config), Corpus = ?config(corpus, Config), DataDir = ?config(data_dir, Config),
    meck:new(barrel_ngram_corpus_config, [passthrough]),
    meck:expect(barrel_ngram_corpus_config, save,
        fun(_C, #{state := active}) -> {error, injected};
           (C, M) -> meck:passthrough([C, M])
        end),
    ?assertMatch({error, {activation_failed, injected}},
                 barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir, fields => all})),
    meck:unload(barrel_ngram_corpus_config),
    Result = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir, fields => [<<"title">>]}),
    ?assertMatch({error, {config_mismatch, fields, all, [<<"title">>]}}, Result).

%% close/1 receives only Corpus, never Config/data_dir -- it can ONLY
%% discover refs via a runtime persistent_term cache. For a MULTI-shard
%% corpus interrupted before activation, get_pending_meta (published at
%% reconciliation, before any shard starts) is what makes every shard
%% discoverable, not just shard 0 / a default guess.
close_during_initializing_multi_shard(Config) ->
    Db = ?config(db, Config), Corpus = ?config(corpus, Config), DataDir = ?config(data_dir, Config),
    meck:new(barrel_ngram_corpus_config, [passthrough]),
    meck:expect(barrel_ngram_corpus_config, save,
        fun(_C, #{state := active}) -> {error, injected};
           (C, M) -> meck:passthrough([C, M])
        end),
    ?assertMatch({error, {activation_failed, injected}},
                 barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir, shards => 3})),
    meck:unload(barrel_ngram_corpus_config),
    Refs = [Corpus, {Corpus, 0}, {Corpus, 1}, {Corpus, 2}],
    Pids = [barrel_ngram_registry:whereis_name({shard, R}) || R <- [{Corpus, 0}, {Corpus, 1}, {Corpus, 2}]],
    ?assert(lists:all(fun is_pid/1, Pids)),
    ?assertEqual(ok, barrel_ngram:close(Corpus)),
    lists:foreach(
        fun(R) -> ?assertEqual(undefined, barrel_ngram_registry:whereis_name({shard, R})) end,
        [{Corpus, 0}, {Corpus, 1}, {Corpus, 2}]),
    _ = Refs.

%% A shard fails to start partway through a fresh multi-shard open:
%% every already-started shard is stopped, every attempted directory
%% removed, and the `initializing' corpus.meta this attempt itself wrote
%% is erased -- a clean, fully-rolled-back state, not a leak.
rollback_on_shard_start_failure_multi_shard(Config) ->
    Db = ?config(db, Config), Corpus = ?config(corpus, Config), DataDir = ?config(data_dir, Config),
    meck:new(barrel_ngram_shard_sup, [passthrough]),
    meck:expect(barrel_ngram_shard_sup, start_shard_tracked,
        fun(Ref, SC) ->
            case maps:get(shard_index, SC) of
                1 -> {error, injected_start_failure};
                _ -> meck:passthrough([Ref, SC])
            end
        end),
    Result = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir, shards => 3}),
    ?assertMatch({error, {open_failed, injected_start_failure, ok}}, Result),
    meck:unload(barrel_ngram_shard_sup),
    ?assertEqual(false, barrel_ngram:is_open(Corpus)),
    ?assertEqual(undefined, barrel_ngram_shards:get_meta(Corpus)),
    ?assertEqual(undefined, barrel_ngram_shards:get_pending_meta(Corpus)),
    lists:foreach(
        fun(R) -> ?assertEqual(undefined, barrel_ngram_registry:whereis_name({shard, R})) end,
        [{Corpus, 0}, {Corpus, 1}, {Corpus, 2}]),
    %% every attempted per-shard directory is gone -- the corpus's base
    %% directory itself (never explicitly deleted, only its shard-*
    %% subdirs are tracked/removed) is left behind, empty
    {ok, Entries} = file:list_dir(filename:join(DataDir, Corpus)),
    ?assertEqual([], [E || E <- Entries, lists:prefix("shard-", E)]),
    MetaPath = filename:join([DataDir, Corpus, "corpus.meta"]),
    ?assertEqual({error, enoent}, file:read_file(MetaPath)),
    %% a fresh retry afterward works cleanly, nothing left dangling
    ?assertEqual(ok, barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir, shards => 3})).

%% A ref classified `existing' while disk looked genuinely fresh is an
%% internally inconsistent state (an unrelated live process squatting on
%% one of this corpus's refs) -- never trust disk-based freshness once
%% contradicted by direct evidence of a live process.
corpus_state_inconsistent_detected(Config) ->
    Db = ?config(db, Config), Corpus = ?config(corpus, Config), DataDir = ?config(data_dir, Config),
    %% squat on the ref this fresh, single-shard open will try to use,
    %% answering get_config exactly as the real request would reconcile
    %% (so existing_config_mismatch/2 passes and the flow reaches the
    %% FreshCorpus0-vs-existing-process contradiction check) -- proving
    %% the anomaly is caught even when the foreign process LOOKS
    %% correctly configured, not just when it obviously disagrees
    {ok, DbInstanceId} = barrel_docdb:db_instance_id(Db),
    NormalizedOpts = barrel_ngram_selector_sparse:normalize_opts(#{}),
    LiveConfig = #{db => Db, db_instance_id => DbInstanceId, shards => 1,
                   phase2_selector_opts => NormalizedOpts, fields => all, postings => varint},
    ImposterPid = spawn(fun() -> imposter_loop(LiveConfig) end),
    yes = barrel_ngram_registry:register_name({shard, Corpus}, ImposterPid),
    Result = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir}),
    ?assertMatch({error, {open_failed, {corpus_state_inconsistent, Corpus}, ok}}, Result),
    ?assertEqual(false, barrel_ngram:is_open(Corpus)),
    exit(ImposterPid, kill).

imposter_loop(Reply) ->
    receive
        {'$gen_call', From, get_config} ->
            gen_server:reply(From, {ok, Reply}),
            imposter_loop(Reply);
        _ ->
            imposter_loop(Reply)
    end.

%% The corpus_state_inconsistent variant that matters most in practice:
%% not a foreign process squatting on the ref, but the corpus's OWN
%% real, still-running shard, found `existing' after its on-disk
%% corpus.meta and segments are wiped out from under it (an operator
%% `rm -rf'ing the directory while the corpus stays open, without ever
%% calling close/1 first). Verifies rollback's asymmetry holds here
%% too: the `initializing' corpus.meta THIS reopen attempt wrote is
%% erased (WroteInitializingMeta is keyed to FreshCorpus0, independent
%% of the inconsistency), pending_meta is erased (its job was already
%% done -- the corpus never stopped being genuinely open), but the
%% REAL, PRE-EXISTING get_meta entry from the original open is left
%% completely untouched (only run_close/1's success path ever calls
%% erase_meta/1) -- so close/1 still finds and stops the real shard via
%% get_meta, never silently orphaning it.
corpus_state_inconsistent_real_shard_after_wipe(Config) ->
    Db = ?config(db, Config), Corpus = ?config(corpus, Config), DataDir = ?config(data_dir, Config),
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir}),
    ShardPid = barrel_ngram_registry:whereis_name({shard, Corpus}),
    true = is_pid(ShardPid),
    ok = del_dir_r(binary_to_list(filename:join(DataDir, Corpus))),
    Result = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir}),
    ?assertMatch({error, {open_failed, {corpus_state_inconsistent, Corpus}, ok}}, Result),
    %% the real shard was never touched by the failed reopen -- still
    %% the exact same live process, not restarted, not stopped
    ?assertEqual(ShardPid, barrel_ngram_registry:whereis_name({shard, Corpus})),
    ?assert(is_process_alive(ShardPid)),
    ?assertMatch({ok, _}, barrel_ngram_shards:get_meta(Corpus)),
    ?assertEqual(undefined, barrel_ngram_shards:get_pending_meta(Corpus)),
    %% close/1 still finds it (via get_meta, untouched by the failed
    %% reopen) and stops it cleanly -- not silently orphaned
    ?assertEqual(ok, barrel_ngram:close(Corpus)),
    ?assertNot(is_process_alive(ShardPid)),
    ?assertEqual(undefined, barrel_ngram_registry:whereis_name({shard, Corpus})).

del_dir_r(Dir) ->
    case file:del_dir_r(Dir) of
        ok -> ok;
        {error, enoent} -> ok
    end.

%% Finding: is_open/1 must not keep reporting `true' for every
%% previously-open corpus in the VM after a barrel_ngram_registry
%% crash -- rest_for_one force-restarts barrel_ngram_shard_sup EMPTY
%% (killing every live shard for every corpus, not just one), but
%% get_meta is a persistent_term entry the cascade never touches, so a
%% meta-only is_open/1 would keep lying for corpora that have nothing
%% to do with whatever corpus's operation raced the crash.
registry_crash_makes_every_corpus_report_not_open(Config) ->
    Db = ?config(db, Config), Corpus = ?config(corpus, Config), DataDir = ?config(data_dir, Config),
    CorpusB = <<Corpus/binary, "_b">>,
    DataDirB = filename:join(DataDir, "b"),
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir}),
    ok = barrel_ngram:open(CorpusB, #{db => Db, data_dir => DataDirB}),
    ?assertEqual(true, barrel_ngram:is_open(Corpus)),
    ?assertEqual(true, barrel_ngram:is_open(CorpusB)),
    RegistryPid = whereis(barrel_ngram_registry),
    exit(RegistryPid, kill),
    ok = wait_until(fun() -> is_pid(whereis(barrel_ngram_registry)) end, 100),
    ok = wait_until(fun() -> not barrel_ngram:is_open(Corpus) end, 100),
    ?assertEqual(false, barrel_ngram:is_open(Corpus)),
    ?assertEqual(false, barrel_ngram:is_open(CorpusB)),
    ?assertEqual(ok, barrel_ngram:close(Corpus)),
    ?assertEqual(ok, barrel_ngram:close(CorpusB)).

%% supervisor:start_child/2 is itself an RPC into the lifecycle
%% supervisor's own process; if that process doesn't exist right now
%% the call EXITS in the caller rather than returning an error tuple.
lifecycle_unavailable_when_sup_gone(Config) ->
    Db = ?config(db, Config), Corpus = ?config(corpus, Config), DataDir = ?config(data_dir, Config),
    ok = supervisor:terminate_child(barrel_ngram_sup, barrel_ngram_corpus_lifecycle_sup),
    Result = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir}),
    ?assertMatch({error, {lifecycle_unavailable, _}}, Result),
    {ok, _} = supervisor:restart_child(barrel_ngram_sup, barrel_ngram_corpus_lifecycle_sup),
    %% the supervisor tree is usable again afterward
    ?assertEqual(ok, barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir})).

%% rest_for_one: a registry crash cascades to force-terminate (and
%% freshly restart, empty) both the shard supervisor and the lifecycle
%% supervisor -- every live shard for every corpus goes down with it, no
%% duplicate/orphaned process survives, and a query against the
%% now-unregistered shard fails closed (corpus_not_open), never noproc.
registry_crash_cascades_and_fails_closed(Config) ->
    Db = ?config(db, Config), Corpus = ?config(corpus, Config), DataDir = ?config(data_dir, Config),
    ok = barrel_ngram:open(Corpus, #{db => Db, data_dir => DataDir}),
    ShardPid = barrel_ngram_registry:whereis_name({shard, Corpus}),
    true = is_pid(ShardPid),
    RegistryPid = whereis(barrel_ngram_registry),
    exit(RegistryPid, kill),
    ok = wait_until(fun() -> not is_process_alive(ShardPid) end, 100),
    ok = wait_until(fun() -> is_pid(whereis(barrel_ngram_registry)) end, 100),
    ?assertEqual(undefined, barrel_ngram_registry:whereis_name({shard, Corpus})),
    ?assertEqual({error, corpus_not_open}, barrel_ngram:search(Corpus, <<"whatever">>)),
    %% is_open/1 checks live shard registration, not just meta (meta is
    %% a persistent_term entry, unaffected by the registry crash, and
    %% would otherwise keep reporting true for every previously-open
    %% corpus in the VM after a cascade like this one) -- the real
    %% safety guarantee for a query is still safe_shard_call/2's
    %% fail-closed behavior proved above, not is_open/1.
    ?assertEqual(false, barrel_ngram:is_open(Corpus)),
    %% close/1 still converges cleanly even though nothing is actually
    %% running under the (fresh, empty) post-cascade shard supervisor
    ?assertEqual(ok, barrel_ngram:close(Corpus)).

%% Two concurrent open/2 calls for the same, not-yet-existing corpus:
%% the second is genuine {already_started, Pid} contention against the
%% first's coordinator, waited out and retried rather than failing
%% immediately -- exactly one shard set ends up running, never two.
concurrent_open_same_corpus_serialized(Config) ->
    Db = ?config(db, Config), Corpus = ?config(corpus, Config), DataDir = ?config(data_dir, Config),
    Opts = #{db => Db, data_dir => DataDir},
    Self = self(),
    spawn(fun() -> Self ! {r1, barrel_ngram:open(Corpus, Opts)} end),
    spawn(fun() -> Self ! {r2, barrel_ngram:open(Corpus, Opts)} end),
    R1 = receive {r1, V1} -> V1 after 5000 -> timeout end,
    R2 = receive {r2, V2} -> V2 after 5000 -> timeout end,
    ?assertEqual(ok, R1),
    ?assertEqual(ok, R2),
    ?assertEqual(true, barrel_ngram:is_open(Corpus)),
    ShardPid = barrel_ngram_registry:whereis_name({shard, Corpus}),
    ?assert(is_pid(ShardPid)).

%% Path-traversal and shape validation: rejected before any filesystem
%% access, for both the atom and binary corpus-name forms, and for a
%% malformed (non-map) phase2_selector_opts that would otherwise crash
%% deep inside maps:get/3 with badmap.
validate_open_opts_path_traversal_and_shape(Config) ->
    Db = ?config(db, Config), DataDir = ?config(data_dir, Config),
    ?assertMatch({error, {invalid_option, corpus, _}},
                 barrel_ngram:open('../escape', #{db => Db, data_dir => DataDir})),
    ?assertMatch({error, {invalid_option, corpus, _}},
                 barrel_ngram:open(<<"a/b">>, #{db => Db, data_dir => DataDir})),
    ?assertMatch({error, {invalid_option, corpus, _}},
                 barrel_ngram:open(<<"..">>, #{db => Db, data_dir => DataDir})),
    ?assertMatch({error, {invalid_option, corpus, _}},
                 barrel_ngram:open(<<>>, #{db => Db, data_dir => DataDir})),
    ?assertMatch({error, {invalid_option, phase2_selector_opts, not_a_map}},
                 barrel_ngram:open(<<"ok_corpus">>, #{db => Db, data_dir => DataDir,
                                                      phase2_selector_opts => not_a_map})),
    ?assertMatch({error, {invalid_option, shards, 0}},
                 barrel_ngram:open(<<"ok_corpus">>, #{db => Db, data_dir => DataDir, shards => 0})).

%% barrel_ngram_query:corpus_nc/1's undefined-meta fallback used to call
%% barrel_ngram_shard:get_config/1 directly (a raw gen_server:call),
%% crashing the caller with exit(noproc) for a corpus that was never
%% opened -- instead of going through safe_shard_call/2 like every other
%% shard call site. Regression test for that fix.
search_on_never_opened_corpus_fails_closed(Config) ->
    Db = ?config(db, Config), Corpus = ?config(corpus, Config),
    ?assertEqual({error, corpus_not_open}, barrel_ngram:search(Corpus, <<"whatever">>)),
    ?assertEqual({error, corpus_not_open}, barrel_ngram:regex(Corpus, <<"whate.er">>)),
    _ = Db.

%% `corpus() :: binary() | atom()' -- the atom and binary forms of the
%% SAME name must be the SAME identity everywhere: the same lifecycle
%% lock, the same shard ref, the same runtime meta. `corpus_name/1' in
%% barrel_ngram_corpus_config/barrel_ngram_corpus_lifecycle already
%% normalizes both to the identical on-disk directory -- if the LOCK
%% and REF layer disagreed (treating `foo' and `<<"foo">>' as two
%% different corpora), the binary form could open a SECOND, independent
%% shard against the exact directory the atom form's shard already
%% owns: two uncoordinated writers to the same manifest and segments,
%% and closing one would never stop the other.
atom_and_binary_corpus_names_are_one_identity(Config) ->
    Db = ?config(db, Config), DataDir = ?config(data_dir, Config),
    CorpusBin = ?config(corpus, Config),
    CorpusAtom = binary_to_atom(CorpusBin, utf8),
    ok = barrel_ngram:open(CorpusAtom, #{db => Db, data_dir => DataDir}),
    ShardPid = barrel_ngram_registry:whereis_name({shard, CorpusBin}),
    ?assert(is_pid(ShardPid)),
    %% reopening via the binary form re-attaches to the SAME shard --
    %% not a second one -- and both forms agree it is open
    ?assertEqual(ok, barrel_ngram:open(CorpusBin, #{db => Db, data_dir => DataDir})),
    ?assertEqual(ShardPid, barrel_ngram_registry:whereis_name({shard, CorpusBin})),
    ?assertEqual(true, barrel_ngram:is_open(CorpusAtom)),
    ?assertEqual(true, barrel_ngram:is_open(CorpusBin)),
    %% closing via the ATOM form stops the one and only real shard --
    %% the binary form immediately agrees it is closed too
    ?assertEqual(ok, barrel_ngram:close(CorpusAtom)),
    ?assertNot(is_process_alive(ShardPid)),
    ?assertEqual(undefined, barrel_ngram_registry:whereis_name({shard, CorpusBin})),
    ?assertEqual(false, barrel_ngram:is_open(CorpusBin)).

%%====================================================================
%% Helpers
%%====================================================================

wait_until(_Pred, 0) ->
    {error, timeout};
wait_until(Pred, Attempts) ->
    case Pred() of
        true -> ok;
        false -> timer:sleep(50), wait_until(Pred, Attempts - 1)
    end.
