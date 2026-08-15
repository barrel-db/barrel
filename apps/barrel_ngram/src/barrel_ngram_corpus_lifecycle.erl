%%%-------------------------------------------------------------------
%%% @doc One-shot corpus open/close coordinator.
%%%
%%% Registered under `{via, barrel_ngram_registry, {corpus_lock, Corpus}}'
%%% so open and close for the SAME corpus can never interleave -- the
%%% registry's own `register_name/2' is already serialized through its
%%% own gen_server call, making `via'-name registration an atomic,
%%% reusable per-corpus mutex. Registration happens as part of starting
%%% the process, BEFORE `init/1' ever runs, so mutual exclusion holds
%%% regardless of where the real work runs relative to `init/1' returning.
%%%
%%% `init/1' does NOT run the operation itself -- it hands back control
%%% immediately via `{continue, run}', deferring the entire open/close
%%% body to `handle_continue/2'. This matters because the coordinator is
%%% started via `supervisor:start_child(barrel_ngram_corpus_lifecycle_sup,
%%% ...)': `start_child/2' is a synchronous call into the SUPERVISOR's
%%% own process, which blocks handling that request until `init/1'
%%% returns. Running the whole body inside `init/1' would block the
%%% lifecycle supervisor's mailbox for however long that takes -- unable
%%% to process a shutdown request from its own parent during a
%%% `rest_for_one' cascade. Splitting the work into `handle_continue/2'
%%% keeps `init/1' fast regardless.
%%%
%%% The result is sent DIRECTLY to the caller (by a reference generated
%%% before this process even starts, see `barrel_ngram:lifecycle_call/3')
%%% rather than relying on this process's own exit reason: a monitor
%%% installed after `start_child/2' returns can race a fast
%%% `handle_continue/2' that already finished.
%%%
%%% Two runtime `persistent_term' caches (`barrel_ngram_shards') are
%%% involved: `meta' (query-trusted, published ONLY on full success) and
%%% `pending_meta' (discovery-only, published as soon as a request's
%%% config is reconciled, before any shard starts) -- see
%%% `barrel_ngram_shards''s moduledoc for why the two are never
%%% conflated.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_corpus_lifecycle).
-behaviour(gen_server).

-export([start_link/4]).
-export([init/1, handle_continue/2, handle_call/3, handle_cast/2]).

-spec start_link(pid(), reference(), term(), term()) -> {ok, pid()} | {error, term()} | ignore.
start_link(Caller, ReplyRef, Corpus, Op) ->
    gen_server:start_link({via, barrel_ngram_registry, {corpus_lock, Corpus}},
                          ?MODULE, {Caller, ReplyRef, Corpus, Op}, []).

init({Caller, ReplyRef, Corpus, Op}) ->
    {ok, #{caller => Caller, reply_ref => ReplyRef, corpus => Corpus, op => Op},
     {continue, run}}.

handle_continue(run, #{caller := Caller, reply_ref := ReplyRef,
                       corpus := Corpus, op := Op} = State) ->
    Result = run_op(Corpus, Op),
    Caller ! {ReplyRef, Result},
    {stop, {shutdown, Result}, State}.

handle_call(_Msg, _From, State) ->
    {reply, {error, unexpected_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%%====================================================================
%% run_op/2 for {open, Opts}
%%====================================================================

run_op(Corpus, {open, Opts}) ->
    case normalize(Corpus, Opts) of
        {ok, Config} -> run_open(Corpus, Config);
        {error, _} = Err -> Err
    end;
run_op(Corpus, close) ->
    run_close(Corpus).

%% @private Step 1 (normalize) -- extends barrel_ngram's old normalize/2
%% with `shards' (previously handled separately as a bare `N') and
%% `db_instance_id' (new -- the corpus-level binding this whole finding
%% exists to make durable). A database that cannot be resolved (does not
%% exist / was never opened in this VM) is a clean, distinct error here
%% rather than a crash deep inside shard startup.
normalize(Corpus, Opts) ->
    Db = maps:get(db, Opts),
    case barrel_docdb:db_instance_id(Db) of
        {ok, DbInstanceId} ->
            N = maps:get(shards, Opts, 1),
            Base = #{
                corpus => Corpus,
                db => Db,
                db_instance_id => DbInstanceId,
                shards => N,
                fields => normalize_fields(maps:get(fields, Opts, all)),
                phase2_selector_opts =>
                    barrel_ngram_selector_sparse:normalize_opts(
                      maps:get(phase2_selector_opts, Opts, #{})),
                data_dir => maps:get(data_dir, Opts,
                                     application:get_env(barrel_ngram, data_dir,
                                                         "data/barrel_ngram")),
                postings => maps:get(postings, Opts, varint)
            },
            {ok, maps:merge(Base, maps:with([freeze_threshold, compact_threshold, source], Opts))};
        {error, Reason} ->
            {error, {db_not_available, Reason}}
    end.

normalize_fields(all) -> all;
normalize_fields(List) when is_list(List) -> lists:usort(List).

%% @private Steps 2-9.
run_open(Corpus, Config) ->
    ok = barrel_ngram_corpus_config:cleanup_orphan_tmp(Config),
    case load_freshness(Config) of
        {error, _} = Err ->
            Err;
        {HasCorpusMeta, HasAnyArtifact, Persisted} ->
            FreshCorpus0 = (not HasCorpusMeta) andalso (not HasAnyArtifact),
            case runtime_meta_reconcile(Corpus, Config) of
                {error, _} = Err ->
                    Err;
                ok ->
                    case reconcile_or_init(Corpus, Config, HasCorpusMeta, HasAnyArtifact, Persisted) of
                        {error, _} = Err -> Err;
                        ok -> start_and_finish(Corpus, Config, FreshCorpus0)
                    end
            end
    end.

%% Step 2: three-way branch on corpus.meta, plus the broad HasAnyArtifact
%% existence scan.
load_freshness(Config) ->
    case barrel_ngram_corpus_config:load(Config) of
        {ok, Persisted} ->
            {true, has_any_artifact(Config), Persisted};
        not_found ->
            {false, has_any_artifact(Config), undefined};
        {error, Reason} ->
            {error, {corpus_meta_corrupt, Reason}}
    end.

has_any_artifact(Config) ->
    Base = corpus_base_dir(Config),
    dir_has_any_file(Base) orelse any_shard_subdir_has_file(Base).

dir_has_any_file(Dir) ->
    case file:list_dir(Dir) of
        {ok, [_ | _]} -> true;
        {ok, []} -> false;
        {error, _} -> false
    end.

any_shard_subdir_has_file(Base) ->
    case file:list_dir(Base) of
        {ok, Entries} ->
            lists:any(
              fun(E) ->
                  case lists:prefix("shard-", E) of
                      true -> dir_has_any_file(filename:join(Base, E));
                      false -> false
                  end
              end, Entries);
        {error, _} ->
            false
    end.

%% Runtime-meta reconciliation -- unconditional, BEFORE branching on
%% HasCorpusMeta at all (see the moduledoc note on why this must not
%% live only inside the fresh-disk branch: disk being VALID is no
%% guarantee it describes the corpus's CURRENT binding).
runtime_meta_reconcile(Corpus, Config) ->
    case runtime_meta(Corpus) of
        undefined ->
            ok;
        {ok, RuntimeConfig} ->
            case first_mismatch(RuntimeConfig, Config, runtime_reconcile_fields()) of
                none -> ok;
                {Field, Got, Want} -> {error, {config_mismatch, Field, Got, Want}}
            end
    end.

runtime_meta(Corpus) ->
    case barrel_ngram_shards:get_meta(Corpus) of
        {ok, M} -> {ok, M};
        undefined ->
            case barrel_ngram_shards:get_pending_meta(Corpus) of
                {ok, M} -> {ok, M};
                undefined -> undefined
            end
    end.

runtime_reconcile_fields() ->
    [db, db_instance_id, shards, phase2_selector_opts, fields, postings, data_dir].

%% Fields reconciled against DISK (corpus.meta) or a live shard's own
%% get_config/1 -- data_dir is deliberately excluded: the disk read is
%% already implicitly scoped to the request's own data_dir (there is no
%% other file corpus.meta could have been read from).
disk_reconcile_fields() ->
    [db, db_instance_id, shards, phase2_selector_opts, fields, postings].

%% @private First field (in a fixed, deterministic order) that differs
%% between Persisted and Requested.
first_mismatch(_Persisted, _Requested, []) ->
    none;
first_mismatch(Persisted, Requested, [F | Rest]) ->
    PV = maps:get(F, Persisted, undefined),
    RV = maps:get(F, Requested, undefined),
    case PV =:= RV of
        true -> first_mismatch(Persisted, Requested, Rest);
        false -> {F, PV, RV}
    end.

%% Steps 3/4/5.
reconcile_or_init(Corpus, Config, true, _HasAnyArtifact, Persisted) ->
    %% step 3: a persisted corpus.meta exists (any `state') -- reconcile
    %% against it, ignoring `state' itself (internal bookkeeping).
    case first_mismatch(Persisted, Config, disk_reconcile_fields()) of
        none ->
            barrel_ngram_shards:put_pending_meta(Corpus, Config),
            ok;
        {Field, Got, Want} ->
            {error, {config_mismatch, Field, Got, Want}}
    end;
reconcile_or_init(_Corpus, Config, false, true, _Persisted) ->
    %% step 4: real data exists but no corpus.meta -- a pre-fix corpus.
    {error, {legacy_corpus_requires_reindex, maps:get(corpus, Config)}};
reconcile_or_init(Corpus, Config, false, false, _Persisted) ->
    %% step 5: genuinely fresh. Durable marker BEFORE any shard starts.
    Initializing = corpus_meta_map(Config, initializing),
    case barrel_ngram_corpus_config:save(Config, Initializing) of
        ok ->
            barrel_ngram_shards:put_pending_meta(Corpus, Config),
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

corpus_meta_map(Config, State) ->
    Base = maps:with([db, db_instance_id, shards, phase2_selector_opts, fields, postings], Config),
    Base#{state => State}.

%% Steps 6-9.
start_and_finish(Corpus, Config, FreshCorpus0) ->
    N = maps:get(shards, Config),
    RefsWithIndex = lists:zip(lists:seq(0, N - 1), barrel_ngram_shards:refs(Corpus, N)),
    case start_shards_tracked(RefsWithIndex, Config) of
        {ok, Attempted, Started, Existing} ->
            finish_start(Corpus, Config, FreshCorpus0, Attempted, Started, Existing, ok);
        {error, Reason, Attempted, Started, Existing} ->
            finish_start(Corpus, Config, FreshCorpus0, Attempted, Started, Existing, {error, Reason})
    end.

finish_start(Corpus, Config, FreshCorpus0, Attempted, Started, Existing, Outcome) ->
    case failure_reason(Outcome, Existing, Config, FreshCorpus0) of
        none ->
            activate(Corpus, Config);
        Reason ->
            EffectiveFreshCorpus = FreshCorpus0 andalso Existing =:= [],
            RollbackResult = rollback_started(Corpus, Config, Started, Attempted,
                                              EffectiveFreshCorpus, FreshCorpus0),
            {error, {open_failed, Reason, RollbackResult}}
    end.

%% Step 6's live-config diff, then step 7's disk/runtime-contradiction
%% check -- both independent triggers for step 8's rollback.
failure_reason({error, Reason}, _Existing, _Config, _FreshCorpus0) ->
    Reason;
failure_reason(ok, Existing, Config, FreshCorpus0) ->
    case existing_config_mismatch(Existing, Config) of
        ok ->
            case FreshCorpus0 andalso Existing =/= [] of
                true ->
                    [{IncRef, _} | _] = Existing,
                    {corpus_state_inconsistent, IncRef};
                false ->
                    none
            end;
        {mismatch, _Ref, _Field, _Got, _Want} = M ->
            M
    end.

existing_config_mismatch([], _Config) ->
    ok;
existing_config_mismatch([{Ref, Pid} | Rest], Config) ->
    case barrel_ngram:safe_shard_call(Pid, get_config) of
        {ok, LiveConfig} ->
            case first_mismatch(LiveConfig, Config, disk_reconcile_fields()) of
                none -> existing_config_mismatch(Rest, Config);
                {Field, Got, Want} -> {mismatch, Ref, Field, Got, Want}
            end;
        {error, Reason} ->
            {mismatch, Ref, get_config, undefined, Reason}
    end.

%% Step 9: transition/write corpus.meta to `active', promote pending to
%% real (query-trusted) meta. Explicitly branched -- never an
%% `ok = save(...)' match that would crash the coordinator on failure.
activate(Corpus, Config) ->
    Active = corpus_meta_map(Config, active),
    case barrel_ngram_corpus_config:save(Config, Active) of
        ok ->
            barrel_ngram_shards:put_meta(Corpus, Config),
            barrel_ngram_shards:erase_pending_meta(Corpus),
            ok;
        {error, Reason} ->
            %% every shard is genuinely up and correctly configured;
            %% only the bookkeeping write failed. Rolling back live,
            %% working shards would be actively harmful -- leave them
            %% running, leave corpus.meta as `initializing' (still
            %% durable, still reconcilable). `meta' is never published
            %% here, so is_open/1 and the query path correctly report
            %% "not open" in this narrow window -- wasteful, not unsafe.
            {error, {activation_failed, Reason}}
    end.

%%====================================================================
%% Atomic start-and-classify
%%====================================================================

start_shards_tracked(RefsWithIndex, Config) ->
    start_shards_tracked(RefsWithIndex, Config, [], [], []).

start_shards_tracked([], _Config, Attempted, Started, Existing) ->
    {ok, lists:reverse(Attempted), lists:reverse(Started), lists:reverse(Existing)};
start_shards_tracked([{I, Ref} | Rest], Config, Attempted, Started, Existing) ->
    SC = Config#{shard_index => I},
    Dir = shard_dir(Config, I),
    Attempted1 = [{Ref, Dir} | Attempted],
    case barrel_ngram_shard_sup:start_shard_tracked(Ref, SC) of
        {ok, _Pid, started} ->
            start_shards_tracked(Rest, Config, Attempted1, [Ref | Started], Existing);
        {ok, Pid, existing} ->
            start_shards_tracked(Rest, Config, Attempted1, Started, [{Ref, Pid} | Existing]);
        {error, Reason} ->
            {error, Reason, lists:reverse(Attempted1), lists:reverse(Started), lists:reverse(Existing)}
    end.

%% Mirrors barrel_ngram_shard:shard_dir/4's own convention exactly (both
%% compute the SAME path from the SAME inputs; duplicated rather than
%% exported, to avoid growing that module's public API for a single
%% internal caller). Single shard keeps the corpus dir unchanged;
%% multi-shard nests a shard-<I> subdir under it.
shard_dir(Config, I) ->
    Corpus = maps:get(corpus, Config),
    case maps:get(shards, Config) of
        1 ->
            iolist_to_binary(filename:join([data_dir(Config), corpus_name(Corpus)]));
        _ ->
            Sub = io_lib:format("shard-~6..0b", [I]),
            iolist_to_binary(filename:join([data_dir(Config), corpus_name(Corpus), Sub]))
    end.

%%====================================================================
%% Rollback
%%====================================================================

rollback_started(Corpus, Config, Started, Attempted, EffectiveFreshCorpus, WroteInitializingMeta) ->
    case barrel_ngram_shard_sup:stop_shards_confirmed(Started) of
        ok ->
            case delete_dirs_if(EffectiveFreshCorpus, Attempted) of
                ok ->
                    case delete_meta_if(WroteInitializingMeta, Config) of
                        ok ->
                            barrel_ngram_shards:erase_pending_meta(Corpus),
                            ok;
                        {error, MetaReason} ->
                            {error, {rollback_incomplete, MetaReason}}
                    end;
                {error, DirReason} ->
                    {error, {rollback_incomplete, DirReason}}
            end;
        {error, StopReason} ->
            {error, {rollback_incomplete, StopReason}}
    end.

delete_dirs_if(false, _Attempted) -> ok;
delete_dirs_if(true, Attempted) -> delete_dirs(Attempted).

delete_dirs(Attempted) ->
    Failed = [{Dir, Reason} || {_Ref, Dir} <- Attempted,
                                {error, Reason} <- [file:del_dir_r(Dir)],
                                Reason =/= enoent],
    case Failed of
        [] -> ok;
        _ -> {error, {dirs_not_removed, Failed}}
    end.

delete_meta_if(false, _Config) -> ok;
delete_meta_if(true, Config) ->
    case barrel_ngram_corpus_config:delete(Config) of
        ok -> ok;
        {error, Reason} -> {error, {corpus_meta_not_removed, Reason}}
    end.

%%====================================================================
%% run_op/2 for close
%%====================================================================

run_close(Corpus) ->
    case close_refs(Corpus) of
        {ok, []} ->
            ok;
        {ok, Refs} ->
            case barrel_ngram_shard_sup:stop_shards_confirmed(Refs) of
                ok ->
                    barrel_ngram_shards:erase_meta(Corpus),
                    barrel_ngram_shards:erase_pending_meta(Corpus),
                    ok;
                {error, _} = Err ->
                    Err
            end
    end.

%% close/1 receives only Corpus, no Config/data_dir -- refs can ONLY
%% come from a runtime cache. get_meta first (the corpus fully
%% activated, the common case); else get_pending_meta (an open that
%% reconciled but never reached activation -- fresh, resumed, or an
%% ordinary reopen, all publish pending_meta before starting shards);
%% else nothing was ever durably started in a way close/1 can discover.
close_refs(Corpus) ->
    case runtime_meta(Corpus) of
        {ok, Meta} ->
            N = maps:get(shards, Meta),
            {ok, barrel_ngram_shards:refs(Corpus, N)};
        undefined ->
            {ok, []}
    end.

%%====================================================================
%% Internal
%%====================================================================

data_dir(Config) ->
    maps:get(data_dir, Config,
             application:get_env(barrel_ngram, data_dir, "data/barrel_ngram")).

corpus_base_dir(Config) ->
    filename:join([data_dir(Config), corpus_name(maps:get(corpus, Config))]).

corpus_name(Corpus) when is_binary(Corpus) -> Corpus;
corpus_name(Corpus) when is_atom(Corpus) -> atom_to_binary(Corpus, utf8);
corpus_name(Corpus) -> iolist_to_binary(io_lib:format("~p", [Corpus])).
