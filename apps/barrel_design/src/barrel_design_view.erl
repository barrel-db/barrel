%% @doc The indexer of one view: the only writer of its index.
%%
%% It follows the database's changes feed in batches (barrel_docdb:
%% read_feed/3), maps each changed document and replaces the document's
%% rows, reverse entry, error and the view's checkpoint in one write
%% batch. It keeps running with no readers. A first build reads the feed
%% from the start; the generation becomes complete once a page is short,
%% that is once it has caught up with the database.
%%
%% A changed definition (or `rebuild') fills a new generation while the
%% current one keeps serving and following the feed. When the new one
%% catches up, one batch makes it current and the old rows are deleted.
%%
%% Freshness waits only observe progress; a refresh captures the
%% database's last write once and wakes the indexer. Both answer with
%% the current generation's checkpoint once it covers their target, or
%% with an error carrying the checkpoint reached at their deadline.
-module(barrel_design_view).
-behaviour(gen_server).

-export([start_link/1, wait_for/3, refresh/2, rebuild/1, set_definition/4, status/1]).
-export([init/1, handle_continue/2, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2]).

-define(BATCH, 64).
-define(MAX_WAITERS, 1024).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%% @doc Wait until the current generation covers Target
%% (`#{instance_id, last_seq}') or the deadline passes.
-spec wait_for(pid(), map(), integer()) -> {ok, map()} | {error, term()}.
wait_for(Pid, Target, Deadline) ->
    request(Pid, {wait_for, Target, Deadline}, Deadline).

%% @doc Catch up with the database's last write as of this call.
-spec refresh(pid(), integer()) -> {ok, map()} | {error, term()}.
refresh(Pid, Deadline) ->
    request(Pid, {refresh, Deadline}, Deadline).

-spec rebuild(pid()) -> {ok, non_neg_integer()}.
rebuild(Pid) ->
    gen_server:call(Pid, rebuild, 30000).

-spec set_definition(pid(), map(), binary(), pos_integer()) -> {ok, map()}.
set_definition(Pid, Def, Sig, Version) ->
    gen_server:call(Pid, {set_definition, Def, Sig, Version}, 30000).

-spec status(pid()) -> map().
status(Pid) ->
    gen_server:call(Pid, status, 30000).

request(Pid, Msg, Deadline) ->
    Timeout = max(0, Deadline - now_ms()) + 1000,
    try gen_server:call(Pid, Msg, Timeout)
    catch
        exit:{timeout, _} -> {error, timeout};
        exit:{noproc, _} -> {error, not_running};
        exit:{Reason, {gen_server, call, _}} -> {error, {indexer_stopped, Reason}}
    end.

%%====================================================================
%% gen_server
%%====================================================================

init(#{db := Db, store := Store, design := D, view := V, instance := Inst,
       def := Def, signature := Sig, version := Ver} = Args) ->
    process_flag(trap_exit, true),
    Vid = barrel_design_index:view_id(D, V),
    S = #{db => Db, store => Store, design => D, view => V, vid => Vid,
          instance => Inst, waiters => [], timer => undefined, idle => 0,
          batch => maps:get(batch, Args, app_env(batch_size, ?BATCH)),
          poll => app_env(poll_ms, 25), idle_poll => app_env(idle_poll_ms, 250),
          runtime => #{}},
    {ok, S, {continue, {load, Def, Sig, Ver}}}.

handle_continue({load, Def, Sig, Ver}, #{store := Store, vid := Vid} = S) ->
    State = case barrel_design_index:read_state(Store, Vid) of
        {ok, Persisted} -> Persisted;
        not_found ->
            #{design => maps:get(design, S), view => maps:get(view, S),
              instance => maps:get(instance, S),
              current => cursor(1, Def, Sig, Ver), building => undefined, next_gen => 2}
    end,
    S1 = S#{state => State},
    {S2, _} = define(Def, Sig, Ver, S1),
    S3 = purge_check(S2),
    ok = persist([], S3),
    self() ! tick,
    {noreply, S3}.

handle_call({wait_for, #{instance_id := Inst, last_seq := Seq}, Deadline}, From,
            #{instance := Inst} = S) when is_binary(Seq) ->
    add_waiter({From, Seq, Deadline}, S, false);
handle_call({wait_for, #{instance_id := _}, _}, _From, S) ->
    {reply, {error, wrong_source}, S};
handle_call({wait_for, Target, _}, _From, S) ->
    {reply, {error, {invalid_target, Target}}, S};
handle_call({refresh, Deadline}, From, #{db := Db, instance := Inst} = S) ->
    case barrel_docdb:db_observed_version(Db) of
        {ok, #{instance_id := Inst, last_seq := Seq}} -> add_waiter({From, Seq, Deadline}, S, true);
        {ok, _} -> {reply, {error, wrong_source}, S};
        {error, _} = Error -> {reply, Error, S}
    end;
handle_call(rebuild, _From, #{state := #{current := Cur}} = S) ->
    #{def := Def, signature := Sig, version := Ver} = Cur,
    S1 = start_building(Def, Sig, Ver, S),
    ok = persist([], S1),
    {reply, {ok, building_gen(S1)}, wake(S1)};
handle_call({set_definition, Def, Sig, Ver}, _From, S) ->
    {S1, Outcome} = define(Def, Sig, Ver, S),
    ok = persist([], S1),
    {reply, {ok, Outcome}, wake(S1)};
handle_call(status, _From, S) ->
    {reply, status_of(S), S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info(tick, S) ->
    step(S#{timer := undefined});
handle_info(check_waiters, S) ->
    {noreply, answer(S)};
handle_info({'EXIT', _Pid, normal}, S) ->
    {noreply, S};
handle_info({'EXIT', _Pid, Reason}, S) ->
    {stop, Reason, S};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, #{runtime := Runtime}) ->
    [barrel_design_modules:release(D) || #{digest := D} <- maps:values(Runtime)],
    ok;
terminate(_Reason, _S) ->
    ok.

%%====================================================================
%% Definitions and generations
%%====================================================================

cursor(Gen, Def, Sig, Ver) ->
    #{gen => Gen, def => Def, signature => Sig, version => Ver,
      checkpoint => <<0:96>>, complete => false}.

%% Install a definition: same signature keeps the rows (a caption or a
%% version bump), another one builds a new generation.
define(Def, Sig, Ver, #{state := State} = S) ->
    #{current := Cur, building := Building} = State,
    case {Cur, Building} of
        {#{signature := Sig}, _} ->
            S1 = drop_building(S),
            #{state := St1} = S1,
            {S1#{state := St1#{current := Cur#{def := Def, version := Ver}}}, kept};
        {_, #{signature := Sig}} ->
            {S#{state := State#{building := Building#{def := Def, version := Ver}}}, building};
        _ ->
            {start_building(Def, Sig, Ver, S), rebuilding}
    end.

start_building(Def, Sig, Ver, S) ->
    #{state := #{next_gen := Next} = State} = drop_building(S),
    S#{state := State#{building := cursor(Next, Def, Sig, Ver), next_gen := Next + 1}}.

drop_building(#{state := #{building := undefined}} = S) -> S;
drop_building(#{state := #{building := #{gen := G}} = State} = S) ->
    ok = delete_generation(G, S),
    forget_runtime(G, S#{state := State#{building := undefined}}).

building_gen(#{state := #{building := #{gen := G}}}) -> G.

delete_generation(G, #{store := Store, vid := Vid}) ->
    lists:foreach(fun({L, U}) -> ok = barrel_design_store:delete_range(Store, L, U) end,
                  barrel_design_index:generation_ranges(Vid, G)).

%% A checkpoint older than the database's tombstone purge window may
%% have missed deletes that are no longer in the feed: rebuild.
purge_check(#{db := Db, state := #{current := #{checkpoint := <<Wall:64, _:32>>} = Cur}} = S)
        when Wall > 0 ->
    Window = case barrel_docdb:db_info(Db) of
        {ok, #{config := #{tombstone_purge_after := P}}} when is_integer(P), P > 0 -> P * 1000;
        _ -> infinity
    end,
    case Window =/= infinity andalso erlang:system_time(millisecond) - Wall > Window of
        true ->
            #{def := Def, signature := Sig, version := Ver} = Cur,
            logger:notice("barrel_design: ~s/~s/~s checkpoint is older than the purge "
                          "window; rebuilding", [Db, maps:get(design, S), maps:get(view, S)]),
            start_building(Def, Sig, Ver, S);
        false -> S
    end;
purge_check(S) ->
    S.

%%====================================================================
%% Indexing
%%====================================================================

step(#{state := State} = S) ->
    #{current := Cur, building := Building} = State,
    case advance(Cur, S) of
        {stop, Reason} -> {stop, Reason, S};
        {busy, S1} -> {noreply, answer(schedule(10, S1))};
        {ok, Cur1, FullC, S1} ->
            S2 = put_cursor(current, Cur1, S1),
            case Building of
                undefined -> {noreply, answer(after_step(FullC, S2))};
                _ ->
                    case advance(Building, S2) of
                        {stop, Reason} -> {stop, Reason, S2};
                        {busy, S3} -> {noreply, answer(schedule(10, S3))};
                        {ok, #{complete := true} = Done, FullB, S3} ->
                            S4 = switch(Done, S3),
                            {noreply, answer(after_step(FullC orelse FullB, S4))};
                        {ok, B1, FullB, S3} ->
                            {noreply, answer(after_step(FullC orelse FullB,
                                                        put_cursor(building, B1, S3)))}
                    end
            end
    end.

put_cursor(Role, Cursor, #{state := State} = S) ->
    S#{state := State#{Role := Cursor}}.

%% The building generation caught up: make it current, drop the old rows.
switch(Done, #{state := #{current := #{gen := Old}} = State} = S) ->
    S1 = S#{state := State#{current := Done, building := undefined}},
    ok = persist([], S1),
    ok = delete_generation(Old, S1),
    forget_runtime(Old, S1).

after_step(true, S) -> schedule(0, S#{idle := 0});
after_step(false, #{waiters := [_ | _], poll := Poll} = S) -> schedule(Poll, S#{idle := 0});
after_step(false, #{idle := Idle, poll := Poll, idle_poll := IdlePoll} = S) ->
    Delay = case Idle >= 40 of true -> IdlePoll; false -> Poll end,
    schedule(Delay, S#{idle := Idle + 1}).

schedule(Delay, S) ->
    S#{timer := erlang:send_after(Delay, self(), tick)}.

wake(#{timer := undefined} = S) -> S;
wake(#{timer := Timer} = S) ->
    case erlang:cancel_timer(Timer) of
        false -> S;
        _ -> self() ! tick, S#{timer := undefined, idle := 0}
    end.

%% Read one feed page for a cursor, map it and publish it.
advance(#{gen := G, checkpoint := Since} = Cursor, #{db := Db, batch := Batch, instance := Inst,
                                                    vid := Vid, store := Store} = S) ->
    case barrel_docdb:read_feed(Db, Since, #{limit => Batch}) of
        {ok, Changes, Next, #{instance_id := Inst}} ->
            Ids = [Id || #{id := Id} <- Changes],
            Olds = maps:from_list(lists:zip(Ids, barrel_design_store:multi_get(
                Store, barrel_design_index:reverse_keys(Vid, G, Ids)))),
            case map_changes(Changes, Olds, Cursor, S) of
                {busy, S1} ->
                    {busy, S1};
                {ok, Results, S1} ->
                    Ops = lists:flatmap(fun(#{id := Id, rev := Rev} = C) ->
                        case maps:get(Id, Results) of
                            unchanged -> [];
                            R ->
                                barrel_design_index:doc_ops(Vid, G, Id,
                                    {Rev, maps:get(expires_at, C, 0)}, maps:get(Id, Olds), R)
                        end
                    end, Changes),
                    Full = length(Changes) >= Batch,
                    Cursor1 = Cursor#{checkpoint := Next,
                                      complete := maps:get(complete, Cursor) orelse not Full},
                    Role = role_of(G, S1),
                    ok = persist(Ops, put_cursor(Role, Cursor1, S1)),
                    {ok, Cursor1, Full, S1}
            end;
        {ok, _, _, #{instance_id := Other}} ->
            {stop, {shutdown, {instance_changed, Other}}};
        {error, Reason} ->
            {stop, {shutdown, {db_unavailable, Reason}}}
    end.

role_of(G, #{state := #{current := #{gen := G}}}) -> current;
role_of(_G, _S) -> building.

%% Map the changed documents. A document whose stored revision matches
%% the change is unchanged; a deleted or unselected one gets no rows.
map_changes(Changes, Olds, #{gen := G, def := Def} = Cursor, S) ->
    {Rt, S1} = runtime(Cursor, S),
    Pending = lists:filtermap(fun(#{id := Id, rev := Rev} = C) ->
        case {maps:get(Id, Olds), C} of
            {{ok, Bin}, _} when not is_map_key(deleted, C) ->
                case binary_to_term(Bin) of
                    #{rev := Rev} -> false;
                    _ -> {true, C}
                end;
            _ -> {true, C}
        end
    end, Changes),
    Unchanged = maps:from_list([{Id, unchanged} || #{id := Id} <- Changes -- Pending]),
    {ToMap, Skipped} = lists:partition(fun
        (#{deleted := true}) -> false;
        (#{doc := Doc}) -> barrel_design_manifest:selected(maps:get(selector, Rt), Doc);
        (_) -> false
    end, Pending),
    Skips = maps:from_list([{Id, skip} || #{id := Id} <- Skipped]),
    case run_maps(ToMap, Def, Rt, S1) of
        busy -> {busy, S1};
        {ok, Mapped} ->
            _ = G,
            {ok, maps:merge(maps:merge(Unchanged, Skips), Mapped), S1}
    end.

run_maps([], _Def, _Rt, _S) ->
    {ok, #{}};
run_maps(Changes, #{map := native} = Def, _Rt, _S) ->
    {ok, maps:from_list([{Id, barrel_design_native:map(Def, Doc)}
                         || #{id := Id, doc := Doc} <- Changes])};
run_maps(Changes, _Def, #{handle := H, spec := Spec, digest := Digest}, #{db := Db}) ->
    case barrel_design_slots:try_acquire(Db) of
        busy -> busy;
        ok ->
            Docs = [Doc || #{doc := Doc} <- Changes],
            try
                Results = guest_run(H, Spec, Docs, Digest),
                {ok, maps:from_list(lists:zip([Id || #{id := Id} <- Changes], Results))}
            after
                barrel_design_slots:release(Db)
            end
    end.

%% A restarted engine lost the module: load it again from our bytes.
guest_run(H, Spec, Docs, Digest) ->
    Results = barrel_design_guest:run(H, Spec, Docs, #{}),
    case lists:member({error, module_not_loaded}, Results) of
        true ->
            {ok, H1} = barrel_design_modules:reload(Digest),
            barrel_design_guest:run(H1, Spec, Docs, #{});
        false ->
            Results
    end.

%% Compiled selector and, for a Wasm map, the loaded module of a cursor.
runtime(#{gen := G, def := Def}, #{runtime := Rts, db := Db} = S) ->
    case maps:find(G, Rts) of
        {ok, Rt} -> {Rt, S};
        error ->
            {ok, Sel} = barrel_design_manifest:selector(maps:get(select, Def)),
            Rt = case Def of
                #{map := #{module := Digest, export := Export, fields := Fields}} ->
                    {ok, Bytes} = barrel_design_catalog:module_bytes(Db, Digest),
                    {ok, H} = barrel_design_modules:acquire(Digest, Bytes),
                    #{selector => Sel, handle => H, digest => Digest,
                      spec => #{export => Export, fields => Fields, key => maps:get(key, Def),
                                value => maps:get(value, Def), limits => maps:get(limits, Def)}};
                _ ->
                    #{selector => Sel}
            end,
            {Rt, S#{runtime := Rts#{G => Rt}}}
    end.

forget_runtime(G, #{runtime := Rts} = S) ->
    case maps:take(G, Rts) of
        {#{digest := D}, Rest} ->
            ok = barrel_design_modules:release(D),
            S#{runtime := Rest};
        {_, Rest} -> S#{runtime := Rest};
        error -> S
    end.

persist(Ops, #{store := Store, vid := Vid, state := State}) ->
    barrel_design_store:write(Store, Ops ++ [barrel_design_index:state_op(Vid, State)]).

%%====================================================================
%% Waiters
%%====================================================================

add_waiter(_W, #{waiters := Ws} = S, _Wake) when length(Ws) >= ?MAX_WAITERS ->
    {reply, {error, overloaded}, S};
add_waiter({From, Seq, Deadline} = W, #{waiters := Ws} = S, Wake) ->
    case covered(Seq, S) of
        {true, Checkpoint} -> {reply, {ok, Checkpoint}, S};
        false ->
            S1 = S#{waiters := [W | Ws]},
            %% answer at the deadline even if no pass runs before it
            _ = erlang:send_after(max(0, Deadline - now_ms()), self(), check_waiters),
            _ = From,
            S2 = case Wake of
                true -> wake(S1);
                false -> wake_soon(S1)
            end,
            {noreply, S2}
    end.

%% A waiter shortens an idle poll without forcing a pass.
wake_soon(#{timer := undefined} = S) -> S;
wake_soon(#{timer := T, poll := Poll} = S) ->
    case erlang:read_timer(T) of
        Left when is_integer(Left), Left > Poll ->
            _ = erlang:cancel_timer(T),
            schedule(Poll, S#{idle := 0});
        _ -> S
    end.

covered(Seq, #{state := #{current := #{complete := true, checkpoint := Cp}}, instance := Inst})
        when Cp >= Seq ->
    {true, #{instance_id => Inst, last_seq => Cp}};
covered(_Seq, _S) ->
    false.

answer(#{waiters := []} = S) -> S;
answer(#{waiters := Ws, instance := Inst, state := #{current := Cur}} = S) ->
    Now = now_ms(),
    Left = lists:filter(fun({From, Seq, Deadline}) ->
        case covered(Seq, S) of
            {true, Cp} ->
                gen_server:reply(From, {ok, Cp}),
                false;
            false when Now >= Deadline ->
                Reached = case Cur of
                    #{complete := true, checkpoint := Cp} -> #{instance_id => Inst, last_seq => Cp};
                    _ -> building
                end,
                gen_server:reply(From, {error, {timeout, Reached}}),
                false;
            false ->
                true
        end
    end, Ws),
    S#{waiters := Left}.

%%====================================================================
%% Status
%%====================================================================

status_of(#{state := #{current := Cur, building := B}, instance := Inst} = S) ->
    Errors = error_count(Cur, S),
    Base = #{generation => maps:get(gen, Cur),
             status => case Cur of #{complete := true} -> complete; _ -> building end,
             checkpoint => #{instance_id => Inst, last_seq => maps:get(checkpoint, Cur)},
             signature => binary:encode_hex(maps:get(signature, Cur), lowercase),
             version => maps:get(version, Cur),
             errors => Errors,
             waiters => length(maps:get(waiters, S))},
    case B of
        undefined -> Base;
        #{gen := BG, checkpoint := BC} -> Base#{rebuilding => #{generation => BG, last_seq => BC}}
    end.

error_count(#{gen := G}, #{store := Store, vid := Vid}) ->
    [_, _, {L, U}] = barrel_design_index:generation_ranges(Vid, G),
    barrel_design_store:fold(Store, L, U, fun(_K, _V, N) -> {ok, N + 1} end, 0, undefined).

now_ms() -> erlang:monotonic_time(millisecond).

app_env(Key, Default) -> application:get_env(barrel_design, Key, Default).
