%%%-------------------------------------------------------------------
%%% @doc Composed database lifecycle manager.
%%%
%%% Owns open {@link barrel} database handles for callers that need a
%%% long-lived owner (a barrel database links its vector store to the
%%% process that opens it): the REST server and the agent layer ask
%%% this manager for handles instead of opening databases themselves.
%%% Databases opened directly through {@link barrel:open/2} are not
%%% tracked here and keep their caller-owned lifecycle.
%%%
%%% The manager makes hundreds of ephemeral databases practical:
%%% handles open lazily, every use touches them, a sweep closes
%%% entries idle past `dbs_idle_timeout', and `dbs_max_open' bounds
%%% the open set with LRU eviction of idle entries (pinned entries are
%%% never closed automatically). All knobs live in the `barrel'
%%% application env and are read at use, so they can change at
%%% runtime:
%%% <ul>
%%% <li>`dbs_idle_timeout' - ms of idleness before the sweep closes a
%%%     db (default 300000; 0 disables idle closing)</li>
%%% <li>`dbs_max_open' - max open dbs, 0 = unlimited (default 0);
%%%     at the cap the least recently used unpinned entry idle for at
%%%     least `dbs_evict_guard' ms is evicted, else the open fails
%%%     with `{error, too_many_open_dbs}'</li>
%%% <li>`dbs_evict_guard' - minimum idleness for eviction, shrinking
%%%     the race between handing out a handle and closing its db
%%%     (default 5000 ms)</li>
%%% </ul>
%%%
%%% Cold opens run in a short-lived worker so a slow open never blocks the
%%% manager loop; concurrent `ensure' calls for the same name coalesce onto
%%% one open. Manager-owned databases are held by name (the docdb server and
%%% the vector store both run under their own supervisors, not linked to the
%%% manager); a crash of either is detected by the liveness check on the
%%% next `ensure', which reopens.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_dbs).
-behaviour(gen_server).

-export([start_link/0,
         ensure/1, ensure/2,
         close/1, close_owned/1, destroy/1,
         branch/3,
         pin/1, unpin/1,
         lease/2, release/1, leases/0,
         list/0,
         lookup/1, hold/2, unhold/1,
         sweep/0, sweep/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2]).

-define(SERVER, ?MODULE).
-define(DEFAULT_IDLE_TIMEOUT, 300000).
-define(DEFAULT_EVICT_GUARD, 5000).
%% How long a name-targeting call (close/destroy/branch/pin) waits before
%% retrying while that name is mid-open.
-define(OPENING_RETRY_MS, 25).

-record(entry, {
    db :: barrel:db(),
    last_used :: integer(),  %% monotonic ms
    pinned = false :: boolean(),
    %% query-scoped holds (lease/2): counted, released on holder exit
    leases = 0 :: non_neg_integer(),
    %% opaque tag from the ensure opts (`owner => Term'): lets a
    %% subsystem close exactly the entries it opened (see close_owned/1)
    owner :: term(),
    %% the open options (without owner), to reopen after a hold
    opts = #{} :: map()
}).

%% `opening' holds cold opens in flight in a worker (so a slow open never
%% blocks the manager loop), with waiters coalesced onto that one open (a
%% second open of the same name would race on the RocksDB lock).
%% #{Name => {WorkerPid, [{From, Opts}]}}.
-record(state, {dbs = #{} :: #{binary() => #entry{}},
                opening = #{} :: #{binary() => {pid(), [{term(), map()}]}},
                %% lease monitor ref -> leased name
                lease_mons = #{} :: #{reference() => binary()},
                %% names closed for exclusive file access (export): ensure
                %% refuses them until unhold/1
                held = #{} :: #{binary() => term()}}).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Return the (possibly newly opened) handle for `Name' and touch
%% its last-used time.
-spec ensure(barrel:db_name()) -> {ok, barrel:db()} | {error, term()}.
ensure(Name) ->
    ensure(Name, #{}).

%% @doc Like {@link ensure/1} with open options for a first open (a
%% cached handle is returned as-is; runtime config such as encryption
%% must match what the db was opened with). The extra `owner => Term'
%% option tags the entry for {@link close_owned/1} and is not passed
%% to {@link barrel:open/2}; `must_exist => true' fails a cold open with
%% `{error, not_found}' instead of creating the database.
-spec ensure(barrel:db_name(), map()) ->
    {ok, barrel:db()} | {error, term()}.
ensure(Name, Opts) when is_map(Opts) ->
    gen_server:call(?SERVER, {ensure, to_name(Name), Opts}, infinity).

%% @doc Close and forget the database `Name'. Idempotent.
-spec close(barrel:db_name()) -> ok.
close(Name) ->
    gen_server:call(?SERVER, {close, to_name(Name)}, infinity).

%% @doc Close every entry tagged with `Owner' (a subsystem shutting
%% down closes exactly what it opened, pinned or not).
-spec close_owned(term()) -> ok.
close_owned(Owner) ->
    gen_server:call(?SERVER, {close_owned, Owner}, infinity).

%% @doc Destroy the database `Name': close it and delete its files
%% (docdb and vector store).
-spec destroy(barrel:db_name()) -> ok | {error, term()}.
destroy(Name) ->
    gen_server:call(?SERVER, {destroy, to_name(Name)}, infinity).

%% @doc Fork `Parent' into `BranchName' and own the branch handle.
%% `Opts' are {@link barrel:branch/3} options; `open_opts' inside them
%% is used to open the parent when it is not already open.
-spec branch(barrel:db_name(), barrel:db_name(), map()) ->
    {ok, barrel:db()} | {error, term()}.
branch(Parent, BranchName, Opts) when is_map(Opts) ->
    gen_server:call(?SERVER,
                    {branch, to_name(Parent), to_name(BranchName), Opts},
                    infinity).

%% @doc Exempt an open database from idle closing and eviction (system
%% registries, continuous replication holders).
-spec pin(barrel:db_name()) -> ok | {error, not_open}.
pin(Name) ->
    gen_server:call(?SERVER, {pin, to_name(Name), true}, infinity).

%% @doc Undo {@link pin/1}.
-spec unpin(barrel:db_name()) -> ok | {error, not_open}.
unpin(Name) ->
    gen_server:call(?SERVER, {pin, to_name(Name), false}, infinity).

%% @doc Open `Name' like {@link ensure/2} and hold it against idle
%% closing and eviction until {@link release/1} or the caller exits.
%% Leases are counted, unlike the boolean {@link pin/1}.
-spec lease(barrel:db_name(), map()) ->
    {ok, barrel:db(), reference()} | {error, term()}.
lease(Name, Opts) when is_map(Opts) ->
    lease(to_name(Name), Opts, 2).

lease(_Name, _Opts, 0) ->
    {error, not_open};
lease(Name, Opts, Tries) ->
    case ensure(Name, Opts) of
        {ok, _Db} ->
            case gen_server:call(?SERVER, {lease, Name, self()}, infinity) of
                {ok, _Db1, _Ref} = Ok -> Ok;
                {error, not_open} -> lease(Name, Opts, Tries - 1)
            end;
        {error, _} = Err ->
            Err
    end.

%% @doc Drop a lease taken with {@link lease/2}. Idempotent.
-spec release(reference()) -> ok.
release(Lease) when is_reference(Lease) ->
    gen_server:call(?SERVER, {release, Lease}, infinity).

%% @doc Held lease counts by name (names with no lease are absent).
-spec leases() -> #{binary() => pos_integer()}.
leases() ->
    gen_server:call(?SERVER, leases, infinity).

%% @doc Names of the databases this manager holds open.
-spec list() -> [binary()].
list() ->
    gen_server:call(?SERVER, list, infinity).

%% @doc What the manager knows about an open database: pinned flag,
%% owner tag, and the options it was opened with.
-spec lookup(barrel:db_name()) ->
    {ok, #{pinned := boolean(), owner := term(), opts := map()}} |
    {error, not_open}.
lookup(Name) ->
    gen_server:call(?SERVER, {lookup, to_name(Name)}, infinity).

%% @doc Take exclusive file access to `Name': close it if this manager
%% holds it, then refuse every ensure until {@link unhold/1}. Refused
%% when the database is pinned or leased, tagged with another owner than
%% `owner' in `Opts', held already, or open outside this manager.
%% Returns whether it was open and with which options.
-spec hold(barrel:db_name(), map()) ->
    {ok, #{was_open := boolean(), opts := map()}} | {error, term()}.
hold(Name, Opts) when is_map(Opts) ->
    gen_server:call(?SERVER, {hold, to_name(Name), Opts}, infinity).

%% @doc End a {@link hold/2}. Idempotent.
-spec unhold(barrel:db_name()) -> ok.
unhold(Name) ->
    gen_server:call(?SERVER, {unhold, to_name(Name)}, infinity).

%% @doc Run one idle sweep synchronously (test hook; the periodic
%% timer calls the same code).
-spec sweep() -> ok.
sweep() ->
    gen_server:call(?SERVER, sweep, infinity).

%% @doc Run one idle sweep as if the monotonic clock read `NowMs'
%% (`erlang:monotonic_time(millisecond)'). Test hook.
-spec sweep(integer()) -> ok.
sweep(NowMs) when is_integer(NowMs) ->
    gen_server:call(?SERVER, {sweep, NowMs}, infinity).

%%====================================================================
%% gen_server
%%====================================================================

init([]) ->
    arm_sweep(),
    {ok, #state{}}.

handle_call(Req, From, State) ->
    do_call(Req, From, State).

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(sweep, State) ->
    State1 = do_sweep(State),
    arm_sweep(),
    {noreply, State1};
%% A cold open finished in its worker: install the handle (or report the
%% error) and answer every coalesced waiter.
handle_info({opened, Name, Result}, #state{opening = Opening} = State) ->
    case maps:take(Name, Opening) of
        {{_Pid, Waiters}, Opening1} ->
            State1 = State#state{opening = Opening1},
            case Result of
                {ok, Db} ->
                    %% the first-arrived waiter's owner tags the entry
                    {_From0, Opts0} = lists:last(Waiters),
                    State2 = insert(Name, Db, Opts0, State1),
                    _ = [gen_server:reply(F, {ok, Db}) || {F, _} <- Waiters],
                    {noreply, State2};
                {error, _} = Err ->
                    _ = [gen_server:reply(F, Err) || {F, _} <- Waiters],
                    {noreply, State1}
            end;
        error ->
            {noreply, State}
    end;
%% A name-targeting call deferred while its name was mid-open; retry it.
handle_info({retry_call, Req, From}, State) ->
    case do_call(Req, From, State) of
        {reply, Reply, State1} ->
            gen_server:reply(From, Reply),
            {noreply, State1};
        {noreply, State1} ->
            {noreply, State1}
    end;
%% A lease holder exited without releasing.
handle_info({'DOWN', Ref, process, _Pid, _Reason},
            #state{lease_mons = Mons} = State)
  when is_map_key(Ref, Mons) ->
    {noreply, drop_lease(Ref, State)};
%% Safety net: an open worker died without reporting (it traps its own
%% errors and always sends {opened,...}, so only an external kill lands
%% here). Fail the waiters rather than leave them blocked.
handle_info({'DOWN', _Ref, process, Pid, Reason},
            #state{opening = Opening} = State) ->
    case [{N, W} || {N, {P, W}} <- maps:to_list(Opening), P =:= Pid] of
        [{Name, Waiters}] ->
            _ = [gen_server:reply(F, {error, {open_crashed, Reason}})
                 || {F, _} <- Waiters],
            {noreply, State#state{opening = maps:remove(Name, Opening)}};
        [] ->
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

%%====================================================================
%% Call dispatch (also re-entered by {retry_call, ...})
%%====================================================================

do_call({ensure, Name, _Opts}, _From, #state{held = Held} = State)
        when is_map_key(Name, Held) ->
    {reply, {error, {held, maps:get(Name, Held)}}, State};
do_call({ensure, Name, Opts}, From, State) ->
    {noreply, ensure_async(Name, Opts, From, State)};
do_call({lookup, Name}, _From, #state{dbs = Dbs} = State) ->
    case maps:find(Name, Dbs) of
        {ok, #entry{pinned = P, owner = O, opts = Opts}} ->
            {reply, {ok, #{pinned => P, owner => O, opts => Opts}}, State};
        error ->
            {reply, {error, not_open}, State}
    end;
do_call({hold, Name, _Opts}, _From, #state{held = Held} = State)
        when is_map_key(Name, Held) ->
    {reply, {error, {held, maps:get(Name, Held)}}, State};
do_call({hold, Name, Opts} = Req, From, State) ->
    with_free_name(Name, Req, From, State,
        fun(S) -> do_hold(Name, maps:get(owner, Opts, undefined), S) end);
do_call({unhold, Name}, _From, #state{held = Held} = State) ->
    {reply, ok, State#state{held = maps:remove(Name, Held)}};
do_call({close, Name} = Req, From, State) ->
    with_free_name(Name, Req, From, State,
        fun(#state{dbs = Dbs} = S) ->
            case maps:take(Name, Dbs) of
                {#entry{db = Db}, Rest} ->
                    _ = barrel:close(Db),
                    {reply, ok, S#state{dbs = Rest}};
                error ->
                    {reply, ok, S}
            end
        end);
do_call({close_owned, Owner}, _From, #state{dbs = Dbs} = State) ->
    Owned = [{N, E} || {N, E} <- maps:to_list(Dbs),
                       E#entry.owner =:= Owner],
    Dbs1 = lists:foldl(
        fun({Name, #entry{db = Db}}, Acc) ->
            _ = try barrel:close(Db) catch _:_ -> ok end,
            maps:remove(Name, Acc)
        end, Dbs, Owned),
    {reply, ok, State#state{dbs = Dbs1}};
do_call({destroy, Name} = Req, From, State) ->
    with_free_name(Name, Req, From, State,
        fun(S) ->
            case do_ensure(Name, #{}, S) of
                {ok, Db, S1} ->
                    Rest = maps:remove(Name, S1#state.dbs),
                    Result = try barrel:delete(Db)
                             catch _:Reason -> {error, Reason}
                             end,
                    {reply, Result, S1#state{dbs = Rest}};
                {error, Reason, S1} ->
                    {reply, {error, Reason}, S1}
            end
        end);
do_call({branch, Parent, BranchName, Opts} = Req, From, State) ->
    %% The open targets Parent, so defer while Parent is mid-open.
    with_free_name(Parent, Req, From, State,
        fun(S) ->
            OpenOpts = maps:get(open_opts, Opts, #{}),
            BranchOpts = maps:without([open_opts], Opts),
            case do_ensure(Parent, OpenOpts, S) of
                {ok, ParentDb, S1} ->
                    case maybe_make_room(S1) of
                        {ok, S2} ->
                            case barrel:branch(ParentDb, BranchName,
                                               BranchOpts) of
                                {ok, BranchDb} ->
                                    {reply, {ok, BranchDb},
                                     insert(BranchName, BranchDb,
                                            OpenOpts, S2)};
                                {error, _} = Err ->
                                    {reply, Err, S2}
                            end;
                        {error, Reason} ->
                            {reply, {error, Reason}, S1}
                    end;
                {error, Reason, S1} ->
                    {reply, {error, Reason}, S1}
            end
        end);
do_call({pin, Name, Flag} = Req, From, State) ->
    with_free_name(Name, Req, From, State,
        fun(#state{dbs = Dbs} = S) ->
            case maps:find(Name, Dbs) of
                {ok, Entry} ->
                    Dbs1 = Dbs#{Name := Entry#entry{pinned = Flag}},
                    {reply, ok, S#state{dbs = Dbs1}};
                error ->
                    {reply, {error, not_open}, S}
            end
        end);
do_call({lease, Name, Pid} = Req, From, State) ->
    with_free_name(Name, Req, From, State,
        fun(#state{dbs = Dbs, lease_mons = Mons} = S) ->
            case maps:find(Name, Dbs) of
                {ok, #entry{db = Db, leases = N} = Entry} ->
                    Ref = erlang:monitor(process, Pid),
                    Entry1 = Entry#entry{leases = N + 1,
                                         last_used = now_ms()},
                    {reply, {ok, Db, Ref},
                     S#state{dbs = Dbs#{Name := Entry1},
                             lease_mons = Mons#{Ref => Name}}};
                error ->
                    {reply, {error, not_open}, S}
            end
        end);
do_call({release, Ref}, _From, State) ->
    erlang:demonitor(Ref, [flush]),
    {reply, ok, drop_lease(Ref, State)};
do_call(leases, _From, #state{dbs = Dbs} = State) ->
    {reply, maps:from_list([{N, L} || {N, #entry{leases = L}} <- maps:to_list(Dbs),
                                      L > 0]), State};
do_call(list, _From, #state{dbs = Dbs} = State) ->
    {reply, maps:keys(Dbs), State};
do_call(sweep, _From, State) ->
    {reply, ok, do_sweep(State)};
do_call({sweep, NowMs}, _From, State) ->
    {reply, ok, do_sweep(NowMs, State)}.

%% Run `Thunk(State)' now if `Name' is not mid-open; otherwise defer the
%% whole call until the open finishes, so no synchronous open can race the
%% in-flight one on the same database.
with_free_name(Name, Req, From, #state{opening = Opening} = State, Thunk) ->
    case maps:is_key(Name, Opening) of
        true ->
            erlang:send_after(?OPENING_RETRY_MS, self(),
                              {retry_call, Req, From}),
            {noreply, State};
        false ->
            Thunk(State)
    end.

%% Answer `From' from the cache, coalesce onto an in-flight open, or start
%% one in a worker. Always returns the new state; `From' is replied to via
%% gen_server:reply (now or on open completion).
ensure_async(Name, Opts, From, #state{dbs = Dbs, opening = Opening} = State) ->
    case maps:find(Name, Dbs) of
        {ok, #entry{db = Db} = Entry} ->
            case alive(Db) of
                true ->
                    gen_server:reply(From, {ok, Db}),
                    State#state{dbs =
                        Dbs#{Name := Entry#entry{last_used = now_ms()}}};
                false ->
                    %% docdb or store crashed: stop the stale handle, reopen
                    _ = try barrel:close(Db) catch _:_ -> ok end,
                    start_open(Name, Opts, From,
                               State#state{dbs = maps:remove(Name, Dbs)})
            end;
        error ->
            case maps:find(Name, Opening) of
                {ok, {Pid, Waiters}} ->
                    State#state{opening =
                        Opening#{Name => {Pid, [{From, Opts} | Waiters]}}};
                error ->
                    start_open(Name, Opts, From, State)
            end
    end.

start_open(Name, Opts, From, State) ->
    case maybe_make_room(State) of
        {ok, #state{opening = Opening} = State1} ->
            Server = self(),
            OpenOpts = open_opts(Opts),
            {Pid, _Ref} = spawn_monitor(fun() ->
                Result = try open_checked(Name, Opts, OpenOpts)
                         catch C:E -> {error, {C, E}} end,
                Server ! {opened, Name, Result}
            end),
            State1#state{opening = Opening#{Name => {Pid, [{From, Opts}]}}};
        {error, Reason} ->
            gen_server:reply(From, {error, Reason}),
            State
    end.

terminate(_Reason, #state{dbs = Dbs}) ->
    maps:foreach(
        fun(_Name, #entry{db = Db}) ->
            try barrel:close(Db) catch _:_ -> ok end
        end, Dbs),
    ok.

%%====================================================================
%% Internal
%%====================================================================

do_ensure(Name, _Opts, #state{held = Held} = State)
        when is_map_key(Name, Held) ->
    {error, {held, maps:get(Name, Held)}, State};
do_ensure(Name, Opts, #state{dbs = Dbs} = State) ->
    case maps:find(Name, Dbs) of
        {ok, #entry{db = Db} = Entry} ->
            case alive(Db) of
                true ->
                    Dbs1 = Dbs#{Name := Entry#entry{last_used = now_ms()}},
                    {ok, Db, State#state{dbs = Dbs1}};
                false ->
                    %% the docdb crashed without taking the manager down
                    %% (only the vector store is linked): stop the stale
                    %% handle first, which shuts the still-alive vector
                    %% store (barrel:close stops it before the dead docdb),
                    %% then reopen. Without this the store leaks its
                    %% RocksDB handles on every docdb crash.
                    _ = try barrel:close(Db) catch _:_ -> ok end,
                    reopen(Name, Opts,
                           State#state{dbs = maps:remove(Name, Dbs)})
            end;
        error ->
            reopen(Name, Opts, State)
    end.

reopen(Name, Opts, State) ->
    case maybe_make_room(State) of
        {ok, State1} ->
            case open_checked(Name, Opts, open_opts(Opts)) of
                {ok, Db} ->
                    {ok, Db, insert(Name, Db, Opts, State1)};
                {error, Reason} ->
                    {error, Reason, State1}
            end;
        {error, Reason} ->
            {error, Reason, State}
    end.

%% Strip the manager's own `owner' tag and force store_supervised: a
%% manager-owned store is tracked by name and must not be linked to the
%% (transient or loop) process that opens it.
open_opts(Opts) ->
    (maps:without([owner, must_exist], Opts))#{store_supervised => true}.

%% `must_exist => true' refuses to create a database that is not there.
open_checked(Name, #{must_exist := true} = Opts, OpenOpts) ->
    case barrel_docdb:db_exists(Name, maps:get(docdb, Opts, #{})) of
        true -> barrel:open(Name, OpenOpts);
        false -> {error, not_found}
    end;
open_checked(Name, _Opts, OpenOpts) ->
    barrel:open(Name, OpenOpts).

insert(Name, Db, Opts, #state{dbs = Dbs} = State) ->
    Entry = #entry{db = Db, last_used = now_ms(),
                   owner = maps:get(owner, Opts, undefined),
                   opts = maps:without([owner, must_exist], Opts)},
    State#state{dbs = Dbs#{Name => Entry}}.

drop_lease(Ref, #state{dbs = Dbs, lease_mons = Mons} = State) ->
    case maps:take(Ref, Mons) of
        {Name, Mons1} ->
            Dbs1 = case maps:find(Name, Dbs) of
                {ok, #entry{leases = N} = Entry} when N > 0 ->
                    Dbs#{Name := Entry#entry{leases = N - 1,
                                             last_used = now_ms()}};
                _ ->
                    Dbs
            end,
            State#state{dbs = Dbs1, lease_mons = Mons1};
        error ->
            State
    end.

in_use(#entry{pinned = true}) -> true;
in_use(#entry{leases = N}) -> N > 0.

%% Close `Name' for exclusive access unless something else relies on it.
do_hold(Name, Owner, #state{dbs = Dbs, held = Held} = State) ->
    case maps:find(Name, Dbs) of
        {ok, #entry{pinned = true}} ->
            {reply, {error, pinned}, State};
        {ok, #entry{leases = N}} when N > 0 ->
            {reply, {error, {leased, N}}, State};
        {ok, #entry{owner = O}} when O =/= undefined, O =/= Owner ->
            {reply, {error, {owned_by, O}}, State};
        {ok, #entry{db = Db, opts = Opts}} ->
            _ = barrel:close(Db),
            {reply, {ok, #{was_open => true, opts => Opts}},
             State#state{dbs = maps:remove(Name, Dbs),
                         held = Held#{Name => Owner}}};
        error ->
            case docdb_alive(Name) of
                true ->
                    {reply, {error, open_outside_manager}, State};
                false ->
                    {reply, {ok, #{was_open => false, opts => #{}}},
                     State#state{held = Held#{Name => Owner}}}
            end
    end.

%% The manager holds stores by name (never linked): a handle is live only
%% while both its docdb server and its supervised vector store are up.
alive(#{docdb := DbBin} = Db) ->
    docdb_alive(DbBin) andalso vstore_alive(Db).

docdb_alive(DbBin) ->
    case persistent_term:get({barrel_db, DbBin}, undefined) of
        Pid when is_pid(Pid) -> is_process_alive(Pid);
        _ -> false
    end.

vstore_alive(#{vstore := VBin}) ->
    barrel_vectordb_registry:whereis_name({vstore, VBin}) =/= undefined.

%% Enforce dbs_max_open before an open: evict the least recently used
%% unpinned entry idle for at least the guard, else refuse.
maybe_make_room(#state{dbs = Dbs} = State) ->
    MaxOpen = env(dbs_max_open, 0),
    case MaxOpen > 0 andalso maps:size(Dbs) >= MaxOpen of
        false ->
            {ok, State};
        true ->
            Guard = env(dbs_evict_guard, ?DEFAULT_EVICT_GUARD),
            Cutoff = now_ms() - Guard,
            Candidates = [{E#entry.last_used, N, E}
                          || {N, E} <- maps:to_list(Dbs),
                             not in_use(E),
                             E#entry.last_used =< Cutoff],
            case lists:sort(Candidates) of
                [{_, Name, #entry{db = Db}} | _] ->
                    _ = try barrel:close(Db) catch _:_ -> ok end,
                    {ok, State#state{dbs = maps:remove(Name, Dbs)}};
                [] ->
                    {error, too_many_open_dbs}
            end
    end.

do_sweep(State) ->
    do_sweep(now_ms(), State).

do_sweep(NowMs, #state{dbs = Dbs} = State) ->
    case env(dbs_idle_timeout, ?DEFAULT_IDLE_TIMEOUT) of
        0 ->
            State;
        IdleTimeout ->
            Cutoff = NowMs - IdleTimeout,
            Expired = [{N, E} || {N, E} <- maps:to_list(Dbs),
                                 not in_use(E),
                                 E#entry.last_used =< Cutoff],
            Dbs1 = lists:foldl(
                fun({Name, #entry{db = Db}}, Acc) ->
                    _ = try barrel:close(Db) catch _:_ -> ok end,
                    maps:remove(Name, Acc)
                end, Dbs, Expired),
            State#state{dbs = Dbs1}
    end.

arm_sweep() ->
    case env(dbs_idle_timeout, ?DEFAULT_IDLE_TIMEOUT) of
        0 ->
            %% re-check periodically so enabling the knob at runtime
            %% takes effect
            erlang:send_after(60000, self(), sweep);
        IdleTimeout ->
            Interval = max(1000, min(IdleTimeout div 4, 60000)),
            erlang:send_after(Interval, self(), sweep)
    end,
    ok.

env(Key, Default) ->
    application:get_env(barrel, Key, Default).

now_ms() ->
    erlang:monotonic_time(millisecond).

to_name(Name) when is_binary(Name) -> Name;
to_name(Name) when is_atom(Name) -> atom_to_binary(Name, utf8).
