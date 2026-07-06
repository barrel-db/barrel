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
%%% The manager does not trap exits: if an owned store crashes, the
%%% manager crashes with it, the supervisor restarts it with an empty
%%% cache, and databases reopen on the next ensure.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_dbs).
-behaviour(gen_server).

-export([start_link/0,
         ensure/1, ensure/2,
         close/1, close_owned/1, destroy/1,
         branch/3,
         pin/1, unpin/1,
         list/0,
         sweep/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2]).

-define(SERVER, ?MODULE).
-define(DEFAULT_IDLE_TIMEOUT, 300000).
-define(DEFAULT_EVICT_GUARD, 5000).

-record(entry, {
    db :: barrel:db(),
    last_used :: integer(),  %% monotonic ms
    pinned = false :: boolean(),
    %% opaque tag from the ensure opts (`owner => Term'): lets a
    %% subsystem close exactly the entries it opened (see close_owned/1)
    owner :: term()
}).

-record(state, {dbs = #{} :: #{binary() => #entry{}}}).

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
%% to {@link barrel:open/2}.
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

%% @doc Names of the databases this manager holds open.
-spec list() -> [binary()].
list() ->
    gen_server:call(?SERVER, list, infinity).

%% @doc Run one idle sweep synchronously (test hook; the periodic
%% timer calls the same code).
-spec sweep() -> ok.
sweep() ->
    gen_server:call(?SERVER, sweep, infinity).

%%====================================================================
%% gen_server
%%====================================================================

init([]) ->
    arm_sweep(),
    {ok, #state{}}.

handle_call({ensure, Name, Opts}, _From, State) ->
    case do_ensure(Name, Opts, State) of
        {ok, Db, State1} -> {reply, {ok, Db}, State1};
        {error, Reason, State1} -> {reply, {error, Reason}, State1}
    end;
handle_call({close, Name}, _From, #state{dbs = Dbs} = State) ->
    case maps:take(Name, Dbs) of
        {#entry{db = Db}, Rest} ->
            _ = barrel:close(Db),
            {reply, ok, State#state{dbs = Rest}};
        error ->
            {reply, ok, State}
    end;
handle_call({close_owned, Owner}, _From, #state{dbs = Dbs} = State) ->
    Owned = [{N, E} || {N, E} <- maps:to_list(Dbs),
                       E#entry.owner =:= Owner],
    Dbs1 = lists:foldl(
        fun({Name, #entry{db = Db}}, Acc) ->
            _ = try barrel:close(Db) catch _:_ -> ok end,
            maps:remove(Name, Acc)
        end, Dbs, Owned),
    {reply, ok, State#state{dbs = Dbs1}};
handle_call({destroy, Name}, _From, State) ->
    case do_ensure(Name, #{}, State) of
        {ok, Db, State1} ->
            Rest = maps:remove(Name, State1#state.dbs),
            Result = try barrel:delete(Db)
                     catch _:Reason -> {error, Reason}
                     end,
            {reply, Result, State1#state{dbs = Rest}};
        {error, Reason, State1} ->
            {reply, {error, Reason}, State1}
    end;
handle_call({branch, Parent, BranchName, Opts}, _From, State) ->
    OpenOpts = maps:get(open_opts, Opts, #{}),
    BranchOpts = maps:without([open_opts], Opts),
    Owner = maps:get(owner, OpenOpts, undefined),
    case do_ensure(Parent, OpenOpts, State) of
        {ok, ParentDb, State1} ->
            case maybe_make_room(State1) of
                {ok, State2} ->
                    case barrel:branch(ParentDb, BranchName, BranchOpts) of
                        {ok, BranchDb} ->
                            {reply, {ok, BranchDb},
                             insert(BranchName, BranchDb, Owner, State2)};
                        {error, _} = Err ->
                            {reply, Err, State2}
                    end;
                {error, Reason} ->
                    {reply, {error, Reason}, State1}
            end;
        {error, Reason, State1} ->
            {reply, {error, Reason}, State1}
    end;
handle_call({pin, Name, Flag}, _From, #state{dbs = Dbs} = State) ->
    case maps:find(Name, Dbs) of
        {ok, Entry} ->
            Dbs1 = Dbs#{Name := Entry#entry{pinned = Flag}},
            {reply, ok, State#state{dbs = Dbs1}};
        error ->
            {reply, {error, not_open}, State}
    end;
handle_call(list, _From, #state{dbs = Dbs} = State) ->
    {reply, maps:keys(Dbs), State};
handle_call(sweep, _From, State) ->
    {reply, ok, do_sweep(State)};
handle_call(_Req, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(sweep, State) ->
    State1 = do_sweep(State),
    arm_sweep(),
    {noreply, State1};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{dbs = Dbs}) ->
    maps:foreach(
        fun(_Name, #entry{db = Db}) ->
            try barrel:close(Db) catch _:_ -> ok end
        end, Dbs),
    ok.

%%====================================================================
%% Internal
%%====================================================================

do_ensure(Name, Opts, #state{dbs = Dbs} = State) ->
    case maps:find(Name, Dbs) of
        {ok, #entry{db = Db} = Entry} ->
            case alive(Db) of
                true ->
                    Dbs1 = Dbs#{Name := Entry#entry{last_used = now_ms()}},
                    {ok, Db, State#state{dbs = Dbs1}};
                false ->
                    %% the docdb crashed without taking the manager
                    %% down (only the vector store is linked): drop
                    %% the stale handle and reopen
                    reopen(Name, Opts,
                           State#state{dbs = maps:remove(Name, Dbs)})
            end;
        error ->
            reopen(Name, Opts, State)
    end.

reopen(Name, Opts, State) ->
    Owner = maps:get(owner, Opts, undefined),
    OpenOpts = maps:without([owner], Opts),
    case maybe_make_room(State) of
        {ok, State1} ->
            case barrel:open(Name, OpenOpts) of
                {ok, Db} ->
                    {ok, Db, insert(Name, Db, Owner, State1)};
                {error, Reason} ->
                    {error, Reason, State1}
            end;
        {error, Reason} ->
            {error, Reason, State}
    end.

insert(Name, Db, Owner, #state{dbs = Dbs} = State) ->
    Entry = #entry{db = Db, last_used = now_ms(), owner = Owner},
    State#state{dbs = Dbs#{Name => Entry}}.

alive(#{docdb := DbBin}) ->
    case persistent_term:get({barrel_db, DbBin}, undefined) of
        Pid when is_pid(Pid) -> is_process_alive(Pid);
        _ -> false
    end.

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
                             not E#entry.pinned,
                             E#entry.last_used =< Cutoff],
            case lists:sort(Candidates) of
                [{_, Name, #entry{db = Db}} | _] ->
                    _ = try barrel:close(Db) catch _:_ -> ok end,
                    {ok, State#state{dbs = maps:remove(Name, Dbs)}};
                [] ->
                    {error, too_many_open_dbs}
            end
    end.

do_sweep(#state{dbs = Dbs} = State) ->
    case env(dbs_idle_timeout, ?DEFAULT_IDLE_TIMEOUT) of
        0 ->
            State;
        IdleTimeout ->
            Cutoff = now_ms() - IdleTimeout,
            Expired = [{N, E} || {N, E} <- maps:to_list(Dbs),
                                 not E#entry.pinned,
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
