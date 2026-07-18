%%%-------------------------------------------------------------------
%%% @doc The live-query bridge: owns every barrel:subscribe_query
%%% subscription created over MCP. The bridge process is the
%%% subscription owner, so a bridge crash tears all live queries down
%%% cleanly; databases with active subscriptions are pinned in the
%%% lifecycle manager so the idle sweeper cannot close them mid-query.
%%%
%%% Each subscription materializes its rows (keyed by document id)
%%% from the snapshot and the add/change/remove deltas. Deltas
%%% schedule a debounced notifications/resources/updated for the
%%% subscription's `barrel://db/{db}/live/{sub}' URI (URI only:
%%% clients re-read the resource). Sub ids are 16 random bytes
%%% (base64url): unguessable URIs are the capability, compensating
%%% arity-1 resource reads having no auth context.
%%%
%%% The framework never sweeps its subscription state when an MCP
%%% session expires, so the bridge sweeps itself: a periodic pass
%%% drops subscriptions whose MCP session is gone. Caps bound the
%%% damage of a runaway agent: per-session and global subscription
%%% counts come from `{barrel_server, mcp, #{live => #{...}}}'
%%% (max_per_session 32, max_global 1024, sweep_interval_ms 60000,
%%% debounce_ms 100).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_mcp_live).
-behaviour(gen_server).

-export([start_link/0,
         subscribe/4,
         unsubscribe/1,
         snapshot/1,
         sweep/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2]).

-record(sub, {
    id :: binary(),
    uri :: binary(),
    db :: binary(),
    session :: binary() | undefined,
    barrel_sub :: #{ref := reference(), pid := pid()},
    mref :: reference(),
    rows = #{} :: #{binary() | non_neg_integer() => map()},
    %% Cached id-sorted rows, so a snapshot poll does not re-sort the whole
    %% set in the bridge loop. Invalidated (undefined) on any row change.
    sorted :: [map()] | undefined,
    seq = 0 :: non_neg_integer(),
    ready = false :: boolean(),
    error :: undefined | binary(),
    pending = false :: boolean()
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Create a live query owned by the bridge. Returns the sub id
%% and its resource URI.
-spec subscribe(binary(), binary(), map(), binary() | undefined) ->
    {ok, binary(), binary()} | {error, term()}.
subscribe(DbName, Bql, Params, SessionId) ->
    %% Open the database in the caller's process, not the bridge loop: a
    %% cold or large open must not block every other subscribe/snapshot/
    %% unsubscribe call queued on the singleton bridge.
    case barrel_server_dbs:ensure(DbName) of
        {ok, Db} ->
            gen_server:call(?MODULE,
                            {subscribe, DbName, Db, Bql, Params, SessionId},
                            30000);
        {error, _} = Err ->
            Err
    end.

-spec unsubscribe(binary()) -> ok | {error, not_found}.
unsubscribe(SubId) ->
    gen_server:call(?MODULE, {unsubscribe, SubId}).

%% @doc The materialized state of a subscription: `#{ready, count,
%% rows}' (rows id-sorted), plus `error' after a failed query.
-spec snapshot(binary()) -> {ok, map()} | {error, not_found}.
snapshot(SubId) ->
    gen_server:call(?MODULE, {snapshot, SubId}).

%% @doc Test hook: run the session sweep now.
-spec sweep() -> {ok, non_neg_integer()}.
sweep() ->
    gen_server:call(?MODULE, sweep).

%%====================================================================
%% gen_server
%%====================================================================

%% @private
init([]) ->
    process_flag(trap_exit, true),
    arm_sweep(),
    {ok, #{subs => #{}, by_ref => #{}}}.

%% @private
handle_call({subscribe, DbName, Db, Bql, Params, SessionId}, _From,
            #{subs := Subs} = State) ->
    case check_caps(SessionId, Subs) of
        ok ->
            do_subscribe(DbName, Db, Bql, Params, SessionId, State);
        {error, _} = Err ->
            {reply, Err, State}
    end;
handle_call({unsubscribe, SubId}, _From, #{subs := Subs} = State) ->
    case maps:find(SubId, Subs) of
        {ok, Sub} ->
            {reply, ok, drop_sub(Sub, State)};
        error ->
            {reply, {error, not_found}, State}
    end;
handle_call({snapshot, SubId}, _From, #{subs := Subs} = State) ->
    case maps:find(SubId, Subs) of
        {ok, Sub} ->
            {Snap, Sub1} = snapshot_of(Sub),
            {reply, {ok, Snap}, put_sub(Sub1, State)};
        error ->
            {reply, {error, not_found}, State}
    end;
handle_call(sweep, _From, State) ->
    {Dropped, State1} = sweep_sessions(State),
    {reply, {ok, Dropped}, State1};
handle_call(_Req, _From, State) ->
    {reply, {error, unsupported}, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info({bql_rows, Ref, Rows}, State) ->
    {noreply, with_sub(Ref, State, fun(Sub) ->
        lists:foldl(fun add_row/2, Sub, Rows)
    end)};
handle_info({bql_ready, Ref, _Meta}, State) ->
    {noreply, with_sub(Ref, State, fun(Sub) ->
        schedule_notify(Sub#sub{ready = true})
    end)};
handle_info({bql_change, Ref, Change}, State) ->
    {noreply, with_sub(Ref, State, fun(Sub) ->
        schedule_notify(apply_change(Change, Sub))
    end)};
handle_info({bql_error, Ref, Reason}, State) ->
    {noreply, with_sub(Ref, State, fun(Sub) ->
        schedule_notify(Sub#sub{ready = false, error = err_bin(Reason)})
    end)};
handle_info({notify, SubId}, #{subs := Subs} = State) ->
    case maps:find(SubId, Subs) of
        {ok, #sub{uri = Uri} = Sub} ->
            ok = barrel_mcp:notify_resource_updated(Uri),
            {noreply, put_sub(Sub#sub{pending = false}, State)};
        error ->
            {noreply, State}
    end;
handle_info({'DOWN', MRef, process, _Pid, Reason},
            #{subs := Subs} = State) ->
    %% a live query process died without a bql_error (kill, db close)
    Dead = [Sub || Sub = #sub{mref = M} <- maps:values(Subs), M =:= MRef],
    case Dead of
        [Sub] ->
            {noreply, put_sub(
                schedule_notify(Sub#sub{ready = false,
                                        error = err_bin(Reason)}),
                State)};
        [] ->
            {noreply, State}
    end;
handle_info(sweep, State) ->
    {_Dropped, State1} = sweep_sessions(State),
    arm_sweep(),
    {noreply, State1};
handle_info(_Msg, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, #{subs := Subs}) ->
    %% tear every live query down and release the pins
    lists:foreach(
        fun(#sub{barrel_sub = BSub, db = Db}) ->
            _ = barrel:unsubscribe_query(BSub),
            _ = barrel_dbs:unpin(Db)
        end, maps:values(Subs)),
    ok.

%%====================================================================
%% Subscribe / drop
%%====================================================================

do_subscribe(DbName, Db, Bql, Params, SessionId, State) ->
    %% The database is already open (ensured by the caller). subscribe_query
    %% owns the bridge as the row owner, so it must run here.
    SubOpts = #{params => Params, owner => self()},
    case barrel:subscribe_query(Db, Bql, SubOpts) of
        {ok, #{ref := Ref, pid := Pid} = BSub} ->
            _ = barrel_dbs:pin(DbName),
            SubId = new_sub_id(),
            Uri = live_uri(DbName, SubId),
            Sub = #sub{
                id = SubId,
                uri = Uri,
                db = DbName,
                session = SessionId,
                barrel_sub = BSub,
                mref = erlang:monitor(process, Pid)
            },
            #{subs := Subs, by_ref := ByRef} = State,
            State1 = State#{
                subs => Subs#{SubId => Sub},
                by_ref => ByRef#{Ref => SubId}
            },
            {reply, {ok, SubId, Uri}, State1};
        {error, _} = Err ->
            {reply, Err, State}
    end.

drop_sub(#sub{id = SubId, db = Db, barrel_sub = #{ref := Ref} = BSub,
              mref = MRef},
         #{subs := Subs, by_ref := ByRef} = State) ->
    erlang:demonitor(MRef, [flush]),
    _ = barrel:unsubscribe_query(BSub),
    Subs1 = maps:remove(SubId, Subs),
    case lists:any(fun(#sub{db = D}) -> D =:= Db end,
                   maps:values(Subs1)) of
        true -> ok;
        false -> _ = barrel_dbs:unpin(Db), ok
    end,
    State#{subs => Subs1, by_ref => maps:remove(Ref, ByRef)}.

check_caps(SessionId, Subs) ->
    Cfg = live_config(),
    MaxGlobal = maps:get(max_global, Cfg, 1024),
    MaxPerSession = maps:get(max_per_session, Cfg, 32),
    Global = map_size(Subs),
    Mine = length([S || S = #sub{session = Sid} <- maps:values(Subs),
                        Sid =:= SessionId, Sid =/= undefined]),
    if
        Global >= MaxGlobal -> {error, too_many_live_queries};
        SessionId =/= undefined, Mine >= MaxPerSession ->
            {error, too_many_live_queries};
        true -> ok
    end.

%%====================================================================
%% Row materialization
%%====================================================================

with_sub(Ref, #{subs := Subs, by_ref := ByRef} = State, Fun) ->
    case maps:find(Ref, ByRef) of
        {ok, SubId} ->
            case maps:find(SubId, Subs) of
                {ok, Sub} -> put_sub(Fun(Sub), State);
                error -> State
            end;
        error ->
            State
    end.

put_sub(#sub{id = SubId} = Sub, #{subs := Subs} = State) ->
    State#{subs => Subs#{SubId => Sub}}.

%% snapshot rows are keyed by document id; the rare projection
%% without one falls back to an arrival index
add_row(Row, #sub{rows = Rows, seq = Seq} = Sub) ->
    case Row of
        #{<<"id">> := Id} ->
            Sub#sub{rows = Rows#{Id => Row}, sorted = undefined};
        _ ->
            Sub#sub{rows = Rows#{Seq => Row}, seq = Seq + 1,
                    sorted = undefined}
    end.

apply_change(#{action := remove, id := Id}, #sub{rows = Rows} = Sub) ->
    Sub#sub{rows = maps:remove(Id, Rows), sorted = undefined};
apply_change(#{id := Id, row := Row}, #sub{rows = Rows} = Sub) ->
    Sub#sub{rows = Rows#{Id => Row}, sorted = undefined};
apply_change(_Change, Sub) ->
    Sub.

%% Returns the snapshot and the sub with its sorted-rows cache filled, so a
%% repeated poll with no row changes reuses the sort.
snapshot_of(#sub{ready = Ready, error = Error} = Sub) ->
    {Sorted, Sub1} = sorted_rows(Sub),
    Base = #{ready => Ready, count => length(Sorted), rows => Sorted},
    Snap = case Error of
        undefined -> Base;
        _ -> Base#{error => Error}
    end,
    {Snap, Sub1}.

sorted_rows(#sub{sorted = Sorted} = Sub) when Sorted =/= undefined ->
    {Sorted, Sub};
sorted_rows(#sub{rows = Rows} = Sub) ->
    Sorted = [Row || {_K, Row} <- lists:keysort(1, maps:to_list(Rows))],
    {Sorted, Sub#sub{sorted = Sorted}}.

%%====================================================================
%% Debounce and sweep
%%====================================================================

schedule_notify(#sub{pending = true} = Sub) ->
    Sub;
schedule_notify(#sub{id = SubId} = Sub) ->
    Debounce = maps:get(debounce_ms, live_config(), 100),
    erlang:send_after(Debounce, self(), {notify, SubId}),
    Sub#sub{pending = true}.

sweep_sessions(#{subs := Subs} = State) ->
    Dead = [Sub || Sub = #sub{session = Sid} <- maps:values(Subs),
                   Sid =/= undefined, not session_alive(Sid)],
    State1 = lists:foldl(fun drop_sub/2, State, Dead),
    {length(Dead), State1}.

session_alive(SessionId) ->
    case barrel_mcp_session:get(SessionId) of
        {ok, _} -> true;
        {error, not_found} -> false
    end.

arm_sweep() ->
    Interval = maps:get(sweep_interval_ms, live_config(), 60000),
    erlang:send_after(Interval, self(), sweep).

%%====================================================================
%% Internal
%%====================================================================

live_config() ->
    Cfg = application:get_env(barrel_server, mcp, #{}),
    maps:get(live, Cfg, #{}).

new_sub_id() ->
    base64:encode(crypto:strong_rand_bytes(16), #{mode => urlsafe,
                                                  padding => false}).

live_uri(DbName, SubId) ->
    <<"barrel://db/", DbName/binary, "/live/", SubId/binary>>.

err_bin(Reason) when is_atom(Reason) ->
    atom_to_binary(Reason, utf8);
err_bin(Reason) ->
    iolist_to_binary(io_lib:format("~p", [Reason])).
