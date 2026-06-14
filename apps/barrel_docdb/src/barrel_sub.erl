%%%-------------------------------------------------------------------
%%% @doc barrel_sub - Path subscription manager for barrel_docdb
%%%
%%% Manages MQTT-style path subscriptions for document changes.
%%% Subscribers receive notifications when documents matching their
%%% patterns are modified.
%%%
%%% Pattern examples:
%%% ```
%%% "users/+/profile" - matches users/123/profile, users/abc/profile
%%% "orders/#"        - matches orders/123, orders/123/items/1
%%% "config"          - matches exactly config
%%% '''
%%%
%%% Subscriptions are ephemeral - tied to subscriber process lifetime.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_sub).

-behaviour(gen_server).

-include("barrel_docdb.hrl").

%% API
-export([
    start_link/0,
    stop/0,
    subscribe/3,
    unsubscribe/2,
    match/2,
    list_subscriptions/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {
    %% Database name -> match_trie
    tries = #{} :: #{db_name() => match_trie:trie()},
    %% SubRef -> {DbName, Pattern, Pid}
    subs = #{} :: #{reference() => {db_name(), binary(), pid()}},
    %% Pid -> [SubRef] (for cleanup on process exit)
    by_pid = #{} :: #{pid() => [reference()]},
    %% {DbName, Pattern} -> [SubRef] (for finding subscribers)
    by_pattern = #{} :: #{{db_name(), binary()} => [reference()]},
    %% Pid -> MonitorRef (to track subscriber processes)
    monitors = #{} :: #{pid() => reference()}
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the subscription manager
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Stop the subscription manager
-spec stop() -> ok.
stop() ->
    gen_server:stop(?MODULE).

%% @doc Subscribe to document changes matching a pattern
%% Returns a subscription reference that can be used to unsubscribe
-spec subscribe(db_name(), binary(), pid()) -> {ok, reference()} | {error, term()}.
subscribe(DbName, Pattern, Pid) when is_binary(Pattern), is_pid(Pid) ->
    case match_trie:validate({filter, Pattern}) of
        true ->
            gen_server:call(?MODULE, {subscribe, DbName, Pattern, Pid});
        false ->
            {error, invalid_pattern}
    end.

%% @doc Unsubscribe using the subscription reference
-spec unsubscribe(reference(), pid()) -> ok.
unsubscribe(SubRef, Pid) when is_reference(SubRef), is_pid(Pid) ->
    gen_server:call(?MODULE, {unsubscribe, SubRef, Pid}).

%% @doc Find all subscriber PIDs matching the given paths.
%% Paths should be slash-separated binaries like `"users/123/name"'.
-spec match(db_name(), [binary()]) -> [pid()].
match(DbName, Paths) when is_list(Paths) ->
    gen_server:call(?MODULE, {match, DbName, Paths}).

%% @doc List all subscriptions for a database (for debugging)
-spec list_subscriptions(db_name()) -> [{reference(), binary(), pid()}].
list_subscriptions(DbName) ->
    gen_server:call(?MODULE, {list_subscriptions, DbName}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    {ok, #state{}}.

handle_call({subscribe, DbName, Pattern, Pid}, _From, State) ->
    {Reply, NewState} = do_subscribe(DbName, Pattern, Pid, State),
    {reply, Reply, NewState};

handle_call({unsubscribe, SubRef, Pid}, _From, State) ->
    NewState = do_unsubscribe(SubRef, Pid, State),
    {reply, ok, NewState};

handle_call({match, DbName, Paths}, _From, State) ->
    Pids = do_match(DbName, Paths, State),
    {reply, Pids, State};

handle_call({list_subscriptions, DbName}, _From, State) ->
    Subs = do_list_subscriptions(DbName, State),
    {reply, Subs, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _MonRef, process, Pid, _Reason}, State) ->
    %% Subscriber process died - cleanup all its subscriptions
    NewState = cleanup_subscriber(Pid, State),
    {noreply, NewState};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{tries = Tries}) ->
    %% Delete all tries
    maps:foreach(fun(_, Trie) -> match_trie:delete(Trie) end, Tries),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

do_subscribe(DbName, Pattern, Pid, State) ->
    #state{
        tries = Tries,
        subs = Subs,
        by_pid = ByPid,
        by_pattern = ByPattern,
        monitors = Monitors
    } = State,

    %% Get or create trie for this database
    Trie = case maps:get(DbName, Tries, undefined) of
        undefined ->
            match_trie:new(public);
        T ->
            T
    end,

    %% Insert pattern into trie
    ok = match_trie:insert(Trie, Pattern),

    %% Create subscription reference
    SubRef = make_ref(),

    %% Monitor subscriber if not already monitoring
    NewMonitors = case maps:is_key(Pid, Monitors) of
        true ->
            Monitors;
        false ->
            MonRef = erlang:monitor(process, Pid),
            maps:put(Pid, MonRef, Monitors)
    end,

    %% Update state
    NewTries = maps:put(DbName, Trie, Tries),
    NewSubs = maps:put(SubRef, {DbName, Pattern, Pid}, Subs),
    NewByPid = maps:update_with(Pid, fun(Refs) -> [SubRef | Refs] end, [SubRef], ByPid),
    PatternKey = {DbName, Pattern},
    NewByPattern = maps:update_with(PatternKey, fun(Refs) -> [SubRef | Refs] end, [SubRef], ByPattern),

    NewState = State#state{
        tries = NewTries,
        subs = NewSubs,
        by_pid = NewByPid,
        by_pattern = NewByPattern,
        monitors = NewMonitors
    },

    {{ok, SubRef}, NewState}.

do_unsubscribe(SubRef, Pid, State) ->
    #state{
        tries = Tries,
        subs = Subs,
        by_pid = ByPid,
        by_pattern = ByPattern,
        monitors = Monitors
    } = State,

    case maps:get(SubRef, Subs, undefined) of
        undefined ->
            State;
        {DbName, Pattern, SubPid} when SubPid =:= Pid ->
            %% Remove from subs
            NewSubs = maps:remove(SubRef, Subs),

            %% Remove from by_pid
            NewByPid = case maps:get(Pid, ByPid, []) of
                [SubRef] ->
                    %% Last subscription for this pid - remove monitor
                    maps:remove(Pid, ByPid);
                PidRefs ->
                    maps:put(Pid, lists:delete(SubRef, PidRefs), ByPid)
            end,

            %% Check if we should demonitor
            NewMonitors = case maps:get(Pid, NewByPid, undefined) of
                undefined ->
                    case maps:get(Pid, Monitors, undefined) of
                        undefined -> Monitors;
                        MonRef ->
                            erlang:demonitor(MonRef, [flush]),
                            maps:remove(Pid, Monitors)
                    end;
                _ ->
                    Monitors
            end,

            %% Remove from by_pattern
            PatternKey = {DbName, Pattern},
            NewByPattern = case maps:get(PatternKey, ByPattern, []) of
                [SubRef] ->
                    %% Last subscriber for this pattern - remove from trie
                    Trie = maps:get(DbName, Tries),
                    match_trie:delete(Trie, Pattern),
                    maps:remove(PatternKey, ByPattern);
                PatternRefs ->
                    maps:put(PatternKey, lists:delete(SubRef, PatternRefs), ByPattern)
            end,

            State#state{
                subs = NewSubs,
                by_pid = NewByPid,
                by_pattern = NewByPattern,
                monitors = NewMonitors
            };
        {_, _, _} ->
            %% Pid doesn't own this subscription
            State
    end.

do_match(DbName, Paths, #state{tries = Tries, by_pattern = ByPattern, subs = Subs}) ->
    case maps:get(DbName, Tries, undefined) of
        undefined ->
            [];
        Trie ->
            %% Find all matching patterns for each path
            MatchingPatterns = lists:foldl(
                fun(Path, Acc) ->
                    Patterns = match_trie:match(Trie, Path),
                    lists:usort(Patterns ++ Acc)
                end,
                [],
                Paths
            ),

            %% Collect unique pids from matching subscriptions
            MatchingPids = lists:foldl(
                fun(Pattern, PidAcc) ->
                    PatternKey = {DbName, Pattern},
                    SubRefs = maps:get(PatternKey, ByPattern, []),
                    lists:foldl(
                        fun(SubRef, Acc) ->
                            case maps:get(SubRef, Subs, undefined) of
                                {_, _, Pid} -> [Pid | Acc];
                                undefined -> Acc
                            end
                        end,
                        PidAcc,
                        SubRefs
                    )
                end,
                [],
                MatchingPatterns
            ),

            lists:usort(MatchingPids)
    end.

do_list_subscriptions(DbName, #state{subs = Subs}) ->
    [{SubRef, Pattern, Pid} || {SubRef, {Db, Pattern, Pid}} <- maps:to_list(Subs), Db =:= DbName].

cleanup_subscriber(Pid, State) ->
    #state{by_pid = ByPid, monitors = Monitors} = State,

    case maps:get(Pid, ByPid, []) of
        [] ->
            State;
        SubRefs ->
            %% Remove all subscriptions for this pid
            NewState = lists:foldl(
                fun(SubRef, AccState) ->
                    do_unsubscribe(SubRef, Pid, AccState)
                end,
                State,
                SubRefs
            ),
            %% Ensure monitor is removed
            NewMonitors = maps:remove(Pid, Monitors),
            NewState#state{monitors = NewMonitors, by_pid = maps:remove(Pid, ByPid)}
    end.
