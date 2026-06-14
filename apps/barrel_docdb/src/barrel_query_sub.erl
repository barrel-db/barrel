%%%-------------------------------------------------------------------
%%% @doc barrel_query_sub - Query-based subscription manager
%%%
%%% Manages query-based subscriptions for document changes.
%%% Subscribers receive notifications when documents matching their
%%% queries are modified.
%%%
%%% Uses path optimization: only evaluates the full query when a
%%% change affects paths referenced by the query.
%%%
%%% Example:
%%% ```
%%% Query = #{where => [{path, [<<"type">>], <<"user">>}]},
%%% {ok, SubRef} = barrel_query_sub:subscribe(DbRef, Query, self()),
%%% receive
%%%     {barrel_query_change, DbName, #{id := DocId}} -> ok
%%% end,
%%% ok = barrel_query_sub:unsubscribe(SubRef)
%%% '''
%%%
%%% Subscriptions are ephemeral - tied to subscriber process lifetime.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_query_sub).

-behaviour(gen_server).

-include("barrel_docdb.hrl").

%% API
-export([
    start_link/0,
    stop/0,
    subscribe/3,
    unsubscribe/1,
    notify_change/4
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

-record(query_sub, {
    ref :: reference(),
    db_name :: db_name(),
    query_plan :: barrel_query:query_plan(),
    path_patterns :: [binary()],  % Extracted paths for pre-filtering
    pid :: pid()
}).

-record(state, {
    %% SubRef -> #query_sub{}
    subs = #{} :: #{reference() => #query_sub{}},
    %% Pid -> [SubRef] (for cleanup on process exit)
    by_pid = #{} :: #{pid() => [reference()]},
    %% DbName -> [SubRef] (for fast lookup by database)
    by_db = #{} :: #{db_name() => [reference()]},
    %% Pid -> MonitorRef
    monitors = #{} :: #{pid() => reference()},
    %% Per-database match_trie for path pattern matching
    tries = #{} :: #{db_name() => match_trie:trie()}
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the query subscription manager
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Stop the query subscription manager
-spec stop() -> ok.
stop() ->
    gen_server:stop(?MODULE).

%% @doc Subscribe to document changes matching a query
%% Returns a subscription reference that can be used to unsubscribe.
%% The query must be a valid barrel_query specification.
-spec subscribe(db_name(), barrel_query:query_spec(), pid()) ->
    {ok, reference()} | {error, term()}.
subscribe(DbName, QuerySpec, Pid) when is_map(QuerySpec), is_pid(Pid) ->
    case barrel_query:compile(QuerySpec) of
        {ok, QueryPlan} ->
            gen_server:call(?MODULE, {subscribe, DbName, QueryPlan, Pid});
        {error, _} = Error ->
            Error
    end.

%% @doc Unsubscribe using the subscription reference
-spec unsubscribe(reference()) -> ok.
unsubscribe(SubRef) when is_reference(SubRef) ->
    gen_server:call(?MODULE, {unsubscribe, SubRef}).

%% @doc Notify query subscribers about a document change
%% Called by barrel_db_server after document modifications.
%% Only evaluates queries whose path patterns match the changed paths.
-spec notify_change(db_name(), docid(), binary(), map()) -> ok.
notify_change(DbName, DocId, Rev, DocBody) ->
    gen_server:cast(?MODULE, {notify_change, DbName, DocId, Rev, DocBody}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    {ok, #state{}}.

handle_call({subscribe, DbName, QueryPlan, Pid}, _From, State) ->
    {Reply, NewState} = do_subscribe(DbName, QueryPlan, Pid, State),
    {reply, Reply, NewState};

handle_call({unsubscribe, SubRef}, _From, State) ->
    NewState = do_unsubscribe(SubRef, State),
    {reply, ok, NewState};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({notify_change, DbName, DocId, Rev, DocBody}, State) ->
    do_notify_change(DbName, DocId, Rev, DocBody, State),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _MonRef, process, Pid, _Reason}, State) ->
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

do_subscribe(DbName, QueryPlan, Pid, State) ->
    #state{
        subs = Subs,
        by_pid = ByPid,
        by_db = ByDb,
        monitors = Monitors,
        tries = Tries
    } = State,

    %% Extract path patterns from query for optimization
    PathPatterns = barrel_query:extract_paths(QueryPlan),

    %% Create subscription reference
    SubRef = make_ref(),

    %% Create subscription record
    QuerySub = #query_sub{
        ref = SubRef,
        db_name = DbName,
        query_plan = QueryPlan,
        path_patterns = PathPatterns,
        pid = Pid
    },

    %% Monitor subscriber if not already monitoring
    NewMonitors = case maps:is_key(Pid, Monitors) of
        true ->
            Monitors;
        false ->
            MonRef = erlang:monitor(process, Pid),
            maps:put(Pid, MonRef, Monitors)
    end,

    %% Get or create trie for this database and insert patterns
    Trie = case maps:get(DbName, Tries, undefined) of
        undefined ->
            match_trie:new(public);
        T ->
            T
    end,

    %% Insert all path patterns into the trie
    lists:foreach(fun(Pattern) ->
        match_trie:insert(Trie, Pattern)
    end, PathPatterns),

    %% Update state
    NewSubs = maps:put(SubRef, QuerySub, Subs),
    NewByPid = maps:update_with(Pid, fun(Refs) -> [SubRef | Refs] end, [SubRef], ByPid),
    NewByDb = maps:update_with(DbName, fun(Refs) -> [SubRef | Refs] end, [SubRef], ByDb),
    NewTries = maps:put(DbName, Trie, Tries),

    NewState = State#state{
        subs = NewSubs,
        by_pid = NewByPid,
        by_db = NewByDb,
        monitors = NewMonitors,
        tries = NewTries
    },

    {{ok, SubRef}, NewState}.

do_unsubscribe(SubRef, State) ->
    #state{
        subs = Subs,
        by_pid = ByPid,
        by_db = ByDb,
        monitors = Monitors,
        tries = Tries
    } = State,

    case maps:get(SubRef, Subs, undefined) of
        undefined ->
            State;
        #query_sub{db_name = DbName, path_patterns = PathPatterns, pid = Pid} ->
            %% Remove from subs
            NewSubs = maps:remove(SubRef, Subs),

            %% Remove from by_pid
            NewByPid = case maps:get(Pid, ByPid, []) of
                [SubRef] ->
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

            %% Remove from by_db
            NewByDb = case maps:get(DbName, ByDb, []) of
                [SubRef] ->
                    maps:remove(DbName, ByDb);
                DbRefs ->
                    maps:put(DbName, lists:delete(SubRef, DbRefs), ByDb)
            end,

            %% Remove path patterns from trie (if no other subs use them)
            NewTries = case maps:get(DbName, NewByDb, []) of
                [] ->
                    %% No more subscriptions for this DB, delete trie
                    case maps:get(DbName, Tries, undefined) of
                        undefined -> Tries;
                        Trie ->
                            match_trie:delete(Trie),
                            maps:remove(DbName, Tries)
                    end;
                _ ->
                    %% Other subscriptions exist, remove our patterns
                    Trie = maps:get(DbName, Tries),
                    lists:foreach(fun(Pattern) ->
                        %% Check if any other sub uses this pattern
                        OtherRefs = maps:get(DbName, NewByDb, []),
                        UsedByOther = lists:any(fun(ORef) ->
                            case maps:get(ORef, NewSubs, undefined) of
                                undefined -> false;
                                #query_sub{path_patterns = OPatterns} ->
                                    lists:member(Pattern, OPatterns)
                            end
                        end, OtherRefs),
                        case UsedByOther of
                            false ->
                                match_trie:delete(Trie, Pattern);
                            true ->
                                ok
                        end
                    end, PathPatterns),
                    Tries
            end,

            State#state{
                subs = NewSubs,
                by_pid = NewByPid,
                by_db = NewByDb,
                monitors = NewMonitors,
                tries = NewTries
            }
    end.

do_notify_change(DbName, DocId, Rev, DocBody, State) ->
    #state{subs = Subs, by_db = ByDb, tries = Tries} = State,

    %% Check if any subscriptions for this database
    case maps:get(DbName, ByDb, []) of
        [] ->
            ok;
        SubRefs ->
            %% Get topics from document paths
            Paths = barrel_ars:analyze(DocBody),
            Topics = barrel_ars:paths_to_topics(Paths),

            %% Check which patterns match using trie
            Trie = maps:get(DbName, Tries),
            MatchingPatterns = lists:foldl(
                fun(Topic, Acc) ->
                    Patterns = match_trie:match(Trie, Topic),
                    lists:usort(Patterns ++ Acc)
                end,
                [],
                Topics
            ),

            %% For each matching pattern, find subscriptions and evaluate query
            lists:foreach(fun(SubRef) ->
                case maps:get(SubRef, Subs, undefined) of
                    undefined ->
                        ok;
                    #query_sub{path_patterns = SubPatterns, query_plan = QueryPlan, pid = Pid} ->
                        %% Check if any of the subscription's patterns matched
                        case lists:any(fun(P) -> lists:member(P, MatchingPatterns) end, SubPatterns) of
                            false ->
                                ok;
                            true ->
                                %% Evaluate the full query against the document
                                case evaluate_query(QueryPlan, DocBody) of
                                    true ->
                                        %% Document matches query, send notification
                                        Notification = {barrel_query_change, DbName, #{
                                            id => DocId,
                                            rev => Rev
                                        }},
                                        Pid ! Notification;
                                    false ->
                                        ok
                                end
                        end
                end
            end, SubRefs)
    end.

%% @private Evaluate a compiled query against a document
evaluate_query(QueryPlan, DocBody) ->
    Conditions = element(2, QueryPlan),  % #query_plan.conditions
    Bindings = element(3, QueryPlan),    % #query_plan.bindings
    case matches_conditions(DocBody, Conditions, Bindings, #{}) of
        {true, _} -> true;
        false -> false
    end.

%% @private Check if a document matches conditions
%% (Simplified version of barrel_query:matches_conditions for in-memory eval)
matches_conditions(_Doc, [], _Bindings, BoundVars) ->
    {true, BoundVars};
matches_conditions(Doc, [Condition | Rest], Bindings, BoundVars) ->
    case match_condition(Doc, Condition, Bindings, BoundVars) of
        {true, NewBoundVars} ->
            matches_conditions(Doc, Rest, Bindings, NewBoundVars);
        false ->
            false
    end.

match_condition(Doc, {path, Path, Value}, _Bindings, BoundVars) ->
    case get_path_value(Doc, Path) of
        {ok, DocValue} ->
            case is_logic_var(Value) of
                true ->
                    {true, BoundVars#{Value => DocValue}};
                false ->
                    case DocValue =:= Value of
                        true -> {true, BoundVars};
                        false -> false
                    end
            end;
        not_found ->
            false
    end;

match_condition(Doc, {compare, Path, Op, Value}, _Bindings, BoundVars) ->
    case get_path_value(Doc, Path) of
        {ok, DocValue} ->
            CompareValue = case is_logic_var(Value) of
                true -> maps:get(Value, BoundVars, undefined);
                false -> Value
            end,
            case compare_values(DocValue, Op, CompareValue) of
                true -> {true, BoundVars};
                false -> false
            end;
        not_found ->
            false
    end;

match_condition(Doc, {'and', Conditions}, Bindings, BoundVars) ->
    matches_conditions(Doc, Conditions, Bindings, BoundVars);

match_condition(Doc, {'or', Conditions}, Bindings, BoundVars) ->
    match_any(Doc, Conditions, Bindings, BoundVars);

match_condition(Doc, {'not', Condition}, Bindings, BoundVars) ->
    case match_condition(Doc, Condition, Bindings, BoundVars) of
        {true, _} -> false;
        false -> {true, BoundVars}
    end;

match_condition(Doc, {in, Path, Values}, _Bindings, BoundVars) ->
    case get_path_value(Doc, Path) of
        {ok, DocValue} ->
            case lists:member(DocValue, Values) of
                true -> {true, BoundVars};
                false -> false
            end;
        not_found ->
            false
    end;

match_condition(Doc, {contains, Path, Value}, _Bindings, BoundVars) ->
    case get_path_value(Doc, Path) of
        {ok, DocValue} when is_list(DocValue) ->
            case lists:member(Value, DocValue) of
                true -> {true, BoundVars};
                false -> false
            end;
        _ ->
            false
    end;

match_condition(Doc, {exists, Path}, _Bindings, BoundVars) ->
    case get_path_value(Doc, Path) of
        {ok, _} -> {true, BoundVars};
        not_found -> false
    end;

match_condition(Doc, {missing, Path}, _Bindings, BoundVars) ->
    case get_path_value(Doc, Path) of
        {ok, _} -> false;
        not_found -> {true, BoundVars}
    end;

match_condition(Doc, {regex, Path, Pattern}, _Bindings, BoundVars) ->
    case get_path_value(Doc, Path) of
        {ok, DocValue} when is_binary(DocValue) ->
            case re:run(DocValue, Pattern) of
                {match, _} -> {true, BoundVars};
                nomatch -> false
            end;
        _ ->
            false
    end;

match_condition(Doc, {prefix, Path, Prefix}, _Bindings, BoundVars) ->
    case get_path_value(Doc, Path) of
        {ok, DocValue} when is_binary(DocValue) ->
            PrefixLen = byte_size(Prefix),
            case DocValue of
                <<Prefix:PrefixLen/binary, _/binary>> -> {true, BoundVars};
                _ -> false
            end;
        _ ->
            false
    end;

match_condition(_Doc, _, _Bindings, _BoundVars) ->
    false.

match_any(_Doc, [], _Bindings, _BoundVars) ->
    false;
match_any(Doc, [Condition | Rest], Bindings, BoundVars) ->
    case match_condition(Doc, Condition, Bindings, BoundVars) of
        {true, NewBoundVars} -> {true, NewBoundVars};
        false -> match_any(Doc, Rest, Bindings, BoundVars)
    end.

%% @private Get a value from a document at the given path
get_path_value(Doc, []) ->
    {ok, Doc};
get_path_value(Doc, [Key | Rest]) when is_map(Doc), is_binary(Key) ->
    case maps:find(Key, Doc) of
        {ok, Value} -> get_path_value(Value, Rest);
        error -> not_found
    end;
get_path_value(Doc, [Index | Rest]) when is_list(Doc), is_integer(Index) ->
    case Index < length(Doc) of
        true ->
            Value = lists:nth(Index + 1, Doc),
            get_path_value(Value, Rest);
        false ->
            not_found
    end;
get_path_value(_, _) ->
    not_found.

%% @private Check if a value is a logic variable
is_logic_var(Atom) when is_atom(Atom) ->
    case atom_to_list(Atom) of
        [$? | _] -> true;
        _ -> false
    end;
is_logic_var(_) ->
    false.

%% @private Compare two values
compare_values(A, '>', B) when is_number(A), is_number(B) -> A > B;
compare_values(A, '<', B) when is_number(A), is_number(B) -> A < B;
compare_values(A, '>=', B) when is_number(A), is_number(B) -> A >= B;
compare_values(A, '=<', B) when is_number(A), is_number(B) -> A =< B;
compare_values(A, '=/=', B) -> A =/= B;
compare_values(A, '==', B) -> A =:= B;
compare_values(A, '>', B) when is_binary(A), is_binary(B) -> A > B;
compare_values(A, '<', B) when is_binary(A), is_binary(B) -> A < B;
compare_values(A, '>=', B) when is_binary(A), is_binary(B) -> A >= B;
compare_values(A, '=<', B) when is_binary(A), is_binary(B) -> A =< B;
compare_values(_, _, _) -> false.

cleanup_subscriber(Pid, State) ->
    #state{by_pid = ByPid, monitors = Monitors} = State,

    case maps:get(Pid, ByPid, []) of
        [] ->
            State;
        SubRefs ->
            NewState = lists:foldl(
                fun(SubRef, AccState) ->
                    do_unsubscribe(SubRef, AccState)
                end,
                State,
                SubRefs
            ),
            NewMonitors = maps:remove(Pid, Monitors),
            NewState#state{monitors = NewMonitors, by_pid = maps:remove(Pid, ByPid)}
    end.
