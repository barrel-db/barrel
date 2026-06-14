%%%-------------------------------------------------------------------
%%% @doc Parallel execution utilities for barrel_docdb
%%%
%%% Provides bounded parallel map operations using a static worker pool.
%%% Workers are pre-spawned and reused, eliminating per-operation spawn overhead.
%%% Similar to PostgreSQL's background worker pool approach.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_parallel).

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    start_link/1,
    stop/0,
    pmap/2,
    pmap/3,
    pfiltermap/2,
    pfiltermap/3,
    get_default_workers/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

%% Default threshold - below this, sequential is faster
-define(PARALLEL_THRESHOLD, 10).
-define(SERVER, ?MODULE).

-record(state, {
    workers :: [pid()],
    worker_count :: pos_integer()
}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start the worker pool with default worker count
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    start_link(get_default_workers()).

%% @doc Start the worker pool with specified worker count
-spec start_link(pos_integer()) -> {ok, pid()} | {error, term()}.
start_link(WorkerCount) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, WorkerCount, []).

%% @doc Stop the worker pool
-spec stop() -> ok.
stop() ->
    gen_server:stop(?SERVER).

%% @doc Get default number of workers (scheduler count)
-spec get_default_workers() -> pos_integer().
get_default_workers() ->
    erlang:system_info(schedulers).

%% @doc Parallel map with default concurrency
-spec pmap(fun((A) -> B), [A]) -> [B] when A :: term(), B :: term().
pmap(Fun, Items) ->
    pmap(Fun, Items, get_default_workers()).

%% @doc Parallel map with bounded concurrency, preserves order
-spec pmap(fun((A) -> B), [A], pos_integer()) -> [B] when A :: term(), B :: term().
pmap(_Fun, [], _MaxWorkers) ->
    [];
pmap(Fun, Items, MaxWorkers) when MaxWorkers > 0 ->
    case length(Items) of
        N when N =< ?PARALLEL_THRESHOLD ->
            %% Small list: sequential is faster
            lists:map(Fun, Items);
        _ ->
            pmap_pool(Fun, Items, MaxWorkers)
    end.

%% @doc Parallel filtermap with default concurrency
-spec pfiltermap(fun((A) -> boolean() | {true, B}), [A]) -> [B]
    when A :: term(), B :: term().
pfiltermap(Fun, Items) ->
    pfiltermap(Fun, Items, get_default_workers()).

%% @doc Parallel filtermap with bounded concurrency, preserves order
-spec pfiltermap(fun((A) -> boolean() | {true, B}), [A], pos_integer()) -> [B]
    when A :: term(), B :: term().
pfiltermap(_Fun, [], _MaxWorkers) ->
    [];
pfiltermap(Fun, Items, MaxWorkers) when MaxWorkers > 0 ->
    case length(Items) of
        N when N =< ?PARALLEL_THRESHOLD ->
            lists:filtermap(Fun, Items);
        _ ->
            pfiltermap_pool(Fun, Items, MaxWorkers)
    end.

%%%===================================================================
%%% Internal - Pool-based execution
%%%===================================================================

%% @private Execute pmap using worker pool if available, otherwise spawn workers
pmap_pool(Fun, Items, MaxWorkers) ->
    case whereis(?SERVER) of
        undefined ->
            %% Pool not started, use spawn-based fallback
            pmap_spawn(Fun, Items, MaxWorkers);
        _Pid ->
            %% Use pool
            execute_pool(map, Fun, Items, MaxWorkers)
    end.

%% @private Execute pfiltermap using worker pool if available
pfiltermap_pool(Fun, Items, MaxWorkers) ->
    case whereis(?SERVER) of
        undefined ->
            pfiltermap_spawn(Fun, Items, MaxWorkers);
        _Pid ->
            execute_pool(filtermap, Fun, Items, MaxWorkers)
    end.

%% @private Execute work via the pool
execute_pool(Type, Fun, Items, MaxWorkers) ->
    case gen_server:call(?SERVER, {execute, Type, Fun, Items, MaxWorkers}, infinity) of
        {ok, Result} ->
            Result;
        {error, {Class, Reason, Stack}} ->
            erlang:raise(Class, Reason, Stack)
    end.

%%%===================================================================
%%% Internal - Spawn-based fallback
%%%===================================================================

%% @private Spawn-based pmap (fallback when pool not available)
pmap_spawn(Fun, Items, MaxWorkers) ->
    Parent = self(),
    Ref = make_ref(),
    IndexedItems = lists:zip(lists:seq(1, length(Items)), Items),
    Results = pmap_batches(Fun, IndexedItems, MaxWorkers, Parent, Ref, []),
    Sorted = lists:sort(Results),
    [Value || {_Index, Value} <- Sorted].

%% @private Spawn-based pfiltermap (fallback)
pfiltermap_spawn(Fun, Items, MaxWorkers) ->
    Parent = self(),
    Ref = make_ref(),
    IndexedItems = lists:zip(lists:seq(1, length(Items)), Items),
    Results = pfiltermap_batches(Fun, IndexedItems, MaxWorkers, Parent, Ref, []),
    Sorted = lists:sort(Results),
    [Value || {_Index, Value} <- Sorted].

%% @private Process pmap items in batches
pmap_batches(_Fun, [], _MaxWorkers, _Parent, _Ref, Acc) ->
    Acc;
pmap_batches(Fun, Items, MaxWorkers, Parent, Ref, Acc) ->
    {Batch, Remaining} = safe_split(MaxWorkers, Items),
    WorkerRefs = [spawn_worker(Fun, Index, Item, Parent, Ref)
                  || {Index, Item} <- Batch],
    BatchResults = collect_results(WorkerRefs, Ref, []),
    pmap_batches(Fun, Remaining, MaxWorkers, Parent, Ref, BatchResults ++ Acc).

%% @private Process filtermap items in batches
pfiltermap_batches(_Fun, [], _MaxWorkers, _Parent, _Ref, Acc) ->
    Acc;
pfiltermap_batches(Fun, Items, MaxWorkers, Parent, Ref, Acc) ->
    {Batch, Remaining} = safe_split(MaxWorkers, Items),
    WorkerRefs = [spawn_filtermap_worker(Fun, Index, Item, Parent, Ref)
                  || {Index, Item} <- Batch],
    BatchResults = collect_filtermap_results(WorkerRefs, Ref, []),
    pfiltermap_batches(Fun, Remaining, MaxWorkers, Parent, Ref, BatchResults ++ Acc).

%% @private Spawn a worker process for pmap
spawn_worker(Fun, Index, Item, Parent, Ref) ->
    {_Pid, MonRef} = spawn_monitor(fun() ->
        Parent ! {Ref, Index, barrel_lib:safe_apply(fun() -> Fun(Item) end)}
    end),
    {Index, MonRef}.

%% @private Spawn a worker for filtermap
spawn_filtermap_worker(Fun, Index, Item, Parent, Ref) ->
    {_Pid, MonRef} = spawn_monitor(fun() ->
        Parent ! {Ref, Index, barrel_lib:safe_apply(fun() -> Fun(Item) end)}
    end),
    {Index, MonRef}.

%% @private Collect results from workers
collect_results([], _Ref, Acc) ->
    Acc;
collect_results(WorkerRefs, Ref, Acc) ->
    Timeout = application:get_env(barrel_docdb, query_timeout_ms, 30000),
    receive
        {Ref, Index, {ok, Result}} ->
            WorkerRefs2 = lists:keydelete(Index, 1, WorkerRefs),
            collect_results(WorkerRefs2, Ref, [{Index, Result} | Acc]);
        {Ref, Index, {error, {Class, Reason, Stack}}} ->
            cleanup_workers(WorkerRefs, Index),
            erlang:raise(Class, Reason, Stack);
        {'DOWN', MonRef, process, _Pid, Reason} ->
            case lists:keyfind(MonRef, 2, WorkerRefs) of
                {Index, MonRef} when Reason =/= normal ->
                    cleanup_workers(WorkerRefs, Index),
                    error({worker_crashed, Reason});
                _ ->
                    collect_results(WorkerRefs, Ref, Acc)
            end
    after Timeout ->
        Pending = length(WorkerRefs),
        Done = length(Acc),
        Total = Pending + Done,
        cleanup_workers(WorkerRefs, undefined),
        logger:warning("barrel_parallel: spawn pool timed out after ~pms; "
                       "~p/~p tasks completed", [Timeout, Done, Total]),
        barrel_metrics:inc_query_timeouts(),
        error({query_timeout, #{completed => Done,
                                total => Total,
                                missing_batches => Pending,
                                timeout_ms => Timeout}})
    end.

%% @private Collect filtermap results
collect_filtermap_results([], _Ref, Acc) ->
    Acc;
collect_filtermap_results(WorkerRefs, Ref, Acc) ->
    Timeout = application:get_env(barrel_docdb, query_timeout_ms, 30000),
    receive
        {Ref, Index, {ok, false}} ->
            WorkerRefs2 = lists:keydelete(Index, 1, WorkerRefs),
            collect_filtermap_results(WorkerRefs2, Ref, Acc);
        {Ref, Index, {ok, true}} ->
            WorkerRefs2 = lists:keydelete(Index, 1, WorkerRefs),
            collect_filtermap_results(WorkerRefs2, Ref, Acc);
        {Ref, Index, {ok, {true, Value}}} ->
            WorkerRefs2 = lists:keydelete(Index, 1, WorkerRefs),
            collect_filtermap_results(WorkerRefs2, Ref, [{Index, Value} | Acc]);
        {Ref, Index, {error, {Class, Reason, Stack}}} ->
            cleanup_workers(WorkerRefs, Index),
            erlang:raise(Class, Reason, Stack);
        {'DOWN', MonRef, process, _Pid, Reason} ->
            case lists:keyfind(MonRef, 2, WorkerRefs) of
                {Index, MonRef} when Reason =/= normal ->
                    cleanup_workers(WorkerRefs, Index),
                    error({worker_crashed, Reason});
                _ ->
                    collect_filtermap_results(WorkerRefs, Ref, Acc)
            end
    after Timeout ->
        Pending = length(WorkerRefs),
        Done = length(Acc),
        Total = Pending + Done,
        cleanup_workers(WorkerRefs, undefined),
        logger:warning("barrel_parallel: spawn pool timed out after ~pms; "
                       "~p/~p tasks completed", [Timeout, Done, Total]),
        barrel_metrics:inc_query_timeouts(),
        error({query_timeout, #{completed => Done,
                                total => Total,
                                missing_batches => Pending,
                                timeout_ms => Timeout}})
    end.

%% @private Clean up remaining workers
cleanup_workers(WorkerRefs, ExceptIndex) ->
    lists:foreach(fun({Index, MonRef}) ->
        case Index of
            ExceptIndex -> ok;
            _ -> demonitor(MonRef, [flush])
        end
    end, WorkerRefs).

%% @private Safe split
safe_split(N, List) when N >= length(List) ->
    {List, []};
safe_split(N, List) ->
    lists:split(N, List).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(WorkerCount) ->
    process_flag(trap_exit, true),
    Workers = [spawn_pool_worker() || _ <- lists:seq(1, WorkerCount)],
    {ok, #state{workers = Workers, worker_count = WorkerCount}}.

handle_call({execute, Type, Fun, Items, MaxWorkers}, _From, State) ->
    #state{workers = Workers} = State,
    %% Use min of requested workers and available workers
    UseWorkers = min(MaxWorkers, length(Workers)),
    ActiveWorkers = lists:sublist(Workers, UseWorkers),

    %% Execute and catch any errors - return them as tuples to avoid crashing gen_server
    Result = barrel_lib:safe_apply(
        fun() -> execute_on_workers(Type, Fun, Items, ActiveWorkers) end),
    {reply, Result, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, Reason}, #state{workers = Workers} = State) ->
    case lists:member(Pid, Workers) of
        true when Reason =/= normal ->
            %% Worker died, spawn replacement
            NewWorker = spawn_pool_worker(),
            NewWorkers = [NewWorker | lists:delete(Pid, Workers)],
            {noreply, State#state{workers = NewWorkers}};
        _ ->
            {noreply, State}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{workers = Workers}) ->
    [exit(W, shutdown) || W <- Workers],
    ok.

%%%===================================================================
%%% Pool worker implementation
%%%===================================================================

%% @private Spawn a pool worker
spawn_pool_worker() ->
    Parent = self(),
    spawn_link(fun() -> pool_worker_loop(Parent) end).

%% @private Worker loop - waits for work, executes, sends result
pool_worker_loop(Pool) ->
    receive
        {work, Caller, Ref, Index, Fun, Item} ->
            _ = Caller ! {Ref, Index, barrel_lib:safe_apply(fun() -> Fun(Item) end)},
            pool_worker_loop(Pool);
        stop ->
            ok
    end.

%% @private Execute work items on pool workers
execute_on_workers(Type, Fun, Items, Workers) ->
    Caller = self(),
    Ref = make_ref(),
    IndexedItems = lists:zip(lists:seq(1, length(Items)), Items),

    %% Distribute work round-robin to workers
    WorkerCount = length(Workers),
    WorkerArray = list_to_tuple(Workers),

    %% Send all work items
    lists:foreach(fun({Index, Item}) ->
        WorkerIdx = ((Index - 1) rem WorkerCount) + 1,
        Worker = element(WorkerIdx, WorkerArray),
        Worker ! {work, Caller, Ref, Index, Fun, Item}
    end, IndexedItems),

    %% Collect results
    Results = collect_pool_results(Type, Ref, length(Items), []),

    %% Sort by index and extract values
    Sorted = lists:sort(Results),
    case Type of
        map ->
            [Value || {_Index, Value} <- Sorted];
        filtermap ->
            [Value || {_Index, Value} <- Sorted]
    end.

%% @private Collect results from pool workers
collect_pool_results(_Type, _Ref, 0, Acc) ->
    Acc;
collect_pool_results(Type, Ref, Remaining, Acc) ->
    Timeout = application:get_env(barrel_docdb, query_timeout_ms, 30000),
    receive
        {Ref, Index, {ok, Result}} ->
            NewAcc = case Type of
                map ->
                    [{Index, Result} | Acc];
                filtermap ->
                    case Result of
                        false -> Acc;
                        true -> Acc;  % true without value - skip
                        {true, Value} -> [{Index, Value} | Acc]
                    end
            end,
            collect_pool_results(Type, Ref, Remaining - 1, NewAcc);
        {Ref, _Index, {error, {Class, Reason, Stack}}} ->
            erlang:raise(Class, Reason, Stack)
    after Timeout ->
        Completed = length(Acc),
        Total = Completed + Remaining,
        logger:warning("barrel_parallel: pool timed out after ~pms; "
                       "~p/~p tasks completed", [Timeout, Completed, Total]),
        barrel_metrics:inc_query_timeouts(),
        error({query_timeout, #{completed => Completed,
                                total => Total,
                                missing_batches => Remaining,
                                timeout_ms => Timeout}})
    end.
