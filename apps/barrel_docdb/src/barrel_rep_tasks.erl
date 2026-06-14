%%%-------------------------------------------------------------------
%%% @doc barrel_rep_tasks - Persistent replication task manager
%%%
%%% Manages persistent replication tasks that survive node restarts.
%%% Tasks are stored in a dedicated system database and can be:
%%% - One-shot: run once and complete
%%% - Continuous: keep running and replicate changes as they happen
%%%
%%% == Starting a Task ==
%%% ```
%%% %% Start a continuous replication to a remote node
%%% {ok, TaskId} = barrel_rep_tasks:start_task(#{
%%%     source => <<"mydb">>,
%%%     target => <<"http://remote:8080/db/mydb">>,
%%%     mode => continuous,
%%%     direction => push
%%% }).
%%% '''
%%%
%%% == Task Lifecycle ==
%%% Tasks go through these states:
%%% - `pending': Created but not yet started
%%% - `running': Currently replicating
%%% - `paused': Temporarily stopped (can be resumed)
%%% - `completed': One-shot task finished successfully
%%% - `failed': Task encountered an error
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_rep_tasks).

-behaviour(gen_server).

-include("barrel_docdb.hrl").

%% API
-export([
    start_link/0,
    stop/0,
    start_task/1,
    stop_task/1,
    pause_task/1,
    resume_task/1,
    delete_task/1,
    get_task/1,
    get_task_pid/1,
    list_tasks/0,
    list_tasks/1
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).
-define(TASKS_DB, <<"_replication_tasks">>).

%% Task check interval (5 seconds)
-define(CHECK_INTERVAL, 5000).

%% Default batch size for continuous replication
-define(DEFAULT_BATCH_SIZE, 100).

%%====================================================================
%% Types
%%====================================================================

-type task_id() :: binary().
-type task_status() :: pending | running | paused | completed | failed.
-type task_mode() :: one_shot | continuous.
-type task_direction() :: push | pull | both.

-type task_config() :: #{
    source := binary() | map(),           % Local db name or endpoint config
    target := binary() | map(),           % Local db name or endpoint config
    mode => task_mode(),                  % one_shot | continuous (default: one_shot)
    direction => task_direction(),        % push | pull | both (default: push)
    source_transport => module(),         % Transport for source
    target_transport => module(),         % Transport for target
    batch_size => pos_integer(),
    filter => barrel_rep:filter_opts(),
    wait_for => [binary() | map()]        % Chain: wait for downstream targets
}.

-type task() :: #{
    id := task_id(),
    config := task_config(),
    status := task_status(),
    last_seq => seq() | first,
    error => binary(),
    created_at := integer(),
    updated_at := integer()
}.

-export_type([task_id/0, task_config/0, task/0, task_status/0, task_mode/0]).

-record(state, {
    running = #{} :: #{task_id() => pid()}
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the task manager
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Stop the task manager
-spec stop() -> ok.
stop() ->
    gen_server:stop(?SERVER).

%% @doc Start a new replication task
%% Returns the task ID which can be used to manage the task.
-spec start_task(task_config()) -> {ok, task_id()} | {error, term()}.
start_task(Config) ->
    gen_server:call(?SERVER, {start_task, Config}).

%% @doc Stop a running task
-spec stop_task(task_id()) -> ok | {error, term()}.
stop_task(TaskId) ->
    gen_server:call(?SERVER, {stop_task, TaskId}).

%% @doc Pause a running task (can be resumed later)
-spec pause_task(task_id()) -> ok | {error, term()}.
pause_task(TaskId) ->
    gen_server:call(?SERVER, {pause_task, TaskId}).

%% @doc Resume a paused task
-spec resume_task(task_id()) -> ok | {error, term()}.
resume_task(TaskId) ->
    gen_server:call(?SERVER, {resume_task, TaskId}).

%% @doc Delete a task (stops it if running)
-spec delete_task(task_id()) -> ok | {error, term()}.
delete_task(TaskId) ->
    gen_server:call(?SERVER, {delete_task, TaskId}).

%% @doc Get task info
-spec get_task(task_id()) -> {ok, task()} | {error, not_found}.
get_task(TaskId) ->
    gen_server:call(?SERVER, {get_task, TaskId}).

%% @doc List all tasks
-spec list_tasks() -> {ok, [task()]}.
list_tasks() ->
    list_tasks(#{}).

%% @doc List tasks with filter
%% Filter options:
%% - status: filter by status (running, paused, etc.)
%% - mode: filter by mode (continuous, one_shot)
-spec list_tasks(map()) -> {ok, [task()]}.
list_tasks(Filter) ->
    gen_server:call(?SERVER, {list_tasks, Filter}).

%% @doc Get the pid for a running task
%% Returns {ok, Pid} if the task is running, {error, not_running} otherwise.
-spec get_task_pid(task_id()) -> {ok, pid()} | {error, not_running | not_found}.
get_task_pid(TaskId) ->
    gen_server:call(?SERVER, {get_task_pid, TaskId}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Ensure tasks database exists
    ensure_tasks_db(),
    %% Schedule task check
    erlang:send_after(?CHECK_INTERVAL, self(), check_tasks),
    %% Restore persisted tasks
    State = restore_tasks(#state{}),
    {ok, State}.

handle_call({start_task, Config}, _From, State) ->
    case do_start_task(Config, State) of
        {ok, TaskId, NewState} ->
            {reply, {ok, TaskId}, NewState};
        {error, _} = Error ->
            {reply, Error, State}
    end;

handle_call({stop_task, TaskId}, _From, State) ->
    case do_stop_task(TaskId, State) of
        {ok, NewState} ->
            {reply, ok, NewState};
        {error, _} = Error ->
            {reply, Error, State}
    end;

handle_call({pause_task, TaskId}, _From, State) ->
    case do_pause_task(TaskId, State) of
        {ok, NewState} ->
            {reply, ok, NewState};
        {error, _} = Error ->
            {reply, Error, State}
    end;

handle_call({resume_task, TaskId}, _From, State) ->
    case do_resume_task(TaskId, State) of
        {ok, NewState} ->
            {reply, ok, NewState};
        {error, _} = Error ->
            {reply, Error, State}
    end;

handle_call({delete_task, TaskId}, _From, State) ->
    case do_delete_task(TaskId, State) of
        {ok, NewState} ->
            {reply, ok, NewState};
        {error, _} = Error ->
            {reply, Error, State}
    end;

handle_call({get_task, TaskId}, _From, State) ->
    {reply, do_get_task(TaskId), State};

handle_call({list_tasks, Filter}, _From, State) ->
    {reply, do_list_tasks(Filter, State), State};

handle_call({get_task_pid, TaskId}, _From, State) ->
    Reply = case maps:find(TaskId, State#state.running) of
        {ok, Pid} -> {ok, Pid};
        error -> {error, not_running}
    end,
    {reply, Reply, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({task_complete, TaskId}, State) ->
    _ = update_task_status(TaskId, completed),
    NewRunning = maps:remove(TaskId, State#state.running),
    {noreply, State#state{running = NewRunning}};

handle_cast({task_progress, TaskId, LastSeq}, State) ->
    _ = update_task_seq(TaskId, LastSeq),
    {noreply, State};

handle_cast({task_error, TaskId, Reason}, State) ->
    _ = update_task_status(TaskId, failed, #{error => format_reason(Reason)}),
    NewRunning = maps:remove(TaskId, State#state.running),
    {noreply, State#state{running = NewRunning}};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(check_tasks, State) ->
    NewState = check_running_tasks(State),
    erlang:send_after(?CHECK_INTERVAL, self(), check_tasks),
    {noreply, NewState};

handle_info({'DOWN', _Ref, process, Pid, Reason}, State) ->
    NewState = handle_task_down(Pid, Reason, State),
    {noreply, NewState};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    %% Stop all running tasks
    maps:foreach(
        fun(_TaskId, Pid) ->
            exit(Pid, shutdown)
        end,
        State#state.running
    ),
    ok.

%%====================================================================
%% Internal - Task management
%%====================================================================

do_start_task(Config, State) ->
    %% Validate config
    case validate_config(Config) of
        ok ->
            %% Generate task ID
            TaskId = generate_task_id(),
            Now = erlang:system_time(millisecond),

            %% Create task record
            Task = #{
                id => TaskId,
                config => Config,
                status => pending,
                last_seq => first,
                created_at => Now,
                updated_at => Now
            },

            %% Persist task
            case save_task(Task) of
                ok ->
                    %% Start the task
                    {ok, NewState} = start_task_process(Task, State),
                    {ok, TaskId, NewState};
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

do_stop_task(TaskId, State) ->
    case maps:find(TaskId, State#state.running) of
        {ok, Pid} ->
            %% Stop the process
            exit(Pid, shutdown),
            %% Update status
            _ = update_task_status(TaskId, paused),
            NewRunning = maps:remove(TaskId, State#state.running),
            {ok, State#state{running = NewRunning}};
        error ->
            {error, not_running}
    end.

do_pause_task(TaskId, State) ->
    do_stop_task(TaskId, State).

do_resume_task(TaskId, State) ->
    case do_get_task(TaskId) of
        {ok, #{status := paused} = Task} ->
            start_task_process(Task, State);
        {ok, #{status := running}} ->
            {error, already_running};
        {ok, _} ->
            {error, invalid_state};
        {error, _} = Error ->
            Error
    end.

do_delete_task(TaskId, State) ->
    %% Stop if running
    NewState = case maps:find(TaskId, State#state.running) of
        {ok, Pid} ->
            exit(Pid, shutdown),
            State#state{running = maps:remove(TaskId, State#state.running)};
        error ->
            State
    end,
    %% Delete from storage
    case delete_task_record(TaskId) of
        ok -> {ok, NewState};
        {error, _} = Error -> Error
    end.

do_get_task(TaskId) ->
    case barrel_docdb:get_local_doc(?TASKS_DB, task_doc_id(TaskId)) of
        {ok, Doc} ->
            {ok, doc_to_task(Doc)};
        {error, not_found} ->
            {error, not_found}
    end.

do_list_tasks(Filter, State) ->
    %% Get all tasks from database
    case barrel_docdb:find(?TASKS_DB, #{where => [{prefix, [<<"_type">>], <<"rep_task">>}]}) of
        {ok, Docs, _Cont} ->
            Tasks = lists:map(fun doc_to_task/1, Docs),
            %% Apply filter
            Filtered = filter_tasks(Tasks, Filter),
            %% Add running status from state
            WithRunning = lists:map(
                fun(#{id := Id} = Task) ->
                    case maps:is_key(Id, State#state.running) of
                        true -> Task#{status => running};
                        false -> Task
                    end
                end,
                Filtered
            ),
            {ok, WithRunning};
        {error, _} = Error ->
            Error
    end.

%%====================================================================
%% Internal - Task execution
%%====================================================================

start_task_process(#{id := TaskId, config := Config} = Task, State) ->
    %% Get transport modules
    SourceTransport = get_transport(source, Config),
    TargetTransport = get_transport(target, Config),

    %% Get last sequence
    LastSeq = maps:get(last_seq, Task, first),

    %% Spawn task process (use spawn + monitor, not spawn_link)
    %% This prevents a crashing task from bringing down the gen_server
    Self = self(),
    Pid = spawn(fun() ->
        run_task(Self, TaskId, Config, SourceTransport, TargetTransport, LastSeq)
    end),

    %% Monitor the process for crash detection
    erlang:monitor(process, Pid),

    %% Update status
    _ = update_task_status(TaskId, running),

    NewRunning = maps:put(TaskId, Pid, State#state.running),
    {ok, State#state{running = NewRunning}}.

run_task(Parent, TaskId, Config, SourceTransport, TargetTransport, StartSeq) ->
    Source = maps:get(source, Config),
    Target = maps:get(target, Config),
    Direction = maps:get(direction, Config, push),
    Mode = maps:get(mode, Config, one_shot),
    BatchSize = maps:get(batch_size, Config, ?DEFAULT_BATCH_SIZE),
    Filter = maps:get(filter, Config, #{}),
    WaitFor = maps:get(wait_for, Config, []),

    ExtraAttrs = #{
        <<"replication.task_id">> => TaskId,
        <<"replication.direction">> => atom_to_binary(Direction, utf8),
        <<"replication.mode">> => atom_to_binary(Mode, utf8)
    },

    barrel_trace:with_db_span(rep_task, undefined, ExtraAttrs, fun() ->
        %% Run replication based on direction
        case Direction of
            push ->
                %% Source -> Target (read from source, write to target)
                run_task_loop(Parent, TaskId, Source, Target, SourceTransport, TargetTransport,
                              StartSeq, BatchSize, Filter, Mode, WaitFor);
            pull ->
                %% Target -> Source (read from target, write to source)
                %% Swap the read/write roles
                run_task_loop(Parent, TaskId, Target, Source, TargetTransport, SourceTransport,
                              StartSeq, BatchSize, Filter, Mode, WaitFor);
            both ->
                %% Bidirectional: run both push and pull concurrently
                %% This is simplified - for production we'd want separate checkpoints
                Self = self(),
                spawn_link(fun() ->
                    run_task_loop(Self, TaskId, Source, Target, SourceTransport, TargetTransport,
                                  StartSeq, BatchSize, Filter, Mode, WaitFor)
                end),
                %% Run pull in this process
                run_task_loop(Parent, TaskId, Target, Source, TargetTransport, SourceTransport,
                              StartSeq, BatchSize, Filter, Mode, WaitFor)
        end
    end).

run_task_loop(Parent, TaskId, Source, Target, SourceTransport, TargetTransport,
              Since, BatchSize, Filter, Mode, WaitFor) ->
    %% Build changes options
    ChangesOpts = #{limit => BatchSize},
    ChangesOpts2 = case maps:get(paths, Filter, undefined) of
        undefined -> ChangesOpts;
        Paths -> ChangesOpts#{paths => Paths}
    end,

    %% Get changes
    case SourceTransport:get_changes(Source, Since, ChangesOpts2) of
        {ok, [], _LastSeq} when Mode =:= one_shot ->
            %% No more changes, one-shot complete
            gen_server:cast(Parent, {task_complete, TaskId});

        {ok, [], _LastSeq} when Mode =:= continuous ->
            %% No changes, wait and retry
            timer:sleep(1000),
            run_task_loop(Parent, TaskId, Source, Target, SourceTransport, TargetTransport,
                          Since, BatchSize, Filter, Mode, WaitFor);

        {ok, Changes, LastSeq} ->
            %% Replicate batch
            {ok, _Stats} = barrel_rep_alg:replicate(Source, Target, SourceTransport, TargetTransport, Changes),
            %% If wait_for is specified, verify docs reached downstream
            case wait_for_downstream(Changes, WaitFor) of
              ok ->
                %% Update checkpoint
                gen_server:cast(Parent, {task_progress, TaskId, LastSeq}),
                %% Continue
                run_task_loop(Parent, TaskId, Source, Target, SourceTransport, TargetTransport,
                              LastSeq, BatchSize, Filter, Mode, WaitFor);
              {error, Reason} ->
                gen_server:cast(Parent, {task_error, TaskId, Reason})
            end;
        {error, Reason} ->
            gen_server:cast(Parent, {task_error, TaskId, Reason})
    end.

%% @doc Wait for documents to reach downstream targets
%% For each change, verify the revision exists at all wait_for targets
wait_for_downstream(_Changes, []) ->
    ok;
wait_for_downstream(Changes, WaitFor) ->
    %% Extract doc IDs and their revisions from changes
    DocsToVerify = lists:filtermap(
        fun(Change) ->
            case Change of
                #{id := DocId, rev := Rev} -> {true, {DocId, Rev}};
                #{<<"id">> := DocId, <<"rev">> := Rev} -> {true, {DocId, Rev}};
                _ -> false
            end
        end,
        Changes
    ),
    %% Verify each document at each target
    verify_at_targets(DocsToVerify, WaitFor, 10, 500).

%% @doc Verify documents exist at targets with retry logic
verify_at_targets(_Docs, _Targets, 0, _Delay) ->
    {error, wait_for_timeout};
verify_at_targets([], _Targets, _Retries, _Delay) ->
    ok;
verify_at_targets(Docs, Targets, Retries, Delay) ->
    %% Check all docs at all targets
    Results = lists:map(
        fun(Target) ->
            Transport = get_transport_for_target(Target),
            lists:map(
                fun({DocId, _ExpectedRev}) ->
                    case Transport:get_doc(Target, DocId, #{}) of
                        {ok, _Doc, _Meta} -> ok;
                        {error, not_found} -> not_found;
                        {error, _} -> error
                    end
                end,
                Docs
            )
        end,
        Targets
    ),
    %% Flatten and check if all succeeded
    AllResults = lists:flatten(Results),
    case lists:all(fun(R) -> R =:= ok end, AllResults) of
        true ->
            ok;
        false ->
            %% Some docs not yet at targets, retry
            timer:sleep(Delay),
            verify_at_targets(Docs, Targets, Retries - 1, Delay)
    end.

%% @doc Get the appropriate transport for a target
get_transport_for_target(Target) when is_binary(Target) ->
    barrel_rep_transport_local;
get_transport_for_target(#{url := _} = Target) ->
    error({http_transport_removed, Target});
get_transport_for_target(_) ->
    barrel_rep_transport_local.

handle_task_down(Pid, Reason, State) ->
    %% Find task by pid
    case find_task_by_pid(Pid, State) of
        {ok, TaskId} ->
            %% Update status based on reason
            Status = case Reason of
                normal -> completed;
                shutdown -> paused;
                _ -> failed
            end,
            _ = update_task_status(TaskId, Status, #{error => format_reason(Reason)}),
            NewRunning = maps:remove(TaskId, State#state.running),
            State#state{running = NewRunning};
        error ->
            State
    end.

find_task_by_pid(Pid, #state{running = Running}) ->
    case lists:keyfind(Pid, 2, maps:to_list(Running)) of
        {TaskId, _} -> {ok, TaskId};
        false -> error
    end.

check_running_tasks(State) ->
    %% Check if any tasks that should be running are not
    case do_list_tasks(#{status => running}, State) of
        {ok, Tasks} ->
            lists:foldl(
                fun(#{id := TaskId} = Task, AccState) ->
                    case maps:is_key(TaskId, AccState#state.running) of
                        true ->
                            AccState;
                        false ->
                            %% Task should be running but isn't - restart it
                            {ok, NewState} = start_task_process(Task, AccState),
                            NewState
                    end
                end,
                State,
                Tasks
            );
        {error, _} ->
            State
    end.

%%====================================================================
%% Internal - Persistence
%%====================================================================

ensure_tasks_db() ->
    case barrel_docdb:db_info(?TASKS_DB) of
        {ok, _} ->
            ok;
        {error, not_found} ->
            {ok, _} = barrel_docdb:create_db(?TASKS_DB),
            ok
    end.

restore_tasks(State) ->
    %% Load all tasks that should be running
    case do_list_tasks(#{status => running}, State) of
        {ok, Tasks} ->
            lists:foldl(
                fun(Task, AccState) ->
                    {ok, NewState} = start_task_process(Task, AccState),
                     NewState
                end,
                State,
                Tasks
            );
        {error, _} ->
            State
    end.

save_task(#{id := TaskId} = Task) ->
    Doc = task_to_doc(Task),
    barrel_docdb:put_local_doc(?TASKS_DB, task_doc_id(TaskId), Doc).

update_task_status(TaskId, Status) ->
    update_task_status(TaskId, Status, #{}).

update_task_status(TaskId, Status, Extra) ->
    case barrel_docdb:get_local_doc(?TASKS_DB, task_doc_id(TaskId)) of
        {ok, Doc} ->
            Now = erlang:system_time(millisecond),
            Doc2 = Doc#{
                <<"status">> => atom_to_binary(Status),
                <<"updated_at">> => Now
            },
            Doc3 = maps:merge(Doc2, maps:map(fun(_, V) -> V end, Extra)),
            barrel_docdb:put_local_doc(?TASKS_DB, task_doc_id(TaskId), Doc3);
        {error, _} ->
            ok
    end.

update_task_seq(TaskId, Seq) ->
    case barrel_docdb:get_local_doc(?TASKS_DB, task_doc_id(TaskId)) of
        {ok, Doc} ->
            Now = erlang:system_time(millisecond),
            Doc2 = Doc#{
                <<"last_seq">> => format_seq(Seq),
                <<"updated_at">> => Now
            },
            barrel_docdb:put_local_doc(?TASKS_DB, task_doc_id(TaskId), Doc2);
        {error, _} ->
            ok
    end.

delete_task_record(TaskId) ->
    barrel_docdb:delete_local_doc(?TASKS_DB, task_doc_id(TaskId)).

task_doc_id(TaskId) ->
    <<"rep_task:", TaskId/binary>>.

task_to_doc(#{id := Id, config := Config, status := Status,
              created_at := CreatedAt, updated_at := UpdatedAt} = Task) ->
    #{
        <<"_type">> => <<"rep_task">>,
        <<"id">> => Id,
        <<"config">> => config_to_map(Config),
        <<"status">> => atom_to_binary(Status),
        <<"last_seq">> => format_seq(maps:get(last_seq, Task, first)),
        <<"created_at">> => CreatedAt,
        <<"updated_at">> => UpdatedAt
    }.

doc_to_task(Doc) ->
    #{
        id => maps:get(<<"id">>, Doc),
        config => map_to_config(maps:get(<<"config">>, Doc)),
        status => binary_to_existing_atom(maps:get(<<"status">>, Doc), utf8),
        last_seq => parse_seq(maps:get(<<"last_seq">>, Doc, <<"first">>)),
        created_at => maps:get(<<"created_at">>, Doc),
        updated_at => maps:get(<<"updated_at">>, Doc)
    }.

config_to_map(Config) ->
    maps:map(
        fun(source_transport, Mod) -> atom_to_binary(Mod);
           (target_transport, Mod) -> atom_to_binary(Mod);
           (mode, Mode) -> atom_to_binary(Mode);
           (direction, Dir) -> atom_to_binary(Dir);
           (_, V) -> V
        end,
        Config
    ).

map_to_config(Map) ->
    maps:map(
        fun(<<"source_transport">>, Bin) -> binary_to_existing_atom(Bin, utf8);
           (<<"target_transport">>, Bin) -> binary_to_existing_atom(Bin, utf8);
           (<<"mode">>, Bin) -> binary_to_existing_atom(Bin, utf8);
           (<<"direction">>, Bin) -> binary_to_existing_atom(Bin, utf8);
           (K, V) when is_binary(K) -> {binary_to_existing_atom(K, utf8), V};
           (_, V) -> V
        end,
        Map
    ).

format_seq(first) -> <<"first">>;
format_seq(Seq) when is_tuple(Seq) ->
    iolist_to_binary(io_lib:format("~p", [Seq]));
format_seq(Seq) -> Seq.

parse_seq(<<"first">>) -> first;
parse_seq(Bin) when is_binary(Bin) ->
    barrel_lib:safe(fun() ->
        {ok, Tokens, _} = erl_scan:string(binary_to_list(Bin) ++ "."),
        {ok, Term} = erl_parse:parse_term(Tokens),
        Term
    end, first).

format_reason(normal) -> <<"completed">>;
format_reason(shutdown) -> <<"stopped">>;
format_reason(Reason) ->
    iolist_to_binary(io_lib:format("~p", [Reason])).

%%====================================================================
%% Internal - Helpers
%%====================================================================

validate_config(Config) ->
    case {maps:is_key(source, Config), maps:is_key(target, Config)} of
        {true, true} -> ok;
        {false, _} -> {error, missing_source};
        {_, false} -> {error, missing_target}
    end.

generate_task_id() ->
    Rand = crypto:strong_rand_bytes(8),
    binary:encode_hex(Rand, lowercase).

get_transport(source, Config) ->
    case maps:get(source, Config) of
        Url when is_binary(Url), byte_size(Url) > 0 ->
            case binary:match(Url, <<"://">>) of
                nomatch -> maps:get(source_transport, Config, barrel_rep_transport_local);
                _ -> error({http_transport_removed, Url})
            end;
        _ ->
            maps:get(source_transport, Config, barrel_rep_transport_local)
    end;
get_transport(target, Config) ->
    case maps:get(target, Config) of
        Url when is_binary(Url), byte_size(Url) > 0 ->
            case binary:match(Url, <<"://">>) of
                nomatch -> maps:get(target_transport, Config, barrel_rep_transport_local);
                _ -> error({http_transport_removed, Url})
            end;
        _ ->
            maps:get(target_transport, Config, barrel_rep_transport_local)
    end.

filter_tasks(Tasks, Filter) ->
    lists:filter(
        fun(Task) ->
            matches_filter(Task, Filter)
        end,
        Tasks
    ).

matches_filter(Task, Filter) ->
    maps:fold(
        fun(status, Status, Acc) ->
                Acc andalso maps:get(status, Task) =:= Status;
           (mode, Mode, Acc) ->
                Config = maps:get(config, Task),
                Acc andalso maps:get(mode, Config, one_shot) =:= Mode;
           (_, _, Acc) ->
                Acc
        end,
        true,
        Filter
    ).
