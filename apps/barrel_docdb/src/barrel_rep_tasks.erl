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
-export([init/1, handle_call/3, handle_cast/2, handle_continue/2,
         handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).
-define(TASKS_DB, <<"_replication_tasks">>).

%% Task check interval (5 seconds)
-define(CHECK_INTERVAL, 5000).

%% Default batch size for continuous replication
-define(DEFAULT_BATCH_SIZE, 100).

%% Remote-source idle polling: doubles on empty, resets on data
-define(POLL_MIN, 500).
-define(POLL_MAX, 15000).

%% Continuous-task error backoff: doubles on error, resets on success
-define(BACKOFF_MIN, 1000).
-define(BACKOFF_MAX, 60000).

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
    %% Trap exits so a task crash arrives as {'EXIT',...} (handled below)
    %% rather than taking the manager down, and so a manager restart tears
    %% its linked tasks down before restore re-creates them (otherwise old
    %% tasks orphan and double on every restart).
    process_flag(trap_exit, true),
    %% Open the tasks db and restore off the boot path, so a slow or failing
    %% store open cannot stall or crash the supervisor.
    {ok, #state{}, {continue, startup}}.

handle_continue(startup, State) ->
    case ensure_tasks_db() of
        ok ->
            erlang:send_after(?CHECK_INTERVAL, self(), check_tasks),
            {noreply, restore_tasks(State)};
        {error, Reason} ->
            logger:warning("barrel_rep_tasks: tasks db unavailable (~p); "
                           "retrying", [Reason]),
            erlang:send_after(?CHECK_INTERVAL, self(), startup_retry),
            {noreply, State}
    end.

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

handle_cast({task_error, TaskId, Reason}, State) ->
    _ = update_task_status(TaskId, failed,
                           #{<<"error">> => format_reason(Reason)}),
    NewRunning = maps:remove(TaskId, State#state.running),
    {noreply, State#state{running = NewRunning}};

handle_cast({task_last_error, TaskId, Reason}, State) ->
    %% continuous task riding out a transient error: record it
    %% without a status change, the task stays running
    _ = update_task_last_error(TaskId, Reason),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(check_tasks, State) ->
    NewState = check_running_tasks(State),
    erlang:send_after(?CHECK_INTERVAL, self(), check_tasks),
    {noreply, NewState};

handle_info(startup_retry, State) ->
    {noreply, State, {continue, startup}};

handle_info({'EXIT', Pid, Reason}, State) ->
    %% A linked task process exited (crash, normal, or shutdown). The
    %% manager's own supervisor exit is handled by gen_server, not here.
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
    %% Task docs are LOCAL docs: they bypass the path index, so they
    %% must be enumerated with a local-doc fold (find/2 cannot see
    %% them, which used to make list_tasks and restore return nothing)
    case barrel_docdb:fold_local_docs(?TASKS_DB, <<"rep_task:">>,
             fun(_DocId, Doc, Acc) -> [doc_to_task(Doc) | Acc] end, []) of
        {ok, Tasks} ->
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

    %% Link the task to the manager (which traps exits): the task dies
    %% with the manager, and a task crash arrives as {'EXIT',...} without
    %% taking the manager down.
    Self = self(),
    Pid = spawn_link(fun() ->
        run_task(Self, TaskId, Config, SourceTransport, TargetTransport, LastSeq)
    end),

    %% Update status
    _ = update_task_status(TaskId, running),

    NewRunning = maps:put(TaskId, Pid, State#state.running),
    {ok, State#state{running = NewRunning}}.

run_task(Parent, TaskId, Config, SourceTransport, TargetTransport, StartSeq) ->
    %% Endpoints resolve once at task start (normalized URL, auth from
    %% the sync_auth env), so the same term flows everywhere.
    Source = resolve_endpoint(maps:get(source, Config)),
    Target = resolve_endpoint(maps:get(target, Config)),
    Direction = maps:get(direction, Config, push),
    Mode = maps:get(mode, Config, one_shot),

    Ctx0 = #{
        parent => Parent,
        task_id => TaskId,
        mode => Mode,
        batch_size => maps:get(batch_size, Config, ?DEFAULT_BATCH_SIZE),
        filter => maps:get(filter, Config, #{}),
        wait_for => [resolve_endpoint(W)
                     || W <- maps:get(wait_for, Config, [])]
    },

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
                run_direction(Ctx0, Source, Target, SourceTransport,
                              TargetTransport, StartSeq);
            pull ->
                %% Target -> Source (read from target, write to source)
                run_direction(Ctx0, Target, Source, TargetTransport,
                              SourceTransport, StartSeq);
            both ->
                %% Bidirectional: run both push and pull concurrently.
                %% Simplified (shared checkpoint). Both directions keep the
                %% manager as their parent (Ctx0) so their progress/error
                %% casts are actually read, not sent to this task process
                %% where nothing receives them.
                spawn_link(fun() ->
                    run_direction(Ctx0, Source, Target,
                                  SourceTransport, TargetTransport,
                                  StartSeq)
                end),
                %% Run pull in this process
                run_direction(Ctx0, Target, Source, TargetTransport,
                              SourceTransport, StartSeq)
        end
    end).

run_direction(Ctx, From, To, FromTransport, ToTransport, Since) ->
    Ctx2 = Ctx#{
        from => From,
        to => To,
        from_transport => FromTransport,
        to_transport => ToTransport,
        wake => init_wake(Ctx, From, Since),
        poll => ?POLL_MIN,
        backoff => ?BACKOFF_MIN
    },
    run_task_loop(Ctx2, Since).

%% Continuous local sources wake on the changes stream, subscribed
%% BEFORE the first drain so no write slips between drain and
%% subscribe. Remote (and streamless) sources poll adaptively.
init_wake(#{mode := continuous}, From, Since) when is_binary(From) ->
    case barrel_docdb:subscribe_changes(From, Since,
                                        #{mode => push,
                                          owner => self()}) of
        {ok, Stream} -> {stream, Stream};
        {error, _} -> poll
    end;
init_wake(_Ctx, _From, _Since) ->
    poll.

run_task_loop(Ctx, Since) ->
    #{parent := Parent, task_id := TaskId, mode := Mode,
      from := From, to := To,
      from_transport := FromTransport, to_transport := ToTransport,
      batch_size := BatchSize, filter := Filter,
      wait_for := WaitFor} = Ctx,

    %% Changes options carry the whole filter: paths, query, channel
    ChangesOpts = maps:merge(#{limit => BatchSize},
                             maps:with([paths, query, channel], Filter)),

    case FromTransport:get_changes(From, Since, ChangesOpts) of
        {ok, [], _LastSeq} when Mode =:= one_shot ->
            %% No more changes, one-shot complete
            gen_server:cast(Parent, {task_complete, TaskId});

        {ok, [], _LastSeq} ->
            %% Idle: block on the wake signal (stream or poll timer)
            run_task_loop(wait_for_wake(Ctx), Since);

        {ok, Changes, LastSeq} ->
            case barrel_rep_checkpoint:seq_advanced(Since, LastSeq) of
                false ->
                    %% Non-empty batch that did not advance the sequence: a
                    %% non-conforming source. Continuous backs off, one-shot
                    %% fails, instead of spinning at 100% CPU forever.
                    handle_loop_error(Ctx, Since, no_progress);
                true ->
                    case replicate_batch(From, To, FromTransport, ToTransport,
                                         Changes, WaitFor) of
                        ok ->
                            %% Persist the checkpoint here in the task
                            %% process, off the manager's message loop.
                            _ = update_task_seq(TaskId, LastSeq),
                            run_task_loop(reset_pacing(Ctx), LastSeq);
                        {error, Reason} ->
                            handle_loop_error(Ctx, Since, Reason)
                    end
            end;
        {error, Reason} ->
            handle_loop_error(Ctx, Since, Reason)
    end.

replicate_batch(From, To, FromTransport, ToTransport, Changes, WaitFor) ->
    case barrel_rep_alg:replicate(From, To, FromTransport, ToTransport,
                                  Changes) of
        {ok, _Stats} ->
            %% If wait_for is specified, verify docs reached downstream
            wait_for_downstream(Changes, WaitFor);
        {error, _} = Error ->
            Error
    end.

%% Continuous tasks ride out transient errors: record last_error on
%% the task doc (status stays running) and back off exponentially
%% with jitter. One-shot keeps fail-fast.
handle_loop_error(#{mode := continuous} = Ctx, Since, Reason) ->
    #{parent := Parent, task_id := TaskId, backoff := Backoff} = Ctx,
    gen_server:cast(Parent, {task_last_error, TaskId, Reason}),
    timer:sleep(with_jitter(Backoff)),
    run_task_loop(Ctx#{backoff := min(Backoff * 2, ?BACKOFF_MAX)}, Since);
handle_loop_error(#{parent := Parent, task_id := TaskId}, _Since,
                  Reason) ->
    gen_server:cast(Parent, {task_error, TaskId, Reason}).

with_jitter(Ms) ->
    Ms + rand:uniform(max(Ms div 4, 1)).

reset_pacing(Ctx) ->
    Ctx#{poll := ?POLL_MIN, backoff := ?BACKOFF_MIN}.

%% Idle continuous task. Stream-woken: the stream payload is
%% unfiltered, so it is a wake-up signal only; ack immediately and
%% drain through the transport with the task's filter. Polling:
%% adaptive interval, doubled while idle.
wait_for_wake(#{wake := {stream, Stream}} = Ctx) ->
    case barrel_changes_stream:await(Stream, infinity) of
        {ReqId, _Changes} ->
            ok = barrel_changes_stream:ack(Stream, ReqId),
            Ctx;
        [] ->
            %% stream went away: degrade to polling
            Ctx#{wake := poll}
    end;
wait_for_wake(#{wake := poll, poll := Poll} = Ctx) ->
    timer:sleep(Poll),
    Ctx#{poll := min(Poll * 2, ?POLL_MAX)}.

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

%% @doc Get the appropriate transport for a (resolved) target
get_transport_for_target(Target) ->
    case is_remote(Target) of
        true -> barrel_rep_transport_http;
        false -> barrel_rep_transport_local
    end.

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
            _ = update_task_status(TaskId, Status,
                                   #{<<"error">> => format_reason(Reason)}),
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
            case barrel_docdb:create_db(?TASKS_DB) of
                {ok, _} -> ok;
                {error, _} = E -> E
            end;
        {error, _} = E ->
            E
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
    update_task_doc(TaskId, fun(Doc) ->
        maps:merge(Doc#{<<"status">> => atom_to_binary(Status)}, Extra)
    end).

update_task_seq(TaskId, Seq) ->
    update_task_doc(TaskId, fun(Doc) ->
        Doc#{<<"last_seq">> => format_seq(Seq)}
    end).

update_task_last_error(TaskId, Reason) ->
    update_task_doc(TaskId, fun(Doc) ->
        Doc#{<<"last_error">> => format_reason(Reason)}
    end).

update_task_doc(TaskId, Fun) ->
    case barrel_docdb:get_local_doc(?TASKS_DB, task_doc_id(TaskId)) of
        {ok, Doc} ->
            Doc2 = Fun(Doc),
            Doc3 = Doc2#{<<"updated_at">> =>
                             erlang:system_time(millisecond)},
            barrel_docdb:put_local_doc(?TASKS_DB, task_doc_id(TaskId),
                                       Doc3);
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
    Task = #{
        id => maps:get(<<"id">>, Doc),
        config => map_to_config(maps:get(<<"config">>, Doc)),
        status => binary_to_existing_atom(maps:get(<<"status">>, Doc), utf8),
        last_seq => parse_seq(maps:get(<<"last_seq">>, Doc, <<"first">>)),
        created_at => maps:get(<<"created_at">>, Doc),
        updated_at => maps:get(<<"updated_at">>, Doc)
    },
    maps:fold(
        fun(<<"error">>, V, Acc) -> Acc#{error => V};
           (<<"last_error">>, V, Acc) -> Acc#{last_error => V};
           (_, _, Acc) -> Acc
        end,
        Task,
        Doc).

%% The persisted config uses binary keys and wire-safe values: enums
%% as binaries, the filter through barrel_rep_filter, and endpoints
%% by their URL only (no secrets in task docs; auth re-resolves from
%% the sync_auth env at task start).
config_to_map(Config) ->
    maps:fold(
        fun(source, V, Acc) -> Acc#{<<"source">> => endpoint_to_doc(V)};
           (target, V, Acc) -> Acc#{<<"target">> => endpoint_to_doc(V)};
           (mode, V, Acc) -> Acc#{<<"mode">> => atom_to_binary(V, utf8)};
           (direction, V, Acc) ->
               Acc#{<<"direction">> => atom_to_binary(V, utf8)};
           (source_transport, V, Acc) ->
               Acc#{<<"source_transport">> => atom_to_binary(V, utf8)};
           (target_transport, V, Acc) ->
               Acc#{<<"target_transport">> => atom_to_binary(V, utf8)};
           (batch_size, V, Acc) -> Acc#{<<"batch_size">> => V};
           (filter, V, Acc) ->
               Acc#{<<"filter">> => barrel_rep_filter:to_wire(V)};
           (wait_for, V, Acc) ->
               Acc#{<<"wait_for">> => [endpoint_to_doc(W) || W <- V]};
           (_, _, Acc) -> Acc
        end,
        #{},
        Config).

endpoint_to_doc(#{url := Url}) -> Url;
endpoint_to_doc(Other) -> Other.

map_to_config(Map) ->
    maps:fold(
        fun(<<"source">>, V, Acc) -> Acc#{source => V};
           (<<"target">>, V, Acc) -> Acc#{target => V};
           (<<"mode">>, V, Acc) ->
               Acc#{mode => binary_to_existing_atom(V, utf8)};
           (<<"direction">>, V, Acc) ->
               Acc#{direction => binary_to_existing_atom(V, utf8)};
           (<<"source_transport">>, V, Acc) ->
               Acc#{source_transport => binary_to_existing_atom(V, utf8)};
           (<<"target_transport">>, V, Acc) ->
               Acc#{target_transport => binary_to_existing_atom(V, utf8)};
           (<<"batch_size">>, V, Acc) -> Acc#{batch_size => V};
           (<<"filter">>, V, Acc) ->
               case barrel_rep_filter:from_wire(V) of
                   {ok, Filter} -> Acc#{filter => Filter};
                   {error, _} -> Acc
               end;
           (<<"wait_for">>, V, Acc) -> Acc#{wait_for => V};
           (_, _, Acc) -> Acc
        end,
        #{},
        Map).

format_seq(first) -> <<"first">>;
format_seq(Seq) -> barrel_rep_checkpoint:encode_seq(Seq).

parse_seq(<<"first">>) -> first;
parse_seq(Bin) when is_binary(Bin) ->
    case barrel_rep_checkpoint:decode_seq(Bin) of
        first -> legacy_parse_seq(Bin);
        Seq -> Seq
    end.

%% Task docs written before the b64 codec stored the HLC with ~p;
%% accept them once so a restart across the upgrade keeps its place.
legacy_parse_seq(Bin) ->
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
        {true, true} ->
            %% a bad remote URL fails at start_task, not asynchronously
            %% inside the task process
            try
                _ = resolve_endpoint(maps:get(source, Config)),
                _ = resolve_endpoint(maps:get(target, Config)),
                ok
            catch
                error:{invalid_sync_url, Url} ->
                    {error, {invalid_sync_url, Url}}
            end;
        {false, _} -> {error, missing_source};
        {_, false} -> {error, missing_target}
    end.

generate_task_id() ->
    Rand = crypto:strong_rand_bytes(8),
    binary:encode_hex(Rand, lowercase).

%% An explicit *_transport in the config wins; otherwise remote
%% endpoints (URLs or endpoint maps) get the HTTP transport and
%% database names the local one.
get_transport(source, Config) ->
    maps:get(source_transport, Config,
             get_transport_for_target(maps:get(source, Config)));
get_transport(target, Config) ->
    maps:get(target_transport, Config,
             get_transport_for_target(maps:get(target, Config))).

is_remote(#{url := _}) -> true;
is_remote(Term) when is_binary(Term) ->
    binary:match(Term, <<"://">>) =/= nomatch;
is_remote(_) -> false.

%% Remote endpoints normalize once (URL identity, auth from the
%% sync_auth env); local database names pass through.
resolve_endpoint(Term) ->
    case is_remote(Term) of
        true -> barrel_rep_transport_http:endpoint(Term);
        false -> Term
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
