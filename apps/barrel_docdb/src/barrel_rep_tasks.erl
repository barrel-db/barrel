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

-define(MAX_PENDING, 1000).
%% format of the positions this version saves (2.0.0 saved none)
-define(POSITIONS_VSN, 2).
-define(SERVER, ?MODULE).
-define(TASKS_DB, <<"_replication_tasks">>).

%% Task records go through put_local_doc/3 only -- see the identical
%% rationale on barrel_docdb.erl's SYSTEM_DB_OPTS, which this mirrors:
%% this database never stores an attachment or a real (revtree-backed)
%% document, so the stock 64MB write_buffer_size and the unconditional
%% second RocksDB instance barrel_db_server opens for attachments are
%% both sized for a workload this database structurally cannot have.
-define(TASKS_DB_OPTS, #{
    att_opts => #{backend => none, purge_existing => true},
    store_opts => #{write_buffer_size => 4 * 1024 * 1024}
}).

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

%% Bound on how long a stream-subscribed continuous task can go without
%% re-checking attachments: attachments have their own feed, so a doc-only
%% wake signal never fires for an attachment-only write. Separate from the
%% doc-side pacing constants above since it addresses a different concern.
-define(ATT_IDLE_INTERVAL, 5000).

%% Default attachment sync batch size
-define(DEFAULT_ATT_BATCH_SIZE, 100).

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
    wait_for => [binary() | map()],       % Chain: wait for downstream targets
    attachments => boolean(),             % Replicate attachments (default: true)
    att_batch_size => pos_integer(),      % Attachment feed batch size (default: 100)
    att_durability => required | best_effort  % Sync the target before checkpoints (default: required)
}.

-type task() :: #{
    id := task_id(),
    config := task_config(),
    status := task_status(),
    last_seq => seq() | first,
    att_seq => seq() | first,
    error => binary(),
    created_at := integer(),
    updated_at := integer()
}.

-export_type([task_id/0, task_config/0, task/0, task_status/0, task_mode/0]).

-record(state, {
    running = #{} :: #{task_id() => pid()},
    %% tasks restored at boot: their local databases are not open yet
    restored = #{} :: #{task_id() => true}
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
%%
%% Task configs are persisted, so remote endpoints carry no secrets:
%% `auth', `signing', `ssl_options', `headers' or a URL with user info
%% return `{error, {inline_credentials, Keys}}'. Set them in the
%% `sync_auth', `sync_signing' and `sync_ssl' app env instead (see
%% barrel_rep_transport_http). An invalid config returns
%% `{error, Reason}'.
-spec start_task(task_config()) -> {ok, task_id()} | {error, term()}.
start_task(Config) when is_map(Config) ->
    gen_server:call(?SERVER, {start_task, Config});
start_task(_Config) ->
    {error, invalid_config}.

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
            ok = barrel_metrics:set_rep_active(TaskId, false),
            ok = barrel_metrics:set_rep_lag(TaskId, 0),
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
                    {ok, NewState} = spawn_task(Task, State),
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

%% A persisted config is validated again: one that no longer does (a
%% filter that stopped decoding) fails the task instead of running it.
start_task_process(#{id := TaskId, config := Config} = Task, State) ->
    case validate_config(Config) of
        ok ->
            spawn_task(Task, State);
        {error, Reason} ->
            logger:warning("barrel_rep_tasks: task ~s has an invalid "
                           "config: ~p", [TaskId, Reason]),
            _ = update_task_status(TaskId, failed,
                                   #{<<"error">> => format_reason(
                                                      {invalid_config, Reason})}),
            {error, {invalid_config, Reason}}
    end.

spawn_task(#{id := TaskId, config := Config}, State) ->
    %% Get transport modules
    SourceTransport = get_transport(source, Config),
    TargetTransport = get_transport(target, Config),

    %% Link the task to the manager (which traps exits): the task dies
    %% with the manager, and a task crash arrives as {'EXIT',...} without
    %% taking the manager down.
    Self = self(),
    Pid = spawn_link(fun() ->
        run_task(Self, TaskId, Config, SourceTransport, TargetTransport)
    end),

    %% Update status
    _ = update_task_status(TaskId, running),

    NewRunning = maps:put(TaskId, Pid, State#state.running),
    {ok, State#state{running = NewRunning}}.

run_task(Parent, TaskId, Config, SourceTransport, TargetTransport) ->
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
                     || W <- maps:get(wait_for, Config, [])],
        attachments => maps:get(attachments, Config, true),
        att_batch_size => maps:get(att_batch_size, Config,
                                   ?DEFAULT_ATT_BATCH_SIZE),
        att_durability => maps:get(att_durability, Config, required)
    },

    ExtraAttrs = #{
        <<"replication.task_id">> => TaskId,
        <<"replication.direction">> => atom_to_binary(Direction, utf8),
        <<"replication.mode">> => atom_to_binary(Mode, utf8)
    },

    ok = upgrade_positions(TaskId, maps:get(filter, Ctx0)),
    barrel_trace:with_db_span(rep_task, undefined, ExtraAttrs, fun() ->
        %% Run replication based on direction
        case Direction of
            push ->
                %% Source -> Target (read from source, write to target)
                run_direction(Ctx0#{key => <<>>}, Source, Target,
                              SourceTransport, TargetTransport);
            pull ->
                %% Target -> Source (read from target, write to source)
                run_direction(Ctx0#{key => <<>>}, Target, Source,
                              TargetTransport, SourceTransport);
            both ->
                %% Bidirectional: push and pull run concurrently, each
                %% with its own position and pending ids. Both keep the
                %% manager as their parent (Ctx0) so their progress/error
                %% casts are read.
                ok = split_both_positions(TaskId),
                %% The task completes (or fails) once both directions
                %% have: the push side reports to this process.
                Self = self(),
                spawn_link(fun() ->
                    run_direction(Ctx0#{key => <<>>, done_to => Self},
                                  Source, Target,
                                  SourceTransport, TargetTransport)
                end),
                %% Run pull in this process
                run_direction(Ctx0#{key => <<"pull_">>, await_peer => true},
                              Target, Source,
                              TargetTransport, SourceTransport)
        end
    end).

run_direction(#{task_id := TaskId, key := Key} = Ctx, From, To,
              FromTransport, ToTransport) ->
    {Since, Pending} = get_dir_state(TaskId, Key),
    Ctx2 = Ctx#{
        pending => Pending,
        from => From,
        to => To,
        from_transport => FromTransport,
        to_transport => ToTransport,
        wake => init_wake(Ctx, From, Since),
        poll => ?POLL_MIN,
        backoff => ?BACKOFF_MIN
    },
    run_task_loop(retry_pending(Ctx2, Since), Since).

%% Retry the pending ids by id; a success leaves the set, and the set
%% is saved with the current position.
retry_pending(#{pending := []} = Ctx, _Since) ->
    Ctx;
retry_pending(#{pending := Pending, from := From, to := To,
                from_transport := FromTransport,
                to_transport := ToTransport} = Ctx, Since) ->
    case barrel_rep_alg:replicate_ids(From, To, FromTransport, ToTransport,
                                      Pending, #{retry => true}) of
        {ok, #{failed := Failed}} ->
            Ctx1 = Ctx#{pending := lists:usort(Failed)},
            _ = save_dir_state(Ctx1, Since),
            Ctx1;
        {error, _} ->
            Ctx
    end.

%% Continuous local sources wake on the changes stream, subscribed
%% BEFORE the first drain so no write slips between drain and
%% subscribe. Remote (and streamless) sources poll adaptively.
%% The stream is unlinked: it ends with the task through its owner
%% monitor, and a stream that ends leaves the task polling (see
%% wait_for_wake/1) instead of taking it down.
init_wake(#{mode := continuous}, From, Since) when is_binary(From) ->
    case barrel_docdb:subscribe_changes(From, Since,
                                        #{mode => push,
                                          owner => self()}) of
        {ok, Stream} ->
            true = unlink(Stream),
            {stream, Stream};
        {error, _} ->
            poll
    end;
init_wake(_Ctx, _From, _Since) ->
    poll.

run_task_loop(Ctx, Since0) ->
    Since = rewind_to_target(Ctx, Since0),
    #{mode := Mode,
      from := From, to := To,
      from_transport := FromTransport, to_transport := ToTransport,
      batch_size := BatchSize, filter := Filter,
      wait_for := WaitFor} = Ctx,

    %% Changes options carry the whole filter: paths, query, channel
    ChangesOpts = maps:merge(#{limit => BatchSize, replication => true},
                             maps:with([paths, query, channel], Filter)),

    case FromTransport:get_changes(From, Since, ChangesOpts) of
        {ok, [], LastSeq} ->
            case barrel_rep_checkpoint:empty_page_advanced(Since, LastSeq) of
                true ->
                    %% a filtered page with nothing to copy: move on
                    ok = set_lag(Ctx, caught_up),
                    ok = mark_target(Ctx, LastSeq),
                    _ = save_dir_state(Ctx, LastSeq),
                    run_task_loop(Ctx, LastSeq);
                false ->
                    ok = set_lag(Ctx, caught_up),
                    idle_task_loop(Ctx, Since)
            end;

        {ok, Changes, LastSeq} ->
            case barrel_rep_checkpoint:seq_advanced(Since, LastSeq) of
                false ->
                    %% Non-empty batch that did not advance the sequence: a
                    %% non-conforming source. Continuous backs off, one-shot
                    %% fails, instead of spinning at 100% CPU forever.
                    handle_loop_error(Ctx, Since, no_progress);
                true ->
                    case replicate_batch(From, To, FromTransport, ToTransport,
                                         Changes, WaitFor, Ctx) of
                        {ok, Ctx1} ->
                            %% Persist the position and pending ids here
                            %% in the task process, off the manager loop.
                            ok = mark_target(Ctx1, LastSeq),
                            _ = save_dir_state(Ctx1, LastSeq),
                            ok = set_lag(Ctx1, LastSeq),
                            Ctx2 = Ctx1,
                            case Mode of
                                continuous ->
                                    %% Attachments have no retry/backoff
                                    %% risk for one-shot the way docs do;
                                    %% the doc watermark is already durably
                                    %% advanced above, so a broken
                                    %% attachment target never stalls
                                    %% healthy doc replication behind it.
                                    case sync_attachments(Ctx) of
                                        ok ->
                                            run_task_loop(reset_pacing(Ctx2),
                                                         LastSeq);
                                        {error, AttReason} ->
                                            handle_loop_error(
                                                Ctx2, LastSeq,
                                                {att_sync_failed, AttReason})
                                    end;
                                one_shot ->
                                    run_task_loop(reset_pacing(Ctx2), LastSeq)
                            end;
                        {error, Reason} ->
                            handle_loop_error(Ctx, Since, Reason)
                    end
            end;
        {error, Reason} ->
            handle_loop_error(Ctx, Since, Reason)
    end.

%% No progress on the doc side: one-shot finishes, continuous waits.
%% Replication lag: 0 once caught up, else how long ago the last copied
%% change was written on the source.
set_lag(#{task_id := TaskId}, caught_up) ->
    barrel_metrics:set_rep_lag(TaskId, 0);
set_lag(#{task_id := TaskId}, Seq) ->
    Lag = try barrel_hlc:wall_time(Seq) of
              Wall -> max(0, erlang:system_time(millisecond) - Wall) / 1000
          catch _:_ -> 0
          end,
    barrel_metrics:set_rep_lag(TaskId, Lag).

idle_task_loop(#{task_id := TaskId, mode := one_shot} = Ctx, _Since) ->
    %% No more doc changes: run the attachment phase to completion once
    %% (mirrors barrel_rep:replicate_one_shot/2's "doc phase to
    %% completion, then one attachment pass" shape) before reporting the
    %% task done.
    case {sync_attachments(Ctx), maps:get(pending, Ctx)} of
        {ok, [_ | _] = Pending} ->
            report_done(Ctx, {task_error, TaskId, {docs_pending, Pending}});
        {ok, []} ->
            report_done(Ctx, {task_complete, TaskId});
        {{error, Reason}, _} ->
            report_done(Ctx, {task_error, TaskId, {att_sync_failed, Reason}})
    end;
idle_task_loop(Ctx, Since) ->
    %% Idle on the doc side. Attachments have their own feed, so a
    %% doc-only wake signal never fires for an attachment-only write:
    %% catch those up here too before waiting again. Poll
    %% mode already revisits this branch on every sleep cycle; stream
    %% mode's bounded wake (see wait_for_wake/1) is what makes it revisit
    %% this branch when idle on a live stream.
    case sync_attachments(Ctx) of
        ok ->
            run_task_loop(wait_for_wake(retry_pending(Ctx, Since)), Since);
        {error, Reason} ->
            %% the attachment position stays; retry after a backoff
            backoff(Ctx, Since, {att_sync_failed, Reason})
    end.

%% The task's position lives with the task. A target that loses power
%% rolls back to a prefix of its writes, so the task also leaves a marker
%% in the target after each batch: a marker that is gone or behind the
%% position means the target lost changes it had acknowledged, and the
%% task replays from the marker.
target_mark_id(TaskId, Key) ->
    <<"replication-task-", Key/binary, TaskId/binary>>.

mark_target(#{task_id := TaskId, key := Key, to := To,
              to_transport := ToTransport}, Seq) ->
    case ToTransport:put_local_doc(To, target_mark_id(TaskId, Key),
                                   #{<<"last_seq">> => format_seq(Seq)}) of
        ok ->
            ok;
        {error, Reason} ->
            %% the next batch finds the marker behind and sends again
            logger:warning("barrel_rep_tasks: ~s: marking the target "
                           "failed: ~p", [TaskId, Reason]),
            ok
    end.

rewind_to_target(_Ctx, first) ->
    first;
rewind_to_target(#{task_id := TaskId, key := Key, to := To,
                   to_transport := ToTransport}, Since) ->
    case ToTransport:get_local_doc(To, target_mark_id(TaskId, Key)) of
        {ok, #{<<"last_seq">> := Bin}} ->
            Mark = parse_seq(Bin),
            case barrel_rep_checkpoint:seq_advanced(Mark, Since) of
                true -> rewound(TaskId, Since, Mark);
                false -> Since
            end;
        {error, not_found} ->
            rewound(TaskId, Since, first);
        {error, _} ->
            %% unreachable: the batch fails and backs off
            Since
    end.

rewound(TaskId, Since, Mark) ->
    logger:warning("barrel_rep_tasks: ~s: the target is behind the task "
                   "(~p < ~p), replaying from its position",
                   [TaskId, Mark, Since]),
    Mark.

replicate_batch(From, To, FromTransport, ToTransport, Changes, WaitFor,
                #{pending := Pending0} = Ctx) ->
    case barrel_rep_alg:replicate(From, To, FromTransport, ToTransport,
                                  Changes) of
        {ok, #{failed := Failed}} ->
            Pending = lists:usort(Pending0 ++ Failed),
            case length(Pending) > ?MAX_PENDING of
                true ->
                    %% the saved position stands; nothing is skipped
                    {error, too_many_failed_docs};
                false ->
                    %% If wait_for is specified, verify docs reached
                    %% downstream
                    case wait_for_downstream(Changes, WaitFor) of
                        ok ->
                            case batch_attachments(Ctx, Changes) of
                                ok -> {ok, Ctx#{pending := Pending}};
                                {error, _} = Error -> Error
                            end;
                        {error, _} = Error -> Error
                    end
            end;
        {error, _} = Error ->
            Error
    end.

%% @doc Attachment phase for a task: independent lifecycle + checkpoint
%% (own watermark persisted on the task's own doc), same shape as
%% barrel_rep:replicate_one_shot/2's post-doc-phase attachment sync and
%% barrel_timeline's merge_attachments/4 (caller-owned checkpoint via
%% sync_from/7). Skips entirely when the task config disabled it.
sync_attachments(#{attachments := false}) ->
    ok;
sync_attachments(#{task_id := TaskId, key := Key, from := From, to := To,
                   from_transport := FromTransport,
                   to_transport := ToTransport} = Ctx) ->
    AttSince = get_task_att_seq(TaskId, Key),
    CheckpointFun = fun(Seq) -> update_task_att_seq(TaskId, Key, Seq) end,
    case barrel_rep_att:sync_from(From, To, FromTransport, ToTransport,
                                  AttSince, CheckpointFun, att_opts(Ctx)) of
        {ok, _Stats} -> ok;
        skipped -> ok;
        {error, _} = Error -> Error
    end.

att_opts(#{att_batch_size := AttBatchSize, filter := Filter} = Ctx) ->
    #{att_batch_size => AttBatchSize, filter => Filter,
      att_durability => maps:get(att_durability, Ctx, required)}.

%% A filtered task copies the attachments of the documents it copies:
%% their feed rows may have been passed while they were out of scope.
batch_attachments(#{attachments := true, filter := Filter} = Ctx, Changes)
        when map_size(Filter) > 0 ->
    #{from := From, to := To, from_transport := FromTransport,
      to_transport := ToTransport} = Ctx,
    Ids = [Id || #{id := Id} = C <- Changes, not maps:get(deleted, C, false)],
    Prepared = barrel_rep_att:prepare(From, To, FromTransport, ToTransport,
                                      att_opts(Ctx)),
    Synced = case Prepared of
        {ok, AttCtx} -> barrel_rep_att:sync_docs(AttCtx, Ids);
        skipped -> {ok, #{}};
        {error, _} = PrepError -> PrepError
    end,
    case Synced of
        {ok, _} -> ok;
        {error, Reason} -> {error, {att_sync_failed, Reason}}
    end;
batch_attachments(_Ctx, _Changes) ->
    ok.

%% Continuous tasks ride out transient errors: record last_error on
%% the task doc (status stays running) and back off exponentially
%% with jitter. One-shot keeps fail-fast.
%% A continuous task whose local database closed stops as the stream
%% does, so the manager pauses it until the database reopens; a polling
%% task would otherwise retry a closed database forever.
handle_loop_error(#{mode := continuous, from := From, to := To} = Ctx,
                  Since, Reason) ->
    case locals_open_config(#{source => From, target => To}) of
        true -> backoff(Ctx, Since, Reason);
        false -> exit({shutdown, db_closed})
    end;
handle_loop_error(#{task_id := TaskId} = Ctx, _Since, Reason) ->
    report_done(Ctx, {task_error, TaskId, Reason}).

%% End of a one-shot direction. The push side of a `both' task reports
%% to the pull side, which reports for the task once both are done (an
%% error from either wins).
report_done(#{done_to := Peer}, Outcome) ->
    Peer ! {direction_done, Outcome},
    ok;
report_done(#{await_peer := true, parent := Parent}, Outcome) ->
    receive
        {direction_done, PeerOutcome} ->
            gen_server:cast(Parent, worst(Outcome, PeerOutcome))
    end;
report_done(#{parent := Parent}, Outcome) ->
    gen_server:cast(Parent, Outcome).

worst({task_complete, _}, Other) -> Other;
worst(Error, _) -> Error.

backoff(Ctx, Since, Reason) ->
    #{parent := Parent, task_id := TaskId, backoff := Backoff} = Ctx,
    gen_server:cast(Parent, {task_last_error, TaskId, Reason}),
    timer:sleep(with_jitter(Backoff)),
    run_task_loop(Ctx#{backoff := min(Backoff * 2, ?BACKOFF_MAX)}, Since).

with_jitter(Ms) ->
    Ms + rand:uniform(max(Ms div 4, 1)).

reset_pacing(Ctx) ->
    Ctx#{poll := ?POLL_MIN, backoff := ?BACKOFF_MIN}.

%% Idle continuous task. Stream-woken: the stream payload is
%% unfiltered, so it is a wake-up signal only; ack immediately and
%% drain through the transport with the task's filter. The await is
%% bounded (not infinity): attachments have no doc-feed wake signal of
%% their own, so a periodic timeout is what lets run_task_loop's idle
%% branch re-check attachments even when no doc change ever arrives.
%% Polling: adaptive interval, doubled while idle.
wait_for_wake(#{wake := {stream, Stream}} = Ctx) ->
    case barrel_changes_stream:await(Stream, ?ATT_IDLE_INTERVAL) of
        {ReqId, _Changes} ->
            ok = barrel_changes_stream:ack(Stream, ReqId),
            Ctx;
        timeout ->
            %% no doc changes yet; still alive, just re-check next time
            Ctx;
        down ->
            %% stream went away: degrade to polling
            Ctx#{wake := poll}
    end;
wait_for_wake(#{wake := poll, poll := Poll} = Ctx) ->
    timer:sleep(Poll),
    Ctx#{poll := min(Poll * 2, ?POLL_MAX)}.

%% @doc Wait for the replicated versions to reach the downstream
%% targets: each target must answer `have' for every version (a delete
%% counts once its stub is there); missing ids are retried.
wait_for_downstream(_Changes, []) ->
    ok;
wait_for_downstream(Changes, WaitFor) ->
    Versions = maps:from_list(
        [{DocId, Rev} || #{id := DocId, rev := Rev} <- Changes] ++
        [{DocId, Rev} || #{<<"id">> := DocId, <<"rev">> := Rev} <- Changes]),
    verify_at_targets([{T, Versions} || T <- WaitFor], 10, 500).

verify_at_targets([], _Retries, _Delay) ->
    ok;
verify_at_targets(_Pending, 0, _Delay) ->
    {error, wait_for_timeout};
verify_at_targets(Pending, Retries, Delay) ->
    case [{T, V} || {T, V0} <- Pending,
                    V <- [missing_at(T, V0)], map_size(V) > 0] of
        [] ->
            ok;
        Still ->
            timer:sleep(Delay),
            verify_at_targets(Still, Retries - 1, Delay)
    end.

%% The versions the target does not have yet; an error retries all.
missing_at(Target, Versions) ->
    Transport = get_transport_for_target(Target),
    case Transport:diff_versions(Target, Versions) of
        {ok, Diff} ->
            maps:filter(fun(Id, _) -> maps:get(Id, Diff, missing) =/= have end,
                        Versions);
        {error, _} ->
            Versions
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
            {Status, Extra} = down_status(Reason, TaskId),
            _ = update_task_status(
                  TaskId, Status,
                  Extra#{<<"error">> => format_reason(Reason)}),
            NewRunning = maps:remove(TaskId, State#state.running),
            State#state{running = NewRunning};
        error ->
            State
    end.

%% A local database that closed pauses the task until it reopens. The
%% close can also surface as a crash (a read on the closed store), so
%% any other exit counts as a close when a local database is gone: a
%% database unregisters before it closes its store.
down_status(normal, _TaskId) -> {completed, #{}};
down_status(shutdown, _TaskId) -> {paused, #{}};
down_status({shutdown, db_closed}, _TaskId) -> db_closed();
down_status(_Reason, TaskId) ->
    case locals_open(TaskId) of
        true -> {failed, #{}};
        false -> db_closed()
    end.

db_closed() ->
    {paused, #{<<"paused_reason">> => <<"db_closed">>}}.

locals_open(TaskId) ->
    case do_get_task(TaskId) of
        {ok, #{config := Config}} -> locals_open_config(Config);
        {error, _} -> true
    end.

locals_open_config(Config) ->
    lists:all(fun local_open/1, [maps:get(source, Config),
                                 maps:get(target, Config)]).

find_task_by_pid(Pid, #state{running = Running}) ->
    case lists:keyfind(Pid, 2, maps:to_list(Running)) of
        {TaskId, _} -> {ok, TaskId};
        false -> error
    end.

check_running_tasks(State0) ->
    State = resume_reopened(State0),
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
                            started(start_task_process(Task, AccState),
                                    AccState)
                    end
                end,
                State,
                Tasks
            );
        {error, _} ->
            State
    end.

started({ok, State}, _Old) -> State;
started({error, _}, Old) -> Old.

%% Resume the tasks paused by a database close once their local
%% databases are open again.
resume_reopened(State) ->
    case do_list_tasks(#{status => paused}, State) of
        {ok, Tasks} ->
            lists:foldl(fun resume_reopened/2, State, Tasks);
        {error, _} ->
            State
    end.

resume_reopened(#{id := TaskId, paused_reason := <<"db_closed">>,
                  config := Config}, State) ->
    case locals_open_config(Config) of
        true ->
            case do_resume_task(TaskId, State) of
                {ok, NewState} ->
                    Restored = maps:remove(TaskId, NewState#state.restored),
                    NewState#state{restored = Restored};
                {error, _} ->
                    State
            end;
        false ->
            _ = maps:is_key(TaskId, State#state.restored)
                andalso open_locals(Config),
            State
    end;
resume_reopened(_Task, State) ->
    State.

%% After a restart a task's databases are closed until something opens
%% them. The `task_db_opener' env, `{M, F}' called as `M:F(Name)', opens
%% one the way the application does (barrel_server sets it). Only a
%% database still on disk is opened.
open_locals(Config) ->
    case application:get_env(barrel_docdb, task_db_opener) of
        {ok, {M, F}} ->
            [open_local(M, F, E) || E <- [maps:get(source, Config),
                                          maps:get(target, Config)],
                                    not local_open(E)],
            true;
        undefined ->
            true
    end.

open_local(M, F, Name) when is_binary(Name) ->
    case barrel_docdb:db_exists(Name, #{}) of
        true ->
            try M:F(Name)
            catch C:R ->
                logger:warning("barrel_rep_tasks: opening ~s failed: ~p",
                               [Name, {C, R}])
            end;
        false ->
            ok
    end;
open_local(_M, _F, _Endpoint) ->
    ok.

local_open(Endpoint) ->
    case is_remote(Endpoint) of
        true -> true;
        false -> local_running(barrel_docdb:db_pid(Endpoint))
    end.

local_running({ok, _Pid}) -> true;
local_running({error, _}) -> false.

%%====================================================================
%% Internal - Persistence
%%====================================================================

ensure_tasks_db() ->
    case barrel_docdb:db_info(?TASKS_DB) of
        {ok, _} ->
            ok;
        {error, not_found} ->
            case barrel_docdb:create_db(?TASKS_DB, ?TASKS_DB_OPTS) of
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
                fun(#{id := Id} = Task, AccState) ->
                    case start_task_process(Task, AccState) of
                        {ok, NewState} ->
                            Restored = NewState#state.restored,
                            NewState#state{restored = Restored#{Id => true}};
                        {error, _} ->
                            AccState
                    end
                end,
                State,
                Tasks
            );
        {error, _} ->
            State
    end.

save_task(#{id := TaskId} = Task) ->
    Doc = task_to_doc(Task),
    %% synced: a task the caller was told exists survives a power cut
    barrel_docdb:put_local_doc(?TASKS_DB, task_doc_id(TaskId), Doc,
                               #{sync => true}).

update_task_status(TaskId, Status) ->
    update_task_status(TaskId, Status, #{}).

%% A status change drops the pause reason unless Extra sets it again.
update_task_status(TaskId, Status, Extra) ->
    ok = barrel_metrics:set_rep_active(TaskId, Status =:= running),
    update_task_doc(TaskId, fun(Doc) ->
        Doc1 = maps:remove(<<"paused_reason">>, Doc),
        maps:merge(Doc1#{<<"status">> => atom_to_binary(Status)}, Extra)
    end, #{sync => true}).

%% A direction's position and pending ids, saved together. `Key' is
%% <<>> for push (and single-direction tasks), <<"pull_">> for the pull
%% side of a `both' task.
save_dir_state(#{task_id := TaskId, key := Key, pending := Pending}, Seq) ->
    update_task_doc(TaskId, fun(Doc) ->
        Doc#{<<Key/binary, "last_seq">> => format_seq(Seq),
             <<Key/binary, "pending">> => Pending}
    end).

get_dir_state(TaskId, Key) ->
    case barrel_docdb:get_local_doc(?TASKS_DB, task_doc_id(TaskId)) of
        {ok, Doc} ->
            {parse_seq(maps:get(<<Key/binary, "last_seq">>, Doc,
                                <<"first">>)),
             [Id || Id <- maps:get(<<Key/binary, "pending">>, Doc, []),
                    is_binary(Id)]};
        {error, _} ->
            {first, []}
    end.

%% Pre-2.1 positions may sit past rows never copied (attachments, and
%% docs when filtered): they restart from first once, pending ids kept.
upgrade_positions(TaskId, Filter) ->
    _ = update_task_doc(TaskId, fun(Doc) ->
        upgrade_doc(TaskId, map_size(Filter) > 0, Doc)
    end),
    ok.

upgrade_doc(_TaskId, _Filtered, #{<<"vsn">> := V} = Doc)
        when is_integer(V), V >= ?POSITIONS_VSN ->
    Doc;
upgrade_doc(TaskId, Filtered, Doc) ->
    Keys = [<<"att_seq">>, <<"pull_att_seq">>]
        ++ [K || Filtered, K <- [<<"last_seq">>, <<"pull_last_seq">>]],
    First = format_seq(first),
    Moved = [K || K <- Keys, maps:get(K, Doc, First) =/= First],
    case Moved of
        [] -> ok;
        _ -> logger:warning("barrel_rep_tasks: ~s: pre-2.1 positions ~p "
                            "reset, replaying from the start once (2.0 "
                            "could save positions past rows it never "
                            "copied)", [TaskId, Moved])
    end,
    maps:merge(Doc#{<<"vsn">> => ?POSITIONS_VSN},
               maps:from_list([{K, First} || K <- Moved])).

%% A `both' task saved before positions were split shares one last_seq
%% (and one att_seq) across both feeds, which can skip either side's
%% rows: both directions restart from first once.
split_both_positions(TaskId) ->
    update_task_doc(TaskId, fun(Doc) -> split_att(split_docs(Doc)) end),
    ok.

split_docs(#{<<"pull_last_seq">> := _} = Doc) -> Doc;
split_docs(Doc) -> Doc#{<<"last_seq">> => format_seq(first),
                        <<"pull_last_seq">> => format_seq(first)}.

split_att(#{<<"pull_att_seq">> := _} = Doc) -> Doc;
split_att(Doc) -> Doc#{<<"att_seq">> => format_seq(first),
                       <<"pull_att_seq">> => format_seq(first)}.

%% Attachment positions per direction, keyed like save_dir_state/2.
update_task_att_seq(TaskId, Key, Seq) ->
    case update_task_doc(TaskId, fun(Doc) ->
             Doc#{<<Key/binary, "att_seq">> => format_seq(Seq)}
         end) of
        ok -> ok;
        {error, _} = Error -> Error;
        Other -> {error, Other}
    end.

get_task_att_seq(TaskId, Key) ->
    case barrel_docdb:get_local_doc(?TASKS_DB, task_doc_id(TaskId)) of
        {ok, Doc} ->
            parse_seq(maps:get(<<Key/binary, "att_seq">>, Doc, <<"first">>));
        {error, not_found} ->
            first
    end.

update_task_last_error(TaskId, Reason) ->
    update_task_doc(TaskId, fun(Doc) ->
        Doc#{<<"last_error">> => format_reason(Reason)}
    end).

%% Read-modify-write of the task doc, serialized per task: the manager
%% and a `both' task's two directions update it concurrently.
%% Positions are written unsynced: one lost in a power cut only makes
%% the task send again. Status changes are synced.
update_task_doc(TaskId, Fun) ->
    update_task_doc(TaskId, Fun, #{}).

update_task_doc(TaskId, Fun, Opts) ->
    global:trans({{?MODULE, TaskId}, self()},
                 fun() -> do_update_task_doc(TaskId, Fun, Opts) end,
                 [node()]).

do_update_task_doc(TaskId, Fun, Opts) ->
    case barrel_docdb:get_local_doc(?TASKS_DB, task_doc_id(TaskId)) of
        {ok, Doc} ->
            Doc2 = Fun(Doc),
            Doc3 = Doc2#{<<"updated_at">> =>
                             erlang:system_time(millisecond)},
            barrel_docdb:put_local_doc(?TASKS_DB, task_doc_id(TaskId),
                                       Doc3, Opts);
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
        <<"att_seq">> => format_seq(maps:get(att_seq, Task, first)),
        <<"vsn">> => ?POSITIONS_VSN,
        <<"created_at">> => CreatedAt,
        <<"updated_at">> => UpdatedAt
    }.

doc_to_task(Doc) ->
    Task = #{
        id => maps:get(<<"id">>, Doc),
        config => map_to_config(maps:get(<<"config">>, Doc)),
        status => binary_to_existing_atom(maps:get(<<"status">>, Doc), utf8),
        last_seq => parse_seq(maps:get(<<"last_seq">>, Doc, <<"first">>)),
        %% Unconditional, defaulted like last_seq (not the maps:fold below,
        %% which is only for genuinely-absent-until-first-error fields): a
        %% task doc written before this field existed must still parse,
        %% defaulting to a full attachment backfill.
        att_seq => parse_seq(maps:get(<<"att_seq">>, Doc, <<"first">>)),
        created_at => maps:get(<<"created_at">>, Doc),
        updated_at => maps:get(<<"updated_at">>, Doc)
    },
    maps:fold(
        fun(<<"error">>, V, Acc) -> Acc#{error => V};
           (<<"last_error">>, V, Acc) -> Acc#{last_error => V};
           (<<"pull_att_seq">>, V, Acc) -> Acc#{pull_att_seq => parse_seq(V)};
           (<<"paused_reason">>, V, Acc) -> Acc#{paused_reason => V};
           (_, _, Acc) -> Acc
        end,
        Task,
        Doc).

%% The persisted config uses binary keys and wire-safe values: enums
%% as binaries, the filter through barrel_rep_filter, and remote
%% endpoints by their URL and tuning keys (no secrets in task docs;
%% credentials resolve from the app env at task start).
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
           (attachments, V, Acc) -> Acc#{<<"attachments">> => V};
           (att_batch_size, V, Acc) -> Acc#{<<"att_batch_size">> => V};
           (att_durability, V, Acc) ->
               Acc#{<<"att_durability">> => atom_to_binary(V, utf8)};
           (_, _, Acc) -> Acc
        end,
        #{},
        Config).

endpoint_to_doc(#{url := Url} = Endpoint) ->
    maps:fold(fun(pool, V, Acc) -> Acc#{<<"pool">> => atom_to_binary(V)};
                 (request_timeout, infinity, Acc) ->
                      Acc#{<<"request_timeout">> => <<"infinity">>};
                 (K, V, Acc) when K =:= connect_timeout;
                                  K =:= recv_timeout;
                                  K =:= request_timeout ->
                      Acc#{atom_to_binary(K) => V};
                 (_, _, Acc) -> Acc
              end, #{<<"url">> => Url}, Endpoint);
endpoint_to_doc(Other) ->
    Other.

%% Reads both the map form and the older URL-only form.
endpoint_from_doc(#{<<"url">> := Url} = Doc) ->
    maps:fold(fun(<<"pool">>, V, Acc) when is_binary(V) ->
                      Acc#{pool => binary_to_atom(V)};
                 (<<"request_timeout">>, <<"infinity">>, Acc) ->
                      Acc#{request_timeout => infinity};
                 (K, V, Acc) when K =:= <<"connect_timeout">>;
                                  K =:= <<"recv_timeout">>;
                                  K =:= <<"request_timeout">> ->
                      Acc#{binary_to_atom(K) => V};
                 (_, _, Acc) -> Acc
              end, #{url => Url}, Doc);
endpoint_from_doc(Other) ->
    Other.

%% An unknown enum stays a binary, which validation then rejects.
enum_from_doc(V, Allowed) ->
    case [A || A <- Allowed, atom_to_binary(A) =:= V] of
        [A] -> A;
        [] -> V
    end.

module_from_doc(V) when is_binary(V) ->
    try binary_to_existing_atom(V)
    catch error:badarg -> V
    end;
module_from_doc(V) ->
    V.

map_to_config(Map) ->
    maps:fold(
        fun(<<"source">>, V, Acc) -> Acc#{source => endpoint_from_doc(V)};
           (<<"target">>, V, Acc) -> Acc#{target => endpoint_from_doc(V)};
           (<<"mode">>, V, Acc) ->
               Acc#{mode => enum_from_doc(V, [one_shot, continuous])};
           (<<"direction">>, V, Acc) ->
               Acc#{direction => enum_from_doc(V, [push, pull, both])};
           (<<"source_transport">>, V, Acc) ->
               Acc#{source_transport => module_from_doc(V)};
           (<<"target_transport">>, V, Acc) ->
               Acc#{target_transport => module_from_doc(V)};
           (<<"batch_size">>, V, Acc) -> Acc#{batch_size => V};
           (<<"filter">>, V, Acc) ->
               %% an undecodable filter is kept as such: validation
               %% fails the task rather than running it unfiltered
               case barrel_rep_filter:from_wire(V) of
                   {ok, Filter} -> Acc#{filter => Filter};
                   {error, Reason} -> Acc#{filter => {undecodable, Reason}}
               end;
           (<<"wait_for">>, V, Acc) when is_list(V) ->
               Acc#{wait_for => [endpoint_from_doc(W) || W <- V]};
           (<<"wait_for">>, V, Acc) -> Acc#{wait_for => V};
           (<<"attachments">>, V, Acc) -> Acc#{attachments => V};
           (<<"att_batch_size">>, V, Acc) -> Acc#{att_batch_size => V};
           (<<"att_durability">>, V, Acc) ->
               Acc#{att_durability => enum_from_doc(V, [required,
                                                        best_effort])};
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

%% Total: every bad shape is an error, never a crash of the manager.
%% Unknown keys are left alone.
validate_config(Config) when is_map(Config) ->
    try
        Source = required(source, Config, missing_source),
        Target = required(target, Config, missing_target),
        WaitFor = case maps:get(wait_for, Config, []) of
            L when is_list(L) -> L;
            Bad -> throw({invalid, {invalid_option, {wait_for, Bad}}})
        end,
        case lists:usort(lists:append(
                 [credential_keys(E) || E <- [Source, Target | WaitFor]])) of
            [] -> ok;
            Keys -> throw({invalid, {inline_credentials, Keys}})
        end,
        ok = check_endpoint(Source, maps:get(source_transport, Config, undefined)),
        ok = check_endpoint(Target, maps:get(target_transport, Config, undefined)),
        lists:foreach(fun(W) -> check_endpoint(W, undefined) end, WaitFor),
        maps:foreach(fun check_option/2, Config)
    catch
        throw:{invalid, Reason} -> {error, Reason}
    end;
validate_config(_Config) ->
    {error, invalid_config}.

required(Key, Config, Missing) ->
    case Config of
        #{Key := V} -> V;
        _ -> throw({invalid, Missing})
    end.

%% Secrets an endpoint would persist in the task doc.
credential_keys(#{url := Url} = Endpoint) ->
    secret_keys(Endpoint) ++ userinfo_keys(Url);
credential_keys(Endpoint) when is_map(Endpoint) ->
    secret_keys(Endpoint);
credential_keys(Url) when is_binary(Url) ->
    userinfo_keys(Url);
credential_keys(_Endpoint) ->
    [].

secret_keys(Endpoint) ->
    [K || K <- [auth, signing, ssl_options, headers], maps:is_key(K, Endpoint)].

userinfo_keys(Url) ->
    case barrel_rep_transport_http:has_userinfo(Url) of
        true -> [userinfo];
        false -> []
    end.

%% A custom transport owns its endpoint shape.
check_endpoint(Endpoint, Transport) when Transport =:= undefined;
                                         Transport =:= barrel_rep_transport_local;
                                         Transport =:= barrel_rep_transport_http ->
    case is_remote(Endpoint) of
        true ->
            try _ = barrel_rep_transport_http:endpoint(Endpoint), ok
            catch error:_ -> throw({invalid, {invalid_endpoint, Endpoint}})
            end;
        false when is_binary(Endpoint) ->
            case barrel_docdb:validate_db_name(Endpoint) of
                ok -> ok;
                {error, _} -> throw({invalid, {invalid_endpoint, Endpoint}})
            end;
        false ->
            throw({invalid, {invalid_endpoint, Endpoint}})
    end;
check_endpoint(_Endpoint, _Transport) ->
    ok.

check_option(mode, V) when V =:= one_shot; V =:= continuous -> ok;
check_option(direction, V) when V =:= push; V =:= pull; V =:= both -> ok;
check_option(batch_size, V) when is_integer(V), V > 0 -> ok;
check_option(att_batch_size, V) when is_integer(V), V > 0 -> ok;
check_option(attachments, V) when is_boolean(V) -> ok;
check_option(att_durability, V) when V =:= required; V =:= best_effort -> ok;
check_option(source_transport, V) when is_atom(V) -> ok;
check_option(target_transport, V) when is_atom(V) -> ok;
check_option(filter, V) ->
    case check_filter(V) of
        ok -> ok;
        {error, Reason} -> throw({invalid, Reason})
    end;
check_option(K, V) when K =:= mode; K =:= direction; K =:= batch_size;
                        K =:= att_batch_size; K =:= attachments;
                        K =:= att_durability;
                        K =:= source_transport; K =:= target_transport ->
    throw({invalid, {invalid_option, {K, V}}});
check_option(_K, _V) ->
    ok.

%% The filter must compile and survive the task doc round trip.
check_filter(Filter) when is_map(Filter) ->
    try
        case barrel_rep_filter:check(Filter) of
            ok ->
                case barrel_rep_filter:from_wire(
                       barrel_rep_filter:to_wire(Filter)) of
                    {ok, _} -> ok;
                    {error, Reason} -> {error, {invalid_filter, Reason}}
                end;
            {error, _} = Err ->
                Err
        end
    catch
        _:R -> {error, {invalid_filter, R}}
    end;
check_filter(Filter) ->
    {error, {invalid_filter, Filter}}.

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
