%% Follow the source database even when nobody is querying the view.
-module(receivables_index).
-behaviour(gen_server).

-export([start_link/2, wait_for/3, refresh/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(BATCH_SIZE, 32).
-define(POLL_MS, 25).

-record(state, {db, instance, apply_change, since = first, waiting = [], timer}).

start_link(DbPid, ApplyChange) ->
    gen_server:start_link(?MODULE, {DbPid, ApplyChange}, []).

%% A freshness wait only observes progress. It never initiates an index pass.
wait_for(Index, #{instance_id := Id, last_seq := <<_:96>>} = Target, Timeout)
        when is_binary(Id), is_integer(Timeout), Timeout >= 0 ->
    case request(Index, {wait_for, Target}, Timeout) of
        {ok, _Checkpoint} -> ok;
        {error, _} = Error -> Error
    end;
wait_for(_Index, _Target, _Timeout) -> {error, invalid_freshness}.

refresh(Index, Timeout) when is_integer(Timeout), Timeout >= 0 ->
    request(Index, refresh, Timeout);
refresh(_Index, _Timeout) -> {error, invalid_timeout}.

request(Index, Operation, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    try gen_server:call(Index, {Operation, Deadline}, Timeout)
    catch
        exit:{timeout, _} -> {error, timeout};
        exit:{Reason, {gen_server, call, _}} -> {error, {index_stopped, Reason}}
    end.

init({DbPid, ApplyChange}) ->
    {ok, Instance} = barrel_docdb:db_instance_id(DbPid),
    monitor(process, DbPid),
    self() ! index,
    {ok, #state{db = DbPid, instance = Instance, apply_change = ApplyChange}}.

handle_call({{wait_for, Target}, Deadline}, From, State) ->
    await_target(wait_for, {From, Target, Deadline}, State);
handle_call({refresh, Deadline}, From, #state{db = Db} = State) ->
    case barrel_docdb:db_observed_version(Db) of
        {ok, Target} -> await_target(refresh, {From, Target, Deadline}, State);
        {error, _} = Error -> {reply, Error, State}
    end.

await_target(Mode, {_, #{instance_id := Id}, _} = Waiter,
             #state{instance = Id, since = Since, waiting = Waiting, timer = Timer} = State) ->
    case unanswered([Waiter], Since) of
        [] -> {noreply, State};
        [Waiter] ->
            case Mode of
                refresh -> wake(Timer);
                wait_for -> ok
            end,
            {noreply, State#state{waiting = [Waiter | Waiting]}}
    end;
await_target(_Mode, _Waiter, State) ->
    {reply, {error, wrong_source}, State}.

%% Startup, an expired timer, or an earlier refresh has already queued the
%% next pass. Only the caller that cancels a live timer needs to wake it.
wake(undefined) -> ok;
wake(Timer) ->
    case erlang:cancel_timer(Timer) of
        false -> ok;
        _Remaining -> self() ! index
    end.

handle_cast(_Message, State) -> {noreply, State}.

handle_info(index, #state{db = Db, since = Since, apply_change = Apply, waiting = Waiting} = State) ->
    {ok, Changes, Next} = barrel_docdb:get_changes(Db, Since,
        #{include_docs => true, limit => ?BATCH_SIZE}),
    lists:foreach(Apply, Changes),
    %% Advance even for paid invoices and deletes. Their removals are indexed work.
    Pending = unanswered(Waiting, Next),
    Delay = case length(Changes) of
        ?BATCH_SIZE -> 0;
        _ -> ?POLL_MS
    end,
    Timer = erlang:send_after(Delay, self(), index),
    {noreply, State#state{since = Next, waiting = Pending, timer = Timer}};
handle_info({'DOWN', _, process, Db, Reason}, #state{db = Db} = State) ->
    {stop, {shutdown, {source_closed, Reason}}, State}.

unanswered([], _Since) -> [];
unanswered(Waiting, Since) ->
    Through = case Since of
        first -> building;
        _ -> barrel_hlc:encode(Since)
    end,
    Now = erlang:monotonic_time(millisecond),
    lists:filter(fun({From, #{last_seq := Seq} = Target, Deadline}) ->
        if
            Now >= Deadline ->
                gen_server:reply(From, {error, timeout}),
                false;
            is_binary(Through), Through >= Seq ->
                gen_server:reply(From, {ok, Target#{last_seq => Through}}),
                false;
            true -> true
        end
    end, Waiting).
