%%%-------------------------------------------------------------------
%%% @doc Changes stream for barrel_docdb
%%%
%%% A gen_statem process that provides a streaming interface to the
%%% changes feed. Supports two modes:
%%% - iterate: Pull-based, client calls next/1 to get changes
%%% - push: Push-based, changes are sent to subscriber
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_changes_stream).

-behaviour(gen_statem).

-include("barrel_docdb.hrl").

%% API
-export([
    start_link/3,
    next/1,
    stop/1
]).

%% Push mode API
-export([
    await/1,
    await/2,
    ack/2
]).

%% gen_statem callbacks
-export([
    init/1,
    callback_mode/0,
    terminate/3
]).

%% State functions
-export([
    iterate/3,
    push/3,
    wait_pending/3
]).

-define(CHANGES_INTERVAL, 100).
-define(BATCH_SIZE, 100).
-define(MAX_PENDING, 5).

%%====================================================================
%% Types
%%====================================================================

-type stream_mode() :: iterate | push.
-type stream_opts() :: #{
    since => seq() | first,
    mode => stream_mode(),
    include_docs => boolean(),
    interval => pos_integer(),
    batch_size => pos_integer(),
    owner => pid()
}.

-export_type([stream_mode/0, stream_opts/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Start a changes stream
-spec start_link(barrel_store_rocksdb:db_ref(), db_name(), stream_opts()) ->
    {ok, pid()} | {error, term()}.
start_link(StoreRef, DbName, Opts) ->
    gen_statem:start_link(?MODULE, [StoreRef, DbName, Opts], []).

%% @doc Get the next change (iterate mode)
-spec next(pid()) -> {ok, barrel_changes:change()} | done | {error, term()}.
next(Pid) ->
    Tag = make_ref(),
    gen_statem:cast(Pid, {next, {self(), Tag}}),
    receive
        {Tag, Result} -> Result
    after 5000 ->
        {error, timeout}
    end.

%% @doc Wait for changes (push mode)
-spec await(pid()) -> {reference(), [barrel_changes:change()]} | [].
await(Pid) ->
    await(Pid, infinity).

%% @doc Wait for changes with timeout (push mode)
-spec await(pid(), timeout()) -> {reference(), [barrel_changes:change()]} | [].
await(Pid, Timeout) ->
    MRef = monitor(process, Pid),
    receive
        {changes, ReqId, Changes} ->
            demonitor(MRef, [flush]),
            {ReqId, Changes};
        {'DOWN', MRef, process, _, _Reason} ->
            []
    after Timeout ->
        demonitor(MRef, [flush]),
        []
    end.

%% @doc Acknowledge receipt of changes (push mode)
-spec ack(pid(), reference()) -> ok.
ack(Pid, ReqId) ->
    gen_statem:cast(Pid, {ack, ReqId}).

%% @doc Stop the stream
-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_statem:cast(Pid, stop).

%%====================================================================
%% gen_statem callbacks
%%====================================================================

callback_mode() -> state_functions.

init([StoreRef, DbName, Opts]) ->
    Since = maps:get(since, Opts, first),
    Mode = maps:get(mode, Opts, iterate),
    IncludeDocs = maps:get(include_docs, Opts, false),

    BaseState = #{
        store_ref => StoreRef,
        db_name => DbName,
        since => Since,
        include_docs => IncludeDocs
    },

    case Mode of
        iterate ->
            {ok, iterate, BaseState};
        push ->
            Interval = maps:get(interval, Opts, ?CHANGES_INTERVAL),
            BatchSize = maps:get(batch_size, Opts, ?BATCH_SIZE),
            Owner = maps:get(owner, Opts, undefined),
            PushState = BaseState#{
                interval => Interval,
                batch_size => BatchSize,
                owner => Owner,
                pending => []
            },
            {ok, push, PushState, [{next_event, info, send_changes}]}
    end.

terminate(_Reason, _State, _Data) ->
    ok.

%%====================================================================
%% State: iterate (pull mode)
%%====================================================================

iterate(cast, {next, {From, Tag}}, #{store_ref := StoreRef,
                                     db_name := DbName,
                                     since := Since} = State) ->
    FoldFun = fun(Change, _Acc) -> {stop, {found, Change}} end,
    case barrel_changes:fold_changes(StoreRef, DbName, Since, FoldFun, not_found) of
        {ok, {found, Change}, NewSeq} ->
            From ! {Tag, {ok, Change}},
            {keep_state, State#{since => NewSeq}};
        {ok, not_found, _} ->
            From ! {Tag, done},
            {stop, normal, State}
    end;

iterate(cast, stop, State) ->
    {stop, normal, State};

iterate(_EventType, _Event, State) ->
    {keep_state, State}.

%%====================================================================
%% State: push (push mode)
%%====================================================================

push(info, send_changes, #{store_ref := StoreRef,
                           db_name := DbName,
                           since := Since,
                           batch_size := BatchSize,
                           interval := Interval,
                           owner := Owner,
                           pending := Pending} = State) ->
    FoldFun = fun(Change, Acc) ->
        NewAcc = [Change | Acc],
        case length(NewAcc) >= BatchSize of
            true -> {stop, NewAcc};
            false -> {ok, NewAcc}
        end
    end,
    {ok, RevChanges, NewSeq} = barrel_changes:fold_changes(StoreRef, DbName, Since, FoldFun, []),
    Changes = lists:reverse(RevChanges),

    %% Only track non-empty batches to prevent stall during idle periods
    case Changes of
        [] ->
            %% No changes - don't add to pending, just schedule next poll
            erlang:send_after(Interval, self(), send_changes),
            {keep_state, State#{since => NewSeq}};
        _ ->
            %% Have changes - send and track
            ReqId = make_ref(),
            _ = case Owner of
                undefined -> ok;
                Pid when is_pid(Pid) -> Pid ! {changes, ReqId, Changes}
            end,

            NewPending = [ReqId | Pending],
            NewState = State#{since => NewSeq, pending => NewPending},

            case length(NewPending) < ?MAX_PENDING of
                true ->
                    erlang:send_after(Interval, self(), send_changes),
                    {keep_state, NewState};
                false ->
                    {next_state, wait_pending, NewState}
            end
    end;

push(cast, stop, State) ->
    {stop, normal, State};

push(_EventType, _Event, State) ->
    {keep_state, State}.



%%====================================================================
%% State: wait_pending (waiting for acks)
%%====================================================================

wait_pending(cast, {ack, ReqId}, #{pending := Pending, interval := Interval} = State) ->
    case Pending -- [ReqId] of
        [] ->
            {next_state, push, State#{pending => []},
             [{next_event, info, send_changes}]};
        NewPending when length(NewPending) < ?MAX_PENDING ->
            erlang:send_after(Interval, self(), send_changes),
            {next_state, push, State#{pending => NewPending}};
        NewPending ->
            {keep_state, State#{pending => NewPending}}
    end;

wait_pending(cast, stop, State) ->
    {stop, normal, State};

wait_pending(_EventType, _Event, State) ->
    {keep_state, State}.
