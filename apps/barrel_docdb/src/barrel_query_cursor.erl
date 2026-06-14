%%%-------------------------------------------------------------------
%%% @doc Query cursor management for chunked query execution
%%%
%%% Manages cursor state for paginated queries using ETS.
%%% Each cursor holds:
%%% - Snapshot reference for consistent reads across chunks
%%% - Last key position for resumption
%%% - Query metadata for validation
%%% - Expiry time for cleanup
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_query_cursor).
-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([create/5, lookup/1, update/2, release/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(TABLE, barrel_query_cursors).
-define(CLEANUP_INTERVAL_MS, 10000).  %% Cleanup every 10 seconds
-define(DEFAULT_TTL_MS, 60000).       %% Cursors expire after 60 seconds

-record(cursor, {
    id :: binary(),
    store_ref :: term(),
    db_name :: binary(),
    snapshot :: term() | undefined,
    last_key :: binary(),
    query_type :: atom(),
    created_at :: integer(),
    expires_at :: integer()
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the cursor manager
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Create a new cursor with snapshot
%% Returns an opaque cursor token
-spec create(term(), binary(), atom(), binary(), term()) -> binary().
create(StoreRef, DbName, QueryType, LastKey, Snapshot) ->
    CursorId = generate_cursor_id(),
    Now = erlang:system_time(millisecond),
    Cursor = #cursor{
        id = CursorId,
        store_ref = StoreRef,
        db_name = DbName,
        snapshot = Snapshot,
        last_key = LastKey,
        query_type = QueryType,
        created_at = Now,
        expires_at = Now + ?DEFAULT_TTL_MS
    },
    true = ets:insert(?TABLE, {CursorId, Cursor}),
    CursorId.

%% @doc Look up a cursor by token
%% Returns cursor data if valid and not expired
-spec lookup(binary()) -> {ok, #cursor{}} | {error, not_found | expired}.
lookup(CursorId) ->
    case ets:lookup(?TABLE, CursorId) of
        [{CursorId, Cursor}] ->
            Now = erlang:system_time(millisecond),
            case Cursor#cursor.expires_at > Now of
                true ->
                    %% Extend TTL on access
                    NewCursor = Cursor#cursor{expires_at = Now + ?DEFAULT_TTL_MS},
                    true = ets:insert(?TABLE, {CursorId, NewCursor}),
                    {ok, NewCursor};
                false ->
                    %% Expired - clean up
                    release(CursorId),
                    {error, expired}
            end;
        [] ->
            {error, not_found}
    end.

%% @doc Update cursor position after a chunk
-spec update(binary(), binary()) -> ok | {error, not_found}.
update(CursorId, NewLastKey) ->
    case ets:lookup(?TABLE, CursorId) of
        [{CursorId, Cursor}] ->
            Now = erlang:system_time(millisecond),
            NewCursor = Cursor#cursor{
                last_key = NewLastKey,
                expires_at = Now + ?DEFAULT_TTL_MS
            },
            true = ets:insert(?TABLE, {CursorId, NewCursor}),
            ok;
        [] ->
            {error, not_found}
    end.

%% @doc Release a cursor and its snapshot
-spec release(binary()) -> ok.
release(CursorId) ->
    case ets:lookup(?TABLE, CursorId) of
        [{CursorId, Cursor}] ->
            %% Release snapshot if present
            case Cursor#cursor.snapshot of
                undefined -> ok;
                Snapshot ->
                    barrel_store_rocksdb:safe_release_snapshot(Snapshot)
            end,
            true = ets:delete(?TABLE, CursorId),
            ok;
        [] ->
            ok
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Create ETS table for cursor storage
    ?TABLE = ets:new(?TABLE, [
        named_table,
        public,           %% Allow direct access from query processes
        set,
        {read_concurrency, true},
        {write_concurrency, true}
    ]),
    %% Schedule periodic cleanup
    erlang:send_after(?CLEANUP_INTERVAL_MS, self(), cleanup),
    {ok, #{}}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(cleanup, State) ->
    cleanup_expired_cursors(),
    erlang:send_after(?CLEANUP_INTERVAL_MS, self(), cleanup),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    %% Release all snapshots on shutdown
    ets:foldl(fun({_Id, Cursor}, Acc) ->
        case Cursor#cursor.snapshot of
            undefined -> ok;
            Snapshot ->
                barrel_store_rocksdb:safe_release_snapshot(Snapshot)
        end,
        Acc
    end, ok, ?TABLE),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

generate_cursor_id() ->
    %% Generate a unique cursor ID
    Rand = crypto:strong_rand_bytes(16),
    base64:encode(Rand).

cleanup_expired_cursors() ->
    Now = erlang:system_time(millisecond),
    %% Find and delete expired cursors
    Expired = ets:foldl(fun({Id, Cursor}, Acc) ->
        case Cursor#cursor.expires_at =< Now of
            true -> [Id | Acc];
            false -> Acc
        end
    end, [], ?TABLE),
    %% Release each expired cursor
    lists:foreach(fun(Id) -> release(Id) end, Expired).
