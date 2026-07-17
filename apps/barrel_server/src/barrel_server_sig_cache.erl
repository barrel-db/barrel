%%%-------------------------------------------------------------------
%%% @doc Replay cache for signed sync requests.
%%%
%%% A bounded, public ETS set keyed by `{KeyId, Ts, Sig}'. The verifier
%%% ({@link barrel_server_auth}) calls {@link check_and_insert/2} on the
%%% hot path: a first sighting inserts and returns `ok'; a repeat within
%%% the window returns `{error, replayed}'. This closes the replay gap the
%%% retired barrel_memory peer-auth scheme left open (it had only a
%%% timestamp window).
%%%
%%% This gen_server owns the table and sweeps expired entries; the atomic
%%% `ets:insert_new/2' means the check needs no server round-trip. If the
%%% table is absent (server not started), the check fails closed.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_sig_cache).
-behaviour(gen_server).

-export([start_link/0, check_and_insert/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(TABLE, ?MODULE).
-define(DEFAULT_SWEEP_MS, 60000).

%%====================================================================
%% API
%%====================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Record a request signature. `TtlMs' bounds how long the entry is
%% retained (use twice the accepted skew). Returns `ok' the first time a
%% `Key' is seen, `{error, replayed}' on a repeat, and `{error, no_cache}'
%% if the cache is not running (fail closed).
-spec check_and_insert(term(), pos_integer()) ->
    ok | {error, replayed | no_cache}.
check_and_insert(Key, TtlMs) ->
    Expiry = erlang:system_time(millisecond) + TtlMs,
    try ets:insert_new(?TABLE, {Key, Expiry}) of
        true -> ok;
        false -> {error, replayed}
    catch
        error:badarg -> {error, no_cache}
    end.

%%====================================================================
%% gen_server
%%====================================================================

init([]) ->
    _ = ets:new(?TABLE, [named_table, public, set,
                         {read_concurrency, true},
                         {write_concurrency, true}]),
    SweepMs = application:get_env(barrel_server, sig_replay_sweep_ms,
                                  ?DEFAULT_SWEEP_MS),
    schedule_sweep(SweepMs),
    {ok, #{sweep_ms => SweepMs}}.

handle_call(_Req, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(sweep, #{sweep_ms := SweepMs} = State) ->
    Now = erlang:system_time(millisecond),
    %% delete every entry whose expiry is at or before now
    _ = ets:select_delete(?TABLE, [{{'_', '$1'}, [{'=<', '$1', Now}], [true]}]),
    schedule_sweep(SweepMs),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%====================================================================
%% Internal
%%====================================================================

schedule_sweep(SweepMs) ->
    erlang:send_after(SweepMs, self(), sweep).
