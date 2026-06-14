%%%-------------------------------------------------------------------
%%% @doc Hybrid Logical Clock utilities for barrel_docdb
%%%
%%% HLC timestamps are used for ordering changes across distributed nodes.
%%% They combine physical wall clock time with a logical counter to ensure
%%% causality while maintaining a close relationship to real time.
%%%
%%% Format: {WallTime, Logical} where WallTime is in milliseconds and
%%% Logical is a counter that increments when events happen in the
%%% physical clock's future.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_hlc).

-compile({no_auto_import, [now/0]}).

-include_lib("hlc/include/hlc.hrl").

%% Global HLC operations (use the node-global clock)
-export([
    now/0,
    update/1,
    timestamp/0,
    get_hlc/0,
    sync_hlc/1,
    new_hlc/0,
    maybe_sync_from_header/1
]).

%% Instance-based operations (for testing or custom clocks)
-export([
    start_link/0,
    start_link/1,
    stop/1,
    now/1,
    update/2,
    timestamp/1
]).

%% Encoding/Decoding
-export([
    encode/1,
    decode/1,
    to_binary/1,
    from_binary/1,
    to_string/1,
    from_string/1
]).

%% Comparison
-export([
    compare/2,
    less/2,
    equal/2
]).

%% Constants
-export([
    min/0,
    max/0
]).

%% Accessor functions
-export([
    wall_time/1
]).

%% Types
-export_type([
    clock/0,
    timestamp/0
]).

%%====================================================================
%% Types
%%====================================================================

-type clock() :: hlc:clock().
%% HLC clock process reference.

-type timestamp() :: #timestamp{}.
%% HLC timestamp with wall_time and logical components.

%% Global clock name (started by barrel_docdb_sup)
-define(GLOBAL_CLOCK, barrel_hlc_clock).

%%====================================================================
%% Global HLC Operations
%%====================================================================
%% These functions operate on the node-global HLC clock that is started
%% by the barrel_docdb supervisor. Use these for distributed ordering.

%% @doc Get the current global HLC timestamp without advancing the clock.
-spec get_hlc() -> timestamp().
get_hlc() ->
    timestamp().

%% @doc Synchronize with a remote HLC timestamp.
%% Call this when receiving data from another node to maintain causality.
%% Returns {ok, NewTimestamp} or {error, clock_skew}.
-spec sync_hlc(timestamp()) -> {ok, timestamp()} | {error, clock_skew}.
sync_hlc(RemoteTS) ->
    update(RemoteTS).

%% @doc Generate a new global HLC timestamp, advancing the clock.
%% Use this when generating a new event that needs to be ordered.
-spec new_hlc() -> timestamp().
new_hlc() ->
    now().

%% @doc Sync HLC from a base64-encoded HTTP header value.
%% Silently ignores undefined or invalid values.
-spec maybe_sync_from_header(binary() | undefined) -> ok.
maybe_sync_from_header(undefined) ->
    ok;
maybe_sync_from_header(HlcBase64) ->
    barrel_lib:safe(fun() ->
        HlcBin = base64:decode(HlcBase64),
        Hlc = decode(HlcBin),
        _ = sync_hlc(Hlc),
        ok
    end, ok).

%% @doc Generate a new timestamp for a local event using global clock.
-spec now() -> timestamp().
now() ->
    hlc:now(?GLOBAL_CLOCK).

%% @doc Update the global clock with a remote timestamp.
%% Rejects non-timestamp input at the API boundary so a misuse cannot
%% crash the global `barrel_hlc_clock' gen_server.
-spec update(timestamp()) -> {ok, timestamp()} | {error, clock_skew}.
update(#timestamp{} = RemoteTS) ->
    case hlc:update(?GLOBAL_CLOCK, RemoteTS) of
        {ok, NewTS} ->
            {ok, NewTS};
        {timeahead, _} ->
            {error, clock_skew}
    end.

%% @doc Get the current global timestamp without advancing the clock.
-spec timestamp() -> timestamp().
timestamp() ->
    hlc:timestamp(?GLOBAL_CLOCK).

%%====================================================================
%% Instance-based Clock Management
%%====================================================================
%% These functions are for testing or when you need a custom clock
%% instance separate from the global node clock.

%% @doc Start a new HLC clock with default settings.
%% Uses physical system clock and no offset limit.
-spec start_link() -> {ok, clock()}.
start_link() ->
    hlc:start_link().

%% @doc Start a new HLC clock with maximum offset limit.
%% MaxOffset is the maximum allowed clock skew in milliseconds.
%% A value of 0 disables offset checking.
-spec start_link(MaxOffset :: non_neg_integer()) -> {ok, clock()}.
start_link(MaxOffset) ->
    hlc:start_link(fun hlc:physical_clock/0, MaxOffset).

%% @doc Stop the HLC clock.
-spec stop(clock()) -> ok | {error, term()}.
stop(Clock) ->
    hlc:stop(Clock).

%% @doc Generate a new timestamp using a specific clock instance.
-spec now(clock()) -> timestamp().
now(Clock) ->
    hlc:now(Clock).

%% @doc Update a specific clock with a remote timestamp.
-spec update(clock(), timestamp()) -> {ok, timestamp()} | {error, clock_skew}.
update(Clock, RemoteTS) ->
    case hlc:update(Clock, RemoteTS) of
        {ok, NewTS} ->
            {ok, NewTS};
        {timeahead, _} ->
            {error, clock_skew}
    end.

%% @doc Get the current timestamp from a specific clock without advancing it.
-spec timestamp(clock()) -> timestamp().
timestamp(Clock) ->
    hlc:timestamp(Clock).

%%====================================================================
%% Encoding/Decoding
%%====================================================================

%% @doc Encode timestamp to binary for storage.
%% Uses big-endian encoding to maintain lexicographic sort order.
%% Format: 12-byte binary with WallTime (64-bit) and Logical (32-bit).
-spec encode(timestamp()) -> binary().
encode(#timestamp{wall_time = WallTime, logical = Logical}) ->
    <<WallTime:64/big-unsigned, Logical:32/big-unsigned>>.

%% @doc Decode binary to timestamp.
-spec decode(binary()) -> timestamp().
decode(<<WallTime:64/big-unsigned, Logical:32/big-unsigned>>) ->
    #timestamp{wall_time = WallTime, logical = Logical};
decode(_) ->
    erlang:error(badarg).

%% @doc Alias for encode/1 for consistency with other modules.
-spec to_binary(timestamp()) -> binary().
to_binary(TS) ->
    encode(TS).

%% @doc Alias for decode/1 for consistency with other modules.
-spec from_binary(binary()) -> timestamp().
from_binary(Bin) ->
    decode(Bin).

%% @doc Convert timestamp to human-readable string.
%% Format: "WallTime:Logical" (e.g., "1609459200000:42")
-spec to_string(timestamp()) -> binary().
to_string(#timestamp{wall_time = WallTime, logical = Logical}) ->
    WallBin = integer_to_binary(WallTime),
    LogicalBin = integer_to_binary(Logical),
    <<WallBin/binary, ":", LogicalBin/binary>>.

%% @doc Parse timestamp from string.
-spec from_string(binary()) -> timestamp() | {error, invalid_timestamp}.
from_string(Bin) when is_binary(Bin) ->
    case binary:split(Bin, <<":">>) of
        [WallBin, LogicalBin] ->
            barrel_lib:safe(fun() ->
                WallTime = binary_to_integer(WallBin),
                Logical = binary_to_integer(LogicalBin),
                #timestamp{wall_time = WallTime, logical = Logical}
            end, {error, invalid_timestamp});
        _ ->
            {error, invalid_timestamp}
    end;
from_string(_) ->
    {error, invalid_timestamp}.

%%====================================================================
%% Comparison
%%====================================================================

%% @doc Compare two timestamps.
%% Returns `lt' if TS1 is before TS2, `eq' if equal, `gt' if TS1 is after TS2.
-spec compare(timestamp(), timestamp()) -> lt | eq | gt.
compare(#timestamp{wall_time = W1, logical = L1},
        #timestamp{wall_time = W2, logical = L2}) ->
    if
        W1 < W2 -> lt;
        W1 > W2 -> gt;
        L1 < L2 -> lt;
        L1 > L2 -> gt;
        true -> eq
    end.

%% @doc Check if TS1 is less than TS2.
-spec less(timestamp(), timestamp()) -> boolean().
less(TS1, TS2) ->
    hlc:less(TS1, TS2).

%% @doc Check if two timestamps are equal.
-spec equal(timestamp(), timestamp()) -> boolean().
equal(TS1, TS2) ->
    hlc:equal(TS1, TS2).

%%====================================================================
%% Constants
%%====================================================================

%% @doc Minimum timestamp value (for range scans).
-spec min() -> timestamp().
min() ->
    #timestamp{wall_time = 0, logical = 0}.

%% @doc Maximum timestamp value (for range scans).
-spec max() -> timestamp().
max() ->
    %% Use maximum values that fit in our encoding format
    #timestamp{wall_time = 16#FFFFFFFFFFFFFFFF, logical = 16#FFFFFFFF}.

%%====================================================================
%% Accessor Functions
%%====================================================================

%% @doc Extract wall_time (milliseconds since epoch) from a timestamp.
-spec wall_time(timestamp()) -> non_neg_integer().
wall_time(#timestamp{wall_time = WallTime}) ->
    WallTime.
