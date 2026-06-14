%%%-------------------------------------------------------------------
%%% @doc Test suite for barrel_hlc module
%%%
%%% Tests Hybrid Logical Clock functionality including clock operations,
%%% encoding/decoding, and comparison functions.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_hlc_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("hlc/include/hlc.hrl").

%% CT callbacks
-export([all/0, groups/0, init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).

%% Test cases - clock operations
-export([
    clock_start_stop/1,
    clock_now/1,
    clock_update/1,
    clock_timestamp/1,
    clock_causality/1,
    maybe_sync_from_header/1
]).

%% Test cases - encoding/decoding
-export([
    encode_decode/1,
    encode_sort_order/1,
    to_from_string/1
]).

%% Test cases - comparison
-export([
    compare_timestamps/1,
    less_function/1,
    equal_function/1
]).

%% Test cases - constants
-export([
    min_max_constants/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, clock}, {group, encoding}, {group, comparison}, {group, constants}].

groups() ->
    [
        {clock, [sequence], [
            clock_start_stop,
            clock_now,
            clock_update,
            clock_timestamp,
            clock_causality,
            maybe_sync_from_header
        ]},
        {encoding, [sequence], [
            encode_decode,
            encode_sort_order,
            to_from_string
        ]},
        {comparison, [sequence], [
            compare_timestamps,
            less_function,
            equal_function
        ]},
        {constants, [sequence], [
            min_max_constants
        ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Test Cases - Clock Operations
%%====================================================================

clock_start_stop(_Config) ->
    %% Start clock with default settings
    {ok, Clock1} = barrel_hlc:start_link(),
    ?assert(is_pid(Clock1)),
    ?assertEqual(ok, barrel_hlc:stop(Clock1)),

    %% Start clock with max offset
    {ok, Clock2} = barrel_hlc:start_link(5000),
    ?assert(is_pid(Clock2)),
    ?assertEqual(ok, barrel_hlc:stop(Clock2)),
    ok.

clock_now(_Config) ->
    {ok, Clock} = barrel_hlc:start_link(),

    %% Generate timestamp
    TS1 = barrel_hlc:now(Clock),
    ?assert(is_record(TS1, timestamp)),
    ?assert(TS1#timestamp.wall_time > 0),

    %% Generate another timestamp - should be >= first
    TS2 = barrel_hlc:now(Clock),
    ?assertNot(barrel_hlc:less(TS2, TS1)),

    barrel_hlc:stop(Clock),
    ok.

clock_update(_Config) ->
    {ok, Clock} = barrel_hlc:start_link(),

    %% Create a remote timestamp
    RemoteTS = #timestamp{wall_time = erlang:system_time(millisecond), logical = 5},

    %% Update with remote timestamp
    {ok, NewTS} = barrel_hlc:update(Clock, RemoteTS),
    ?assert(is_record(NewTS, timestamp)),

    %% New timestamp should reflect causality with remote
    ?assertNot(barrel_hlc:less(NewTS, RemoteTS)),

    barrel_hlc:stop(Clock),
    ok.

clock_timestamp(_Config) ->
    {ok, Clock} = barrel_hlc:start_link(),

    %% Get timestamp without advancing
    TS1 = barrel_hlc:timestamp(Clock),
    TS2 = barrel_hlc:timestamp(Clock),

    %% Should be equal (clock not advanced)
    ?assertEqual(TS1, TS2),

    %% After now(), timestamp should be different
    _ = barrel_hlc:now(Clock),
    TS3 = barrel_hlc:timestamp(Clock),
    ?assertNotEqual(TS1, TS3),

    barrel_hlc:stop(Clock),
    ok.

clock_causality(_Config) ->
    %% Test causality preservation between two clocks
    {ok, Clock1} = barrel_hlc:start_link(),
    {ok, Clock2} = barrel_hlc:start_link(),

    %% Clock1 generates event
    TS1 = barrel_hlc:now(Clock1),

    %% Clock2 receives and updates
    {ok, TS2} = barrel_hlc:update(Clock2, TS1),

    %% Clock2's new timestamp should be after Clock1's
    ?assert(barrel_hlc:less(TS1, TS2) orelse barrel_hlc:equal(TS1, TS2) =:= false),

    %% Clock2 generates another event
    TS3 = barrel_hlc:now(Clock2),
    ?assert(barrel_hlc:less(TS2, TS3) orelse barrel_hlc:equal(TS2, TS3)),

    %% Clock1 receives from Clock2
    {ok, TS4} = barrel_hlc:update(Clock1, TS3),

    %% TS4 should be after TS3 (causality preserved)
    ?assertNot(barrel_hlc:less(TS4, TS3)),

    barrel_hlc:stop(Clock1),
    barrel_hlc:stop(Clock2),
    ok.

maybe_sync_from_header(_Config) ->
    %% Test that maybe_sync_from_header handles various inputs

    %% undefined should return ok silently
    ?assertEqual(ok, barrel_hlc:maybe_sync_from_header(undefined)),

    %% Valid base64-encoded HLC should sync
    TS = #timestamp{wall_time = erlang:system_time(millisecond) + 1000, logical = 10},
    HlcBin = barrel_hlc:encode(TS),
    HlcBase64 = base64:encode(HlcBin),
    ?assertEqual(ok, barrel_hlc:maybe_sync_from_header(HlcBase64)),

    %% Invalid base64 should return ok (silently ignored)
    ?assertEqual(ok, barrel_hlc:maybe_sync_from_header(<<"not-valid-base64!!!">>)),

    %% Valid base64 but invalid HLC binary should return ok (silently ignored)
    ?assertEqual(ok, barrel_hlc:maybe_sync_from_header(base64:encode(<<"short">>))),

    ok.

%%====================================================================
%% Test Cases - Encoding/Decoding
%%====================================================================

encode_decode(_Config) ->
    %% Test basic encode/decode roundtrip
    TS1 = #timestamp{wall_time = 1609459200000, logical = 42},
    Encoded = barrel_hlc:encode(TS1),
    ?assert(is_binary(Encoded)),
    ?assertEqual(12, byte_size(Encoded)), %% 8 + 4 bytes

    Decoded = barrel_hlc:decode(Encoded),
    ?assertEqual(TS1, Decoded),

    %% Test min/max values
    TSMin = barrel_hlc:min(),
    ?assertEqual(TSMin, barrel_hlc:decode(barrel_hlc:encode(TSMin))),

    %% Test to_binary/from_binary aliases
    TS2 = #timestamp{wall_time = 999999999999, logical = 100},
    ?assertEqual(TS2, barrel_hlc:from_binary(barrel_hlc:to_binary(TS2))),

    ok.

encode_sort_order(_Config) ->
    %% Test that encoded binary sort order matches timestamp order
    TS1 = #timestamp{wall_time = 1000, logical = 0},
    TS2 = #timestamp{wall_time = 1000, logical = 1},
    TS3 = #timestamp{wall_time = 1001, logical = 0},
    TS4 = #timestamp{wall_time = 2000, logical = 999},

    Enc1 = barrel_hlc:encode(TS1),
    Enc2 = barrel_hlc:encode(TS2),
    Enc3 = barrel_hlc:encode(TS3),
    Enc4 = barrel_hlc:encode(TS4),

    %% Binary comparison should match timestamp comparison
    ?assert(Enc1 < Enc2),
    ?assert(Enc2 < Enc3),
    ?assert(Enc3 < Enc4),

    %% Verify with compare function
    ?assertEqual(lt, barrel_hlc:compare(TS1, TS2)),
    ?assertEqual(lt, barrel_hlc:compare(TS2, TS3)),
    ?assertEqual(lt, barrel_hlc:compare(TS3, TS4)),

    ok.

to_from_string(_Config) ->
    %% Test string conversion
    TS = #timestamp{wall_time = 1609459200000, logical = 42},
    Str = barrel_hlc:to_string(TS),
    ?assertEqual(<<"1609459200000:42">>, Str),

    %% Roundtrip
    ?assertEqual(TS, barrel_hlc:from_string(Str)),

    %% Invalid strings
    ?assertEqual({error, invalid_timestamp}, barrel_hlc:from_string(<<"invalid">>)),
    ?assertEqual({error, invalid_timestamp}, barrel_hlc:from_string(<<"not:a:number">>)),
    ?assertEqual({error, invalid_timestamp}, barrel_hlc:from_string(123)),

    ok.

%%====================================================================
%% Test Cases - Comparison
%%====================================================================

compare_timestamps(_Config) ->
    TS1 = #timestamp{wall_time = 1000, logical = 5},
    TS2 = #timestamp{wall_time = 1000, logical = 10},
    TS3 = #timestamp{wall_time = 2000, logical = 0},

    %% Same wall_time, different logical
    ?assertEqual(lt, barrel_hlc:compare(TS1, TS2)),
    ?assertEqual(gt, barrel_hlc:compare(TS2, TS1)),

    %% Different wall_time
    ?assertEqual(lt, barrel_hlc:compare(TS1, TS3)),
    ?assertEqual(lt, barrel_hlc:compare(TS2, TS3)),
    ?assertEqual(gt, barrel_hlc:compare(TS3, TS1)),

    %% Equal
    ?assertEqual(eq, barrel_hlc:compare(TS1, TS1)),
    ?assertEqual(eq, barrel_hlc:compare(TS2, TS2)),

    ok.

less_function(_Config) ->
    TS1 = #timestamp{wall_time = 1000, logical = 0},
    TS2 = #timestamp{wall_time = 1000, logical = 1},
    TS3 = #timestamp{wall_time = 1001, logical = 0},

    ?assert(barrel_hlc:less(TS1, TS2)),
    ?assert(barrel_hlc:less(TS2, TS3)),
    ?assert(barrel_hlc:less(TS1, TS3)),

    ?assertNot(barrel_hlc:less(TS2, TS1)),
    ?assertNot(barrel_hlc:less(TS1, TS1)),

    ok.

equal_function(_Config) ->
    TS1 = #timestamp{wall_time = 1000, logical = 5},
    TS2 = #timestamp{wall_time = 1000, logical = 5},
    TS3 = #timestamp{wall_time = 1000, logical = 6},

    ?assert(barrel_hlc:equal(TS1, TS2)),
    ?assert(barrel_hlc:equal(TS1, TS1)),
    ?assertNot(barrel_hlc:equal(TS1, TS3)),

    ok.

%%====================================================================
%% Test Cases - Constants
%%====================================================================

min_max_constants(_Config) ->
    Min = barrel_hlc:min(),
    Max = barrel_hlc:max(),

    %% Min should be less than any real timestamp
    RealTS = #timestamp{wall_time = 1, logical = 0},
    ?assert(barrel_hlc:less(Min, RealTS)),

    %% Max should be greater than any real timestamp
    ?assert(barrel_hlc:less(RealTS, Max)),

    %% Min < Max
    ?assert(barrel_hlc:less(Min, Max)),

    %% Encoded min < encoded max
    ?assert(barrel_hlc:encode(Min) < barrel_hlc:encode(Max)),

    ok.
