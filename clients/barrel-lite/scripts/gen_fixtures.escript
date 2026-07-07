#!/usr/bin/env escript
%%%-------------------------------------------------------------------
%%% Regenerate the golden codec fixtures the TypeScript client asserts
%%% against. Run from the umbrella root AFTER `rebar3 compile' so the
%%% Erlang codecs are on the path:
%%%
%%%   rebar3 compile
%%%   ./clients/barrel-lite/scripts/gen_fixtures.escript \
%%%       > clients/barrel-lite/test/fixtures/golden.json
%%%
%%% The escript writes only to stdout; redirect it yourself. Every
%%% wall_time is emitted as a decimal STRING so values above 2^53
%%% survive a JSON round-trip in JavaScript.
%%%
%%% Step 4 covers the HLC families (encode, compare, clock traces).
%%% Later steps append version and version-vector families.
%%%-------------------------------------------------------------------
-mode(compile).

main(_Args) ->
    %% The hlc library logs backward-jump and timeahead diagnostics;
    %% in an escript those reach stdout and would corrupt the JSON.
    _ = logger:set_primary_config(level, none),
    ok = add_paths(),
    Fixtures = #{
        <<"max_logical">> => 16#7FFFFFFF,
        <<"hlc_encode">> => hlc_encode(),
        <<"hlc_compare">> => hlc_compare(),
        <<"hlc_traces">> => hlc_traces()
    },
    io:put_chars(json:encode(Fixtures)),
    io:nl().

%% Resolve _build ebin dirs relative to the umbrella root (cwd).
add_paths() ->
    Ebins = filelib:wildcard("_build/default/lib/*/ebin"),
    ok = code:add_pathsz([filename:absname(E) || E <- Ebins]),
    ok.

%%====================================================================
%% HLC encode / decode
%%====================================================================

hlc_encode() ->
    Walls = [0, 1, 1000, 1 bsl 31, (1 bsl 53) - 1, 1 bsl 53,
             (1 bsl 53) + 1, (1 bsl 63) - 1],
    Logicals = [0, 1, 42, 16#7FFFFFFF],
    [encode_case(W, L)
     || W <- Walls, L <- Logicals].

encode_case(Wall, Logical) ->
    TS = mk_ts(Wall, Logical),
    Hex = binary:encode_hex(barrel_hlc:encode(TS), lowercase),
    #{<<"wall">> => integer_to_binary(Wall),
      <<"logical">> => Logical,
      <<"hex">> => Hex}.

%%====================================================================
%% HLC compare
%%====================================================================

hlc_compare() ->
    Points = [{0, 0}, {0, 1}, {1000, 0}, {1000, 5}, {1000, 6},
              {2000, 0}, {(1 bsl 53) + 1, 7}, {(1 bsl 63) - 1, 0}],
    [compare_case(A, B) || A <- Points, B <- Points].

compare_case({W1, L1} = A, {W2, L2} = B) ->
    Result = barrel_hlc:compare(mk_ts(W1, L1), mk_ts(W2, L2)),
    #{<<"a">> => ts_json(A),
      <<"b">> => ts_json(B),
      <<"result">> => atom_to_binary(Result, utf8)}.

%%====================================================================
%% HLC clock traces (now / update, stateful)
%%====================================================================
%%
%% Each trace is a fresh clock plus a sequence of steps. A step sets
%% the manual physical clock, runs an op, and records the resulting
%% timestamp (or "timeahead" when a maxoffset rejects the update). The
%% TS test replays the same steps against its own clock with an
%% injected physical function.

hlc_traces() ->
    [
     %% maxoffset 0: exercises now (advance + tie), then the four
     %% update branches (local-ahead, remote-ahead, equal, phys-ahead).
     run_trace(0,
        [{now, 1000},
         {now, 1000},
         {update, 1000, {500, 5}},
         {update, 1000, {2000, 3}},
         {update, 1000, {2000, 10}},
         {update, 5000, {100, 0}},
         {now, 4000},
         {now, 9000}]),
     %% maxoffset 1000: a remote too far ahead is rejected (state
     %% unchanged), a remote within the bound is accepted.
     run_trace(1000,
        [{update, 100, {10000, 0}},
         {update, 100, {900, 0}},
         {now, 2000}])
    ].

run_trace(MaxOffset, Steps) ->
    {MPid, Fun} = hlc:manual_clock(0),
    {ok, Clock} = hlc:start_link(Fun, MaxOffset),
    Recorded = [record_step(MPid, Clock, S) || S <- Steps],
    hlc:stop(Clock),
    hlc:stop_manual_clock(MPid),
    #{<<"maxoffset">> => MaxOffset,
      <<"steps">> => Recorded}.

record_step(MPid, Clock, {now, Phys}) ->
    ok = hlc:set_manual_clock(MPid, Phys),
    TS = hlc:now(Clock),
    #{<<"op">> => <<"now">>,
      <<"phys">> => integer_to_binary(Phys),
      <<"result">> => ts_json_ts(TS)};
record_step(MPid, Clock, {update, Phys, {RW, RL}}) ->
    ok = hlc:set_manual_clock(MPid, Phys),
    Remote = mk_ts(RW, RL),
    Result =
        case hlc:update(Clock, Remote) of
            {ok, TS} -> ts_json_ts(TS);
            {timeahead, _} -> <<"timeahead">>
        end,
    #{<<"op">> => <<"update">>,
      <<"phys">> => integer_to_binary(Phys),
      <<"remote">> => ts_json({RW, RL}),
      <<"result">> => Result}.

%%====================================================================
%% Helpers
%%====================================================================

mk_ts(Wall, Logical) ->
    barrel_hlc:from_binary(<<Wall:64/big-unsigned, Logical:32/big-unsigned>>).

ts_json({Wall, Logical}) ->
    #{<<"wall">> => integer_to_binary(Wall),
      <<"logical">> => Logical}.

ts_json_ts(TS) ->
    Wall = barrel_hlc:wall_time(TS),
    <<Wall:64/big-unsigned, Logical:32/big-unsigned>> = barrel_hlc:encode(TS),
    #{<<"wall">> => integer_to_binary(Wall),
      <<"logical">> => Logical}.
