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
%%% Covers the HLC families (encode, compare, clock traces), the
%%% version token/compare families, and the version-vector families
%%% (encode, relate, contains).
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
        <<"hlc_traces">> => hlc_traces(),
        <<"version_tokens">> => version_tokens(),
        <<"version_compare">> => version_compare(),
        <<"vv_encode">> => vv_encode(),
        <<"vv_relate">> => vv_relate(),
        <<"vv_contains">> => vv_contains()
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
%% Version tokens
%%====================================================================

%% Authors chosen to exercise the byte-wise tie-break: equal-length
%% hex ids and prefix relationships ("a" < "aa" < "b").
version_tokens() ->
    Cases = [{0, 0, <<"0000000000000000">>},
             {1000, 5, <<"aabbccddeeff0011">>},
             {1000, 5, <<"ffffffffffffffff">>},
             {(1 bsl 53) + 1, 42, <<"00ff00ff00ff00ff">>},
             {2000, 0, <<"a">>},
             {2000, 0, <<"aa">>},
             {2000, 0, <<"b">>}],
    [version_token_case(W, L, A) || {W, L, A} <- Cases].

version_token_case(Wall, Logical, Author) ->
    V = barrel_version:new(mk_ts(Wall, Logical), Author),
    #{<<"wall">> => integer_to_binary(Wall),
      <<"logical">> => Logical,
      <<"author">> => Author,
      <<"token">> => barrel_version:to_token(V)}.

version_compare() ->
    Points = [{1000, 0, <<"aaaa">>},
              {1000, 0, <<"bbbb">>},
              {1000, 1, <<"aaaa">>},
              {2000, 0, <<"aaaa">>},
              {1000, 0, <<"a">>},
              {1000, 0, <<"aa">>}],
    [version_compare_case(A, B) || A <- Points, B <- Points].

version_compare_case({W1, L1, A1} = A, {W2, L2, A2} = B) ->
    VA = barrel_version:new(mk_ts(W1, L1), A1),
    VB = barrel_version:new(mk_ts(W2, L2), A2),
    Result = barrel_version:compare(VA, VB),
    Winner = barrel_version:max(VA, VB),
    #{<<"a">> => version_json(A),
      <<"b">> => version_json(B),
      <<"result">> => atom_to_binary(Result, utf8),
      <<"winner">> => version_json_v(Winner)}.

%%====================================================================
%% Version vectors
%%====================================================================

vv_encode() ->
    VVs = [[],
           [{<<"aabbccddeeff0011">>, 1000, 5}],
           [{<<"b">>, 3000, 0}, {<<"a">>, 1000, 1}, {<<"aa">>, 2000, 2}],
           [{<<"00">>, 0, 0}, {<<"ff">>, (1 bsl 53) + 1, 42}]],
    [vv_encode_case(Entries) || Entries <- VVs].

vv_encode_case(Entries) ->
    VV = mk_vv(Entries),
    Hex = binary:encode_hex(barrel_vv:encode(VV), lowercase),
    #{<<"entries">> => [vv_entry_json(E) || E <- Entries],
      <<"hex">> => Hex}.

vv_relate() ->
    VVs = [[],
           [{<<"a">>, 1000, 0}],
           [{<<"a">>, 1000, 1}],
           [{<<"a">>, 1000, 0}, {<<"b">>, 2000, 0}],
           [{<<"a">>, 1000, 1}, {<<"b">>, 2000, 0}],
           [{<<"b">>, 2000, 0}]],
    [vv_relate_case(A, B) || A <- VVs, B <- VVs].

vv_relate_case(A, B) ->
    Result = barrel_vv:compare(mk_vv(A), mk_vv(B)),
    #{<<"a">> => [vv_entry_json(E) || E <- A],
      <<"b">> => [vv_entry_json(E) || E <- B],
      <<"result">> => atom_to_binary(Result, utf8)}.

vv_contains() ->
    VV = [{<<"a">>, 1000, 5}, {<<"b">>, 2000, 0}],
    Versions = [{<<"a">>, 1000, 4},
                {<<"a">>, 1000, 5},
                {<<"a">>, 1000, 6},
                {<<"a">>, 999, 0},
                {<<"c">>, 500, 0},
                {<<"b">>, 2000, 0}],
    [vv_contains_case(VV, V) || V <- Versions].

vv_contains_case(VVEntries, {Node, Wall, Logical}) ->
    Version = barrel_version:new(mk_ts(Wall, Logical), Node),
    Result = barrel_vv:contains(mk_vv(VVEntries), Version),
    #{<<"vv">> => [vv_entry_json(E) || E <- VVEntries],
      <<"node">> => Node,
      <<"wall">> => integer_to_binary(Wall),
      <<"logical">> => Logical,
      <<"result">> => Result}.

%%====================================================================
%% Helpers
%%====================================================================

mk_vv(Entries) ->
    lists:foldl(
        fun({Node, Wall, Logical}, VV) ->
            barrel_vv:bump(VV, barrel_version:new(mk_ts(Wall, Logical), Node))
        end,
        barrel_vv:new(),
        Entries).

version_json({Wall, Logical, Author}) ->
    #{<<"wall">> => integer_to_binary(Wall),
      <<"logical">> => Logical,
      <<"author">> => Author}.

version_json_v({Hlc, Author}) ->
    <<Wall:64/big-unsigned, Logical:32/big-unsigned>> = barrel_hlc:encode(Hlc),
    #{<<"wall">> => integer_to_binary(Wall),
      <<"logical">> => Logical,
      <<"author">> => Author}.

vv_entry_json({Node, Wall, Logical}) ->
    #{<<"node">> => Node,
      <<"wall">> => integer_to_binary(Wall),
      <<"logical">> => Logical}.

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
