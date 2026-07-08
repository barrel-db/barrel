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
        <<"vv_contains">> => vv_contains(),
        <<"bql_local_run">> => bql_local_run(),
        <<"bql_errors">> => bql_errors(),
        <<"vector_embedding">> => vector_embedding()
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
%% BQL local run (end-to-end parity: query + docs -> ordered rows)
%%====================================================================
%%
%% Runs the real compile pipeline store-free: compile -> the barrel_query
%% spec is matched per doc, frames_for/residual_match/finalize apply the
%% post stages. The TS runLocal must produce the same rows in the same
%% order. Queries with id conditions are excluded on purpose (those
%% lower to an id scan the store-free path does not model).

bql_docs() ->
    [#{<<"id">> => <<"d1">>, <<"kind">> => <<"fruit">>, <<"name">> => <<"apple">>,
       <<"price">> => 3, <<"tags">> => [<<"red">>, <<"sweet">>],
       <<"meta">> => #{<<"color">> => <<"red">>}},
     #{<<"id">> => <<"d2">>, <<"kind">> => <<"fruit">>, <<"name">> => <<"pear">>,
       <<"price">> => 5, <<"tags">> => [<<"green">>]},
     #{<<"id">> => <<"d3">>, <<"kind">> => <<"tool">>, <<"name">> => <<"hammer">>,
       <<"price">> => 12, <<"meta">> => #{<<"color">> => <<"gray">>}},
     #{<<"id">> => <<"d4">>, <<"kind">> => <<"fruit">>, <<"name">> => <<"banana">>,
       <<"price">> => 2, <<"note">> => null},
     #{<<"id">> => <<"d5">>, <<"kind">> => <<"veg">>, <<"price">> => 7}].

bql_run_cases() ->
    [{<<"SELECT * FROM db">>, #{}},
     {<<"SELECT name, price FROM db WHERE kind = 'fruit'">>, #{}},
     {<<"SELECT name FROM db WHERE price > 3">>, #{}},
     {<<"SELECT name FROM db WHERE price >= 3 AND kind = 'fruit'">>, #{}},
     {<<"SELECT name FROM db WHERE name IN ('apple', 'pear')">>, #{}},
     {<<"SELECT name FROM db WHERE name != 'apple'">>, #{}},
     {<<"SELECT name FROM db WHERE name LIKE 'a%'">>, #{}},
     {<<"SELECT name FROM db WHERE name LIKE '%r'">>, #{}},
     {<<"SELECT id FROM db WHERE note IS NULL">>, #{}},
     {<<"SELECT id FROM db WHERE name IS MISSING">>, #{}},
     {<<"SELECT name FROM db WHERE price BETWEEN 3 AND 6">>, #{}},
     {<<"SELECT name FROM db WHERE meta.color = 'red'">>, #{}},
     {<<"SELECT name FROM db WHERE CONTAINS(tags, 'sweet')">>, #{}},
     {<<"SELECT name FROM db WHERE NOT (kind = 'tool')">>, #{}},
     {<<"SELECT name, price FROM db WHERE kind = 'fruit' ORDER BY price">>, #{}},
     {<<"SELECT name FROM db WHERE kind = 'fruit' ORDER BY price DESC">>, #{}},
     {<<"SELECT name FROM db ORDER BY price LIMIT 2 OFFSET 1">>, #{}},
     {<<"SELECT name AS n FROM db WHERE kind = 'fruit' ORDER BY name">>, #{}},
     {<<"SELECT id, name FROM db ORDER BY name">>, #{}},
     {<<"SELECT tag FROM db, UNNEST(tags) AS tag WHERE kind = 'fruit'">>, #{}},
     {<<"SELECT name FROM db WHERE price = $p">>, #{<<"p">> => 5}}].

bql_local_run() ->
    Docs = bql_docs(),
    [bql_run_case(Q, Params, Docs) || {Q, Params} <- bql_run_cases()].

bql_run_case(Query, Params, Docs) ->
    {ok, Plan} = barrel_bql:compile(Query, #{params => Params}),
    Spec = maps:get(spec, Plan),
    {ok, QPlan} = barrel_query:compile(Spec),
    Unnest = maps:get(unnest, Plan),
    Post = maps:get(post, Plan),
    Residual = maps:get(residual, Post),
    Sorted = lists:sort(
        fun(A, B) -> maps:get(<<"id">>, A) =< maps:get(<<"id">>, B) end, Docs),
    Frames = [F || D <- Sorted,
                   barrel_query:match(QPlan, D),
                   F <- barrel_bql_exec:frames_for(D, Unnest),
                   barrel_bql_exec:residual_match(Residual, F)],
    Rows = barrel_bql_exec:finalize(Frames, Post, Unnest),
    #{<<"query">> => Query,
      <<"params">> => Params,
      <<"docs">> => Docs,
      <<"rows">> => Rows}.

%%====================================================================
%% BQL compile errors (query -> error tag | null)
%%====================================================================

bql_errors() ->
    Queries = [<<"SELECT * FROM db WHERE x = null">>,
               <<"SELECT * FROM db WHERE _foo = 1">>,
               <<"SELECT * FROM db WHERE 1 = 1">>,
               <<"SELECT * FROM db ORDER BY a, b">>,
               <<"SELECT * FROM vector_top_k('q', k => 5)">>,
               <<"SELECT * FROM db WHERE name = 'ok'">>,
               <<"SELCT bad syntax">>],
    [#{<<"query">> => Q, <<"error">> => bql_error_tag(Q)} || Q <- Queries].

bql_error_tag(Query) ->
    case barrel_bql:compile(Query, #{params => #{}}) of
        {ok, _} -> null;
        {error, {parse_error, _, _}} -> <<"parse_error">>;
        {error, {invalid_query, _, Reason}} when is_tuple(Reason) ->
            atom_to_binary(element(1, Reason), utf8);
        {error, {invalid_query, _, Reason}} when is_atom(Reason) ->
            atom_to_binary(Reason, utf8)
    end.

%%====================================================================
%% Vector embeddings (float32 LE -> base64, the wire form)
%%====================================================================

vector_embedding() ->
    Vectors = [[],
               [0.0],
               [1.0, -1.0, 0.5],
               [0.1, 0.2, 0.3, 0.4],
               [1.0e-8, 3.4028235e38, 0.3333333432674408],
               to_floats(lists:seq(1, 16))],
    [vector_embedding_case(V) || V <- Vectors].

vector_embedding_case(Vec) ->
    #{<<"vector">> => Vec,
      <<"dim">> => length(Vec),
      <<"base64">> => base64:encode(barrel_doc:encode_embedding(Vec))}.

to_floats(Ints) ->
    [float(I) || I <- Ints].

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
