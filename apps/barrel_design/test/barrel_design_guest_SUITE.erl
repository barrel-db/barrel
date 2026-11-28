%% Guest admission, the runner's limits and the node module holder.
-module(barrel_design_guest_SUITE).

-export([all/0, groups/0, init_per_suite/1, end_per_suite/1]).
-export([accepts_abi_module/1, digest_mismatch/1, rejected_modules/1,
         rows_from_fields/1, fresh_state/1, out_of_fuel/1, deadline_kills/1,
         memory_grow_refused/1, field_limits/1, row_limits/1, invalid_rows/1,
         partial_emits_discarded/1, host_call_limit/1, one_load_per_digest/1,
         reload_after_cache_restart/1, preview_wat/1, wat_module_identity/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [{group, admission}, {group, runner}, {group, holder}, preview_wat].

groups() ->
    [{admission, [], [accepts_abi_module, digest_mismatch, rejected_modules,
                      wat_module_identity]},
     {runner, [], [rows_from_fields, fresh_state, out_of_fuel, deadline_kills,
                   memory_grow_refused, field_limits, row_limits, invalid_rows,
                   partial_emits_discarded, host_call_limit]},
     {holder, [], [one_load_per_digest, reload_after_cache_restart]}].

init_per_suite(Config) ->
    ok = barrel_design_test_util:start(Config),
    Config.

end_per_suite(_Config) ->
    barrel_design_test_util:stop().

%%====================================================================
%% Admission
%%====================================================================

accepts_abi_module(_Config) ->
    Bytes = guest("by_customer.wasm"),
    {ok, #{imports := [field, emit], pages := 1}} =
        barrel_design_module:check_bytes(Bytes, crypto:hash(sha256, Bytes), opts()),
    Zig = guest("open_by_customer.wasm"),
    {ok, #{imports := Imports, pages := 1}} =
        barrel_design_module:check_bytes(Zig, crypto:hash(sha256, Zig), opts()),
    ?assertEqual([emit, field], lists:sort(Imports)).

%% A module given as WAT is stored as its canonical encoding: layout,
%% comments and identifiers do not change its digest, and it decodes to
%% the same module as the wasm-tools binary of the same text.
wat_module_identity(_Config) ->
    Wat = guest("by_customer.wat"),
    {ok, D, Bytes} = barrel_design:module({wat, Wat}),
    ?assertEqual(D, crypto:hash(sha256, Bytes)),
    {ok, _} = barrel_design_module:check_bytes(Bytes, D, opts()),
    Renamed = binary:replace(binary:replace(Wat, <<"$field">>, <<"$get_field">>, [global]),
                             <<"$key">>, <<"$k">>, [global]),
    Reflowed = <<";; another layout\n", (binary:replace(Renamed, <<"\n">>, <<" \n  ">>, [global]))/binary>>,
    {ok, D, Bytes} = barrel_design:module({wat, Reflowed}),
    {ok, D, Bytes} = barrel_design:module({wat, binary_to_list(Wat)}),
    Tool = guest("by_customer.wasm"),
    %% wasm-tools also writes a name section, so its digest differs ...
    {ok, ToolD, Tool} = barrel_design:module({wasm, Tool}),
    ?assertNotEqual(D, ToolD),
    %% ... while the code is the same
    {ok, Mine} = wasm:compile(Bytes),
    {ok, Theirs} = wasm:compile(Tool),
    Strip = fun(M) -> setelement(tuple_size(M), setelement(2, M, x), []) end,
    ?assertEqual(Strip(Mine), Strip(Theirs)),
    {error, {invalid_module, wat, _}} = barrel_design:module({wat, <<"(module (func">>}).

digest_mismatch(_Config) ->
    {error, {digest_mismatch, <<0:256>>}} =
        barrel_design_module:check_bytes(guest("by_customer.wasm"), <<0:256>>, opts()).

rejected_modules(_Config) ->
    Cases = [
        {{bad_signature, field},
         "(module (import \"barrel_v1\" \"field\" (func (param i32) (result i32)))"
         " (memory 1 1) (func (export \"map\")))"},
        {{import_not_allowed, <<"env">>, <<"clock">>},
         "(module (import \"env\" \"clock\" (func (result i64)))"
         " (memory 1 1) (func (export \"map\")))"},
        {{memory_pages, 2, 1},
         "(module (memory 2 2) (func (export \"map\")))"},
        {unbounded_memory, "(module (memory 1) (func (export \"map\")))"},
        {no_memory, "(module (func (export \"map\")))"},
        {start_function,
         "(module (memory 1 1) (func $s) (start $s) (func (export \"map\")))"},
        {shared_memory, "(module (memory 1 1 shared) (func (export \"map\")))"},
        {{missing_export, <<"map">>}, "(module (memory 1 1) (func (export \"other\")))"},
        {{bad_signature, <<"map">>},
         "(module (memory 1 1) (func (export \"map\") (param i32)))"},
        {unbounded_table,
         "(module (memory 1 1) (table 1 funcref) (func (export \"map\")))"},
        {{feature_not_allowed, v128},
         "(module (memory 1 1) (func (export \"map\") (local v128)))"}
    ],
    lists:foreach(fun({Why, Wat}) ->
        {ok, M} = wasm:compile({wat, list_to_binary(Wat)}),
        ?assertEqual({error, {rejected_module, t, Why}},
                     barrel_design_module:check(M, t, opts()))
    end, Cases).

%%====================================================================
%% Runner
%%====================================================================

rows_from_fields(_Config) ->
    Docs = [#{<<"customer">> => <<"Ada">>, <<"cents">> => 1250},
            #{<<"customer">> => <<"Bea">>, <<"cents">> => -900},
            #{<<"customer">> => <<"Cy">>},
            #{<<"cents">> => 5},
            #{<<"customer">> => <<"Di">>, <<"cents">> => <<"10">>}],
    ?assertEqual([{ok, [{<<"Ada">>, 1250}]}, {ok, [{<<"Bea">>, -900}]}, {ok, []},
                  {ok, []}, {ok, []}],
                 run(by_customer(), spec(by_customer), Docs)).

%% A mutable global starts from its initial value on every document.
fresh_state(_Config) ->
    {ok, M} = wasm:compile(guest("counter.wasm")),
    Spec = (spec(scalar))#{fields => []},
    ?assertEqual([{ok, [{1, 1}]} || _ <- lists:seq(1, 5)], run(M, Spec, [#{} || _ <- lists:seq(1, 5)])).

out_of_fuel(_Config) ->
    {ok, M} = wasm:compile(guest("loop.wasm")),
    Spec = with_limits((spec(scalar))#{fields => []}, #{fuel => 1000}),
    ?assertEqual([{error, out_of_fuel}], run(M, Spec, [#{}])).

%% High fuel, short deadline: the job is killed, its pages are freed and
%% the next document still runs.
deadline_kills(_Config) ->
    {ok, M} = wasm:compile(guest("loop.wasm")),
    Before = pages_in_use(),
    Spec = with_limits((spec(scalar))#{fields => []}, #{fuel => 10000000, timeout_ms => 20}),
    T0 = erlang:monotonic_time(millisecond),
    ?assertEqual([{error, timeout}, {error, timeout}],
                 barrel_design_guest:run(M, Spec, [#{}, #{}], #{retries => 0})),
    ?assert(erlang:monotonic_time(millisecond) - T0 < 1000),
    %% the engine frees a killed job's pages when it sees the exit
    ok = wait_until(fun() -> pages_in_use() =:= Before end, 2000),
    %% no job is left behind
    ?assertEqual([], [P || P <- processes(), is_job(P)]).

memory_grow_refused(_Config) ->
    {ok, M} = wasm:compile({wat, list_to_binary(
        "(module (import \"barrel_v1\" \"emit\" (func $e (param i32 i32 i32 i32)))"
        " (memory 1 1)"
        " (func (export \"map\")"
        "   (i32.store8 (i32.const 0) (i32.add (memory.grow (i32.const 1)) (i32.const 1)))"
        "   (call $e (i32.const 0) (i32.const 1) (i32.const 0) (i32.const 1))))")}),
    ?assertEqual([{ok, [{0, 0}]}], run(M, (spec(scalar))#{fields => []}, [#{}])).

field_limits(_Config) ->
    Long = binary:copy(<<"x">>, 2000),
    Doc = #{<<"customer">> => Long, <<"cents">> => 1},
    %% larger than the capacity the guest offers (1024)
    ?assertEqual([{error, {field_capacity, 0}}], run(by_customer(), spec(by_customer), [Doc])),
    %% larger than the view allows any field to be
    ?assertEqual([{error, {field_too_large, 0}}],
                 run(by_customer(), with_limits(spec(by_customer), #{max_field_bytes => 100}), [Doc])).

row_limits(_Config) ->
    {ok, M} = wasm:compile(guest("fanout.wasm")),
    Spec = spec(fanout),
    Doc = #{<<"lines">> => lists:seq(1, 10)},
    [{ok, Rows}] = run(M, Spec, [Doc]),
    ?assertEqual([{[I - 1], I} || I <- lists:seq(1, 10)], Rows),
    ?assertEqual([{error, too_many_rows}], run(M, with_limits(Spec, #{max_rows => 4}), [Doc])),
    ?assertEqual([{error, value_too_large}],
                 run(M, with_limits(Spec, #{max_value_bytes => 1}), [#{<<"lines">> => [1000]}])),
    ?assertEqual([{error, key_too_large}],
                 run(M, with_limits(Spec, #{max_key_bytes => 1}), [Doc])).

%% Rows must match the declared key and value types.
invalid_rows(_Config) ->
    Spec = (spec(by_customer))#{key => {scalar, #{type => i64, category => false}}},
    ?assertEqual([{error, invalid_row}],
                 run(by_customer(), Spec, [#{<<"customer">> => <<"Ada">>, <<"cents">> => 1}])).

%% A guest that emits, then traps, contributes nothing.
partial_emits_discarded(_Config) ->
    {ok, M} = wasm:compile({wat, list_to_binary(
        "(module (import \"barrel_v1\" \"emit\" (func $e (param i32 i32 i32 i32)))"
        " (memory 1 1)"
        " (func (export \"map\")"
        "   (call $e (i32.const 0) (i32.const 1) (i32.const 0) (i32.const 1))"
        "   unreachable))")}),
    ?assertEqual([{error, unreachable}, {error, unreachable}],
                 run(M, (spec(scalar))#{fields => []}, [#{}, #{}])).

host_call_limit(_Config) ->
    ?assertEqual([{error, host_call_limit}],
                 run(by_customer(), with_limits(spec(by_customer), #{max_host_calls => 1}),
                     [#{<<"customer">> => <<"Ada">>, <<"cents">> => 1}])).

%%====================================================================
%% Holder
%%====================================================================

one_load_per_digest(_Config) ->
    Bytes = guest("by_customer.wasm"),
    D = crypto:hash(sha256, Bytes),
    #{loads := L0} = barrel_design_modules:info(),
    {ok, H} = barrel_design_modules:acquire(D, Bytes),
    {ok, H} = barrel_design_modules:acquire(D, Bytes),
    Self = self(),
    Other = spawn(fun() -> {ok, H} = barrel_design_modules:acquire(D, Bytes), Self ! held,
                           receive stop -> ok end end),
    receive held -> ok end,
    #{loads := L1, resident := #{D := 3}} = barrel_design_modules:info(),
    ?assertEqual(L0 + 1, L1),
    %% many calls, no further loads
    Docs = [#{<<"customer">> => <<"Ada">>, <<"cents">> => I} || I <- lists:seq(1, 50)],
    50 = length([ok || {ok, [_]} <- run(H, spec(by_customer), Docs)]),
    ok = barrel_design_modules:release(D),
    ok = barrel_design_modules:release(D),
    %% the last user exiting unloads it
    Ref = monitor(process, Other),
    Other ! stop,
    receive {'DOWN', Ref, _, _, _} -> ok end,
    #{loads := L1, resident := Resident} = barrel_design_modules:info(),
    ?assertNot(maps:is_key(D, Resident)),
    {error, #{kind := module_not_loaded}} = wasm:instantiate(H, #{}, #{}).

reload_after_cache_restart(_Config) ->
    Bytes = guest("by_customer.wasm"),
    D = crypto:hash(sha256, Bytes),
    {ok, H} = barrel_design_modules:acquire(D, Bytes),
    Cache = whereis(wasm_module_cache),
    Ref = monitor(process, Cache),
    exit(Cache, kill),
    receive {'DOWN', Ref, _, _, _} -> ok end,
    wait_registered(wasm_module_cache),
    %% the new cache registers before its init has dropped the modules;
    %% a call returns only after init
    _ = wasm_module_cache:stats(),
    Doc = #{<<"customer">> => <<"Ada">>, <<"cents">> => 7},
    [{error, module_not_loaded}] = run(H, spec(by_customer), [Doc]),
    {ok, H} = barrel_design_modules:reload(D),
    [{ok, [{<<"Ada">>, 7}]}] = run(H, spec(by_customer), [Doc]),
    ok = barrel_design_modules:release(D).

%%====================================================================
%% Preview
%%====================================================================

preview_wat(_Config) ->
    View = #{select => <<"SELECT * FROM db WHERE type = 'sale'">>,
             map => #{fields => [{[<<"customer">>], text}, {[<<"cents">>], i64}]},
             key => #{type => text}, value => #{type => i64}, reduce => '_sum'},
    Docs = [#{<<"id">> => <<"a">>, <<"type">> => <<"sale">>, <<"customer">> => <<"Ada">>,
              <<"cents">> => 1250},
            #{<<"id">> => <<"b">>, <<"type">> => <<"refund">>, <<"customer">> => <<"Ada">>,
              <<"cents">> => 10},
            #{<<"id">> => <<"c">>, <<"type">> => <<"sale">>, <<"customer">> => <<"Bea">>}],
    {ok, [#{id := <<"a">>, rows := [#{key := <<"Ada">>, value := 1250}]},
          #{id := <<"b">>, selected := false},
          #{id := <<"c">>, rows := []}]} =
        barrel_design:preview({wat, file_text("by_customer.wat")}, View, Docs),
    {error, {rejected_module, preview, {memory_pages, 2, 1}}} =
        barrel_design:preview({wat, "(module (memory 2 2) (func (export \"map\")))"},
                              View#{map => #{fields => []}}, Docs).

%%====================================================================
%% Helpers
%%====================================================================

opts() -> #{export => <<"map">>, max_memory_pages => 1}.

by_customer() ->
    {ok, M} = wasm:compile(guest("by_customer.wasm")),
    M.

spec(by_customer) ->
    #{export => <<"map">>, fields => [{[<<"customer">>], text}, {[<<"cents">>], i64}],
      key => {scalar, #{type => text, category => false}}, value => #{type => i64},
      limits => barrel_design_manifest:default_limits()};
spec(scalar) ->
    #{export => <<"map">>, fields => [], key => {scalar, #{type => i64, category => false}},
      value => #{type => i64}, limits => barrel_design_manifest:default_limits()};
spec(fanout) ->
    #{export => <<"map">>, fields => [{[<<"lines">>], {array, i64}}],
      key => {array, [#{type => i64, category => false}]}, value => #{type => i64},
      limits => barrel_design_manifest:default_limits()}.

with_limits(#{limits := L} = Spec, More) -> Spec#{limits := maps:merge(L, More)}.

run(Module, Spec, Docs) ->
    barrel_design_guest:run(Module, Spec, Docs, #{retries => 0}).

guest(Name) ->
    {ok, Bin} = file:read_file(filename:join(guests_dir(), Name)),
    Bin.

file_text(Name) ->
    binary_to_list(guest(Name)).

guests_dir() ->
    Here = filename:dirname(code:which(?MODULE)),
    Candidates = [filename:join(Here, "guests"),
                  filename:join([Here, "..", "..", "..", "..", "apps", "barrel_design", "test", "guests"]),
                  filename:join([code:lib_dir(barrel_design), "test", "guests"])],
    case [D || D <- Candidates, filelib:is_file(filename:join(D, "by_customer.wasm"))] of
        [D | _] -> D;
        [] -> ct:fail({no_guests_dir, Candidates})
    end.

pages_in_use() ->
    maps:get(pages_in_use, wasm_engine:stats()).

is_job(Pid) ->
    case erlang:process_info(Pid, current_function) of
        {current_function, {barrel_design_guest, _, _}} -> true;
        _ -> false
    end.

wait_until(Fun, Ms) when Ms =< 0 ->
    case Fun() of true -> ok; false -> {error, timeout} end;
wait_until(Fun, Ms) ->
    case Fun() of
        true -> ok;
        false -> receive after 5 -> wait_until(Fun, Ms - 5) end
    end.

wait_registered(Name) ->
    case whereis(Name) of
        undefined -> receive after 10 -> wait_registered(Name) end;
        _ -> ok
    end.
