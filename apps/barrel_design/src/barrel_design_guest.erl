%% @doc Run a guest map over a batch of documents.
%%
%% A batch runs in one job process spawned by the caller, with a heap
%% cap that kills it. Each document gets a fresh instance (no state
%% survives from one document to the next), finite fuel, a call depth,
%% a host-call and a page limit, and is destroyed after its call. The
%% caller enforces a wall-clock deadline per document by killing the
%% job; the remaining documents continue in a new one. Rows a guest
%% emits are staged in the job and reported only when its call returns.
%%
%% Guest imports (`barrel_v1'):
%% <ul>
%% <li>`field(slot, dest, capacity) -> len': copies the slot's value, as
%%     wire CBOR, to guest memory and returns its length, or -1 when the
%%     document lacks the field or it has another type. A value larger
%%     than the capacity traps.</li>
%% <li>`emit(key, key_len, value, value_len)': stages one row. Key and
%%     value are CBOR (integers, UTF-8 texts, booleans, null, arrays) and
%%     must match the view's declared types.</li>
%% </ul>
-module(barrel_design_guest).

-export([run/4]).

-define(SLOTS, '$barrel_design_slots').
-define(ROWS, '$barrel_design_rows').
-define(BUDGET, '$barrel_design_budget').
-define(TYPES, '$barrel_design_types').
-define(I64_MIN, -(1 bsl 63)).
-define(I64_MAX, (1 bsl 63) - 1).

-type result() :: {ok, [{barrel_design_key:key(), barrel_design_key:key()}]}
                | {error, term()}.
-export_type([result/0]).

%% @doc Map each document; results come back in document order. Opts:
%% `retries' for a document that timed out or whose job died (default
%% 2) before its error is reported.
-spec run(term(), map(), [map()], map()) -> [result()].
run(_Module, _Spec, [], _Opts) ->
    [];
run(Module, Spec, Docs, Opts) ->
    Retries = maps:get(retries, Opts, 2),
    Indexed = lists:zip(lists:seq(1, length(Docs)), Docs),
    batch(Module, Spec, Indexed, Retries, Retries, []).

batch(_Module, _Spec, [], _Left, _Retries, Acc) ->
    lists:reverse(Acc);
batch(Module, Spec, Pending, Left, Retries, Acc) ->
    #{limits := #{timeout_ms := Timeout, max_heap_words := Heap}} = Spec,
    Ref = make_ref(),
    Parent = self(),
    {Job, Mon} = spawn_opt(fun() -> job(Parent, Ref, Module, Spec, Pending) end,
                           [monitor, {message_queue_data, off_heap},
                            {max_heap_size, #{size => Heap, kill => true,
                                              error_logger => false}}]),
    case collect(Ref, Job, Mon, Pending, Timeout, Acc) of
        {done, Acc1} ->
            lists:reverse(Acc1);
        {failed, I, _Why, Rest, Acc1} when Left > 0 ->
            %% a timeout or a dead job may be transient: the same
            %% document again, after a short pause
            timer:sleep(10 * (Retries - Left + 1)),
            batch(Module, Spec, [lists:keyfind(I, 1, Pending) | Rest], Left - 1,
                  Retries, Acc1);
        {failed, _I, Why, Rest, Acc1} ->
            batch(Module, Spec, Rest, Retries, Retries, [{error, Why} | Acc1])
    end.

%% Wait for each pending document's result within its deadline.
collect(Ref, Job, Mon, [], _Timeout, Acc) ->
    receive
        {Ref, done} -> ok;
        {'DOWN', Mon, process, Job, _} -> ok
    after 1000 -> exit(Job, kill)
    end,
    erlang:demonitor(Mon, [flush]),
    {done, Acc};
collect(Ref, Job, Mon, [{I, _} | Rest], Timeout, Acc) ->
    receive
        {Ref, I, Result} ->
            collect(Ref, Job, Mon, Rest, Timeout, [Result | Acc]);
        {'DOWN', Mon, process, Job, Reason} ->
            {failed, I, {worker_died, died(Reason)}, Rest, Acc}
    after Timeout ->
        exit(Job, kill),
        receive {'DOWN', Mon, process, Job, _} -> ok end,
        flush(Ref),
        {failed, I, timeout, Rest, Acc}
    end.

died(killed) -> killed;
died(Reason) -> Reason.

flush(Ref) ->
    receive {Ref, _, _} -> flush(Ref); {Ref, done} -> flush(Ref)
    after 0 -> ok
    end.

%%====================================================================
%% Job process
%%====================================================================

job(Parent, Ref, Module, Spec, Pending) ->
    PMon = erlang:monitor(process, Parent),
    lists:foreach(fun({I, Doc}) ->
        receive {'DOWN', PMon, process, Parent, _} -> exit(normal)
        after 0 -> ok
        end,
        Parent ! {Ref, I, map_doc(Module, Spec, Doc)}
    end, Pending),
    Parent ! {Ref, done}.

map_doc(Module, #{export := Export, fields := Fields, key := Key, value := Value,
                  limits := L}, Doc) ->
    #{fuel := Fuel, max_depth := Depth, max_host_calls := Calls,
      max_memory_pages := Pages, max_rows := Rows, max_key_bytes := KeyBytes,
      max_value_bytes := ValueBytes, max_field_bytes := FieldBytes} = L,
    put(?SLOTS, list_to_tuple([slot(P, T, Doc, FieldBytes) || {P, T} <- Fields])),
    put(?ROWS, []),
    put(?BUDGET, {Rows, KeyBytes, ValueBytes}),
    put(?TYPES, {Key, Value}),
    Imports = #{{<<"barrel_v1">>, <<"field">>} => fun field/2,
                {<<"barrel_v1">>, <<"emit">>} => fun emit/2},
    Limits = #{fuel => Fuel, max_depth => Depth, max_host_calls => Calls,
               max_memory_pages => Pages},
    barrel_design_stats:bump(guest_calls),
    Result = instantiate_and_call(Module, Imports, Limits, Export),
    case Result of
        {ok, _} -> ok;
        {error, _} -> barrel_design_stats:bump(guest_errors)
    end,
    Result.

instantiate_and_call(Module, Imports, Limits, Export) ->
    try wasm:instantiate(Module, Imports, Limits) of
        {ok, Inst} ->
            try wasm:call(Inst, Export, []) of
                {ok, []} -> {ok, lists:reverse(get(?ROWS))};
                {ok, _} -> {error, unexpected_results};
                {error, E} -> {error, classify(E)}
            after
                wasm:destroy(Inst)
            end;
        {error, E} ->
            {error, classify(E)}
    after
        erase(?SLOTS), erase(?ROWS), erase(?BUDGET), erase(?TYPES)
    end.

classify(#{kind := host_error, ctx := #{reason := Reason}}) -> Reason;
classify(#{kind := Kind}) -> Kind;
classify(Other) -> Other.

%%====================================================================
%% Imports
%%====================================================================

field(Ctx, [Slot, Ptr, Cap]) ->
    Slots = get(?SLOTS),
    case Slot >= 0 andalso Slot < tuple_size(Slots) of
        false ->
            {trap, {bad_slot, Slot}};
        true ->
            case element(Slot + 1, Slots) of
                missing -> {ok, [-1]};
                too_large -> {trap, {field_too_large, Slot}};
                Bin when byte_size(Bin) > (Cap band 16#FFFFFFFF) ->
                    {trap, {field_capacity, Slot}};
                Bin ->
                    case wasm:write_memory(Ctx, Ptr band 16#FFFFFFFF, Bin) of
                        ok -> {ok, [byte_size(Bin)]};
                        {error, _} -> {trap, out_of_bounds}
                    end
            end
    end.

emit(Ctx, [KP, KL0, VP, VL0]) ->
    {RowsLeft, MaxKey, MaxValue} = get(?BUDGET),
    KL = KL0 band 16#FFFFFFFF,
    VL = VL0 band 16#FFFFFFFF,
    if
        RowsLeft =< 0 -> {trap, too_many_rows};
        KL > MaxKey -> {trap, key_too_large};
        VL > MaxValue -> {trap, value_too_large};
        true ->
            case {wasm:read_memory(Ctx, KP band 16#FFFFFFFF, KL),
                  wasm:read_memory(Ctx, VP band 16#FFFFFFFF, VL)} of
                {{ok, KBin}, {ok, VBin}} -> stage(KBin, VBin, RowsLeft, MaxKey, MaxValue);
                _ -> {trap, out_of_bounds}
            end
    end.

stage(KBin, VBin, RowsLeft, MaxKey, MaxValue) ->
    {KeySpec, ValueSpec} = get(?TYPES),
    case {barrel_design_cbor:decode(KBin), barrel_design_cbor:decode(VBin)} of
        {{ok, Key}, {ok, Value}} ->
            case key_ok(KeySpec, Key) andalso type_ok(maps:get(type, ValueSpec), Value) of
                true ->
                    put(?ROWS, [{Key, Value} | get(?ROWS)]),
                    put(?BUDGET, {RowsLeft - 1, MaxKey, MaxValue}),
                    {ok, []};
                false ->
                    {trap, invalid_row}
            end;
        _ ->
            {trap, invalid_cbor}
    end.

key_ok({array, Comps}, Key) when is_list(Key), length(Key) =:= length(Comps) ->
    lists:all(fun({#{type := T}, V}) -> type_ok(T, V) end, lists:zip(Comps, Key));
key_ok({array, _}, _Key) -> false;
key_ok({scalar, #{type := T}}, Key) -> type_ok(T, Key).

type_ok(text, V) -> is_binary(V);
type_ok(i64, V) -> is_integer(V);
type_ok(bool, V) -> is_boolean(V);
type_ok(any, V) -> barrel_design_key:valid(V).

%%====================================================================
%% Field slots (host side, before the call)
%%====================================================================

slot(Path, Type, Doc, Max) ->
    case lookup(Path, Doc) of
        not_found -> missing;
        {ok, V} ->
            case typed(Type, V, Max) of
                false -> missing;
                too_large -> too_large;
                true ->
                    Bin = barrel_docdb_codec_cbor:encode_wire(V),
                    case byte_size(Bin) > Max of
                        true -> too_large;
                        false -> Bin
                    end
            end
    end.

typed(text, V, _) -> is_binary(V) andalso unicode:characters_to_binary(V) =:= V;
typed(i64, V, _) -> is_integer(V) andalso V >= ?I64_MIN andalso V =< ?I64_MAX;
typed(bool, V, _) -> is_boolean(V);
typed({array, E}, V, Max) when is_list(V) ->
    case length(V) > Max of
        true -> too_large;
        false -> lists:all(fun(X) -> typed(E, X, Max) end, V)
    end;
typed({array, _}, _V, _) -> false;
typed(any, _V, _) -> true.

lookup([], V) -> {ok, V};
lookup([K | Rest], M) when is_map(M), is_binary(K) ->
    case maps:find(K, M) of
        {ok, V} -> lookup(Rest, V);
        error -> not_found
    end;
lookup([I | Rest], L) when is_list(L), is_integer(I), I >= 0, I < length(L) ->
    lookup(Rest, lists:nth(I + 1, L));
lookup(_, _) -> not_found.
