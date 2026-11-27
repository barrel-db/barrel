%% @doc Admission checks for a guest module (ABI `barrel_v1').
%%
%% Linking alone would accept an import of the right name with any
%% signature, since a host fun takes the signature the guest declares.
%% These checks run before any instantiation, on the decoded module:
%%
%% <ul>
%% <li>imports: only `barrel_v1.field(i32, i32, i32) -> i32' and
%%     `barrel_v1.emit(i32, i32, i32, i32)', each at most once;</li>
%% <li>the map export is a function taking and returning nothing;</li>
%% <li>exactly one memory, defined in the module, 32-bit, unshared, with
%%     a declared maximum within the view's page limit;</li>
%% <li>no start function, no imported memory, table or global, tables
%%     bounded, no GC types or exception tags;</li>
%% <li>no SIMD, atomics, exception or GC instructions.</li>
%% </ul>
-module(barrel_design_module).

-export([check_bytes/3, check/3]).

-include_lib("wasm/include/wasm.hrl").

-define(MAX_MODULE_BYTES, 4194304).
-define(MAX_TABLE, 4096).
-define(FIELD_TYPE, #functype{params = [i32, i32, i32], results = [i32]}).
-define(EMIT_TYPE, #functype{params = [i32, i32, i32, i32], results = []}).
-define(MAP_TYPE, #functype{params = [], results = []}).

%% Instruction or type atoms carrying any of these mark a feature a view
%% guest may not use.
-define(DENIED, [<<"v128">>, <<"i8x16">>, <<"i16x8">>, <<"i32x4">>, <<"i64x2">>,
                 <<"f32x4">>, <<"f64x2">>, <<"atomic">>, <<"relaxed">>,
                 <<"throw">>, <<"try_table">>, <<"struct">>, <<"array_">>,
                 <<"i31">>, <<"any_convert">>, <<"extern_convert">>,
                 <<"ref_cast">>, <<"ref_test">>, <<"br_on_cast">>]).

%% @doc Check portable bytes against their declared digest, then their
%% contents (see check/3).
-spec check_bytes(binary(), binary(), #{export := binary(),
                                         max_memory_pages := pos_integer()}) ->
    {ok, map()} | {error, term()}.
check_bytes(Bytes, Digest, _Opts) when byte_size(Bytes) > ?MAX_MODULE_BYTES ->
    {error, {module_too_large, Digest, byte_size(Bytes)}};
check_bytes(Bytes, Digest, Opts) when is_binary(Bytes) ->
    case crypto:hash(sha256, Bytes) of
        Digest ->
            case wasm:compile(Bytes) of
                {ok, Module} -> check(Module, Digest, Opts);
                {error, Error} -> {error, {invalid_module, Digest, kind(Error)}}
            end;
        _ ->
            {error, {digest_mismatch, Digest}}
    end.

%% @doc Check a decoded module (from bytes or inline WAT).
-spec check(tuple(), term(), #{export := binary(),
                               max_memory_pages := pos_integer()}) ->
    {ok, map()} | {error, term()}.
check(#module{} = M, Id, #{export := Export, max_memory_pages := MaxPages}) ->
    try
        Imports = imports(M),
        export(M, Export),
        Pages = memory(M, MaxPages),
        M#module.start =:= undefined orelse throw(start_function),
        M#module.tags =:= [] orelse throw(exception_tags),
        lists:foreach(fun(#tabletype{limits = #limits{max = Max, index_type = i32}})
                            when is_integer(Max), Max =< ?MAX_TABLE -> ok;
                         (_) -> throw(unbounded_table)
                      end, M#module.tables),
        lists:foreach(fun(#subtype{body = #functype{}}) -> ok;
                         (_) -> throw(gc_types)
                      end, M#module.types),
        features([M#module.funcs, M#module.types, M#module.globals]),
        {ok, #{imports => Imports, pages => Pages, export => Export}}
    catch
        throw:Reason -> {error, {rejected_module, Id, Reason}}
    end.

imports(#module{imports = Imports, types = Types}) ->
    Names = lists:map(
        fun(#import{module = <<"barrel_v1">>, name = <<"field">>, desc = {func, T}}) ->
                type_of(Types, T) =:= ?FIELD_TYPE orelse throw({bad_signature, field}),
                field;
           (#import{module = <<"barrel_v1">>, name = <<"emit">>, desc = {func, T}}) ->
                type_of(Types, T) =:= ?EMIT_TYPE orelse throw({bad_signature, emit}),
                emit;
           (#import{module = Mod, name = Name}) ->
                throw({import_not_allowed, Mod, Name})
        end, Imports),
    length(lists:usort(Names)) =:= length(Names) orelse throw(duplicate_import),
    Names.

export(#module{exports = Exports, types = Types, funcs = Funcs, imports = Imports}, Name) ->
    case [Idx || #export{name = N, desc = {func, Idx}} <- Exports, N =:= Name] of
        [Idx] ->
            NImported = length([I || #import{desc = {func, _}} = I <- Imports]),
            Idx >= NImported orelse throw({export_is_import, Name}),
            #func{type = T} = lists:nth(Idx - NImported + 1, Funcs),
            type_of(Types, T) =:= ?MAP_TYPE orelse throw({bad_signature, Name});
        [] ->
            throw({missing_export, Name})
    end.

memory(#module{mems = [#memtype{limits = L}]}, MaxPages) ->
    case L of
        #limits{shared = true} -> throw(shared_memory);
        #limits{index_type = i64} -> throw(memory64);
        #limits{max = undefined} -> throw(unbounded_memory);
        #limits{min = Min, max = Max} when Max > MaxPages; Min > Max ->
            throw({memory_pages, Max, MaxPages});
        #limits{max = Max} -> Max
    end;
memory(#module{mems = []}, _MaxPages) -> throw(no_memory);
memory(#module{}, _MaxPages) -> throw(multiple_memories).

type_of(Types, Idx) when Idx >= 0, Idx < length(Types) ->
    #subtype{body = Body} = lists:nth(Idx + 1, Types),
    Body;
type_of(_Types, _Idx) ->
    throw(bad_type_index).

features(Terms) ->
    walk(Terms, fun(A) ->
        Name = atom_to_binary(A),
        lists:any(fun(D) -> binary:match(Name, D) =/= nomatch end, ?DENIED)
            andalso throw({feature_not_allowed, A})
    end).

walk(A, F) when is_atom(A) -> F(A);
walk(L, F) when is_list(L) -> lists:foreach(fun(E) -> walk(E, F) end, L);
walk(T, F) when is_tuple(T) -> walk(tuple_to_list(T), F);
walk(_, _F) -> ok.

kind(#{kind := K}) -> K;
kind(Other) -> Other.
