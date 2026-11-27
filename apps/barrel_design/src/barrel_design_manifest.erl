%% @doc Design manifests: validation, normalization and view signatures.
%%
%% A design is plain data: a name, a version and named views. A view
%% selects documents with a BQL predicate, builds rows with either a
%% native projection (key and value paths) or a Wasm map, and may total
%% them with `_sum' or `_count'. Keys of the input may be atoms or
%% binaries, so a manifest read from JSON normalizes the same way.
%%
%% The signature of a view hashes everything that changes its rows:
%% selector, key and value specs, reducer, map module digest, ABI,
%% export, fields, limits and the key encoding version. Titles do not
%% count, so a caption edit keeps the index.
-module(barrel_design_manifest).

-export([normalize/1, signature/1, modules/1, default_limits/0]).
-export([selector/1, selected/2]).

-define(NAME_MAX, 64).
-define(MAX_VIEWS, 32).
-define(MAX_KEY_PARTS, 8).
-define(MAX_FIELDS, 16).
-define(MAX_PATH, 16).

-define(LIMITS, [
    %% key, default, min, max
    {fuel, 50000, 1, 10000000},
    {timeout_ms, 100, 1, 5000},
    {max_memory_pages, 1, 1, 16},
    {max_depth, 64, 1, 1024},
    {max_heap_words, 262144, 16384, 8388608},
    {max_host_calls, 256, 1, 100000},
    {max_rows, 16, 1, 1024},
    {max_key_bytes, 512, 1, 1024},
    {max_value_bytes, 256, 1, 4096},
    {max_field_bytes, 4096, 1, 65536}
]).

-type design() :: #{name := binary(), version := pos_integer(),
                    views := #{binary() => view()}, title => binary()}.
-type view() :: map().
-export_type([design/0, view/0]).

%%====================================================================
%% Normalize
%%====================================================================

-spec normalize(map()) -> {ok, design()} | {error, term()}.
normalize(Design) when is_map(Design) ->
    try
        D = keys(Design, [name, version, views, title]),
        Name = name(get(name, D), design_name),
        Version = case get(version, D) of
            V when is_integer(V), V >= 1 -> V;
            V -> throw({invalid_version, V})
        end,
        Views0 = case get(views, D) of
            Vs when is_map(Vs), map_size(Vs) >= 1, map_size(Vs) =< ?MAX_VIEWS -> Vs;
            Vs -> throw({invalid_views, Vs})
        end,
        Views = maps:from_list(
            [begin
                 VName = name(K, view_name),
                 {VName, view(VName, Val)}
             end || {K, Val} <- maps:to_list(Views0)]),
        map_size(Views) =:= map_size(Views0) orelse throw(duplicate_view_name),
        Base = #{name => Name, version => Version, views => Views},
        {ok, case maps:find(title, D) of
                 {ok, T} -> Base#{title => text(T, title)};
                 error -> Base
             end}
    catch
        throw:Reason -> {error, Reason}
    end;
normalize(Other) ->
    {error, {invalid_design, Other}}.

view(Name, View) when is_map(View) ->
    V = keys(View, [select, key, value, reduce, map, limits, title]),
    Select = case get(select, V) of
        S when is_binary(S) -> S;
        S -> throw({invalid_select, Name, S})
    end,
    case selector(Select) of
        {ok, _} -> ok;
        {error, Why} -> throw({invalid_select, Name, Why})
    end,
    Map = map_spec(Name, maps:get(map, V, native)),
    Key = key_spec(Name, Map, get(key, V)),
    Value = value_spec(Name, Map, maps:get(value, V, undefined)),
    Reduce = reduce(Name, maps:get(reduce, V, none)),
    check_reduce(Name, Reduce, Value),
    Limits = limits(Name, maps:get(limits, V, #{})),
    Base = #{select => Select, map => Map, key => Key, value => Value,
             reduce => Reduce, limits => Limits},
    case maps:find(title, V) of
        {ok, T} -> Base#{title => text(T, {Name, title})};
        error -> Base
    end;
view(Name, View) ->
    throw({invalid_view, Name, View}).

map_spec(_Name, native) -> native;
map_spec(_Name, <<"native">>) -> native;
map_spec(Name, Map) when is_map(Map) ->
    M = keys(Map, [abi, module, export, fields]),
    case maps:get(abi, M, 1) of
        1 -> ok;
        Abi -> throw({unsupported_abi, Name, Abi})
    end,
    Digest = digest(Name, get(module, M)),
    Export = case maps:get(export, M, <<"map">>) of
        E when is_binary(E), byte_size(E) >= 1, byte_size(E) =< 64 -> E;
        E -> throw({invalid_export, Name, E})
    end,
    Fields = case maps:get(fields, M, []) of
        Fs when is_list(Fs), length(Fs) =< ?MAX_FIELDS -> [field(Name, F) || F <- Fs];
        Fs -> throw({invalid_fields, Name, Fs})
    end,
    #{abi => 1, module => Digest, export => Export, fields => Fields};
map_spec(Name, Other) ->
    throw({invalid_map, Name, Other}).

digest(_Name, <<D:32/binary>>) -> D;
digest(Name, Hex) when is_binary(Hex), byte_size(Hex) =:= 64 ->
    try binary:decode_hex(Hex)
    catch error:badarg -> throw({invalid_module_digest, Name})
    end;
digest(Name, _) ->
    throw({invalid_module_digest, Name}).

field(Name, {Path, Type}) ->
    field(Name, #{path => Path, type => Type});
field(Name, F) when is_map(F) ->
    M = keys(F, [path, type]),
    Path = path(Name, get(path, M)),
    Type = field_type(Name, get(type, M)),
    Path =/= [] orelse Type =:= any orelse throw({root_field_must_be_any, Name}),
    {Path, Type};
field(Name, F) ->
    throw({invalid_field, Name, F}).

key_spec(Name, Map, Parts) when is_list(Parts), Parts =/= [],
                                length(Parts) =< ?MAX_KEY_PARTS ->
    Comps = [key_part(Name, Map, P) || P <- Parts],
    Cats = [C || #{category := C} <- Comps],
    %% categories are leading columns: true* false*
    {_, Rest} = lists:splitwith(fun(C) -> C end, Cats),
    lists:member(true, Rest) andalso throw({category_not_leading, Name}),
    {array, Comps};
key_spec(Name, Map, Part) when is_map(Part) ->
    Comp = key_part(Name, Map, Part),
    maps:get(category, Comp) andalso throw({category_needs_array_key, Name}),
    {scalar, Comp};
key_spec(Name, _Map, Other) ->
    throw({invalid_key, Name, Other}).

key_part(Name, Map, P) when is_map(P) ->
    M = keys(P, [type, category, path, title]),
    Type = key_type(Name, maps:get(type, M, any)),
    Category = case maps:get(category, M, false) of
        C when is_boolean(C) -> C;
        C -> throw({invalid_category, Name, C})
    end,
    Base = #{type => Type, category => Category},
    WithPath = case {Map, maps:find(path, M)} of
        {native, {ok, Path}} -> Base#{path => path(Name, Path)};
        {native, error} -> throw({native_key_needs_path, Name});
        {_, {ok, _}} -> throw({wasm_key_has_path, Name});
        {_, error} -> Base
    end,
    case maps:find(title, M) of
        {ok, T} -> WithPath#{title => text(T, {Name, title})};
        error -> WithPath
    end;
key_part(Name, _Map, P) ->
    throw({invalid_key_part, Name, P}).

value_spec(_Name, _Map, undefined) -> #{type => any};
value_spec(Name, native, V) when is_map(V) ->
    M = keys(V, [path, sum, type, title]),
    case {maps:find(sum, M), maps:find(path, M)} of
        {{ok, P}, error} -> #{sum => path(Name, P), type => i64};
        {error, {ok, P}} -> #{path => path(Name, P), type => value_type(Name, maps:get(type, M, any))};
        _ -> throw({invalid_value, Name, V})
    end;
value_spec(Name, _Wasm, V) when is_map(V) ->
    M = keys(V, [type, title]),
    #{type => value_type(Name, maps:get(type, M, any))};
value_spec(Name, _Map, V) ->
    throw({invalid_value, Name, V}).

reduce(_Name, R) when R =:= none; R =:= <<"none">> -> none;
reduce(_Name, R) when R =:= '_sum'; R =:= <<"_sum">> -> '_sum';
reduce(_Name, R) when R =:= '_count'; R =:= <<"_count">> -> '_count';
reduce(Name, R) -> throw({invalid_reduce, Name, R}).

check_reduce(Name, '_sum', #{type := T}) when T =/= i64 ->
    throw({sum_needs_i64_value, Name});
check_reduce(_Name, _Reduce, _Value) ->
    ok.

limits(Name, L) when is_map(L) ->
    Given = keys(L, [K || {K, _, _, _} <- ?LIMITS]),
    maps:from_list(
        [case maps:find(K, Given) of
             {ok, V} when is_integer(V), V >= Min, V =< Max -> {K, V};
             {ok, V} -> throw({invalid_limit, Name, K, V});
             error -> {K, Def}
         end || {K, Def, Min, Max} <- ?LIMITS]);
limits(Name, L) ->
    throw({invalid_limits, Name, L}).

-spec default_limits() -> map().
default_limits() ->
    maps:from_list([{K, Def} || {K, Def, _, _} <- ?LIMITS]).

%%====================================================================
%% Signatures and module references
%%====================================================================

%% @doc SHA-256 of the canonical semantic form of a normalized view.
-spec signature(view()) -> binary().
signature(View) ->
    Semantic = #{select => maps:get(select, View),
                 map => maps:get(map, View),
                 key => untitled(maps:get(key, View)),
                 value => maps:remove(title, maps:get(value, View)),
                 reduce => maps:get(reduce, View),
                 limits => maps:get(limits, View),
                 key_encoding => barrel_design_key:version()},
    crypto:hash(sha256, barrel_docdb_codec_cbor:encode_cbor(canonical(Semantic))).

untitled({array, Comps}) -> {array, [maps:remove(title, C) || C <- Comps]};
untitled({scalar, Comp}) -> {scalar, maps:remove(title, Comp)}.

%% atoms and tuples become tagged binaries and lists
canonical(M) when is_map(M) ->
    maps:from_list([{canonical_key(K), canonical(V)} || {K, V} <- maps:to_list(M)]);
canonical(L) when is_list(L) -> [canonical(E) || E <- L];
canonical(T) when is_tuple(T) -> [<<"t">> | [canonical(E) || E <- tuple_to_list(T)]];
canonical(A) when is_atom(A) -> <<"a:", (atom_to_binary(A))/binary>>;
canonical(B) when is_binary(B) -> <<"b:", (binary:encode_hex(B))/binary>>;
canonical(I) when is_integer(I) -> I.

canonical_key(K) when is_atom(K) -> atom_to_binary(K);
canonical_key(K) when is_binary(K) -> K.

%% @doc Module digests the design's views reference.
-spec modules(design()) -> [binary()].
modules(#{views := Views}) ->
    lists:usort([D || #{map := #{module := D}} <- maps:values(Views)]).

%%====================================================================
%% Selectors
%%====================================================================

%% @doc Compile a stored selector: `SELECT * FROM db WHERE ...' over one
%% document, with no parameters, ordering, limits, projection or unnest.
-spec selector(binary()) -> {ok, map()} | {error, term()}.
selector(Bql) ->
    case barrel_bql:compile(Bql) of
        {ok, #{source := {collection, _}, subscribe := false, unnest := undefined,
               spec := Spec,
               post := #{order := undefined, offset := 0, limit := undefined,
                         project := star, doc_where := [], residual := Residual}}} ->
            Where = case maps:get(where, Spec, []) of
                [] -> none;
                Conds ->
                    {ok, Compiled} = barrel_query:compile(#{where => Conds}),
                    Compiled
            end,
            {ok, #{where => Where, residual => Residual,
                   id_prefix => maps:get(id_prefix, Spec, undefined),
                   id_range => maps:get(id_range, Spec, {undefined, undefined})}};
        {ok, _Other} ->
            {error, not_a_document_predicate};
        {error, Error} ->
            {error, barrel_bql:format_error(Error)}
    end.

%% @doc Whether a document (with its `id') passes a compiled selector.
-spec selected(map(), map()) -> boolean().
selected(#{where := Where, residual := Residual, id_prefix := Prefix,
           id_range := {Start, End}}, #{<<"id">> := Id} = Doc) ->
    id_ok(Id, Prefix, Start, End)
        andalso (Where =:= none orelse barrel_query:match(Where, Doc))
        andalso (Residual =:= []
                 orelse barrel_bql_exec:residual_match(
                          Residual, #{doc => Doc, elem => undefined, score => #{}})).

id_ok(Id, Prefix, _Start, _End) when is_binary(Prefix) ->
    Size = byte_size(Prefix),
    case Id of
        <<Prefix:Size/binary, _/binary>> -> true;
        _ -> false
    end;
id_ok(Id, undefined, Start, End) ->
    (Start =:= undefined orelse Id >= Start) andalso (End =:= undefined orelse Id < End).

%%====================================================================
%% Helpers
%%====================================================================

%% Known keys may be atoms or binaries; unknown keys are refused.
keys(Map, Known) ->
    maps:fold(
        fun(K, V, Acc) when is_atom(K) ->
                lists:member(K, Known) orelse throw({unknown_key, K}),
                Acc#{K => V};
           (K, V, Acc) when is_binary(K) ->
                case [A || A <- Known, atom_to_binary(A) =:= K] of
                    [A] -> Acc#{A => V};
                    [] -> throw({unknown_key, K})
                end;
           (K, _V, _Acc) ->
                throw({unknown_key, K})
        end, #{}, Map).

get(K, M) ->
    case maps:find(K, M) of
        {ok, V} -> V;
        error -> throw({missing, K})
    end.

name(N, What) when is_binary(N), byte_size(N) >= 1, byte_size(N) =< ?NAME_MAX ->
    case re:run(N, <<"^[a-z][a-z0-9_]*$">>, [{capture, none}]) of
        match -> N;
        nomatch -> throw({invalid_name, What, N})
    end;
name(N, What) ->
    throw({invalid_name, What, N}).

text(T, _What) when is_binary(T), byte_size(T) =< 256 -> T;
text(T, What) -> throw({invalid_text, What, T}).

path(Name, P) when is_list(P), length(P) =< ?MAX_PATH ->
    lists:foreach(fun(C) when is_binary(C), byte_size(C) >= 1 -> ok;
                     (C) when is_integer(C), C >= 0 -> ok;
                     (C) -> throw({invalid_path, Name, C})
                  end, P),
    P;
path(Name, P) ->
    throw({invalid_path, Name, P}).

key_type(Name, T) ->
    case type(T) of
        Ty when Ty =:= text; Ty =:= i64; Ty =:= bool; Ty =:= any -> Ty;
        _ -> throw({invalid_key_type, Name, T})
    end.

value_type(Name, T) ->
    case type(T) of
        Ty when Ty =:= text; Ty =:= i64; Ty =:= bool; Ty =:= any -> Ty;
        _ -> throw({invalid_value_type, Name, T})
    end.

field_type(Name, T) ->
    case type(T) of
        invalid -> throw({invalid_field_type, Name, T});
        Ty -> Ty
    end.

type(T) when T =:= text; T =:= i64; T =:= bool; T =:= any -> T;
type({array, E}) when E =:= text; E =:= i64 -> {array, E};
type(<<"text">>) -> text;
type(<<"i64">>) -> i64;
type(<<"bool">>) -> bool;
type(<<"any">>) -> any;
type(<<"array:text">>) -> {array, text};
type(<<"array:i64">>) -> {array, i64};
type(_) -> invalid.
