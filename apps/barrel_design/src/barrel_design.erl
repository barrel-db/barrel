%% @doc Versioned designs with automatically maintained views.
%%
%% A design packages named views for a database. Each view selects
%% documents with a BQL predicate and builds ordered key/value rows,
%% either with a native projection or with a Wasm map run under fresh
%% state, finite fuel, a deadline and memory limits.
-module(barrel_design).

-export([preview/3]).

%% @doc Run a view definition over sample documents without installing
%% anything. Source is `{wat, Text}' (parsed in Erlang, never cached) or
%% `{wasm, Bytes}'. The view is a manifest view whose `map' omits
%% `module'. Returns, per document, its rows, its error or that the
%% selector skipped it.
-spec preview({wat, binary() | string()} | {wasm, binary()}, map(), [map()]) ->
    {ok, [map()]} | {error, term()}.
preview(Source, View, Docs) when is_map(View), is_list(Docs) ->
    case preview_module(Source) of
        {ok, Module, Digest} ->
            Map = maps:get(map, View, maps:get(<<"map">>, View, #{})),
            Design = #{name => <<"preview">>, version => 1,
                       views => #{<<"preview">> =>
                                      maps:remove(<<"map">>, View#{map => Map#{module => Digest}})}},
            case barrel_design_manifest:normalize(Design) of
                {ok, #{views := #{<<"preview">> := V}}} -> preview_run(Module, V, Docs);
                {error, _} = Error -> Error
            end;
        {error, _} = Error ->
            Error
    end.

preview_module({wat, Text}) when is_list(Text) ->
    preview_module({wat, unicode:characters_to_binary(Text)});
preview_module({wat, Text}) when is_binary(Text) ->
    case wasm:compile({wat, Text}) of
        {ok, Module} -> {ok, Module, <<0:256>>};
        {error, E} -> {error, {invalid_module, wat, E}}
    end;
preview_module({wasm, Bytes}) when is_binary(Bytes) ->
    case wasm:compile(Bytes) of
        {ok, Module} -> {ok, Module, crypto:hash(sha256, Bytes)};
        {error, E} -> {error, {invalid_module, wasm, E}}
    end.

preview_run(Module, #{map := #{export := Export} = Map, limits := Limits} = View, Docs) ->
    case barrel_design_module:check(Module, preview, #{export => Export,
                                                       max_memory_pages => maps:get(max_memory_pages, Limits)}) of
        {ok, _} ->
            {ok, Sel} = barrel_design_manifest:selector(maps:get(select, View)),
            Spec = guest_spec(View),
            Selected = [D || D <- Docs, barrel_design_manifest:selected(Sel, D)],
            Results = maps:from_list(lists:zip([id(D) || D <- Selected],
                                               barrel_design_guest:run(Module, Spec, Selected, #{}))),
            _ = Map,
            {ok, [preview_row(id(D), maps:find(id(D), Results)) || D <- Docs]};
        {error, _} = Error ->
            Error
    end.

preview_row(Id, error) -> #{id => Id, selected => false};
preview_row(Id, {ok, {ok, Rows}}) ->
    #{id => Id, selected => true, rows => [#{key => K, value => V} || {K, V} <- Rows]};
preview_row(Id, {ok, {error, Why}}) -> #{id => Id, selected => true, error => Why}.

id(#{<<"id">> := Id}) -> Id;
id(_) -> undefined.

%% @private The runner's view of a normalized Wasm view.
guest_spec(#{map := #{export := Export, fields := Fields}, key := Key, value := Value,
             limits := Limits}) ->
    #{export => Export, fields => Fields, key => Key, value => Value, limits => Limits}.
